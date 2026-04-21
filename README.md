# Applied DB Replication

Проект реализует прикладную репликацию данных:

`master-db -> service-master -> Kafka -> service-receiver -> slave-db`

## Как это работает

### `service-master`: от JDBC до Kafka

1. **Прокси над `DataSource` (`MutationCapturingDataSourceBeanPostProcessor`)**  
   После инициализации бина `DataSource` он оборачивается в `MutationCapturingDataSource`: каждый `getConnection()` возвращает прокси `Connection`, а `prepareStatement` / `prepareCall` — прокси `PreparedStatement`. Так перехватывается весь путь ORM (Hibernate, `JdbcTemplate`, нативные запросы), если он использует этот пул.

2. **Переписывание DML (`MutationReturningSqlRewriter`)**  
   Перед выполнением текста SQL для распознанных `INSERT` / `UPDATE` / `DELETE` добавляется или заменяется предложение `RETURNING *`. Для `UPDATE` при отсутствии явного присваивания `version` в `SET` внедряется `version = COALESCE(version, 0) + 1`.

3. **`commitId` для группировки событий**  
   Для каждого прокси `PreparedStatement` фиксируется `commitId`: при активной Spring-транзакции один и тот же id привязывается к транзакции (ресурс в `TransactionSynchronizationManager`); без транзакции — стабильный id на время жизни логического соединения с пулом. Это значение попадает во все сообщения Kafka по этой транзакции и используется как ключ сообщения.

4. **Публикация после успешного DML (`JdbcRowReplicationPayloadMapper`)**  
   После `executeUpdate` / `executeLargeUpdate` / `execute` читается `ResultSet` из `RETURNING`: по каждой строке собирается JSON (имена колонок как в `ResultSet`). Для `DELETE` значение колонки `version` в payload увеличивается на 1. Далее вызывается `ReplicationEventDispatcher.dispatchAfterCommit`.

5. **Отправка в Kafka (`ReplicationEventDispatcher`, пакет `outbound.kafka`)**  
   Регистрируется `TransactionSynchronization.afterCommit`: `KafkaTemplate.send` только после успешного коммита. Без активной транзакции (автокоммит) отправка выполняется сразу после выполнения запроса.

6. **Валидация схемы при старте (пакет `schema`)**  
   Два независимых `ApplicationRunner`: `VersionColumnPresenceStartupValidator` и `PrimaryKeyPresenceStartupValidator`. В текущей схеме (`current_schema()`) для всех `BASE TABLE` проверяются:
   - наличие колонки `version` типа `integer`;
   - наличие **PRIMARY KEY**.  
   Нарушение любого правила останавливает старт (`require` с перечислением таблиц).

### Контракт `version`

- В каждой таблице обязательно должно быть поле `version INTEGER` (проверяется при старте `service-master`, см. выше).
- Рекомендуемый DDL:
  - `version INTEGER NOT NULL DEFAULT 0`
- Поведение при репликации:
  - insert → в БД обычно версия 0;
  - update → инкремент `version` через переписанный SQL;
  - delete → в событии Kafka передаётся снимок строки с `version + 1` в поле `version`.

### Первичный ключ (PK)

- **У каждой реплицируемой таблицы в `master-db` и `slave-db` обязан быть первичный ключ (PRIMARY KEY).** На старте `service-master` проверяет наличие PK во всех таблицах текущей схемы; при отсутствии хотя бы у одной таблицы приложение **не запускается**.
- Без PK репликация не согласуется с моделью событий: в `payload` приходят полные строки, а `service-receiver` строит `INSERT ... ON CONFLICT` и `DELETE` по ключу, сопоставляя строку в `slave` с сообщением.
- Если у таблицы нет PK в кэше receiver (таблица появилась после старта и т.п.), при обработке сообщения возможна ошибка в `ReplicationApplier`.

### Генерируемые на уровне БД колонки (`SERIAL`, `IDENTITY`, `GENERATED`)

Для предсказуемой репликации и корректного JSON в Kafka **в продуктовых таблицах не следует полагаться на значения, которые БД подставляет сама**, в частности:

- типы вида **`SERIAL` / `BIGSERIAL`** и колонки **`GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY`**;
- выражения **`GENERATED ALWAYS AS ... STORED`** (и аналоги), если бизнес-ключ или версия должны однозначно контролироваться приложением.

Причина: событие строится из результата **`RETURNING *`** после DML. Хотя СУБД часто вернёт сгенерированное значение, поведение разных клиентов ORM и порядок flush/insert могут приводить к рассинхрону ожиданий; явные значения PK и `version` на стороне приложения проще согласовать с `service-receiver` и upsert по ключу. **Рекомендуется** задавать ключи и версии явно в приложении (например `BIGINT` без автоинкремента или идентификатор из сервиса генерации id).

В тестовых Liquibase-скриптах этого репозитория `BIGSERIAL` может использоваться для краткости демо-схем; для боевых схем ориентируйтесь на явные ключи и описанный контракт.

### Kafka сообщение

`ReplicationMessage`:

- `eventId` — локальный последовательный id события;
- `commitId` — общий идентификатор на время Spring-транзакции (`TransactionSynchronizationManager`); без активной транзакции — один id на выданное из пула соединение (прокси `Connection`), без запросов к БД;
- `tableName` — имя таблицы;
- `operation` — `INSERT` / `UPDATE` / `DELETE`;
- `payload` — JSON снимок строки;
- `iterationCount` — служебный счётчик **только на стороне receiver**: с `master` всегда приходит `0`; при каждой повторной публикации в Kafka после `REQUEUE` значение **увеличивается на 1** (в том же `ReplicationMessage`, без изменения бизнес-`payload`).

Kafka key теперь равен `commitId` (строка).

### Логика `service-receiver`

- Для `INSERT/UPDATE`:
  - если запись уже есть и `db.version >= message.version`, сообщение пропускается без ошибки;
  - применяется только строго следующая версия: `db.version + 1 == message.version`;
  - если пришел `UPDATE`, а записи еще нет, выполняется upsert как insert.
- Для `DELETE`:
  - удаление выполняется только когда `db.version + 1 == message.version`;
  - иначе сообщение считается out-of-order и переотправляется в тот же topic (requeue).
- Ошибки при upsert (один SQL `INSERT ... ON CONFLICT ...`):
  - **дубль по UNIQUE / PK** (PostgreSQL `SQLSTATE 23505`, Spring `DuplicateKeyException`) — сообщение **пропускается** (`SKIPPED`), в Kafka повторно **не** отправляется;
  - **любая другая ошибка JDBC** при upsert (FK, NOT NULL, сеть и т.д.) — результат `REQUEUE`, `ReplicationConsumer` **снова публикует** то же сообщение в тот же topic с тем же ключом `commitId`, увеличив **`iterationCount` на 1**.
- Лимит повторных прогонов: если `iterationCount > app.replication.max-requeue-iterations` (по умолчанию `10`), сообщение **больше не обрабатывается** (не вызывается `apply`, повторно в Kafka не отправляется). Порог задаётся в `service-receiver` `application.yml` (`app.replication.max-requeue-iterations`).
- Выполнение upsert идет в **отдельной короткой транзакции** `REQUIRES_NEW` (`ReplicationUpsertExecutor`), чтобы ошибка SQL не оставляла внешнюю транзакцию в состоянии aborted.
- Метаданные PK по всем таблицам `current_schema()` читаются **один раз при старте** (`ReplicationPrimaryKeyCache` в пакете `schema`); таблицы, созданные только после старта, в кэше не появятся (нужен рестарт сервиса или расширение логики).
- Чтение колонок из `information_schema` и построение выражений `(payload->>'col')::type` вынесены в `apply.ReplicationTableColumnCatalog` и `apply.ReplicationPayloadJsonSqlExpressions` (для `text[]` поддержан `_text`).

### Структура пакетов (основное)

| Модуль | Пакет | Назначение |
|--------|--------|------------|
| `service-master` | `...master.mutation` | перехват JDBC, переписывание DML, маппинг строк `RETURNING` в JSON |
| `service-master` | `...master.outbound.kafka` | публикация `ReplicationMessage` в Kafka после коммита, producer beans |
| `service-master` | `...master.schema` | проверки схемы БД при старте (`version`, PK) |
| `service-receiver` | `...receiver.inbound.kafka` | consumer/listener, конфигурация Kafka consumer/producer для receiver |
| `service-receiver` | `...receiver.apply` | применение сообщения к БД: каталог колонок, SQL из JSON, upsert в отдельной транзакции, идентификаторы SQL |
| `service-receiver` | `...receiver.schema` | кэш PK таблиц при старте |
| `service-receiver` | `...receiver.replication` | координация: `ReplicationApplier` (оркестрация INSERT/UPDATE/DELETE) |
| `service-receiver` | `...receiver.config` | свойства приложения |

## Запуск окружения

```bash
docker compose up --build
```

- `service-master`: `http://localhost:8081`
- `service-receiver`: `http://localhost:8082`
- `master-db`: `localhost:5433` (`master/master`, db `master`)
- `slave-db`: `localhost:5434` (`slave/slave`, db `slave`)
- `kafka`: `localhost:29092` (внутри docker сети `kafka:9092`)

## Как проверить вручную

1. Поднять окружение через `docker compose up --build`.
2. Внести изменения в `master-db` (insert/update/delete в таблицах с `version`).
3. Проверить в `slave-db`, что:
   - данные синхронизировались;
   - версия инкрементируется последовательно;
   - устаревшие сообщения не ломают данные.

## Тесты и покрытие сценариев

Ниже — какие кейсы закрыты тестами и как их запускать. Для интеграционных тестов с PostgreSQL сначала поднимите БД (например `docker compose up`).

- **`mvn test`** (фаза `test`, Surefire) — только классы с суффиксом `Test` / `Tests` и т.п. по соглашению Surefire; **без** поднятого Postgres/Kafka проходит весь этот набор.
- **`mvn verify`** — плюс **Failsafe** для классов `*IT` (интеграция с реальными БД и Kafka для master E2E). Нужно работающее окружение.

### E2E `service-master` + внешний slave + Kafka

Файл: `service-master/src/test/kotlin/com/applied/replication/master/integration/EndToEndReplicationIT.kt`.

| Сценарий | Где в тестах |
|----------|----------------|
| Репликация insert/update/delete через разные клиенты БД | `InsertTests`, `UpdateTests`, `DeleteTests` |
| Несколько таблиц и FK в одной транзакции | `SingleTransactionMultiInsertTests` |
| `@Transactional` + несколько DML | `TransactionalAnnotationTests` |
| Версии: insert `version=0`, update инкремент | `UpdateTests`, `VersioningAndOrderingTests` |
| Receiver: skip stale, update-as-insert, delete → requeue | `VersioningAndOrderingTests` |

Переменные/свойства подключения (по умолчанию localhost и порты из `docker-compose`): `TEST_MASTER_JDBC_URL`, `TEST_SLAVE_JDBC_URL`, `TEST_KAFKA_BOOTSTRAP_SERVERS` (см. companion в тесте).

Запуск:

```bash
mvn -pl service-master -am -Dit.test=EndToEndReplicationIT -Dfailsafe.failIfNoSpecifiedTests=false verify
```

Отдельные nested-классы:

```bash
mvn -pl service-master -am -Dit.test=EndToEndReplicationIT\$InsertTests -Dfailsafe.failIfNoSpecifiedTests=false verify
mvn -pl service-master -am -Dit.test=EndToEndReplicationIT\$UpdateTests -Dfailsafe.failIfNoSpecifiedTests=false verify
mvn -pl service-master -am -Dit.test=EndToEndReplicationIT\$DeleteTests -Dfailsafe.failIfNoSpecifiedTests=false verify
mvn -pl service-master -am -Dit.test=EndToEndReplicationIT\$VersioningAndOrderingTests -Dfailsafe.failIfNoSpecifiedTests=false verify
```

### Юнит-тесты `service-master` (SQL без БД)

Файл: `service-master/src/test/kotlin/com/applied/replication/master/mutation/MutationReturningSqlRewriterTest.kt`.

| Сценарий | Проверка |
|----------|------------|
| `INSERT` / `DELETE` → добавление `RETURNING *` | несколько тестов |
| `UPDATE` → инкремент `version` перед `WHERE` | `update injects version increment before WHERE` |
| Уже есть `version =` в SET → повторно не дописывается | `update does not inject version when assignment already present` |
| Замена существующего `RETURNING` | `replaces existing RETURNING clause` |
| Не-DML SQL не трогаем | `non mutation sql is unchanged` |
| Разбор `mutationMetaOrNull` (INSERT/UPDATE/DELETE) | `mutation meta parses table and operation` |

```bash
mvn -pl service-master -am -Dtest=MutationReturningSqlRewriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

(Это юнит-тест Surefire, достаточно фазы `test`.)

### Интеграционные и модульные тесты `service-receiver`

- **`ReplicationApplierIT`** — поднимается урезанный Spring-контекст без Kafka (`ReceiverReplicationTestApplication`), схема `recv_applier_*` создается скриптом `receiver-replication-test-schema.sql` при старте. Нужен **доступный PostgreSQL** (по умолчанию `jdbc:postgresql://localhost:5434/slave`, учетные данные как у slave в compose). Переопределение: `TEST_RECEIVER_JDBC_URL` / `TEST_SLAVE_JDBC_URL`, `TEST_RECEIVER_DB_USERNAME`, `TEST_RECEIVER_DB_PASSWORD`.

| Класс / сценарий | Что проверяется |
|------------------|-----------------|
| `PrimaryKeyCacheTests` | кэш PK при старте, пустой список для неизвестной таблицы |
| `PayloadValidationTests` | нет поля `version`, нет PK в кэше, неподдерживаемая операция |
| `UpsertTests` | успешный insert, дубль по UNIQUE → `SKIPPED`, FK → `REQUEUE`, NOT NULL → `REQUEUE`, устаревшая версия / gap / следующая версия, UPDATE без строки как insert, вставка child при живом parent |
| `DeleteTests` | успешный delete, нет строки → `SKIPPED`, неверная версия → `REQUEUE`, нет PK в payload → исключение |

- **`ReplicationConsumerRequeueTest`** (Mockito, без БД): при `REQUEUE` вызывается `KafkaTemplate.send` с тем же сообщением и **`iterationCount + 1`**; при `SKIPPED` / `APPLIED` — `send` не вызывается; при `iterationCount > maxRequeueIterations` — `apply` не вызывается и requeue нет; при `iterationCount == max` — `apply` ещё вызывается.

Запуск только receiver-тестов (юнит Surefire + интеграция Failsafe):

```bash
mvn -pl service-receiver -am -Dtest=ReplicationConsumerRequeueTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl service-receiver -am -Dit.test=ReplicationApplierIT -Dfailsafe.failIfNoSpecifiedTests=false verify
```

Запуск отдельного nested (пример):

```bash
mvn -pl service-receiver -am -Dit.test=ReplicationApplierIT\$UpsertTests -Dfailsafe.failIfNoSpecifiedTests=false verify
```
