# Applied DB Replication

Проект реализует прикладную репликацию `master -> kafka -> receiver -> slave`.

## Что реализовано

- Репликация построена на уровне JDBC: каждое `INSERT/UPDATE/DELETE` выполняется с `RETURNING`, и возвращенные строки сразу формируют событие `ReplicationMessage`.
- Отправка в Kafka выполняется после commit (через `afterCommit`), без outbox-таблиц.
- `service-receiver` читает события и применяет их в `slave` через `UPSERT/DELETE`.
- В runtime-приложениях нет Liquibase.
- Liquibase используется только в интеграционных тестах для подготовки схем обеих БД.

## Запуск инфраструктуры

```bash
docker compose up --build
```

- `service-master`: `localhost:8081`
- `service-receiver`: `localhost:8082`
- `master-db`: `localhost:5433`
- `slave-db`: `localhost:5434`
- `kafka`: `localhost:9092`

## Интеграционные тесты

- Основной e2e-тест: `service-master/src/test/kotlin/com/applied/replication/master/integration/EndToEndReplicationIntegrationTest.kt`
- Тест поднимает Kafka + master/slave Postgres в Docker (Testcontainers), прогоняет Liquibase миграции и проверяет репликацию для `insert/update/delete` через:
  - JDBC `INSERT ... RETURNING`
  - JDBC `UPDATE ... RETURNING`
  - JDBC `DELETE ... RETURNING`
  - JDBC batch insert
