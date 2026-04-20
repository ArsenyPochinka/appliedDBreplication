DROP TABLE IF EXISTS recv_applier_child CASCADE;
DROP TABLE IF EXISTS recv_applier_t1 CASCADE;

CREATE TABLE recv_applier_t1 (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE recv_applier_child (
    id BIGINT PRIMARY KEY,
    parent_id BIGINT NOT NULL REFERENCES recv_applier_t1 (id),
    title TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0
);
