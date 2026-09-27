-- ============================================================
-- Pipeline 2: PostgreSQL CDC (users) -> Hudi
-- 源: PostgreSQL cdc_demo.users
-- 目标: Hudi 表 hudi_catalog.cdc_demo.users
-- ============================================================

USE CATALOG default_catalog;
USE cdc_pipeline;

DROP TABLE IF EXISTS pg_users_source;
CREATE TABLE pg_users_source (
    user_id BIGINT,
    username STRING,
    email STRING,
    age INT,
    city STRING,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres123',
    'database-name' = 'cdc_demo',
    'schema-name' = 'public',
    'table-name' = 'users',
    'slot.name' = 'flink_slot',
    'publication.name' = 'flink_pub',
    'decoding.plugin.name' = 'pgoutput',
    'scan.incremental.snapshot.enabled' = 'true'
);

USE CATALOG hudi_catalog;
CREATE DATABASE IF NOT EXISTS cdc_demo;
USE cdc_demo;

DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id BIGINT,
    username STRING,
    email STRING,
    age INT,
    city STRING,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'changelog.enabled' = 'true',
    'hoodie.datasource.write.recordkey.field' = 'user_id',
    'hoodie.datasource.write.precombine.field' = 'updated_at',
    'hoodie.datasource.write.operation' = 'upsert',
    'hoodie.compact.inline' = 'true',
    'hoodie.compact.inline.max.delta.commits' = '5'
);

INSERT INTO hudi_catalog.cdc_demo.users
SELECT * FROM default_catalog.cdc_pipeline.pg_users_source;
