-- ============================================================
-- 阶段 1-C: PostgreSQL CDC (users) -> Kafka (upsert-kafka)
-- ============================================================

USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS cdc_pipeline;
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
    'decoding.plugin.name' = 'pgoutput',
    'scan.incremental.snapshot.enabled' = 'true'
);

DROP TABLE IF EXISTS kafka_users_sink;
CREATE TABLE kafka_users_sink (
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
    'connector' = 'upsert-kafka',
    'topic' = 'cdc_pg_users',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO kafka_users_sink
SELECT * FROM pg_users_source;
