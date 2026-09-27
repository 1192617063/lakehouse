-- ============================================================
-- 阶段 2-B: Kafka (cdc_pg_users) -> Hudi
-- ============================================================

USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS cdc_pipeline;
USE cdc_pipeline;

DROP TABLE IF EXISTS kafka_users_source;
CREATE TABLE kafka_users_source (
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
    'properties.group.id' = 'flink-hudi-users',
    'key.format' = 'json',
    'value.format' = 'json'
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
    'table.type' = 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field' = 'user_id',
    'hoodie.datasource.write.precombine.field' = 'updated_at',
    'hoodie.datasource.write.operation' = 'upsert'
);

INSERT INTO hudi_catalog.cdc_demo.users
SELECT * FROM default_catalog.cdc_pipeline.kafka_users_source;
