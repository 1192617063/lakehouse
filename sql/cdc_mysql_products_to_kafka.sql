-- ============================================================
-- 阶段 1-B: MySQL CDC (products) -> Kafka (upsert-kafka)
-- ============================================================

USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS cdc_pipeline;
USE cdc_pipeline;

DROP TABLE IF EXISTS mysql_products_source;
CREATE TABLE mysql_products_source (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root123',
    'database-name' = 'cdc_demo',
    'table-name' = 'products',
    'server-time-zone' = 'UTC',
    'scan.incremental.snapshot.enabled' = 'true'
);

DROP TABLE IF EXISTS kafka_products_sink;
CREATE TABLE kafka_products_sink (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'cdc_mysql_products',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO kafka_products_sink
SELECT * FROM mysql_products_source;
