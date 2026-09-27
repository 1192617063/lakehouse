-- ============================================================
-- 阶段 2-C: Kafka (cdc_mysql_products) -> Paimon
-- ============================================================

SET 'execution.checkpointing.interval' = '10s';

USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS cdc_pipeline;
USE cdc_pipeline;

DROP TABLE IF EXISTS kafka_products_source;
CREATE TABLE kafka_products_source (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'cdc_mysql_products',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'properties.group.id' = 'flink-paimon-products',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS cdc_demo;
USE cdc_demo;

DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP(3)
) WITH (
    'bucket' = '1',
    'bucket-key' = 'product_id',
    'commit.force.delay' = '5s'
);

INSERT INTO paimon_catalog.cdc_demo.products
SELECT * FROM default_catalog.cdc_pipeline.kafka_products_source;
