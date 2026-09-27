-- ============================================================
-- 阶段 1-A: MySQL CDC (orders) -> Kafka (upsert-kafka)
-- 源: MySQL cdc_demo.orders
-- 目标: Kafka topic cdc_mysql_orders (upsert-kafka + json)
-- upsert-kafka 以主键为 key，支持 INSERT/UPDATE/DELETE 语义
-- ============================================================

USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS cdc_pipeline;
USE cdc_pipeline;

DROP TABLE IF EXISTS mysql_orders_source;
CREATE TABLE mysql_orders_source (
    order_id BIGINT,
    customer_name STRING,
    product_name STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root123',
    'database-name' = 'cdc_demo',
    'table-name' = 'orders',
    'server-time-zone' = 'UTC',
    'scan.incremental.snapshot.enabled' = 'true'
);

DROP TABLE IF EXISTS kafka_orders_sink;
CREATE TABLE kafka_orders_sink (
    order_id BIGINT,
    customer_name STRING,
    product_name STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'cdc_mysql_orders',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO kafka_orders_sink
SELECT * FROM mysql_orders_source;
