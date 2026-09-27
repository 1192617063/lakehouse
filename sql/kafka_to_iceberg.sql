-- ============================================================
-- 阶段 2-A: Kafka (cdc_mysql_orders) -> Iceberg
-- ============================================================

USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS cdc_pipeline;
USE cdc_pipeline;

DROP TABLE IF EXISTS kafka_orders_source;
CREATE TABLE kafka_orders_source (
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
    'properties.group.id' = 'flink-iceberg-orders',
    'key.format' = 'json',
    'value.format' = 'json'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS cdc_demo;
USE cdc_demo;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
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
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

INSERT INTO iceberg_catalog.cdc_demo.orders
SELECT * FROM default_catalog.cdc_pipeline.kafka_orders_source;
