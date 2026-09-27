-- ============================================================
-- Pipeline 1: MySQL CDC (orders) -> Iceberg
-- 源: MySQL cdc_demo.orders
-- 目标: Iceberg 表 iceberg_catalog.cdc_demo.orders
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
SELECT * FROM default_catalog.cdc_pipeline.mysql_orders_source;
