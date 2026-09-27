-- ============================================================
-- Pipeline 3: MySQL CDC (products) -> Paimon
-- 源: MySQL cdc_demo.products
-- 目标: Paimon 表 paimon_catalog.cdc_demo.products
-- ============================================================

USE CATALOG default_catalog;
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
    updated_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'bucket' = '1',
    'changelog-producer' = 'input'
);

INSERT INTO paimon_catalog.cdc_demo.products
SELECT * FROM default_catalog.cdc_pipeline.mysql_products_source;
