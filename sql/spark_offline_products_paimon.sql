-- ============================================================
-- Spark 离线作业: MySQL products -> Paimon (ODS 层回填)
--
-- 一套表结构: 与 Flink 写入同一张物理表 paimon.cdc_demo.products
-- 一套逻辑:   均以 product_id 为主键做 upsert
-- ============================================================

-- 1. 通过 JDBC 读取 MySQL 源表
DROP TABLE IF EXISTS mysql_products_src;
CREATE TABLE mysql_products_src USING jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/cdc_demo?useSSL=false&serverTimezone=UTC',
  dbtable 'products',
  user 'root',
  password 'root123',
  driver 'com.mysql.cj.jdbc.Driver',
  fetchsize '1000'
);

-- 2. 目标表结构与 Flink 完全一致
CREATE TABLE IF NOT EXISTS paimon.cdc_demo.products (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP
) USING paimon
TBLPROPERTIES (
    'bucket' = '1',
    'bucket-key' = 'product_id'
);

-- 3. MERGE INTO: 按主键 upsert
MERGE INTO paimon.cdc_demo.products t
USING mysql_products_src s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
