-- ============================================================
-- Spark 离线作业: MySQL orders -> Iceberg (ODS 层回填)
--
-- 目的: 作为 Flink 实时 CDC 的离线补充/回灌链路
--   - 实时(Flink): MySQL CDC -> Kafka -> Iceberg.orders (秒级延迟)
--   - 离线(Spark): MySQL JDBC -> Iceberg.orders (批量 upsert)
--
-- 一套表结构: 与 Flink 写入同一张物理表 iceberg.cdc_demo.orders
-- 一套逻辑:   均以 order_id 为主键做 upsert，字段 1:1 映射无转换
--
-- 使用场景: 初始化全量导入、CDC 中断后数据修复、周期性全量校对
-- 注意: 与 Flink 同时写入同一张表时可能产生竞争，建议在 Flink 作业暂停时执行
-- ============================================================

-- 1. 通过 JDBC 读取 MySQL 源表（与 Flink CDC 同源）
DROP TABLE IF EXISTS mysql_orders_src;
CREATE TABLE mysql_orders_src USING jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/cdc_demo?useSSL=false&serverTimezone=UTC',
  dbtable 'orders',
  user 'root',
  password 'root123',
  driver 'com.mysql.cj.jdbc.Driver',
  fetchsize '1000'
);

-- 2. 目标表结构与 Flink 完全一致（若已存在则跳过）
CREATE TABLE IF NOT EXISTS iceberg.cdc_demo.orders (
    order_id BIGINT,
    customer_name STRING,
    product_name STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- 3. MERGE INTO: 按主键 upsert（与 Flink CDC 语义一致）
MERGE INTO iceberg.cdc_demo.orders t
USING mysql_orders_src s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
