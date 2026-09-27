-- ============================================================
-- Spark 离线作业: DWS 层聚合（基于 ODS 层湖仓数据）
--
-- 数据流:
--   ODS (Flink 实时写入)  ->  DWS (Spark 离线聚合)
--   iceberg.cdc_demo.orders  +  cdc_demo.users (Hudi, 通过 Hive 目录访问)
--   paimon.cdc_demo.products
--
-- 说明:
--   - DWS 表由 Spark 批量生成，可按天/小时调度
--   - ODS 层数据来自 Flink 实时 CDC，保证新鲜度
--   - 离线聚合利用 Spark 的大规模批处理能力
--   - 结果写入独立的 DWS 表，不与实时链路竞争 ODS 表
-- ============================================================

-- 1. DWS: 按城市统计订单汇总（基于 Iceberg ODS + Hudi ODS）
CREATE TABLE IF NOT EXISTS iceberg.cdc_demo.dws_orders_by_city (
    city STRING,
    total_orders BIGINT,
    total_quantity BIGINT,
    total_amount DECIMAL(18,2),
    avg_price DECIMAL(10,2),
    dt STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 全量覆盖写入（按天分区）
INSERT OVERWRITE iceberg.cdc_demo.dws_orders_by_city
SELECT
    u.city,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity) AS total_quantity,
    SUM(o.price * o.quantity) AS total_amount,
    AVG(o.price) AS avg_price,
    CAST(CURRENT_DATE AS STRING) AS dt
FROM iceberg.cdc_demo.orders o
LEFT JOIN cdc_demo.users u
    ON o.customer_name = u.username
GROUP BY u.city;

-- 2. DWS: 按商品类别统计库存价值（基于 Paimon ODS）
CREATE TABLE IF NOT EXISTS iceberg.cdc_demo.dws_product_stock_value (
    category STRING,
    product_count BIGINT,
    total_stock BIGINT,
    stock_value DECIMAL(18,2),
    dt STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

INSERT OVERWRITE iceberg.cdc_demo.dws_product_stock_value
SELECT
    category,
    COUNT(product_id) AS product_count,
    SUM(stock) AS total_stock,
    SUM(price * stock) AS stock_value,
    CAST(CURRENT_DATE AS STRING) AS dt
FROM paimon.cdc_demo.products
GROUP BY category;
