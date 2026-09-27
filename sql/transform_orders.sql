-- 示例 SQL 转换文件
-- 可在 source 表上做过滤、投影、聚合等操作
-- source 为程序注册的临时视图名，代表从源库读取的数据

SELECT
    order_id,
    customer_name,
    product_name,
    quantity,
    price,
    order_status,
    created_at,
    updated_at
FROM source
WHERE order_status IN ('SHIPPED', 'DELIVERED')
  AND quantity >= 1
