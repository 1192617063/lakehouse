-- ============================================================
-- Spark 离线作业: PostgreSQL users -> Hudi (ODS 层回填)
--
-- 一套表结构: 与 Flink 写入同一张物理表（Hive Metastore 中 cdc_demo.users）
-- 一套逻辑:   均以 user_id 为主键做 upsert，precombine 字段为 updated_at
--
-- 注意: 通过 Hive Metastore 访问 Hudi 表（与 Flink 共享元数据）
-- ============================================================

-- 1. 通过 JDBC 读取 Postgres 源表
DROP TABLE IF EXISTS pg_users_src;
CREATE TABLE pg_users_src USING jdbc
OPTIONS (
  url 'jdbc:postgresql://postgres:5432/cdc_demo',
  dbtable 'public.users',
  user 'postgres',
  password 'postgres123',
  driver 'org.postgresql.Driver',
  fetchsize '1000'
);

-- 2. MERGE INTO: 按主键 upsert 到 Hudi 表（与 Flink CDC 语义一致）
--    Hudi 表已由 Flink 创建并注册在 Hive Metastore (cdc_demo.users)
MERGE INTO cdc_demo.users t
USING pg_users_src s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
