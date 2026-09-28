# 附录 Spark 离线作业与湖仓一体：离线/实时一套逻辑和表结构

## 一、背景与目标

本项目同时运行 **Flink 实时 CDC** 和 **Spark 离线批处理** 两套链路，核心目标是：

> **离线和实时使用同一套表结构（同一张物理表）和同一套业务逻辑（字段映射、主键 upsert）。**

这样可以保证：

* 实时链路提供秒级数据新鲜度
* 离线链路提供全量回灌、数据修复、批量聚合能力
* 两套链路写入同一张湖仓表，查询端无感知

## 二、整体架构

```
┌─────────────┐     CDC      ┌──────────┐     Flink     ┌──────────────┐
│  MySQL/PG   │ ───────────► │  Kafka   │ ────────────► │  Iceberg/    │
│  (源库)     │              │ (缓冲)   │               │  Hudi/Paimon │
└─────────────┘              └──────────┘               │   (ODS层)    │
       │                         ▲                       └──────┬───────┘
       │                         │                              │
       │    JDBC (全量/批量)      │                              │ 实时 upsert
       └─────────────────────────┘                              │
                Spark 离线回填 (MERGE INTO by PK)                │
                                                                ▼
                                                    ┌──────────────────┐
                                                    │  Spark DWS 聚合  │
                                                    │  (离线批处理)    │
                                                    └──────────────────┘
```

### 链路说明

| **链路** | **引擎** | **数据源**              | **写入方式**         | **延迟** | **用途**    |
| ------ | ------ | -------------------- | ---------------- | ------ | --------- |
| 实时     | Flink  | MySQL/PG CDC → Kafka | upsert by PK     | 秒级     | 持续增量同步    |
| 离线     | Spark  | MySQL/PG JDBC        | MERGE INTO by PK | 分钟级    | 全量回灌、数据修复 |
| 聚合     | Spark  | ODS 湖仓表              | INSERT OVERWRITE | 小时/天级  | DWS 指标计算  |

## 三、如何保证"一套表结构"

### 3.1 同一张物理表

离线和实时写入**同一张湖仓表**，通过 Catalog 共享元数据：

| **表**    | **Catalog**             | **存储**                | **Flink 实时** | **Spark 离线** |
| -------- | ----------------------- | --------------------- | ------------ | ------------ |
| orders   | Iceberg REST            | hdfs\:///user/iceberg | ✅            | ✅ MERGE INTO |
| users    | Hive Metastore (Hudi)   | hdfs\:///user/hudi    | ✅            | ✅ MERGE INTO |
| products | Hive Metastore (Paimon) | hdfs\:///user/paimon  | ✅            | ✅ MERGE INTO |

### 3.2 相同的 Schema 定义

Flink 和 Spark 的建表语句使用**完全相同的字段名、类型、主键**：

```sql
-- Flink (kafka_to_iceberg.sql)
CREATE TABLE iceberg_catalog.cdc_demo.orders (
    order_id BIGINT,
    customer_name STRING,
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version'='2', 'write.upsert.enabled'='true');

-- Spark (spark_offline_orders_iceberg.sql)
CREATE TABLE IF NOT EXISTS iceberg.cdc_demo.orders (
    order_id BIGINT,
    customer_name STRING,
    ...
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.upsert.enabled'='true');
```

> Spark 中使用 `CREATE TABLE IF NOT EXISTS`，表已存在时跳过，不影响 Flink 创建的表。

### 3.3 相同的写入语义（upsert by PK）

两条链路均以**主键 upsert** 方式写入，保证幂等性：

```
Flink:  INSERT INTO ... SELECT * FROM kafka_source  (upsert-kafka connector + Iceberg upsert)
Spark:  MERGE INTO ... USING jdbc_source ON pk WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT
```

## 四、如何保证"一套逻辑"

### 4.1 字段 1:1 映射

ODS 层不做任何转换，源表字段直接映射到目标表：

```
源表字段 → 目标表字段 (同名同类型，无转换)
```

### 4.2 主键一致

* orders: `order_id`
* users: `user_id`
* products: `product_id`

### 4.3 业务逻辑下沉到 DWS 层

复杂的业务逻辑（聚合、关联）统一在 **DWS 层**由 Spark 离线计算，避免实时和离线各写一套：

```sql
-- spark_dws_aggregation.sql
INSERT OVERWRITE iceberg.cdc_demo.dws_orders_by_city
SELECT u.city, COUNT(o.order_id), SUM(o.quantity), ...
FROM iceberg.cdc_demo.orders o
LEFT JOIN cdc_demo.users u ON o.customer_name = u.username
GROUP BY u.city;
```

## 五、离线作业清单

### 5.1 ODS 层回填（与实时共用表）

| **SQL 文件**                          | **源**          | **目标**          | **说明**        |
| ----------------------------------- | -------------- | --------------- | ------------- |
| `spark_offline_orders_iceberg.sql`  | MySQL orders   | Iceberg orders  | 全量回灌 orders   |
| `spark_offline_users_hudi.sql`      | Postgres users | Hudi users      | 全量回灌 users    |
| `spark_offline_products_paimon.sql` | MySQL products | Paimon products | 全量回灌 products |

### 5.2 DWS 层聚合

| **SQL 文件**                  | **源** | **目标**        | **说明**          |
| --------------------------- | ----- | ------------- | --------------- |
| `spark_dws_aggregation.sql` | ODS 表 | Iceberg DWS 表 | 按城市订单汇总、按类别库存价值 |

### 5.3 完整 SQL 源码（可直接复制创建）

> 以下 SQL 文件可通过 heredoc 一键创建到项目 `sql/` 目录。

**创建 `sql/spark_offline_orders_iceberg.sql`**：

```bash
cat > sql/spark_offline_orders_iceberg.sql << 'ICESQL'
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
ICESQL
```

**创建 `sql/spark_offline_users_hudi.sql`**：

```bash
cat > sql/spark_offline_users_hudi.sql << 'HUDISQL'
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
HUDISQL
```

**创建 `sql/spark_offline_products_paimon.sql`**：

```bash
cat > sql/spark_offline_products_paimon.sql << 'PAIMONSQL'
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
PAIMONSQL
```

**创建 `sql/spark_dws_aggregation.sql`**：

```bash
cat > sql/spark_dws_aggregation.sql << 'DWSSQL'
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
DWSSQL
```

> **注意**：heredoc 使用单引号分隔符（`<< 'ICESQL'` 等）防止 shell 变量展开，
> 每个 SQL 文件使用不同的结束标记避免嵌套冲突。

**创建 `scripts/spark-sql.sh`**（Spark SQL 作业提交辅助脚本）：

```bash
cat > scripts/spark-sql.sh << 'SQLSHEOF'
#!/usr/bin/env bash
# ============================================================
# Spark SQL 离线作业提交脚本
#
# 用法:
#   ./scripts/spark-sql.sh                                  # 进入 spark-sql 交互模式
#   ./scripts/spark-sql.sh -f /opt/spark/sql/xxx.sql        # 执行 SQL 文件
#   ./scripts/spark-sql.sh -e "SHOW TABLES IN iceberg.cdc_demo"  # 执行单条 SQL
#
# 说明:
#   - 默认使用 local[*] 模式（适配本环境 Kerberos 认证）
#   - 如需集群模式，设置 SPARK_MASTER=spark://spark-master:7077
#   - 该脚本在 spark-master 容器内执行 spark-sql
# ============================================================
set -e

SPARK_MASTER="${SPARK_MASTER:-local[*]}"

# 判断是否为交互模式
if [ -t 0 ]; then
    TTY="-it"
else
    TTY=""
fi

docker exec $TTY spark-master bash -c "
export SPARK_HOME=/opt/bitnami/spark
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export KRB5CCNAME=/tmp/krb5cc_spark
exec /opt/bitnami/spark/bin/spark-sql --master '$SPARK_MASTER' \"\$@\"
" _ "$@"
SQLSHEOF
chmod +x scripts/spark-sql.sh
```

## 六、运行方式

### 6.1 提交单个离线作业

```bash
# 进入 spark-sql 交互模式
./scripts/spark-sql.sh

# 执行 SQL 文件
./scripts/spark-sql.sh -f /opt/spark/sql/spark_offline_orders_iceberg.sql
```

### 6.2 批量提交所有 ODS 回填作业

```bash
for sql in spark_offline_orders_iceberg spark_offline_users_hudi spark_offline_products_paimon; do
    docker exec spark-master bash -c "
        export KRB5CCNAME=/tmp/krb5cc_spark
        /opt/bitnami/spark/bin/spark-sql --master local[2] -f /opt/spark/sql/${sql}.sql
    "
done
```

### 6.3 调度 DWS 聚合作业

```bash
docker exec spark-master bash -c '
    export KRB5CCNAME=/tmp/krb5cc_spark
    /opt/bitnami/spark/bin/spark-sql --master local[2] -f /opt/spark/sql/spark_dws_aggregation.sql
'
```

## 七、实时与离线协同策略

### 7.1 典型场景

| **场景** | **操作**                           |
| ------ | -------------------------------- |
| 初始化    | 先跑 Spark 离线全量导入，再启动 Flink CDC    |
| 日常运行   | Flink 实时增量，Spark 定期跑 DWS 聚合      |
| 数据修复   | 暂停 Flink → Spark 全量回灌 → 重启 Flink |
| 数据校对   | Spark 全量扫描与 Flink 实时结果对比         |

### 7.2 并发写入注意事项

* Iceberg: 支持并发 upsert，通过乐观锁冲突检测
* Hudi: 支持并发 upsert，建议同一时间只有一个 writer
* Paimon: 支持并发写入，内部协调 commit

> **建议**：离线回填时暂停对应的 Flink 作业，避免双写竞争。

## 八、关键配置说明

> Spark 的部署配置（Dockerfile、spark-defaults.conf、docker-compose volumes 等）统一放在
> [APPENDIX\_DEPLOYMENT.md](./APPENDIX_DEPLOYMENT.md) 第 4.7 节，本文档不再重复。

本节仅说明与"离线/实时共用一套表"相关的 Catalog 架构：

* **Iceberg**：REST Catalog（`iceberg-rest:8181`），Flink 和 Spark 通过相同 REST 端点访问同一张物理表
* **Hudi**：通过 Hive Metastore 访问（`cdc_demo.users`），Flink 和 Spark 共享元数据
* **Paimon**：Hive Metastore（`thrift://hive-metastore:9083`），Flink 和 Spark 共享元数据
* **Kerberos**：票据缓存 `KRB5CCNAME=/tmp/krb5cc_spark`，driver/executor 共享，使用 `local[*]` 模式
* **平台用户**：`lakehouse@LAKEHOUSE.COM`（keytab: `lakehouse.keytab`），用于 HDFS 访问和 Kerberos 认证

> **文件装载一致性原则**：
>
> * **静态文件**（entrypoint、扩展 jar）→ `COPY` 进镜像
> * **所有配置**（spark-defaults.conf、hive-site.xml、krb5.conf、hadoop conf）→ volume 挂载，修改后重启容器即可，无需重建镜像
> * **密钥/开发目录**（keytabs、sql、scripts）→ volume 挂载

## 九、验证结果

| **表**           | **Flink 实时** | **Spark 离线回填**     | **验证方式**     |
| --------------- | ------------ | ------------------ | ------------ |
| Iceberg orders  | ✅ 持续流入       | ✅ MERGE INTO       | Trino COUNT  |
| Hudi users      | ✅ 持续流入       | ✅ MERGE INTO       | Trino COUNT  |
| Paimon products | ⚠️ 写入异常      | ✅ MERGE INTO (25行) | Spark COUNT  |
| DWS 聚合          | -            | ✅ INSERT OVERWRITE | Spark SELECT |

​
