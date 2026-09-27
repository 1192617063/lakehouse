# 生产数据操作指南：存量导入、积压处理、通用批量程序与数据入库

> 本文档涵盖湖仓项目在**生产环境**中常见的数据操作场景与解决方案，
> 包括：存量数据初始化导入、Kafka 数据积压处理、通用化离线批量导出程序、
> 数据入库（HFile BulkLoad 等）等一系列生产实操。

---

## 目录

1. [生产数据上线流程总览](#1-生产数据上线流程总览)
2. [存量数据全量导入](#2-存量数据全量导入)
3. [Kafka 数据积压处理](#3-kafka-数据积压处理)
4. [通用化离线批量导出程序](#4-通用化离线批量导出程序)
5. [数据入库（BulkLoad / HFile / Hive 外表）](#5-数据入库bulkload--hfile--hive-外表)
6. [生产异常排查清单](#6-生产异常排查清单)

---

## 1. 生产数据上线流程总览

生产环境新表上线的标准流程：

```
┌──────────────────────────────────────────────────────────┐
│  阶段 1: 存量数据全量导入 (离线 Spark)                     │
│  ─────────────────────────────────────                   │
│  源库(MySQL/PG/Mongo) → Spark → HDFS 文件 → 湖仓表        │
│  使用 MERGE INTO / INSERT OVERWRITE 确保幂等              │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│  阶段 2: 启动实时 CDC 作业 (Flink)                         │
│  ─────────────────────────────────────                   │
│  Kafka CDC → Flink → 同一张湖仓表                         │
│  与离线共用表结构 + 主键 upsert，保证数据一致性            │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│  阶段 3: 数据校验与监控                                    │
│  ─────────────────────────────────────                   │
│  源库行数 vs 湖仓行数、增量延迟监控、数据质量校验           │
└──────────────────────────────────────────────────────────┘
```

### 关键原则

1. **先全量，后增量**：存量数据通过离线批处理导入完成后，再启动实时 CDC
2. **同一套表结构**：离线和实时写入同一张物理表，Schema 完全一致
3. **主键幂等**：通过主键 upsert（MERGE INTO / Flink upsert）保证重复执行不产生脏数据
4. **水位线对齐**：记录全量导入完成时的 binlog offset，Flink 从该位点开始消费，避免数据遗漏或重复

---

## 2. 存量数据全量导入

### 2.1 为什么需要存量导入

生产环境中，业务库通常已有大量历史数据。直接启动 Flink CDC 只能消费**启动后的增量变更**，
历史数据需要通过离线批处理一次性导入。

### 2.2 全量导入策略

| 策略 | 适用场景 | 实现方式 |
|------|---------|---------|
| **全量覆盖** | 小表（< 1000万行） | Spark JDBC 全表读取 → MERGE INTO |
| **分批导入** | 大表（按主键/时间分片） | Spark JDBC partitionColumn 分片读取 |
| **SST/HFile 直导** | 超大数据量（亿级） | 生成 HFile → HBase BulkLoad |

### 2.3 全量 + 增量衔接（水位线对齐）

```
时间轴: ──────────────────────────────────────────────►
         │                                         │
         t0 (全量开始)        t1 (全量结束)        t2 (实时启动)
         │                    │                    │
         ├─ 全量导入 ──────────┤                    │
         │                    │                    │
                              ├─ 记录 binlog offset ─┤
                                                   │
                                                   ├─ Flink 从 t1 位点消费 ──┤
```

**操作步骤**：

```bash
# 1. 记录源库当前 binlog 位点 (MySQL)
mysql -h mysql -uroot -proot123 -e "SHOW MASTER STATUS;" > /tmp/binlog_pos.txt

# 2. 执行全量导入 (Spark)
./scripts/spark-sql.sh -f sql/spark_offline_orders_iceberg.sql

# 3. 验证全量数据行数
docker exec spark-master spark-sql ... -e "SELECT count(*) FROM iceberg.cdc_demo.orders;"

# 4. 启动 Flink CDC (从记录的 binlog 位点开始)
#    在 Flink SQL 中设置: 'scan.startup.mode' = 'specific-offset'
```

### 2.4 全量导入 SQL 示例（已实现）

本项目已实现三张表的全量导入 SQL：

| 文件 | 源 → 目标 | 写入方式 |
|------|----------|---------|
| `sql/spark_offline_orders_iceberg.sql` | MySQL orders → Iceberg | MERGE INTO order_id |
| `sql/spark_offline_users_hudi.sql` | Postgres users → Hudi | MERGE INTO user_id |
| `sql/spark_offline_products_paimon.sql` | MySQL products → Paimon | MERGE INTO product_id |

执行方式：

```bash
./scripts/spark-sql.sh -f sql/spark_offline_orders_iceberg.sql
./scripts/spark-sql.sh -f sql/spark_offline_users_hudi.sql
./scripts/spark-sql.sh -f sql/spark_offline_products_paimon.sql
```

---

## 3. Kafka 数据积压处理

### 3.1 积压产生的原因

| 原因 | 说明 | 处理方式 |
|------|------|---------|
| **全量导入期间** | 源库 binlog 持续产生，Flink 未启动 | 全量完成后启动 Flink，自然消化 |
| **Flink 消费能力不足** | 并行度不够或反压 | 增加并行度、优化算子 |
| **下游写入瓶颈** | 湖仓表写入慢（小文件过多等） | 调大 checkpoint、合并小文件 |
| **作业异常重启** | 重启期间消息堆积 | 恢复后自动追平 |

### 3.2 积压监控

```bash
# 查看 Kafka 消费组 lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group flink-cdc-orders

# 关键指标: LAG (积压消息数)
# 正常: LAG 持续下降或为 0
# 异常: LAG 持续上升 → 需要扩容或排查
```

### 3.3 积压处理方案

#### 方案一：增加 Flink 并行度（推荐）

```sql
-- Flink SQL: 增加源表并行度
SET 'parallelism.default' = '4';

-- 或针对特定算子
SELECT /*+ PARALLEL(4) */ * FROM kafka_orders;
```

#### 方案二：临时关闭实时，离线追赶

```bash
# 1. 停止 Flink 作业 (保存 checkpoint)
# 2. 用 Spark 批量消费积压的 Kafka 数据写入湖仓表
# 3. 重启 Flink 从 checkpoint 恢复

# Spark 读取 Kafka 批量写入 (Structured Streaming batch)
spark.read.format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "cdc_demo.orders") \
  .option("startingOffsets", "earliest") \
  .option("endingOffsets", "latest") \
  .load()
```

#### 方案三：跳过积压数据（仅在可丢弃场景）

```bash
# 修改 Flink 消费组 offset 到 latest（丢弃积压）
# 谨慎使用！仅适用于可丢失的日志类数据
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group flink-cdc-orders \
  --reset-offsets \
  --to-latest \
  --topic cdc_demo.orders \
  --execute
```

### 3.4 湖仓表小文件合并

积压消化后，湖仓表可能产生大量小文件，需要定期合并：

```sql
-- Iceberg 表合并小文件
CALL iceberg.system.rewrite_data_files(
  table => 'cdc_demo.orders',
  options => map('target-file-size-bytes', '536870912')
);

-- Hudi 表合并 (compaction)
-- Flink Hudi 会自动 compaction，也可手动触发
-- Paimon 表合并
CALL paimon.system.compact('cdc_demo.products');
```

---

## 4. 通用化离线批量导出程序

### 4.1 程序概述

本项目提供了一个通用化的 Spark 离线批量导出程序，支持：

- **多数据源**：MySQL、PostgreSQL、MongoDB
- **多输出格式**：Parquet、ORC、CSV、TXT（自定义分隔符）、JSON
- **SQL 转换**：支持传入 SQL 文件对源数据做过滤/投影/聚合
- **可配置**：数据库连接、输出路径、格式均通过参数配置

### 4.2 文件清单

| 文件 | 说明 |
|------|------|
| `scripts/spark_batch_export.py` | PySpark 主程序 |
| `scripts/run_batch_export.sh` | Shell 封装脚本 |
| `sql/transform_orders.sql` | SQL 转换示例 |

#### 4.2.1 完整源码（可直接复制创建）

> 以下两个文件可通过 heredoc 一键创建到项目 `scripts/` 目录。

**创建 `scripts/spark_batch_export.py`**：

```bash
cat > scripts/spark_batch_export.py << 'PYEOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用化 Spark 离线批量数据导出程序

功能:
  - 从多种数据源读取数据 (MySQL / PostgreSQL / MongoDB)
  - 支持传入 SQL 文件进行数据转换
  - 输出多种文件格式 (parquet / orc / csv / 自定义分隔符 txt)
  - 可作为数据入湖/入仓前的 ETL 工具

用法:
  spark-submit spark_batch_export.py \
    --source-type mysql \
    --source-url "jdbc:mysql://mysql:3306/cdc_demo" \
    --source-user root \
    --source-password root123 \
    --source-table orders \
    --sql-file /opt/spark/sql/transform.sql \
    --output-path hdfs://namenode:9000/tmp/export/orders \
    --output-format parquet

  spark-submit spark_batch_export.py \
    --source-type mongodb \
    --source-url "mongodb://root:root123@mongodb:27017" \
    --source-database cdc_demo \
    --source-collection orders \
    --output-path hdfs://namenode:9000/tmp/export/mongo_orders \
    --output-format orc

  spark-submit spark_batch_export.py \
    --source-type mysql \
    --source-url "jdbc:mysql://mysql:3306/cdc_demo" \
    --source-user root \
    --source-password root123 \
    --source-table orders \
    --output-path hdfs://namenode:9000/tmp/export/orders_txt \
    --output-format txt \
    --delimiter "|"
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session(app_name):
    """创建 SparkSession，配置 Kerberos 和 Hadoop"""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,"
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .getOrCreate()
    )
    return spark


def read_from_jdbc(spark, args):
    """从 JDBC 数据源读取 (MySQL / PostgreSQL)"""
    jdbc_df = (
        spark.read.format("jdbc")
        .option("url", args.source_url)
        .option("dbtable", args.source_table)
        .option("user", args.source_user)
        .option("password", args.source_password)
        .option("driver", args.jdbc_driver)
        .option("fetchsize", "10000")
        .load()
    )
    return jdbc_df


def read_from_mongodb(spark, args):
    """从 MongoDB 读取数据 (mongo-spark-connector 10.x)"""
    mongo_df = (
        spark.read.format("mongodb")
        .option("spark.mongodb.read.connection.uri", args.source_url)
        .option("spark.mongodb.read.database", args.source_database)
        .option("spark.mongodb.read.collection", args.source_collection)
        .load()
    )
    return mongo_df


def apply_sql_transform(spark, df, sql_file):
    """应用 SQL 转换逻辑（从 SQL 文件读取）"""
    if not sql_file:
        return df

    with open(sql_file, "r") as f:
        sql = f.read().strip()

    # 注册临时视图，SQL 中可引用 source 表
    df.createOrReplaceTempView("source")

    # SQL 文件中可以使用 source 作为表名
    # 例如: SELECT order_id, customer_name FROM source WHERE quantity > 10
    result_df = spark.sql(sql)
    return result_df


def write_output(df, args):
    """将数据写入指定格式"""
    writer = df.write.mode(args.write_mode)

    if args.output_format == "parquet":
        writer.parquet(args.output_path)
    elif args.output_format == "orc":
        writer.orc(args.output_path)
    elif args.output_format == "csv":
        writer.option("header", "true").csv(args.output_path)
    elif args.output_format == "txt":
        # 自定义分隔符的文本文件
        delimiter = args.delimiter if args.delimiter else ","
        writer.option("header", "false").option("delimiter", delimiter).csv(args.output_path)
    elif args.output_format == "json":
        writer.json(args.output_path)
    else:
        raise ValueError(f"不支持的输出格式: {args.output_format}")

    print(f"[INFO] 数据已写入: {args.output_path} (格式: {args.output_format})")


def parse_args():
    parser = argparse.ArgumentParser(description="通用化 Spark 离线批量数据导出程序")

    # 数据源配置
    parser.add_argument("--source-type", required=True,
                        choices=["mysql", "postgresql", "mongodb"],
                        help="数据源类型")
    parser.add_argument("--source-url", required=True,
                        help="数据源连接 URL (JDBC URL 或 MongoDB URI)")
    parser.add_argument("--source-user", help="数据库用户名")
    parser.add_argument("--source-password", help="数据库密码")
    parser.add_argument("--source-table", help="数据库表名 (JDBC)")
    parser.add_argument("--source-database", help="MongoDB 数据库名")
    parser.add_argument("--source-collection", help="MongoDB 集合名")

    # JDBC 驱动 (自动推断)
    parser.add_argument("--jdbc-driver", default=None,
                        help="JDBC 驱动类名 (不指定则根据 source-type 自动推断)")

    # SQL 转换
    parser.add_argument("--sql-file", default=None,
                        help="SQL 转换文件路径 (可选, 文件中可用 source 作为表名)")

    # 输出配置
    parser.add_argument("--output-path", required=True,
                        help="输出路径 (HDFS / 本地)")
    parser.add_argument("--output-format", default="parquet",
                        choices=["parquet", "orc", "csv", "txt", "json"],
                        help="输出文件格式")
    parser.add_argument("--delimiter", default=",",
                        help="TXT/CSV 分隔符 (默认逗号)")
    parser.add_argument("--write-mode", default="overwrite",
                        choices=["overwrite", "append", "errorifexists", "ignore"],
                        help="写入模式")

    # 分区
    parser.add_argument("--partition-by", default=None,
                        help="分区字段 (逗号分隔)")

    return parser.parse_args()


def get_jdbc_driver(source_type):
    """根据数据源类型返回 JDBC 驱动类名"""
    drivers = {
        "mysql": "com.mysql.cj.jdbc.Driver",
        "postgresql": "org.postgresql.Driver",
    }
    return drivers.get(source_type)


def main():
    args = parse_args()

    # 自动推断 JDBC 驱动
    if args.source_type in ("mysql", "postgresql") and not args.jdbc_driver:
        args.jdbc_driver = get_jdbc_driver(args.source_type)

    spark = create_spark_session(f"batch-export-{args.source_type}")

    try:
        # 读取数据
        print(f"[INFO] 从 {args.source_type} 读取数据...")
        if args.source_type in ("mysql", "postgresql"):
            if not args.source_table:
                raise ValueError("JDBC 数据源需要指定 --source-table")
            df = read_from_jdbc(spark, args)
        elif args.source_type == "mongodb":
            if not args.source_database or not args.source_collection:
                raise ValueError("MongoDB 数据源需要指定 --source-database 和 --source-collection")
            df = read_from_mongodb(spark, args)

        print(f"[INFO] 读取行数: {df.count()}, 列数: {len(df.columns)}")

        # 应用 SQL 转换
        if args.sql_file:
            print(f"[INFO] 应用 SQL 转换: {args.sql_file}")
            df = apply_sql_transform(spark, df, args.sql_file)

        # 分区写入
        if args.partition_by:
            df = df.repartition(*[col(c.strip()) for c in args.partition_by.split(",")])

        # 写出
        write_output(df, args)
        print("[INFO] 作业完成")

    except Exception as e:
        print(f"[ERROR] 作业失败: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
PYEOF
chmod +x scripts/spark_batch_export.py
```

**创建 `scripts/run_batch_export.sh`**：

```bash
cat > scripts/run_batch_export.sh << 'SHEOF'
#!/usr/bin/env bash
# ============================================================
# 通用化 Spark 离线批量导出脚本
#
# 功能: 从数据库读取数据，输出为指定格式文件
# 支持数据源: MySQL / PostgreSQL / MongoDB
# 支持输出格式: parquet / orc / csv / txt(自定义分隔符) / json
#
# 用法:
#   # MySQL -> Parquet
#   ./scripts/run_batch_export.sh mysql orders parquet
#
#   # PostgreSQL -> ORC
#   ./scripts/run_batch_export.sh postgresql users orc
#
#   # 自定义: MySQL -> TXT (| 分隔)
#   ./scripts/run_batch_export.sh mysql orders txt "|"
#
#   # 使用 SQL 转换文件
#   SQL_FILE=/opt/spark/sql/transform.sql ./scripts/run_batch_export.sh mysql orders parquet
# ============================================================
set -e

SOURCE_TYPE="${1:-mysql}"
SOURCE_TABLE="${2:-orders}"
OUTPUT_FORMAT="${3:-parquet}"
DELIMITER="${4:-,}"

# 数据源连接配置 (可通过环境变量覆盖)
declare -A SOURCE_CONFIGS
SOURCE_CONFIGS[mysql_url]="jdbc:mysql://mysql:3306/cdc_demo?useSSL=false&serverTimezone=UTC"
SOURCE_CONFIGS[mysql_user]="root"
SOURCE_CONFIGS[mysql_password]="root123"
SOURCE_CONFIGS[postgresql_url]="jdbc:postgresql://postgres:5432/cdc_demo"
SOURCE_CONFIGS[postgresql_user]="postgres"
SOURCE_CONFIGS[postgresql_password]="postgres123"
SOURCE_CONFIGS[mongodb_uri]="mongodb://root:root123@mongodb:27017/?authSource=admin"
SOURCE_CONFIGS[mongodb_database]="cdc_demo"

# 输出根路径
OUTPUT_ROOT="${OUTPUT_ROOT:-hdfs://namenode:9000/tmp/export}"
OUTPUT_PATH="${OUTPUT_ROOT}/${SOURCE_TABLE}_${OUTPUT_FORMAT}"

# SQL 转换文件 (可选)
SQL_FILE="${SQL_FILE:-}"

# 分区字段 (可选)
PARTITION_BY="${PARTITION_BY:-}"

echo "============================================"
echo "  Spark 离线批量导出"
echo "============================================"
echo "数据源:   ${SOURCE_TYPE}"
echo "表名:     ${SOURCE_TABLE}"
echo "输出格式: ${OUTPUT_FORMAT}"
echo "输出路径: ${OUTPUT_PATH}"
[ -n "$SQL_FILE" ] && echo "SQL文件:  ${SQL_FILE}"
[ -n "$PARTITION_BY" ] && echo "分区字段: ${PARTITION_BY}"
echo "============================================"

# 根据数据源类型选择 jars
JARS="/opt/bitnami/spark/extra-jars/mysql-connector-j-8.4.0.jar,/opt/bitnami/spark/extra-jars/postgresql-42.7.3.jar"
if [ "${SOURCE_TYPE}" = "mongodb" ]; then
  JARS="${JARS},/opt/bitnami/spark/extra-jars/mongo-spark-connector_2.12-10.4.0.jar,/opt/bitnami/spark/extra-jars/bson-5.2.0.jar,/opt/bitnami/spark/extra-jars/mongodb-driver-core-5.2.0.jar,/opt/bitnami/spark/extra-jars/mongodb-driver-sync-5.2.0.jar"
fi

# 构造 spark-submit 命令
SPARK_CMD=(
  spark-submit
  --master "${SPARK_MASTER:-local[*]}"
  --conf "spark.kerberos.keytab=/etc/security/keytabs/spark.service.keytab"
  --conf "spark.kerberos.principal=spark/spark-master.lakehouse.com@LAKEHOUSE.COM"
  --jars "${JARS}"
  /opt/spark/scripts/spark_batch_export.py
  --source-type "${SOURCE_TYPE}"
  --output-path "${OUTPUT_PATH}"
  --output-format "${OUTPUT_FORMAT}"
)

# 数据源特定参数
case "${SOURCE_TYPE}" in
  mysql|postgresql)
    SPARK_CMD+=(
      --source-url "${SOURCE_CONFIGS[${SOURCE_TYPE}_url]}"
      --source-user "${SOURCE_CONFIGS[${SOURCE_TYPE}_user]}"
      --source-password "${SOURCE_CONFIGS[${SOURCE_TYPE}_password]}"
      --source-table "${SOURCE_TABLE}"
    )
    ;;
  mongodb)
    SPARK_CMD+=(
      --source-url "${SOURCE_CONFIGS[mongodb_uri]}"
      --source-database "${SOURCE_CONFIGS[mongodb_database]}"
      --source-collection "${SOURCE_TABLE}"
    )
    ;;
esac

# 可选参数
[ -n "$SQL_FILE" ] && SPARK_CMD+=(--sql-file "${SQL_FILE}")
[ -n "$PARTITION_BY" ] && SPARK_CMD+=(--partition-by "${PARTITION_BY}")
[ "${OUTPUT_FORMAT}" = "txt" ] && SPARK_CMD+=(--delimiter "${DELIMITER}")

# 执行 - 将命令写入临时脚本文件以保证引号正确
echo ""
echo "[执行] spark-submit ${SPARK_CMD[*]:1}"
echo ""

# 构造带正确引号的命令字符串
CMD_STR=""
for arg in "${SPARK_CMD[@]}"; do
  CMD_STR+="'${arg//\'/\'\\\'\'}' "
done

docker exec spark-master bash -c "
export SPARK_HOME=/opt/bitnami/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export KRB5CCNAME=/tmp/krb5cc_spark
${CMD_STR}
"

echo ""
echo "[完成] 数据已导出到: ${OUTPUT_PATH}"
SHEOF
chmod +x scripts/run_batch_export.sh
```

> **注意**：heredoc 使用 `<< 'PYEOF'` / `<< 'SHEOF'`（加单引号）防止 shell 变量展开，
> 两个文件使用不同的结束标记避免嵌套冲突。

### 4.3 使用方式

#### 方式一：Shell 脚本（推荐）

```bash
# MySQL orders → Parquet
./scripts/run_batch_export.sh mysql orders parquet

# PostgreSQL users → ORC
./scripts/run_batch_export.sh postgresql users orc

# MySQL orders → TXT (| 分隔符)
./scripts/run_batch_export.sh mysql orders txt "|"

# 使用 SQL 转换文件过滤
SQL_FILE=/opt/spark/sql/transform_orders.sql \
  ./scripts/run_batch_export.sh mysql orders parquet

# 自定义输出根路径
OUTPUT_ROOT=hdfs://namenode:9000/data/export \
  ./scripts/run_batch_export.sh mysql orders parquet

# 指定分区字段
PARTITION_BY=order_status \
  ./scripts/run_batch_export.sh mysql orders parquet
```

#### 方式二：直接 spark-submit

```bash
docker exec spark-master bash -c '
export SPARK_HOME=/opt/bitnami/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export KRB5CCNAME=/tmp/krb5cc_spark

/opt/bitnami/spark/bin/spark-submit --master local[*] \
  --jars /opt/bitnami/spark/extra-jars/mysql-connector-j-8.4.0.jar \
  /opt/spark/scripts/spark_batch_export.py \
  --source-type mysql \
  --source-url "jdbc:mysql://mysql:3306/cdc_demo?useSSL=false&serverTimezone=UTC" \
  --source-user root \
  --source-password root123 \
  --source-table orders \
  --output-path "hdfs://namenode:9000/tmp/export/orders" \
  --output-format parquet \
  --write-mode overwrite
'
```

### 4.4 参数说明

| 参数 | 必填 | 说明 |
|------|------|------|
| `--source-type` | 是 | 数据源类型：mysql / postgresql / mongodb |
| `--source-url` | 是 | JDBC URL 或 MongoDB URI |
| `--source-user` | JDBC必填 | 数据库用户名 |
| `--source-password` | JDBC必填 | 数据库密码 |
| `--source-table` | JDBC必填 | 数据库表名 |
| `--source-database` | MongoDB必填 | MongoDB 数据库名 |
| `--source-collection` | MongoDB必填 | MongoDB 集合名 |
| `--sql-file` | 否 | SQL 转换文件路径 |
| `--output-path` | 是 | 输出路径（HDFS / 本地） |
| `--output-format` | 否 | parquet / orc / csv / txt / json（默认 parquet） |
| `--delimiter` | 否 | TXT/CSV 分隔符（默认逗号） |
| `--write-mode` | 否 | overwrite / append / errorifexists / ignore |
| `--partition-by` | 否 | 分区字段（逗号分隔） |

### 4.5 SQL 转换文件格式

SQL 文件中使用 `source` 作为源数据表名。可通过以下命令创建示例转换文件：

```bash
cat > sql/transform_orders.sql << 'TRANSFERSQL'
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
TRANSFERSQL
```

> **说明**：`source` 是程序自动注册的临时视图名，SQL 中直接引用 `source` 即可对源数据做过滤/投影/聚合。

### 4.6 MongoDB 支持

MongoDB 需要额外的连接器 jar（`mongo-spark-connector` + Java 驱动）。
**jar 下载命令统一放在 [DEPLOYMENT_NOTES.md](./DEPLOYMENT_NOTES.md)** 的 Spark jar 下载小节，
下载后需重建 Spark 镜像（jar 已 COPY 进镜像）。

> **注意**：`mongo-spark-connector 10.x` 的配置项使用 `spark.mongodb.read.connection.uri` / `spark.mongodb.read.database` / `spark.mongodb.read.collection`，
> 连接 URI 需带 `?authSource=admin`（认证库）。

使用示例：

```bash
# MongoDB orders 集合 → Parquet
./scripts/run_batch_export.sh mongodb orders parquet

# MongoDB orders 集合 → JSON
./scripts/run_batch_export.sh mongodb orders json
```

`source-collection` 参数对应 MongoDB 集合名，`source-database` 对应数据库名。

### 4.7 验证结果

以下为在本地湖仓环境（`local[*]` 模式）实际执行的测试用例与结果。

#### 测试用例 1：MySQL orders → Parquet

**执行命令**：

```bash
docker exec spark-master bash -c '
export SPARK_HOME=/opt/bitnami/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export KRB5CCNAME=/tmp/krb5cc_spark
/opt/bitnami/spark/bin/spark-submit --master local[2] \
  --jars /opt/bitnami/spark/extra-jars/mysql-connector-j-8.4.0.jar \
  /opt/spark/scripts/spark_batch_export.py \
  --source-type mysql \
  --source-url "jdbc:mysql://mysql:3306/cdc_demo?useSSL=false&serverTimezone=UTC" \
  --source-user root \
  --source-password root123 \
  --source-table orders \
  --output-path "hdfs://namenode:9000/tmp/export/orders_parquet" \
  --output-format parquet \
  --write-mode overwrite
'
```

**实际输出**：

```
[INFO] 从 mysql 读取数据...
[INFO] 读取行数: 2304, 列数: 8
[INFO] 数据已写入: hdfs://namenode:9000/tmp/export/orders_parquet (格式: parquet)
[INFO] 作业完成
```

**结果**：✅ 成功，2304 行 8 列数据写入 HDFS Parquet 文件。

---

#### 测试用例 2：MySQL orders → TXT（自定义 `|` 分隔符）

**执行命令**：

```bash
docker exec spark-master bash -c '
... /opt/spark/scripts/spark_batch_export.py \
  --source-type mysql \
  --source-table orders \
  --output-path "hdfs://namenode:9000/tmp/export/orders_txt" \
  --output-format txt \
  --delimiter "|" \
  --write-mode overwrite
'
```

**实际输出**：

```
[INFO] 从 mysql 读取数据...
[INFO] 读取行数: 2309, 列数: 8
[INFO] 数据已写入: hdfs://namenode:9000/tmp/export/orders_txt (格式: txt)
[INFO] 作业完成
```

**HDFS 文件内容验证**（`hdfs dfs -cat` 抽样前 3 行）：

```
3|Julia|Product_273|2|6.45|SHIPPED|2026-09-25T22:18:55.000Z|2026-09-26T00:05:16.000Z
6|George|Product_859|1|217.75|SHIPPED|2026-09-25T22:19:01.000Z|2026-09-25T23:21:48.000Z
8|Diana|Product_258|2|313.65|CREATED|2026-09-25T22:19:11.000Z|2026-09-25T22:19:11.000Z
```

**结果**：✅ 成功，`|` 分隔符生效，每行 8 个字段，无表头，符合 TXT 格式预期。

---

#### 测试用例 3：MySQL orders + SQL 转换 → ORC

**SQL 转换文件** (`sql/transform_orders.sql`)：

```sql
SELECT
    order_id, customer_name, product_name, quantity,
    price, order_status, created_at, updated_at
FROM source
WHERE order_status IN ('SHIPPED', 'DELIVERED')
  AND quantity >= 1
```

**执行命令**：

```bash
docker exec spark-master bash -c '
... /opt/spark/scripts/spark_batch_export.py \
  --source-type mysql \
  --source-table orders \
  --sql-file /opt/spark/sql/transform_orders.sql \
  --output-path "hdfs://namenode:9000/tmp/export/orders_orc" \
  --output-format orc \
  --write-mode overwrite
'
```

**实际输出**：

```
[INFO] 从 mysql 读取数据...
[INFO] 读取行数: 2316, 列数: 8
[INFO] 应用 SQL 转换: /opt/spark/sql/transform_orders.sql
[INFO] 数据已写入: hdfs://namenode:9000/tmp/export/orders_orc (格式: orc)
[INFO] 作业完成
```

**结果**：✅ 成功，SQL 过滤条件生效（仅保留 `SHIPPED`/`DELIVERED` 状态），输出 ORC 格式。

---

#### 测试用例 4：MySQL products → Parquet（通过 Shell 封装脚本）

**执行命令**：

```bash
./scripts/run_batch_export.sh mysql products parquet
```

**关键输出**：

```
============================================
  Spark 离线批量导出
============================================
数据源:   mysql
表名:     products
输出格式: parquet
输出路径: hdfs://namenode:9000/tmp/export/products_parquet
============================================

[INFO] 从 mysql 读取数据...
[INFO] 读取行数: 5, 列数: 6
[INFO] 数据已写入: hdfs://namenode:9000/tmp/export/products_parquet (格式: parquet)
[INFO] 作业完成
[完成] 数据已导出到: hdfs://namenode:9000/tmp/export/products_parquet
```

**结果**：✅ 成功，Shell 封装脚本自动处理 Kerberos 环境变量和引号转义，5 行 6 列数据写入 Parquet。

---

#### 测试用例 5：PostgreSQL users → Parquet

**执行命令**（通过 Shell 封装脚本）：

```bash
./scripts/run_batch_export.sh postgresql users parquet
```

**实际输出**：

```
[INFO] 从 postgresql 读取数据...
[INFO] 读取行数: 2165, 列数: 8
[INFO] 数据已写入: hdfs://namenode:9000/tmp/export/users_parquet (格式: parquet)
[INFO] 作业完成
[完成] 数据已导出到: hdfs://namenode:9000/tmp/export/users_parquet
```

**结果**：✅ 成功，PostgreSQL `users` 表 2165 行 8 列数据写入 Parquet，Shell 脚本自动加载 `postgresql-42.7.3.jar`。

---

#### 测试用例 6：MongoDB orders → Parquet

**前置准备**：向 MongoDB `cdc_demo.orders` 集合插入 8 条测试文档。

**执行命令**（通过 Shell 封装脚本）：

```bash
./scripts/run_batch_export.sh mongodb orders parquet
```

**实际输出**：

```
[INFO] 从 mongodb 读取数据...
[INFO] 读取行数: 8, 列数: 8
[INFO] 数据已写入: hdfs://namenode:9000/tmp/export/orders_parquet (格式: parquet)
[INFO] 作业完成
[完成] 数据已导出到: hdfs://namenode:9000/tmp/export/orders_parquet
```

**结果**：✅ 成功，MongoDB `orders` 集合 8 条文档读取并写入 Parquet，自动加载 4 个 MongoDB 相关 jar。

---

#### 测试用例 7：MongoDB orders → JSON

**执行命令**：

```bash
./scripts/run_batch_export.sh mongodb orders json
```

**HDFS 文件内容验证**（`hdfs dfs -cat` 抽样前 2 行）：

```json
{"_id":"6ab71285e9bbcc9b87fe6911","created_at":"2026-09-20T10:00:00.000Z","customer_name":"Alice","order_id":1,"order_status":"SHIPPED","price":99.5,"product_name":"Product_A","quantity":2}
{"_id":"6ab71285e9bbcc9b87fe6912","created_at":"2026-09-21T11:00:00.000Z","customer_name":"Bob","order_id":2,"order_status":"CREATED","price":199.0,"product_name":"Product_B","quantity":1}
```

**结果**：✅ 成功，MongoDB 文档以 JSON Lines 格式输出，`_id` 字段保留，字段名与 BSON key 一致。

---

#### 测试汇总

| # | 测试场景 | 数据源 | 输出格式 | 行数 | 结果 |
|---|---------|--------|---------|------|------|
| 1 | 全量导出 | MySQL orders | Parquet | 2304 | ✅ |
| 2 | 自定义分隔符 | MySQL orders | TXT (`\|`) | 2309 | ✅ |
| 3 | SQL 转换过滤 | MySQL orders | ORC | 过滤后 | ✅ |
| 4 | Shell 脚本封装 | MySQL products | Parquet | 5 | ✅ |
| 5 | Shell 脚本封装 | PostgreSQL users | Parquet | 2165 | ✅ |
| 6 | Shell 脚本封装 | MongoDB orders | Parquet | 8 | ✅ |
| 7 | JSON 输出 | MongoDB orders | JSON | 8 | ✅ |

**环境信息**：Spark `local[*]` 模式，Kerberos 认证，HDFS 存储。

**依赖 jar 清单**：

| 数据源 | 所需 jar |
|--------|---------|
| MySQL | `mysql-connector-j-8.4.0.jar` |
| PostgreSQL | `postgresql-42.7.3.jar` |
| MongoDB | `mongo-spark-connector_2.12-10.4.0.jar` + `bson-5.2.0.jar` + `mongodb-driver-core-5.2.0.jar` + `mongodb-driver-sync-5.2.0.jar` |

**已知限制**：
- `local[*]` 模式验证通过，standalone 集群模式存在 executor Kerberos 认证问题（详见 SPARK_OFFLINE_LAKEHOUSE.md）

---

## 5. 数据入库（BulkLoad / HFile / Hive 外表）

### 5.1 入库方式对比

| 方式 | 适用场景 | 性能 | 复杂度 |
|------|---------|------|--------|
| **Spark MERGE INTO** | 中小表，需要更新语义 | 中 | 低 |
| **INSERT OVERWRITE** | 全量覆盖，分区表 | 高 | 低 |
| **HFile BulkLoad** | HBase 超大数据量 | 极高 | 高 |
| **Hive 外表** | 已有 HDFS 数据文件 | 极高 | 低 |

### 5.2 导出文件直接入湖（Hive 外表方式）

对于已经导出到 HDFS 的 Parquet/ORC 文件，可以直接建外表关联：

```sql
-- Spark SQL: 创建 Hive 外表指向导出的 Parquet 文件
CREATE EXTERNAL TABLE IF NOT EXISTS cdc_demo.orders_export (
  order_id BIGINT,
  customer_name STRING,
  product_name STRING,
  quantity INT,
  price DECIMAL(10,2),
  order_status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/tmp/export/orders_parquet';

-- 查询验证
SELECT count(*) FROM cdc_demo.orders_export;
```

### 5.3 导出数据导入 Iceberg 表

```sql
-- 将导出的 Parquet 数据导入 Iceberg 表
INSERT INTO iceberg.cdc_demo.orders
SELECT * FROM cdc_demo.orders_export;
```

### 5.4 HFile BulkLoad（HBase 场景）

对于写入 HBase 的超大数据量场景，直接 Put 性能差，应使用 BulkLoad：

```python
# spark_hfile_bulkload.py (概念示例)
# 1. Spark 读取源数据
# 2. 按 HBase rowkey 排序
# 3. 生成 HFile 到 HDFS 临时目录
# 4. 通过 HBase LoadIncrementalHFiles 将 HFile 移动到 HBase region

df = spark.read.jdbc(url, table, props)
# 转换为 (rowkey, family:qualifier, value) 三元组
hfile_rdd = df.rdd.map(lambda row: (row.key, row.cf, row.col, row.value))
# 排序并写入 HFile
hfile_rdd.sortByKey().saveAsNewAPIHadoopFile(
    path=hfile_output_path,
    keyClass="org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    valueClass="org.apache.hadoop.hbase.KeyValue",
    outputFormatClass="org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2"
)
# 执行 LoadIncrementalHFiles
# hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles <hfile_path> <table_name>
```

### 5.5 入库后的校验

```bash
# 1. 行数对比
echo "源库行数:"
mysql -h mysql -uroot -proot123 -e "SELECT count(*) FROM cdc_demo.orders;"

echo "湖仓行数:"
./scripts/spark-sql.sh -e "SELECT count(*) FROM iceberg.cdc_demo.orders;"

# 2. 抽样数据对比
# 3. 校验 NULL 值、主键重复、字段类型
```

---

## 6. 生产异常排查清单

### 6.1 全量导入问题

| 现象 | 可能原因 | 排查命令 |
|------|---------|---------|
| Spark JDBC 读取慢 | 无分区，单线程读取 | 加 `partitionColumn`/`numPartitions` |
| OOM | 数据量过大 | 增加 `fetchsize`，分批读取 |
| Kerberos 认证失败 | 票据过期 | `kinit` 或检查 `KRB5CCNAME` |
| 连接超时 | 网络/防火墙 | 测试 `telnet mysql 3306` |

### 6.2 Kafka 积压问题

| 现象 | 排查方向 |
|------|---------|
| LAG 持续增长 | 检查 Flink 作业是否 running、checkpoint 是否正常 |
| 反压（Backpressure） | 查看 Flink WebUI 反压指标，定位慢算子 |
| Checkpoint 超时 | 增大 checkpoint 间隔或超时时间 |
| 数据丢失 | 确认消费组 offset、检查 exactly-once 配置 |

### 6.3 数据一致性问题

| 现象 | 排查方向 |
|------|---------|
| 湖仓行数 < 源库 | 检查是否有数据在全量导入后、实时启动前产生 |
| 湖仓行数 > 源库 | 可能重复写入，检查主键 upsert 是否生效 |
| 字段值为 NULL | 检查字段映射、类型转换 |
| 主键重复 | MERGE INTO 的 ON 条件是否正确 |

### 6.4 性能优化清单

- [ ] Spark JDBC 使用 `partitionColumn` 并行读取
- [ ] 设置合理的 `fetchsize`（建议 10000）
- [ ] 输出文件合并小文件（target-file-size 512MB）
- [ ] Iceberg/Paimon 表开启 compaction
- [ ] Flink checkpoint 间隔合理（60s ~ 5min）
- [ ] 监控 HDFS NameNode 压力

---

## 附录：相关文档

- [SPARK_OFFLINE_LAKEHOUSE.md](./SPARK_OFFLINE_LAKEHOUSE.md) — Spark 离线作业与湖仓一体
- [DEPLOYMENT_NOTES.md](./DEPLOYMENT_NOTES.md) — 部署笔记
