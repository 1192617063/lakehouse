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
