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
  --conf "spark.kerberos.principal=spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM"
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
