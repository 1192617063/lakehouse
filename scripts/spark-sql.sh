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
