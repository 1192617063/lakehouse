#!/usr/bin/env bash
# 把 SQL 提交给 Flink JobManager（JM 已经有 Kerberos UGI subject）
# 绕过 SQL Client embedded 模式的 JVM Subject 问题

SQL_FILE="$1"
[ -z "$SQL_FILE" ] && { echo "Usage: $0 <sql-file>"; exit 1; }

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
JM="${FLINK_JM:-flink-jobmanager:8081}"

echo "=== 提交 SQL 到 Flink JobManager ($JM) ==="
cat "$SQL_FILE"
echo "=== Running... ==="

# Flink 1.19 自带 sql-gateway.sh + sql-client.sh 可以远程 connect
# 但更简单：用 flink run-application 提交 SQL
cd "$FLINK_HOME"
./bin/flink run-application \
  -t kubernetes-session 2>/dev/null || \
./bin/flink run \
  -m "$JM" \
  "$FLINK_HOME/lib/flink-sql-client.jar" \
  -f "$SQL_FILE"
