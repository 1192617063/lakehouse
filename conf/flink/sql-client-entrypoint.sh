#!/usr/bin/env bash
# Flink SQL Client entrypoint: Kerberos wrapper
# 1. kinit 创建 ticket cache
# 2. 用 KerberosSqlClientWrapper 作为 main：keytab 登录 + 设置 JVM Subject + 调 SqlClient.main
#    （Flink SQL Client embedded 模式是独立 JVM，不做 Kerberos subject 初始化）

set -e

KRB_PRINCIPAL="flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM"
KRB_KEYTAB="/etc/security/keytabs/flink.service.keytab"
FLINK_HOME="/opt/flink"

echo "[entrypoint] kinit..."
kinit -kt "$KRB_KEYTAB" "$KRB_PRINCIPAL" && echo "[entrypoint] kinit ok" || echo "[entrypoint] kinit fail"

# 构建 Flink classpath（和 sql-client.sh 一样）
CP="$FLINK_HOME/lib/*:$FLINK_HOME/lib/extra/*:$FLINK_HOME/opt/*:$FLINK_HOME/lib/KerberosSqlClientWrapper.jar"
# 加 Hadoop conf 和 Hadoop shaded jar（Flink 内部需要）
# $HADOOP_CONF_DIR 已经在环境变量里

echo "[entrypoint] Running: java KerberosSqlClientWrapper $*"
exec java \
  $FLINK_ENV_JAVA_OPTS \
  -Djava.security.auth.login.config=/etc/security/flink-client-jaas.conf \
  -Djavax.security.auth.useSubjectCredsOnly=false \
  -cp "$CP" \
  KerberosSqlClientWrapper "$@"
