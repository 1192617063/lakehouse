#!/usr/bin/env bash
# Flink SQL Client 交互脚本（Kerberos 认证）
# 容器名 flink-sql-client（compose 定义），entrypoint sleep infinity
set -e

docker exec -it flink-sql-client bash -c '
# Kerberos kinit（用 flink service keytab）
export KRB5CCNAME=/tmp/krb5cc_flink
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flinkjobmanager.lakehouse.com@LAKEHOUSE.COM
klist 2>/dev/null | head -2

# Flink SQL Client
export FLINK_CLASSPATH=/opt/flink/lib/extra/*
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
/opt/flink/bin/sql-client.sh -i /opt/flink/conf/sql-client-init.sql
'
