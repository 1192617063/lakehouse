#!/usr/bin/env bash
# SQL Gateway entrypoint: 在独立 JVM daemon 启动前做 keytab 登录 + UGI subject 初始化
# 问题：Flink Security 模块只在 ClusterEntrypoint (JM/TM) 初始化时生效
# SQL Gateway 不走 ClusterEntrypoint，所以必须手动做 keytab 登录

set -e

KRB_PRINCIPAL="flink/flinkjobmanager.lakehouse.com@LAKEHOUSE.COM"
KRB_KEYTAB="/etc/security/keytabs/flink.service.keytab"

# 先清理残留 daemon
/opt/flink/bin/sql-gateway.sh stop-all 2>/dev/null || true

echo "[gateway-entrypoint] kinit with keytab..."
kinit -kt "$KRB_KEYTAB" "$KRB_PRINCIPAL" \
  && echo "[gateway-entrypoint] kinit succeeded" \
  || { echo "[gateway-entrypoint] WARNING: kinit failed"; }

echo "[gateway-entrypoint] Starting SQL Gateway: $*"
exec "$@"
