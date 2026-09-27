#!/bin/bash
# Spark 入口脚本：先做 Kerberos 认证，再启动 Spark
set -e

# 确保 spark 用户存在
if ! id spark &>/dev/null; then
    groupadd -f spark 2>/dev/null || true
    useradd -g spark -m -s /bin/bash spark 2>/dev/null || \
        useradd -m -s /bin/bash spark 2>/dev/null || true
fi

# Kerberos 认证（确保 TGT 存在，Spark 原生登录会再用 keytab 做一次）
export KRB5CCNAME=/tmp/krb5cc_spark
if [ -f /etc/security/keytabs/spark.service.keytab ]; then
    kinit -kt /etc/security/keytabs/spark.service.keytab \
        spark/spark-master.lakehouse.com@LAKEHOUSE.COM \
        -c $KRB5CCNAME 2>/dev/null && echo "[spark-entrypoint] kinit OK" || \
        echo "[spark-entrypoint] WARN: kinit failed"
    # 确保 spark 用户也能访问
    SPARK_UID=$(id -u spark 2>/dev/null || echo 1001)
    SPARK_GID=$(id -g spark 2>/dev/null || echo 1001)
    chown ${SPARK_UID}:${SPARK_GID} $KRB5CCNAME 2>/dev/null || true
    chmod 600 $KRB5CCNAME 2>/dev/null || true
    # 默认 cache 路径也复制一份
    cp $KRB5CCNAME /tmp/krb5cc_${SPARK_UID} 2>/dev/null || true
    chown ${SPARK_UID}:${SPARK_GID} /tmp/krb5cc_${SPARK_UID} 2>/dev/null || true
    chmod 600 /tmp/krb5cc_${SPARK_UID} 2>/dev/null || true
fi

# 启动 Spark
exec /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh
