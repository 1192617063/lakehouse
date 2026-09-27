#!/bin/bash
set -e

REALM="${KRB5_REALM:-LAKEHOUSE.COM}"
KDC_HOST="${KRB5_KDC:-kerberos}"
ADMIN_PASSWORD="${KRB5_ADMIN_PASSWORD:-admin123}"
KEYTAB_DIR="${KRB5_KEYTAB_DIR:-/etc/security/keytabs}"

mkdir -p "$KEYTAB_DIR"

cat > /etc/krb5.conf <<KRB5
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    udp_preference_limit = 1

[realms]
    ${REALM} = {
        kdc = ${KDC_HOST}
        admin_server = ${KDC_HOST}
        default_domain = lakehouse.com
    }

[domain_realm]
    .lakehouse.com = ${REALM}
    lakehouse.com = ${REALM}
KRB5

if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "Creating KDC database for realm ${REALM}..."
    # 不用 -P 参数，避免某些环境下 -P 导致 -s (stash) 被忽略
    kdb5_util create -r "${REALM}" -s </dev/null
    # 显式 stash master key（确保 kadmind 能 fetch master key）
    kdb5_util stash -P "${ADMIN_PASSWORD}" </dev/null 2>/dev/null || \
        kadmin.local -q "ktadd -k /dev/null K/M@${REALM}" 2>/dev/null || true
    # 如果 .stash 仍不存在，再手动试一次
    if [ ! -f /var/lib/krb5kdc/.stash ]; then
        echo "⚠️  .stash 未生成，手动 kdb5_util stash..."
        kdb5_util stash -P "${ADMIN_PASSWORD}" 2>&1 || true
    fi
    echo "KDC database created. .stash exists: $( [ -f /var/lib/krb5kdc/.stash ] && echo YES || echo NO )"
fi

cat > /etc/krb5kdc/kadm5.acl <<ACL
*/admin@${REALM}    *
ACL

kadmin.local -q "addprinc -pw ${ADMIN_PASSWORD} admin/admin@${REALM}" 2>/dev/null || true

create_and_export() {
    local principal="$1"
    local keytab="$2"
    # keytab 已存在则跳过，避免重启时轮换密钥导致其它组件认证失败
    if [ -f "$keytab" ]; then
        echo "Keytab $keytab already exists, skipping key rotation"
        return
    fi
    kadmin.local -q "addprinc -randkey ${principal}" 2>/dev/null || true
    kadmin.local -q "ktadd -k ${keytab} ${principal}" 2>/dev/null || true
    chmod 644 "${keytab}" 2>/dev/null || true
}

# NameNode keytab：nn + HTTP 两个 principal
NN_KEYTAB="${KEYTAB_DIR}/nn.service.keytab"
if [ ! -f "${NN_KEYTAB}" ]; then
    kadmin.local -q "addprinc -randkey nn/namenode.lakehouse.com@${REALM}" 2>/dev/null || true
    kadmin.local -q "addprinc -randkey HTTP/namenode.lakehouse.com@${REALM}" 2>/dev/null || true
    kadmin.local -q "ktadd -k ${NN_KEYTAB} nn/namenode.lakehouse.com@${REALM} HTTP/namenode.lakehouse.com@${REALM}" 2>/dev/null || true
    chmod 644 "${NN_KEYTAB}" 2>/dev/null || true
else
    echo "Keytab ${NN_KEYTAB} already exists, skipping key rotation"
fi

create_and_export "dn/datanode.lakehouse.com@${REALM}" "${KEYTAB_DIR}/dn.service.keytab"

# Hive keytab：metastore + hiveserver + HTTP 三个 principal
HIVE_KEYTAB="${KEYTAB_DIR}/hive.service.keytab"
if [ ! -f "${HIVE_KEYTAB}" ]; then
    kadmin.local -q "addprinc -randkey hive/hivemetastore.lakehouse.com@${REALM}" 2>/dev/null || true
    kadmin.local -q "addprinc -randkey hive/hiveserver.lakehouse.com@${REALM}" 2>/dev/null || true
    kadmin.local -q "addprinc -randkey HTTP/hiveserver.lakehouse.com@${REALM}" 2>/dev/null || true
    kadmin.local -q "ktadd -k ${HIVE_KEYTAB} hive/hivemetastore.lakehouse.com@${REALM} hive/hiveserver.lakehouse.com@${REALM} HTTP/hiveserver.lakehouse.com@${REALM}" 2>/dev/null || true
    chmod 644 "${HIVE_KEYTAB}" 2>/dev/null || true
else
    echo "Keytab ${HIVE_KEYTAB} already exists, skipping key rotation"
fi

create_and_export "flink/flinkjobmanager.lakehouse.com@${REALM}" "${KEYTAB_DIR}/flink.service.keytab"
create_and_export "kafka/kafka.lakehouse.com@${REALM}" "${KEYTAB_DIR}/kafka.service.keytab"
create_and_export "spark/sparkmaster.lakehouse.com@${REALM}" "${KEYTAB_DIR}/spark.service.keytab"
create_and_export "iceberg/icebergrest.lakehouse.com@${REALM}" "${KEYTAB_DIR}/iceberg.service.keytab"
create_and_export "trino/trino.lakehouse.com@${REALM}" "${KEYTAB_DIR}/trino.service.keytab"
create_and_export "yarn/resourcemanager.lakehouse.com@${REALM}" "${KEYTAB_DIR}/rm.service.keytab"
create_and_export "yarn/nodemanager.lakehouse.com@${REALM}" "${KEYTAB_DIR}/nm.service.keytab"

# HBase keytab：master + regionserver 两个 principal（同一个 keytab）
# 注意：keytab 已存在时也要 check principal 是否存在（之前 principal 被吞过静默丢失）
HBASE_KEYTAB="${KEYTAB_DIR}/hbase.service.keytab"
HBASE_MASTER="hbase/hbasemaster.lakehouse.com@${REALM}"
HBASE_RS="hbase/hbaseregionserver.lakehouse.com@${REALM}"

HBASE_MISSING=false
kadmin.local -q "getprinc ${HBASE_MASTER}" 2>&1 | grep -q "Principal does not exist" && HBASE_MISSING=true

if $HBASE_MISSING; then
    echo "⚠️ HBase principal 缺失（被之前的 bug 吞了），重建..."
    kadmin.local -q "addprinc -randkey ${HBASE_MASTER}"
    kadmin.local -q "addprinc -randkey ${HBASE_RS}"
    kadmin.local -q "ktadd -k ${HBASE_KEYTAB} ${HBASE_MASTER} ${HBASE_RS}"
elif [ ! -f "${HBASE_KEYTAB}" ]; then
    kadmin.local -q "addprinc -randkey ${HBASE_MASTER}" 2>/dev/null || true
    kadmin.local -q "addprinc -randkey ${HBASE_RS}" 2>/dev/null || true
    kadmin.local -q "ktadd -k ${HBASE_KEYTAB} ${HBASE_MASTER} ${HBASE_RS}" 2>/dev/null || true
fi
chmod 644 "${HBASE_KEYTAB}" 2>/dev/null || true

kadmin.local -q "addprinc -randkey lakehouse@${REALM}" 2>/dev/null || true
if [ ! -f "${KEYTAB_DIR}/lakehouse.keytab" ]; then
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/lakehouse.keytab lakehouse@${REALM}" 2>/dev/null || true
fi
chmod 644 "${KEYTAB_DIR}/lakehouse.keytab" 2>/dev/null || true

echo "KDC principals and keytabs ready."

echo "Starting KDC..."
krb5kdc -n &
KDC_PID=$!

echo "Starting kadmin..."
kadmind -nofork &
KADMIN_PID=$!

echo "KDC is ready. Realm: ${REALM}, admin password: ${ADMIN_PASSWORD}"
echo "Platform user: lakehouse@${REALM}"

wait $KDC_PID $KADMIN_PID
