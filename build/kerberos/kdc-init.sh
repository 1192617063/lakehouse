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
    kdb5_util create -r "${REALM}" -s -P "${ADMIN_PASSWORD}"
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

create_and_export "flink/flink-jobmanager.lakehouse.com@${REALM}" "${KEYTAB_DIR}/flink.service.keytab"
create_and_export "kafka/kafka.lakehouse.com@${REALM}" "${KEYTAB_DIR}/kafka.service.keytab"
create_and_export "spark/spark-master.lakehouse.com@${REALM}" "${KEYTAB_DIR}/spark.service.keytab"
create_and_export "iceberg/iceberg-rest.lakehouse.com@${REALM}" "${KEYTAB_DIR}/iceberg.service.keytab"
create_and_export "trino/trino.lakehouse.com@${REALM}" "${KEYTAB_DIR}/trino.service.keytab"
create_and_export "rm/namenode.lakehouse.com@${REALM}" "${KEYTAB_DIR}/rm.service.keytab"
create_and_export "nm/namenode.lakehouse.com@${REALM}" "${KEYTAB_DIR}/nm.service.keytab"

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
