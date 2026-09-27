#!/bin/bash
# switch-to-simple.sh — 一键回滚到 SIMPLE auth
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== 回滚到 SIMPLE auth ==="

sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hadoop/core-site.xml
sed -i 's|spark.hadoop.hadoop.security.authentication kerberos|spark.hadoop.hadoop.security.authentication simple|' conf/spark/spark-defaults.conf
sed -i 's|<value>KERBEROS</value>|<value>NOSASL</value>|' conf/hive/hive-site.xml
sed -i 's|<name>hive.metastore.kerberos|<name>!hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>hive.metastore.sasl|<name>!hive.metastore.sasl|g' conf/hive/hive-site.xml

for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  sed -i 's/hive.metastore.authentication.type=KERBEROS/hive.metastore.authentication.type=NONE/' "$f"
  sed -i 's/hive.hdfs.authentication.type=KERBEROS/hive.hdfs.authentication.type=NONE/' "$f"
  sed -i 's/^hive.metastore.service.principal/# hive.metastore.service.principal/' "$f"
  sed -i 's/^hive.metastore.client.principal/# hive.metastore.client.principal/' "$f"
  sed -i 's/^hive.metastore.client.keytab/# hive.metastore.client.keytab/' "$f"
  sed -i 's/^hive.hdfs.trino.principal/# hive.hdfs.trino.principal/' "$f"
  sed -i 's/^hive.hdfs.trino.keytab/# hive.hdfs.trino.keytab/' "$f"
done

echo "✅ 全部切回 SIMPLE，执行: docker compose down && docker compose up -d"
