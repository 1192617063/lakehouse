#!/bin/bash
# switch-to-kerberos.sh — 一键从 SIMPLE auth 切换到 Kerberos
# ⚠️ 切换后需执行：docker compose down && docker compose up -d
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== [1/4] HDFS core-site.xml ==="
sed -i 's|<value>simple</value>|<value>kerberos</value>|' conf/hadoop/core-site.xml
echo "  ✅ hadoop.security.authentication = kerberos"

echo "=== [2/4] Spark spark-defaults.conf ==="
sed -i 's|spark.hadoop.hadoop.security.authentication simple|spark.hadoop.hadoop.security.authentication kerberos|' conf/spark/spark-defaults.conf
echo "  ✅ Spark Hadoop auth = kerberos"

echo "=== [3/4] Hive hive-site.xml ==="
sed -i 's|<value>NOSASL</value>|<value>KERBEROS</value>|' conf/hive/hive-site.xml
# 去掉 ! 前缀恢复 metastore kerberos/sasl 属性（value 已经是对的，不碰 value）
sed -i 's|<name>!hive.metastore.kerberos|<name>hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.sasl|<name>hive.metastore.sasl|g' conf/hive/hive-site.xml
echo "  ✅ HS2 KERBEROS + Metastore Kerberos 恢复"

echo "=== [4/4] Trino catalogs ==="
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  sed -i 's/hive.metastore.authentication.type=NONE/hive.metastore.authentication.type=KERBEROS/' "$f"
  sed -i 's/hive.hdfs.authentication.type=NONE/hive.hdfs.authentication.type=KERBEROS/' "$f"
  sed -i 's/^# hive.metastore.service.principal/hive.metastore.service.principal/' "$f"
  sed -i 's/^# hive.metastore.client.principal/hive.metastore.client.principal/' "$f"
  sed -i 's/^# hive.metastore.client.keytab/hive.metastore.client.keytab/' "$f"
  sed -i 's/^# hive.hdfs.trino.principal/hive.hdfs.trino.principal/' "$f"
  sed -i 's/^# hive.hdfs.trino.keytab/hive.hdfs.trino.keytab/' "$f"
done
echo "  ✅ Trino catalogs KERBEROS + principal/keytab 打开"

echo ""
echo "=== ⚠️  HBase 保持 SIMPLE（Hadoop 2.10.2 不兼容 3.3.6 Kerberos）==="
echo "    切换 HBase Kerberos 见 docs/SWITCH_TO_KERBEROS.md Step 6"
echo ""
echo "=== 🚀 现在执行全栈重启 ==="
echo "  docker compose down && docker compose up -d"
echo ""
echo "=== ✅ 配置切换完成，重启后生效 ==="
