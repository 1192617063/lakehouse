#!/bin/bash
# switch-to-kerberos.sh — 一键从 SIMPLE auth 切换回 Kerberos（完整全栈含 HBase）
# ⚠️ 切换后需执行：docker compose down && docker compose up -d
#
# 反向脚本：scripts/switch-to-simple.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# --- source check-auth-status.sh 函数 ---
source "$(dirname "$0")/check-auth-status.sh"

echo "=========================================="
echo "  SIMPLE → Kerberos auth 切换脚本"
echo "=========================================="
echo ""

# --- 安全快照 ---
if git rev-parse --git-dir > /dev/null 2>&1; then
  echo "=== [备份] git diff 快照到 .switch-state.before-kerberos/ ==="
  mkdir -p .switch-state.before-kerberos
  git diff > .switch-state.before-kerberos/config.patch 2>/dev/null || true
  echo "  ✅ 已保存 git diff，可 patch -R 恢复"
else
  echo "=== [备份] 非 git 仓库，手动 cp ==="
  mkdir -p .switch-state.before-kerberos/conf
  cp -r conf/hadoop conf/spark conf/hive conf/trino conf/hbase .switch-state.before-kerberos/conf/ 2>/dev/null || true
  cp docker-compose.yaml .switch-state.before-kerberos/ 2>/dev/null || true
  echo "  ✅ conf/ + docker-compose.yaml 已备份"
fi
echo ""

echo "=== [1/5] HDFS core-site.xml ==="
sed -i 's|<value>simple</value>|<value>kerberos</value>|' conf/hadoop/core-site.xml
echo "  ✅ hadoop.security.authentication = kerberos"

echo "=== [2/5] Spark spark-defaults.conf ==="
sed -i 's|spark.hadoop.hadoop.security.authentication simple|spark.hadoop.hadoop.security.authentication kerberos|' conf/spark/spark-defaults.conf
echo "  ✅ Spark Hadoop auth = kerberos"

echo "=== [3/5] Hive hive-site.xml ==="
sed -i 's|<value>NOSASL</value>|<value>KERBEROS</value>|' conf/hive/hive-site.xml
# 去掉 ! 前缀恢复 metastore kerberos/sasl 属性
sed -i 's|<name>!hive.metastore.kerberos|<name>hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.sasl|<name>hive.metastore.sasl|g' conf/hive/hive-site.xml
echo "  ✅ HS2 KERBEROS + Metastore Kerberos 恢复"

echo "=== [4/5] Trino catalogs (hive/hudi/iceberg) ==="
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  sed -i 's/hive.metastore.authentication.type=NONE/hive.metastore.authentication.type=KERBEROS/' "$f"
  sed -i 's/hive.hdfs.authentication.type=NONE/hive.hdfs.authentication.type=KERBEROS/' "$f"
  # 去掉 principal/keytab 行的 # 注释
  sed -i 's/^# hive.metastore.service.principal/hive.metastore.service.principal/' "$f"
  sed -i 's/^# hive.metastore.client.principal/hive.metastore.client.principal/' "$f"
  sed -i 's/^# hive.metastore.client.keytab/hive.metastore.client.keytab/' "$f"
  sed -i 's/^# hive.hdfs.trino.principal/hive.hdfs.trino.principal/' "$f"
  sed -i 's/^# hive.hdfs.trino.keytab/hive.hdfs.trino.keytab/' "$f"
done
echo "  ✅ Trino catalogs KERBEROS + principal/keytab 打开"

echo "=== [5/5] HBase ==="

# 5a. hbase-site.xml — simple → kerberos
sed -i 's|<value>simple</value>|<value>kerberos</value>|' conf/hbase/hbase-site.xml
echo "  ✅ hbase-site.xml simple → kerberos"

# 5b. docker-compose.yaml — 恢复 Kerberos 版 HBase 服务定义
#     因为 docker-compose.yaml 只有两份固定的 HBase 定义（Kerberos 版和 SIMPLE 版），
#     且 Kerberos 版是 git HEAD 里的版本，直接 git checkout 恢复最稳
if git rev-parse --git-dir > /dev/null 2>&1; then
  # 只恢复 HBase 相关部分？不——整个文件 checkout 更安全
  echo "  用 git checkout 恢复 docker-compose.yaml（Kerberos 版是 HEAD）..."
  git checkout HEAD -- docker-compose.yaml
  echo "  ✅ docker-compose.yaml 已恢复 Kerberos 版（classpath mounts + kinit + JAVA_TOOL_OPTIONS）"
else
  echo "  ⚠️ 非 git 仓库，手动用 Python 替换 SIMPLE → Kerberos（需要当前文件是 SIMPLE 版）"
  python3 <<'PYEOF'
with open("docker-compose.yaml") as f: c = f.read()

# 把 SIMPLE 版 HBase master volumes/env/command 换成 Kerberos 版
# --- master volumes ---
c = c.replace(
    """    volumes:
      # SIMPLE auth：HBase 用独立 Hadoop conf（不读全局 kerberos）
      - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.63""",
    """    volumes:
      # 关键：kerberos 版 core-site.xml 放到 HBase classpath 上 → Hadoop Configuration 自动加载
      - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.63""")

# master command
c = c.replace(
    'command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && /opt/hbase/bin/hbase master start"]',
    'command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase master start"]')

# master env: 加 JAVA_TOOL_OPTIONS, 移除 HADOOP_USER_NAME
c = c.replace(
    """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      HADOOP_USER_NAME: hbase
    ports:
      - "16010:16010"
      - "9090:9090"
    depends_on: [namenode, zookeeper]""",
    """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      JAVA_TOOL_OPTIONS: "-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
    ports:
      - "16010:16010"
      - "9090:9090"
    depends_on: [namenode, zookeeper]""")

# --- rs volumes ---
c = c.replace(
    """    volumes:
      - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.64""",
    """    volumes:
      - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.64""")

# rs command
c = c.replace(
    'command: ["bash", "-c", "/opt/hbase/bin/hbase regionserver start"]',
    'command: ["bash", "-c", "kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase regionserver start"]')

# rs env
c = c.replace(
    """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      HADOOP_USER_NAME: hbase
    ports:
      - "16020:16020"
    depends_on: [hbase-master, zookeeper]""",
    """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      JAVA_TOOL_OPTIONS: "-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
    ports:
      - "16020:16020"
    depends_on: [hbase-master, zookeeper]""")

with open("docker-compose.yaml","w") as f: f.write(c)
print("  ✅ docker-compose.yaml HBase 服务 Kerberos 版替换完成")
PYEOF
fi

echo ""
echo "=========================================="
echo "  ✅ 配置切换完成！"
echo "=========================================="
echo ""
echo "下一步（关键顺序：KDC 必须先起来）："
echo "  docker compose down"
echo "  docker compose up -d kerberos zookeeper mysql postgres    # 先起依赖"
echo "  sleep 30 && docker compose up -d                         # 再起全栈"
echo ""
echo "⚠️  切 Kerberos 后如果 HBase 起不来（kinit: principal not found）"
echo "     手动补 principal："
echo "       docker exec kerberos kadmin.local -q \"addprinc -randkey hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM\""
echo "       docker exec kerberos kadmin.local -q \"addprinc -randkey hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM\""
echo "       docker exec kerberos kadmin.local -q \"ktadd -k /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM\""
echo ""
echo "切回 SIMPLE: ./scripts/switch-to-simple.sh && docker compose down && docker compose up -d"
echo "备份位置：.switch-state.before-kerberos/"

# --- 自动跑一次静态检查 ---
check_auth_status
echo ""
echo "💡  重启后确认全栈正常：./scripts/check-auth-status.sh --live"
