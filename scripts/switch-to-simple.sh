#!/bin/bash
# switch-to-simple.sh — 一键回滚到 SIMPLE auth（从全栈 Kerberos）
#
# ⚠️ 切换后必须执行：docker compose down && docker compose up -d
#    HBase 是全栈中最复杂的，回滚涉及 hbase-site.xml + docker-compose volumes/command/environment
#
# 反向脚本：scripts/switch-to-kerberos.sh（如果还要回 Kerberos）

set -euo pipefail
cd "$(dirname "$0")/.."

# --- source check-auth-status.sh 函数 ---
source "$(dirname "$0")/check-auth-status.sh"

echo "=========================================="
echo "  Kerberos → SIMPLE auth 回滚脚本"
echo "=========================================="
echo ""

# --- 安全快照：先 git commit 或备份 ---
if git rev-parse --git-dir > /dev/null 2>&1; then
  echo "=== [备份] git diff 快照到 .switch-state.before-simple/ ==="
  mkdir -p .switch-state.before-simple
  git diff > .switch-state.before-simple/config.patch 2>/dev/null || true
  echo "  ✅ 已保存 git diff，可 patch -R 恢复"
else
  echo "=== [备份] 非 git 仓库，手动 cp ==="
  mkdir -p .switch-state.before-simple/conf
  cp -r conf/hadoop conf/spark conf/hive conf/trino conf/hbase .switch-state.before-simple/conf/ 2>/dev/null || true
  cp docker-compose.yaml .switch-state.before-simple/ 2>/dev/null || true
  echo "  ✅ conf/ + docker-compose.yaml 已备份到 .switch-state.before-simple/"
fi
echo ""

echo "=== [1/5] HDFS core-site.xml ==="
sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hadoop/core-site.xml
echo "  ✅ hadoop.security.authentication = simple"

echo "=== [2/5] Spark spark-defaults.conf ==="
sed -i 's|spark.hadoop.hadoop.security.authentication kerberos|spark.hadoop.hadoop.security.authentication simple|' conf/spark/spark-defaults.conf
echo "  ✅ Spark Hadoop auth = simple"

echo "=== [3/5] Hive hive-site.xml ==="
sed -i 's|<value>KERBEROS</value>|<value>NOSASL</value>|' conf/hive/hive-site.xml
# Metastore kerberos/sasl 属性名加 ! 前缀禁用
sed -i 's|<name>hive.metastore.kerberos|<name>!hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>hive.metastore.sasl|<name>!hive.metastore.sasl|g' conf/hive/hive-site.xml
echo "  ✅ HS2 NOSASL + Metastore Kerberos 注释（! 前缀）"

echo "=== [4/5] Trino catalogs (hive/hudi/iceberg) ==="
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  # KERBEROS → NONE
  sed -i 's/hive.metastore.authentication.type=KERBEROS/hive.metastore.authentication.type=NONE/' "$f"
  sed -i 's/hive.hdfs.authentication.type=KERBEROS/hive.hdfs.authentication.type=NONE/' "$f"
  # principal/keytab 行加 # 注释
  sed -i 's/^hive.metastore.service.principal/# hive.metastore.service.principal/' "$f"
  sed -i 's/^hive.metastore.client.principal/# hive.metastore.client.principal/' "$f"
  sed -i 's/^hive.metastore.client.keytab/# hive.metastore.client.keytab/' "$f"
  sed -i 's/^hive.hdfs.trino.principal/# hive.hdfs.trino.principal/' "$f"
  sed -i 's/^hive.hdfs.trino.keytab/# hive.hdfs.trino.keytab/' "$f"
done
echo "  ✅ Trino catalogs NONE + principal/keytab 注释"

echo "=== [5/5] HBase（3 处：hbase-site.xml + docker-compose volumes/command/env）==="

# 5a. hbase-site.xml — 从 Kerberos 改回 SIMPLE
# hbase.security.authentication: kerberos → simple
sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hbase/hbase-site.xml
# hadoop.security.authorization: false → 不变（本身就 false）
# 注释掉 Hadoop Kerberos 属性（让 Hadoop 默认读 core-default.xml 的 simple）
# 但保留 hadoop.security.authorization=false 没问题
echo "  ✅ hbase-site.xml Kerberos → simple"

# 5b. docker-compose.yaml — HBase volumes 从 classpath mounts 回退
#     Kerberos: ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro (classpath)
#     SIMPLE:   恢复 ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro（或完全移除 classpath mounts）
#
#     策略：移除 classpath 上的 kerberos core-site.xml/hdfs-site.xml/yarn-site.xml/mapred-site.xml
#           （Hadoop Configuration 会 fallback 到 core-default.xml → simple）
#           恢复 hbase-hadoop/ 作为 HADOOP_CONF_DIR
#     同时移除 krb5.conf / keytabs volumes（SIMPLE 不需要）
#     移除 command 里的 kinit 前缀
#     移除 JAVA_TOOL_OPTIONS（Kerberos JVM 系统属性）

python3 <<'PYEOF'
import re  # 纯标准库，不依赖 yaml

with open("docker-compose.yaml", "r") as f:
    content = f.read()

# --- hbase-master ---
# volumes: 替换 classpath mounts 为 hbase-hadoop/ 整体挂载
old_master_volumes = """    volumes:
      # 关键：kerberos 版 core-site.xml 放到 HBase classpath 上 → Hadoop Configuration 自动加载
      - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro"""

new_master_volumes = """    volumes:
      # SIMPLE auth：HBase 用独立 Hadoop conf（不读全局 kerberos）
      - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro"""

content = content.replace(old_master_volumes, new_master_volumes)

# command: 移除 kinit 前缀
old_master_cmd = 'command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase master start"]'
new_master_cmd = 'command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && /opt/hbase/bin/hbase master start"]'
content = content.replace(old_master_cmd, new_master_cmd)

# environment: 移除 JAVA_TOOL_OPTIONS
old_master_env = """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      JAVA_TOOL_OPTIONS: "-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
    ports:
      - "16010:16010"
      - "9090:9090"
    depends_on: [namenode, zookeeper]"""

new_master_env = """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      HADOOP_USER_NAME: hbase
    ports:
      - "16010:16010"
      - "9090:9090"
    depends_on: [namenode, zookeeper]"""

content = content.replace(old_master_env, new_master_env)

# --- hbase-regionserver ---
old_rs_volumes = """    volumes:
      - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro"""

new_rs_volumes = """    volumes:
      - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro"""

content = content.replace(old_rs_volumes, new_rs_volumes)

# RS command: 移除 kinit 前缀
old_rs_cmd = 'command: ["bash", "-c", "kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase regionserver start"]'
new_rs_cmd = 'command: ["bash", "-c", "/opt/hbase/bin/hbase regionserver start"]'
content = content.replace(old_rs_cmd, new_rs_cmd)

# RS environment: 移除 JAVA_TOOL_OPTIONS，加 HADOOP_USER_NAME=hbase
old_rs_env = """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      JAVA_TOOL_OPTIONS: "-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
    ports:
      - "16020:16020"
    depends_on: [hbase-master, zookeeper]"""

new_rs_env = """    environment:
      HBASE_HOME: /opt/hbase
      JAVA_HOME: /opt/java/openjdk
      HADOOP_USER_NAME: hbase
    ports:
      - "16020:16020"
    depends_on: [hbase-master, zookeeper]"""

content = content.replace(old_rs_env, new_rs_env)

with open("docker-compose.yaml", "w") as f:
    f.write(content)

print("  ✅ docker-compose.yaml HBase 服务回滚完成")
PYEOF

echo ""
echo "=========================================="
echo "  ✅ 配置回滚完成！"
echo "=========================================="
echo ""
echo "下一步："
echo "  docker compose down"
echo "  docker compose up -d"
echo ""
echo "注意："
echo "  - Kerberos KDC 可以继续保留（principal/keytabs 不动），"
echo "    只是 HDFS/Hive/Spark/Trino/HBase 暂时用 SIMPLE auth 免认证"
echo "  - 想彻底清理 KDC: rm -rf data/kerberos/* conf/kerberos/keytabs/*.keytab"
echo "  - 想回 Kerberos: ./scripts/switch-to-kerberos.sh && docker compose down && docker compose up -d"
echo ""
echo "备份位置：.switch-state.before-simple/"

# --- 自动跑一次静态检查 ---
check_auth_status
echo ""
echo "💡  重启后确认全栈正常：./scripts/check-auth-status.sh --live 或直接 docker compose ps"
