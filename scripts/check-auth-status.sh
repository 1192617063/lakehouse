#!/bin/bash
# check-auth-status.sh — 检查当前 auth 配置是否一致（静态 + 可选 --live 容器健康检查）
#
# 用法:
#   ./scripts/check-auth-status.sh           # 只看 conf 文件当前配置
#   ./scripts/check-auth-status.sh --live    # 再加 docker compose ps + 端口探测 + HBase Shell status
#
# 被 source 时导出函数 check_auth_status，switch-to-simple.sh / switch-to-kerberos.sh 切换完自动调用

check_auth_status() {
  local LIVE="${1:-}"
  local MISMATCH=0
  local EXPECTED=""
  local FOUND

  echo "=========================================="
  echo "  当前 Auth 配置检查"
  echo "=========================================="
  echo ""

  # --- 1. HDFS core-site.xml ---
  FOUND=$(grep -A1 'hadoop.security.authentication' conf/hadoop/core-site.xml 2>/dev/null | grep "<value>" | sed 's/.*<value>\(.*\)<\/value>.*/\1/' | tr '[:upper:]' '[:lower:]')
  echo "① HDFS core-site.xml hadoop.security.authentication = $FOUND"
  [ -z "$EXPECTED" ] && EXPECTED="$FOUND"
  [ "$FOUND" != "$EXPECTED" ] && { echo "   ⚠️ 与 HDFS 不一致"; MISMATCH=1; }

  # --- 2. Spark spark-defaults.conf ---
  FOUND=$(grep "hadoop.security.authentication" conf/spark/spark-defaults.conf 2>/dev/null | awk '{print $NF}' | tr '[:upper:]' '[:lower:]')
  echo "② Spark spark-defaults.conf                        = $FOUND"
  [ "$FOUND" != "$EXPECTED" ] && { echo "   ⚠️ 与 HDFS 不一致"; MISMATCH=1; }

  # --- 3. Hive HS2 + Metastore ---
  # 只取 hive.server2.authentication 这一个 property（不 grep 到后续的 keytab/principal）
  local HS2=$(grep -A2 'hive.server2.authentication' conf/hive/hive-site.xml 2>/dev/null | grep '<value>' | head -1 | sed 's/.*<value>\(.*\)<\/value>.*/\1/' | tr '[:upper:]' '[:lower:]')
  # Metastore kerberos 禁用 = <name>以 !hive.metastore.kerberos 开头（被 switch-to-simple 加的前缀）
  local METASTORE_KERBEROS_DISABLED=$(grep -c '!hive.metastore.kerberos' conf/hive/hive-site.xml 2>/dev/null)
  # Metastore kerberos 启用 = 有 hive.metastore.kerberos.keytab（无前缀 !）
  local METASTORE_KERBEROS_ENABLED=$(grep 'hive.metastore.kerberos.keytab' conf/hive/hive-site.xml 2>/dev/null | grep -cv '^    <name>!' || true)
  echo "③ Hive hive-site.xml  HS2 auth                      = $HS2"
  echo "                       Metastore kerberos 禁用（!）     = $( [ "$METASTORE_KERBEROS_DISABLED" -gt 0 ] && echo YES || echo NO )"
  echo "                       Metastore kerberos 启用          = $( [ "$METASTORE_KERBEROS_ENABLED" -gt 0 ] && echo YES || echo NO )"
  if [ "$EXPECTED" = "kerberos" ]; then
    [ "$HS2" != "kerberos" ] && { echo "   ⚠️ Kerberos 态但 HS2=$HS2"; MISMATCH=1; }
    [ "$METASTORE_KERBEROS_DISABLED" -gt 0 ] && { echo "   ⚠️ Kerberos 态但 Metastore kerberos 被禁用（! 前缀）"; MISMATCH=1; }
    [ "$METASTORE_KERBEROS_ENABLED" -eq 0 ] && { echo "   ⚠️ Kerberos 态但 Metastore kerberos 未启用"; MISMATCH=1; }
  else
    [ "$HS2" != "nosasl" ] && { echo "   ⚠️ SIMPLE 态但 HS2=$HS2（应该 NOSASL）"; MISMATCH=1; }
    [ "$METASTORE_KERBEROS_DISABLED" -eq 0 ] && [ "$METASTORE_KERBEROS_ENABLED" -gt 0 ] && { echo "   ⚠️ SIMPLE 态但 Metastore kerberos 未禁用"; MISMATCH=1; }
  fi

  # --- 4. Trino catalogs ---
  local TRINO_HIVE=$(grep "hive.metastore.authentication.type=" conf/trino/catalog/hive.properties 2>/dev/null | tail -1 | sed 's/.*=\(.*\)/\1/' | tr '[:upper:]' '[:lower:]')
  local TRINO_HDFS=$(grep "hive.hdfs.authentication.type=" conf/trino/catalog/hive.properties 2>/dev/null | tail -1 | sed 's/.*=\(.*\)/\1/' | tr '[:upper:]' '[:lower:]')
  local TRINO_PRINCIPAL_COMMENTED=$(grep -c "^# hive.metastore.client.principal=" conf/trino/catalog/hive.properties 2>/dev/null)
  echo "④ Trino hive.properties  metastore auth             = $TRINO_HIVE"
  echo "                       hdfs auth                      = $TRINO_HDFS"
  echo "                       principal 注释（#）            = $( [ "$TRINO_PRINCIPAL_COMMENTED" -gt 0 ] && echo YES || echo NO )"
  [ "$TRINO_HIVE" != "$EXPECTED" ] && { echo "   ⚠️ 与 HDFS 不一致"; MISMATCH=1; }
  [ "$TRINO_HDFS" != "$EXPECTED" ] && { echo "   ⚠️ 与 HDFS 不一致"; MISMATCH=1; }

  # --- 5. HBase ---
  local HB_AUTH=$(grep -A1 'hbase.security.authentication' conf/hbase/hbase-site.xml 2>/dev/null | grep "<value>" | sed 's/.*<value>\(.*\)<\/value>.*/\1/' | tr '[:upper:]' '[:lower:]')
  local HB_HADOOP_AUTH=$(grep -A1 '<name>hadoop.security.authentication</name>' conf/hbase/hbase-site.xml 2>/dev/null | grep "<value>" | sed 's/.*<value>\(.*\)<\/value>.*/\1/' | tr '[:upper:]' '[:lower:]')
  echo "⑤ HBase hbase-site.xml  hbase.security.authentication = $HB_AUTH"
  echo "                       hadoop.security.authentication  = $HB_HADOOP_AUTH"
  [ "$HB_AUTH" != "$EXPECTED" ] && { echo "   ⚠️ 与 HDFS 不一致"; MISMATCH=1; }
  [ "$HB_HADOOP_AUTH" != "$EXPECTED" ] && { echo "   ⚠️ hadoop.security.authentication 与 HDFS 不一致"; MISMATCH=1; }

  # --- 6. HBase docker-compose.yaml ---
  local DC_HAS_CLASSPATH=0
  local DC_HAS_KINIT=0
  local DC_HAS_JAVA_TOOL=0
  local DC_HAS_HADOOP_USER=0
  local DC_HAS_HBASE_HADOOP_VOL=0

  # volumes: classpath core-site.xml → Kerberos, hbase-hadoop → SIMPLE
  grep -q 'core-site.xml:/opt/hbase/conf/core-site.xml' docker-compose.yaml 2>/dev/null && DC_HAS_CLASSPATH=1
  grep -q 'hbase-hadoop:/opt/hadoop/etc/hadoop' docker-compose.yaml 2>/dev/null && DC_HAS_HBASE_HADOOP_VOL=1
  # command: kinit 前缀 → Kerberos
  grep -q 'command:.*kinit.*hbase.service.keytab' docker-compose.yaml 2>/dev/null && DC_HAS_KINIT=1
  # environment: JAVA_TOOL_OPTIONS Kerberos → Kerberos
  grep -q 'JAVA_TOOL_OPTIONS.*krb5.conf' docker-compose.yaml 2>/dev/null && DC_HAS_JAVA_TOOL=1
  # environment: HADOOP_USER_NAME=hbase → SIMPLE
  grep -q 'HADOOP_USER_NAME: hbase' docker-compose.yaml 2>/dev/null && DC_HAS_HADOOP_USER=1

  echo ""
  echo "⑥ HBase docker-compose.yaml:"
  echo "   volumes 含 classpath core-site.xml  = $( [ $DC_HAS_CLASSPATH -eq 1 ] && echo YES || echo NO )"
  echo "   volumes 含 hbase-hadoop 独立 conf    = $( [ $DC_HAS_HBASE_HADOOP_VOL -eq 1 ] && echo YES || echo NO )"
  echo "   command 含 kinit 前缀                = $( [ $DC_HAS_KINIT -eq 1 ] && echo YES || echo NO )"
  echo "   env 含 JAVA_TOOL_OPTIONS (Kerberos)  = $( [ $DC_HAS_JAVA_TOOL -eq 1 ] && echo YES || echo NO )"
  echo "   env 含 HADOOP_USER_NAME: hbase       = $( [ $DC_HAS_HADOOP_USER -eq 1 ] && echo YES || echo NO )"

  # 一致性检查
  if [ "$EXPECTED" = "kerberos" ]; then
    [ $DC_HAS_CLASSPATH -ne 1 ] && { echo "   ⚠️ Kerberos 态但缺 classpath core-site.xml mount"; MISMATCH=1; }
    [ $DC_HAS_KINIT -ne 1 ] && { echo "   ⚠️ Kerberos 态但缺 kinit 命令"; MISMATCH=1; }
    [ $DC_HAS_JAVA_TOOL -ne 1 ] && { echo "   ⚠️ Kerberos 态但缺 JAVA_TOOL_OPTIONS"; MISMATCH=1; }
  elif [ "$EXPECTED" = "simple" ]; then
    [ $DC_HAS_HADOOP_USER -ne 1 ] && { echo "   ⚠️ SIMPLE 态但缺 HADOOP_USER_NAME=hbase（会 PermissionDenied）"; MISMATCH=1; }
    [ $DC_HAS_CLASSPATH -eq 1 ] && { echo "   ⚠️ SIMPLE 态但 classpath 还挂着 Kerberos core-site.xml"; MISMATCH=1; }
  fi

  # --- 7. 总结 ---
  echo ""
  echo "=========================================="
  if [ $MISMATCH -eq 0 ]; then
    echo "  ✅ 全栈 auth 一致：$(echo $EXPECTED | tr '[:lower:]' '[:upper:]')"
  else
    echo "  ⚠️  配置不一致（MISMATCH=$MISMATCH），建议重跑切换脚本"
  fi
  echo "=========================================="

  # --- 8. --live 模式：容器 + 端口 + HBase Shell ---
  if [ "$LIVE" = "--live" ]; then
    echo ""
    echo "=========================================="
    echo "  Live 容器健康检查"
    echo "=========================================="
    echo ""
    echo "① docker compose ps - Up:"
    local UP_COUNT=$(docker compose ps 2>/dev/null | grep -c "Up\|healthy")
    local TOTAL_COUNT=$(docker compose ps -a 2>/dev/null | tail -n +2 | wc -l)
    echo "   $UP_COUNT / $TOTAL_COUNT Up"
    echo ""
    echo "② 关键端口（HTTP only）:"
    # 只探测 Web UI 端口，RPC 端口（16020/2181/21066/9090）跳过
    for item in "9870:HDFS NameNode WebUI" "16010:HBase Master WebUI" "16030:HBase RS WebUI" "8081:Flink JM WebUI" "8085:Trino WebUI" "8088:Yarn RM WebUI" "4040:Spark UI" "9083:Hive Metastore WebUI"; do
      port=${item%%:*}
      name=${item##*:}
      local CODE
      CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 2 "http://localhost:$port/" 2>/dev/null || echo "000")
      if [ "$CODE" != "000" ]; then
        echo "   ✅ $port ($name) HTTP $CODE"
      else
        echo "   ❌ $port ($name) 未响应"
      fi
    done
    echo ""
    echo "③ HBase Shell status:"
    docker exec hbase-master timeout 15 /opt/hbase/bin/hbase shell 2>/dev/null <<EOF | grep -E "^\s+[0-9]+ [a-z]" | head -3
status
exit
EOF
    echo ""
    echo "④ Kerberos 特项（仅 Kerberos 态）:"
    if [ "$EXPECTED" = "kerberos" ]; then
      local KDC_UP=$(docker compose ps kerberos 2>/dev/null | grep -c "Up")
      echo "   KDC 容器: $( [ $KDC_UP -gt 0 ] && echo ✅ Up || echo ❌ Down )"
      docker exec kerberos kadmin -p admin/admin -w admin123 -q "getprincs" 2>/dev/null | wc -l | xargs -I{} echo "   KDC principals: {}"
    fi
    echo "=========================================="
  fi
}

# 如果直接被调用（不是 source），跑一次
# source 时 BASH_SOURCE[0] != $0 （$0 还是主脚本，BASH_SOURCE[0] 是当前文件）
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  cd "$SCRIPT_DIR/.."
  check_auth_status "${1:-}"
fi
