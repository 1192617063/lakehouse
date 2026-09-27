#!/usr/bin/env bash
set +e
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "========== 容器状态 =========="
docker compose -f "$PROJECT_ROOT/docker-compose.yaml" ps --format "table {{.Name}}\t{{.State}}\t{{.Status}}"

echo
echo "========== 服务连通性 =========="
printf "HDFS NN (9870)  : "; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:9870
printf "HDFS 写 (iceberg): "; docker exec namenode bash -c "kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM && hdfs dfs -mkdir -p /lakehouse/iceberg" >/dev/null 2>&1 && echo OK || echo FAIL
printf "HDFS 写 (paimon) : "; docker exec namenode bash -c "kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM && hdfs dfs -mkdir -p /lakehouse/paimon" >/dev/null 2>&1 && echo OK || echo FAIL
printf "HiveMS  (9083)   : "; (exec 3<>/dev/tcp/127.0.0.1/9083) 2>/dev/null && echo OK || echo FAIL
printf "Kafka   (9092)   : "; docker exec kafka pgrep -f kafka.Kafka >/dev/null 2>&1 && echo OK || echo FAIL
printf "Flink   (8081)   : "; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8081
printf "DorisFE (9030)   : "; mysql -uroot -h127.0.0.1 -P9030 -e "select 1" >/dev/null 2>&1 && echo OK || echo FAIL
printf "DorisBE (Alive)  : "; mysql -uroot -h127.0.0.1 -P9030 -e "SHOW BACKENDS\G" 2>/dev/null | grep -o "Alive: [a-z]*" | head -1 || echo FAIL
printf "MySQL   (3306)   : "; mysql -uroot -h127.0.0.1 -P3306 -proot123 -e "select 1" >/dev/null 2>&1 && echo OK || echo FAIL
printf "Spark   (8080)   : "; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080
printf "MongoDB (27017)  : "; docker exec mongodb mongosh -u root -p root123 --authenticationDatabase admin --eval "db.runCommand({ping:1})" >/dev/null 2>&1 && echo OK || echo FAIL
printf "Iceberg (8181)   : "; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8181
printf "Trino   (8085)   : "; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8085/v1/info

echo
echo "========== 依赖 Jar 检查 =========="
FLINK_LIB="$PROJECT_ROOT/lib/flink/extra"
SPARK_LIB="$PROJECT_ROOT/lib/spark"
printf "Paimon  Flink jar: "; [ -f "$FLINK_LIB/paimon-flink-1.19-0.9.0.jar" ] && echo OK || echo MISSING
printf "Iceberg Flink jar: "; [ -f "$FLINK_LIB/iceberg-flink-runtime-1.19-1.7.1.jar" ] && echo OK || echo MISSING
printf "Hudi    Flink jar: "; [ -f "$FLINK_LIB/hudi-flink1.19-bundle-1.0.2.jar" ] && echo OK || echo MISSING
printf "Hive    Flink jar: "; [ -f "$FLINK_LIB/flink-connector-hive_2.12-1.19.1.jar" ] && echo OK || echo MISSING
printf "Hadoop  Flink jar: "; [ -f "$FLINK_LIB/flink-shaded-hadoop3-uber-blink-3.7.0.jar" ] && echo OK || echo MISSING
printf "Calcite stub jar : "; [ -f "$FLINK_LIB/calcite-stub.jar" ] && echo OK || echo MISSING
printf "Paimon  Spark jar: "; [ -f "$SPARK_LIB/paimon-spark-3.5-0.9.0.jar" ] && echo OK || echo MISSING
printf "Iceberg Spark jar: "; [ -f "$SPARK_LIB/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" ] && echo OK || echo MISSING
printf "Hudi    Spark jar: "; [ -f "$SPARK_LIB/hudi-spark3.5-bundle_2.12-1.0.2.jar" ] && echo OK || echo MISSING
