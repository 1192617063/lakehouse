# 湖仓学习环境部署笔记

> 适用环境：Linux + Docker + Docker Compose（单机部署）
> 目标：在全新环境中，按本文档步骤从零部署一套完整可用的湖仓环境
> 组件：HDFS + Hive + Iceberg REST + Paimon + Hudi + Flink + Spark + Doris + Kafka + MySQL + Postgres + MongoDB + Kerberos

---

## 一、环境要求

| 资源 | 最低要求 | 推荐 |
|------|----------|------|
| 内存 | 16 GB | 24 GB+ |
| CPU | 4 核 | 8 核 |
| 磁盘 | 100 GB | 500 GB+ |
| 系统 | Linux | Rocky/Ubuntu/CentOS |
| Docker | 20.10+ | 24.0+ |
| Docker Compose | v2 | v2 |

**安装 Docker（如未安装）：**
```bash
curl -fsSL https://get.docker.com | bash
systemctl enable --now docker
```

---

## 二、创建项目目录结构

```bash
mkdir -p lakehouse && cd lakehouse
mkdir -p conf/{hadoop,hive,kerberos/keytabs,kafka,flink,spark,doris}
mkdir -p build/{flink,kerberos,iceberg-rest}
mkdir -p lib/{backup,flink/extra,spark,mysql,empty}
mkdir -p data/{hadoop/namenode,hadoop/datanode,kafka,doris/fe,doris/be,mysql,postgres,mongodb,kerberos,flink/catalog-store}
mkdir -p scripts sql/flink docs
```

---

## 三、下载依赖包

### 3.1 下载所有 JAR 包（直接下载到目标目录）

```bash
cat > lib/backup/download-commands.sh << 'SCRIPT'
#!/bin/bash
set -e
MAVEN="https://repo1.maven.org/maven2"

FLINK_EXTRA="lib/flink/extra"
SPARK_DIR="lib/spark"
MYSQL_DIR="lib/mysql"
BACKUP="lib/backup"
mkdir -p "$FLINK_EXTRA" "$SPARK_DIR" "$MYSQL_DIR" "$BACKUP"

echo "===== 下载 Flink 扩展 jar 到 $FLINK_EXTRA ====="

wget -q -O "$FLINK_EXTRA/flink-connector-hive_2.12-1.19.1.jar" \
  "$MAVEN/org/apache/flink/flink-connector-hive_2.12/1.19.1/flink-connector-hive_2.12-1.19.1.jar" && echo "[OK] flink-connector-hive"

wget -q -O "$FLINK_EXTRA/paimon-flink-1.19-0.9.0.jar" \
  "$MAVEN/org/apache/paimon/paimon-flink-1.19/0.9.0/paimon-flink-1.19-0.9.0.jar" && echo "[OK] paimon-flink"

wget -q -O "$FLINK_EXTRA/iceberg-flink-runtime-1.19-1.7.1.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.1/iceberg-flink-runtime-1.19-1.7.1.jar" && echo "[OK] iceberg-flink-runtime"

wget -q -O "$FLINK_EXTRA/iceberg-hive-runtime-1.7.1.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-hive-runtime/1.7.1/iceberg-hive-runtime-1.7.1.jar" && echo "[OK] iceberg-hive-runtime"

wget -q -O "$FLINK_EXTRA/flink-sql-connector-kafka-3.3.0-1.19.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar" && echo "[OK] flink-kafka-connector"

wget -q -O "$FLINK_EXTRA/flink-doris-connector-1.19-1.6.2.jar" \
  "$MAVEN/org/apache/doris/flink-doris-connector-1.19/1.6.2/flink-doris-connector-1.19-1.6.2.jar" && echo "[OK] flink-doris-connector"

wget -q -O "$FLINK_EXTRA/flink-sql-connector-mysql-cdc-3.2.0.jar" \
  "$MAVEN/com/ververica/flink-sql-connector-mysql-cdc/3.2.0/flink-sql-connector-mysql-cdc-3.2.0.jar" && echo "[OK] flink-mysql-cdc"

wget -q -O "$FLINK_EXTRA/flink-sql-connector-postgres-cdc-3.2.0.jar" \
  "$MAVEN/com/ververica/flink-sql-connector-postgres-cdc/3.2.0/flink-sql-connector-postgres-cdc-3.2.0.jar" && echo "[OK] flink-postgres-cdc"

wget -q -O "$FLINK_EXTRA/hudi-flink1.19-bundle-1.0.2.jar" \
  "$MAVEN/org/apache/hudi/hudi-flink1.19-bundle/1.0.2/hudi-flink1.19-bundle-1.0.2.jar" && echo "[OK] hudi-flink-bundle"

wget -q -O "$FLINK_EXTRA/flink-shaded-hadoop3-uber-blink-3.7.0.jar" \
  "$MAVEN/com/alibaba/blink/flink-shaded-hadoop3-uber/blink-3.7.0/flink-shaded-hadoop3-uber-blink-3.7.0.jar" && echo "[OK] flink-shaded-hadoop3"

wget -q -O "$FLINK_EXTRA/hive-standalone-metastore-3.1.3.jar" \
  "$MAVEN/org/apache/hive/hive-standalone-metastore/3.1.3/hive-standalone-metastore-3.1.3.jar" && echo "[OK] hive-standalone-metastore"

# fb303: Hive Metastore Thrift 连接所需（hive-standalone-metastore 不包含此依赖）
wget -q -O "$FLINK_EXTRA/libfb303-0.9.3.jar" \
  "$MAVEN/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar" && echo "[OK] libfb303"

wget -q -O "$FLINK_EXTRA/mysql-connector-j-8.4.0.jar" \
  "$MAVEN/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar" && echo "[OK] mysql-connector (flink)"

wget -q -O "$FLINK_EXTRA/postgresql-42.7.3.jar" \
  "$MAVEN/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" && echo "[OK] postgresql"

echo "===== 下载 Spark 扩展 jar 到 $SPARK_DIR ====="

wget -q -O "$SPARK_DIR/paimon-spark-3.5-0.9.0.jar" \
  "$MAVEN/org/apache/paimon/paimon-spark-3.5/0.9.0/paimon-spark-3.5-0.9.0.jar" && echo "[OK] paimon-spark"

wget -q -O "$SPARK_DIR/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" && echo "[OK] iceberg-spark"

wget -q -O "$SPARK_DIR/hudi-spark3.5-bundle_2.12-1.0.2.jar" \
  "$MAVEN/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.0.2/hudi-spark3.5-bundle_2.12-1.0.2.jar" && echo "[OK] hudi-spark-bundle"

# Spark 离线作业 JDBC 驱动（直连 MySQL/Postgres 做全量回灌）
wget -q -O "$SPARK_DIR/mysql-connector-j-8.4.0.jar" \
  "$MAVEN/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar" && echo "[OK] mysql-connector (spark)"

wget -q -O "$SPARK_DIR/postgresql-42.7.3.jar" \
  "$MAVEN/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" && echo "[OK] postgresql (spark)"

# Spark 离线作业 MongoDB 连接器（读取 MongoDB 数据）
wget -q -O "$SPARK_DIR/mongo-spark-connector_2.12-10.4.0.jar" \
  "$MAVEN/org/mongodb/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar" && echo "[OK] mongo-spark-connector"

wget -q -O "$SPARK_DIR/bson-5.2.0.jar" \
  "$MAVEN/org/mongodb/bson/5.2.0/bson-5.2.0.jar" && echo "[OK] bson"

wget -q -O "$SPARK_DIR/mongodb-driver-core-5.2.0.jar" \
  "$MAVEN/org/mongodb/mongodb-driver-core/5.2.0/mongodb-driver-core-5.2.0.jar" && echo "[OK] mongodb-driver-core"

wget -q -O "$SPARK_DIR/mongodb-driver-sync-5.2.0.jar" \
  "$MAVEN/org/mongodb/mongodb-driver-sync/5.2.0/mongodb-driver-sync-5.2.0.jar" && echo "[OK] mongodb-driver-sync"

echo "===== 下载 MySQL 驱动到 $MYSQL_DIR ====="

wget -q -O "$MYSQL_DIR/mysql-connector-j-8.4.0.jar" \
  "$MAVEN/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar" && echo "[OK] mysql-connector (mysql)"

echo "===== 下载 hive-exec 到 $BACKUP（需特殊处理，见 3.3）====="

wget -q -O "$BACKUP/hive-exec-3.1.3.jar" \
  "$MAVEN/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar" && echo "[OK] hive-exec"

echo "===== 全部下载完成 ====="
SCRIPT
chmod +x lib/backup/download-commands.sh
bash lib/backup/download-commands.sh
```

### 3.2 下载 Hadoop 3.3.6（覆盖 Hive 镜像内置的 3.1.0）

```bash
cd lib
wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
rm -f hadoop-3.3.6.tar.gz
cd ..

# 将自定义 Hadoop 配置复制到 hadoop-3.3.6/etc/hadoop/
# （hive 服务挂载 ./lib/hadoop-3.3.6:/opt/hadoop:ro，需确保配置在内）
cp conf/hadoop/core-site.xml conf/hadoop/hdfs-site.xml \
   conf/hadoop/yarn-site.xml conf/hadoop/mapred-site.xml \
   lib/hadoop-3.3.6/etc/hadoop/
echo "[OK] Hadoop 配置已同步到 lib/hadoop-3.3.6/etc/hadoop/"
```

### 3.3 处理 Hudi 特殊依赖（关键步骤）

Hudi 1.0.2 + Flink 1.19.1 存在 Calcite 类加载冲突和 Parquet 版本冲突，需特殊处理：

**步骤 A：修改 hive-exec-3.1.3.jar（移除旧版 Calcite optimizer 和旧版 Parquet）**

```bash
# 在项目根目录下执行
PROJECT_ROOT="$(pwd)"
cd /tmp
rm -rf hive-exec-modified && mkdir hive-exec-modified && cd hive-exec-modified
unzip -q -o "$PROJECT_ROOT/lib/backup/hive-exec-3.1.3.jar"
# 移除旧版 Calcite optimizer 包
rm -rf org/apache/hadoop/hive/ql/optimizer/calcite
find org/apache/hadoop/hive/ql/parse -name "CalcitePlanner*" -delete
# 移除旧版 Parquet 1.10.0（Hudi 自带 Parquet 1.13.1）
rm -rf org/apache/parquet
# 重新打包
zip -r -q /tmp/hive-exec-3.1.3-modified.jar .
cp /tmp/hive-exec-3.1.3-modified.jar "$PROJECT_ROOT/lib/flink/extra/hive-exec-3.1.3.jar"
cd "$PROJECT_ROOT"
echo "[OK] hive-exec-3.1.3.jar 已处理"
```

**步骤 B：生成 calcite-stub.jar（最小化 Calcite stub 类）**

宿主可能没有 JDK，使用 Docker 中的 Java 编译：

```bash
mkdir -p /tmp/calcite-stub-src/org/apache/calcite/plan
mkdir -p /tmp/calcite-stub-src/org/apache/calcite/plan/hep
mkdir -p /tmp/calcite-stub-src/org/apache/hadoop/hive/ql/optimizer/calcite/reloperators
mkdir -p /tmp/calcite-stub-src/org/apache/hadoop/hive/ql/optimizer/calcite/rules

cat > /tmp/calcite-stub-src/org/apache/calcite/plan/RelOptRule.java << 'EOF'
package org.apache.calcite.plan;
public abstract class RelOptRule {}
EOF

cat > /tmp/calcite-stub-src/org/apache/calcite/plan/hep/HepProgram.java << 'EOF'
package org.apache.calcite.plan.hep;
public class HepProgram {}
EOF

cat > /tmp/calcite-stub-src/org/apache/calcite/plan/hep/HepProgramBuilder.java << 'EOF'
package org.apache.calcite.plan.hep;
public class HepProgramBuilder { public HepProgram build() { return new HepProgram(); } }
EOF

cat > /tmp/calcite-stub-src/org/apache/hadoop/hive/ql/optimizer/calcite/reloperators/RelOptHiveTable.java << 'EOF'
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;
import org.apache.calcite.plan.RelOptRule;
public class RelOptHiveTable extends RelOptRule {}
EOF

cat > /tmp/calcite-stub-src/org/apache/hadoop/hive/ql/optimizer/calcite/rules/HiveAugmentMaterializationRule.java << 'EOF'
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;
import org.apache.calcite.plan.RelOptRule;
public class HiveAugmentMaterializationRule extends RelOptRule {}
EOF

docker run --rm \
  -v /tmp/calcite-stub-src:/src \
  -v "$(pwd)/lib/flink/extra:/out" \
  -w /src eclipse-temurin:11-jdk \
  bash -c 'javac -cp /out/hive-exec-3.1.3.jar $(find . -name "*.java") && jar cf /out/calcite-stub.jar $(find . -name "*.class")'

echo "[OK] calcite-stub.jar 已生成"
```

**步骤 C：生成 hive-calcite-stub.jar（从原始 hive-exec 提取 calcite optimizer 类）**

步骤 A 移除了 hive-exec 中的 `org/apache/hadoop/hive/ql/optimizer/calcite/` 包，
但 Hudi 的 Hive Catalog 依赖该包中的类（如 `HiveAugmentMaterializationRule`，
实际包路径为 `rules.views`，与步骤 B 的 stub 包路径不同）。
需从备份的原始 hive-exec 中提取这些类单独打包：

```bash
mkdir -p /tmp/hive-calcite-stub
cd /tmp/hive-calcite-stub
python3 -c "
import zipfile
z = zipfile.ZipFile('$(pwd)/lib/backup/hive-exec-3.1.3.jar')
for name in z.namelist():
    if name.startswith('org/apache/hadoop/hive/ql/optimizer/calcite/'):
        z.extract(name, '.')
print('Extracted calcite optimizer classes')
"
python3 -c "
import zipfile, os
with zipfile.ZipFile('$(pwd)/lib/flink/extra/hive-calcite-stub.jar', 'w', zipfile.ZIP_DEFLATED) as z:
    for root, dirs, files in os.walk('.'):
        for f in files:
            if f.endswith('.class'):
                path = os.path.join(root, f)
                arc = os.path.relpath(path, '.')
                z.write(path, arc)
print('hive-calcite-stub.jar created')
"
cd "$(pwd)"
echo "[OK] hive-calcite-stub.jar 已生成"
```

---

## 四、创建配置文件

### 4.1 镜像版本配置

> 以下镜像使用华为云 SWR 镜像源，国内可直接拉取。如需更换为 Docker Hub 官方镜像，去掉 `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/` 前缀即可。

```bash
cat > .env << 'EOF'
KAFKA_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/kafka:3.9.0
FLINK_BASE_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/flink:1.19.1-scala_2.12-java11
FLINK_IMAGE=lakehouse-flink:1.19.1
DORIS_FE_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/selectdb/doris.fe-ubuntu:2.1.7
DORIS_BE_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/selectdb/doris.be-ubuntu:2.1.7
MYSQL_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/mysql:8.0.46
SPARK_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/spark:3.5.6
HADOOP_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/hadoop:3.3.6
HIVE_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/hive:3.1.3
POSTGRES_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/postgres:16.4
MONGODB_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/mongo:7.0
TRINO_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/trinodb/trino:482
EOF
```

### 4.2 Hadoop 配置

```bash
cat > conf/hadoop/core-site.xml << 'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/data/hadoop/tmp</value>
  </property>
  <property>
    <name>fs.hdfs.impl.disable.cache</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>false</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>authentication</value>
  </property>
  <property>
    <name>hadoop.security.auth_to_local</name>
    <value>RULE:[2:$1@$0](nn@.*)s/.*/hdfs/
RULE:[2:$1@$0](dn@.*)s/.*/hdfs/
RULE:[2:$1@$0](hive@.*)s/.*/hive/
RULE:[2:$1@$0](flink@.*)s/.*/flink/
RULE:[2:$1@$0](spark@.*)s/.*/spark/
RULE:[2:$1@$0](kafka@.*)s/.*/kafka/
RULE:[2:$1@$0](rm@.*)s/.*/yarn/
RULE:[2:$1@$0](nm@.*)s/.*/yarn/
RULE:[1:$1@$0](lakehouse@.*)s/.*/lakehouse/
DEFAULT</value>
  </property>
  <property>
    <name>mapreduce.job.kerberos.principal</name>
    <value>hive</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
  <property>
    <name>mapreduce.cluster.local.dir</name>
    <value>/tmp/mapred/local</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hive.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hive.groups</name>
    <value>*</value>
  </property>
</configuration>
EOF
```

```bash
cat > conf/hadoop/hdfs-site.xml << 'EOF'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///data/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>namenode:9000</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>0.0.0.0:9870</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>nn/namenode.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>dfs.namenode.keytab.file</name>
    <value>/etc/security/keytabs/nn.service.keytab</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.internal.spnego.principal</name>
    <value>HTTP/namenode.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>dfs.web.authentication.kerberos.principal</name>
    <value>HTTP/namenode.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>dfs.web.authentication.kerberos.keytab</name>
    <value>/etc/security/keytabs/nn.service.keytab</value>
  </property>
  <property>
    <name>dfs.datanode.kerberos.principal</name>
    <value>dn/datanode.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>dfs.datanode.keytab.file</name>
    <value>/etc/security/keytabs/dn.service.keytab</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:1019</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:1006</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:9864</value>
  </property>
  <property>
    <name>dfs.block.access.token.enable</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.datanode.use.jsvc.allowed</name>
    <value>false</value>
  </property>
  <property>
    <name>ignore.secure.ports.for.testing</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.http.policy</name>
    <value>HTTP_ONLY</value>
  </property>
</configuration>
EOF
```

```bash
cat > conf/hadoop/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.principal</name>
    <value>hdfs/namenode.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
</configuration>
EOF
```

```bash
cat > conf/hadoop/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
  <property>
    <name>mapreduce.cluster.local.dir</name>
    <value>/tmp/mapred/local</value>
  </property>
  <property>
    <name>mapreduce.job.kerberos.principal</name>
    <value>hive</value>
  </property>
</configuration>
EOF
```

### 4.3 Hive 配置

```bash
cat > conf/hive/hive-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://mysql:3306/hive_metastore?createDatabaseIfNotExist=true&amp;useSSL=false&amp;allowPublicKeyRetrieval=true&amp;serverTimezone=UTC&amp;characterEncoding=UTF-8</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://namenode:9000/user/hive/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore:9083</value>
  </property>
  <property>
    <name>hive.metastore.kerberos.keytab.file</name>
    <value>/etc/security/keytabs/hive.service.keytab</value>
  </property>
  <property>
    <name>hive.metastore.kerberos.principal</name>
    <value>hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>hive.metastore.sasl.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.server2.thrift.port</name>
    <value>21066</value>
  </property>
  <property>
    <name>hive.server2.webui.port</name>
    <value>21067</value>
  </property>
  <property>
    <name>hive.server2.transport.mode</name>
    <value>binary</value>
  </property>
  <property>
    <name>hive.server2.authentication</name>
    <value>KERBEROS</value>
  </property>
  <property>
    <name>hive.server2.authentication.kerberos.keytab</name>
    <value>/etc/security/keytabs/hive.service.keytab</value>
  </property>
  <property>
    <name>hive.server2.authentication.kerberos.principal</name>
    <value>hive/hiveserver.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>hive.server2.webui.spnego.keytab</name>
    <value>/etc/security/keytabs/hive.service.keytab</value>
  </property>
  <property>
    <name>hive.server2.webui.spnego.principal</name>
    <value>HTTP/hiveserver.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
  </property>
  <property>
    <name>hive.exec.scratchdir</name>
    <value>/tmp/hive</value>
  </property>
  <property>
    <name>hive.exec.local.scratchdir</name>
    <value>/tmp/hive-local-scratch</value>
  </property>
  <property>
    <name>hive.downloaded.resources.dir</name>
    <value>/tmp/hive-resources</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>metastore.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.exec.dynamic.partition</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.exec.dynamic.partition.mode</name>
    <value>nonstrict</value>
  </property>
</configuration>
EOF
```

### 4.4 Kerberos 配置

```bash
cat > conf/kerberos/krb5.conf << 'EOF'
[libdefaults]
    default_realm = LAKEHOUSE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    udp_preference_limit = 1
    default_ccache_name = FILE:/tmp/krb5cc_%{uid}

[realms]
    LAKEHOUSE.COM = {
        kdc = kerberos
        admin_server = kerberos
        default_domain = lakehouse.com
    }

[domain_realm]
    .lakehouse.com = LAKEHOUSE.COM
    lakehouse.com = LAKEHOUSE.COM
    namenode = LAKEHOUSE.COM
    datanode = LAKEHOUSE.COM
    hivemetastore = LAKEHOUSE.COM
    hiveserver = LAKEHOUSE.COM
    flink-jobmanager = LAKEHOUSE.COM
    flink-taskmanager = LAKEHOUSE.COM
    spark-master = LAKEHOUSE.COM
    spark-worker = LAKEHOUSE.COM
    kafka = LAKEHOUSE.COM
    kerberos = LAKEHOUSE.COM

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmind.log
EOF
```

```bash
cat > conf/kerberos/flink-client-jaas.conf << 'EOF'
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/etc/security/keytabs/flink.service.keytab"
  principal="flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM"
  storeKey=true
  useTicketCache=true
  doNotPrompt=true;
};
EOF
```

```bash
cat > conf/kerberos/iceberg-rest-jaas.conf << 'EOF'
com.sun.security.jgss.initiate {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  useTicketCache=false
  doNotPrompt=true
  keyTab="/etc/security/keytabs/iceberg.service.keytab"
  principal="iceberg/iceberg-rest.lakehouse.com@LAKEHOUSE.COM";
};
EOF
```

### 4.5 Kafka 配置

```bash
cat > conf/kafka/kafka_server_jaas.conf << 'EOF'
KafkaServer {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/kafka.service.keytab"
    principal="kafka/kafka.lakehouse.com@LAKEHOUSE.COM"
    storeKey=true;
};

Client {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/kafka.service.keytab"
    principal="kafka/kafka.lakehouse.com@LAKEHOUSE.COM"
    storeKey=true;
};
EOF
```

### 4.6 Flink 配置

```bash
cat > conf/flink/flink-conf.yaml << 'EOF'
jobmanager.rpc.address: flink-jobmanager
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 1024m

taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 4096m
taskmanager.memory.managed.fraction: 0.4

rest.address: flink-jobmanager
rest.port: 8081

state.backend: rocksdb
state.backend.incremental: true
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 10min
execution.checkpointing.min-pause: 30s

classloader.resolve-order: parent-first
classloader.parent-first-patterns.additional: org.apache.hadoop.;org.apache.hive.;org.apache.iceberg.;org.apache.paimon.

env.java.opts.all: --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED

table.exec.resource.default-parallelism: 1
table.planner: blink

table.catalog-store.kind: file
table.catalog-store.file.path: /opt/flink/conf/catalog-store

security.kerberos.login.use-ticket-cache: false
security.kerberos.login.keytab: /etc/security/keytabs/flink.service.keytab
security.kerberos.login.principal: flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM
security.kerberos.login.contexts: Client,KafkaClient
security.delegation.tokens.enabled: false
EOF
```

```bash
cat > conf/flink/sql-client-defaults.yaml << 'EOF'
execution:
  planner: blink
  type: streaming
  parallelism: 4
  result-mode: tableau
EOF
```

```bash
cat > conf/flink/sql-client-init.sql << 'EOF'
-- Iceberg REST Catalog
DROP CATALOG IF EXISTS iceberg_catalog;
CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest:8181',
  'warehouse'='hdfs://namenode:9000/user/iceberg'
);

-- Paimon Catalog（基于 Hive Metastore）
DROP CATALOG IF EXISTS paimon_catalog;
CREATE CATALOG paimon_catalog WITH (
  'type'='paimon',
  'metastore'='hive',
  'uri'='thrift://hive-metastore:9083',
  'warehouse'='hdfs://namenode:9000/user/paimon',
  'hive-conf-dir'='/opt/flink/conf',
  'hive.metastore.kerberos.principal'='hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM'
);

-- Hudi Catalog（基于 Hive Metastore）
DROP CATALOG IF EXISTS hudi_catalog;
CREATE CATALOG hudi_catalog WITH (
  'type'='hudi',
  'catalog.path'='hdfs://namenode:9000/user/hudi/catalog',
  'mode'='hms',
  'hive.conf.dir'='/opt/flink/conf'
);

-- Hive Catalog
DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG hive_catalog WITH (
  'type'='hive',
  'hive-conf-dir'='/opt/flink/conf'
);

USE CATALOG iceberg_catalog;
EOF
```

```bash
cat > conf/flink/sql-client-entrypoint.sh << 'EOF'
#!/usr/bin/env bash
set -e
echo "[entrypoint] Running kinit with flink keytab..."
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM \
  && echo "[entrypoint] kinit succeeded" \
  || echo "[entrypoint] WARNING: kinit failed"
exec "$@"
EOF
chmod +x conf/flink/sql-client-entrypoint.sh
```

> **注意**：Flink 1.19 SQL Client 不支持通过配置文件（`table.catalogs`）定义 catalog，
> 必须通过 `sql-client-init.sql` 中的 `CREATE CATALOG` DDL 创建，创建后会持久化到
> `table.catalog-store.file.path` 目录。`conf.d/catalogs.yaml` 方式无效。

### 4.7 Spark 配置

```bash
cat > conf/spark/spark-defaults.conf << 'EOF'
spark.driver.extraClassPath /opt/bitnami/spark/extra-jars/paimon-spark-3.5-0.9.0.jar:/opt/bitnami/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar:/opt/bitnami/spark/extra-jars/hudi-spark3.5-bundle_2.12-1.0.2.jar:/opt/bitnami/spark/extra-jars/mysql-connector-j-8.4.0.jar:/opt/bitnami/spark/extra-jars/postgresql-42.7.3.jar:/opt/bitnami/spark/extra-jars/mongo-spark-connector_2.12-10.4.0.jar:/opt/bitnami/spark/extra-jars/bson-5.2.0.jar:/opt/bitnami/spark/extra-jars/mongodb-driver-core-5.2.0.jar:/opt/bitnami/spark/extra-jars/mongodb-driver-sync-5.2.0.jar
spark.executor.extraClassPath /opt/bitnami/spark/extra-jars/paimon-spark-3.5-0.9.0.jar:/opt/bitnami/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar:/opt/bitnami/spark/extra-jars/hudi-spark3.5-bundle_2.12-1.0.2.jar:/opt/bitnami/spark/extra-jars/mysql-connector-j-8.4.0.jar:/opt/bitnami/spark/extra-jars/postgresql-42.7.3.jar:/opt/bitnami/spark/extra-jars/mongo-spark-connector_2.12-10.4.0.jar:/opt/bitnami/spark/extra-jars/bson-5.2.0.jar:/opt/bitnami/spark/extra-jars/mongodb-driver-core-5.2.0.jar:/opt/bitnami/spark/extra-jars/mongodb-driver-sync-5.2.0.jar
spark.executor.extraJavaOptions -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false
spark.driver.extraJavaOptions -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.extensions org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.spark.sql.hudi.HoodieSparkSessionExtension
spark.sql.catalog.paimon org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.metastore hive
spark.sql.catalog.paimon.uri thrift://hive-metastore:9083
spark.sql.catalog.paimon.warehouse hdfs://namenode:9000/user/paimon
spark.sql.catalog.paimon.hive-conf-dir /opt/bitnami/spark/conf
spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type rest
spark.sql.catalog.iceberg.uri http://iceberg-rest:8181
spark.sql.catalog.iceberg.warehouse hdfs://namenode:9000/user/iceberg
spark.sql.catalog.hudi org.apache.spark.sql.hudi.catalog.HoodieCatalog
spark.sql.catalog.hudi.warehouse hdfs://namenode:9000/user/hudi
spark.kerberos.keytab /etc/security/keytabs/spark.service.keytab
spark.kerberos.principal spark/spark-master.lakehouse.com@LAKEHOUSE.COM
spark.kerberos.relogin.enabled true
spark.executorEnv.KRB5CCNAME /tmp/krb5cc_spark
spark.yarn.appMasterEnv.KRB5CCNAME /tmp/krb5cc_spark
spark.hadoop.hadoop.security.authentication kerberos
spark.hadoop.dfs.namenode.kerberos.principal nn/namenode.lakehouse.com@LAKEHOUSE.COM
spark.hadoop.dfs.namenode.kerberos.internal.spnego.principal HTTP/namenode.lakehouse.com@LAKEHOUSE.COM
spark.hadoop.mapreduce.job.kerberos.principal spark/spark-master.lakehouse.com@LAKEHOUSE.COM
EOF
```

> **说明**：
> - JDBC 驱动（mysql/postgresql）用于 Spark 离线作业直连源库做全量回灌
> - Paimon catalog 使用 Hive Metastore，与 Flink 共享元数据
> - Hudi 表通过 Hive Metastore（spark_catalog）访问，catalog name 为 `cdc_demo.users`
> - KRB5CCNAME 指向共享票据缓存 `/tmp/krb5cc_spark`，由入口脚本 kinit 生成

```bash
cat > conf/spark/spark-entrypoint.sh << 'EOF'
#!/bin/bash
set -e

if ! id spark &>/dev/null; then
    echo "spark:x:1001:0:Spark:/home/spark:/bin/bash" >> /etc/passwd
    mkdir -p /home/spark
    chown 1001:0 /home/spark
fi
export HOME=/home/spark
export SPARK_SUBMIT_OPTS="-Duser.home=/home/spark"

# Kerberos 认证：使用 spark 服务 keytab 获取 TGT
export KRB5CCNAME=/tmp/krb5cc_spark
if [ -f /etc/security/keytabs/spark.service.keytab ]; then
    kinit -kt /etc/security/keytabs/spark.service.keytab \
        spark/spark-master.lakehouse.com@LAKEHOUSE.COM \
        -c $KRB5CCNAME 2>/dev/null && echo "[spark-entrypoint] Kerberos ticket acquired" || \
        echo "[spark-entrypoint] WARN: kinit failed"
    chmod 644 $KRB5CCNAME 2>/dev/null || true
fi

exec /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh
EOF
chmod +x conf/spark/spark-entrypoint.sh
```

Spark 镜像需基于 bitnami/spark 构建，安装 `krb5-user`：

```bash
cat > build/spark/Dockerfile << 'EOF'
ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}
USER root
RUN apt-get update -qq && \
    apt-get install -y -qq krb5-user libpam-krb5 2>&1 | grep -v "warning:" || true && \
    rm -rf /var/lib/apt/lists/*
COPY conf/spark/spark-entrypoint.sh /opt/bitnami/spark/conf/spark-entrypoint.sh
RUN chmod +x /opt/bitnami/spark/conf/spark-entrypoint.sh
COPY lib/spark/*.jar /opt/bitnami/spark/extra-jars/
EOF
```

> **注意**：所有配置（spark-defaults.conf、hive-site.xml、krb5.conf、hadoop conf）均不 COPY 进镜像，
> 而是通过 volume 挂载（见下方 docker-compose），这样修改任何配置后只需重启 Spark 容器，无需重建镜像。

> **一致性原则**：
> - **静态文件**（entrypoint、扩展 jar）通过 `COPY` 内置到镜像
> - **所有配置**（spark-defaults.conf、hive-site.xml、krb5.conf、hadoop conf）通过 volume 挂载
> - **密钥/开发目录**（keytabs、sql、scripts）通过 volume 挂载

`.env` 中配置：
```
SPARK_IMAGE=lakehouse-spark:3.5.6
SPARK_BASE_IMAGE=swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/spark:3.5.6
```

> **注意**：本环境 Spark 使用 `local[*]` 模式运行以适配 Kerberos 认证。集群模式下 executor 需额外配置 HDFS delegation token。

### 4.8 Trino 配置

```bash
mkdir -p conf/trino/catalog

cat > conf/trino/config.properties << 'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://trino:8080
query.max-memory=4GB
query.max-memory-per-node=2GB
EOF

cat > conf/trino/jvm.config << 'EOF'
-server
-Xmx4G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:+UseG1GC
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
-Djava.security.krb5.conf=/etc/krb5.conf
EOF

cat > conf/trino/node.properties << 'EOF'
node.environment=lakehouse
node.id=trino-coordinator
node.data-dir=/data/trino
EOF

# Hive catalog (HMS + HDFS with Kerberos)
cat > conf/trino/catalog/hive.properties << 'EOF'
connector.name=hive
fs.hadoop.enabled=true
hive.config.resources=/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml
hive.metastore.uri=thrift://hive-metastore:9083
hive.metastore.authentication.type=KERBEROS
hive.metastore.service.principal=hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM
hive.metastore.client.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.metastore.client.keytab=/etc/security/keytabs/trino.service.keytab
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.impersonation.enabled=false
hive.hdfs.trino.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.hdfs.trino.keytab=/etc/security/keytabs/trino.service.keytab
EOF

# Iceberg REST catalog
cat > conf/trino/catalog/iceberg.properties << 'EOF'
connector.name=iceberg
fs.hadoop.enabled=true
hive.config.resources=/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.trino.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.hdfs.trino.keytab=/etc/security/keytabs/trino.service.keytab
EOF

# Hudi catalog (HMS-based)
cat > conf/trino/catalog/hudi.properties << 'EOF'
connector.name=hudi
fs.hadoop.enabled=true
hive.config.resources=/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml
hive.metastore.uri=thrift://hive-metastore:9083
hive.metastore.authentication.type=KERBEROS
hive.metastore.service.principal=hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM
hive.metastore.client.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.metastore.client.keytab=/etc/security/keytabs/trino.service.keytab
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.trino.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.hdfs.trino.keytab=/etc/security/keytabs/trino.service.keytab
EOF
```

> **注意**：Trino 482 要求 `fs.hadoop.enabled=true` 才能启用 HDFS 访问，否则所有 `hive.hdfs.*` 属性会被忽略。Trino 镜像以非 root 用户运行，keytab 必须为 644 权限。

### 4.9 Doris 启动脚本

```bash
cat > conf/doris/fe-start.sh << 'EOF'
#!/bin/bash
bash /usr/local/bin/init_fe.sh &
FE_PID=$!

echo "Waiting for Doris FE to start on port 9030..."
for i in $(seq 1 90); do
    if mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" &>/dev/null; then
        echo "Doris FE is ready (attempt $i)."
        break
    fi
    sleep 2
done

mysql -h 127.0.0.1 -P 9030 -u root -e "SET PASSWORD FOR 'root' = PASSWORD('');" 2>/dev/null
echo "[OK] Root password set to empty (BE requires passwordless root access)"

mysql -h 127.0.0.1 -P 9030 -u root -e "
CREATE USER IF NOT EXISTS 'lakehouse'@'%' IDENTIFIED BY 'lakehouse123';
GRANT ALL PRIVILEGES ON *.* TO 'lakehouse'@'%';
FLUSH PRIVILEGES;
" 2>/dev/null
echo "[OK] User 'lakehouse' created with password 'lakehouse123'"

wait $FE_PID
EOF
chmod +x conf/doris/fe-start.sh
```

---

## 五、创建构建文件

### 5.1 Flink 镜像 Dockerfile

```bash
cat > build/flink/Dockerfile << 'EOF'
ARG FLINK_BASE_IMAGE
FROM ${FLINK_BASE_IMAGE}

RUN apt-get update -qq && \
    apt-get install -y -qq krb5-user && \
    rm -rf /var/lib/apt/lists/*

COPY lib/flink/extra/*.jar /opt/flink/lib/extra/
EOF
```

> **说明**：Flink 扩展 jar（含 CDC fat jar、Iceberg、Hudi、Paimon、Hive、Hadoop 等）在镜像构建时通过 `COPY` 内置到 `/opt/flink/lib/extra/`。docker-compose.yaml 中**不再**挂载 `./lib/flink/extra`，修改 jar 后需重新 `docker compose build` 重建镜像。`FLINK_CLASSPATH=/opt/flink/lib/extra/*` 使这些 jar 生效。

> **注意**：需创建 `.dockerignore` 排除 `data/` 等目录，否则构建时可能因 HDFS 数据目录权限导致失败：

```bash
cat > .dockerignore << 'EOF'
data/
logs/
*.log
.git/
docs/
EOF
```

### 5.2 Kerberos KDC 镜像

```bash
cat > build/kerberos/Dockerfile << 'EOF'
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV KRB5_REALM=LAKEHOUSE.COM
ENV KRB5_KDC=kerberos
ENV KRB5_ADMIN_SERVER=kerberos

RUN apt-get update && \
    apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-user && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY kdc-init.sh /usr/local/bin/kdc-init.sh
RUN chmod +x /usr/local/bin/kdc-init.sh

EXPOSE 88 749

CMD ["/usr/local/bin/kdc-init.sh"]
EOF
```

```bash
cat > build/kerberos/kdc-init.sh << 'EOF'
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
EOF
chmod +x build/kerberos/kdc-init.sh
```

### 5.3 Iceberg REST Catalog 镜像

```bash
cat > build/iceberg-rest/Dockerfile << 'EOF'
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/iceberg-rest-fixture:1.10.1

USER root

RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends krb5-user && \
    rm -rf /var/lib/apt/lists/*

COPY IcebergRestLauncher.java /opt/iceberg/src/IcebergRestLauncher.java
COPY entrypoint.sh /opt/iceberg/bin/entrypoint.sh
RUN chmod +x /opt/iceberg/bin/entrypoint.sh

ENTRYPOINT ["/opt/iceberg/bin/entrypoint.sh"]
EOF
```

```bash
cat > build/iceberg-rest/entrypoint.sh << 'EOF'
#!/bin/bash
set -e

kinit -kt /etc/security/keytabs/iceberg.service.keytab iceberg/iceberg-rest.lakehouse.com@LAKEHOUSE.COM
echo "Kerberos ticket obtained for iceberg/iceberg-rest.lakehouse.com@LAKEHOUSE.COM"

exec java \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -cp "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/opt/iceberg/libs/mysql-connector-j-8.4.0.jar:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*" \
  /opt/iceberg/src/IcebergRestLauncher.java "$@"
EOF
chmod +x build/iceberg-rest/entrypoint.sh
```

```bash
cat > build/iceberg-rest/IcebergRestLauncher.java << 'EOF'
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.rest.RESTCatalogServer;

public class IcebergRestLauncher {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);

        String principal = "iceberg/iceberg-rest.lakehouse.com@LAKEHOUSE.COM";
        String keytab = "/etc/security/keytabs/iceberg.service.keytab";
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        System.out.println("Kerberos login successful: " + UserGroupInformation.getCurrentUser());

        RESTCatalogServer.main(args);
    }
}
EOF
```

### 5.4 辅助脚本

```bash
cat > scripts/lakehouse_status.sh << 'EOF'
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
printf "MySQL   Spark jar: "; [ -f "$SPARK_LIB/mysql-connector-j-8.4.0.jar" ] && echo OK || echo MISSING
printf "PG      Spark jar: "; [ -f "$SPARK_LIB/postgresql-42.7.3.jar" ] && echo OK || echo MISSING
printf "Mongo   Spark jar: "; [ -f "$SPARK_LIB/mongo-spark-connector_2.12-10.4.0.jar" ] && echo OK || echo MISSING
printf "BSON    Spark jar: "; [ -f "$SPARK_LIB/bson-5.2.0.jar" ] && echo OK || echo MISSING
EOF
chmod +x scripts/lakehouse_status.sh
```

```bash
cat > scripts/flink-sql.sh << 'EOF'
#!/usr/bin/env bash
set -e
docker exec -it flink-sql-client bash -c '
export FLINK_CLASSPATH=/opt/flink/lib/extra/*
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
/opt/flink/bin/sql-client.sh -i /opt/flink/conf/sql-client-init.sql
'
EOF
chmod +x scripts/flink-sql.sh
```

---

## 六、创建 docker-compose.yaml

```bash
cat > docker-compose.yaml << 'COMPOSE'
services:
  namenode:
    image: ${HADOOP_IMAGE}
    container_name: namenode
    hostname: namenode.lakehouse.com
    command: ["hdfs", "namenode"]
    environment:
      - HADOOP_HOME=/opt/hadoop
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.41
    ports: ["9870:9870"]
    volumes:
      - ./conf/hadoop:/opt/hadoop/etc/hadoop
      - ./data/hadoop/namenode:/data/namenode
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    mem_limit: 1g

  datanode:
    image: ${HADOOP_IMAGE}
    container_name: datanode
    hostname: datanode.lakehouse.com
    user: root
    command: ["hdfs", "datanode"]
    environment:
      - HADOOP_HOME=/opt/hadoop
    depends_on: [namenode]
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.42
    volumes:
      - ./conf/hadoop:/opt/hadoop/etc/hadoop
      - ./data/hadoop/datanode:/data/datanode
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    mem_limit: 1.5g

  hive-metastore:
    image: ${HIVE_IMAGE}
    container_name: hive-metastore
    hostname: hivemetastore.lakehouse.com
    environment:
      SERVICE_NAME: metastore
      IS_RESUME: "true"
      HIVE_CUSTOM_CONF_DIR: /opt/hive/custom-conf
    ports: ["9083:9083"]
    depends_on: [namenode, mysql]
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.43
    volumes:
      - ./lib/mysql/mysql-connector-j-8.4.0.jar:/opt/hive/lib/mysql-connector-j-8.4.0.jar:ro
      - ./lib/empty:/opt/tez:ro
      - ./lib/hadoop-3.3.6:/opt/hadoop:ro
      - ./conf/hive:/opt/hive/custom-conf:ro
      - ./conf/hadoop/core-site.xml:/opt/hive/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hive/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hive/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hive/conf/mapred-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    mem_limit: 1g

  hive-server:
    image: ${HIVE_IMAGE}
    container_name: hive-server
    hostname: hiveserver.lakehouse.com
    environment:
      SERVICE_NAME: hiveserver2
      IS_RESUME: "true"
      HIVE_CUSTOM_CONF_DIR: /opt/hive/custom-conf
    ports: ["21066:21066", "21067:21067"]
    depends_on: [hive-metastore]
    volumes:
      - ./lib/mysql/mysql-connector-j-8.4.0.jar:/opt/hive/lib/mysql-connector-j-8.4.0.jar:ro
      - ./lib/empty:/opt/tez:ro
      - ./lib/hadoop-3.3.6:/opt/hadoop:ro
      - ./conf/hive:/opt/hive/custom-conf:ro
      - ./conf/hadoop/core-site.xml:/opt/hive/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hive/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hive/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hive/conf/mapred-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.44
    mem_limit: 1g

  iceberg-rest:
    image: lakehouse-iceberg-rest:1.10.1
    build:
      context: ./build/iceberg-rest
    container_name: iceberg-rest
    environment:
      CATALOG_NAME: iceberg_rest
      HADOOP_CONF_DIR: /opt/hadoop/etc/hadoop
      CATALOG_WAREHOUSE: hdfs://namenode:9000/user/iceberg
      CATALOG_HADOOP__CONF__DIR: /opt/hadoop/etc/hadoop
      CATALOG_CATALOG__IMPL: org.apache.iceberg.jdbc.JdbcCatalog
      CATALOG_URI: jdbc:mysql://mysql:3306/iceberg_catalog?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=UTF-8
      CATALOG_JDBC_USER: iceberg
      CATALOG_JDBC_PASSWORD: iceberg
    ports: ["8181:8181"]
    volumes:
      - ./lib/mysql/mysql-connector-j-8.4.0.jar:/opt/iceberg/libs/mysql-connector-j-8.4.0.jar:ro
      - ./lib/hadoop-3.3.6/share/hadoop/common:/opt/hadoop/share/hadoop/common:ro
      - ./lib/hadoop-3.3.6/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/kerberos/iceberg-rest-jaas.conf:/opt/iceberg/conf/iceberg-rest-jaas.conf:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.50
    depends_on: [namenode, mysql]
    mem_limit: 1g

  kafka:
    image: ${KAFKA_IMAGE}
    container_name: kafka
    hostname: kafka.lakehouse.com
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: SASL_PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: SASL_PLAINTEXT://kafka.lakehouse.com:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SASL_PLAINTEXT
      KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: GSSAPI
      KAFKA_SASL_ENABLED_MECHANISMS: GSSAPI
      KAFKA_SASL_KERBEROS_SERVICE_NAME: kafka
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OPTS: "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf -Dsun.security.krb5.debug=false"
    ports:
      - "9092:9092"
    volumes:
      - ./data/kafka:/var/lib/kafka/data
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./conf/kafka/kafka_server_jaas.conf:/etc/kafka/kafka_server_jaas.conf:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.2
    mem_limit: 1.5g

  flink-jobmanager:
    image: ${FLINK_IMAGE}
    build:
      context: .
      dockerfile: build/flink/Dockerfile
      args:
        FLINK_BASE_IMAGE: ${FLINK_BASE_IMAGE}
    container_name: flink-jobmanager
    hostname: flink-jobmanager.lakehouse.com
    command: jobmanager
    environment:
      FLINK_CLASSPATH: /opt/flink/lib/extra/*
      HADOOP_CONF_DIR: /opt/hadoop/etc/hadoop
      HADOOP_HOME: /opt/hadoop
    ports:
      - "8081:8081"
    volumes:
      - ./conf/flink/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro
      - ./conf/flink/sql-client-defaults.yaml:/opt/flink/conf/sql-client-defaults.yaml:ro
      - ./conf/flink/sql-client-init.sql:/opt/flink/conf/sql-client-init.sql:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hive/hive-site.xml:/opt/flink/conf/hive-site.xml:ro
      - ./data/flink/catalog-store:/opt/flink/conf/catalog-store
      - ./sql:/opt/flink/sql
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.3
    mem_limit: 4g

  flink-taskmanager:
    image: ${FLINK_IMAGE}
    build:
      context: .
      dockerfile: build/flink/Dockerfile
      args:
        FLINK_BASE_IMAGE: ${FLINK_BASE_IMAGE}
    container_name: flink-taskmanager
    hostname: flink-taskmanager.lakehouse.com
    command: taskmanager
    environment:
      FLINK_CLASSPATH: /opt/flink/lib/extra/*
      HADOOP_CONF_DIR: /opt/hadoop/etc/hadoop
      HADOOP_HOME: /opt/hadoop
    depends_on:
      - flink-jobmanager
    volumes:
      - ./conf/flink/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro
      - ./conf/flink/sql-client-defaults.yaml:/opt/flink/conf/sql-client-defaults.yaml:ro
      - ./conf/flink/sql-client-init.sql:/opt/flink/conf/sql-client-init.sql:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hive/hive-site.xml:/opt/flink/conf/hive-site.xml:ro
      - ./data/flink/catalog-store:/opt/flink/conf/catalog-store
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.4
    mem_limit: 4g

  flink-sql-client:
    image: ${FLINK_IMAGE}
    build:
      context: .
      dockerfile: build/flink/Dockerfile
      args:
        FLINK_BASE_IMAGE: ${FLINK_BASE_IMAGE}
    container_name: flink-sql-client
    hostname: flink-sql-client.lakehouse.com
    entrypoint: ["/opt/flink/conf/sql-client-entrypoint.sh"]
    command: ["sleep", "infinity"]
    environment:
      FLINK_CLASSPATH: /opt/flink/lib/extra/*
      HADOOP_CONF_DIR: /opt/hadoop/etc/hadoop
      HADOOP_HOME: /opt/hadoop
      FLINK_ENV_JAVA_OPTS: "-Djava.security.auth.login.config=/etc/security/flink-client-jaas.conf -Djavax.security.auth.useSubjectCredsOnly=false"
    volumes:
      - ./sql:/opt/flink/sql
      - ./conf/flink/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro
      - ./conf/flink/sql-client-defaults.yaml:/opt/flink/conf/sql-client-defaults.yaml:ro
      - ./conf/flink/sql-client-init.sql:/opt/flink/conf/sql-client-init.sql:ro
      - ./conf/flink/sql-client-entrypoint.sh:/opt/flink/conf/sql-client-entrypoint.sh:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hive/hive-site.xml:/opt/flink/conf/hive-site.xml:ro
      - ./conf/kerberos/flink-client-jaas.conf:/etc/security/flink-client-jaas.conf:ro
      - ./data/flink/catalog-store:/opt/flink/conf/catalog-store
    depends_on:
      - flink-jobmanager
      - flink-taskmanager
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.5
    mem_limit: 1g

  doris-fe:
    image: ${DORIS_FE_IMAGE}
    container_name: doris-fe
    entrypoint: ["/opt/apache-doris/conf/fe-start.sh"]
    environment:
      - FE_SERVERS=fe1:172.30.80.12:9010
      - FE_ID=1
      - JAVA_OPTS=-Xmx2048m -Xms2048m
    ports:
      - "8030:8030"
      - "9030:9030"
    volumes:
      - ./data/doris/fe:/opt/apache-doris/fe/doris-meta
      - ./conf/doris/fe-start.sh:/opt/apache-doris/conf/fe-start.sh:ro
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.12
    mem_limit: 2g

  doris-be:
    image: ${DORIS_BE_IMAGE}
    container_name: doris-be
    environment:
      - FE_SERVERS=fe1:172.30.80.12:9010
      - BE_ADDR=172.30.80.14:9050
    entrypoint: ["bash", "-c", "ulimit -n 655350 && exec bash entry_point.sh"]
    depends_on:
      - doris-fe
    volumes:
      - ./data/doris/be:/opt/apache-doris/be/storage
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.14
    mem_limit: 3g

  mysql:
    image: ${MYSQL_IMAGE}
    container_name: mysql
    command:
      - --binlog-format=ROW
      - --binlog-row-image=FULL
      - --server-id=1
      - --log-bin=mysql-bin
      - --expire-logs-days=7
    environment:
      MYSQL_ROOT_PASSWORD: root123
    ports:
      - "3306:3306"
    volumes:
      - ./data/mysql:/var/lib/mysql
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.21
    mem_limit: 1g

  spark-master:
    image: ${SPARK_IMAGE}
    build:
      context: .
      dockerfile: build/spark/Dockerfile
      args:
        SPARK_BASE_IMAGE: ${SPARK_BASE_IMAGE}
    container_name: spark-master
    hostname: spark-master.lakehouse.com
    user: root
    entrypoint: ["/opt/bitnami/spark/conf/spark-entrypoint.sh"]
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_PORT=7077
      - SPARK_MASTER_WEBUI_PORT=8080
      - HADOOP_HOME=/opt/hadoop
      - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
      - KRB5CCNAME=/tmp/krb5cc_spark
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/spark/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf:ro
      - ./conf/hive/hive-site.xml:/opt/bitnami/spark/conf/hive-site.xml:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./sql:/opt/spark/sql
      - ./scripts:/opt/spark/scripts
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.31
    mem_limit: 1g

  spark-worker:
    image: ${SPARK_IMAGE}
    build:
      context: .
      dockerfile: build/spark/Dockerfile
      args:
        SPARK_BASE_IMAGE: ${SPARK_BASE_IMAGE}
    container_name: spark-worker
    hostname: spark-worker.lakehouse.com
    user: root
    entrypoint: ["/opt/bitnami/spark/conf/spark-entrypoint.sh"]
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_WEBUI_PORT=8081
      - SPARK_WORKER_MEMORY=1024m
      - HADOOP_HOME=/opt/hadoop
      - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
      - KRB5CCNAME=/tmp/krb5cc_spark
    depends_on:
      - spark-master
    ports:
      - "8082:8081"
    volumes:
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/spark/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf:ro
      - ./conf/hive/hive-site.xml:/opt/bitnami/spark/conf/hive-site.xml:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./sql:/opt/spark/sql
      - ./scripts:/opt/spark/scripts
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.32
    mem_limit: 1.5g

  postgres:
    image: ${POSTGRES_IMAGE}
    container_name: postgres
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=10"
      - "-c"
      - "max_wal_senders=10"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres123
      POSTGRES_DB: cdc_demo
    ports:
      - "5432:5432"
    volumes:
      - ./data/postgres:/var/lib/postgresql/data
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.22
    mem_limit: 1g

  mongodb:
    image: ${MONGODB_IMAGE}
    container_name: mongodb
    hostname: mongodb
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: root123
    ports:
      - "27017:27017"
    volumes:
      - ./data/mongodb:/data/db
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.23
    mem_limit: 1g

  trino:
    image: ${TRINO_IMAGE}
    container_name: trino
    hostname: trino.lakehouse.com
    ports:
      - "8085:8080"
    depends_on:
      - namenode
      - hive-metastore
      - iceberg-rest
    volumes:
      - ./conf/trino:/etc/trino:ro
      - ./conf/hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
      - ./data/trino:/data/trino
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.70
    mem_limit: 4g

  kerberos:
    image: lakehouse-kdc:latest
    build:
      context: ./build/kerberos
      dockerfile: Dockerfile
    container_name: kerberos
    environment:
      - KRB5_REALM=LAKEHOUSE.COM
      - KRB5_KDC=kerberos
      - KRB5_ADMIN_PASSWORD=admin123
    ports:
      - "88:88"
      - "749:749"
    volumes:
      - ./data/kerberos:/var/lib/krb5kdc
      - ./conf/kerberos/keytabs:/etc/security/keytabs
    networks:
      lakehouse-net:
        ipv4_address: 172.30.80.60
    mem_limit: 1g

volumes: {}

networks:
  lakehouse-net:
    name: lakehouse-net
    ipam:
      config:
        - subnet: 172.30.80.0/24
COMPOSE
```

---

## 七、构建自定义镜像

```bash
# 构建 Kerberos KDC、Flink、Iceberg REST 三个自定义镜像
docker compose build kerberos flink-jobmanager iceberg-rest
```

---

## 八、启动服务

### 8.1 先启动 Kerberos（生成 keytabs）

```bash
docker compose up -d kerberos

# 等待 KDC 初始化完成（约 10 秒）
sleep 15

# 验证 keytabs 已生成
ls -la conf/kerberos/keytabs/
```

预期看到以下 keytab 文件：
- `nn.service.keytab`、`dn.service.keytab`、`hive.service.keytab`
- `flink.service.keytab`、`kafka.service.keytab`、`spark.service.keytab`
- `iceberg.service.keytab`、`rm.service.keytab`、`nm.service.keytab`
- `lakehouse.keytab`

### 8.2 格式化 HDFS NameNode（首次部署）

```bash
# 首次部署需要格式化 NameNode，否则启动会报 "NameNode is not formatted"
docker compose run --rm namenode hdfs namenode -format -force
```

> 注意：此命令会清空 HDFS 数据，仅首次部署执行。

### 8.3 启动全部服务

```bash
docker compose up -d
```

### 8.4 等待服务就绪

```bash
# 等待所有容器启动（约 1-2 分钟）
sleep 60

# 查看容器状态
docker compose ps
```

> 如果 namenode/datanode/hive-metastore 等容器退出，通常是 Kerberos keytab 与 KDC 数据库不匹配。
> 解决方法：停止 kerberos，删除 `data/kerberos/*` 和 `conf/kerberos/keytabs/*.keytab`，重新启动 kerberos 生成新 keytab，再启动全部服务。

---

## 九、初始化

### 9.1 HDFS 目录初始化

```bash
# 创建湖仓数据目录并设置权限
docker exec namenode bash -c '
kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM
hdfs dfs -mkdir -p /user/hive /user/iceberg /user/paimon /user/flink /user/spark /lakehouse /tmp
hdfs dfs -chmod 777 /user/hive /user/iceberg /user/paimon /user/flink /user/spark /lakehouse /tmp
hdfs dfs -ls /
'
```

### 9.2 MySQL 初始化（Hive + Iceberg catalog 数据库）

```bash
# 创建 hive 和 iceberg 用户及数据库
docker exec mysql mysql -uroot -proot123 -e "
CREATE DATABASE IF NOT EXISTS hive_metastore CHARACTER SET latin1;
CREATE DATABASE IF NOT EXISTS iceberg_catalog CHARACTER SET utf8mb4;
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hive';
CREATE USER IF NOT EXISTS 'hive'@'localhost' IDENTIFIED BY 'hive';
CREATE USER IF NOT EXISTS 'iceberg'@'%' IDENTIFIED BY 'iceberg';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'localhost';
GRANT ALL PRIVILEGES ON iceberg_catalog.* TO 'iceberg'@'%';
FLUSH PRIVILEGES;
"
```

### 9.3 初始化 Hive Metastore 数据库（首次部署）

apache/hive 镜像的 `schematool` 默认读取 `/opt/hive/conf/hive-site.xml`（内置 Derby 配置），
而我们的自定义配置挂载在 `/opt/hive/custom-conf/`。因此需要显式指定 MySQL 连接参数：

```bash
docker compose run --rm --no-deps --entrypoint bash hive-metastore -c '
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
cd /opt/hive
bin/schematool -dbType mysql -initSchema \
  -url "jdbc:mysql://mysql:3306/hive_metastore?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=UTF-8" \
  -userName hive -passWord hive
'
```

> 看到 `schemaTool completed` 表示成功。如果提示已存在 schema（重复执行），可忽略此步。

### 9.4 注册 Doris BE（首次部署）

Doris BE 启动后会自动尝试向 FE 注册，但有时需要手动注册：

```bash
# 查看 BE 是否已注册
docker exec doris-fe mysql -uroot -P9030 -h127.0.0.1 -e "SHOW BACKENDS\G"

# 如果 BE 未注册或 Alive 为 false，手动添加（IP 为 doris-be 容器的 IP）
docker exec doris-fe mysql -uroot -P9030 -h127.0.0.1 -e "ALTER SYSTEM ADD BACKEND '172.30.80.14:9050';"

# 重启 BE 使其生效
docker compose restart doris-be
```

### 9.5 创建 Iceberg catalog 数据库表

```bash
# Iceberg REST 使用 JdbcCatalog，首次启动会自动建表
# 验证 iceberg_catalog 库中已生成表
docker exec mysql mysql -uroot -proot123 -e "SHOW TABLES FROM iceberg_catalog;"
```

---

## 十、验证集群状态

### 10.1 运行集群状态检查脚本

```bash
bash scripts/lakehouse_status.sh
```

### 10.2 逐项验证

**HDFS：**
```bash
docker exec namenode bash -c '
kinit -kt /etc/security/keytabs/lakehouse.keytab lakehouse@LAKEHOUSE.COM
echo "hello lakehouse" > /tmp/test.txt
hdfs dfs -put /tmp/test.txt /
hdfs dfs -cat /test.txt
hdfs dfs -rm /test.txt
'
```

**Hive Metastore：**
```bash
docker exec hive-metastore beeline -u "jdbc:hive2://hive-server:21066/default;principal=hive/hiveserver.lakehouse.com@LAKEHOUSE.COM" -e "SHOW DATABASES;"
```

**Flink：**
```bash
# 进入 Flink SQL Client
bash scripts/flink-sql.sh

# 在 SQL Client 中执行：
# SHOW CATALOGS;
# SHOW DATABASES;
```

**Iceberg REST：**
```bash
curl http://localhost:8181/v1/config
```

**Doris：**
```bash
mysql -uroot -h127.0.0.1 -P9030 -e "SHOW FRONTENDS\G"
mysql -uroot -h127.0.0.1 -P9030 -e "SHOW BACKENDS\G"
```

**Kafka：**
```bash
docker exec kafka bash -c '
kinit -kt /etc/security/keytabs/lakehouse.keytab lakehouse@LAKEHOUSE.COM
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka.lakehouse.com:9092 --list
'
```

**Spark：**
```bash
# Spark Web UI: http://localhost:8080
curl -s http://localhost:8080 | grep -o "Alive Workers.*"
```

**MongoDB：**
```bash
docker exec mongodb mongosh -u root -p root123 --authenticationDatabase admin --eval "db.runCommand({ping:1})"
```

---

## 十一、常用操作

### 11.1 进入 Flink SQL Client

```bash
bash scripts/flink-sql.sh
```

### 11.2 停止 / 启动集群

```bash
# 停止全部
docker compose down

# 启动全部
docker compose up -d

# 重启某个服务
docker compose restart hive-metastore
```

### 11.3 查看日志

```bash
docker logs -f namenode
docker logs -f hive-metastore
docker logs -f flink-jobmanager
```

### 11.4 Kerberos 相关

```bash
# 进入 KDC 容器
docker exec -it kerberos bash

# 查看所有 principal
kadmin.local -q "listprincs"

# 重新生成某个 keytab（例如 hive）
kadmin.local -q "ktadd -k /etc/security/keytabs/hive.service.keytab hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM"
```

---

## 十二、变更记录

### 2026-09-27

#### 1. Spark Hadoop 配置改为 Volume 挂载

**背景**：此前 Spark Dockerfile 通过 `COPY conf/hadoop/ /opt/hadoop/etc/hadoop/` 将 Hadoop 配置内置到镜像。
每次修改 `core-site.xml`、`hdfs-site.xml` 都需要重建 Spark 镜像，开发效率低。

**变更**：
- `build/spark/Dockerfile`：移除 `COPY conf/hadoop/` 行
- `docker-compose.yaml`：spark-master / spark-worker 新增挂载 `./conf/hadoop:/opt/hadoop/etc/hadoop:ro`

**效果**：修改 Hadoop 配置后只需 `docker compose restart spark-master spark-worker`，无需重建镜像。
与 namenode/datanode 的 Hadoop 配置挂载方式保持一致。

**文件装载策略（最终）**：

| 类型 | 文件 | 方式 |
|------|------|------|
| 静态文件 | jar、spark-defaults.conf、hive-site.xml、krb5.conf、entrypoint | COPY 进镜像 |
| 易变配置 | hadoop conf（core-site.xml、hdfs-site.xml） | volume 挂载 |
| 密钥/开发目录 | keytabs、sql、scripts | volume 挂载 |

#### 2. 平台用户 `testuser` → `lakehouse`

**背景**：`testuser` 命名不够正式，作为湖仓平台的通用 Kerberos 用户应使用更规范的名称。

**变更**：
- Kerberos principal：`testuser@LAKEHOUSE.COM` → `lakehouse@LAKEHOUSE.COM`
- keytab 文件：`testuser.keytab` → `lakehouse.keytab`
- Hadoop `auth_to_local` 规则：`testuser` → `lakehouse`
- `build/kerberos/kdc-init.sh`：principal 创建改用 `addprinc -randkey`（随机密钥，更安全），并 `chmod 644`
- 所有文档（DEPLOYMENT_NOTES.md、DBEAVER_CONNECTION_GUIDE.md、SPARK_OFFLINE_LAKEHOUSE.md）同步更新

**影响**：Kerberos 数据库已重建，所有 keytab 重新生成。`lakehouse.keytab` 已验证可正常 kinit 并读写 HDFS。
