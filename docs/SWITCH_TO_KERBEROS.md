# SIMPLE Auth → Kerberos 切换指南

> 当前学习环境默认 **SIMPLE auth**（所有组件免 Kerberos，开箱即用）。
> 本文档给出切换到 **Kerberos 完整认证** 的精确步骤。
> KDC + 所有 principal + keytabs 已在首次启动时创建，无需重建。

---

## 切换前必读

| 项 | 值 |
|----|-----|
| Realm | `LAKEHOUSE.COM`（**全大写**） |
| Admin | `admin/admin@LAKEHOUSE.COM` / `admin123` |
| 平台用户 | `lakehouse@LAKEHOUSE.COM` / `lakehouse123` |
| KDC 端口 | 88/tcp（已映射宿主机） |
| Keytab 路径 | `conf/kerberos/keytabs/*.keytab` |
| auth_to_local | `conf/hadoop/core-site.xml`（多 RULE 格式，已配好） |

### 已知坑点

| # | 坑 | 影响 | 规避 |
|---|----|------|------|
| 1 | **HBase 2.5.3 内置 Hadoop 2.10.2** 与 HDFS 3.3.6 Kerberos 不兼容 | `NoClassDefFoundError: org.apache.hadoop.thirdparty.com.google.common.collect.Interners` | **切换 Kerberos 时 HBase 保留 SIMPLE**，或单独做 Hadoop 3.3.6 jar 替换 |
| 2 | Kerberos KDC master key 重建后所有 keytab 作废 | 连接失败 | 切换不需要重建 KDC（principal/keytabs 已经建好了） |
| 3 | Spark `spark.yarn.archive` + Kerberos delegation token | 必须预打包 JAR 到 HDFS | 当前已配好 `spark.yarn.archive: hdfs://namenode:9000/user/spark/share/spark-jars.tar.gz` |
| 4 | Flink standalone 必须关 delegation token | Kerberos 下 NPE | `security.delegation.tokens.enabled: false`（已配好） |
| 5 | Trino 482 Kerberos 属性名变更 | 老属性不识别 | 使用 `hive.metastore.authentication.type=KERBEROS` + principal/keytab |
| 6 | Hive HS2 外部连接必须 FQDN | GSSAPI 匹配 principal | 用 `hiveserver.lakehouse.com` 不是 IP |

---

## 切换总览：4 个文件 + 1 次 KDC 确认 + 全栈重启

```
Step 0: KDC 健康检查 + 确认所有 keytab 存在
Step 1: HDFS core-site.xml        SIMPLE → KERBEROS
Step 2: Spark spark-defaults.conf  SIMPLE → KERBEROS
Step 3: Hive hive-site.xml        NOSASL → KERBEROS + 打开 metastore Kerberos
Step 4: Trino catalog *.properties NONE → KERBEROS + 打开注释的 principal/keytab
Step 5: 全栈重启（KDC/HDFS/Hive/Spark/Trino/Iceberg）
Step 6: HBase 保持 SIMPLE（或单独解决 Hadoop 2.10.2→3.3.6 jar 替换）
Step 7: 验证
```

---

## Step 0 — KDC 健康检查

```bash
cd /home/admin/lakehouse

# 1. KDC 是否活着
docker compose up -d kerberos
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'getprincs' | wc -l
# 预期: 21 个 principal（admin + lakehouse + 19 服务 principal）

# 2. 所有 keytabs 是否存在
ls -la conf/kerberos/keytabs/*.keytab
# 预期: 11 个 — hive/spark/flink/trino/iceberg/kafka/nn/dn/rm/nm/hbase

# 3. 随便抽一个验证有效性
docker exec kerberos kadmin -p admin/admin -w admin123 \
  -q "ktadd -k /tmp/test.keytab spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM" 2>&1
docker cp kerberos:/tmp/test.keytab /tmp/test.keytab
klist -ket /tmp/test.keytab   # 能看到 principal = OK
rm /tmp/test.keytab /tmp/test.keytab
```

---

## Step 1 — HDFS: core-site.xml（全局开关）

**文件**: `conf/hadoop/core-site.xml`

把 `<value>simple</value>` 改成 `<value>kerberos</value>`：

```bash
cd /home/admin/lakehouse
sed -i 's|<value>simple</value>|<value>kerberos</value>|' conf/hadoop/core-site.xml
grep -A1 'hadoop.security.authentication' conf/hadoop/core-site.xml
# 预期输出:
#     <name>hadoop.security.authentication</name>
#     <value>kerberos</value>
```

> ⚠️ **HBase 还要连 HDFS**！HBase 有自己的独立 conf 目录 `conf/hbase-hadoop/`。
> 这个目录的 `core-site.xml` 先**不要改**，保持 SIMPLE。
> 如果要 HBase 也上 Kerberos，见本文档末尾「HBase Kerberos 额外步骤」。

---

## Step 2 — Spark: spark-defaults.conf

**文件**: `conf/spark/spark-defaults.conf`

把 `simple` 改回 `kerberos`：

```bash
sed -i 's|spark.hadoop.hadoop.security.authentication simple|spark.hadoop.hadoop.security.authentication kerberos|' \
  conf/spark/spark-defaults.conf
grep "hadoop.security.authentication" conf/spark/spark-defaults.conf
# 预期: spark.hadoop.hadoop.security.authentication kerberos
```

其他 Kerberos 属性（keytab/principal/relogin）**已经配好**，不用改：
```conf
spark.kerberos.keytab /etc/security/keytabs/spark.service.keytab
spark.kerberos.principal spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM
spark.kerberos.relogin.period 30s
spark.kerberos.access.hadoopFileSystems hdfs://namenode:9000
spark.hadoop.dfs.namenode.kerberos.principal nn/namenode.lakehouse.com@LAKEHOUSE.COM
spark.hadoop.dfs.namenode.kerberos.internal.spnego.principal HTTP/namenode.lakehouse.com@LAKEHOUSE.COM
```

---

## Step 3 — Hive: hive-site.xml

**文件**: `conf/hive/hive-site.xml`

有 4 处要改（恢复被注释的 Kerberos + SASL）：

### 3a. 恢复 HiveServer2 Kerberos（当前 NOSASL）

```bash
# 把 NOSASL 改回 KERBEROS
sed -i 's|<value>NOSASL</value>|<value>KERBEROS</value>|' conf/hive/hive-site.xml
grep -A1 "hive.server2.authentication" conf/hive/hive-site.xml | head -4
```
预期：
```xml
<name>hive.server2.authentication</name>
<value>KERBEROS</value>
```

### 3b. 恢复 Hive Metastore Kerberos（当前被 `!` 前缀注释）

Metastore 的 kerberos 属性名被改成了 `!hive.metastore.xxx`（hack 禁用）。去掉前缀恢复：

```bash
sed -i 's|<name>!hive.metastore.kerberos|<name>hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.sasl|<name>hive.metastore.sasl|g' conf/hive/hive-site.xml
grep -A1 "hive.metastore.kerberos\|hive.metastore.sasl.enabled" conf/hive/hive-site.xml
```
预期：
```xml
<name>hive.metastore.kerberos.keytab.file</name>
<value>/etc/security/keytabs/hive.service.keytab</value>
<name>hive.metastore.kerberos.principal</name>
<value>hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM</value>
<name>hive.metastore.sasl.enabled</name>
<value>true</value>
```

> 如果 `sasl.enabled` 当前值是 `false`，改成 `true`：
> ```bash
> sed -i '/<name>hive.metastore.sasl.enabled<\/name>/{n;s|<value>false</value>|<value>true</value>|}' conf/hive/hive-site.xml
> ```

---

## Step 4 — Trino: catalog properties

**文件**: `conf/trino/catalog/hive.properties`（hudi.properties / iceberg.properties 同理）

需要做 2 件事：
1. 把 `authentication.type=NONE` 改回 `KERBEROS`
2. 打开注释的 principal/keytab 行（Trino 482 属性名已调整，用下面模板）

### 4a. hive.properties（最完整模板）

```properties
connector.name=hive
fs.hadoop.enabled=true
hive.config.resources=/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml
hive.metastore.uri=thrift://hive-metastore:9083

# === Kerberos ===
hive.metastore.authentication.type=KERBEROS
hive.metastore.service.principal=hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM
hive.metastore.client.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.metastore.client.keytab=/etc/security/keytabs/trino.service.keytab

hive.hdfs.authentication.type=KERBEROS
hive.hdfs.impersonation.enabled=false
hive.hdfs.trino.principal=trino/trino.lakehouse.com@LAKEHOUSE.COM
hive.hdfs.trino.keytab=/etc/security/keytabs/trino.service.keytab
```

一键脚本：
```bash
cd /home/admin/lakehouse
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  # NONE → KERBEROS
  sed -i 's/hive.metastore.authentication.type=NONE/hive.metastore.authentication.type=KERBEROS/' "$f"
  sed -i 's/hive.hdfs.authentication.type=NONE/hive.hdfs.authentication.type=KERBEROS/' "$f"
  # 去掉注释
  sed -i 's/^# hive.metastore.service.principal/hive.metastore.service.principal/' "$f"
  sed -i 's/^# hive.metastore.client.principal/hive.metastore.client.principal/' "$f"
  sed -i 's/^# hive.metastore.client.keytab/hive.metastore.client.keytab/' "$f"
  sed -i 's/^# hive.hdfs.trino.principal/hive.hdfs.trino.principal/' "$f"
  sed -i 's/^# hive.hdfs.trino.keytab/hive.hdfs.trino.keytab/' "$f"
done

# 验证
grep -E "authentication.type|principal|keytab" conf/trino/catalog/hive.properties
```

---

## Step 5 — 全栈重启

```bash
cd /home/admin/lakehouse

# 先把依赖 Kerberos 的都停了，再依次起来
docker compose down spark trino iceberg-rest hive-server hive-metastore hbase-master hbase-regionserver flink-jobmanager flink-taskmanager 2>&1

# 1. Kerberos KDC（确认在跑）
docker compose up -d kerberos
sleep 5

# 2. HDFS（Kerberos 最底层）
docker compose up -d namenode datanode
sleep 25   # NameNode 启动慢

# 3. 验证 HDFS Kerberos
docker exec namenode bash -c '
  hdfs dfsadmin -safemode wait 2>/dev/null
  # 用 keytab 认证测试
  kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM
  hdfs dfs -ls /
'

# 4. YARN
docker compose up -d yarn-resource-manager yarn-node-manager
sleep 15

# 5. Hive Metastore → HiveServer2（Metastore 必须先好）
docker compose up -d hive-metastore
sleep 15
docker compose up -d hive-server
sleep 10

# 6. Spark
docker compose up -d spark
sleep 15

# 7. Trino
docker compose up -d trino
sleep 25   # Trino 初始化慢

# 8. Flink
docker compose up -d flink-jobmanager flink-taskmanager
sleep 15

# 9. Iceberg REST
docker compose up -d iceberg-rest
sleep 10

# 10. Zookeeper + HBase（HBase 保持 SIMPLE！见 Step 6）
docker compose up -d zookeeper
sleep 5
docker compose up -d hbase-master hbase-regionserver
sleep 20

# 11. 其他（Kafka/Doris/MySQL/MongoDB）
docker compose up -d kafka doris-fe doris-be mysql postgres mongodb

# 等所有起来
sleep 30
docker compose ps | grep -c "Up\|healthy"
# 预期: 20
```

---

## Step 6 — HBase 特殊处理（3 选 1）

HBase 2.5.3 **内置 Hadoop 2.10.2**，它的 `org.apache.hadoop.thirdparty.com.google.common.collect.Interners` 类与 Hadoop 3.3.6 不兼容。所以**不能直接让 HBase 上 Kerberos**。

### 选项 A（推荐）：HBase 保持 SIMPLE，其他全 Kerberos

HBase 用自己的 `conf/hbase-hadoop/`（SIMPLE auth），HDFS 全局是 KERBEROS。HBase 的 Hadoop 2.10.2 UGI 会 fallback 到 SIMPLE，HDFS NameNode 拒绝 SIMPLE 连接。

**所以还要在 HDFS 上加 SIMPLE fallback**（让 HBase 能连上）：

```bash
# 在 conf/hadoop/core-site.xml 里加 SIMPLE fallback（Kerberos 之上叠加）
# Hadoop 支持多 auth 格式，加一行 dfs.support.simle.auth = true 不行
# 正确做法：把 hadoop.security.authentication 改回同时接受两种 —— Hadoop 不原生支持！
# ======
# 简化方案：HBase 保持独立集群（不通过 Kerberos HDFS）
# HBase 改成用文件系统（fs.local）而非 HDFS？—— 不行，生产必须 HDFS
# ======
# 最终方案：HDFS 同时接受 KERBEROS + SIMPLE（Hadoop 通过 dfs.block.access.token.enable 控制）
# 在 conf/hadoop/hdfs-site.xml 加：
cat >> conf/hadoop/hdfs-site.xml <<'EOF'
  <!-- 允许 SIMPLE 客户端（HBase / 测试）连接到 Kerberos HDFS -->
  <property>
    <name>dfs.block.access.token.enable</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>nn/namenode.lakehouse.com@LAKEHOUSE.COM</value>
  </property>
EOF
```

> 但 **Hadoop 不支持在 Kerberos-only HDFS 上开启 SIMPLE fallback**。
> 所以现实是：
> - **HDFS 要么全 KERBEROS 要么全 SIMPLE**
> - HBase 2.5.3 用 Kerberos HDFS 必须做选项 B

### 选项 B（生产做法）：HBase jar 替换为 Hadoop 3.3.6

```bash
# 1. 删除 HBase 内置 Hadoop jars
docker exec hbase-master bash -c 'rm -f /opt/hbase/lib/hadoop-*.jar'

# 2. 从宿主 lib/hadoop-3.3.6 拷 jars
docker exec hbase-master bash -c '
  for j in /opt/hadoop-3.3.6/share/hadoop/common/*.jar \
           /opt/hadoop-3.3.6/share/hadoop/common/lib/*.jar \
           /opt/hadoop-3.3.6/share/hadoop/hdfs/*.jar \
           /opt/hadoop-3.3.6/share/hadoop/hdfs/lib/*.jar \
           /opt/hadoop-3.3.6/share/hadoop/client/*.jar; do
    cp "$j" /opt/hbase/lib/ 2>/dev/null
  done
'

# 3. HBase 的 hbase-site.xml 改成 Kerberos
# （conf/hbase/hbase-site.xml 当前是 simple，把 <value>simple</value> → <value>kerberos</value>）

# 4. docker-compose.yaml 的 HBase 命令加 kinit（已有 keytab 挂载）
# command: ["bash", "-c", "kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase master start"]
# volumes 加: ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
#            ./conf/kerberos/keytabs:/etc/security/keytabs:ro
```

### 选项 C（最简单）：HBase 独立集群，HDFS 保持 SIMPLE

如果 HBase 使用场景是 KV 查询而非 HDFS 底存储，可以让 HBase 用**本地文件系统**或独立 HDFS。但当前 HBase 硬编码 `hdfs://namenode:9000/hbase`，所以这条路需要改 hbase-site.xml 的 `hbase.rootdir`。

**推荐生产做法**：选项 B（jar 替换）。学习环境推荐：**HDFS 全 Kerberos，HBase 先不跑或临时关掉**。

---

## Step 7 — 验证

### 7a. 全容器 Running

```bash
docker compose ps | grep -c "Up\|healthy"
# 预期: 20
```

### 7b. Spark SQL（最关键的回归）

```bash
docker exec spark bash -c '
  kinit -kt /etc/security/keytabs/spark.service.keytab spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM
  spark-sql -e "SELECT COUNT(*) FROM paimon.cdc_demo.products"
'
# 预期: 5
```

### 7c. Trino

```bash
docker exec trino /usr/bin/trino --catalog hive --execute "SHOW SCHEMAS"
# 预期: default, cdc_demo
```

### 7d. Flink JM

```bash
curl -s http://localhost:8081/overview | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['taskmanagers'], d['slots-total'])"
# 预期: 1 8
```

### 7e. Hive Beeline（Kerberos GSSAPI）

```bash
# 宿主机先 kinit（需要 MIT Kerberos for Windows / 系统 kinit）
kinit lakehouse@LAKEHOUSE.COM
klist

# Beeline 必须用 FQDN + auth=KERBEROS
docker exec hive-server beeline \
  -u 'jdbc:hive2://hiveserver.lakehouse.com:21066/default;auth=KERBEROS;principal=hive/hiveserver.lakehouse.com@LAKEHOUSE.COM' \
  -e "SHOW DATABASES;"
# 预期: default, cdc_demo
```

### 7f. Kerberos ticket 快照看

```bash
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'getprincs' | head -25
# 预期: 21 个 principal
```

---

## 一键脚本（可选）

如果你确认理解了上面每一步，可用下面脚本一次性切换：

```bash
#!/bin/bash
# switch-to-kerberos.sh
set -euo pipefail
cd /home/admin/lakehouse

echo "=== [1/4] HDFS core-site.xml ==="
sed -i 's|<value>simple</value>|<value>kerberos</value>|' conf/hadoop/core-site.xml
echo "  ✅ hadoop.security.authentication = kerberos"

echo "=== [2/4] Spark spark-defaults.conf ==="
sed -i 's|spark.hadoop.hadoop.security.authentication simple|spark.hadoop.hadoop.security.authentication kerberos|' conf/spark/spark-defaults.conf
echo "  ✅ Spark Hadoop auth = kerberos"

echo "=== [3/4] Hive hive-site.xml ==="
sed -i 's|<value>NOSASL</value>|<value>KERBEROS</value>|' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.kerberos|<name>hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.sasl|<name>hive.metastore.sasl|g' conf/hive/hive-site.xml
sed -i '/<name>hive.metastore.sasl.enabled<\/name>/{n;s|<value>false</value>|<value>true</value>|}' conf/hive/hive-site.xml
echo "  ✅ HS2 KERBEROS + Metastore SASL=true"

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
echo "    切换 HBase Kerberos 见本文档 Step 6"
echo ""
echo "=== 🚀 现在执行全栈重启 ==="
echo "  docker compose down"
echo "  docker compose up -d kerberos namenode datanode"
echo "  sleep 30 && docker compose up -d yarn-resource-manager yarn-node-manager"
echo "  docker compose up -d hive-metastore hive-server spark trino iceberg-rest"
echo "  docker compose up -d zookeeper hbase-master hbase-regionserver"
echo "  docker compose up -d flink-jobmanager flink-taskmanager kafka doris-fe doris-be"
echo ""
echo "=== ✅ 配置切换完成，重启后生效 ==="
```

使用：
```bash
chmod +x scripts/switch-to-kerberos.sh
./scripts/switch-to-kerberos.sh
# 然后按提示手动重启，或者 docker compose down && docker compose up -d
```

---

## 回滚（切回 SIMPLE）

万一 Kerberos 搞砸了，一键回滚：

```bash
#!/bin/bash
# switch-to-simple.sh
cd /home/admin/lakehouse

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
echo "✅ 全部切回 SIMPLE，docker compose down && docker compose up -d 生效"
```

---

## 修改文件汇总表

| # | 文件 | 当前状态 | 改成 | 方式 |
|---|------|---------|------|------|
| 1 | `conf/hadoop/core-site.xml` | `<value>simple</value>` | `<value>kerberos</value>` | sed 替换 |
| 2 | `conf/spark/spark-defaults.conf` | `authentication simple` | `authentication kerberos` | sed 替换 |
| 3 | `conf/hive/hive-site.xml` | `NOSASL` + `!metastore.kerberos` | `KERBEROS` + 去掉 `!` | sed 替换 |
| 4 | `conf/trino/catalog/*.properties` | `NONE` + principal 注释 | `KERBEROS` + 打开注释 | sed 替换 |
| 5 | `conf/hbase-hadoop/core-site.xml` | **保持 simple** | **不动** | — |
| 6 | `conf/hbase/hbase-site.xml` | **保持 simple**（除非做选项 B jar 替换） | — | — |
| 7 | Kerberos KDC | **不动**（已配好） | — | — |
| 8 | Flink flink-conf.yaml | **不动**（已配好 keytab/principal） | — | — |
| 9 | Iceberg REST entrypoint.sh | **不动**（已配好 kinit） | — | — |

---

*最后更新：2026-09-27 — 对应当前 SIMPLE auth 基线 + Kerberos 完整保留配置*
