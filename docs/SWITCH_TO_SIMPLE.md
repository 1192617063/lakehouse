# Kerberos → SIMPLE Auth 回滚指南

> 当前 **默认 Kerberos auth**（所有组件走完整 Kerberos 认证）。
> 本文档给出切换回 **SIMPLE auth** 的精确步骤。
> KDC + 所有 principal + keytabs **保持不动**（不删 KDC，随时能切回 Kerberos）。

---

## 切换前必读

| 项 | 值 |
|----|-----|
| 反向脚本 | `scripts/switch-to-simple.sh`（一键） |
| 反向文档 | `docs/SWITCH_TO_KERBEROS.md`（如果还要切回） |
| Kerberos Realm | `LAKEHOUSE.COM`（全大写） |
| Keytab 路径 | `conf/kerberos/keytabs/*.keytab` |
| **最复杂** | **HBase** — 5 处改动（其他 4 个组件各 1-3 处） |

### 为什么 HBase 最复杂？

HBase 2.5.3 是全栈中唯一需要同时改 **docker-compose + conf 文件** 的服务：

| 组件 | 改 conf 就行？ | 需要改 docker-compose？ |
|------|--------------|------------------------|
| HDFS core-site.xml | ✅ 1 处 sed | ❌ |
| Spark spark-defaults.conf | ✅ 1 处 sed | ❌ |
| Hive hive-site.xml | ✅ 3 处 sed | ❌ |
| Trino catalogs | ✅ 3 文件各 6 处 sed | ❌ |
| **HBase** | ✅ hbase-site.xml（kerberos→simple） | **✅ volumes + command + environment 3 处全改** |

**HBase Kerberos 模式的 3 个 Docker Compose 特性（回滚时要反着做）：**

1. **volumes** — kerberos core-site.xml 挂到 `/opt/hbase/conf/core-site.xml`（classpath 自动加载）
2. **command** — `kinit -kt ... && ...` 前缀先拿 ticket 再启动 Master/RS
3. **environment** — `JAVA_TOOL_OPTIONS=-Djava.security.krb5.conf=... -Djavax.security.auth.useSubjectCredsOnly=false`

SIMPLE 模式下这 3 个全部要恢复成简单状态。

---

## 切换总览

```
Step 0: 备份（git diff 快照 + conf 拷贝）
Step 1: HDFS     core-site.xml        kerberos → simple
Step 2: Spark    spark-defaults.conf  kerberos → simple
Step 3: Hive     hive-site.xml        KERBEROS → NOSASL + Metastore ! 前缀
Step 4: Trino    catalogs *.properties KERBEROS → NONE + principal 注释
Step 5: HBase
  5a: hbase-site.xml                  kerberos → simple
  5b: docker-compose.yaml HBase volumes: classpath mounts → hbase-hadoop/ 整体挂载
  5c: docker-compose.yaml HBase command: 移除 kinit 前缀
  5d: docker-compose.yaml HBase environment: 移除 JAVA_TOOL_OPTIONS
Step 6: 全栈重启（docker compose down && up -d）
Step 7: 验证
```

---

## Step 0 — 备份（强制）

```bash
cd /home/admin/lakehouse

# 方式 A（推荐）：git diff 快照
git diff > .switch-state.before-simple/config.patch
# 回滚用: git apply .switch-state.before-simple/config.patch （patch -R 反向也支持）

# 方式 B：非 git 仓库，手动 cp
mkdir -p .switch-state.before-simple/conf
cp -r conf/hadoop conf/spark conf/hive conf/trino conf/hbase conf/flink conf/kerberos .switch-state.before-simple/conf/ 2>/dev/null || true
cp docker-compose.yaml .switch-state.before-simple/

# 手动备份 hbase-hadoop/（如果要恢复用这个）
cp -r conf/hbase-hadoop /tmp/hbase-hadoop.bak 2>/dev/null || true
```

---

## Step 1 — HDFS: core-site.xml

**文件**: `conf/hadoop/core-site.xml`

```bash
sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hadoop/core-site.xml
grep -A1 'hadoop.security.authentication' conf/hadoop/core-site.xml
# 预期: <value>simple</value>
```

> 这是全局开关。HDFS NameNode 重启后接受 SIMPLE 客户端。

---

## Step 2 — Spark: spark-defaults.conf

**文件**: `conf/spark/spark-defaults.conf`

```bash
sed -i 's|spark.hadoop.hadoop.security.authentication kerberos|spark.hadoop.hadoop.security.authentication simple|' \
  conf/spark/spark-defaults.conf
grep "hadoop.security.authentication" conf/spark/spark-defaults.conf
# 预期: spark.hadoop.hadoop.security.authentication simple
```

---

## Step 3 — Hive: hive-site.xml

**文件**: `conf/hive/hive-site.xml`

### 3a. HiveServer2: KERBEROS → NOSASL

```bash
sed -i 's|<value>KERBEROS</value>|<value>NOSASL</value>|' conf/hive/hive-site.xml
```

### 3b. Hive Metastore Kerberos 属性: 加 `!` 前缀禁用

```bash
sed -i 's|<name>hive.metastore.kerberos|<name>!hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>hive.metastore.sasl|<name>!hive.metastore.sasl|g' conf/hive/hive-site.xml
```

`sasl.enabled` 值应该已经是 `true`，加 `!` 前缀后变成 `!hive.metastore.sasl.enabled`，Hive 会忽略这个属性名。

验证：
```bash
grep -E "hive.server2.authentication|!hive.metastore" conf/hive/hive-site.xml
# 预期:
#   <value>NOSASL</value>
#   <name>!hive.metastore.kerberos.keytab.file</name>
#   <name>!hive.metastore.sasl.enabled</name>
```

---

## Step 4 — Trino: catalog properties

**文件**: `conf/trino/catalog/hive.properties`（hudi.properties / iceberg.properties 同理）

### 4a. 一键脚本

```bash
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  sed -i 's/hive.metastore.authentication.type=KERBEROS/hive.metastore.authentication.type=NONE/' "$f"
  sed -i 's/hive.hdfs.authentication.type=KERBEROS/hive.hdfs.authentication.type=NONE/' "$f"
  # principal/keytab 行加 # 注释
  sed -i 's/^hive.metastore.service.principal/# hive.metastore.service.principal/' "$f"
  sed -i 's/^hive.metastore.client.principal/# hive.metastore.client.principal/' "$f"
  sed -i 's/^hive.metastore.client.keytab/# hive.metastore.client.keytab/' "$f"
  sed -i 's/^hive.hdfs.trino.principal/# hive.hdfs.trino.principal/' "$f"
  sed -i 's/^hive.hdfs.trino.keytab/# hive.hdfs.trino.keytab/' "$f"
done

# 验证
grep -E "authentication.type|principal|keytab" conf/trino/catalog/hive.properties
# 预期:
#   hive.metastore.authentication.type=NONE
#   hive.hdfs.authentication.type=NONE
#   # hive.metastore.service.principal=...
#   # hive.metastore.client.principal=...
#   ...
```

---

## Step 5 — HBase（最复杂，3 层改动）

### 5a. hbase-site.xml

**文件**: `conf/hbase/hbase-site.xml`

把两处 `kerberos` 改成 `simple`：

```bash
sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hbase/hbase-site.xml

# 验证
grep -A1 'security.authentication' conf/hbase/hbase-site.xml
# 预期:
#   hadoop.security.authentication = simple
#   hbase.security.authentication = simple
```

> ⚠️ **注意**：`hbase-site.xml` 里还内嵌了 `hadoop.security.auth_to_local` Kerberos 规则。
> 在 SIMPLE 模式下这些规则**不会被触发**（Hadoop 不会跑 auth_to_local），所以可以留着不动。
> 如果要彻底清理，手动删掉 `<property>hadoop.security.auth_to_local</property>` 块即可。

### 5b. docker-compose.yaml — volumes（hbase-master）

**Kerberos 当前值：**
```yaml
volumes:
  - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
  - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
  - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
  - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
  - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
  - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
  - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
```

**SIMPLE 改成：**
```yaml
volumes:
  - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
  - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
```

**同理 hbase-regionserver 也做同样替换。**

为什么可以简化成这 2 个？
- `./conf/hadoop/core-site.xml` 挂到 classpath `/opt/hbase/conf/core-site.xml` 是 Kerberos 专属 trick（让 Hadoop Configuration 自动读 kerberos auth）。SIMPLE 模式下 Hadoop 默认值就是 `simple`，**不需要挂任何东西到 classpath**
- `./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro` 是原来的独立 Hadoop conf 目录，里面 core-site.xml 的 auth 已经是 `simple`（备份时快照里确认过）
- `krb5.conf` / `keytabs` 只在 Kerberos 认证时用，SIMPLE 不需要

### 5c. docker-compose.yaml — command（hbase-master + hbase-regionserver）

**Kerberos 当前值：**
```yaml
command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase master start"]
```

**SIMPLE 改成：**
```yaml
command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && /opt/hbase/bin/hbase master start"]
```

hbase-regionserver 同理：去掉 `kinit -kt ... && ` 前缀。

### 5d. docker-compose.yaml — environment（hbase-master + hbase-regionserver）

**Kerberos 当前值：**
```yaml
environment:
  HBASE_HOME: /opt/hbase
  JAVA_HOME: /opt/java/openjdk
  JAVA_TOOL_OPTIONS: "-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
```

**SIMPLE 改成：**
```yaml
environment:
  HBASE_HOME: /opt/hbase
  JAVA_HOME: /opt/java/openjdk
```

`JAVA_TOOL_OPTIONS` 里的两个系统属性只在 Kerberos JVM GSSAPI 认证时用。

### 5a-5d 一键 Python 脚本（已经封装在 switch-to-simple.sh 里）

如果你没跑 switch-to-simple.sh，可以手动执行：

```bash
# 这是脚本 5b-5d 的内嵌 Python 代码
python3 -c '
import re
with open("docker-compose.yaml") as f: c = f.read()

# hbase-master volumes
c = c.replace(
  """    volumes:
      # 关键：kerberos 版 core-site.xml 放到 HBase classpath 上 → Hadoop Configuration 自动加载
      - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro""",
  """    volumes:
      # SIMPLE auth：HBase 用独立 Hadoop conf（不读全局 kerberos）
      - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro""")

# hbase-master command
c = c.replace(
  "command: [\"bash\", \"-c\", \"mkdir -p /opt/hbase/zookeeper/data && kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase master start\"]",
  "command: [\"bash\", \"-c\", \"mkdir -p /opt/hbase/zookeeper/data && /opt/hbase/bin/hbase master start\"]")

# hbase-master environment: 去掉 JAVA_TOOL_OPTIONS
c = re.sub(
  r"(hbase-master:.*?environment:.*?JAVA_HOME: /opt/java/openjdk)\n\s*JAVA_TOOL_OPTIONS:.*?\n\s*(ports:)",
  r"\1\n    \2", c, flags=re.DOTALL)

# hbase-regionserver volumes
c = c.replace(
  """    volumes:
      - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro
      - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
      - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
      - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
      - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
      - ./conf/kerberos/keytabs:/etc/security/keytabs:ro""",
  """    volumes:
      - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
      - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro""")

# hbase-regionserver command
c = c.replace(
  "command: [\"bash\", \"-c\", \"kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase regionserver start\"]",
  "command: [\"bash\", \"-c\", \"/opt/hbase/bin/hbase regionserver start\"]")

# hbase-regionserver environment: 去掉 JAVA_TOOL_OPTIONS
c = re.sub(
  r"(hbase-regionserver:.*?environment:.*?JAVA_HOME: /opt/java/openjdk)\n\s*JAVA_TOOL_OPTIONS:.*?\n\s*(ports:)",
  r"\1\n    \2", c, flags=re.DOTALL)

with open("docker-compose.yaml", "w") as f: f.write(c)
print("✅ docker-compose.yaml updated")
'
```

---

## Step 6 — 全栈重启

```bash
cd /home/admin/lakehouse

# 停所有（Kerberos KDC 可以保留）
docker compose down

# 重新全栈启动
docker compose up -d
sleep 60

docker compose ps | grep -c "Up\|healthy"
# 预期: 20
```

> 顺序不重要了（SIMPLE auth 没有 Kerberos 依赖关系），`docker compose up -d` 直接一把起全部即可。

---

## Step 7 — 验证

### 7a. HDFS 不需要 kinit 就能列

```bash
docker exec namenode hdfs dfs -ls /
# 预期: /lakehouse, /tmp, /user, /hbase（不需要 kinit）
```

### 7b. Spark SQL

```bash
docker exec spark spark-sql -e "SELECT 1 as ok"
# 预期: 1

# 读 Paimon CDC（Kerberos 态的关键验证）
docker exec spark spark-sql -e "SELECT COUNT(*) FROM paimon.cdc_demo.products"
# 预期: 5
```

### 7c. Trino

```bash
docker exec trino /usr/bin/trino --catalog hive --execute "SHOW SCHEMAS"
# 预期: cdc_demo, default
```

### 7d. Flink JM

```bash
curl -s http://localhost:8081/overview | python3 -c "import sys,json; d=json.load(sys.stdin); print('TM:', d.get('taskmanagers',0), 'slots:', d.get('slots-total',0))"
# 预期: TM: 1 slots: 8
```

### 7e. HBase Master + RS + Web UI

```bash
curl -s -o /dev/null -w "Master 16010: %{http_code}\n" http://localhost:16010/
curl -s -o /dev/null -w "RS 16020: %{http_code}\n"  http://localhost:16020/
# 预期: Master 200, RS 200

# HBase Shell（SIMPLE 不需要 kinit）
docker exec hbase-master /opt/hbase/bin/hbase shell <<EOF
status
list
exit
EOF
```

### 7f. Hive Beeline（NOSASL 不需要 auth=KERBEROS）

```bash
docker exec hive-server beeline -u 'jdbc:hive2://localhost:21066/default' -e "SHOW DATABASES;"
# 预期: default, cdc_demo
```

---

## 一键脚本

```bash
# 回滚 SIMPLE
./scripts/switch-to-simple.sh

# 切回 Kerberos（反向）
./scripts/switch-to-kerberos.sh
```

| 脚本 | 方向 | 改动 | 需要重启 |
|------|------|------|---------|
| `switch-to-simple.sh` | **Kerberos → SIMPLE** | 5 个 conf 文件 + docker-compose.yaml HBase 服务 | ✅ `docker compose down && up -d` |
| `switch-to-kerberos.sh` | SIMPLE → Kerberos | 4 个 conf 文件 | ✅ `docker compose down && up -d` |

---

## 修改文件汇总表

| # | 文件 | Kerberos 值 | SIMPLE 值 | 方式 |
|---|------|------------|----------|------|
| 1 | `conf/hadoop/core-site.xml` | `<value>kerberos</value>` | `<value>simple</value>` | sed 替换 |
| 2 | `conf/spark/spark-defaults.conf` | `authentication kerberos` | `authentication simple` | sed 替换 |
| 3 | `conf/hive/hive-site.xml` | `KERBEROS` + Metastore 开启 | `NOSASL` + `!metastore.kerberos/sasl` | sed 替换 |
| 4 | `conf/trino/catalog/*.properties` | `KERBEROS` + principal 打开 | `NONE` + principal 注释 | sed 替换 |
| 5a | `conf/hbase/hbase-site.xml` | `kerberos` | `simple` | sed 替换 |
| 5b | `docker-compose.yaml` HBase volumes | classpath 7 个 mount + krb5.conf + keytabs | hbase-hadoop/ 整体挂载 + hbase-site.xml | 手动 / Python 替换 |
| 5c | `docker-compose.yaml` HBase command | `kinit -kt ... && start` | `start` | sed / 手动替换 |
| 5d | `docker-compose.yaml` HBase environment | 含 `JAVA_TOOL_OPTIONS` Kerberos 属性 | 移除 `JAVA_TOOL_OPTIONS` | 手动 / Python 替换 |

**不动的文件**（SIMPLE 模式下仍保留 Kerberos 配置，以后切回时用）：
- `conf/kerberos/krb5.conf`（KDC 继续用）
- `conf/kerberos/keytabs/*.keytab`（保留，不删）
- `conf/flink/flink-conf.yaml`（Flink keytab/principal 配置保留，只是不 kinit 就走 SIMPLE）
- `build/kerberos/kdc-init.sh`（KDC 不变）
- `.env.example` / `docker-compose.yaml` 其他服务（Spark/Hive/Trino/Iceberg 的 Kerberos 挂载和 JAVA_TOOL_OPTIONS 也可以保留，SIMPLE 模式下不会触发——但如果想干净也可以手动移除）

---

## 彻底清理 KDC（可选）

如果你**不再需要 Kerberos**（比如完全放弃），可以删 KDC 数据：

```bash
docker compose down
rm -rf data/kerberos/* conf/kerberos/keytabs/*.keytab
docker volume rm lakehouse_kerberos_data  # 如果用了 named volume
docker compose up -d
```

> ⚠️ 清理后切回 Kerberos 必须重建 KDC（所有 principal + keytabs）。KDC master key 丢了就永远恢复不了。

---

## 快速对比：Kerberos vs SIMPLE

| 方面 | Kerberos（默认） | SIMPLE（回滚后） |
|------|----------------|-----------------|
| HDFS `hdfs dfs -ls /` | 需要 kinit 或 keytab | 直接跑 |
| HBase Shell | 需要在同一 bash 进程 kinit 后跑 | 直接跑 |
| Spark SQL | 需要 keytab 或 delegation token | 直接跑 |
| Hive Beeline | JDBC URL 加 `auth=KERBEROS;principal=...` | `jdbc:hive2://host:21066/db` |
| Trino 连接 | `hive.hdfs.trino.keytab` 必须挂载 | 不需要 |
| **学习曲线** | 陡 | 平 |
| **安全** | ✅ 生产级 | ❌ 本地/测试用 |

---

*最后更新：2026-09-27 — 对应当前全栈 Kerberos 态 + switch-to-simple.sh 已增强 HBase 3 层回滚*
