# Lakehouse Auth 切换指南（Kerberos ↔ SIMPLE）

> 当前默认 **Kerberos auth**（全栈完整认证）。
> KDC + principal + keytabs 持久化在 `data/kerberos/` 和 `conf/kerberos/keytabs/`，**切换 auth 不删 KDC**。

---

## 一键脚本

```bash
# Kerberos → SIMPLE（回滚）
./scripts/switch-to-simple.sh

# SIMPLE → Kerberos（切回）——注意 KDC 必须先起来！
./scripts/switch-to-kerberos.sh
# 切 Kerberos 推荐顺序：
#   docker compose down
#   docker compose up -d kerberos zookeeper mysql postgres   # 先起依赖
#   sleep 30 && docker compose up -d                        # 再起全栈

# 随时检查当前 auth 状态（静态 + 可选 --live 容器健康）
./scripts/check-auth-status.sh
./scripts/check-auth-status.sh --live
```

### 切换速查表（两个方向共用）

| # | 组件 | Kerberos 值 | SIMPLE 值 | 切换方向 |
|---|------|------------|----------|---------|
| 1 | `conf/hadoop/core-site.xml` | `<value>kerberos</value>` | `<value>simple</value>` | 替换 `<value>` |
| 2 | `conf/spark/spark-defaults.conf` | `authentication kerberos` | `authentication simple` | 替换末尾值 |
| 3 | `conf/hive/hive-site.xml` | `<value>KERBEROS</value>` + Metastore Kerberos 开启 | `<value>NOSASL</value>` + Metastore 属性名加 `!` 前缀禁用 | KERBEROS→`!kerberos` / `NOSASL`→`KERBEROS` + 去掉 `!` |
| 4 | `conf/trino/catalog/{hive,hudi,iceberg}.properties` | `KERBEROS` + principal 未注释 | `NONE` + principal 加 `#` 注释 | KERBEROS↔NONE + principal 去注释/加注释 |
| 5a | `conf/hbase/hbase-site.xml` | `<value>kerberos</value>` | `<value>simple</value>` | 替换 `<value>` |
| 5b | docker-compose HBase volumes | classpath 7 个 mounts（含 krb5.conf + keytabs） | `./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro` + hbase-site.xml | 整块替换 |
| 5c | docker-compose HBase command | `kinit -kt ... && ...start` | 纯 `...start` | 去掉/加回 kinit 前缀 |
| 5d | docker-compose HBase environment | `JAVA_TOOL_OPTIONS`（krb5.conf 系统属性） | `HADOOP_USER_NAME: hbase`（SIMPLE 态必须） | 两块互斥替换 |

---

## 方向 A：Kerberos → SIMPLE（回滚）

**使用场景**：KDC 救不回来、开发阶段想免 kinit、CI 环境用。
**复杂度**：⭐⭐⭐⭐⭐（HBase 需同时改 conf + docker-compose 3 层）

### Step 0 — 备份（强制）

```bash
cd /home/admin/lakehouse
# 方式 A（推荐）：git diff 快照
git diff > .switch-state.before-simple/config.patch

# 方式 B：非 git 仓库
mkdir -p .switch-state.before-simple/conf
cp -r conf/hadoop conf/spark conf/hive conf/trino conf/hbase .switch-state.before-simple/conf/
cp docker-compose.yaml .switch-state.before-simple/
```

### Step 1-4 — HDFS / Spark / Hive / Trino

```bash
# HDFS
sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hadoop/core-site.xml

# Spark
sed -i 's|spark.hadoop.hadoop.security.authentication kerberos|spark.hadoop.hadoop.security.authentication simple|' \
  conf/spark/spark-defaults.conf

# Hive: HS2 NOSASL + Metastore Kerberos 加 ! 前缀禁用
sed -i 's|<value>KERBEROS</value>|<value>NOSASL</value>|' conf/hive/hive-site.xml
sed -i 's|<name>hive.metastore.kerberos|<name>!hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>hive.metastore.sasl|<name>!hive.metastore.sasl|g' conf/hive/hive-site.xml

# Trino (3 个 catalog 文件)
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  sed -i 's/hive.metastore.authentication.type=KERBEROS/hive.metastore.authentication.type=NONE/' "$f"
  sed -i 's/hive.hdfs.authentication.type=KERBEROS/hive.hdfs.authentication.type=NONE/' "$f"
  sed -i 's/^hive.metastore.service.principal/# hive.metastore.service.principal/' "$f"
  sed -i 's/^hive.metastore.client.principal/# hive.metastore.client.principal/' "$f"
  sed -i 's/^hive.metastore.client.keytab/# hive.metastore.client.keytab/' "$f"
  sed -i 's/^hive.hdfs.trino.principal/# hive.hdfs.trino.principal/' "$f"
  sed -i 's/^hive.hdfs.trino.keytab/# hive.hdfs.trino.keytab/' "$f"
done
```

### Step 5 — HBase（最复杂，3 层全改）

#### 5a. hbase-site.xml

```bash
# hadoop.security.authentication + hbase.security.authentication 两处
sed -i 's|<value>kerberos</value>|<value>simple</value>|' conf/hbase/hbase-site.xml
# 验证：
grep -A1 'security.authentication' conf/hbase/hbase-site.xml | grep '<value>'
```

> **注意**：`hbase-site.xml` 里还内嵌了 `hadoop.security.auth_to_local` Kerberos 规则。SIMPLE 模式下 Hadoop 不会跑 auth_to_local，所以留着不触发——想彻底清理手动删掉那个 `<property>` 块即可。

#### 5b. docker-compose.yaml — HBase volumes

Kerberos 当前值（classpath 挂载）：
```yaml
volumes:
  - ./conf/hadoop/core-site.xml:/opt/hbase/conf/core-site.xml:ro   # classpath 关键！
  - ./conf/hadoop/hdfs-site.xml:/opt/hbase/conf/hdfs-site.xml:ro
  - ./conf/hadoop/yarn-site.xml:/opt/hbase/conf/yarn-site.xml:ro
  - ./conf/hadoop/mapred-site.xml:/opt/hbase/conf/mapred-site.xml:ro
  - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
  - ./conf/kerberos/krb5.conf:/etc/krb5.conf:ro
  - ./conf/kerberos/keytabs:/etc/security/keytabs:ro
```

SIMPLE 改成（只 2 行，独立 Hadoop conf 目录）：
```yaml
volumes:
  - ./conf/hbase-hadoop:/opt/hadoop/etc/hadoop:ro
  - ./conf/hbase/hbase-site.xml:/opt/hbase/conf/hbase-site.xml:ro
```

> **为什么？** Kerberos 模式下必须把 kerberos core-site.xml 挂到 `/opt/hbase/conf/core-site.xml`（HBase classpath），因为 Hadoop 2.10.2 的 Configuration **不认 HADOOP_CONF_DIR env**。SIMPLE 模式下 Hadoop 默认就是 simple，不需要挂任何东西到 classpath。

#### 5c. docker-compose.yaml — command（去掉 kinit 前缀）

```yaml
# Kerberos:
command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase master start"]
# SIMPLE:
command: ["bash", "-c", "mkdir -p /opt/hbase/zookeeper/data && /opt/hbase/bin/hbase master start"]
```

hbase-regionserver 同理：
```yaml
# Kerberos: kinit -kt ... && start
# SIMPLE: 纯 start
```

#### 5d. docker-compose.yaml — environment（关键坑！）

```yaml
# Kerberos:
environment:
  HBASE_HOME: /opt/hbase
  JAVA_HOME: /opt/java/openjdk
  JAVA_TOOL_OPTIONS: "-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
# SIMPLE（必须加 HADOOP_USER_NAME=hbase，否则 PermissionDenied）:
environment:
  HBASE_HOME: /opt/hbase
  JAVA_HOME: /opt/java/openjdk
  HADOOP_USER_NAME: hbase
```

> **🔥 这个坑踩了才知道**：SIMPLE 模式下容器以 `root` 运行 → Hadoop 认定当前用户是 root → `/hbase` 归 hbase → PermissionDenied。Kerberos 态 principal `hbase/...` 通过 auth_to_local 映射成 `hbase` 就没这问题。

#### 5a-5d 一键 Python（switch-to-simple.sh 内含）

非 git 仓库时手动跑：
```python
# 替换 master/rs 的 volumes + command + environment 四块
# 完整代码见 scripts/switch-to-simple.sh
```

### Step 6 — 全栈重启 + 验证

```bash
docker compose down && docker compose up -d
sleep 60

# 一键检查
./scripts/check-auth-status.sh --live
```

验证标准：
```bash
# ✅ 1. HDFS 无需 kinit
docker exec namenode hdfs dfs -ls /
# ✅ 2. HBase Master Web UI
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:16010/   # 预期 200
# ✅ 3. Spark SQL SELECT 1
docker exec spark spark-sql -e "SELECT 1"
# ✅ 4. Trino
docker exec trino trino --execute "SELECT 1"
```

---

## 方向 B：SIMPLE → Kerberos（切回）

**使用场景**：开发完成要上 Kerberos、重建环境。
**复杂度**：⭐⭐⭐（比 Kerberos→SIMPLE 简单，因为 KDC 帮你兜住 HBase 的权限问题）

### ⚠️ 前置：KDC 必须先起来

```bash
docker compose up -d kerberos
sleep 20
# 验证 KDC 活着
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'getprincs' | wc -l
# 预期 ≥ 18 个 principal
```

### HBase principal 检查

```bash
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'getprincs' | grep hbase
# 预期: hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM + hbase/hbaseregionserver...
# 如果没有（KDC 重建时常见），手动补：
docker exec kerberos kadmin.local -q "addprinc -randkey hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM"
docker exec kerberos kadmin.local -q "addprinc -randkey hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM"
docker exec kerberos kadmin.local -q "ktadd -k /etc/security/keytabs/hbase.service.keytab \
  hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM \
  hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM"
```

### Step 1-4 — HDFS / Spark / Hive / Trino（sed 反向）

```bash
# HDFS
sed -i 's|<value>simple</value>|<value>kerberos</value>|' conf/hadoop/core-site.xml
# Spark
sed -i 's|spark.hadoop.hadoop.security.authentication simple|spark.hadoop.hadoop.security.authentication kerberos|' conf/spark/spark-defaults.conf
# Hive
sed -i 's|<value>NOSASL</value>|<value>KERBEROS</value>|' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.kerberos|<name>hive.metastore.kerberos|g' conf/hive/hive-site.xml
sed -i 's|<name>!hive.metastore.sasl|<name>hive.metastore.sasl|g' conf/hive/hive-site.xml
# Trino
for f in conf/trino/catalog/hive.properties conf/trino/catalog/hudi.properties conf/trino/catalog/iceberg.properties; do
  sed -i 's/hive.metastore.authentication.type=NONE/hive.metastore.authentication.type=KERBEROS/' "$f"
  sed -i 's/hive.hdfs.authentication.type=NONE/hive.hdfs.authentication.type=KERBEROS/' "$f"
  sed -i 's/^# hive.metastore.service.principal/hive.metastore.service.principal/' "$f"
  sed -i 's/^# hive.metastore.client.principal/hive.metastore.client.principal/' "$f"
  sed -i 's/^# hive.metastore.client.keytab/hive.metastore.client.keytab/' "$f"
  sed -i 's/^# hive.hdfs.trino.principal/hive.hdfs.trino.principal/' "$f"
  sed -i 's/^# hive.hdfs.trino.keytab/hive.hdfs.trino.keytab/' "$f"
done
```

### Step 5 — HBase（反向操作，简表）

| 层 | SIMPLE → Kerberos 操作 | 说明 |
|----|----------------------|------|
| 5a hbase-site.xml | `simple → kerberos` | sed 替换 `<value>` |
| 5b volumes | `hbase-hadoop/ → 7 个 classpath mounts` | 把 Kerberos 版 core-site.xml 挂到 `/opt/hbase/conf/core-site.xml`（Hadoop 2.10.2 Configuration 不认 HADOOP_CONF_DIR env） |
| 5c command | 加回 `kinit -kt ... &&` 前缀 | KDC principal 必须存在 |
| 5d environment | **去掉** `HADOOP_USER_NAME: hbase`，**加回** `JAVA_TOOL_OPTIONS`（krb5.conf 系统属性） | Kerberos 态 auth_to_local 自动把 principal 映射成 hbase，不需要 HADOOP_USER_NAME |

> **最稳做法**（git 仓库）：`git checkout HEAD -- docker-compose.yaml` 直接恢复 Kerberos 版 HBase 定义。非 git 仓库用 switch-to-kerberos.sh 里的 Python 替换。

### Step 6 — 全栈重启 + 验证

```bash
# 推荐：KDC 先起来再全栈 up
docker compose down
docker compose up -d kerberos zookeeper mysql postgres
sleep 30
docker compose up -d
sleep 90

./scripts/check-auth-status.sh --live
# 预期: 全栈 auth 一致：KERBEROS + 21/22 Up

# HDFS 验证（Kerberos 态需要 kinit 或 delegation token）
docker exec namenode bash -c "kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM && hdfs dfs -ls /"
# Spark SQL（容器里有 delegation token，直接跑）
docker exec spark spark-sql -e "SELECT 1"
```

---

## 已知坑点总表

| # | 坑 | 影响 | 规避 |
|---|----|------|------|
| 1 | HBase 2.5.3 内置 Hadoop **2.10.2**，asyncfs 按 `HdfsFileStatus=class` 编译，不能塞 3.x jars | `NoClassDefFoundError: Interners` | **永远保留 Hadoop 2.10.2 原生 jars** |
| 2 | Hadoop 2.10.2 Configuration **不认 `HADOOP_CONF_DIR` env**，只认 classpath 上的 core-site.xml | Kerberos 配置读不到 | HBase Kerberos 态必须把 kerberos core-site.xml 挂到 `/opt/hbase/conf/core-site.xml` |
| 3 | SIMPLE 态 HBase Master/RS 以 `root` 运行 → HDFS `/hbase` owner=`hbase` → PermissionDenied | Master 直接 abort | env 加 `HADOOP_USER_NAME: hbase`（Kerberos 态不需要） |
| 4 | `kdb5_util create -s -P password` 在某些 KDC 版本**不生成 `.stash`** → kadmind 报 `Can not fetch master key` | KDC 起不来 | 拆成两步：`kdb5_util create -s` + 显式 `kdb5_util stash -P password` |
| 5 | 切 Kerberos 时 KDC principal 丢了（重建 KDC 时 keytab exists → skip 但 principal 也没 add） | HBase `kinit: principal not found` | 切换前 `kadmin -q 'getprincs' \| grep hbase` 确认 HBase principal 存在 |
| 6 | `switch-to-simple.sh` 早期版 `import yaml` 但系统没装 PyYAML | `ModuleNotFoundError: No module named 'yaml'` | 只用到 `re`（纯标准库）就够了 |
| 7 | Hive Beeline Kerberos 外部连接必须用 FQDN（`hiveserver.lakehouse.com` 不是 IP） | GSSAPI principal 匹配失败 | JDBC URL 用 hostname |

---

## Auth 相关文件清单

| 文件 | 角色 | 被谁引用 |
|------|------|---------|
| `scripts/switch-to-simple.sh` | Kerberos→SIMPLE 一键脚本 | docker-compose + 5 conf + python3 |
| `scripts/switch-to-kerberos.sh` | SIMPLE→Kerberos 一键脚本 | docker-compose（git checkout HEAD）+ 5 conf |
| `scripts/check-auth-status.sh` | 静态 + `--live` 状态检查 | 独立 / 被两个 switch source |
| `build/kerberos/kdc-init.sh` | KDC 重建（含 .stash 修复） | KDC 容器 entrypoint |
| `conf/hadoop/core-site.xml` | 全局 auth 开关 | 所有组件（Kerberos auth_to_local rules） |
| `conf/hbase/hbase-site.xml` | HBase 内嵌 Hadoop conf | HBase classpath |
| `conf/kerberos/keytabs/*.keytab` | 所有服务 Kerberos key | 各服务 kinit / delegation token |
| `conf/kerberos/krb5.conf` | realm 配置 | 各容器 JVM 系统属性 |

---

## 彻底清理 KDC（可选）

如果你**不再需要 Kerberos**（比如完全放弃）：

```bash
docker compose down
rm -rf data/kerberos/* conf/kerberos/keytabs/*.keytab
# ⚠️ 清理后切回 Kerberos 必须重建 KDC（所有 principal + keytabs）
# KDC master key 丢了就永远恢复不了
docker compose up -d
```

---

*最后更新：2026-09-27 — 实测双向切换全部跑通，坑点表来自 3 次真实切换踩坑*
