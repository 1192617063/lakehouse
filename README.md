# Lakehouse 湖仓一体学习平台

> 基于 Docker Compose 的**一键启动湖仓平台**，集成 **Kerberos 完整配置（默认启用）**
> 支持 Iceberg / Hudi / Paimon / HBase 四种湖存储 + Spark / Flink / Trino / Hive / Doris 五种查询引擎。
> 包含完整的 **实时 CDC 管道**、**离线数仓**、**存量迁移方案**。

---

## ⚡ 快速开始

```bash
# 1. 解压 Release 包（首次使用）
tar -xzf lakehouse-release-*.tar.gz && cd lakehouse-release-*

# 2. 准备环境变量（可选，默认华为云镜像源）
cp .env.example .env     # 或直接用已有的 .env
# 如需换阿里云：编辑 .env 修改各 IMAGE 的 registry

# 3. 一键启动（首次启动会自动建 KDC + 生成所有 keytabs）
docker compose up -d
sleep 40   # 等 Kerberos + NameNode + HBase 起来

# 4. 快速验证
docker compose ps              # 所有服务 Up
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'listprincs'  # 19 principal
docker exec spark bash -c 'echo lakehouse123 | kinit lakehouse@LAKEHOUSE.COM && spark-sql --master local[1] -e "SHOW DATABASES;"'
docker exec trino bash -c 'trino --execute "SHOW CATALOGS;"'

# 5. 停止
docker compose down
```

---

## 核心组件（20 个容器，Kerberos auth）

| # | 组件 | 版本 | 容器 Hostname | Kerberos Principal |
|---|------|------|--------------|-------------------|
| 1 | HDFS NameNode | 3.3.6 | `namenode.lakehouse.com` | `nn/namenode.lakehouse.com` |
| 2 | HDFS DataNode | 3.3.6 | `datanode.lakehouse.com` | `dn/datanode.lakehouse.com` |
| 3 | YARN ResourceManager | 3.3.6 | `resourcemanager.lakehouse.com` | `yarn/resourcemanager.lakehouse.com` |
| 4 | YARN NodeManager | 3.3.6 | `nodemanager.lakehouse.com` | `yarn/nodemanager.lakehouse.com` |
| 5 | Kerberos KDC | latest | `kerberos.lakehouse.com` | `admin/admin@LAKEHOUSE.COM` |
| 6 | **Zookeeper** | 3.9 | `zookeeper.lakehouse.com` | HBase / Kafka 协调 |
| 7 | Hive Metastore | 3.1.3 | `hivemetastore.lakehouse.com` | `hive/hivemetastore.lakehouse.com` |
| 8 | HiveServer2 | 3.1.3 | `hiveserver.lakehouse.com` | `hive/hiveserver.lakehouse.com` + `HTTP/hiveserver` |
| 9 | Iceberg REST | 1.10.1 | `icebergrest.lakehouse.com` | `iceberg/icebergrest.lakehouse.com` |
| 10 | Flink JobManager | 1.19.1 | `flinkjobmanager.lakehouse.com` | `flink/flinkjobmanager.lakehouse.com` |
| 11 | Flink TaskManager | 1.19.1 | — | 同 JM |
| 12 | Spark | 3.5.6 | `sparkmaster.lakehouse.com` | `spark/sparkmaster.lakehouse.com` |
| 13 | Trino | 482 | `trino.lakehouse.com` | `trino/trino.lakehouse.com` |
| 14 | **HBase Master** | 2.5.3 | `hbasemaster.lakehouse.com` | `hbase/hbasemaster.lakehouse.com` |
| 15 | **HBase Regionserver** | 2.5.3 | `hbaseregionserver.lakehouse.com` | `hbase/hbaseregionserver.lakehouse.com` |
| 16 | Kafka | 3.9.0 | `kafka.lakehouse.com` | `kafka/kafka.lakehouse.com` |
| 17 | Doris FE | 2.1.7 | — | 独立认证 |
| 18 | Doris BE | 2.1.7 | — | 独立认证 |
| 19 | MySQL | 8.0.46 | — | Hive Metastore DB |
| 20 | MongoDB | 7.0 | — | 演示数据源（可迁移到 HBase） |

> ⚠️ **Principal 命名规范**：hostname 部分**无连字符**（`flinkjobmanager` 不是 `flink-jobmanager`）。Kerberos GSSAPI 严格匹配。
> 
> 🔑 **Auth 模式**：默认 **Kerberos**。KDC 首次启动自动创建所有 principal + 生成 keytabs。
> 学习需要切换 SIMPLE：执行 `./scripts/switch-to-simple.sh`，回切 Kerberos 执行 `./scripts/switch-to-kerberos.sh`。

---

## Kerberos Realm

| 项 | 值 |
|----|-----|
| Realm | `LAKEHOUSE.COM`（**全大写**） |
| KDC 端口 | 88/tcp（已映射宿主机） |
| Admin | `admin/admin@LAKEHOUSE.COM` / `admin123` |
| 平台用户 | `lakehouse@LAKEHOUSE.COM` / `lakehouse123` |
| KDC 初始化 | `build/kerberos/kdc-init.sh`（首次启动自动执行） |
| Keytab 路径 | `conf/kerberos/keytabs/*.keytab`（**运行时生成，不入库**） |

### 各引擎 Kerberos 认证矩阵

| 引擎 | 认证方式 | Kerberos 配置 |
|------|---------|--------------|
| Spark on YARN | keytab + delegation token | `spark.yarn.archive` HDFS 预打包 JAR |
| Flink Standalone | keytab 本地 kinit（**关闭 delegation token**） | `security.delegation.tokens.enabled: false` |
| Trino | keytab | `fs.hadoop.enabled=true` |
| HiveServer2 | Kerberos SASL GSSAPI | 外部连接必须用 FQDN |
| Iceberg REST | keytab 自定义 UGI | Java Launcher 初始化 Kerberos |
| HBase | **Kerberos**（keytab 自动登录） | core-site.xml 挂到 HBase classpath（/opt/hbase/conf/core-site.xml）让 Hadoop Configuration 自动加载 kerberos |
| Kafka | SASL_PLAINTEXT + keytab | KRaft 模式内置 |

### Kerberos 重建（关键操作）

```bash
# KDC master key 改了 = 所有 keytab 全废。必须同时删 KDC 数据 + keytabs
docker compose down
rm -rf data/kerberos/* conf/kerberos/keytabs/*.keytab
docker compose up -d kerberos   # 自动重建所有 principal + keytabs
sleep 25
docker compose up -d            # 起所有服务
```

---

## 湖存储格式选型

| 格式 | 适用场景 | 推荐引擎 | 写入 | 查询 |
|------|---------|---------|------|------|
| **Iceberg** | 离线数据湖、ACID、多引擎共存 | Spark, Trino, Flink, Hive | CDC + Spark SQL | Trino/SQL |
| **Hudi** | 实时 CDC Upsert、增量消费 | Spark, Flink | CDC + Spark | Spark |
| **Paimon** | Flink 实时 CDC、Changelog | Flink | CDC | Flink（Trino 有限） |
| **HBase** | KV 点查、宽表、时序数据 | Flink, Spark, API | Flink/Java API | HBase Shell/API |

---

## 目录结构

```
lakehouse-release-*/
├── docker-compose.yaml    # 统一编排入口（20 个服务，含 Zookeeper + HBase）
├── .env.example           # 镜像地址模板（默认华为云源）
├── build/                 # 各组件 Dockerfile + KDC 初始化
│   ├── kerberos/kdc-init.sh   ← 新增组件在此加 principal（已含 HBase）
│   ├── hive/entrypoint.sh
│   ├── iceberg-rest/
│   ├── hbase/              ← HBase 2.5.3 + Java 11 + 原生 Hadoop 2.10.2 jars + Kerberos
│   ├── flink/
│   └── spark/
├── conf/                  # 所有服务配置（keytabs 运行时生成）
│   ├── kerberos/          # krb5.conf + keytabs/（运行时生成）
│   ├── hadoop/            # core/hdfs/yarn/mapred-site.xml（**Kerberos auth**）
│   ├── hbase-hadoop/      # ⚠️ **已废弃**（之前 SIMPLE auth 隔离用）
│   ├── hbase/             # hbase-site.xml（Kerberos + hadoop.security.* 属性内嵌）
│   ├── hive/              # hive-site.xml（KERBEROS HS2 + SASL=true Metastore）
│   ├── flink/
│   ├── spark/             # spark-defaults.conf（**Kerberos auth**）
│   ├── trino/             # catalog/*.properties（**KERBEROS auth**）
│   ├── iceberg/
│   ├── doris/
│   └── mysql/
├── scripts/               # 辅助脚本
├── sql/                   # CDC + 离线数仓 SQL
├── docs/                  # 完整文档体系（10 篇）
├── lib/                   # 预打包二进制（release 不包含）
└── data/                  # 运行时数据（release 不包含）
```

---

## Release 包

Release 包包含：
- ✅ 所有 `build/` `conf/` `scripts/` `sql/` `docs/` `docker-compose.yaml` `.env.example`
- ✅ Kerberos kdc-init.sh（含所有 19 principal 定义）
- ✅ HBase 2.5.3 Dockerfile（含 tarball）
- ❌ **不包含** `data/`（运行时 HDFS 数据）、`lib/`（大二进制）、`keytabs/`（运行时自动生成）

下载解压后直接 `docker compose up -d` 即可，Kerberos KDC 会在首次启动时自动创建所有 principal + 生成所有 keytabs。

### 镜像构建

首次启动需要构建自定义镜像：
- `lakehouse-kdc`（Kerberos KDC）
- `lakehouse-flink`（Flink 1.19.1 + Iceberg/Paimon/HBase connector）
- `lakehouse-spark`（Spark 3.5.6 + Iceberg/Paimon/Hudi connector + Kerberos entrypoint）
- `lakehouse-hbase:2.5.3`（HBase 2.5.3 + 原生 Hadoop 2.10.2 jars + Kerberos classpath core-site.xml 挂载）

如果在离线环境，可以先在线 `docker compose build` 然后 `docker save/load` 导出。

---

## 快速验证各组件

| 组件 | 验证命令 | 预期 |
|------|---------|------|
| Kerberos | `docker exec kerberos kadmin -p admin/admin -w admin123 -q 'listprincs'` | 21 个 principal |
| HDFS | `docker exec namenode hdfs dfs -ls /` | /lakehouse, /tmp, /user, /hbase |
| Spark SQL | `docker exec spark spark-sql -e "SELECT COUNT(*) FROM paimon.cdc_demo.products"` | 5 |
| Flink JM | `curl -s http://localhost:8081/overview \| python3 -c "..."` | 1 TM / 8 slots |
| Trino | `docker exec trino /usr/bin/trino --catalog hive --execute "SHOW SCHEMAS"` | cdc_demo, default |
| Hive | `docker exec hive-server beeline -u 'jdbc:hive2://localhost:21066/default' -e "SHOW DATABASES;"` | default, cdc_demo |
| Iceberg REST | `curl -s http://localhost:8181/v1/namespaces` | JSON namespaces |
| HBase | `docker exec hbase-master /opt/hbase/bin/hbase shell` 然后 `status` | active master |
| Kafka | `docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` | topic 列表 |
| **全栈状态** | `docker compose ps` | 20 个 Up |

---

## 已知限制

| 限制 | 说明 | 规避 |
|------|------|------|
| Trino 482 不识别 PaimonSerDe | Trino Paimon connector 不完整 | 用 Flink/Spark 查 Paimon |
| Hive Tez 0.10.2 不兼容 Hive 3.1.3 | 当前用 MR 引擎 | Tez 必须 0.9.1 |
| Flink standalone 必须关 delegation token | Flink + Hadoop delegation token NPE | `security.delegation.tokens.enabled: false` |
| Spark Kryo + yarn.archive → EOF | Kryo serializer + spark.yarn.archive 冲突 | 用默认 JavaSerializer |
| Doris 无 Kerberos | Doris 用独立认证 | FE/BE 用 root 账号 |
| **HBase Kerberos（已修复 ✅）** | HBase 2.5.3 内置 Hadoop 2.10.2，asyncfs 不兼容 3.x HdfsFileStatus（interface≠class）；Hadoop 2.10.2 Configuration 不认 HADOOP_CONF_DIR env | 保留原生 2.10.2 jars；core-site.xml 挂到 /opt/hbase/conf/core-site.xml（classpath 自动加载） |
| **Kerberos → SIMPLE auth 切换** | 需改 4 个配置文件 + 重启全栈 | `./scripts/switch-to-simple.sh`（一键），回切 Kerberos：`./scripts/switch-to-kerberos.sh` |

---

## 文档索引

| 文档 | 内容 |
|------|------|
| [总览笔记](docs/LAKEHOUSE_OVERVIEW.md) | ⭐ 平台架构、组件表、快速上手（**先读这个**） |
| [Kerberos 清单](docs/KERBEROS_PRINCIPALS.md) | 所有 principal + 管理命令 + 新增组件 Checklist |
| [**Kerberos→SIMPLE 回滚**](docs/SWITCH_TO_SIMPLE.md) | ⭐ **一键回滚到 SIMPLE auth**（含完整步骤 + HBase 3 层改动详解 + 对比表） |
| [**SIMPLE→Kerberos 切换**](docs/SWITCH_TO_KERBEROS.md) | ⭐ **从 SIMPLE auth 切回 Kerberos 完整步骤**（含一键脚本 + HBase 修复） |
| [存量迁移与集群规划](docs/MIGRATION_AND_CLUSTER_PLANNING.md) | 迁移方法论 + 实时/离线数仓架构设计 |
| [MongoDB → HBase 迁移](docs/MIGRATION_MONGODB_TO_HBASE.md) | MongoDB 存量数据迁移实施指南 |
| [CDC 管道](docs/CDC_PIPELINE.md) | MySQL/PG → Flink CDC → Kafka → Iceberg/Hudi/Paimon |
| [Spark 离线湖仓](docs/SPARK_OFFLINE_LAKEHOUSE.md) | Spark on YARN Kerberos + delegation token |
| [DBeaver 连接](docs/DBEAVER_CONNECTION_GUIDE.md) | Windows DBeaver Kerberos 连 HiveServer2 |
| [部署指南](docs/DEPLOYMENT_NOTES.md) | 全新环境从零部署步骤 |
| [故障排查](docs/TROUBLESHOOTING.md) | 所有 Kerberos 坑点 + 解决方案 |
| [生产操作](docs/PRODUCTION_DATA_OPS.md) | 存量导入、积压处理、HFile BulkLoad |

### 一键脚本

```bash
# 从 SIMPLE 切到 Kerberos（配置改完后需 docker compose down && up -d）
./scripts/switch-to-kerberos.sh

# 回滚到 SIMPLE
./scripts/switch-to-simple.sh
```

---

## 安全说明

- `.gitignore` 排除 `data/` `lib/` `*.log` `conf/kerberos/keytabs/*.keytab`
- Kerberos keytabs 由 KDC 在容器启动时自动生成，**不入库**
- `lakehouse@LAKEHOUSE.COM` 是平台默认用户（密码 `lakehouse123`），生产环境请重置
- Kerberos realm `LAKEHOUSE.COM` 固定，所有 service principal hostname 必须无连字符

---

*最后更新：2026-09-27 — 20 容器**全栈 Kerberos** 完成（默认 Kerberos，可一键切换 SIMPLE）：HBase Kerberos 修复（保留 Hadoop 2.10.2 原生 jars + core-site.xml 挂 classpath 让 Hadoop Configuration 自动加载 kerberos）+ KDC 全 principal/keytabs（含 HBase master+rs）+ Spark/Flink/Trino/Hive/Doris 全回归通过 + Release Ready*
