# 湖仓一体学习平台 — 总览笔记

> 本文档是**全平台总览**，串起所有组件的架构、认证、数据流向与适用场景。
> 各组件的深入技术细节请参考对应专题笔记，具体操作命令以本文档为准。

---

## 一、平台架构总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         应用 / 客户端层                                      │
│  DBeaver │ Trino CLI │ Spark SQL │ Flink SQL │ Hive Beeline │ Doris FE      │
└───────────────┬──────────────┬──────────────┬───────────────────────────────┘
                │              │              │
          Kerberos TGT     Kerberos TGT   Kerberos TGT
                │              │              │
┌───────────────▼──────────────▼──────────────▼───────────────────────────────┐
│                        计算引擎层                                            │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐                │
│  │   Spark 3.5 │ │  Flink 1.19│ │  Trino 482  │ │ Doris 2.1.x │                │
│  │ (YARN + K) │ │ (Standalone│ │ (Kerberos)  │ │ (OLAP分析)  │                │
│  │  .5.6      │ │  +Kerberos)│ │              │ │             │                │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘                │
│        │              │              │              │                        │
│  ┌─────┴──────────────┴──────────────┴──────────────┴──────┐                │
│  │              元数据 & 协议层                                │                │
│  │  Hive Metastore (9083) │ Iceberg REST (8181) │ HBase Thrift (9090) │                │
│  └──────────────┬──────────────────────┬─────────────────────┘                │
└─────────────────┼──────────────────────┼───────────────────────────────────┘
                  │                      │
┌─────────────────▼──────────────────────▼───────────────────────────────────┐
│                         湖存储层                                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐             │
│  │  Iceberg   │  │   Hudi     │  │   Paimon   │  │   HBase    │             │
│  │  (REST-CAT) │  │ (CDC+Upsert)│  │ (MOR表)   │  │ (KV/列族)   │             │
│  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘             │
│         │                │                │                │                 │
│  ┌──────┴────────────────┴────────────────┴────────────────┴─────┐          │
│  │                   HDFS 3.3.6 (Kerberos)                       │          │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────┐  │          │
│  │  │  NameNode   │  │  DataNode  │  │ YARN RM    │  │ DN   │  │          │
│  │  └────────────┘  └────────────┘  └────────────┘  └──────┘  │          │
│  └────────────────────────────────────────────────────────────────┘          │
└────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         数据源层                                              │
│  MySQL 8.0 │ Postgres 16 │ MongoDB 7.0 │ Kafka 3.9.0 (Kerberos)              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         安全基础设施                                          │
│  MIT Kerberos KDC (88/749) — realm: LAKEHOUSE.COM                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、组件清单与 Kerberos Principal 对照

| # | 组件 | 版本 | Hostname / FQDN | Kerberos Principal | 端口 | 说明 |
|---|------|------|----------------|-------------------|------|------|
| 1 | HDFS NameNode | 3.3.6 | `namenode.lakehouse.com` | `nn/namenode.lakehouse.com@LAKEHOUSE.COM` | 9870, 8020 | HDFS 元数据 |
| 2 | HDFS DataNode | 3.3.6 | `datanode.lakehouse.com` | `dn/datanode.lakehouse.com@LAKEHOUSE.COM` | 9864, 9866 | HDFS 数据块 |
| 3 | YARN ResourceManager | 3.3.6 | `resourcemanager.lakehouse.com` | `yarn/resourcemanager.lakehouse.com@LAKEHOUSE.COM` | 8088, 8090 | YARN 调度 |
| 4 | YARN NodeManager | 3.3.6 | `nodemanager.lakehouse.com` | `yarn/nodemanager.lakehouse.com@LAKEHOUSE.COM` | 8042 | YARN 执行节点 |
| 5 | Hive Metastore | 3.1.3 | `hivemetastore.lakehouse.com` | `hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM` | 9083 | 表元数据 |
| 6 | HiveServer2 | 3.1.3 | `hiveserver.lakehouse.com` | `hive/hiveserver.lakehouse.com@LAKEHOUSE.COM` + `HTTP/hiveserver.lakehouse.com` | 21066, 10002 | SQL 服务 |
| 7 | Iceberg REST | 1.5.2 | `icebergrest.lakehouse.com` | `iceberg/icebergrest.lakehouse.com@LAKEHOUSE.COM` | 8181 | Iceberg REST 目录 |
| 8 | Flink JobManager | 1.19.1 | `flinkjobmanager.lakehouse.com` | `flink/flinkjobmanager.lakehouse.com@LAKEHOUSE.COM` | 8081 | Flink 实时任务 |
| 9 | Spark | 3.5.6 | `sparkmaster.lakehouse.com` | `spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM` | 4040, 8080 | Spark 离线/批处理 |
| 10 | Trino | 482 | `trino.lakehouse.com` | `trino/trino.lakehouse.com@LAKEHOUSE.COM` | 8085 | 联邦查询引擎 |
| 11 | Doris FE | 2.1.7 | — | — | 8030, 9030 | Doris OLAP FE |
| 12 | Doris BE | 2.1.7 | — | — | 8040, 9060 | Doris OLAP BE |
| 13 | Kafka | 3.9.0 | `kafka.lakehouse.com` | `kafka/kafka.lakehouse.com@LAKEHOUSE.COM` | 9092 | 消息队列 |
| 14 | HBase Master | 2.5.3 | `hbasemaster.lakehouse.com` | `hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM` | 16010, 9090 | HBase 列族存储 |
| 15 | HBase Regionserver | 2.5.3 | `hbaseregionserver.lakehouse.com` | `hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM` | 16020 | HBase 区域服务器 |
| 16 | MySQL | 8.0.46 | — | — | 3306 | 源数据 + Hive Meta |
| 17 | PostgreSQL | 16.4 | — | — | 5432 | 源数据 |
| 18 | MongoDB | 7.0 | — | — | 27017 | 源数据（需迁移→HBase） |
| 19 | Kerberos KDC | latest | `kerberos.lakehouse.com` | `admin/admin@LAKEHOUSE.COM` | 88, 749 | 认证中心 |

**平台用户**：`lakehouse@LAKEHOUSE.COM`（密码 `lakehouse123`）—— 所有应用登录用。

> ⚠️ **Principal 命名规范**：hostname 部分**不能有连字符**（`-`）—— Kerberos GSSAPI 匹配 service principal 时，连字符会导致 Java hostname → principal hostname 转换失败。本平台统一使用无连字符格式。

---

## 三、技术选型与适用场景

### 3.1 湖存储格式对比

| 格式 | 适用场景 | 写入引擎 | 查询引擎 | 优点 | 局限 |
|------|---------|---------|---------|------|------|
| **Apache Iceberg** | 离线数据湖、ACID 批处理、多引擎共存 | Spark, Flink, Trino, Hive | Spark, Trino, Flink, Hive, Doris | 成熟稳定、REST Catalog、时间旅行、兼容最好 | CDC Upsert 需额外处理 |
| **Apache Hudi** | 实时 CDC Upsert 场景、增量消费、 MOR 表 | Spark, Flink | Spark, Hive, Flink, Trino(有限) | 原生 CDC、增量查询、支持 MOR 合并 | Trino 支持较弱 |
| **Apache Paimon** | Flink 实时 CDC 首选、流式入湖、MOR 表 | Flink, Spark(有限) | Flink, Spark(有限), Trino | Flink 原生、流式读写最好、支持 Changelog | Spark/Trino 支持仍在改进 |
| **Apache HBase** | KV/列族存储、随机点查、宽列、时序数据 | Flink, Spark, Java API | Spark, Flink, HBase Shell, Hive/JDBC(有限) | 亚秒级随机读、列族灵活、海量数据 | 不支持 SQL JOIN、不适合分析查询 |

### 3.2 计算引擎选型

| 引擎 | 核心用途 | 认证方式 | 典型场景 |
|------|---------|---------|---------|
| **Spark 3.5.6** | 离线批处理、ETL、大规模 SQL | YARN + Kerberos keytab + delegation token | 每日/每小时批处理、批量导出、机器学习特征工程 |
| **Flink 1.19.1** | 实时流处理、CDC、低延迟 | Standalone + Kerberos keytab（**关闭 delegation token**） | 实时 CDC 入湖、窗口计算、实时告警 |
| **Trino 482** | 联邦查询、交互式即席查询 | Kerberos keytab | 跨湖（Iceberg + Hive + Doris）联合查询、BI 直连 |
| **Doris 2.1.7** | OLAP 分析、实时可查 | 无 Kerberos（独立认证） | 高频低延迟分析、点查、报表 |
| **Hive 3.1.3** | SQL 兼容层、ETL | Kerberos SASL + TEZ | 传统 Hive SQL 脚本、BI 工具兼容 |

### 3.3 数据流全景

```
数据源                    捕获方式                   湖存储                    查询引擎
───────────────────────────────────────────────────────────────────────────────────
MySQL (8.0)  ──── Flink CDC ────► Kafka ── Flink ──► Paimon/Iceberg
Postgres (16) ─── Flink CDC ────► Kafka ── Flink ──► Paimon/Iceberg
MongoDB (7.0) ─── Flink CDC ────► Kafka ── Flink ──► Paimon/Iceberg
                 存量迁移   ────────► Spark ──────► HBase

Kafka (Kerberos) ── Flink ──► Paimon/Iceberg
                              Spark YARN ──► Iceberg/Hudi

Paimon ──► Flink (实时下游) + Spark + Trino (离线查询)
Iceberg ──► Spark + Flink + Trino + Doris (多引擎查询)
HBase ──► Spark + Flink + HBase Shell (KV 查询)
Hudi ──► Spark + Flink (CDC Upsert)
Doris ──► Stream Load 或 Paimon/Iceberg 同步
```

---

## 四、快速上手

### 4.1 启动全平台

```bash
cd /home/admin/lakehouse
docker compose up -d
sleep 30  # 等 Kerberos KDC 和 namenode 起来
```

### 4.2 各组件快速验证

| 组件 | 验证命令 | 预期 |
|------|---------|------|
| Kerberos | `docker exec kerberos kadmin -p admin/admin -w admin123 -q 'listprincs'` | 18 个 principal |
| HDFS | `docker exec namenode bash -c 'hdfs dfs -ls /lakehouse'` | 目录存在 |
| YARN | `curl -s http://localhost:8088/ws/v1/cluster/info` | JSON 返回 |
| Spark SQL | `docker exec spark bash -c 'echo lakehouse123 \| kinit lakehouse@LAKEHOUSE.COM && spark-sql --master local[1] -e "SHOW DATABASES;"'` | `default`, `cdc_demo` |
| Flink SQL | `docker exec flink-sql-client bash -c 'ls /opt/flink/lib/'` | 有 iceberg/paimon jar |
| Trino | `docker exec trino bash -c 'trino --execute "SHOW CATALOGS;"'` | `iceberg`, `hive`, `hudi` |
| Hive Beeline | `docker exec hive-server bash -c 'echo lakehouse123 \| kinit lakehouse@LAKEHOUSE.COM && timeout 15 beeline -u "jdbc:hive2://hiveserver.lakehouse.com:21066/default;auth=KERBEROS;principal=hive/hiveserver.lakehouse.com@LAKEHOUSE.COM" -e "SHOW DATABASES;"'` | `default`, `cdc_demo` |
| Iceberg REST | `curl -s http://localhost:8181/v1/namespaces` | JSON namespaces |
| Kafka | `docker exec kafka bash -c 'kafka-topics.sh --bootstrap-server localhost:9092 --command-config /opt/kafka/config/client.properties --list'` | 列出 topic |
| Doris | `curl -s http://localhost:8030/api/bootstrap` | Doris FE JSON |
| HBase | `docker exec hbase-master bash -c 'kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && /opt/hbase/bin/hbase shell -e "version"'` | Version 2.5.3 |

### 4.3 Kerberos 快速管理

```bash
# 查看所有 principal
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'listprincs'

# 重置 lakehouse 密码
docker exec kerberos kadmin -p admin/admin -w admin123 \
  -q 'change_password -pw newpassword lakehouse@LAKEHOUSE.COM'

# 导出用户 keytab（运维用）
docker exec kerberos kadmin -p admin/admin -w admin123 \
  -q 'ktadd -k /tmp/lakehouse.keytab lakehouse@LAKEHOUSE.COM'

# 客户端获取 ticket（lakehouse 用户）
kinit lakehouse@LAKEHOUSE.COM
# 或用密码
echo "lakehouse123" | kinit lakehouse@LAKEHOUSE.COM
klist  # 查看 ticket

# 启动后自动 kinit（配置文件方式）
# 详见各组件专题文档
```

---

## 五、核心配置文件位置

| 配置文件 | 路径 | 说明 |
|---------|------|------|
| docker-compose | `docker-compose.yaml` | 所有服务编排 |
| Hadoop HDFS/YARN | `conf/hadoop/core-site.xml`, `conf/hadoop/hdfs-site.xml`, `conf/hadoop/yarn-site.xml` | Kerberos + 地址 |
| Kerberos krb5.conf | `conf/kerberos/krb5.conf` + 各容器独立 krb5.conf | realm + domain_realm |
| Kerberos keytabs | `conf/kerberos/keytabs/*.keytab` | 所有服务 principal 的 keytab |
| KDC 初始化脚本 | `build/kerberos/kdc-init.sh` | 新建服务在此加 principal |
| Hive | `conf/hive/hive-site.xml` + `build/hive/entrypoint.sh` | Kerberos SASL + TEZ + ORC |
| Spark | `conf/spark/spark-defaults.conf` + `conf/spark/spark-entrypoint.sh` | Kerberos + delegation token + Iceberg/Paimon 目录 |
| Flink | `conf/flink/flink-conf.yaml` + `conf/flink/flink-sql-gateway-config.yaml` | **关闭 delegation token** + SQL Gateway |
| Iceberg REST | `conf/iceberg/iceberg-rest.properties` + `build/iceberg-rest/entrypoint.sh` | Kerberos + REST catalog |
| Trino | `conf/trino/config.properties` + `conf/trino/catalog/*.properties` | Kerberos + Iceberg/Hive/Hudi catalog |
| HBase | `conf/hbase/hbase-site.xml` + `build/hbase/Dockerfile` | Kerberos + HDFS root |
| Kafka | Kafka 镜像内置 SASL_PLAINTEXT + keytab | Kerberos SASL |
| Doris | `conf/doris/fe.conf` + `conf/doris/be.conf` | 独立认证（无 Kerberos） |

---

## 六、专题文档索引

| 专题 | 文件 | 内容 |
|------|------|------|
| **从零部署指南** | [DEPLOYMENT_NOTES.md](DEPLOYMENT_NOTES.md) | 全新环境 docker compose 部署流程 |
| **部署故障排查** | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | 13-18 节 Kerberos 相关坑点全集 |
| **DBeaver 连接指南** | [DBEAVER_CONNECTION_GUIDE.md](DBEAVER_CONNECTION_GUIDE.md) | Windows DBeaver → HiveServer2 Kerberos 连接 |
| **CDC 实时数据管道** | [CDC_PIPELINE.md](CDC_PIPELINE.md) | MySQL/PG → Flink CDC → Kafka → Iceberg/Hudi/Paimon |
| **Spark 离线湖仓一体** | [SPARK_OFFLINE_LAKEHOUSE.md](SPARK_OFFLINE_LAKEHOUSE.md) | Spark on YARN Kerberos + delegation token + 批量导出 |
| **生产数据操作** | [PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md) | 存量导入、积压处理、HFile BulkLoad |
| **MongoDB→HBase 迁移** | [LEARNING_ROADMAP.md#-case-study-bmongodb--hbase-存量迁移](LEARNING_ROADMAP.md) | Roadmap 高级 Case Study B（已合并） |
| **存量迁移与集群规划** | [LEARNING_ROADMAP.md#-case-study-a存量迁移五步法--数仓分层设计](LEARNING_ROADMAP.md) | Roadmap 高级 Case Study A：五步法 + 选型决策树 + 架构图 + Kerberos 矩阵 |
| **Kerberos Principal 清单** | [KERBEROS_PRINCIPALS.md](KERBEROS_PRINCIPALS.md) | 所有 principal + 用途 + 管理命令 |

---

## 七、固定 IP 规划（172.30.80.0/24）

| IP | 服务 | 说明 |
|----|------|------|
| 172.30.80.3 | flink-jobmanager | Flink JM |
| 172.30.80.4 | flink-taskmanager | Flink TM |
| 172.30.80.5 | flink-sql-client | Flink SQL Client / Gateway |
| 172.30.80.21 | mysql | MySQL 8.0 |
| 172.30.80.22 | postgres | PostgreSQL 16 |
| 172.30.80.23 | mongodb | MongoDB 7.0 |
| 172.30.80.31 | spark | Spark Master |
| 172.30.80.33 | yarn-resource-manager | YARN RM |
| 172.30.80.34 | yarn-node-manager | YARN NM |
| 172.30.80.41 | namenode | HDFS NN |
| 172.30.80.42 | datanode | HDFS DN |
| 172.30.80.43 | hive-metastore | Hive Metastore |
| 172.30.80.44 | hive-server | HiveServer2 |
| 172.30.80.50 | iceberg-rest | Iceberg REST Catalog |
| 172.30.80.60 | kerberos | KDC |
| 172.30.80.63 | hbase-master | HBase Master |
| 172.30.80.64 | hbase-regionserver | HBase Regionserver |
| 172.30.80.2 | kafka | Kafka Broker |
| 172.30.80.12 | doris-fe | Doris FE |
| 172.30.80.14 | doris-be | Doris BE |
| 172.30.80.70 | trino | Trino Coordinator |

---

## 八、新增组件 Checklist

新增一个需要 Kerberos 认证的组件，需要改以下文件：

1. **`build/kerberos/kdc-init.sh`** — 加 `create_and_export "service/hostname.lakehouse.com@${REALM}" "${KEYTAB_DIR}/service.keytab"`
2. **`conf/kerberos/krb5.conf`** — 在 `[domain_realm]` 加短名映射（如 `hbasemaster = LAKEHOUSE.COM`）
3. **`conf/hadoop/core-site.xml`** — 在 `hadoop.security.auth_to_local` RULE 里加（如 `RULE:[2:$1@$0](hbase@.*)s/.*/hbase/`）
4. **`docker-compose.yaml`** — 加 service（hostname 必须无连字符）
5. **重建 KDC + 重启**：`docker compose down && rm -rf data/kerberos/* conf/kerberos/keytabs/*.keytab && docker compose up -d`

---

## 九、常见问题速查

| 问题 | 快速解决 |
|------|---------|
| Kerberos principal hostname 不匹配 | 检查 kdc-init.sh + krb5.conf + 各组件配置里 hostname 是否一致且无连字符 |
| Flink delegation token NPE | `security.delegation.tokens.enabled: false`（standalone 模式必须关） |
| Spark Kryo + yarn.archive → EOFException | 用默认 JavaSerializer 或单独分发 jar |
| DBeaver 连 HiveServer2 Kerberos 失败 | 必须用 `hiveserver.lakehouse.com`（不能 IP），principal 精确匹配 |
| Hive TEZ_HOME 冲突 | `TEZ_HOME=/dev/null` 让 glob 不匹配 |
| Flink SQL Client 无 Kerberos | 用 SQL Gateway 模式（embedded JVM Subject 无 TGT） |
| KDC 重建后 keytab 失效 | 必须同时删 `data/kerberos/*` 和 `conf/kerberos/keytabs/*.keytab` 再重建 |
| Trino 不识别 PaimonSerDe | Trino 482 Paimon connector 不完整，用 Flink/Spark 查 Paimon |
| HBase Thrift 连接 Kerberos | 客户端需要先 `kinit` 再连 `hbasemaster.lakehouse.com:9090` |

---

*最后更新：2026-09-27 — FQDN 去连字符 + 新增 HBase + Hive ORC 优化*
