# 存量数据迁移与实时集群规划方案

> 本文档是**跨源存量数据迁移 + 实时数仓集群规划**的全局方案。
> 包含迁移方法论、组件选型决策、实时/离线数仓架构设计、以及各类数据源迁移的详细实施指引。
>
> **适用读者**：架构师、数据工程师、平台运维。

---

## 一、方法论：从源到湖的五步迁移法

```
┌─────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌─────────────┐
│   1. 盘点     │──▶│   2. 选型     │──▶│   3. 存量     │──▶│   4. 增量     │──▶│ 5. 切换     │
│ 源数据画像    │   │ 湖+引擎匹配   │   │ 全量导入      │   │ CDC 实时同步  │   │ 业务迁移     │
└─────────────┘   └──────────────┘   └──────────────┘   └──────────────┘   └─────────────┘
     │                    │                  │                 │                  │
 数据量、Schema     Iceberg/Hudi/Paimon    Spark/Flink      Flink CDC         双写→切读
 增长速率、QPS      Spark/Flink/Trino      BulkLoad         Kafka             灰度验证
```

### 1.1 第一步：源数据盘点

| 维度 | 要回答的问题 | 工具 |
|------|------------|------|
| 数据量 | 总大小？每日增量？表数量？ | `du -sh` / `SELECT COUNT(*)` / `SHOW TABLE STATUS` |
| Schema | 字段类型？主键？是否有 JSON/嵌套？ | `DESCRIBE` / `mongosh coll.stats()` |
| 访问模式 | 读多写多？随机点查？批量扫描？ | 应用日志 + Profiler |
| 一致性要求 | 强一致 / 最终一致 / 可容忍延迟？ | 业务需求文档 |
| 现有技术栈 | MySQL / PG / MongoDB / ES / 其他？ | 技术负责人访谈 |

### 1.2 第二步：组件选型决策树

```
                              ┌─────────────────────┐
                              │    数据源盘点完成     │
                              └─────────┬───────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
               需要随机点查          需要复杂 SQL           需要纯列式存储
               (KV/宽表)           (JOIN/聚合)            (OLAP 高并发)
                    │                   │                   │
                    ▼                   ▼                   ▼
               ┌────────┐         ┌────────────┐      ┌──────────┐
               │ HBase  │         │ 选湖格式：  │      │   Doris  │
               │ 列族KV  │         │ ◆ Iceberg │      │ Stream   │
               │        │         │ ◆ Hudi     │      │ Load     │
               │        │         │ ◆ Paimon   │      │          │
               └───┬────┘         └────┬───────┘      └────┬─────┘
                   │                   │                    │
                   ▼                   ▼                    ▼
            ┌─────────────┐     ┌─────────────┐       ┌─────────────┐
            │ 写：Flink/   │     │ 写：CDC/     │       │ 写：Stream   │
            │  Spark      │     │  Spark/Flink │       │  Load/Flink  │
            │ 读：API/Spark│     │ 读：Trino/   │       │ 读：Doris    │
            └─────────────┘     │  Spark/Flink │       │  FE 查询      │
                                 └─────────────┘       └─────────────┘
```

### 1.3 第三步：存量数据导入方案

| 数据源 | 推荐导入方式 | 注意事项 |
|--------|------------|---------|
| **MySQL** | Spark JDBC 并行读取 → Iceberg | 按主键分片并行，避免锁表；大表分批 |
| **PostgreSQL** | Spark JDBC 或 COPY → S3 → Spark → Iceberg | PG 有分区表注意递归扫子分区 |
| **MongoDB** | mongo-spark-connector → HBase/Iceberg | 默认 ObjectID 处理；大集合分批 |
| **ES / OpenSearch** | Spark ES Connector → Iceberg | scroll API 批量拉取 |
| **Kafka Topic（历史）** | Spark Structured Streaming read → Iceberg | 从 earliest offset 消费 |
| **HBase 现有集群** | ExportSnapshot → Import 或 Spark bulkload | 保持 RowKey 兼容 |

### 1.4 第四步：实时增量（CDC）

本平台 Flink CDC 支持的源：

| 源 | CDC 方式 | 本平台连接器 | 参考文档 |
|----|---------|-------------|---------|
| MySQL | Flink CDC (Debezium) | ✅ | [CDC_PIPELINE.md](CDC_PIPELINE.md) |
| PostgreSQL | Flink CDC (Debezium) | ✅ | [CDC_PIPELINE.md](CDC_PIPELINE.md) |
| MongoDB | Flink CDC 官方 | ✅ | [MIGRATION_MONGODB_TO_HBASE.md](MIGRATION_MONGODB_TO_HBASE.md) |
| Oracle | Flink CDC (Debezium) | 可选 | 需额外安装 |
| SQL Server | Flink CDC (Debezium) | 可选 | 需额外安装 |
| Kafka（再次分发） | Flink Kafka Source | ✅ | [CDC_PIPELINE.md](CDC_PIPELINE.md) |

### 1.5 第五步：业务切换策略

```
阶段 0 (准备期):  业务 → 源库       [稳定运行]
阶段 1 (双写期):  业务 → 源库 + 湖   [CDC 同步中，数据双写]
阶段 2 (灰度读):  业务 → 湖 (10%)   [小流量读湖，验证正确性]
阶段 3 (全量读):  业务 → 湖 (100%)  [读全切湖，写仍双写]
阶段 4 (切写):   业务 → 湖 (全读写)  [源库只读/下线]
阶段 5 (收尾):   源库下线、归档      [完全切换]
```

---

## 二、实时 / 离线数仓架构设计

### 2.1 本平台支持的数仓模式

```
┌──────────────────────────────────────────────────────────────────┐
│                    数据接入层 (ODS)                                │
│  MySQL/PG/MongoDB ──Flink CDC──► Kafka ──Flink──► 湖存储 (实时)   │
│  MySQL/PG/MongoDB ──Spark JDBC──► 湖存储                        │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                    明细层 (DWD / ODS-S)                            │
│  Paimon (Changelog Table)  /  Iceberg (Append Only)               │
│  实时：Flink 每 1min Mini-Batch                                   │
│  离线：Spark 每日/每小时分区追加                                    │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                    汇总层 (DWS)                                    │
│  Hudi (MOR 表, CDC Upsert) / Iceberg (聚合表)                     │
│  实时：Flink 窗口聚合                                              │
│  离线：Spark GroupBy / Cube                                        │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                    服务层 (ADS / Application)                      │
│  Doris (Stream Load OLAP) / HBase (KV/宽表) / Iceberg (BI 查询)   │
│  写入：Doris Stream Load, HBase Flink Connector                   │
│  查询：Trino, Doris FE, HBase Shell, Hive Beeline                  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 各层湖格式推荐

| 数仓层 | 推荐格式 | 推荐引擎 | 理由 |
|--------|---------|---------|------|
| **ODS（贴源）** | Paimon Changelog + Iceberg Append | Flink（实时）/ Spark（离线） | Paimon 原生 Changelog 支持 CDC 原样 |
| **DWD（明细）** | Iceberg Append + Paimon | Trino / Flink | Trino 兼容最好；Paimon 流式 |
| **DWS（汇总）** | Hudi MOR + Iceberg | Spark / Flink | Hudi MOR 支持 CDC Upsert；Iceberg 支持 Time Travel |
| **ADS（应用）** | **Doris + HBase** | Doris FE / HBase API / Spark | Doris OLAP 高并发点查；HBase KV 微秒级 |

### 2.3 实时数仓 Flink Pipeline 参考架构

```
MySQL ──Debezium──► Kafka (product_event topic) ──Flink──┐
PG    ──Debezium──► Kafka (user_action topic)    ──Flink──┤──► Kafka Connect──► HBase
MongoDB──Change Stream──► Kafka (device_event)   ──Flink──┤──► Flink SQL──► Paimon
                                                         ├──► Flink SQL──► Iceberg
                                                         └──► Flink SQL──► Doris Stream Load

并行 Flink Job（每个 topic 独立 Job）:
  Job 1: MySQL → Kafka → Flink → Paimon lakehouse.product
  Job 2: PG    → Kafka → Flink → Iceberg lakehouse.action
  Job 3: Mongo → Kafka → Flink → HBase lakehouse.device
  Job 4: Kafka → Flink Window → Doris lakehouse.event_summary
```

### 2.4 离线数仓 Spark Pipeline 参考架构

```
每日凌晨 1AM Spark YARN Job:
  Step 1: Spark JDBC → MySQL → Iceberg lakehouse.ods_product_daily
  Step 2: Spark SQL JOIN → Iceberg lakehouse.dwd_user_product
  Step 3: Spark GROUP BY CUBE → Iceberg lakehouse.dws_user_product_agg
  Step 4: Spark → Doris Stream Load → lakehouse.ads_report
  Step 5: Spark SQL → HBase BulkLoad → lakehouse.feature_store_user

Flink（每日 2AM 增量同步）:
  CDC 追增量 → Paimon → 覆盖当日 DWD 分区
```

---

## 三、各组件深度适用场景表

### 3.1 存储组件

| 组件 | 最强场景 | 次强场景 | 避坑 |
|------|---------|---------|------|
| **Iceberg** | 多引擎共存湖仓、时间旅行、Schema 演进 | 离线 ETL | CDC Upsert 需额外 `MERGE INTO` |
| **Hudi** | CDC 增量消费、MOR 合并、增量查询 | 流式入湖 | Trino 支持有限 |
| **Paimon** | Flink 实时 CDC、Changelog、MOR | Flink 流式读写 | Spark/Trino 支持仍在改进 |
| **HBase** | KV 点查、宽表、时序、特征存储 | 海量结构化数据 | 不支持 SQL JOIN、不适合分析 |
| **Doris** | OLAP 高并发、报表、向量查询 | HTAP | 内存消耗大，数据模型 Doris 特有 |

### 3.2 计算组件

| 组件 | 最强场景 | 次强场景 | 避坑 |
|------|---------|---------|------|
| **Spark** | 大规模离线 ETL、ML、批量导出 | 交互式 SQL（livy） | Kerberos 必须 YARN，不能 standalone |
| **Flink** | 实时 CDC、窗口计算、低延迟 | 离线批处理（Flink Batch） | Standalone 必须关 delegation token |
| **Trino** | 联邦查询、交互式 BI、跨湖联合 | ETL 编排 | 写 Iceberg 需 Iceberg REST |
| **Hive** | SQL 兼容、传统 ETL | 低并发查询 | Tez 版本必须 0.9.1（0.10 不兼容 Hive 3.1.3） |

### 3.3 数据源组件

| 组件 | 最强场景 | 迁移目标 |
|------|---------|---------|
| **MySQL 8.0** | 业务 OLTP、事务 | → Iceberg/Paimon (CDC) 或 Doris |
| **PostgreSQL 16** | 地理信息、JSONB 业务 | → Iceberg/Paimon (CDC) |
| **MongoDB 7.0** | 文档存储、灵活 Schema | → **HBase** (KV) 或 Iceberg (CDC) |
| **Kafka 3.9** | 消息队列、事件驱动 | → Paimon/Iceberg/HBase (Flink) |

---

## 四、本平台各组件 Kerberos 认证矩阵

| 客户端 | 访问目标 | 认证方式 | 本平台支持 |
|--------|---------|---------|-----------|
| Spark SQL / Shell | HDFS, Hive Metastore, Iceberg REST, Kafka | YARN delegation token + keytab | ✅ |
| Flink SQL Gateway / Client | HDFS, HBase, Kafka, Paimon/Iceberg REST | keytab login（**关闭 delegation token**） | ✅ |
| Trino CLI | Hive Metastore, Iceberg REST, HDFS | keytab 自动获取 TGT | ✅ |
| Hive Beeline / DBeaver | HiveServer2 | SASL GSSAPI（principal 精确匹配 FQDN） | ✅ |
| HBase Shell / Java API | HBase Master, HDFS | keytab login | ✅ |
| Kafka Consumer/Producer | Kafka Broker | SASL_PLAINTEXT + Kerberos keytab | ✅ |
| Doris FE/BE | — | 独立认证（不通过 Kerberos） | ⭕ |

---

## 五、迁移实操 Checklist（通用）

| # | 阶段 | 事项 | 预计耗时 | 验收标准 |
|---|------|------|---------|---------|
| 1 | **评估** | 盘点源库表、数据量、访问模式 | 1-2 天 | 输出数据画像报告 |
| 2 | **选型** | 确定湖格式 + 引擎组合 | 0.5 天 | 架构图 + 组件选型决策 |
| 3 | **准备** | 创建目标湖表、HBase 表、Doris 表 | 0.5 天 | 元数据就绪 |
| 4 | **存量** | Spark 批量全量导入 | 1-6 小时 | HDFS 数据量 = 源 × 1.5 |
| 5 | **增量** | Flink CDC 任务上线（full snapshot + incremental） | 1 小时 | Flink job RUNNING，Checkpoint OK |
| 6 | **双写验证** | 业务源库 + 湖双写，数据一致性对账 | 3-7 天 | 对账 99.99% 一致 |
| 7 | **灰度** | 10% → 50% → 100% 读流量切湖 | 3-14 天 | 业务无感知 |
| 8 | **收尾** | 源库下线、归档、监控告警切换 | 0.5 天 | 完成迁移 |

---

## 六、参考专题文档索引

| 专题 | 文件 | 内容 |
|------|------|------|
| 平台总览 | [LAKEHOUSE_OVERVIEW.md](LAKEHOUSE_OVERVIEW.md) | 架构、组件表、快速上手 |
| Kerberos 管理 | [KERBEROS_PRINCIPALS.md](KERBEROS_PRINCIPALS.md) | 所有 principal + 管理命令 |
| MongoDB → HBase | [MIGRATION_MONGODB_TO_HBASE.md](MIGRATION_MONGODB_TO_HBASE.md) | MongoDB 存量迁移详细步骤 |
| CDC 数据管道 | [CDC_PIPELINE.md](CDC_PIPELINE.md) | MySQL/PG → Kafka → Iceberg/Hudi/Paimon |
| Spark 离线湖仓 | [SPARK_OFFLINE_LAKEHOUSE.md](SPARK_OFFLINE_LAKEHOUSE.md) | Spark on YARN Kerberos + 批量 |
| DBeaver 连接 | [DBEAVER_CONNECTION_GUIDE.md](DBEAVER_CONNECTION_GUIDE.md) | Windows DBeaver Kerberos |
| 生产操作 | [PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md) | 存量导入、积压处理 |
| 部署指南 | [DEPLOYMENT_NOTES.md](DEPLOYMENT_NOTES.md) | 从零部署 |
| 故障排查 | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | 所有 Kerberos 坑点 |

---

*最后更新：2026-09-27 — 新增 HBase + MongoDB→HBase 迁移方案 + 去连字符 FQDN*
