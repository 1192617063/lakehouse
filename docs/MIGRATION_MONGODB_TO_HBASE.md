# MongoDB 存量数据迁移至 HBase — 实施指南

> **适用场景**：业务系统长期使用 MongoDB 存储大量结构化文档，需要将存量数据迁移到湖仓一体平台的 HBase 列族存储，实现**历史数据统一管理 + 实时同步**。
>
> **本文档聚焦 MongoDB → HBase 迁移的完整技术路径**，配合 [MIGRATION_AND_CLUSTER_PLANNING.md](MIGRATION_AND_CLUSTER_PLANNING.md) 可获得更大范围的迁移规划参考。

---

## 一、为什么迁移 MongoDB → HBase

### 1.1 适用场景

| 场景 | 推荐程度 | 理由 |
|------|---------|------|
| 用户画像 / 宽表（几百列） | ⭐⭐⭐⭐⭐ | HBase 列族模型天然适配宽表，列可动态扩展 |
| IoT 时序数据（设备 × 时间点） | ⭐⭐⭐⭐ | HBase 列族 + 时间戳版本管理，天然适合时序 |
| 推荐系统特征存储 | ⭐⭐⭐⭐⭐ | 特征按 user_id / item_id 组织，随机点查微秒级 |
| 风控 / 日志审计 | ⭐⭐⭐ | 支持点查，但**分析查询差**（不适合 JOIN/聚合） |
| 通用业务 CRUD（MySQL-like） | ⭐ | **不推荐**，HBase 不支持 SQL JOIN，应用层改造成本高 |

### 1.2 不适用场景

- **需要复杂 SQL JOIN / 聚合** → 应该用 Iceberg + Spark/Trino 或 Doris
- **多文档事务** → HBase 不支持跨行事务
- **全文搜索** → 保留 MongoDB 或迁移 ES

---

## 二、迁移方案选型

### 2.1 三种迁移路径对比

| 方案 | 实现 | 优点 | 缺点 | 适用规模 |
|------|------|------|------|---------|
| **A. Spark 批量 Export** | `mongo-spark-connector` → HBase | 简单、支持 Kerberos、一次全量 | 对 MongoDB 源集群有读压力 | ≤ 100GB |
| **B. Flink CDC + HBase Sink** | Flink MongoDB CDC Connector → HBase | **存量 + 增量一体化**、无停服 | 需要 MongoDB Change Streams（≥4.0 Replica Set） | **任意规模（推荐）** |
| **C. MongoDump → Import → Spark → HBase** | 离线导出 JSON → Spark 解析写 HBase | 脱离源集群、可离线跑 | 多一步人工操作、增量不自动 | 小数据量一次性迁移 |

### 2.2 本平台推荐：方案 B（Flink CDC 一体化）

理由：
1. 本平台已有 Flink CDC Pipeline 成熟（见 [CDC_PIPELINE.md](CDC_PIPELINE.md)）
2. 同时完成**历史存量导入 + 实时增量同步**，实现零停服迁移
3. MongoDB 7.0 + Change Streams 原生支持
4. HBase Sink 支持 Upsert（同 RowKey 自动覆盖旧版本）

---

## 三、实施步骤（方案 B：Flink CDC）

### 3.1 前置条件

| 项 | 要求 | 本平台现状 |
|----|------|-----------|
| MongoDB 版本 | ≥ 4.0 且为 Replica Set（Change Streams 需要） | MongoDB 7.0 ✅ |
| MongoDB Host | `mongodb.lakehouse.com:27017` | ✅ |
| HBase Kerberos | 已配置（见 HBASE_KERBEROS.md） | hbasemaster:16010 ✅ |
| HBase Thrift 端口 | 9090 可用 | ✅ |
| Flink HBase Connector jar | `flink-sql-connector-hbase-2.2-x.x.jar` | 需检查 |
| Flink MongoDB CDC jar | `flink-connector-mongodb-cdc-x.x.jar` | 需检查 |

### 3.2 第一步：准备 MongoDB 源数据（可选）

```bash
# 容器内造一些测试数据
docker exec mongodb mongosh --eval '
use products;
db.dropDatabase();
db.products.insertMany([
  { _id: "p1", name: "手机", price: 3999, category: "电子产品", stock: 100 },
  { _id: "p2", name: "笔记本", price: 6999, category: "电子产品", stock: 50 },
  { _id: "p3", name: "耳机", price: 299, category: "配件", stock: 500 },
]);
// 开启 Change Stream（MongoDB 7.0 默认开启，但需验证）
db.getSiblingDB("admin").runCommand({ setParameter: 1, changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 100 } } });
'

# 查看集合
docker exec mongodb mongosh --eval 'use products; db.products.find().toArray();'
```

### 3.3 第二步：HBase 创建目标表

```bash
docker exec hbase-master bash -c '
kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM 2>&1

# 创建 HBase 表：列族 info 存基础属性，ts 存版本时间戳
/opt/hbase/bin/hbase shell << EOF
create "lakehouse:products", { NAME => "info", VERSIONS => 5 }, { NAME => "meta", VERSIONS => 1 }
put    "lakehouse:products", "p1", "info:name",     "手机"
put    "lakehouse:products", "p1", "info:price",   "3999"
put    "lakehouse:products", "p1", "meta:category", "电子产品"
list "lakehouse:.*"
scan  "lakehouse:products"
EOF
'
```

### 3.4 第三步：Spark 批量导入（方案 A，适合≤100GB）

```bash
docker exec spark bash -c '
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
echo "lakehouse123" | kinit lakehouse@LAKEHOUSE.COM 2>&1

spark-sql --master yarn --deploy-mode client \
  --jars /opt/spark/jars/mongo-spark-connector_2.12-10.4.2.jar,/opt/spark/jars/hbase-spark-2.5.3.jar \
  -d spark.mongodb.input.uri=mongodb://mongodb:27017/products \
  -e "
    -- 读取 MongoDB products 集合
    CREATE TEMPORARY VIEW mongo_products
    USING com.mongodb.spark.sql
    OPTIONS (
      uri = \"mongodb://mongodb:27017\",
      database = \"products\",
      collection = \"products\"
    );

    -- 批量写入 HBase（需要 hbase-spark connector）
    -- 这里演示写入 Iceberg 作为中间态（更简单）
    CREATE TABLE iceberg.cdc_demo.products_from_mongo (
      id STRING, name STRING, price DOUBLE, category STRING, stock INT
    ) USING iceberg;

    INSERT INTO iceberg.cdc_demo.products_from_mongo
    SELECT _id AS id, name, price, category, stock FROM mongo_products;

    SELECT COUNT(*) FROM iceberg.cdc_demo.products_from_mongo;
  "
'
```

### 3.5 第四步：Flink CDC 一体化（方案 B，推荐）

```sql
-- Flink SQL Gateway 或 Flink SQL Client
-- 步骤 1：MongoDB CDC 源表
CREATE TABLE mongo_products (
  _id STRING,
  name STRING,
  price DOUBLE,
  category STRING,
  stock INT,
  ts TIMESTAMP(3) METADATA FROM `op_ts`,
  op STRING METADATA FROM `op`,
  PRIMARY KEY (_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'connection-string' = 'mongodb://mongodb:27017',
  'database' = 'products',
  'collection' = 'products',
  'scan.startup.mode' = 'initial',        -- 先全量快照，再追增量
  'scan.full-startup' = 'true'
);

-- 步骤 2：HBase Sink 表（Upsert 模式，同 RowKey 自动覆盖）
CREATE TABLE hbase_products (
  rowkey STRING,
  info ROW<name STRING, price DOUBLE, stock INT>,
  meta ROW<category STRING, op STRING, ts TIMESTAMP(3)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'lakehouse:products',
  'zookeeper.quorum' = 'hbasemaster:2181',  -- HBase master embedded ZK
  'sink.ignore-null-value' = 'false'
);

-- 步骤 3：ETL 同步
INSERT INTO hbase_products
SELECT
  _id AS rowkey,
  ROW(name, price, stock) AS info,
  ROW(category, op, ts)  AS meta
FROM mongo_products;
```

### 3.6 第五步：从 MongoDB 查数据验证

```bash
docker exec mongodb mongosh --eval '
use products;
db.products.insertMany([
  { _id: "p4", name: "键盘", price: 599, category: "配件", stock: 200 },
  { _id: "p5", name: "显示器", price: 1299, category: "电子产品", stock: 80 }
]);
'
# 等 Flink CDC 同步后，HBase 应该有新数据
docker exec hbase-master bash -c '
kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM 2>&1
/opt/hbase/bin/hbase shell -e "scan \"lakehouse:products\""
'
```

---

## 四、全量迁移 Checklist

| 阶段 | 事项 | 验收 |
|------|------|------|
| **评估** | 盘点 MongoDB 集合、估算数据量、确认 Change Stream 开启 | 数据量 ≤ HDFS 可用空间 70% |
| **准备** | 创建 HBase 表（RowKey 设计）、准备 Flink/Mongo jar | HBase table exists |
| **执行** | 跑 Flink CDC（full snapshot）→ 等 snapshot 完成 → 自动追增量 | Flink job RUNNING，HBase 有完整数据 |
| **切换** | 业务流量切换到 HBase（或双写验证） | 业务方确认数据一致 |
| **收尾** | 下线 MongoDB 源、归档、清理 | MongoDB 安全下线 |

### RowKey 设计原则

HBase RowKey 直接影响查询性能，核心原则：

1. **热点分散**：不要用单调递增的时间戳开头（会集中写一个 Region）
   - ❌ `20240101_user001`
   - ✅ `hash(user_id)_20240101` 或 `device_id`（自然分散）
2. **按查询模式排序**：如果经常按 `user_id` 查，RowKey 以 `user_id` 开头
3. **固定长度 + 前缀索引**：RowKey 不宜过长（HBase 全存内存），建议 16-64 字节
4. **避免 RowKey 为空**：HBase 不允许空 RowKey

### 数据量估算

| 源数据量 | 迁移耗时估算（Flink CDC） | HDFS 空间需求 | HBase Region 规划 |
|---------|------------------------|--------------|------------------|
| ≤ 100GB | ≤ 1 小时 | 源 × 1.5（HDFS 3 副本 + WAL） | 每个 Region 10-20GB |
| 100GB - 1TB | 1-6 小时 | 源 × 2.0 | 预分裂 Region |
| 1TB - 5TB | 6-24 小时 | 源 × 2.5 | 独立 HBase 集群 |
| > 5TB | 24+ 小时 | 源 × 3.0 | 专业方案咨询 |

---

## 五、风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| MongoDB Read Pressure | 业务慢 | 在业务低峰跑 full snapshot；或用 secondary 节点 |
| HBase Region Hotspot | 写入延迟 | RowKey 预分裂 + 设计热点分散 RowKey |
| Kerberos 中断 | CDC 任务挂掉 | Flink checkpoint + savepoint 自动恢复 |
| HDFS 空间不足 | 任务失败 | 迁移前 HDFS dfsadmin -report 检查 |
| 增量数据积压 | 数据延迟 | Flink 并行度 = Kafka partition 数；监控 lag |
| Schema 不兼容 | 数据丢失 | 严格 schema 校验（Flink CDC 有 schema change notification） |

---

## 六、与其他迁移方式的关系

| 迁移目标 | 技术 | 参考文档 |
|---------|------|---------|
| MongoDB → Iceberg | Flink CDC（同本方案，Sink 改 Iceberg） | [CDC_PIPELINE.md](CDC_PIPELINE.md) |
| MySQL → HBase | Flink CDC + HBase Sink | 本方案改 source |
| Postgres → HBase | Flink CDC + HBase Sink | 本方案改 source |
| MongoDB → Doris | Spark SQL + Doris Stream Load | [PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md) |

---

*最后更新：2026-09-27 — 新增 HBase 组件后首版*
