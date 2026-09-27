# 🧠 Lakehouse 湖仓一体学习路径（初→中→高级实战）

> 作者：你自己（跟着做就是你写的）
> 环境：当前这套 Docker Compose 22 容器的 Lakehouse（Kerberos 默认）
> 已有 docs 覆盖了架构、Kerberos、CDC 管道、Spark on YARN 等专题。本笔记聚焦**从入门到资深的实战练习**。
> 每个案例都能在这套环境里**端到端跑通**，包含：目标 → 步骤 → 验证 → 坑点 → 扩展思考题。

---

## 📍 当前环境速查

```
22 个容器全活（Kerberos 默认）
├── KDC / Zookeeper / MySQL / Postgres / Kafka / Mongo
├── HDFS (NN:9870, DN) / YARN (RM:8088, NM)
├── HBase (Master:16010, RS:16020)
├── Hive (HS2:21066, Metastore:9083)
├── Spark (3.5.6, local + on YARN)
├── Flink (1.19.1, JM:8081)
├── Trino (482, 8085) + Iceberg Rest + Doris (FE:8030, BE)
└── switch-to-simple/kerberos.sh + check-auth-status.sh（auth 切换 + 状态检查）
```

Spark SQL 已有库：`default`（products 表）、`cdc_demo`、`trino_test`。
Trino catalogs：`hive`、`hudi`、`iceberg`、`system`。
HBase：当前无表（从零练建表）。

---

# 第一阶段：初级 —— 环境理解 + SQL 查询 + 数据导入

**目标**：跑通所有组件，理解湖仓一体"存算分离、多引擎共享同一数据底座"的核心设计。
**不要急着写代码**——先搞清楚每个组件**是什么、管什么、和谁打交道**。

---

## 练习 1.1 —— 全栈 auth 状态检查（你今天刚踩的坑）

**学习目标**：理解 Kerberos/SIMPLE 认证对全栈的影响——**一个 auth 开关 = 5 个 conf 文件 + docker-compose HBase 3 层**。

### 步骤

```bash
# 1. 静态检查（只读 conf，不碰容器）
./scripts/check-auth-status.sh

# 2. 加 --live 看容器健康
./scripts/check-auth-status.sh --live
```

### 验证

输出末尾出现 `✅ 全栈 auth 一致：KERBEROS` + `21 / 22 Up` = 通过。

### 坑点

| 现象 | 根因 | 修复 |
|------|------|------|
| Kerberos 态 HDFS CLI 连不上 `AccessControlException` | 宿主机没 kinit | `docker exec kerberos kadmin.local -q "ktadd -k /tmp/krb5.keytab admin/admin"` 然后 `kinit -kt /tmp/krb5.keytab admin/admin` |
| SIMPLE 态 HBase Master 报 `PermissionDenied user=root` | 容器 root 进程访问 `/hbase`（owner=hbase） | docker-compose HBase env 加 `HADOOP_USER_NAME: hbase` |
| `kadmind: Can not fetch master key` | `kdb5_util create -s -P password` 在某些 KDC 版本不生成 `.stash` | `create -s` 拆两步 + 显式 `stash -P password` |

### 思考题

> 为什么 switch-to-simple.sh 比 switch-to-kerberos.sh **更复杂**？
> 提示：Kerberos 态有 KDC + principal + keytab 约束，SIMPLE 态反而需要在各个组件里**补 USER_NAME / 删 kinit / 去掉 classpath mounts**。约束越多的模式 → 切换越简单。

---

## 练习 1.2 —— 用 3 种引擎查同一张表

**学习目标**：湖仓一体的核心——**同一份数据、多种计算引擎、同一个 Metastore**。

### 步骤

Spark SQL 写表 → Hive Metastore 自动可见 → Trino 查 → 不同引擎结果必须一致：

```bash
# 1. Spark SQL 写一张测试表（用 -e 非交互模式）
docker exec spark spark-sql -e "
CREATE TABLE IF NOT EXISTS default.t_lakehouse_practice (
    id INT,
    name STRING,
    price DECIMAL(10,2),
    stock INT,
    ts TIMESTAMP
)
STORED AS PARQUET;

INSERT INTO default.t_lakehouse_practice VALUES
(1, 'iPhone-16', 6999.00, 120, CURRENT_TIMESTAMP),
(2, 'MacBook-M4', 14999.00, 45, CURRENT_TIMESTAMP),
(3, 'AirPods-Pro3', 1899.00, 300, CURRENT_TIMESTAMP),
(4, 'iPad-Air', 4799.00, 80, CURRENT_TIMESTAMP),
(5, 'AppleWatch-S10', 3199.00, 150, CURRENT_TIMESTAMP);

SELECT COUNT(*) FROM default.t_lakehouse_practice;
"

# 2. Trino 查（同一个 Metastore + 同一份 HDFS 数据）
docker exec trino trino --execute "SELECT * FROM hive.default.t_lakehouse_practice ORDER BY id"

# 3. Hive Beeline 查（同一个 HS2）
docker exec hive-server beeline -u 'jdbc:hive2://localhost:21066/default' \
  -e "SELECT AVG(price), SUM(stock) FROM t_lakehouse_practice"
```

### 验证

三个引擎都返回 5 行 + 相同聚合值 = 通过。

### 思考题

> 数据在哪？Metastore 里存了什么？Spark / Trino / Hive 各自扮演什么角色？
> 画一张图：HDFS 存 Parquet 文件 → Hive Metastore 存 schema → Spark/Trino/Hive 分别算。

---

## 练习 1.3 —— 数据导入：CSV → HDFS → Spark SQL

**学习目标**：理解湖仓的"原始层"→ 清洗层 → 消费层；理解 HDFS 权限、Spark parquet 写入、Metastore 注册。

### 步骤

```bash
# 1. 造数据（宿主机）
mkdir -p data/practice
cat > data/practice/orders.csv <<'CSV'
order_id,user_id,product_id,amount,status,created_at
1001,U001,P01,299.00,paid,2025-09-01
1002,U002,P02,1599.00,paid,2025-09-02
1003,U001,P03,89.00,refunded,2025-09-03
1004,U003,P01,299.00,paid,2025-09-04
1005,U002,P04,4799.00,paid,2025-09-05
1006,U004,P05,1899.00,pending,2025-09-06
1007,U001,P02,1599.00,paid,2025-09-07
CSV

# 3. 拷到 HDFS（Spark 读 HDFS URI，不要读容器本地路径）
docker cp data/practice/orders.csv namenode:/tmp/orders.csv
docker exec namenode bash -c "
  hdfs dfs -put -f /tmp/orders.csv /tmp/orders.csv && \
  hdfs dfs -ls /tmp/orders.csv
"

# 4. Spark SQL 读 HDFS URI，清洗（去掉 refunded），写 Parquet 到 HDFS + 注册表
docker exec spark spark-sql -e "
CREATE TEMP VIEW orders_raw
USING CSV
OPTIONS (path 'hdfs://namenode:9000/tmp/orders.csv', header 'true', inferSchema 'true');

CREATE TABLE default.orders
PARTITIONED BY (status)
STORED AS PARQUET
AS SELECT * FROM orders_raw WHERE status != 'refunded';

SELECT status, COUNT(*), SUM(amount) FROM default.orders GROUP BY status;
"

# 5. Trino 也查同一张
docker exec trino trino --execute "SELECT * FROM hive.default.orders"
```

### 验证

Spark SQL 返回 `paid=4, pending=1` + `dfs -ls` 看到分区目录 `status=paid/` 和 `status=pending/` = 通过。

### 坑点

| 现象 | 根因 | 修复 |
|------|------|------|
| `Permission denied` 写 HDFS | Kerberos 态 HDFS 权限没配 | 容器里 Spark 有 delegation token，宿主机 HDFS CLI 没，用容器里跑 |
| CSV header 不识别 | Spark 默认 CSV 没开 header | `OPTIONS (..., header "true")` |
| Trino 查不到分区 | Trino 有分区缓存 | `CALL hive.system.refresh_partition('hive', 'default', 'orders')` |

### 思考题

> 为什么 Hive/Spark/Trino 都要走 **Hive Metastore**？能不能各用各的 schema？
> 提示："单写多读"、"联邦查询"、"Schema Evolution" 都依赖同一个 schema 中心。

---

## 练习 1.4 —— HBase 入门：建表、写、读、扫描

**学习目标**：HBase 是湖仓"热数据层"（近实时 KV / 宽表），和 HDFS 冷数据形成冷热分层。

### 步骤

```bash
# 1. 建表 + 列族
docker exec hbase-master hbase shell <<'HBASE'
create 't_user_profile', 'info', 'tags'
put 't_user_profile', 'U001', 'info:name', 'Alice', 'info:age', '28', 'info:city', 'Beijing'
put 't_user_profile', 'U001', 'tags:vip', 'true', 'tags:category', 'tech'
put 't_user_profile', 'U002', 'info:name', 'Bob', 'info:age', '32', 'info:city', 'Shanghai'
put 't_user_profile', 'U003', 'info:name', 'Carol', 'info:age', '25', 'info:city', 'Guangzhou'
list
count 't_user_profile'
scan 't_user_profile'
get 't_user_profile', 'U001'
HBASE

# 2. Spark 读 HBase（SHC connector）
docker exec spark spark-sql <<'EOF'
-- 如果有 SHC 就直接读，没有就建外表用 Hive StorageHandler 旁路
-- 先看 HBase REST API 有没有启
EOF

# 3. Spark 写 HBase（批量）
docker exec spark spark-shell <<'SPARK'
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
val conf = HBaseConfiguration.create()
val table = TableName.valueOf("t_user_profile")
val put = new Put("U004".getBytes())
put.addColumn("info".getBytes, "name".getBytes, "David".getBytes)
put.addColumn("info".getBytes, "age".getBytes, "29".getBytes)
val conn = ConnectionFactory.createConnection(conf)
conn.getTable(table).put(put)
conn.close()
println("写入 U004 成功")
SPARK
```

### 验证

HBase Shell `count` 返回 4 → Spark Shell 写入 U004 → 再 `scan` 看到 5 行 = 通过。

### 思考题

> HBase 用什么做 RowKey？如果是"订单号+商品号+时间戳"的联合 RowKey，怎么设计让**最近的订单**扫描最快？
> 提示：RowKey 前缀 + Region 切分 + 避免热点。

---

## ✅ 第一阶段通关标准

1. ✅ `check-auth-status.sh --live` 跑通，理解输出每一项含义
2. ✅ Spark SQL / Trino / Hive Beeline 查同一张表，结果一致
3. ✅ CSV→HDFS→Spark SQL Parquet 分区表，Trino 也能查
4. ✅ HBase Shell + Spark Shell 读写 4 行以上数据

---

# 第二阶段：中级 —— CDC 管道 + 流批一体 + Schema Evolution

**目标**：搭建一条真实 CDC 管道（MySQL → Flink → Kafka → Iceberg/Hudi/Paimon），理解流批架构、Schema 演进、UPSERT、Compaction。

---

## 练习 2.1 —— MySQL CDC 全链路（Flink → Kafka → Iceberg）

**学习目标**：CDC 是湖仓"活水"——OLTP 库变化实时同步到湖仓。这是真实生产里最常见的管道。

### 步骤

```bash
# 1. MySQL 建库建表 + 造数据
docker exec -i mysql mysql -uroot -proot <<'SQL'
CREATE DATABASE IF NOT EXISTS shop;
USE shop;
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO products (name, price, stock) VALUES
('iPhone-16', 6999.00, 120), ('MacBook-M4', 14999.00, 45),
('AirPods-Pro3', 1899.00, 300), ('iPad-Air', 4799.00, 80);
SELECT * FROM products;
SQL

# 2. Flink SQL Gateway 起 CDC（docker exec flink-sql-client）
#    MySQL CDC Source → Kafka Topic (UPSERT-kafka-connector) → Iceberg Sink
docker exec flink-sql-client <<'SQL'
-- MySQL CDC Source（Docker MySQL binlog）
CREATE TABLE mysql_products (
    id INT,
    name STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'database-name' = 'shop',
    'table-name' = 'products',
    'username' = 'root',
    'password' = 'root',
    'scan.startup.mode' = 'initial',
    'server-time-zone' = 'Asia/Shanghai'
);

-- Iceberg Sink（upsert on primary key）
CREATE TABLE iceberg_products (
    id INT,
    name STRING,
    price DECIMAL(10,2),
    stock INT,
    updated_at TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'iceberg',
    'catalog' = 'iceberg',
    'database' = 'shop',
    'table' = 'products',
    'write.upsert.enabled' = 'true'
);

-- 管道
INSERT INTO iceberg_products SELECT * FROM mysql_products;
SQL

# 3. MySQL 造变更
docker exec mysql mysql -uroot -proot -e "
  UPDATE shop.products SET stock=100 WHERE id=1;
  INSERT INTO shop.products (name, price, stock) VALUES ('HomePod', 2299.00, 200);
"

# 4. Trino / Spark 看 Iceberg 结果
docker exec trino trino --execute "SELECT * FROM iceberg.shop.products ORDER BY id"
```

### 验证

MySQL 改 stock → Iceberg 秒级同步 → Trino 查 stock 变成 100 + 新增 HomePod = 通过。

### 坑点

| 现象 | 根因 | 修复 |
|------|------|------|
| Flink CDC 报 `Can't read binlog` | MySQL 没开 binlog 或格式不对 | 确认 `server-id=1`、`log_bin`、`binlog_format=ROW` |
| Iceberg upsert 不生效 | 没声明 PRIMARY KEY 或 `write.upsert.enabled=false` | Flink DDL 里 `PRIMARY KEY (...) NOT ENFORCED` + Sink 加 upsert.enabled |
| Trino 查 Iceberg 报 `Table does not exist` | Iceberg catalog 没注册 | 看 docker-compose trino iceberg.properties catalog 配置 |

### 扩展

- [ ] 加一个 Hudi 和 Paimon Sink，对比三者的 upsert/merge 实现差异
- [ ] 在 Flink 里加窗口聚合：每 5 分钟统计 stock<50 的商品数
- [ ] 让 CDC 暂停 10 分钟 → MySQL 改 10 条 → 恢复 → 看是否幂等（exactly-once）

---

## 练习 2.2 —— Schema Evolution（Parquet / Iceberg 的隐藏杀手锏）

**学习目标**：湖仓表可以**不中断服务地加列、改列类型、重命名**。这是数仓/湖仓和传统数仓最根本的架构差异之一。

### 步骤

```bash
# 1. Spark SQL 建一张基础表
docker exec spark spark-sql <<'EOF'
CREATE TABLE default.t_evolve (id INT, name STRING) STORED AS PARQUET;
INSERT INTO default.t_evolve VALUES (1, 'a'), (2, 'b');
DESCRIBE default.t_evolve;
EOF

# 2. 加列（向后兼容！老数据自动补 NULL）
docker exec spark spark-sql <<'EOF'
ALTER TABLE default.t_evolve ADD COLUMNS (age INT);
INSERT INTO default.t_evolve VALUES (3, 'c', 30);
SELECT * FROM default.t_evolve;
-- 预期：行 1,2 的 age=NULL，行 3 的 age=30
EOF

# 3. Trino 查（实时看到新 schema，Metastore 自动同步）
docker exec trino trino --execute "SELECT * FROM hive.default.t_evolve"

# 4. Iceberg 的 Schema Evolution 更高级：
#    rename column / change type / drop column
#    Spark: ALTER TABLE iceberg_db.tbl RENAME COLUMN old TO new
#    但 Parquet 的底层数据文件不变，这就是"Metadata Operation"
```

### 验证

Spark `DESCRIBE` 看到 age 列 → 查询显示老数据 age=NULL → Trino 也能看到 = 通过。

### 思考题

> 加列的时候，底层 Parquet 文件**真的被重写了吗**？老的 2 个数据文件里根本没 age 字段，Spark/Trino 是怎么"补 NULL"的？
> 提示：Schema 是**分离存储**的——Metastore / Iceberg Manifest 存 schema，Parquet 文件只存它创建时的 schema。查询时引擎**合并**两边。

---

## 练习 2.3 —— HBase BulkLoad（百万级数据导入实战）

**学习目标**：HBase 导入大量数据时，`put` 逐条写 RegionServer 会压垮节点。BulkLoad 是**直接生成 HFile 放到 HDFS 对应 Region 目录**，零写放大。

### 步骤

```bash
# 1. 造 100 万行 CSV（宿主机）
python3 -c "
import csv, random
with open('/tmp/hbase_bulk.csv', 'w') as f:
    w = csv.writer(f)
    for i in range(1000000):
        w.writerow([f'U{i:07d}', f'user_{i}', random.randint(18,80), random.choice(['Beijing','Shanghai','Shenzhen'])])
"
ls -lh /tmp/hbase_bulk.csv  # 约 30MB

# 2. Spark Shell BulkLoad
docker cp /tmp/hbase_bulk.csv spark:/tmp/hbase_bulk.csv
docker exec spark spark-shell <<'SPARK'
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import org.apache.spark._

val hbaseConf = HBaseConfiguration.create()
val tableName = "t_user_bulk"
TableName.valueOf(tableName)

// Spark 读 CSV → RDD[(ImmutableBytesWritable, Put)]
val sc = spark.sparkContext
val raw = sc.textFile("hdfs:///tmp/hbase_bulk.csv")

val pairRdd = raw.map { line =>
  val parts = line.split(",")
  val rowKey = Bytes.toBytes(parts(0))
  val put = new Put(rowKey)
  put.addColumn("info".getBytes, "name".getBytes, parts(1).getBytes)
  put.addColumn("info".getBytes, "age".getBytes, parts(2).getBytes)
  put.addColumn("info".getBytes, "city".getBytes, parts(3).getBytes)
  (new ImmutableBytesWritable(rowKey), put)
}

// HBase Shell 先建表：预分区 + Region 对齐
// 这里假设表已存在，直接 BulkLoad
val stagingDir = "hdfs:///tmp/hbase_staging"
PairRDDFunctions.saveAsNewAPIHadoopFile(
  pairRdd, stagingDir,
  classOf[ImmutableBytesWritable], classOf[Put],
  classOf[HFileOutputFormat2], hbaseConf
)

// 触发 BulkLoad：HFile → HBase Region 目录
val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
val conn = ConnectionFactory.createConnection(hbaseConf)
val table = conn.getTable(TableName.valueOf(tableName))
bulkLoader.doBulkLoad(new org.apache.hadoop.fs.Path(stagingDir), conn.getAdmin, table,
  conn.getRegionLocator(TableName.valueOf(tableName)))
println(s"✅ BulkLoad 完成: $stagingDir → $tableName")
SPARK

# 3. 验证
docker exec hbase-master hbase shell <<'HBASE'
count 't_user_bulk', INTERVAL => 100000
HBASE
```

### 验证

`count` 返回 1,000,000 = 通过。对比逐条 put 写法的耗时（至少 10 倍慢）。

### 坑点

| 现象 | 根因 | 修复 |
|------|------|------|
| BulkLoad 报 `NoSuchRegion` | 数据 RowKey 不在任何 Region 范围内 | 建表时 `create 't', 'info', {SPLITS => ...}` 预分区，确保 RowKey 前缀覆盖 |
| Region 切分了但 HFile 没跟上 | BulkLoad 完成后 Region 变了 | 先等表稳定（无 split/compaction）再 BulkLoad |
| `Permission denied /tmp/hbase_staging` | Kerberos 态 HDFS staging 目录权限 | 容器里 Spark 有 delegation token，没问题；宿主机 CLI 要 kinit |

---

## 练习 2.4 —— Trino 联邦查询（异构源 JOIN）

**学习目标**：Trino 可以同时查 Hive Metastore（HDFS）、MySQL、MongoDB、HBase，跨源 JOIN 一行 SQL 搞定。

### 步骤

```sql
-- Trino 当前已配置的 catalogs: hive / iceberg / hudi / system
-- 假设已安装 mysql / mongodb connector

-- 1. 查 Trino catalogs
SHOW CATALOGS;

-- 2. 跨源 JOIN：Hive 订单表 + MySQL 用户表 + HBase 用户画像
SELECT
    o.order_id, o.user_id, o.amount, o.status,
    u.name AS user_name, u.level,
    hp.info_city, hp.info_age
FROM hive.default.orders o
JOIN mysql.shop.users u ON o.user_id = CAST(u.id AS VARCHAR)
LEFT JOIN hbase.user_profiles hp ON o.user_id = hp.row_key
WHERE o.status = 'paid' AND u.level >= 3
ORDER BY o.amount DESC;
```

### 思考题

> 跨源 JOIN 的性能瓶颈在哪？Trino 会把哪张表 **Broadcast** 到各个 Worker？什么时候会 **Shuffle**？
> 写一个 `EXPLAIN ANALYZE` 看 Trino 的执行计划，找到 `EXCHANGE` 和 `HASH JOIN` 的位置。

---

## ✅ 第二阶段通关标准

1. ✅ MySQL → Flink → Kafka → Iceberg CDC 管道跑通，更新秒级可见
2. ✅ Spark SQL ALTER TABLE ADD COLUMNS，Trino 实时看到新列
3. ✅ HBase BulkLoad 100 万行 < 30 秒
4. ✅ Trino 跨 2 种以上 catalog JOIN 查询

---

# 第三阶段：高级 —— 性能调优 + 数据质量 + 生产运维

**目标**：达到资深湖仓开发水平——能定位小文件、数据倾斜、OOM、GC 停顿；能写监控告警；能设计数仓分层。

---

## 练习 3.1 —— 小文件治理（Compaction / Optimize / RewriteDataFiles）

**学习目标**：湖仓最常见的生产问题——**每个 Insert/CDC 微批产生几百个小文件**，NameNode 内存爆炸 + 查询慢 10 倍。Iceberg/Hudi/Paimon 都有 compaction 但机制不同。

### 步骤

```bash
# 1. Spark SQL 批量 Insert 制造小文件（每个 Insert 可能 10 个文件）
for i in {1..20}; do
  docker exec spark spark-sql -e "
    INSERT INTO default.t_lakehouse_practice VALUES
    ($((RAND()*1000000)), 'prod_$i', ${RAND()*9999}, ${RAND()*5000}, CURRENT_TIMESTAMP);
  "
done

# 2. 看有多少小文件
docker exec spark spark-sql -e "
SELECT
  COUNT(*) AS total_files,
  ROUND(SUM(size)/1024/1024, 2) AS total_mb,
  ROUND(AVG(size)/1024, 2) AS avg_kb,
  MIN(size)/1024 AS min_kb,
  MAX(size)/1024/1024 AS max_mb
FROM (
  SELECT input_file_name() AS f,
         SUM(file_size) AS size
  FROM default.t_lakehouse_practice
  LATERAL VIEW explode(...)  -- Spark 3.5 有 DESCRIBE HISTORY / SHOW FILES
  GROUP BY f
)
"

# 3. Iceberg RewriteDataFiles（最彻底）
docker exec spark spark-sql -e "
CALL iceberg.system.rewrite_data_files(
  table => 'shop.products',
  options => map('target-file-size-bytes', '134217728', 'strategy', 'binpack')
);
"

# 4. Hudi compaction（表参数或调度）
#    hoodie.compact.schedule.enable=true / hoodie.compact.execution.enable=true

# 5. Paimon optimize
docker exec spark spark-sql -e "CALL sys.optimize('shop.products')"
```

### 验证

优化前：平均文件 < 1MB → 优化后：平均文件 > 100MB，总文件数减少 90% 以上。

### 思考题

> Iceberg 的 `rewrite_data_files`、Hudi 的 compaction、Paimon 的 optimize —— 三者本质上都在做"小文件合并"，但触发时机、IO 模型、对查询影响完全不同。对比一下：
> | | Iceberg | Hudi | Paimon |
> |---|---|---|---|
> | 触发 | 手动 CALL / 后台 Service | 定时 + 写入同步 | Optimize 服务 |
> | 读不受影响？ | ✅ 新 Manifest 原子替换 | ✅ Log + Base File 分离 | ✅ Snapshot 切换 |
> | Upsert 时也要合并？ | 是（Position Merge） | 是（MOR） | 是（Merge Tree） |

---

## 练习 3.2 —— 数据倾斜实战（Join + Aggregate）

**学习目标**：生产上 80% 的慢 SQL 根因是数据倾斜——某个 Key 占了 90% 的数据，导致单个 Task 跑 10 倍慢甚至 OOM。

### 步骤

```bash
# 1. 造倾斜数据（某个 user_id 有 100 万单，其他人 10 单）
docker exec spark spark-sql <<'EOF'
WITH skewed_users AS (
  SELECT explode(ARRAY(
    'U_MASTER', 'U001','U002','U003','U004','U005','U006','U007','U008','U009'
  )) AS user_id
),
master_user AS (SELECT REPEAT('U_MASTER', 1000000) AS user_id),  -- 伪：用循环
normal_users AS (
  SELECT CONCAT('U_', lpad(CAST(floor(rand()*10000) AS STRING), 4, '0')) AS user_id
  FROM (SELECT 1) LATERAL VIEW explode(split(space(100000), ' ')) t AS x
)
SELECT user_id FROM master_user UNION ALL SELECT user_id FROM normal_users;
-- 实际造数据：用 INSERT 多次写入同一个 user_id
EOF

# 2. 执行倾斜查询 + 看 Stage 耗时分布
docker exec spark spark-sql <<'EOF'
-- 这个 GROUP BY 会倾斜
SELECT user_id, COUNT(*), SUM(amount)
FROM default.orders
GROUP BY user_id
ORDER BY COUNT(*) DESC;

-- 看 Spark UI: http://localhost:4040/ → Stages → 找 "data skew" 标记的 Stage
-- Skew Join 在 Spark 3.x 会自动检测并广播小表 + 加盐分裂大表
EOF

# 3. 手动加盐分裂倾斜 Key（U_MASTER 拆成 U_MASTER_0..9）
#    先在大表里给倾斜 Key 加随机后缀（10 桶），小表 Expand 10 份
```

### 验证

优化前：`GROUP BY U_MASTER` 那个 Task 耗时 120s，其他 Task 5s → 总耗时 120s
优化后：分裂成 10 个 Task 各 15s + 广播 Join 小表 → 总耗时 ~20s

### 思考题

> Spark 3.x 的 **AQE（Adaptive Query Execution）** 会**自动**检测数据倾斜并 broadcast + 加盐。那为什么资深开发还要自己写加盐 SQL？什么时候 AQE 帮不上忙？
> 提示：AQE 只在 **Shuffle 后**检测，如果倾斜发生在写入前（INSERT OVERWRITE SELECT），AQE 管不到。

---

## 练习 3.3 —— 湖仓分层设计（ODS → DWD → DWS → ADS）

**学习目标**：数仓分层是"资深"的标志——能把混乱的原始表组织成可复用、可追溯、可治理的数仓。

### 步骤（在现有环境里用 Spark SQL 演示）

```
ODS 层（贴源层）：MySQL CDC 原始数据，加 etl_time / op_type（I/U/D）
  └── hive.default.ods_shop_products

DWD 层（明细层）：清洗、补维度、统一类型
  └── hive.default.dwd_order_detail  (订单明细 + 用户维度 + 商品维度 JOIN)

DWS 层（汇总层）：宽表、窗口聚合、周期指标
  └── hive.default.dws_user_daily_behavior  (按用户 + 日聚合)

ADS 层（应用层）：面向业务/报表，直接给 BI 用
  └── hive.default.ads_daily_top10_products
```

```sql
-- DWS: 按用户 + 日聚合行为
CREATE TABLE hive.default.dws_user_daily_behavior
PARTITIONED BY (dt STRING)
STORED AS PARQUET
AS SELECT
    user_id,
    COUNT(*) AS order_cnt,
    SUM(amount) AS order_amount,
    COUNT(DISTINCT product_id) AS category_cnt
FROM hive.default.dwd_order_detail
WHERE dt = '${yesterday}'
GROUP BY user_id, dt;

-- ADS: 面向报表的 Top10 商品
CREATE TABLE hive.default.ads_daily_top10_products STORED AS PARQUET AS
SELECT product_id, SUM(amount) AS sales
FROM hive.default.dwd_order_detail
WHERE dt = '${yesterday}'
GROUP BY product_id
ORDER BY sales DESC
LIMIT 10;
```

### 思考题

> 分层设计时最容易犯的 3 个反模式：
> 1. ADS 直接读 ODS（跳层）→ 改字段要改所有报表
> 2. 每层都重复 JOIN 3 张大表（过度规范化）→ 小团队的 DWS 宽表是答案
> 3. DWD 放业务逻辑（"如果 status=paid 就算有效"）→ 业务变了改 DWD 影响 DWS/ADS
> 你的设计怎么避开这三个坑？

---

## 练习 3.4 —— 生产运维（监控 + 告警 + 紧急回滚）

**学习目标**：资深开发 ≠ 会写代码 = 能在 **凌晨 3 点被电话叫醒**时 5 分钟内定位问题。

### 实践内容

| 项 | 命令/做法 | 什么情况看 |
|---|---|---|
| **全栈健康** | `./scripts/check-auth-status.sh --live` | auth 不一致 / 某个容器 Down |
| **HBase 卡 Regions** | `docker logs hbase-master \| grep RegionOpening` | 某个 Region 长时间 OPENING |
| **Flink BackPressure** | Flink UI (8081) → JobGraph → 红色节点 | CDC 管道有积压 |
| **YARN 资源耗尽** | `curl localhost:8088/ws/v1/cluster` | Spark on YARN 报 pending |
| **HDFS 数据倾斜** | `hdfs dfsadmin -report` | DN 使用率差 > 30% |
| **Iceberg Snapshot 膨胀** | Spark: `DESCRIBE HISTORY shop.products` | 上千个 Snapshot 导致查询变慢 |
| **紧急 auth 回滚** | `./scripts/switch-to-simple.sh && docker compose down && up -d` | Kerberos KDC 真挂了救不回来时 |

### 造一个故障演练

```bash
# 1. 故意搞垮 KDC
docker stop kerberos

# 2. 等 60 秒看哪些容器先挂（HBase → HDFS → Spark on YARN）
docker compose ps -a | head -10

# 3. 判断严重程度：HBase 起不来但 Trino 还能查 Iceberg？
#    → 说明 Iceberg REST 不依赖 Kerberos（因为它用 Hadoop delegation token）

# 4. 选择回 SIMPLE（KDC 救不回来）还是修 KDC？
#    → 修 KDC: docker start kerberos + docker exec kerberos kadmin.local ...
#    → 回 SIMPLE: ./scripts/switch-to-simple.sh && docker compose down && up -d
```

---

## ✅ 第三阶段通关 = 你就是资深湖仓开发了

| 技能 | 验证方式 |
|------|---------|
| 小文件治理 | 造 1000 个小文件 → Iceberg RewriteDataFiles → 验证文件数 < 100 |
| 数据倾斜 | 造倾斜数据 → GROUP BY 单 Key 拖慢 → 加盐分裂后 Stage 耗时均匀 |
| 数仓分层 | ODS→DWD→DWS→ADS 四层，每层 schema 清晰，有血缘 |
| 生产监控 | 凌晨 3 点 KDC 挂了 → 5 分钟内判断是修 KDC 还是切 SIMPLE |

---

## 📚 进阶方向（学无止境）

| 方向 | 推荐路径 | 对应练习 |
|------|---------|---------|
| **计算引擎内核** | Spark Catalyst / Flink Runtime / Trino 调度 | 3.2 数据倾斜 → 看 AQE 源码 |
| **存储格式** | Iceberg Manifest 结构 / Hudi MOR Merge Tree / Paimon Bucket | 3.1 小文件 → 对比三种优化 |
| **流式** | Flink 状态后端 / Checkpoint / Exactly-once | 2.1 CDC 管道 → 造故障恢复 |
| **数据湖生态** | Nessie / EMR Serverless / Databricks Photon | 当前环境是纯 OSS 版，下一步云原生 |
| **AI × 湖仓** | Iceberg + Spark + LLM（结构化数据喂模型）| 扩展：用 Spark MLlib 训练用户画像模型 |

---

## 📂 推荐阅读顺序（和现有 docs 配合）

```
练 1.1  →  docs/SWITCH_TO_SIMPLE.md + SWITCH_TO_KERBEROS.md + check-auth-status.sh 源码
练 1.2  →  docs/LAKEHOUSE_OVERVIEW.md（架构）
练 2.1  →  docs/CDC_PIPELINE.md
练 2.3  →  docs/PRODUCTION_DATA_OPS.md
练 3.1  →  Iceberg 官方 Compaction 文档
练 3.2  →  Spark 3.x AQE + Skew Join 源码
```

**记笔记的方法建议**：每做完一个练习，把 `docker logs` 里的报错 + 你找到的修复写到 `docs/TROUBLESHOOTING.md` 里——**这才是真正的成长**。

---

*最后更新：2026-09-27 — 对应 Lakehouse 全栈 Kerberos 态 + switch-to-simple/kerberos + check-auth-status.sh*
