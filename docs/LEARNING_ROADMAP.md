# 🗺️ Lakehouse Learning Roadmap — 从入门到资深

> 目标：**通过当前这套 22 容器的 Docker Compose Lakehouse（Kerberos 默认），练出生产级湖仓开发能力**。
> 形式：阶段式 Roadmap —— 每阶段有明确的练习 → 验证标准 → 坑点 → 思考题。
> 深度参考：复杂专题（CDC 管道、Spark 离线作业、Kerberos 管理）保留独立附录，Roadmap 里用"📖 详见"引用。

---

## 📍 当前环境速查

```
22 个容器（Kerberos 默认）
├── KDC / Zookeeper / MySQL / Postgres / Kafka / MongoDB
├── HDFS (NN:9870) / YARN (RM:8088)
├── HBase (Master:16010, RS:16020)
├── Hive (HS2:21066, Metastore:9083)
├── Spark (3.5.6) + Flink (1.19.1, JM:8081)
├── Trino (482, 8085) + Iceberg Rest + Doris (FE:8030)
└── 脚本: switch-to-simple/kerberos.sh + check-auth-status.sh
```

---

# 第一阶段：初级 —— 看懂环境 + 动手跑通

**目标**：理解湖仓"存算分离、多引擎共享同一数据底座"的核心设计。别着急写复杂代码——先**看懂每个组件是什么、管什么、和谁打交道**。

---

## 练 1.1 —— Auth 状态检查（从这里开始）

**学习目标**：一个 auth 开关 = 5 个 conf 文件 + docker-compose HBase 3 层改动。

```bash
# 静态（只读 conf）
./scripts/check-auth-status.sh

# 动态（+ 容器数 + HTTP 端口 + HBase Shell + KDC principals）
./scripts/check-auth-status.sh --live
```

**验证**：输出末尾 `✅ 全栈 auth 一致：KERBEROS` + `21/22 Up` = 通过。

> 📖 完整 auth 切换操作 + 7 个坑点：[docs/AUTH_SWITCH.md](AUTH_SWITCH.md)

---

## 练 1.2 —— 三引擎查同一张表

**学习目标**：同一份数据、多种计算引擎、同一个 Metastore。

```bash
# 1. Spark SQL 写表
docker exec spark spark-sql -e "
CREATE TABLE IF NOT EXISTS default.t_lakehouse_practice (
    id INT, name STRING, price DECIMAL(10,2), stock INT, ts TIMESTAMP
) STORED AS PARQUET;
INSERT INTO default.t_lakehouse_practice VALUES
(1, 'iPhone-16', 6999.00, 120, CURRENT_TIMESTAMP),
(2, 'MacBook-M4', 14999.00, 45, CURRENT_TIMESTAMP),
(3, 'AirPods-Pro3', 1899.00, 300, CURRENT_TIMESTAMP);
SELECT COUNT(*) FROM default.t_lakehouse_practice;
"

# 2. Trino 查
docker exec trino trino --execute "SELECT * FROM hive.default.t_lakehouse_practice ORDER BY id"

# 3. Hive Beeline 查
docker exec hive-server beeline -u 'jdbc:hive2://localhost:21066/default' \
  -e "SELECT AVG(price), SUM(stock) FROM t_lakehouse_practice"
```

**验证**：三个引擎返回相同聚合值 = 通过。

**思考题**：画一张图描述 Spark SQL 写表后 Trino 是怎么"看到"这张表的。Metastore 存了什么？HDFS 存了什么？

---

## 练 1.3 —— CSV → HDFS → Spark SQL 分区表

```bash
# 1. 造数据（宿主机）
mkdir -p /tmp/practice && cat > /tmp/practice/orders.csv <<'CSV'
order_id,user_id,product_id,amount,status,created_at
1001,U001,P01,299.00,paid,2025-09-01
1002,U002,P02,1599.00,paid,2025-09-02
1003,U001,P03,89.00,refunded,2025-09-03
CSV

# 2. CSV 先上 HDFS（Kerberos 态 Spark 要读 HDFS URI）
docker cp /tmp/practice/orders.csv namenode:/tmp/orders.csv
docker exec namenode bash -c "kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM && hdfs dfs -put -f /tmp/orders.csv /tmp/orders.csv"

# 3. Spark SQL 读 + 清洗 + 分区写入
docker exec spark spark-sql -e "
CREATE TEMP VIEW orders_raw USING CSV OPTIONS (path 'hdfs://namenode:9000/tmp/orders.csv', header 'true', inferSchema 'true');
CREATE TABLE default.orders PARTITIONED BY (status) STORED AS PARQUET
AS SELECT * FROM orders_raw WHERE status != 'refunded';
SELECT status, COUNT(*), SUM(amount) FROM default.orders GROUP BY status;
"

# 4. Trino 查同一张
docker exec trino trino --execute "SELECT * FROM hive.default.orders"
```

**验证**：Trino 只看到 `paid` 行，`refunded` 被过滤 = 通过。

**坑点**：
| 现象 | 根因 |
|------|------|
| Spark 报 `Path does not exist` | 路径用了容器本地 `/tmp/orders.csv` 没上 HDFS |
| Trino 查不到分区 | Trino 分区缓存，跑 `CALL hive.system.refresh_partition('hive','default','orders')` |

---

## 练 1.4 —— HBase 入门：建表 + Spark Shell 批量写

```bash
# 1. HBase Shell 建表 + put
docker exec hbase-master hbase shell <<'HBASE'
create 't_user_profile', 'info', 'tags'
put 't_user_profile', 'U001', 'info:name', 'Alice', 'info:age', '28'
put 't_user_profile', 'U002', 'info:name', 'Bob', 'info:age', '32'
put 't_user_profile', 'U003', 'info:name', 'Carol', 'info:age', '25'
scan 't_user_profile'
count 't_user_profile'
HBASE

# 2. Spark Shell 写第 4 行
docker exec spark spark-shell <<'SPARK'
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
val conf = HBaseConfiguration.create()
val conn = ConnectionFactory.createConnection(conf)
val table = conn.getTable(TableName.valueOf("t_user_profile"))
val put = new Put("U004".getBytes); put.addColumn("info".getBytes,"name".getBytes,"David".getBytes)
table.put(put); conn.close(); println("✅ 写入 U004")
SPARK
```

**验证**：HBase Shell `count` 返回 4 → Spark 写后再 `scan` 看到 5 行 = 通过。

**思考题**：HBase RowKey 设计——"订单号+商品号+时间戳"联合 RowKey 怎么设计让**最近的订单**扫描最快？RowKey 前缀 + Region 切分 + 避免热点。

---

## ✅ 第一阶段通关

1. ✅ `check-auth-status.sh --live` 看懂输出
2. ✅ 三引擎查同一张表，结果一致
3. ✅ CSV→HDFS→Spark SQL→Trino 全链路
4. ✅ HBase Shell + Spark Shell 读写

---

# 第二阶段：中级 —— CDC 管道 + 流批一体 + Schema Evolution

---

## Module 2.1 —— CDC 管道（MySQL → Flink → Kafka → Iceberg/Hudi/Paimon）

**学习目标**：CDC 是湖仓"活水"——OLTP 库变化实时同步到湖仓。生产里最常见的管道。

### 📖 深度参考

完整代码 + MySQL binlog 配置 + Flink SQL DDL + Kafka upsert topic + Iceberg Sink 配置 + Exactly-once 故障恢复：

👉 **[docs/CDC_PIPELINE.md](CDC_PIPELINE.md)**

### 最小跑通清单（速查）

```bash
# 1. MySQL 造数据
docker exec mysql mysql -uroot -proot -e "
CREATE DATABASE IF NOT EXISTS shop; USE shop;
CREATE TABLE products (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), price DECIMAL(10,2), stock INT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);
INSERT INTO shop.products (name, price, stock) VALUES ('iPhone-16', 6999.00, 120), ('MacBook-M4', 14999.00, 45);"

# 2. Flink CDC DDL 起管道（完整 DDL 在 CDC_PIPELINE.md 里）
docker exec flink-sql-client   # 粘贴 DDL 代码

# 3. MySQL 造变更 → Trino 查 Iceberg
docker exec mysql mysql -uroot -proot -e "UPDATE shop.products SET stock=100 WHERE id=1"
docker exec trino trino --execute "SELECT * FROM iceberg.shop.products WHERE id=1"
# 预期：stock 立刻变成 100（秒级同步）
```

### 思考题

1. Flink CDC + Iceberg upsert 的 Exactly-once 是怎么实现的？Checkpoint + Two-Phase Commit？
2. Iceberg `write.upsert.enabled=true` 和 Flink Sink `PRIMARY KEY NOT ENFORCED` —— 两个地方都要声明主键？
3. 管道挂 10 分钟，MySQL 改 100 条，恢复后不会漏也不会重复——怎么保证？

---

## 练 2.2 —— Schema Evolution（湖仓的杀手锏）

湖仓表可以**不中断服务地加列、改列类型、重命名**——传统数仓做不到。

```bash
# 建表
docker exec spark spark-sql -e "
CREATE TABLE default.t_evolve (id INT, name STRING) STORED AS PARQUET;
INSERT INTO default.t_evolve VALUES (1, 'a'), (2, 'b');"

# 加列（向后兼容！老数据自动补 NULL）
docker exec spark spark-sql -e "
ALTER TABLE default.t_evolve ADD COLUMNS (age INT);
INSERT INTO default.t_evolve VALUES (3, 'c', 30);
SELECT * FROM default.t_evolve;"
# 预期：行 1,2 的 age=NULL，行 3 的 age=30

# Trino 查（Metastore 自动同步）
docker exec trino trino --execute "SELECT * FROM hive.default.t_evolve"
```

**思考题**：底层 Parquet 文件真的被重写了吗？老文件没 age 字段，Spark/Trino 怎么"补 NULL"？
> 提示：Schema 分离存储——Metastore/Iceberg Manifest 存 schema，Parquet 文件只存它创建时的 schema，查询时引擎合并两边。

---

## 练 2.3 —— HBase BulkLoad

**学习目标**：HBase 导入大量数据时逐条 put 会压垮节点。BulkLoad 是**直接生成 HFile 放到 HDFS Region 目录，零写放大**。

### 📖 深度参考

完整 Spark Shell 代码 + 预分区 SPLITS 策略 + RowKey 热点规避 + Region 对齐检查：

👉 **[docs/PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md)**

### 核心原理（一句话）

`saveAsNewAPIHadoopFile` → 生成 HFile → `LoadIncrementalHFiles.doBulkLoad` 原子 move 到 HBase Region 目录。RegionServer **完全不参与写入**。

### 最小验证

```bash
docker exec hbase-master hbase shell <<'HBASE'
count 't_user_bulk', INTERVAL => 100000
HBASE
# 预期：和源文件行数一致（对比逐条 put 至少慢 10 倍）
```

---

## 练 2.4 —— Trino 跨源 JOIN

```sql
-- 查 Trino catalogs
SHOW CATALOGS;   -- hive, iceberg, hudi, system

-- 跨源 JOIN：Hive 订单 + MySQL 用户 + HBase 用户画像
SELECT o.order_id, o.user_id, u.name, hp.info_city
FROM hive.default.orders o
JOIN mysql.shop.users u ON o.user_id = CAST(u.id AS VARCHAR)
LEFT JOIN hbase.user_profiles hp ON o.user_id = hp.row_key
WHERE o.status = 'paid';
```

**思考题**：写 `EXPLAIN ANALYZE` 看 Trino 执行计划——哪个表会被 Broadcast？什么时候会 Shuffle？

---

## ✅ 第二阶段通关

1. ✅ CDC 管道秒级同步 MySQL → Iceberg
2. ✅ Spark SQL ALTER TABLE ADD COLUMNS + Trino 实时可见
3. ✅ HBase BulkLoad 百万级数据（参考 PRODUCTION_DATA_OPS.md）
4. ✅ Trino 跨 ≥2 catalogs JOIN

---

# 第三阶段：高级 —— 资深开发能力

**目标**：能定位小文件、数据倾斜、OOM；能写监控告警；能设计数仓分层；能做存量迁移。

---

## 练 3.1 —— 小文件治理（Iceberg RewriteDataFiles）

湖仓最常见生产问题——微批 Insert 产生几百个小文件，NameNode 内存爆炸 + 查询慢 10 倍。

```bash
# 造小文件（多次 Insert）
for i in {1..20}; do
  docker exec spark spark-sql -e "
    INSERT INTO default.t_lakehouse_practice VALUES
    ($i, 'prod_$i', ${RAND()}, ${RAND()}, CURRENT_TIMESTAMP);"
done

# 查文件数
docker exec spark spark-sql -e "
SELECT COUNT(*) FROM (SELECT input_file_name() AS f FROM default.t_lakehouse_practice GROUP BY f);"

# Iceberg 合并
docker exec spark spark-sql -e "
CALL iceberg.system.rewrite_data_files(
  table => 'shop.products',
  options => map('target-file-size-bytes', '134217728', 'strategy', 'binpack'));"
```

### 三格式 Compaction 对比

| | Iceberg | Hudi | Paimon |
|---|---|---|---|
| 触发 | 手动 CALL / 后台 Service | 定时 + 写入同步 | Optimize 服务 |
| 读不受影响？ | ✅ Manifest 原子替换 | ✅ Log + Base 分离 | ✅ Snapshot 切换 |
| Upsert 时也要合并？ | 是（Position Merge） | 是（MOR） | 是（Merge Tree） |

---

## 练 3.2 —— 数据倾斜（GROUP BY 单 Key 拖慢）

80% 的慢 SQL 根因——某个 Key 占了 90% 的数据，单个 Task 跑 10 倍慢甚至 OOM。

```bash
# 造倾斜数据
docker exec spark spark-sql -e "
CREATE TABLE skew_users (user_id STRING, cnt INT) STORED AS PARQUET;
INSERT INTO skew_users SELECT 'U_MASTER', 1 FROM (SELECT 1) LATERAL VIEW explode(split(space(1000000),' ')) t AS x;
INSERT INTO skew_users SELECT CONCAT('U_', lpad(CAST(floor(rand()*1000) AS STRING),4,'0')), 1 FROM orders;"

# 倾斜查询
docker exec spark spark-sql -e "
SELECT user_id, SUM(cnt) FROM skew_users GROUP BY user_id ORDER BY SUM(cnt) DESC;"
# 看 Spark UI http://localhost:4040/ → Stages → Skew Join 标记

# Spark 3.x AQE 会自动检测并广播 + 加盐
# 手动加盐：U_MASTER 拆成 U_MASTER_0..9，小表 Expand 10 份
```

**思考题**：AQE 会自动检测，为什么资深开发还要写加盐 SQL？什么时候 AQE 帮不上忙？
> 提示：AQE 只在 **Shuffle 后**检测，如果倾斜发生在写入前（INSERT OVERWRITE SELECT），AQE 管不到。

---

## 练 3.3 —— Iceberg Time Travel + Snapshot 数据回滚

**学习目标**：湖仓最杀手的功能——**误操作后秒级回滚**。传统 DB 要 restore backup（小时级），湖仓只需 `rollback_to_snapshot` 原子切换 Manifest。

### 步骤

```bash
# 1. Spark 建表（用 Spark Iceberg 扩展，spark_catalog 默认带 Iceberg）
docker exec spark spark-sql -e "
CREATE TABLE IF NOT EXISTS default.tt_tbl (id INT, val STRING) USING iceberg;
INSERT INTO default.tt_tbl VALUES (1, 'good_a'), (2, 'good_b');
-- 记下 Snapshot ID
DESCRIBE HISTORY default.tt_tbl LIMIT 10;"

# 2. 造错误（误删/误更新）
docker exec spark spark-sql -e "
DELETE FROM default.tt_tbl WHERE id = 1;   -- 误删了一行！
UPDATE default.tt_tbl SET val = 'WRONG' WHERE id = 2;   -- 误改了！
SELECT * FROM default.tt_tbl;   -- 现在只剩 1 行 WRONG"

# 3. 看快照历史，找要回滚的 Snapshot ID
docker exec spark spark-sql -e "DESCRIBE HISTORY default.tt_tbl;"
# 预期：看到 snapshots，operation 列显示 append → overwrite(delete) → overwrite(update)

# 4. 回滚！
docker exec spark spark-sql -e "
CALL iceberg.system.rollback_to_snapshot('default.tt_tbl', <SNAPSHOT_ID_BEFORE_ERROR>);
SELECT * FROM default.tt_tbl;"
# 预期：恢复到 2 行，id=1 存在，val='good_b'

# 5. Trino 也能查 Time Travel
docker exec trino trino --execute "
SELECT * FROM iceberg.default.tt_tbl FOR VERSION AS OF <SNAPSHOT_ID>;"
```

**验证**：`rollback_to_snapshot` 后 Spark + Trino 都看到正确数据 = 通过。

### 三种 Time Travel 语法对比

| 引擎 | 语法 | 说明 |
|------|------|------|
| Spark SQL | `CALL iceberg.system.rollback_to_snapshot('db.tbl', snap_id)` | **原子切换 Manifest**（生产回滚首选） |
| Spark SQL | `SELECT * FROM tbl TIMESTAMP AS OF '2026-09-27 10:00:00'` | 只读查询某个时间点的状态 |
| Trino | `SELECT * FROM iceberg.db.tbl FOR VERSION AS OF snap_id` | 只读查询指定快照 |

**思考题**：`rollback_to_snapshot` 和 `DELETE FROM tbl WHERE snapshot_id > x` 的区别？Manifest 是怎么原子替换的？
> 提示：Iceberg 把"当前快照指针"存在单独的 manifest 元数据文件里，rollback 就是改这个指针。数据文件（Parquet）**一个都不动**，所以回滚是微秒级的。

---

## 练 3.4 —— HBase Region 手动运维（split + compaction）

**学习目标**：HBase 生产运维三件事：Region 手动 split、Major Compaction 合并 StoreFile、Region 迁移。当 Region 过大（>10GB）或不均匀时必须手动干预。

### 步骤

```bash
# 1. 预分区建表（3 个 Region：-∞~a / a~m / m~∞）
docker exec hbase-master bash -c "/opt/hbase/bin/hbase shell <<'HBASE'
create 't_region_ops', 'info', {SPLITS => ['a','m']}
put 't_region_ops', 'a_001', 'info:v', 'hello1'
put 't_region_ops', 'm_001', 'info:v', 'hello2'
put 't_region_ops', 'z_001', 'info:v', 'hello3'
echo '=== 初始 Region ==='
list_regions 't_region_ops'

# 2. 在 'a' 和 'm' 之间手动 split（4 个 Region）
split_region 't_region_ops', 'c'
echo '=== split 后 ==='
list_regions 't_region_ops'

# 3. Major Compaction 合并 StoreFile（生产手动触发）
major_compact 't_region_ops'
echo '=== major compact 完成 ==='

# 4. 生产清理
disable 't_region_ops'; drop 't_region_ops'
exit
HBASE"

# 5. Web UI 看 Region 列表
curl -s http://localhost:16010/table.jsp?name=t_region_ops | grep region | head -10
```

**验证**：初始 3 Region → split 后 4 Region（list_regions 输出增加一行）= 通过。

### Region 运维命令速查

| 命令 | 用途 | 触发时机 |
|------|------|---------|
| `split_region 't', 'split_key'` | 手动在 split_key 处切分 Region | Region 过大（>10GB）或热点集中 |
| `major_compact 't'` | 合并所有 StoreFile 到 1 个 | 生产手动触发 / 定期调度 |
| `compact 't', 'cf'` | 合并某个列族的 StoreFile | Minor compaction（默认自动） |
| `move_region 'region_enc', 'target_rs'` | 手动迁移 Region | RegionServer 负载不均 |

**思考题**：如果 Region 太多（比如 10000+）HBase Meta 会变成瓶颈——怎么在 Region 增长和 Meta 压力之间平衡？
> 提示：预分区数 × 增长速度 = 预估 Region 数 → 控制在表数据量 1TB~2TB 时 ≤ 100 Region。

---

## 练 3.5 —— Spark 大 Shuffle 调优（Broadcast 阈值 + Shuffle 分区）

**学习目标**：实测发现——**同样 10000×10000 Cross Join**，强制禁用 broadcast（`autoBroadcastJoinThreshold = -1`）比默认慢 3 倍。理解 Spark AQE / Broadcast Join / Shuffle Partitions 的边界。

### 步骤

```bash
# 造数据（已在前面的测试里造好）
docker exec spark spark-sql -e "
CREATE TABLE IF NOT EXISTS default.big_left  (id INT) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS default.big_right (id INT) STORED AS PARQUET;
INSERT INTO default.big_left  SELECT EXPLODE(SEQUENCE(1,10000));
INSERT INTO default.big_right SELECT EXPLODE(SEQUENCE(1,10000));
SELECT COUNT(*) FROM default.big_left, default.big_right;"

# 【对比 1】强制 SortMergeJoin（禁用 Broadcast）—— 慢
docker exec spark spark-sql -e "
SET spark.sql.autoBroadcastJoinThreshold = -1;
SELECT COUNT(*) FROM default.big_left l JOIN default.big_right r ON l.id = r.id;"
# 实测耗时：~3.7s

# 【对比 2】恢复默认 —— 快
docker exec spark spark-sql -e "
SET spark.sql.autoBroadcastJoinThreshold = 10485760;   # 默认 10MB
SELECT COUNT(*) FROM default.big_left l JOIN default.big_right r ON l.id = r.id;"
# 实测耗时：~0.8s（Spark 把小表 autoBroadcast）

# 【对比 3】调整 Shuffle Partitions —— 控制并发粒度
docker exec spark spark-sql -e "
SET spark.sql.shuffle.partitions = 20;   # 默认 200，小表可以减
-- 用 EXPLAIN ANALYZE 看 Stage 分布
EXPLAIN ANALYZE SELECT COUNT(*) FROM default.big_left l JOIN default.big_right r ON l.id = r.id;"
```

### 关键参数速查

| 参数 | 默认 | 生产调优 | 什么时候调 |
|------|------|---------|-----------|
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | 64~256MB | 小维表 Join 自动广播（但别太大，不然 Driver OOM） |
| `spark.sql.shuffle.partitions` | 200 | Executor 数 × 2~4 | 大数据集 Shuffle 并发；小数据集减到 20~50 |
| `spark.executor.memoryOverhead` | 10% | 1GB / Executor | Executor Container 被杀（YARN 报 "exceeding memory limits"） |
| `spark.driver.memoryOverhead` | 10% | 512MB+ | Driver OOM（大 Broadcast 表） |

**验证**：对比 3 种配置的 `EXPLAIN ANALYZE`，确认 Broadcast Join vs SortMerge Join 的区别 = 通过。

**思考题**：为什么 `autoBroadcastJoinThreshold = -1` 禁用后会慢 3 倍？SortMerge Join vs Broadcast Join 的核心差异是什么？
> 提示：Broadcast Join = 小表全量复制到每个 Executor，零 Shuffle；SortMerge Join = 两边都要 Shuffle（大表的瓶颈）。

---

## 练 3.6 —— Kafka 积压诊断与消费提速

**学习目标**：生产最常见的报警——Consumer lag 告警。积压超过 1000 条 = 紧急、>100000 = 严重。怎么造 lag、查 lag、消 lag？

### 步骤

```bash
# 1. 造 lag：Producer 快速造 10 万条，Consumer 故意慢
docker exec kafka bash -c "for i in \$(seq 1 100000); do echo \"payload_\$i\"; done | \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic lag_demo 2>/dev/null"
# producer 完成

# 2. 查 Consumer lag（先起一个 group）
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic lag_demo \
  --group lag_group_demo --timeout-ms 10000 --max-messages 5000 &
sleep 3
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group lag_group_demo 2>&1 | grep -E "lag|CURRENT"
# 预期：LAG>95000（Consumer 才消费了 5000 条）

# 3. 提速：增加 Consumer 并行度（多个 Consumer 实例 / 增加分区）
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --alter --topic lag_demo --partitions 4 2>/dev/null
# 分区数从 1→4，Consumer 组里起 4 个实例 = 消费速度 ×4

# 4. 监控 lag 实时变化
while true; do
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group lag_group_demo 2>&1 | awk '/lag/{sum+=$2} END{print "剩余 lag:", sum}'
  sleep 2
done
# 预期：lag 快速降到 0

# 清理
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic lag_demo 2>/dev/null
```

### 消 lag 三条铁律

| 方法 | 提速倍数 | 注意事项 |
|------|---------|---------|
| 增加 Consumer 实例（同一组） | ×N（≤ 分区数） | Consumer 数不能超过分区数，多余的 idle |
| 增加 Topic 分区数 | ×N | 分区数只能增不能减；增了要 Producer 也重平衡 |
| 重平衡 Consumer offset | 快速跳到最新 | **会丢消息**，仅当积压不重要时用 |

> 📖 **完整 Kafka 积压排查清单**（含 Flink Checkpoint lag、Spark Structured Streaming lag、常见 root cause）：
> **[docs/PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md)** —— 第 3 章

---

## 练 3.7 —— Flink CDC Exactly-once 故障恢复

**学习目标**：Flink CDC 管道挂了 10 分钟 → MySQL 改了 100 条 → 管道恢复后**既不漏也不重复**。Exactly-once 怎么做到的？Checkpoint + Barrier + Flink State Backend。

### 步骤

```bash
# 1. 确保 Flink CDC 管道在跑（参考 CDC_PIPELINE.md 起 MySQL CDC → Iceberg）
# 2. 起一个 SQL 客户端看 Flink 作业列表
docker exec flink-jobmanager bash -c "curl -s http://localhost:8081/v1/jobs | head -c 500"

# 3. 故意 kill Flink JobManager（制造故障）
docker stop flink-jobmanager flink-taskmanager
sleep 60

# 4. Flink 挂了的时候，MySQL 造变更
docker exec mysql mysql -uroot -proot -e "
CREATE DATABASE IF NOT EXISTS shop; USE shop;
CREATE TABLE IF NOT EXISTS products (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), price DECIMAL(10,2), stock INT);
INSERT INTO shop.products (name, price, stock) VALUES
('iPhone-XR', 3999.00, 500), ('iPad-Pro', 7999.00, 200), ('AirPods-3', 999.00, 1000);"

# 5. 重启 Flink
docker start flink-jobmanager flink-taskmanager
sleep 120

# 6. 验证 Exactly-once：Flink 从最近一次 Checkpoint 恢复
docker exec flink-jobmanager bash -c "
curl -s http://localhost:8081/v1/jobs/\$JOB_ID/checkpoints 2>/dev/null | python3 -m json.tool | head -30"

# 7. 查 Iceberg：MySQL 新造的 3 条应该全在，而且不重复
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.shop.products;"
# 预期：初始 4 + 新增 3 = 7 行（如果重复了说明 Exactly-once 没生效）
```

### Exactly-once 三要素

| 要素 | 作用 |
|------|------|
| **Checkpoint** | 周期性把 Operator State（包括 Kafka Consumer offset）快照持久化 |
| **Barrier** | Checkpoint 插入到 DataStream 中，所有并行子任务对齐后才拍快照 |
| **Two-Phase Commit（Sink）** | Sink 端先写临时文件 → Barrier 通过 → 原子 commit（Iceberg/Hudi/Paimon 都支持） |

**思考题**：Flink 挂了 10 分钟期间 Kafka 还在产消息，Checkpoint 里存的 Kafka offset 能精确回到故障前那一刻吗？还是有窗口？
> 提示：Checkpoint 间隔默认 1min，所以最坏情况会重复消费 1min 内的消息。Exactly-once 通过 Sink 的幂等写入 + 主键去重来保证不脏数据。

---

## 练 3.8 —— 跨源一致性对账（MySQL ↔ 湖仓）

**学习目标**：CDC 管道跑起来后，怎么验证数据真的对得上？"源库 1000 条，湖仓应该也是 1000 条，值也一样"——这个对账必须自动化。

### 步骤

```bash
# 1. MySQL 造测试数据 + 启动 CDC
docker exec mysql mysql -uroot -proot -e "
CREATE DATABASE IF NOT EXISTS audit; USE audit;
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (id INT PRIMARY KEY, user_id VARCHAR(10), amount DECIMAL(10,2), status VARCHAR(10));
INSERT INTO orders VALUES (1,'U001',299.00,'paid'),(2,'U002',1599.00,'paid'),(3,'U003',89.00,'refunded');
INSERT INTO orders VALUES (4,'U001',299.00,'paid'),(5,'U002',4799.00,'paid');"
# Flink CDC 同步到 Iceberg（DDL 在 CDC_PIPELINE.md）
sleep 60

# 2. Spark SQL 查两边
docker exec spark spark-sql -e "
-- Spark JDBC 查 MySQL（源库）
SELECT 'mysql_src' AS src, COUNT(*) AS cnt, SUM(amount) AS amt FROM (
  SELECT * FROM jdbc_read.`mysql`.audit.orders
);" 2>&1 | tail -5 || echo "JDBC 可能未配置，用 Trino mysql catalog 替代"

docker exec trino trino --execute "
SELECT 'mysql_src' AS src, COUNT(*) AS cnt, CAST(SUM(amount) AS VARCHAR) AS amt FROM mysql.audit.orders
UNION ALL
SELECT 'lake_iceberg' AS src, COUNT(*) AS cnt, CAST(SUM(amount) AS VARCHAR) AS amt FROM iceberg.audit.orders;" 2>&1

# 3. 逐行对账（LEFT JOIN + IS NULL = 找丢失行）
docker exec trino trino --execute "
SELECT m.id, m.amount AS mysql_amt, i.amount AS iceberg_amt
FROM mysql.audit.orders m
LEFT JOIN iceberg.audit.orders i ON m.id = i.id
WHERE i.id IS NULL OR m.amount <> i.amount;" 2>&1
# 预期：空结果 = 完全一致；有结果 = 有丢数或值不一致
```

### 对账 SQL 模板（直接用）

```sql
-- 行数对账
(SELECT 'src' AS tag, COUNT(*) FROM mysql.audit.orders)
UNION ALL
(SELECT 'lake' AS tag, COUNT(*) FROM iceberg.audit.orders);

-- 逐行对账
SELECT s.*, l.* FROM mysql.audit.orders s
LEFT JOIN iceberg.audit.orders l ON s.id = l.id
WHERE l.id IS NULL;

-- 聚合对账（防止数值不一致但行数一样）
SELECT SUM(s.amount), SUM(l.amount) FROM mysql.audit.orders s
JOIN iceberg.audit.orders l ON s.id = l.id;
```

> 📖 **完整数据质量校验框架**（Great Expectations 风格的断言、字段校验、空值/重复/类型检查）：
> **[docs/PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md)** —— 第 3 章（全量导入校验 5 条黄金规则）

---

## 🧱 Case Study A：存量迁移五步法 + 数仓分层设计

> 本笔记整合了之前三篇迁移专题文档的精华，作为 Roadmap 的高级实战。
> 完整深度参考：[docs/AUTH_SWITCH.md](AUTH_SWITCH.md)（Kerberos 认证矩阵）+ [docs/CDC_PIPELINE.md](CDC_PIPELINE.md)（CDC 实施）。

### 方法论：从源到湖五步迁移法

```
① 盘点（数据画像）  →  ② 选型（湖+引擎匹配）  →  ③ 存量（全量导入）  →  ④ 增量（CDC 实时同步）  →  ⑤ 切换（双写→切读）
│                         │                         │                       │                        │
│ 数据量 / Schema         │ Iceberg/Hudi/Paimon     │ Spark BulkLoad        │ Flink CDC              │ 灰度验证 / 双写
│ 增长速率 / QPS          │ Spark/Flink/Trino       │ JDBC 并行读取          │ Kafka / 实时管道        │ 源库下线
```

#### ① 源数据盘点（必须先做）

| 维度 | 工具 |
|------|------|
| 数据量 | `du -sh` / `SELECT COUNT(*)` |
| Schema | `DESCRIBE` / `mongosh coll.stats()` |
| 访问模式 | 应用日志 + Profiler |
| 一致性要求 | 业务需求文档 |

#### ② 选型决策树

```
                 数据盘点完成
                      │
    ┌─────────────────┼─────────────────┐
    │                 │                 │
需要随机点查       需要复杂 SQL      需要 OLAP 高并发
(KV/宽表)          (JOIN/聚合)      (报表)
    │                 │                 │
    ▼                 ▼                 ▼
  HBase             湖格式：           Doris
  列族KV           Iceberg/Hudi/     Stream Load
                   Paimon
```

#### ③ 存量导入方案

| 数据源 | 推荐方式 |
|--------|---------|
| MySQL | Spark JDBC 并行读取 → Iceberg（按主键分片避免锁表） |
| PostgreSQL | Spark JDBC 或 COPY → S3 → Iceberg（注意分区表递归） |
| MongoDB | mongo-spark-connector → HBase / Iceberg |
| Kafka（历史） | Spark Structured Streaming read → Iceberg（从 earliest offset） |
| 现有 HBase 集群 | ExportSnapshot → Import 或 Spark bulkload（保持 RowKey 兼容） |

#### ④ 增量同步（CDC）

| 源 | 本平台支持 | 参考 |
|----|----------|------|
| MySQL ✅ | Flink CDC (Debezium) | CDC_PIPELINE.md |
| PostgreSQL ✅ | Flink CDC (Debezium) | CDC_PIPELINE.md |
| MongoDB ✅ | Flink CDC 官方 | 见下方 Case Study B |

#### ⑤ 业务切换五阶段

```
阶段 0  业务 → 源库        [稳定运行]
阶段 1  源库 + 湖 双写      [CDC 同步中]
阶段 2  10% 流量灰度读湖    [验证正确性]
阶段 3  100% 读湖 + 双写   [全量读切湖]
阶段 4  全读写切湖          [源库只读]
阶段 5  源库下线、归档
```

### 数仓分层设计

```
┌────────────────────────────────────────────────────────────┐
│  ODS 贴源层    Paimon Changelog / Iceberg Append Only        │
│  Flink CDC → Kafka → Flink → 湖（实时）                     │
│  Spark JDBC 批量 → 湖（离线每日）                            │
└────────────────────────────┬───────────────────────────────┘
                             │
┌────────────────────────────▼───────────────────────────────┐
│  DWD 明细层    Iceberg Append + Paimon                      │
│  实时：Flink 每 1min Mini-Batch 清洗、统一类型、补维度       │
│  离线：Spark 每日分区追加                                    │
└────────────────────────────┬───────────────────────────────┘
                             │
┌────────────────────────────▼───────────────────────────────┐
│  DWS 汇总层    Hudi MOR + Iceberg                           │
│  实时：Flink 窗口聚合（每 5min）                             │
│  离线：Spark GROUP BY / CUBE                                 │
└────────────────────────────┬───────────────────────────────┘
                             │
┌────────────────────────────▼───────────────────────────────┐
│  ADS 服务层    Doris + HBase + Iceberg                       │
│  Doris FE OLAP 高并发点查 / HBase KV 微秒级 / Iceberg BI     │
└────────────────────────────────────────────────────────────┘
```

### 各层湖格式推荐

| 数仓层 | 推荐格式 | 理由 |
|--------|---------|------|
| ODS（贴源） | **Paimon Changelog** + Iceberg Append | Paimon 原生 Changelog 支持 CDC 原样 |
| DWD（明细） | **Iceberg** + Paimon | Trino 兼容最好；Paimon 流式 |
| DWS（汇总） | **Hudi MOR** + Iceberg | Hudi MOR 支持 CDC Upsert；Iceberg 支持 Time Travel |
| ADS（应用） | **Doris + HBase** | Doris OLAP 高并发；HBase KV 微秒级 |

### 一套表结构 + 一套逻辑（离线 + 实时）

> 深度参考：[docs/SPARK_OFFLINE_LAKEHOUSE.md](SPARK_OFFLINE_LAKEHOUSE.md)

| 策略 | 说明 |
|------|------|
| **同一张物理表** | Flink CDC 实时写的 Paimon 和 Spark 离线回填的 Paimon 是同一张表（Changelog Table 天然 Upsert） |
| **相同 Schema** | 离线和实时读的源表 Schema 保持一致（Spark SQL 用 AS SELECT 保持） |
| **Upsert by PK** | 离线作业用 MERGE INTO / INSERT OVERWRITE + 主键；实时 Flink CDC Upsert 到相同表 |
| **业务逻辑下沉 DWS** | ADS 层直接读 DWS 聚合结果，不做业务计算 |

### Kerberos 认证矩阵（各客户端访问各目标）

| 客户端 | 访问目标 | 认证方式 | 本平台支持 |
|--------|---------|---------|-----------|
| Spark SQL | HDFS + Metastore + Iceberg REST + Kafka | YARN delegation token + keytab | ✅ |
| Flink SQL | HDFS + HBase + Kafka + Paimon | keytab login（**关 delegation token**） | ✅ |
| Trino CLI | Metastore + Iceberg REST + HDFS | keytab 自动 TGT | ✅ |
| Hive Beeline / DBeaver | HiveServer2 | SASL GSSAPI | ✅ |
| HBase Shell / API | HBase Master + HDFS | keytab login | ✅ |

---

## 🔧 Case Study B：MongoDB → HBase 存量迁移

**适用场景**：文档存储需要 KV 点查、微秒级响应、特征存储。

**不适用**：需要 SQL JOIN、复杂聚合、海量列式分析（→ 用 Iceberg）。

### 三种路径对比

| 路径 | 工具 | 适用数据量 | 优缺点 |
|------|------|----------|--------|
| A. Spark JDBC 批量 | mongo-spark-connector | ≤100GB | 简单，但全量一次性，无实时 |
| B. Flink CDC 一体化 | Flink Mongo CDC + HBase Connector | 不限（推荐） | 实时增量 + 历史全量，Exactly-once |
| C. Mongo Export → HBase BulkLoad | mongodump + HBase BulkLoad | 大表快速全量 | 快但无增量，需手动 Region 对齐 |

### 实施步骤（方案 B：Flink CDC 一体化）

```bash
# 1. MongoDB 造测试数据（可选）
docker exec mongodb mongosh <<'MONGO'
use test;
db.products.insertMany([
  {_id: ObjectId(), name: 'iPhone-16', price: 6999, stock: 120, category: 'phone'},
  {_id: ObjectId(), name: 'MacBook-M4', price: 14999, stock: 45, category: 'laptop'},
  {_id: ObjectId(), name: 'AirPods-Pro3', price: 1899, stock: 300, category: 'audio'},
]);
MONGO

# 2. HBase 建表（列族 info 存基础属性，ts 存版本时间戳）
docker exec hbase-master hbase shell <<'HBASE'
create 'hbase_mongo_products', 'info', 'ts', {SPLITS => ['a','f','k','p','u','z']}
HBASE

# 3. Flink CDC（MySQL CDC + HBase Sink，完整 DDL 在 CDC_PIPELINE.md）
docker exec flink-sql-client   # 粘贴 Mongo CDC + HBase Sink DDL

# 4. 验证：MongoDB 改 → HBase 秒级同步
docker exec mongodb mongosh -e "db.products.updateOne({name:'iPhone-16'}, {\$set:{stock:88}})"
# 等 Flink CDC 同步后
docker exec hbase-master hbase shell <<'HBASE'
get 'hbase_mongo_products', '<rowkey>'
HBASE
```

### RowKey 设计原则

| 原则 | 示例 | 为什么 |
|------|------|--------|
| **均匀前缀** | `MD5(primary_key).substring(0,4) + primary_key` | 避免热点，Region 分布均匀 |
| **复合 RowKey** | `user_id#product_id#timestamp` | 支持多维度扫描 |
| **倒序时间** | `Long.MAX_VALUE - timestamp` | 最新数据排最前，扫描最快 |

---

## ✅ 第三阶段通关 = 你就是资深湖仓开发

| 技能 | 验证 |
|------|------|
| 小文件治理 | 造 1000 小文件 → RewriteDataFiles → 文件数 < 100 |
| 数据倾斜 | 造倾斜数据 → Spark UI Stage 耗时均匀 |
| 数仓分层 | ODS→DWD→DWS→ADS 四层，每层 schema 清晰有血缘 |
| 存量迁移 | MongoDB→HBase 全链路 + 切换五阶段规划 |
| Kerberos 认证矩阵 | 知道每个客户端怎么连、delegation token vs keytab 的边界 |

---

## 🎓 进阶方向

| 方向 | 推荐路径 |
|------|---------|
| 计算引擎内核 | Spark Catalyst / Flink Runtime / Trino 调度源码 |
| 存储格式 | Iceberg Manifest / Hudi MOR Merge Tree / Paimon Bucket |
| 流式高级 | Flink 状态后端 / Checkpoint / Exactly-once |
| 云原生湖仓 | Nessie / EMR Serverless / Databricks Photon |
| AI × 湖仓 | Iceberg + Spark MLlib 训练用户画像模型 |

---

## 📂 深度参考附录（Roadmap 引用的独立专题）

| 专题 | 什么时候读 | 文件 |
|------|----------|------|
| **CDC 管道完整代码** | 练 2.1 想粘代码跑 | [docs/CDC_PIPELINE.md](CDC_PIPELINE.md) |
| **Spark 离线作业完整 SQL** | 练 3.3 想粘 SQL / Spark Offline 方法论 | [docs/SPARK_OFFLINE_LAKEHOUSE.md](SPARK_OFFLINE_LAKEHOUSE.md) |
| **Auth 双向切换** | auth 出问题 / 想切换 | [docs/AUTH_SWITCH.md](AUTH_SWITCH.md) |
| **Kerberos 管理** | principal / keytab 管理 | [docs/KERBEROS_PRINCIPALS.md](KERBEROS_PRINCIPALS.md) |
| **HBase 生产操作** | BulkLoad / Region 管理 | [docs/PRODUCTION_DATA_OPS.md](PRODUCTION_DATA_OPS.md) |
| **故障排查** | 报 Kerberos 错 / classpath 错 / PermissionDenied | [docs/TROUBLESHOOTING.md](TROUBLESHOOTING.md) |
| **DBeaver 连接** | Windows DBeaver Kerberos | [docs/DBEAVER_CONNECTION_GUIDE.md](DBEAVER_CONNECTION_GUIDE.md) |
| **部署指南** | 全新环境从零部署 | [docs/DEPLOYMENT_NOTES.md](DEPLOYMENT_NOTES.md) |

---

*最后更新：2026-09-27 — Roadmap 重命名 + 物理合并两篇 MIGRATION 专题 + 保留 CDC/SPARK_OFFLINE 为深度附录*
