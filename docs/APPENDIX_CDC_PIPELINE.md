# CDC 数据流转完整笔记：MySQL/PostgreSQL → Kafka → 湖仓

> 本文档记录从自动数据生成开始，通过 Flink CDC 将 MySQL/PostgreSQL 变更数据捕获到 Kafka，再经 Flink 写入 Iceberg/Hudi/Paimon 三种湖仓格式，并通过各查询引擎验证的完整流程。

---

## 一、架构总览

```
┌─────────────┐     ┌──────────┐     ┌────────┐     ┌──────────┐
│   MySQL     │────▶│ Flink    │────▶│ Kafka  │────▶│  Flink   │
│ (orders,    │     │ CDC      │     │ Topic  │     │ Sink     │
│  products)  │     └──────────┘     └────────┘     └────┬─────┘
└─────────────┘                                         │
┌─────────────┐     ┌──────────┐     ┌────────┐          │
│ PostgreSQL  │────▶│ Flink    │────▶│ Kafka  │──────────┤
│ (users)     │     │ CDC      │     │ Topic  │          │
└─────────────┘     └──────────┘     └────────┘          │
                                                         ▼
                              ┌──────────────────────────────────┐
                              │         湖仓存储层 (HDFS)         │
                              │  ┌────────┐ ┌──────┐ ┌────────┐ │
                              │  │ Iceberg│ │ Hudi │ │ Paimon │ │
                              │  └────────┘ └──────┘ └────────┘ │
                              └──────────────┬───────────────────┘
                                             │
                              ┌──────────────▼───────────────────┐
                              │         查询验证层                 │
                              │  Flink SQL / Trino / Spark / Hive │
                              └──────────────────────────────────┘
```

### 数据流转路径

| 源库 | 源表 | Kafka Topic | 湖仓格式 | 目标表 |
|------|------|-------------|----------|--------|
| MySQL | orders | cdc_mysql_orders | Iceberg | iceberg_catalog.cdc_demo.orders |
| PostgreSQL | users | cdc_pg_users | Hudi (COW) | hudi_catalog.cdc_demo.users |
| MySQL | products | cdc_mysql_products | Paimon | paimon_catalog.cdc_demo.products |

---

## 二、环境信息

### 核心服务

| 组件 | 地址/端口 | 认证 |
|------|-----------|------|
| MySQL | mysql:3306 | root/root123 |
| PostgreSQL | postgres:5432 | postgres/postgres123 |
| Kafka | kafka.lakehouse.com:9092 | SASL_PLAINTEXT + GSSAPI (Kerberos) |
| Flink JobManager | 172.30.80.3:8081 | Kerberos principal: flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM |
| HDFS | hdfs://namenode:9000 | Kerberos |
| Hive Metastore | thrift://hive-metastore:9083 | Kerberos |
| Iceberg REST | http://iceberg-rest:8181 | - |
| Trino | localhost:8085 | - |

### 关键配置文件

- Flink 配置: `conf/flink/flink-conf.yaml`
- Flink SQL 初始化: `conf/flink/sql-client-init.sql`
- CDC SQL 管道: `sql/cdc_*.sql` (阶段1), `sql/kafka_to_*.sql` (阶段2)
- 数据生成脚本: `scripts/gen_mysql_data.sh`, `scripts/gen_postgres_data.sh`

> **部署说明**：
> - Flink 扩展 jar（CDC、Iceberg、Hudi、Paimon 等）在镜像构建时 `COPY` 到 `/opt/flink/lib/extra/`，docker-compose 不挂载该目录。修改 jar 后需 `docker compose build flink-jobmanager flink-taskmanager` 重建镜像。
> - `./sql` 目录挂载到容器 `/opt/flink/sql`，SQL 文件修改后直接生效。

---

## 三、数据生成

### 3.1 MySQL 数据生成

脚本：`scripts/gen_mysql_data.sh`

- 在容器内后台运行，每 2 秒对 `cdc_demo.orders` 表执行 INSERT/UPDATE/DELETE
- `orders` 表结构：order_id, customer_name, product_name, quantity, price, order_status, created_at, updated_at
- `products` 表有初始 3 条数据，不定期 UPDATE

启动方式：
```bash
docker exec -d mysql bash /scripts/gen_mysql_data.sh
```

### 3.2 PostgreSQL 数据生成

脚本：`scripts/gen_postgres_data.sh`

- 每 3 秒对 `cdc_demo.users` 表执行 INSERT/UPDATE
- `users` 表结构：user_id, username, email, age, city, status, created_at, updated_at
- **必须设置** `ALTER TABLE public.users REPLICA IDENTITY FULL;` 否则 UPDATE/DELETE 的 before 字段为 null

启动方式：
```bash
docker exec -d postgres bash /scripts/gen_postgres_data.sh
```

---

## 四、Flink CDC → Kafka（阶段 1）

### 4.1 MySQL CDC → Kafka

SQL 文件：`sql/cdc_mysql_orders_to_kafka.sql`, `sql/cdc_mysql_products_to_kafka.sql`

```sql
-- MySQL CDC Source
CREATE TABLE mysql_orders_source (
    order_id BIGINT,
    customer_name STRING,
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root123',
    'database-name' = 'cdc_demo',
    'table-name' = 'orders',
    'server-time-zone' = 'Asia/Shanghai'
);

-- Kafka Sink (upsert-kafka + json)
CREATE TABLE kafka_orders_sink (
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'cdc_mysql_orders',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO kafka_orders_sink SELECT * FROM mysql_orders_source;
```

### 4.2 PostgreSQL CDC → Kafka

SQL 文件：`sql/cdc_postgres_users_to_kafka.sql`

```sql
CREATE TABLE pg_users_source (
    user_id BIGINT,
    username STRING,
    ...
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres123',
    'database-name' = 'cdc_demo',
    'schema-name' = 'public',
    'table-name' = 'users',
    'slot.name' = 'flink_slot',
    'decoding.plugin.name' = 'pgoutput'
);
```

> **注意**：PostgreSQL CDC 不支持 `publication.name` 选项，只支持 `slot.name` 和 `decoding.plugin.name`。

### 4.3 提交 CDC 作业

```bash
# kinit 获取 Kerberos ticket
docker exec flink-jobmanager bash -c '
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM
'

# 提交每个 CDC 作业（必须 cat init + pipeline，因为 SQL Client 只执行第一个 -f 文件）
for sql in cdc_mysql_orders_to_kafka cdc_mysql_products_to_kafka cdc_postgres_users_to_kafka; do
    docker exec flink-jobmanager bash -c "
        cat /opt/flink/conf/sql-client-init.sql /opt/flink/sql/${sql}.sql > /tmp/run.sql
        /opt/flink/bin/sql-client.sh -f /tmp/run.sql
    "
done
```

---

## 五、Kafka → 湖仓（阶段 2）

### 5.1 Kafka → Iceberg

SQL 文件：`sql/kafka_to_iceberg.sql`

```sql
-- Kafka Source (upsert-kafka)
CREATE TABLE kafka_orders_source (
    order_id BIGINT,
    customer_name STRING,
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'cdc_mysql_orders',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'properties.group.id' = 'flink-iceberg-orders',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Iceberg Sink
CREATE TABLE iceberg_catalog.cdc_demo.orders (
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

INSERT INTO iceberg_catalog.cdc_demo.orders
SELECT * FROM default_catalog.cdc_pipeline.kafka_orders_source;
```

### 5.2 Kafka → Hudi

SQL 文件：`sql/kafka_to_hudi.sql`

```sql
-- 注意：使用 COPY_ON_WRITE (COW) 表，数据立即可查询
-- MOR 表需要 compaction 后才能通过 Trino 查询
CREATE TABLE hudi_catalog.cdc_demo.users (
    ...
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field' = 'user_id',
    'hoodie.datasource.write.precombine.field' = 'updated_at',
    'hoodie.datasource.write.operation' = 'upsert'
);
```

> **重要**：Hudi 作业需要设置 `SET 'parallelism.default' = '1'`，否则使用默认 8 并行度会导致 checkpoint 失败（资源不足）。

### 5.3 Kafka → Paimon

SQL 文件：`sql/kafka_to_paimon.sql`

```sql
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE kafka_products_source (
    product_id BIGINT,
    ...
) WITH (
    'connector' = 'kafka',
    'topic' = 'cdc_mysql_products',
    'properties.bootstrap.servers' = 'kafka.lakehouse.com:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.mechanism' = 'GSSAPI',
    'properties.group.id' = 'flink-paimon-products',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE paimon_catalog.cdc_demo.products (
    ...
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'bucket' = '1',
    'changelog-producer' = 'input',
    'write-buffer-size' = '1mb',
    'commit.force.delay' = '5s'
);
```

---

## 六、查询验证

### 6.1 Trino 查询 Iceberg

```bash
# 查看表
docker exec trino trino --execute "SHOW TABLES FROM iceberg.cdc_demo;"

# 行数
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.cdc_demo.orders;"

# 样例数据
docker exec trino trino --execute "
SELECT order_id, customer_name, order_status
FROM iceberg.cdc_demo.orders
ORDER BY order_id DESC LIMIT 5;
"
```

验证结果（示例）：
```
order_id | customer_name | order_status
---------+---------------+-------------
1464     | Helen         | CREATED
1463     | Diana         | CREATED
1462     | Eric          | CREATED
```

### 6.2 Trino 查询 Hudi

```bash
# 行数
docker exec trino trino --execute "SELECT COUNT(*) FROM hudi.cdc_demo.users;"

# 样例数据（Hudi 有 _hoodie_* 元数据列）
docker exec trino trino --execute "
SELECT user_id, username, city, status
FROM hudi.cdc_demo.users
ORDER BY user_id DESC LIMIT 5;
"
```

### 6.3 Flink SQL 查询 Paimon

```bash
docker exec flink-jobmanager bash -c '
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM
cat /opt/flink/conf/sql-client-init.sql > /tmp/q.sql
echo "SET sql-client.execution.result-mode=TABLEAU;" >> /tmp/q.sql
echo "USE CATALOG paimon_catalog;" >> /tmp/q.sql
echo "SELECT COUNT(*) FROM cdc_demo.products;" >> /tmp/q.sql
/opt/flink/bin/sql-client.sh -f /tmp/q.sql
'
```

> **注意**：Flink SQL Client 非交互模式查询需要设置 `result-mode=TABLEAU`。

### 6.4 验证数据一致性

```bash
# Kafka 消息数 vs 湖仓表行数
# 注意：CDC 包含 UPDATE/DELETE，湖仓表行数 ≤ Kafka 消息数

# Iceberg（支持 upsert，行数 = 去重后的记录数）
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.cdc_demo.orders;"

# Hudi（COW 表，支持 upsert）
docker exec trino trino --execute "SELECT COUNT(*) FROM hudi.cdc_demo.users;"
```

---

## 七、作业管理

### 7.1 查看作业状态

```bash
# 查看运行中的作业
docker exec flink-jobmanager /opt/flink/bin/flink list

# 查看所有作业（含已完成/失败）
docker exec flink-jobmanager /opt/flink/bin/flink list -a
```

### 7.2 取消作业

```bash
# 方式1：CLI（可能因 CancelOptions 错误失败）
docker exec flink-jobmanager /opt/flink/bin/flink cancel <job_id>

# 方式2：REST API（推荐）
curl -s -X PATCH "http://localhost:8081/jobs/<job_id>?mode=cancel"
```

### 7.3 重启所有作业

当 jobmanager 重启后所有作业丢失，需要重新提交：

> **注意**：`./sql` 目录已挂载到容器 `/opt/flink/sql`，SQL 文件修改后直接生效，无需 `docker cp`。

```bash
# 1. 确保 Kerberos ticket
docker exec flink-jobmanager bash -c '
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM
'

# 2. 提交所有 6 个作业
for sql in cdc_mysql_orders_to_kafka cdc_mysql_products_to_kafka cdc_postgres_users_to_kafka \
           kafka_to_iceberg kafka_to_hudi kafka_to_paimon; do
    docker exec flink-jobmanager bash -c "
        cat /opt/flink/conf/sql-client-init.sql /opt/flink/sql/${sql}.sql > /tmp/run.sql
        /opt/flink/bin/sql-client.sh -f /tmp/run.sql
    "
    sleep 3
done
```

---

## 八、Kafka 运维

### 8.1 查看 Topic 消息

```bash
docker exec kafka bash -c '
cat > /tmp/cp.properties << EOF
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=GSSAPI
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/etc/security/keytabs/kafka.service.keytab" principal="kafka/kafka.lakehouse.com@LAKEHOUSE.COM" storeKey=true;
EOF

# 消费消息（带 key）
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka.lakehouse.com:9092 \
    --consumer.config /tmp/cp.properties --topic cdc_mysql_orders \
    --from-beginning --max-messages 5 --property print.key=true
'
```

### 8.2 查看 Consumer Group 偏移

```bash
docker exec kafka bash -c '
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka.lakehouse.com:9092 \
    --command-config /tmp/cp.properties --describe --group flink-iceberg-orders
'
```

---

## 九、当前状态（截至最近一次验证）

| 管道 | 状态 | 数据量 | 验证方式 |
|------|------|--------|----------|
| MySQL orders → Iceberg | ✅ 正常 | 1300+ 行 | Trino 查询 |
| PostgreSQL users → Hudi | ✅ 正常 | 860+ 行 | Trino 查询 |
| MySQL products → Paimon | ⚠️ Source 正常，Sink 未提交 | 0 行 | 待修复 |

### Paimon 已知问题

- Paimon Kafka Source 能正常消费数据（通过 print sink 验证）
- Paimon Writer 接收到记录（read-records > 0）但不写入文件（write-records = 0）
- 已尝试配置：`changelog-producer=input`、`write-buffer-size=1mb`、`commit.force.delay=5s`、`execution.checkpointing.interval=10s`
- Checkpoint 正常完成，但 Global Committer 未提交数据文件到 HDFS
- 该问题需要进一步排查 Paimon Writer 的 commit 机制

---

## 十、相关文档

- 部署问题排查：`docs/APPENDIX_TROUBLESHOOTING.md`
- 部署说明：`docs/APPENDIX_DEPLOYMENT_NOTES.md`
