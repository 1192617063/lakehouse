# 附录 Kerberos Principal 清单 — 用途与管理命令

> 本平台所有 Kerberos principal 集中在 [kdc-init.sh](../build/kerberos/kdc-init.sh) 初始化。
> 新增组件、重建 KDC、排查 principal 问题都以此文档为准。

***

## 一、全部 Principal 一览

| **Principal**                                         | **所属组件**             | **用途**                                          | **Keytab 文件**            |
| ----------------------------------------------------- | -------------------- | ----------------------------------------------- | ------------------------ |
| `admin/admin@LAKEHOUSE.COM`                           | KDC 管理               | kadmin 管理命令（admin123）                           | KDC 内置                   |
| `nn/namenode.lakehouse.com@LAKEHOUSE.COM`             | HDFS NameNode        | NN 启动向 KDC 认证                                   | `nn.service.keytab`      |
| `dn/datanode.lakehouse.com@LAKEHOUSE.COM`             | HDFS DataNode        | DN 启动认证 + 数据块传递                                 | `dn.service.keytab`      |
| `hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM`      | Hive Metastore       | Metastore Server 认证                             | `hive.service.keytab`    |
| `hive/hiveserver.lakehouse.com@LAKEHOUSE.COM`         | HiveServer2          | HS2 SASL 认证                                     | `hive.service.keytab`    |
| `HTTP/hiveserver.lakehouse.com@LAKEHOUSE.COM`         | HiveServer2          | HS2 HTTP 端点 SPNEGO                              | `hive.service.keytab`    |
| `flink/flinkjobmanager.lakehouse.com@LAKEHOUSE.COM`   | Flink JobManager     | JM Kerberos 登录 + HDFS 操作                        | `flink.service.keytab`   |
| `spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM`       | Spark Master         | Spark on YARN Kerberos 登录 + delegation token 获取 | `spark.service.keytab`   |
| `iceberg/icebergrest.lakehouse.com@LAKEHOUSE.COM`     | Iceberg REST         | REST Server Kerberos 认证                         | `iceberg.service.keytab` |
| `trino/trino.lakehouse.com@LAKEHOUSE.COM`             | Trino                | Trino Coordinator HDFS Kerberos                 | `trino.service.keytab`   |
| `yarn/resourcemanager.lakehouse.com@LAKEHOUSE.COM`    | YARN ResourceManager | YARN RM 认证                                      | `rm.service.keytab`      |
| `yarn/nodemanager.lakehouse.com@LAKEHOUSE.COM`        | YARN NodeManager     | YARN NM 认证                                      | `nm.service.keytab`      |
| `kafka/kafka.lakehouse.com@LAKEHOUSE.COM`             | Kafka Broker         | Kerberos SASL 认证                                | `kafka.service.keytab`   |
| `hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM`       | HBase Master         | HBase Master Kerberos 认证                        | `hbase.service.keytab`   |
| `hbase/hbaseregionserver.lakehouse.com@LAKEHOUSE.COM` | HBase Regionserver   | HBase Regionserver 认证                           | `hbase.service.keytab`   |
| `lakehouse@LAKEHOUSE.COM`                             | **平台用户**             | 所有客户端应用登录用（密码 `lakehouse123`）                   | `lakehouse.keytab`       |

***

## 二、关键设计决策

### 2.1 hostname 无连字符

所有 service principal 的 hostname 部分**不使用连字符**（`-`）：

* ❌ 旧：`flink-jobmanager.lakehouse.com` → 对应 `flink/flink-jobmanager.lakehouse.com`
* ✅ 新：`flinkjobmanager.lakehouse.com` → 对应 `flink/flinkjobmanager.lakehouse.com`

**原因**：Kerberos GSSAPI 在 Java 里做 `ServiceName` 匹配时，会把容器 hostname（如 `flinkjobmanager.lakehouse.com`）和 KDC principal hostname（如 `flink-jobmanager.lakehouse.com`）做精确匹配。连字符不同会导致 GSSAPI 拒绝连接。

### 2.2 keytab 共享

| **共享 keytab**          | **包含 principal**                                   | **原因**                                 |
| ---------------------- | -------------------------------------------------- | -------------------------------------- |
| `hive.service.keytab`  | `hivemetastore` + `hiveserver` + `HTTP/hiveserver` | Metastore 和 HiveServer2 共用 Hive keytab |
| `hbase.service.keytab` | `hbasemaster` + `hbaseregionserver`                | Master 和 Regionserver 共用 HBase keytab  |

### 2.3 Hadoop auth\_to\_local

所有组件用户 ID（principal 第一段）必须在 [core-site.xml](../conf/hadoop/core-site.xml) 的 `hadoop.security.auth_to_local` RULE 里有映射。当前规则：

```
RULE:[1:$1@$0](lakehouse@.*)s/.*/lakehouse/
RULE:[2:$1@$0](nn@.*)s/.*/hdfs/
RULE:[2:$1@$0](dn@.*)s/.*/hdfs/
RULE:[2:$1@$0](hive@.*)s/.*/hive/
RULE:[2:$1@$0](flink@.*)s/.*/flink/
RULE:[2:$1@$0](spark@.*)s/.*/spark/
RULE:[2:$1@$0](trino@.*)s/.*/trino/
RULE:[2:$1@$0](iceberg@.*)s/.*/iceberg/
RULE:[2:$1@$0](yarn@.*)s/.*/yarn/
RULE:[2:$1@$0](kafka@.*)s/.*/kafka/
RULE:[2:$1@$0](hbase@.*)s/.*/hbase/
RULE:s/@.*//   (fallback：去掉 @LAKEHOUSE.COM)
```

### 2.4 domain\_realm

​[krb5.conf](../conf/kerberos/krb5.conf) 的 `[domain_realm]` 必须有每个 hostname 的短名映射到 realm，否则容器内 `kinit` 时 hostname 无法解析 realm：

```
hbasemaster         = LAKEHOUSE.COM
hbaseregionserver   = LAKEHOUSE.COM
flinkjobmanager     = LAKEHOUSE.COM
sparkmaster         = LAKEHOUSE.COM
...（所有 hostname 短名都要加）
```

***

## 三、常用管理命令

### 3.1 KDC 管理（容器内执行）

```bash
# 进入 KDC 容器
docker exec -it kerberos bash

# 列出所有 principal
kadmin.local -q 'listprincs'
# 或远程管理
kadmin -p admin/admin -w admin123 -q 'listprincs'

# 新增 principal（手工）
kadmin.local -q "addprinc -randkey service/hostname.lakehouse.com@LAKEHOUSE.COM"

# 导出 keytab
kadmin.local -q "ktadd -k /etc/security/keytabs/new.service.keytab service/hostname.lakehouse.com@LAKEHOUSE.COM"
chmod 644 /etc/security/keytabs/new.service.keytab

# 重置用户密码
kadmin.local -q 'change_password -pw newpassword lakehouse@LAKEHOUSE.COM'

# 强制 rotation（删除旧 principal 再重建）
kadmin.local -q "delprinc -force service/hostname.lakehouse.com@LAKEHOUSE.COM"
kadmin.local -q "addprinc -randkey service/hostname.lakehouse.com@LAKEHOUSE.COM"
kadmin.local -q "ktadd -k /etc/security/keytabs/service.service.keytab service/hostname.lakehouse.com@LAKEHOUSE.COM"
```

### 3.2 容器内 kinit 验证

```bash
# 进入任意需要 Kerberos 的容器
docker exec -it spark bash

# 用 keytab 服务 principal 登录
kinit -kt /etc/security/keytabs/spark.service.keytab spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM
klist   # 验证有 TGT

# 用密码平台用户登录
echo "lakehouse123" | kinit lakehouse@LAKEHOUSE.COM
klist

# 测试 HDFS Kerberos
hdfs dfs -ls /lakehouse
# 测试 Hive Metastore Kerberos
hive -e 'SHOW DATABASES;'
```

### 3.3 keytab 文件管理

```bash
# 查看 keytab 内容
klist -k /etc/security/keytabs/flink.service.keytab

# 验证 keytab 是否有效
klist -kte /etc/security/keytabs/flink.service.keytab
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flinkjobmanager.lakehouse.com@LAKEHOUSE.COM && klist

# 检查所有 keytabs 是否匹配 KDC 当前 principal
for k in conf/kerberos/keytabs/*.keytab; do
  echo "=== $k ==="; klist -k "$k"; echo "";
done
```

### 3.4 重建 KDC（重大操作）

> ⚠️ **KDC master key 变了，所有 keytabs 全部失效**。必须同步删 KDC 数据目录和 keytabs。

```bash
cd /home/admin/lakehouse

# 1. 停所有容器
docker compose down

# 2. 清旧数据（关键！）
rm -rf data/kerberos/*
rm -rf conf/kerberos/keytabs/*.keytab

# 3. 重建 KDC 镜像（如果 kdc-init.sh 改过）
docker compose build kerberos

# 4. 启 KDC
docker compose up -d kerberos
sleep 20

# 5. 验证新 principal
docker exec kerberos kadmin -p admin/admin -w admin123 -q 'listprincs'

# 6. 启所有服务
docker compose up -d
```

***

## 四、新增用户 Step-by-Step（已有一键脚本 `add-user.sh`）

### 方式 1：一键脚本（推荐）

```bash
# 普通用户 principal (user@REALM)
./scripts/add-user.sh alice Alice123

# 服务 principal (service/fqdn@REALM)
./scripts/add-user.sh --service flink-jobmanager --host flinkjobmanager.lakehouse.com

# 随机密码（自动生成）
./scripts/add-user.sh bob
# 输出: 📝 随机生成密码: Kx9mPq2vRt4w...（只显示这一次！）
```

脚本自动做的事：

1. ✅ KDC 容器存在性检查
2. ✅ 在 KDC 里 `addprinc`（用户给密码 / 服务用 `-randkey`）
3. ✅ 导出 keytab 到 `conf/kerberos/keytabs/<name>.keytab`
4. ✅ 在 KDC 容器内 kinit + klist 验证 keytab 可用
5. ✅ 幂等：principal 已存在则跳过 `addprinc`，只刷新 keytab

### 方式 2：手动（理解原理）

```bash
# ① 进 KDC 容器用 kadmin（跨网络 admin/admin 凭证）
docker exec kerberos kadmin -p admin/admin -w admin123

# ② 新增用户 principal（有密码）
kadmin: addprinc -pw MyP@ssw0rd alice@LAKEHOUSE.COM

# ③ 或服务 principal（无密码，-randkey 生成随机 key）
kadmin: addprinc -randkey demoapp/demoapp.lakehouse.com@LAKEHOUSE.COM

# ④ 导出 keytab
kadmin: ktadd -k /etc/security/keytabs/alice.keytab alice@LAKEHOUSE.COM

# ⑤ 拷到宿主机（如果 KDC 没 volume 挂载 keytabs 目录）
docker cp kerberos:/etc/security/keytabs/alice.keytab conf/kerberos/keytabs/

# ⑥ 验证
docker exec kerberos kinit -kt /etc/security/keytabs/alice.keytab alice@LAKEHOUSE.COM
docker exec kerberos klist
# 预期输出: Default principal: alice@LAKEHOUSE.COM
```

### ⑦ 重要：auth\_to\_local 映射（HBase / Hadoop 必需）

Hadoop / HBase 收到 Kerberos principal 时要映射到 Unix 用户名，否则会报 `PermissionDenied user=alice is not owner of inode=/hbase`。

修改 `conf/hadoop/core-site.xml`：

```xml
<property>
  <name>hadoop.security.auth_to_local</name>
  <value>
    {/* 在 RULE 里加一条，把 alice@REALM 映射成 alice */}
    RULE:[2:$1](alice)s/.*/alice/
    RULE:[2:$1](bob)s/.*/bob/
    DEFAULT
  </value>
</property>
```

改完重启 HBase Master：`docker compose restart hbase-master`

### ⑧ 进 kdc-init.sh 固化（让下次重建 KDC 也带这个用户）

在 `build/kerberos/kdc-init.sh` 的 "用户 principal" 段加：

```bash
kadmin.local -q "addprinc -randkey alice@${REALM}" 2>/dev/null || true
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/alice.keytab alice@${REALM}" 2>/dev/null || true
```

***

## 五、新增组件 Checklist

每新增一个需要 Kerberos 认证的组件，**必须**做以下修改：

| **步骤** | **文件**                       | **修改内容**                                                                                               |
| ------ | ---------------------------- | ------------------------------------------------------------------------------------------------------ |
| ①      | `build/kerberos/kdc-init.sh` | 加 principal 创建 + keytab 导出                                                                             |
| ②      | `conf/kerberos/krb5.conf`    | `[domain_realm]` 加短名 → LAKEHOUSE.COM 映射                                                                |
| ③      | `conf/hadoop/core-site.xml`  | `auth_to_local` RULE 加用户 ID 映射                                                                         |
| ④      | `docker-compose.yaml`        | 加 service（hostname 无连字符）                                                                               |
| ⑤      | 组件配置                         | 各组件 conf 里 kerberos principal = FQDN                                                                   |
| ⑥      | 重建 KDC                       | `docker compose down && rm -rf data/kerberos/* conf/kerberos/keytabs/*.keytab && docker compose up -d` |

***

## 五、常见 Kerberos 错误与诊断

| **错误信息**                                              | **根因**                                         | **解决**                                      |
| ----------------------------------------------------- | ---------------------------------------------- | ------------------------------------------- |
| `GSSException: Server not found in Kerberos database` | KDC 里没有对应 principal 或 hostname 不匹配             | `listprincs` 查 principal 名是否精确匹配            |
| `Preauthentication failed`                            | Keytab 里的 key 跟 KDC master key 不匹配             | 删 keytab + 重建 KDC                           |
| `Service principal not authorized`                    | auth\_to\_local 没映射                            | core-site.xml 加 RULE                        |
| `Server not found in Kerberos database`               | `domain_realm` 短名映射缺失                          | krb5.conf 加                                 |
| `kinit: Password incorrect`                           | 密码改过忘了                                         | `kadmin -q 'change_password -pw ...'`       |
| Flink `delegation.token NPE`                          | Flink standalone + Hadoop delegation token 不兼容 | `security.delegation.tokens.enabled: false` |
| Spark Kryo + yarn.archive → EOF                       | Kryo serializer + spark.yarn.archive 分发 jar 冲突 | 用默认 JavaSerializer                          |

***

## 六、Kerberos Ticket 过期重新认证 — 所有组件命令总表

> **背景**：Kerberos TGT 票据默认有效期 24 小时（本环境配置 `ticket_lifetime=24h`）。
> 过期后需要重新 `kinit` 才能访问 HDFS / Hive Metastore / HiveServer2 等受保护资源。
> **服务进程**（HiveServer2 / Flink JM 等）启动时通过 entrypoint 自动 kinit，
> 但 **容器重启** 或 **KDC 重建** 后会丢失，需要手动 kinit。

### 6.1 一次性查看所有票据状态

```bash
# 遍历所有容器的 krb5cc 文件，查看剩余有效时间
for cc in spark flink hive-server hive-metastore namenode datanode resourcemanager nodemanager iceberg-rest hbase-master hbase-regionserver; do
  echo "=== $cc ==="
  docker exec $cc klist 2>&1 | head -3
done
```

### 6.2 各组件 kinit 命令

| 组件 | 容器名 | kinit 命令 | 场景 |
|------|--------|------------|------|
| **Spark SQL / spark-submit** | `spark` | `docker exec spark kinit -kt /etc/security/keytabs/spark.service.keytab spark/sparkmaster.lakehouse.com@LAKEHOUSE.COM` | spark-sql、spark-submit 提交前 ticket 过期 |
| **Flink SQL Client** | `flink-jobmanager` | `docker exec flink-jobmanager kinit -kt /etc/security/keytabs/flink.service.keytab flink/flinkjobmanager.lakehouse.com@LAKEHOUSE.COM` | 进入 sql-client 前、提交作业前 |
| **Hive beeline (容器内)** | `hive-server` | `docker exec hive-server bash -c 'export KRB5CCNAME=/tmp/krb5cc_beeline && kinit -kt /etc/security/keytabs/hive.service.keytab hive/hiveserver.lakehouse.com@LAKEHOUSE.COM && beeline -u "jdbc:hive2://localhost:21066/default;principal=hive/hiveserver.lakehouse.com@LAKEHOUSE.COM" -e "SHOW DATABASES;"'` | beeline 必须 kinit + beeline 同一个 bash 进程 |
| **HDFS 操作** | `namenode` | `docker exec namenode bash -c 'kinit -kt /etc/security/keytabs/nn.service.keytab nn/namenode.lakehouse.com@LAKEHOUSE.COM && hdfs dfs -ls /'` | hdfs dfs -ls/-put/-get 等命令前 |
| **Hive Metastore / YARN RM / NM / Iceberg REST** | 各自容器 | entrypoint 已自动 kinit，**一般不需要手动** | 容器重启后若日志里有 Kerberos 错误再手动 |
| **HBase Master** | `hbase-master` | `docker exec hbase-master bash -c 'kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hbasemaster.lakehouse.com@LAKEHOUSE.COM && hbase shell -e "status"'` | HBase Shell 报 Kerberos 错误时 |
| **DBeaver (Windows Hive)** | — | **Windows 上 kinit**（二选一）<br>A. 密码：`& "C:\Program Files\MIT\Kerberos\bin\kinit.exe" lakehouse@LAKEHOUSE.COM` 输入 `lakehouse123`<br>B. keytab：`& "C:\Program Files\MIT\Kerberos\bin\kinit.exe" -kt C:\kerberos\lakehouse.keytab lakehouse@LAKEHOUSE.COM` | DBeaver 连接 HiveServer2 前先拿 TGT；TGT 过期后必须重新 kinit（DBeaver 不会自动续） |

### 6.3 通配符一键重新认证（所有 service keytab）

```bash
# 对所有有 service keytab 的容器批量 kinit (可复制粘贴直接跑)
for pair in \
  "spark spark.service.keytab sparkmaster" \
  "flink-jobmanager flink.service.keytab flinkjobmanager" \
  "hive-server hive.service.keytab hiveserver" \
  "hive-metastore hive.service.keytab hivemetastore" \
  "namenode nn.service.keytab namenode" \
  "resourcemanager rm.service.keytab resourcemanager" \
  "iceberg-rest iceberg.service.keytab iceberg-rest" \
  "hbase-master hbase.service.keytab hbasemaster"; do
  container=$(echo $pair | awk '{print $1}')
  keytab=$(echo $pair | awk '{print $2}')
  host=$(echo $pair | awk '{print $3}')
  echo -n "$container → "
  docker exec $container kinit -kt /etc/security/keytabs/$keytab $host.lakehouse.com@LAKEHOUSE.COM 2>&1 && echo "OK" || echo "FAIL"
done
```

### 6.4 DBeaver（Windows）TGT 过期处理

DBeaver 连接 HiveServer2 Kerberos 失败（`GSS initiate failed` / `No valid credentials`）时：

```powershell
# 先清掉旧票据
& "C:\Program Files\MIT\Kerberos\bin\kdestroy.exe"

# 重新拿 TGT (密码式)
& "C:\Program Files\MIT\Kerberos\bin\kinit.exe" lakehouse@LAKEHOUSE.COM
# 输入密码: lakehouse123

# 或者 keytab 式（推荐，不用输密码）
& "C:\Program Files\MIT\Kerberos\bin\kinit.exe" -kt C:\kerberos\lakehouse.keytab lakehouse@LAKEHOUSE.COM

# 验证
& "C:\Program Files\MIT\Kerberos\bin\klist.exe"
```

> ⚠️ DBeaver **不会自动续期** Kerberos TGT。TGT 24 小时过期后必须手动 kinit 再重新连接。

***

_最后更新：2026-09-28 — 新增第六节 Kerberos Ticket 过期处理总表 + 所有组件 kinit 命令_
