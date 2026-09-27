# DBeaver 连接湖仓数据库指南（Windows 客户端）

> 本文档说明如何从 Windows 上的 DBeaver 连接湖仓中的所有数据库，
> 包括需要 Kerberos 认证的 Hive Server2。

## 一、环境信息

| 项目 | 值 |
|------|-----|
| 服务器 IP（Linux 宿主机） | `172.24.64.215`（替换为你的实际 IP） |
| Kerberos Realm | `LAKEHOUSE.COM` |
| KDC 主机 | `kerberos`（端口 `88`） |
| 平台用户 | `lakehouse@LAKEHOUSE.COM` |
| 平台用户 keytab | `conf/kerberos/keytabs/lakehouse.keytab` |

## 二、连接清单总览

| 数据库 | 端口 | 认证方式 | 用户名 | 密码 |
|--------|------|---------|--------|------|
| MySQL | 3306 | 密码 | `root` | `root123` |
| PostgreSQL | 5432 | 密码 | `postgres` | `postgres123` |
| MongoDB | 27017 | 密码 | `root` | `root123` |
| Doris | 9030 | 无密码 | `root` | （空） |
| Trino | 8085 | 无认证 | 任意 | （空） |
| Hive Server2 | 21066 | **Kerberos** | `lakehouse` | （keytab） |

## 三、无需 Kerberos 的数据库连接

### 3.1 MySQL

| 配置项 | 值 |
|--------|-----|
| 主机 | `172.24.64.215` |
| 端口 | `3306` |
| 数据库 | `cdc_demo` |
| 用户名 | `root` |
| 密码 | `root123` |
| 驱动 | MySQL 8 |

DBeaver → 新建连接 → MySQL → 填入上表即可。

### 3.2 PostgreSQL

| 配置项 | 值 |
|--------|-----|
| 主机 | `172.24.64.215` |
| 端口 | `5432` |
| 数据库 | `cdc_demo` |
| 用户名 | `postgres` |
| 密码 | `postgres123` |
| 驱动 | PostgreSQL |

### 3.3 MongoDB

| 配置项 | 值 |
|--------|-----|
| 主机 | `172.24.64.215` |
| 端口 | `27017` |
| 认证数据库 | `admin` |
| 用户名 | `root` |
| 密码 | `root123` |
| 驱动 | MongoDB |

> DBeaver 新建连接 → MongoDB → 在 "Authentication" 标签页选择 `SCRAM-SHA-1`，
> 填写用户名/密码，认证库填 `admin`。

### 3.4 Doris

| 配置项 | 值 |
|--------|-----|
| 主机 | `172.24.64.215` |
| 端口 | `9030` |
| 用户名 | `root` |
| 密码 | （空） |
| 驱动 | Doris / MySQL 兼容 |

> Doris FE 端口 9030 兼容 MySQL 协议，也可用 MySQL 驱动连接。

### 3.5 Trino

| 配置项 | 值 |
|--------|-----|
| 主机 | `172.24.64.215` |
| 端口 | `8085` |
| 用户名 | 任意（如 `admin`） |
| 密码 | （空） |
| 驱动 | Trino |

> Trino 服务端未启用 Kerberos（HTTP 模式），客户端无需认证。
> Trino 内部通过 Kerberos 访问 Hive Metastore 和 HDFS，对用户透明。

## 四、Hive Server2（Kerberos 认证）

Hive Server2 启用了 Kerberos 认证，从 Windows 连接需要额外配置。

### 4.1 准备 Kerberos 文件

从 Linux 服务器拷贝以下两个文件到 Windows，**建议统一放在 `C:\kerberos\` 目录**（路径中不要有空格和中文）。

1. `conf/kerberos/krb5.conf` → Windows 上重命名为 `krb5.ini`
2. `conf/kerberos/keytabs/lakehouse.keytab`

#### 方式一：PowerShell scp 拷贝

```powershell
# 先创建目录
New-Item -ItemType Directory -Force -Path C:\kerberos

# 拷贝文件（替换为你的服务器 IP 和用户名）
scp admin@172.24.64.215:/home/admin/lakehouse/conf/kerberos/krb5.conf C:\kerberos\krb5.ini
scp admin@172.24.64.215:/home/admin/lakehouse/conf/kerberos/keytabs/lakehouse.keytab C:\kerberos\lakehouse.keytab
```

#### 方式二：直接从项目目录复制

如果能直接访问服务器文件系统，手动复制：
- `krb5.conf` → `C:\kerberos\krb5.ini`（**必须改扩展名**，Windows Java 识别 `.ini`）
- `lakehouse.keytab` → `C:\kerberos\lakehouse.keytab`

#### 最终目录结构

```
C:\kerberos\
├── krb5.ini          ← Kerberos 配置（原 krb5.conf 改名）
└── lakehouse.keytab  ← lakehouse 平台用户的密钥表
```

> **路径注意事项**：
> - 路径中**避免空格**（如 `C:\Program Files\`），否则需要加引号且容易出错
> - 路径中**避免中文**，防止编码问题
> - 推荐使用 `C:\kerberos\` 这样的简单路径

### 4.2 修改 krb5.ini（Windows 版）

由于 Windows 上无法解析 `kerberos` 这个 Docker 内部主机名，需要把 KDC 地址改成服务器 IP。

用记事本或 VS Code 打开 `C:\kerberos\krb5.ini`，完整内容如下：

```ini
[libdefaults]
    default_realm = LAKEHOUSE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    udp_preference_limit = 1
    default_ccache_name = FILE:%TEMP%\krb5cc_%{uid}

[realms]
    LAKEHOUSE.COM = {
        kdc = 172.24.64.215:88
        admin_server = 172.24.64.215:749
        default_domain = lakehouse.com
    }

[domain_realm]
    .lakehouse.com = LAKEHOUSE.COM
    lakehouse.com = LAKEHOUSE.COM
```

> **关键修改点**：
> - `kdc`：从 `kerberos` → `172.24.64.215:88`（你的服务器 IP + KDC 端口）
> - `admin_server`：从 `kerberos` → `172.24.64.215:749`
> - `default_ccache_name`：改成 `FILE:%TEMP%\krb5cc_%{uid}`，避免写入 `/tmp` 失败
>
> **保存编码**：务必保存为 **ANSI 或 UTF-8 无 BOM**，不要带 BOM，否则 Java 解析可能出错。

### 4.3 配置 DBeaver JVM 参数（指定 krb5.ini 路径）

DBeaver 通过 JVM 系统属性 `java.security.krb5.conf` 读取 Kerberos 配置。需要修改 `dbeaver.ini`。

#### 找到 dbeaver.ini 的位置

根据 DBeaver 安装方式不同，位置不同：

| 安装方式 | dbeaver.ini 路径 |
|---------|-----------------|
| 安装版（默认） | `C:\Program Files\DBeaver\dbeaver.ini` |
| 安装版（自定义） | 你安装的目录下 |
| 便携版（zip） | 解压目录下的 `dbeaver.ini` |
| 应用商店版 | 较难修改，建议改用便携版 |

> 可用 PowerShell 查找：`Get-ChildItem -Path C:\ -Recurse -Filter dbeaver.ini -ErrorAction SilentlyContinue | Select-Object FullName`

#### 修改 dbeaver.ini

在 `dbeaver.ini` 文件中，找到 `-vmargs` 这一行，**在它后面**添加三行：

```ini
-vmargs
-Djava.security.krb5.conf=C:\kerberos\krb5.ini
-Djavax.security.auth.useSubjectCredsOnly=false
-Dsun.security.krb5.debug=false
```

**完整示例**（`-vmargs` 之后的部分）：

```ini
-vmargs
-XX:+IgnoreUnrecognizedVMOptions
-Dosgi.requiredJavaVersion=17
-Dosgi.instance.area.default=@user.home/dbeaver-data
-Djava.security.krb5.conf=C:\kerberos\krb5.ini
-Djavax.security.auth.useSubjectCredsOnly=false
-Dsun.security.krb5.debug=false
-Xms256m
-Xmx2048m
```

> **路径写法注意**：
> - Windows 路径用反斜杠 `\`，Java 能正确识别
> - 如果路径含空格，**不要加引号**，直接写：`-Djava.security.krb5.conf=C:\Program Files\DBeaver\krb5.ini`（JVM 参数不支持引号包裹值）
> - 因此强烈建议把文件放在无空格路径如 `C:\kerberos\`

修改后**必须完全关闭并重启 DBeaver**（不是重新连接，是退出程序再打开）。

#### 验证 krb5.ini 是否被加载

启动 DBeaver 后，菜单 → 窗口 → 查看日志，搜索 `krb5`，确认没有加载错误。
或临时把 `-Dsun.security.krb5.debug=true`，在 DBeaver 控制台/日志中看到 `Krb5Conf` 相关输出即表示加载成功。

### 4.4 创建 Hive 连接（指定 keytab 路径）

DBeaver → 新建连接 → Apache Hive → 填写：

| 配置项 | 值 |
|--------|-----|
| 主机 | `172.24.64.215` |
| 端口 | `21066` |
| 数据库/模式 | `default` |
| 认证（Authentication） | `Kerberos` |
| Principal | `hive/hiveserver.lakehouse.com@LAKEHOUSE.COM` |
| Keytab | `C:\kerberos\lakehouse.keytab` |
| User principal | `lakehouse@LAKEHOUSE.COM` |

> **各字段含义**：
> - **Principal**：Hive 服务端的 Kerberos 主体，格式为 `服务名/主机名@REALM`，必须与服务端 `hive-site.xml` 中 `hive.server2.authentication.kerberos.principal` 一致
> - **Keytab**：Windows 本地 keytab 文件的**绝对路径**，用反斜杠 `\`
> - **User principal**：使用 keytab 的用户主体，必须与 keytab 中的 principal 一致（本环境为 `lakehouse@LAKEHOUSE.COM`）
>
> **JDBC URL**（DBeaver 自动生成，无需手动输入）：
> `jdbc:hive2://172.24.64.215:21066/default;principal=hive/hiveserver.lakehouse.com@LAKEHOUSE.COM`

#### （可选）手动用 keytab 验证认证

如果安装了 MIT Kerberos for Windows，可先在命令行验证 keytab 是否可用：

```powershell
& "C:\Program Files\MIT\Kerberos\bin\kinit.exe" -kt C:\kerberos\lakehouse.keytab lakehouse@LAKEHOUSE.COM
& "C:\Program Files\MIT\Kerberos\bin\klist.exe"
```

能看到票据则说明 keytab 和 KDC 连接正常，DBeaver 连接失败则是 DBeaver 自身配置问题。

### 4.5 hosts 配置（可选但推荐）

为避免 Kerberos 反向 DNS 解析问题，在 Windows 的 `C:\Windows\System32\drivers\etc\hosts` 中添加：

```
172.24.64.215  hiveserver.lakehouse.com
172.24.64.215  kerberos
```

## 五、验证连接

连接成功后，可执行以下 SQL 验证：

```sql
-- MySQL
SHOW DATABASES;
SELECT * FROM cdc_demo.orders LIMIT 5;

-- PostgreSQL
\dt
SELECT * FROM users LIMIT 5;

-- Doris
SHOW DATABASES;

-- Trino
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.cdc_demo.orders LIMIT 5;

-- Hive Server2
SHOW DATABASES;
SELECT * FROM cdc_demo.orders LIMIT 5;
```

## 六、常见问题

### Q1: DBeaver 连接 Hive 报 "Unable to obtain password from user"

- 检查 `dbeaver.ini` 中 `java.security.krb5.conf` 路径是否正确（用反斜杠，不要有空格）
- 确认 `krb5.ini` 中 KDC 地址为服务器 IP 而非 `kerberos`
- 检查 keytab 文件路径是否正确，文件是否存在
- 确认 `-Djavax.security.auth.useSubjectCredsOnly=false` 已添加
- 修改 `dbeaver.ini` 后是否**完全重启**了 DBeaver

### Q2: 报 "Cannot locate KDC" / "Cannot get kdc for realm"

- `krb5.ini` 中 `kdc` 地址写错，确认是 `服务器IP:88`
- 确认服务器防火墙开放了 88 端口（Kerberos KDC）
- 确认 `krb5.ini` 没有 BOM 头（用 VS Code 另存为 UTF-8 无 BOM）

### Q3: 报 "Server not found in Kerberos database"

- 确认 `Principal` 填的是 `hive/hiveserver.lakehouse.com@LAKEHOUSE.COM`
- 检查 Windows hosts 文件是否配置了 `hiveserver.lakehouse.com` 指向服务器 IP
- Kerberos 会对主机名做反向解析，hosts 配置可避免解析失败

### Q4: 报 "KDC has no support for encryption type"

- 确认 `krb5.ini` 中 `[libdefaults]` 的加密类型配置与服务端一致
- 本环境使用默认加密类型，通常无需额外配置

### Q5: Trino 连接正常但查询 Hive/Iceberg 表报错

- Trino 服务端通过 Kerberos 访问 HDFS 和 HMS，检查 Trino 容器日志
- 客户端无需 Kerberos，但服务端 keytab 需有效（`trino.service.keytab`）

### Q6: MongoDB 连接报 "Authentication failed"

- 确认认证数据库填 `admin`
- 驱动选择 SCRAM-SHA-1 认证方式

### Q7: keytab 路径正确但仍报 "Keytab file not found"

- 在 DBeaver 中 keytab 路径**不要加引号**，直接写 `C:\kerberos\lakehouse.keytab`
- 确认文件扩展名是 `.keytab` 而非 `.keytab.txt`（Windows 可能隐藏扩展名）
- 在文件资源管理器中勾选"文件扩展名"查看真实文件名

### Q8: 修改了 dbeaver.ini 但不生效

- 确认修改的是**正在运行的那个** DBeaver 的 `dbeaver.ini`（可能装了多个版本）
- 必须**完全退出** DBeaver 进程（任务管理器确认）再重新打开
- 便携版和安装版的 `dbeaver.ini` 位置不同，参考 4.3 节

## 七、连接速查表

| 数据库 | 连接 URL 模板 |
|--------|--------------|
| MySQL | `jdbc:mysql://172.24.64.215:3306/cdc_demo` |
| PostgreSQL | `jdbc:postgresql://172.24.64.215:5432/cdc_demo` |
| MongoDB | `mongodb://root:root123@172.24.64.215:27017/?authSource=admin` |
| Doris | `jdbc:mysql://172.24.64.215:9030/` |
| Trino | `jdbc:trino://172.24.64.215:8085` |
| Hive | `jdbc:hive2://172.24.64.215:21066/default;principal=hive/hiveserver.lakehouse.com@LAKEHOUSE.COM` |
