lakehouse 数据湖平台完整 Docker Compose 部署方案
## 项目概述
基于 Docker Compose 的一键启动数据湖平台，集成 Kerberos 统一认证，
支持多引擎（Spark/Flink/Hive/Trino）访问 HDFS + Iceberg + Paimon + Hudi。

## 核心组件（均启用 Kerberos 认证）
- **HDFS**: NameNode + DataNode (Hadoop 3.3.6)
- **Kerberos**: 内置 KDC，自动为所有组件生成 principal 和 keytab
- **Hive**: Metastore + HiveServer2 (3.1.3, MySQL 后端)
- **Spark**: 3.5.6 standalone 模式
- **Flink**: 1.19.1 CDC 流处理 + SQL Client
- **Trino**: 482 查询联邦引擎
- **Iceberg REST**: 1.10.1 服务
- **Kafka**: 3.9.0 (KRaft 模式)
- **Doris**: FE + BE 查询加速
- **MySQL / PostgreSQL / MongoDB**: 业务数据库

## 目录结构
- build/       各组件 Dockerfile + KDC 初始化脚本
- conf/        所有服务配置（Kerberos keytabs 不入库，运行时生成）
- sql/         CDC 管道 SQL + Spark 离线数仓脚本
- scripts/     辅助脚本（数据生成、集群状态等）
- docs/        部署说明 + 故障排查
- .env         镜像地址本地配置（随仓库提供，可按需修改）
- docker-compose.yaml  统一编排入口

## 安全
- .gitignore 排除 data/（运行时数据）、lib/（预打包二进制）
- Kerberos keytabs 由 KDC 在容器启动时自动生成，不入库
- .env 随仓库提供镜像地址，各环境可按需修改

## 已知限制
- Spark standalone distributed 模式下 executor 无法完整获取 HDFS delegation token，
  建议使用 local[*] 模式或切换到 Spark on YARN（待接入 YARN 组件）
