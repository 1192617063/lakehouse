-- 注意：Flink 1.19 SQL Client 不支持通过配置文件(table.catalogs)定义 catalog
-- 必须通过 DDL 创建，创建后会持久化到 table.catalog-store.file.path 指定的目录
-- 因此每次启动时用 DROP + CREATE 保证 catalog 一致

-- Iceberg REST Catalog
DROP CATALOG IF EXISTS iceberg_catalog;
CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest:8181',
  'warehouse'='hdfs://namenode:9000/user/iceberg'
);

-- Paimon Catalog（基于 Hive Metastore）
DROP CATALOG IF EXISTS paimon_catalog;
CREATE CATALOG paimon_catalog WITH (
  'type'='paimon',
  'metastore'='hive',
  'uri'='thrift://hivemetastore.lakehouse.com:9083',
  'warehouse'='hdfs://namenode:9000/user/paimon',
  'hive-conf-dir'='/opt/flink/conf',
  'hive.metastore.kerberos.principal'='hive/hivemetastore.lakehouse.com@LAKEHOUSE.COM'
);

-- Hudi Catalog（基于 Hive Metastore）
DROP CATALOG IF EXISTS hudi_catalog;
CREATE CATALOG hudi_catalog WITH (
  'type'='hudi',
  'catalog.path'='hdfs://namenode:9000/user/hudi/catalog',
  'mode'='hms',
  'hive.conf.dir'='/opt/flink/conf'
);

-- Hive Catalog
DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG hive_catalog WITH (
  'type'='hive',
  'hive-conf-dir'='/opt/flink/conf'
);

USE CATALOG iceberg_catalog;
