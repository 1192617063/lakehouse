ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}

USER root

# 安装 Kerberos 客户端，用于 kinit 认证 HDFS
RUN apt-get update -qq && \
    apt-get install -y -qq krb5-user libpam-krb5 2>&1 | grep -v "warning:" || true && \
    rm -rf /var/lib/apt/lists/*

# 拷贝入口脚本（处理 Kerberos 认证后再启动 Spark）
COPY conf/spark/spark-entrypoint.sh /opt/bitnami/spark/conf/spark-entrypoint.sh
RUN chmod +x /opt/bitnami/spark/conf/spark-entrypoint.sh

# 拷贝扩展 jar（Iceberg/Hudi/Paimon/JDBC/MongoDB）
COPY lib/spark/*.jar /opt/bitnami/spark/extra-jars/

# 注意: 所有配置(spark-defaults.conf/hive-site.xml/krb5.conf/hadoop conf)
#       均通过 volume 挂载，避免每次修改配置都要重建镜像
