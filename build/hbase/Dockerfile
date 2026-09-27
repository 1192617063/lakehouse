# HBase 2.5.3 + Java 11 + 原生 Hadoop 2.10.2 jars（HBase 自带）
# 关键：HBase 2.5.3 用 Hadoop 2.x compat layer（HdfsFileStatus 2.x 是 class）
# Kerberos：把 core-site.xml 挂到 HBase classpath 上（/opt/hbase/conf/core-site.xml）
# Hadoop Configuration 加载顺序：classpath:/core-default.xml → classpath:/core-site.xml

FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.5.3

ENV HBASE_HOME=/opt/hbase
ENV JAVA_HOME=/opt/java/openjdk

# Kerberos client + netcat
RUN apt-get update -qq && \
    apt-get install -y -qq krb5-user netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

COPY hbase-${HBASE_VERSION}-bin.tar.gz /tmp/hbase.tar.gz

# 解压 HBase —— 保留所有自带 jars（Hadoop 2.10.2）
RUN tar -xzf /tmp/hbase.tar.gz -C /opt && \
    mv /opt/hbase-${HBASE_VERSION} /opt/hbase && \
    rm /tmp/hbase.tar.gz && \
    echo "✅ HBase ${HBASE_VERSION} ready" && \
    echo "  Hadoop common: $(ls /opt/hbase/lib/hadoop-common*.jar)" && \
    echo "  HDFS class:    $(jar tf /opt/hbase/lib/hadoop-hdfs-*.jar | grep HdfsFileStatus.class | head -1)"

# hbase-env.sh 基础设置
RUN echo 'export JAVA_HOME=/opt/java/openjdk' >> /opt/hbase/conf/hbase-env.sh && \
    echo 'export HADOOP_HOME=/opt/hadoop' >> /opt/hbase/conf/hbase-env.sh && \
    echo "✅ hbase-env.sh configured"

EXPOSE 16010 16020 16030 9090 2181

WORKDIR /opt/hbase
