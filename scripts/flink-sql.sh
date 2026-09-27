#!/usr/bin/env bash
set -e
docker exec -it flink-sql-client bash -c '
export FLINK_CLASSPATH=/opt/flink/lib/extra/*
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
/opt/flink/bin/sql-client.sh -i /opt/flink/conf/sql-client-init.sql
'
