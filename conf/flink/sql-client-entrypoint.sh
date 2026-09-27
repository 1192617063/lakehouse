#!/usr/bin/env bash
set -e
echo "[entrypoint] Running kinit with flink keytab..."
kinit -kt /etc/security/keytabs/flink.service.keytab flink/flink-jobmanager.lakehouse.com@LAKEHOUSE.COM \
  && echo "[entrypoint] kinit succeeded" \
  || echo "[entrypoint] WARNING: kinit failed"
exec "$@"
