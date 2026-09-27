#!/bin/bash
set -e

kinit -kt /etc/security/keytabs/iceberg.service.keytab iceberg/icebergrest.lakehouse.com@LAKEHOUSE.COM
echo "Kerberos ticket obtained for iceberg/icebergrest.lakehouse.com@LAKEHOUSE.COM"

exec java \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -cp "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/opt/iceberg/libs/mysql-connector-j-8.4.0.jar:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*" \
  /opt/iceberg/src/IcebergRestLauncher.java "$@"
