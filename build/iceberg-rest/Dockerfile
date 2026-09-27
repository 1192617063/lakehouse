FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/iceberg-rest-fixture:1.10.1

USER root

RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends krb5-user && \
    rm -rf /var/lib/apt/lists/*

COPY IcebergRestLauncher.java /opt/iceberg/src/IcebergRestLauncher.java
COPY entrypoint.sh /opt/iceberg/bin/entrypoint.sh
RUN chmod +x /opt/iceberg/bin/entrypoint.sh

ENTRYPOINT ["/opt/iceberg/bin/entrypoint.sh"]
