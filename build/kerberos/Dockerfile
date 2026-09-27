FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV KRB5_REALM=LAKEHOUSE.COM
ENV KRB5_KDC=kerberos
ENV KRB5_ADMIN_SERVER=kerberos

RUN apt-get update && \
    apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-user && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY kdc-init.sh /usr/local/bin/kdc-init.sh
RUN chmod +x /usr/local/bin/kdc-init.sh

EXPOSE 88 749

CMD ["/usr/local/bin/kdc-init.sh"]
