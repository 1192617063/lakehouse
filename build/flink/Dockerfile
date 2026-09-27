ARG FLINK_BASE_IMAGE
FROM ${FLINK_BASE_IMAGE}

RUN apt-get update -qq && \
    apt-get install -y -qq krb5-user && \
    rm -rf /var/lib/apt/lists/*

COPY lib/flink/extra/*.jar /opt/flink/lib/extra/
