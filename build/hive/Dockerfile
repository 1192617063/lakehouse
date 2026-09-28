FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/apache/hive:3.1.3

# ============================================================
# Hive 自定义镜像（基于 apache/hive:3.1.3）
# 修复:
#   1. beeline 需要 Kerberos 客户端 (kinit/klist/kdestroy)
#   2. hive 用户没有 /home/hive 目录，beeline 报 No such file
#   3. SLF4J multiple bindings 警告 (Hadoop 的 slf4j-reload4j vs Hive 的 log4j-slf4j-impl)
#      → 无法修改 Hadoop volume，只能接受这个 WARN (不影响功能)
#
# 注: Debian 11 bullseye 已 EOL，安全补丁 deb11u8 包在所有镜像源 404
#     这里使用主仓库 deb11u5 版本手动 dpkg -i 安装
# ============================================================

USER root

# 离线安装 Kerberos 客户端 (kinit/klist/kdestroy)
# 所有 .deb 文件预下载自 deb.debian.org bullseye main repo
COPY build/hive/debs/*.deb /tmp/krb5-debs/

# 先装 lib 依赖，再装 krb5-user
# --force-depends 跳过 krb5-config → bind9-host 依赖 (我们不需要 DNS SRV)
RUN dpkg -i /tmp/krb5-debs/libkrb5support0_1.18.3-6+deb11u5_amd64.deb \
           /tmp/krb5-debs/libkrb5-3_1.18.3-6+deb11u5_amd64.deb \
           /tmp/krb5-debs/libgssapi-krb5-2_1.18.3-6+deb11u5_amd64.deb \
 && dpkg -i /tmp/krb5-debs/libgssrpc4_1.18.3-6+deb11u5_amd64.deb \
            /tmp/krb5-debs/libkadm5clnt-mit12_1.18.3-6+deb11u5_amd64.deb \
            /tmp/krb5-debs/libkadm5srv-mit12_1.18.3-6+deb11u5_amd64.deb \
            /tmp/krb5-debs/libkdb5-10_1.18.3-6+deb11u5_amd64.deb \
 && dpkg -i --force-depends /tmp/krb5-debs/krb5-user_1.18.3-6+deb11u5_amd64.deb \
 && rm -rf /tmp/krb5-debs

# 创建 hive 用户的 home 目录 (beeline 需要写入 .beeline 状态文件)
RUN mkdir -p /home/hive && chown hive:hive /home/hive

USER hive
