#!/bin/bash
# Release 打包脚本
# 用法: ./scripts/release.sh
# 注意：Release 不包含大二进制（HBase tarball、Hadoop lib、Spark tarballs）
#      首次启动时 docker compose build 会自动下载它们

set -euo pipefail

VERSION=$(date +%Y%m%d)
RELEASE_NAME="lakehouse-release-${VERSION}"
RELEASE_DIR="/tmp/${RELEASE_NAME}"
RELEASE_TGZ="/tmp/${RELEASE_NAME}.tar.gz"

echo "========================================"
echo "  Lakehouse Release: ${RELEASE_NAME}"
echo "========================================"

# 1. 清理旧目录
rm -rf "${RELEASE_DIR}" "${RELEASE_TGZ}"
mkdir -p "${RELEASE_DIR}"

# 2. 拷贝必要文件
echo "拷贝文件..."
cp -r \
  build \
  conf \
  docs \
  scripts \
  sql \
  docker-compose.yaml \
  README.md \
  .env.example \
  "${RELEASE_DIR}/"

# 3. 清理 release 中不该有的文件
echo "清理运行时/大文件..."
rm -rf "${RELEASE_DIR}/conf/kerberos/keytabs/"*.keytab 2>/dev/null || true
rm -rf "${RELEASE_DIR}/data" 2>/dev/null || true
# 删除大二进制（首次 build 自动下载）
rm -f "${RELEASE_DIR}/build/hbase/"*.tar.gz 2>/dev/null || true
rm -f "${RELEASE_DIR}/build/spark/"*.tar.gz 2>/dev/null || true
rm -rf "${RELEASE_DIR}/build/spark-jars/" 2>/dev/null || true
# 递归清理所有 tarballs
find "${RELEASE_DIR}" -name "*.tar.gz" -delete 2>/dev/null || true
find "${RELEASE_DIR}" -type d -empty -delete 2>/dev/null || true

# 4. 打包
echo "打包..."
tar -czf "${RELEASE_TGZ}" -C /tmp "${RELEASE_NAME}"

echo ""
echo "========================================"
echo "  ✅ Release 打包完成！"
echo "========================================"
echo "文件: ${RELEASE_TGZ}"
echo "大小: $(du -sh "${RELEASE_TGZ}" | cut -f1)"
echo ""
echo "解压后部署:"
echo "  tar -xzf ${RELEASE_TGZ}"
echo "  cd ${RELEASE_NAME}"
echo "  cp .env.example .env"
echo "  docker compose build --no-cache   # 可选：首次 build 大镜像"
echo "  docker compose up -d"
echo ""
