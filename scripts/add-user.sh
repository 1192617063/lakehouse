#!/bin/bash
# ============================================================
# add-user.sh — 为 Lakehouse Kerberos KDC 新增用户/服务 principal
#
# 两种模式：
#   1. 用户 principal：add-user.sh alice [password]  → alice@REALM
#   2. 服务 principal：add-user.sh --service flink [host] → flink/fqdn@REALM
#
# 自动做的事：
#   - 在 KDC 里 addprinc（user 给随机密码 / 服务用 -randkey）
#   - 生成 keytab 到 conf/kerberos/keytabs/
#   - 可选：更新 hbase-site.xml auth_to_local rules（如果 HBase 态）
#
# 参考：docs/APPENDIX_KERBEROS.md 「新增用户 Step-by-Step」
# ============================================================
set -euo pipefail

REALM="${KRB5_REALM:-LAKEHOUSE.COM}"
KDC_HOST="${KRB5_KDC:-kerberos}"
ADMIN_PRINCIPAL="${KRB5_ADMIN_PRINCIPAL:-admin/admin}"
ADMIN_PASSWORD="${KRB5_ADMIN_PASSWORD:-admin123}"
KEYTAB_DIR="$(cd "$(dirname "$0")/.." && pwd)/conf/kerberos/keytabs"

# ---------- 参数解析 ----------
SERVICE_MODE=false
USER_NAME=""
SERVICE_NAME=""
SERVICE_HOST=""
PASSWORD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service)  SERVICE_MODE=true; shift ;;
    --host)     SERVICE_HOST="$2"; shift 2 ;;
    --password) PASSWORD="$2"; shift 2 ;;
    --realm)    REALM="$2"; shift 2 ;;
    -h|--help)
      cat <<USAGE
用法:
  $(basename "$0") <用户名> [password]
    新增用户 principal (user@REALM)，生成 conf/kerberos/keytabs/<user>.keytab

  $(basename "$0") --service <服务名> [--host <fqdn>]
    新增服务 principal (service/fqdn@REALM)，生成 keytab
    默认 host: \$(hostname -f) 或 ${KDC_HOST}

例子:
  $(basename "$0") alice                              # 用户 alice@LAKEHOUSE.COM
  $(basename "$0") alice MyP@ssw0rd                   # 用户 + 指定密码
  $(basename "$0") --service trino --host trino.lakehouse.com
  $(basename "$0") --service spark

前置: KDC 容器必须 up（docker compose up -d kerberos）
USAGE
      exit 0 ;;
    *)
      if [ -z "$USER_NAME" ]; then
        if $SERVICE_MODE; then SERVICE_NAME="$1"; else USER_NAME="$1"; fi
      elif [ -z "$PASSWORD" ] && ! $SERVICE_MODE; then
        PASSWORD="$1"
      fi
      shift ;;
  esac
done

# ---------- 前置检查 ----------
echo "🔍 前置检查..."
if ! docker compose ps --format '{{.Name}}' 2>/dev/null | grep -q "^kerberos"; then
  echo "❌ KDC 容器没起来！先: docker compose up -d kerberos"
  exit 1
fi
mkdir -p "$KEYTAB_DIR"

# ---------- 构造 principal ----------
if $SERVICE_MODE; then
  if [ -z "$SERVICE_NAME" ]; then
    echo "❌ --service 模式需要服务名"
    exit 1
  fi
  HOST="${SERVICE_HOST:-$(docker exec kerberos hostname -f 2>/dev/null || echo ${KDC_HOST})}"
  PRINCIPAL="${SERVICE_NAME}/${HOST}@${REALM}"
  KEYTAB_NAME="${SERVICE_NAME}.service.keytab"
  ADD_MODE="randkey"   # 服务 principal 用随机 key（无密码）
else
  if [ -z "$USER_NAME" ]; then
    echo "❌ 请给用户名"
    exit 1
  fi
  PRINCIPAL="${USER_NAME}@${REALM}"
  KEYTAB_NAME="${USER_NAME}.keytab"
  if [ -z "$PASSWORD" ]; then
    PASSWORD=$(openssl rand -base64 12 | tr -dc 'a-zA-Z0-9' | head -c 16)
    echo "📝 随机生成密码: $PASSWORD（记下来，只显示这一次）"
  fi
  ADD_MODE="pw"        # 用户 principal 用指定密码
fi

echo "📋 Principal: $PRINCIPAL"
echo "📁 Keytab:    $KEYTAB_DIR/$KEYTAB_NAME"

# ---------- 在 KDC 里操作 ----------
echo ""
echo "🔑 在 KDC 添加 principal..."

# 用 kadmin（跨网络，需要 admin/admin）而非 kadmin.local（必须在 KDC 容器内本地）
KADMIN_CMD="docker exec kerberos kadmin -p ${ADMIN_PRINCIPAL} -w ${ADMIN_PASSWORD} -q"

# 先看 principal 是否已存在
EXISTS=$(${KADMIN_CMD} "getprinc ${PRINCIPAL}" 2>&1) || true
if echo "$EXISTS" | grep -qi "Principal does not exist"; then
  echo "  新增 principal..."
  if [ "$ADD_MODE" = "randkey" ]; then
    ${KADMIN_CMD} "addprinc -randkey ${PRINCIPAL}" 2>&1
  else
    ${KADMIN_CMD} "addprinc -pw ${PASSWORD} ${PRINCIPAL}" 2>&1
  fi
else
  echo "  ⚠️  principal 已存在，跳过 addprinc"
fi

# 导出 keytab：在 KDC 容器里直接写宿主机 volume 挂载路径
echo "  导出 keytab..."
KEYTAB_HOST_PATH="${KEYTAB_DIR}/${KEYTAB_NAME}"
# 方式 A：如果 keytab 目录已挂到 KDC 容器，直接 ktadd 写绝对路径
# 方式 B：否则先生成到容器内临时位置，再 docker cp
TMP_IN_KDC="/tmp/${KEYTAB_NAME}.tmp"
if docker exec kerberos ls "$KEYTAB_DIR" >/dev/null 2>&1; then
  ${KADMIN_CMD} "ktadd -k ${KEYTAB_HOST_PATH} ${PRINCIPAL}" 2>&1
else
  ${KADMIN_CMD} "ktadd -k ${TMP_IN_KDC} ${PRINCIPAL}" 2>&1
  docker cp "kerberos:${TMP_IN_KDC}" "${KEYTAB_HOST_PATH}" 2>/dev/null
  docker exec kerberos rm -f "${TMP_IN_KDC}" 2>/dev/null || true
fi
chmod 600 "${KEYTAB_HOST_PATH}" 2>/dev/null || true
ls -la "${KEYTAB_HOST_PATH}" 2>&1

# ---------- 验证 ----------
echo ""
echo "✅ 验证 keytab 可用性（在 KDC 容器内 kinit + klist）..."
# KDC 容器里应该有 keytab 目录挂载，或者我们可以从宿主机路径拷过去
if docker exec kerberos ls -d "$(dirname "$KEYTAB_HOST_PATH")" >/dev/null 2>&1; then
  # 已挂载 volume，直接用
  docker exec kerberos bash -c "kinit -kt ${KEYTAB_HOST_PATH} ${PRINCIPAL} 2>&1 && klist 2>&1 | head -3"
else
  # 拷进 KDC 临时位置
  docker cp "${KEYTAB_HOST_PATH}" "kerberos:${TMP_IN_KDC}" 2>/dev/null
  docker exec kerberos bash -c "kinit -kt ${TMP_IN_KDC} ${PRINCIPAL} 2>&1 && klist 2>&1 | head -3"
fi

echo ""
echo "🎉 完成！"
echo "   Principal: $PRINCIPAL"
echo "   Keytab:    $KEYTAB_DIR/$KEYTAB_NAME"
if [ "$ADD_MODE" = "pw" ]; then
  echo "   Password:  $PASSWORD"
fi
echo ""
echo "📖 下一步：docs/APPENDIX_KERBEROS.md → 新增用户 Step-by-Step"
echo "   - 如果是新组件服务：kdc-init.sh 里补上 addprinc + ktadd 段"
echo "   - 如果是用户：HBase 态可能要加 auth_to_local rules（让 HBase 能映射到 Unix 用户）"
echo "   - 给该用户的 kinit: kinit -kt $KEYTAB_NAME $(basename "$PRINCIPAL")"
