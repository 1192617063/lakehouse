#!/bin/bash
# PostgreSQL CDC 数据生成脚本
# 在 postgres 容器内执行，创建 cdc_demo 库的 users 表，并持续生成变更数据

set -e

PSQL_CMD="psql -U postgres -d cdc_demo"

echo "=== [1/2] 创建表 ==="
$PSQL_CMD <<'SQL'
CREATE TABLE IF NOT EXISTS users (
    user_id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT,
    city VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 为 CDC 创建 replication slot 和 publication（如果不存在）
SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'flink_slot');

DROP PUBLICATION IF EXISTS flink_pub;
CREATE PUBLICATION flink_pub FOR TABLE users;
SQL

echo "PostgreSQL 表创建完成"
echo ""

echo "=== [2/2] 启动数据生成循环（每 3 秒一条 INSERT + 偶尔 UPDATE） ==="

CITIES=("Beijing" "Shanghai" "Guangzhou" "Shenzhen" "Hangzhou" "Chengdu" "Nanjing" "Wuhan")

while true; do
    USERNAME="user_$((RANDOM % 100000))"
    EMAIL="${USERNAME}@example.com"
    AGE=$((RANDOM % 50 + 18))
    CITY=${CITIES[$((RANDOM % 8))]}

    ACTION=$((RANDOM % 10))
    if [ $ACTION -lt 8 ]; then
        # INSERT
        $PSQL_CMD -c "
            INSERT INTO users (username, email, age, city)
            VALUES ('${USERNAME}', '${EMAIL}', ${AGE}, '${CITY}');
        " 2>/dev/null
        echo "[$(date '+%H:%M:%S')] INSERT user: ${USERNAME} (${CITY}, age ${AGE})"
    else
        # UPDATE - 更新某个用户的 city
        MAX_ID=$($PSQL_CMD -t -A -c "SELECT COALESCE(MAX(user_id),1) FROM users;" 2>/dev/null)
        if [ "$MAX_ID" -gt 0 ] 2>/dev/null; then
            TARGET=$((RANDOM % MAX_ID + 1))
            NEW_CITY=${CITIES[$((RANDOM % 8))]}
            $PSQL_CMD -c "
                UPDATE users SET city='${NEW_CITY}', updated_at=CURRENT_TIMESTAMP WHERE user_id=${TARGET};
            " 2>/dev/null
            echo "[$(date '+%H:%M:%S')] UPDATE user ${TARGET} -> city=${NEW_CITY}"
        fi
    fi

    sleep 3
done
