#!/bin/bash
# MySQL CDC 数据生成脚本
# 在 mysql 容器内执行，创建 cdc_demo 库、orders/products 表，并持续生成变更数据

set -e

MYSQL_CMD="mysql -uroot -proot123"

echo "=== [1/3] 创建数据库和表 ==="
$MYSQL_CMD <<'SQL'
CREATE DATABASE IF NOT EXISTS cdc_demo;
USE cdc_demo;

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    price DECIMAL(10,2) NOT NULL,
    order_status VARCHAR(20) NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 初始数据
INSERT INTO products (product_name, category, price, stock) VALUES
('Laptop Pro 15', 'Electronics', 8999.00, 100),
('Wireless Mouse', 'Electronics', 129.00, 500),
('Mechanical Keyboard', 'Electronics', 599.00, 200),
('USB-C Hub', 'Accessories', 199.00, 300),
('4K Monitor', 'Electronics', 2999.00, 150);
SQL

echo "MySQL 表创建完成"
echo ""

echo "=== [2/3] 启动数据生成循环（每 2 秒一条 INSERT + 偶尔 UPDATE/DELETE） ==="

CUSTOMERS=("Alice" "Bob" "Charlie" "Diana" "Eric" "Fiona" "George" "Helen" "Ivan" "Julia")
CATEGORIES=("Electronics" "Accessories" "Books" "Clothing" "Food")

while true; do
    CUST=${CUSTOMERS[$((RANDOM % 10))]}
    PROD_NAME="Product_$((RANDOM % 1000))"
    QTY=$((RANDOM % 10 + 1))
    PRICE_CENTS=$((RANDOM % 100000))
    PRICE=$(printf "%d.%02d" $((PRICE_CENTS / 100)) $((PRICE_CENTS % 100)))

    ACTION=$((RANDOM % 10))
    if [ $ACTION -lt 7 ]; then
        # INSERT
        $MYSQL_CMD cdc_demo -e "
            INSERT INTO orders (customer_name, product_name, quantity, price)
            VALUES ('${CUST}', '${PROD_NAME}', ${QTY}, ${PRICE});
        " 2>/dev/null
        echo "[$(date '+%H:%M:%S')] INSERT order: ${CUST} -> ${PROD_NAME} x${QTY} @${PRICE}"
    elif [ $ACTION -lt 9 ]; then
        # UPDATE - 更新某个订单状态
        MAX_ID=$($MYSQL_CMD -N -e "SELECT COALESCE(MAX(order_id),1) FROM cdc_demo.orders;" 2>/dev/null)
        if [ "$MAX_ID" -gt 0 ] 2>/dev/null; then
            TARGET=$((RANDOM % MAX_ID + 1))
            $MYSQL_CMD cdc_demo -e "
                UPDATE orders SET order_status='SHIPPED', updated_at=CURRENT_TIMESTAMP WHERE order_id=${TARGET};
            " 2>/dev/null
            echo "[$(date '+%H:%M:%S')] UPDATE order ${TARGET} -> SHIPPED"
        fi
    else
        # DELETE - 删除某个订单
        MAX_ID=$($MYSQL_CMD -N -e "SELECT COALESCE(MAX(order_id),1) FROM cdc_demo.orders;" 2>/dev/null)
        if [ "$MAX_ID" -gt 0 ] 2>/dev/null; then
            TARGET=$((RANDOM % MAX_ID + 1))
            $MYSQL_CMD cdc_demo -e "
                DELETE FROM orders WHERE order_id=${TARGET};
            " 2>/dev/null
            echo "[$(date '+%H:%M:%S')] DELETE order ${TARGET}"
        fi
    fi

    sleep 2
done
