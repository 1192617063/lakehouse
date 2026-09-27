#!/bin/bash
bash /usr/local/bin/init_fe.sh &
FE_PID=$!

echo "Waiting for Doris FE to start on port 9030..."
for i in $(seq 1 90); do
    if mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" &>/dev/null; then
        echo "Doris FE is ready (attempt $i)."
        break
    fi
    sleep 2
done

mysql -h 127.0.0.1 -P 9030 -u root -e "SET PASSWORD FOR 'root' = PASSWORD('');" 2>/dev/null
echo "[OK] Root password set to empty (BE requires passwordless root access)"

mysql -h 127.0.0.1 -P 9030 -u root -e "
CREATE USER IF NOT EXISTS 'lakehouse'@'%' IDENTIFIED BY 'lakehouse123';
GRANT ALL PRIVILEGES ON *.* TO 'lakehouse'@'%';
FLUSH PRIVILEGES;
" 2>/dev/null
echo "[OK] User 'lakehouse' created with password 'lakehouse123'"

wait $FE_PID
