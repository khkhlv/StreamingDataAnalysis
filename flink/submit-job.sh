#!/bin/bash
# submit-job.sh - Отправка PyFlink job в кластер Flink

set -e
/docker-entrypoint.sh jobmanager &

# Проверка готовности Flink (ожидание до 60 секунд)
echo "⏳ Проверка готовности кластера Flink..."
for i in {1..12}; do
    if curl -s http://localhost:8081/overview > /dev/null 2>&1; then
        echo "✅ Flink кластер готов"
        break
    fi
    if [ $i -eq 12 ]; then
        echo "❌ Ошибка: Flink кластер не отвечает после 60 секунд"
        echo "Проверьте логи: docker-compose logs -f jobmanager"
        exit 1
    fi
    sleep 5
    echo -n "."
done
echo ""

SCRIPT_DIR="/scripts"
JOB_FILE="$SCRIPT_DIR/indicators.py"
CONTAINER_NAME="flink-jobmanager"
KAFKA_CONTAINER="kafka"
JOB_PATH="/scripts/indicators.py" 

echo "🚀 Подготовка к отправке PyFlink job..."
echo "📁 Job файл: $JOB_FILE"
echo ""

# Проверка существования файла job
if [ ! -f "$JOB_FILE" ]; then
    echo "❌ Ошибка: Файл job не найден: $JOB_FILE"
    exit 1
fi

# Создание топиков (если не существуют)
echo "🔧 Создание топиков Kafka (если не существуют)..."
kafka-topics --bootstrap-server kafka:9092 --create --topic moex_raw_quotes --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || echo 'ℹ moex_raw_quotes уже существует'
kafka-topics --bootstrap-server kafka:9092 --create --topic moex_indicators --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || echo 'ℹ moex_indicators уже существует'
        

# Отправка job
echo "⚡ Отправка PyFlink job в кластер..."
echo "Выполняется команда: flink run -py $JOB_PATH"

# Используем docker exec вместо docker-compose exec для лучшей совместимости
flink run -py /scripts/indicators.py
echo "   • Веб-интерфейс Flink: http://localhost:8081"
echo "   • Статус заданий:      http://localhost:8081/#/job/list"