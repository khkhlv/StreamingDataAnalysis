#!/bin/bash
# submit-job.sh - Отправка PyFlink job в кластер Flink
# Использование: ./submit-job.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_FILE="$SCRIPT_DIR/indicators.py"
CONTAINER_NAME="flink-jobmanager"
KAFKA_CONTAINER="kafka"
JOB_PATH="/opt/flink/indicators.py"  # Корректный путь в контейнере


echo "🚀 Подготовка к отправке PyFlink job..."
echo "📁 Job файл: $JOB_FILE"
echo ""

# Проверка существования файла job
if [ ! -f "$JOB_FILE" ]; then
    echo "❌ Ошибка: Файл job не найден: $JOB_FILE"
    echo "Проверьте структуру проекта:"
    echo "  moex-streaming-demo/"
    echo "  └── flink/"
    echo "      └── indicators.py"
    exit 1
fi

# Проверка запущенных контейнеров
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "❌ Ошибка: Контейнер $CONTAINER_NAME не запущен"
    echo "Запустите: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "$KAFKA_CONTAINER"; then
    echo "❌ Ошибка: Контейнер $KAFKA_CONTAINER не запущен"
    echo "Запустите: docker-compose up -d"
    exit 1
fi

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

# Проверка готовности Kafka
echo "⏳ Проверка готовности Kafka..."
for i in {1..6}; do
    if docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo "✅ Kafka готова"
        break
    fi
    if [ $i -eq 6 ]; then
        echo "❌ Ошибка: Kafka не готова после 30 секунд"
        echo "Проверьте логи: docker-compose logs -f kafka"
        exit 1
    fi
    sleep 5
    echo -n "."
done
echo ""

# Создание топиков (если не существуют)
echo "🔧 Создание топиков Kafka (если не существуют)..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic moex_raw_quotes \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists > /dev/null 2>&1 && echo "✅ Топик moex_raw_quotes готов"

docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic moex_indicators \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists > /dev/null 2>&1 && echo "✅ Топик moex_indicators готов"

# Копирование файла в контейнер
echo "📤 Копирование job в контейнер $CONTAINER_NAME..."
docker cp "$JOB_FILE" "$CONTAINER_NAME:$JOB_PATH"

# Отправка job
echo "⚡ Отправка PyFlink job в кластер..."
echo "Выполняется команда: flink run -py $JOB_PATH"
echo ""

# Используем docker exec вместо docker-compose exec для лучшей совместимости
START_TIME=$(date +%s)
if docker exec "$CONTAINER_NAME" flink run -py "$JOB_PATH" 2>&1; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo ""
    echo "✅ Job успешно отправлен за $DURATION сек!"
    echo ""
    echo "📊 Мониторинг:"
    echo "   • Веб-интерфейс Flink: http://localhost:8081"
    echo "   • Статус заданий:      http://localhost:8081/#/job/list"
    echo ""
    exit 0
else
    EXIT_CODE=$?
    echo ""
    echo "❌ Ошибка отправки job (код: $EXIT_CODE)"
    exit $EXIT_CODE
fi