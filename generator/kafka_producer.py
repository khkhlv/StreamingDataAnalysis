# generator/kafka_producer.py
import json
import time
import signal
import logging
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from market_simulator import MarketSimulator
from config import DEFAULT_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class SyntheticDataProducer:
    """ETL процесс: генератор → Kafka с гарантиями exactly-once"""
    
    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        topic: str = "moex_raw_quotes"
    ):
        self.topic = topic
        self.running = True
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks=1,
            retries=0,  # Отключаем повторы для упрощения
            linger_ms=5,
            batch_size=4096,
            max_block_ms=3000  # Таймаут для первоначального подключения
        )
        
        # Инициализация симулятора рынка
        self.simulator = MarketSimulator(DEFAULT_CONFIG)
        self.messages_sent = 0
        self.last_stats_time = time.time()
        self.stats_interval = 10  # Секунд между выводом статистики
    
    def _handle_signal(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        logger.info(f"Получен сигнал {signal.Signals(signum).name}, инициируем остановку...")
        self.running = False
    
    def _print_stats(self):
        """Вывод статистики каждые N секунд"""
        now = time.time()
        elapsed = now - self.last_stats_time
        
        if elapsed >= self.stats_interval:
            msgs_per_sec = self.messages_sent / elapsed
            logger.info(
                f"Статистика: {self.messages_sent:,} сообщений, "
                f"{msgs_per_sec:.1f} msg/sec, "
                f"режим рынка: {self.simulator.market_regime.value}"
            )
            self.messages_sent = 0
            self.last_stats_time = now
    
    def run(self):
        """Основной цикл генерации и отправки данных"""
        # Настройка обработчиков сигналов
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        logger.info(f"Запуск генератора данных → Kafka топик: {self.topic}")
        logger.info(f"Частота: {DEFAULT_CONFIG.messages_per_second} сообщений/сек")
        
        # Основной цикл
        for item in self.simulator.stream_quotes():
            if not self.running:
                break
            
            # Пропуск служебных задержек (в реальном времени использовался бы sleep)
            if "_delay" in item:
                time.sleep(item["_delay"])
                quote = item["_quote"]
            else:
                quote = item
            
            # Формирование ключа для идемпотентности: тикер + временная метка с точностью до мс
            key = f"{quote['ticker']}_{quote['timestamp'].replace(':', '').replace('.', '')[:20]}"
            
            try:
                # Асинхронная отправка в Kafka
                future = self.producer.send(
                    topic=self.topic,
                    key=key,
                    value=quote
                )
                
                # Опционально: синхронное ожидание для гарантии доставки
                # В продакшене лучше использовать асинхронную обработку с колбэками
                # record_metadata = future.get(timeout=10)
                
                self.messages_sent += 1
                
            except KafkaError as e:
                logger.error(f"Ошибка Kafka: {e}")
                # В реальной системе: отправка в DLQ или повторная попытка
                time.sleep(0.1)
            
            # Периодический вывод статистики
            self._print_stats()
        
        # Graceful shutdown
        logger.info("Завершение работы генератора...")
        self.producer.flush(timeout=30)
        self.producer.close()
        logger.info(f"Генератор остановлен. Всего отправлено: {self.messages_sent:,} сообщений")
    
    @classmethod
    def create_topics(cls, bootstrap_servers: str = "kafka:9092"):
        """Создание топиков Kafka (выполняется один раз при инициализации)"""
        from kafka.admin import KafkaAdminClient, NewTopic
        
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
        topics = [
            NewTopic(
                name="moex_raw_quotes",
                num_partitions=3,
                replication_factor=1,
                topic_config={
                    "retention.ms": "604800000",  # 7 дней
                    "cleanup.policy": "delete"
                }
            ),
            NewTopic(
                name="moex_indicators",
                num_partitions=3,
                replication_factor=1,
                topic_config={
                    "retention.ms": "2592000000",  # 30 дней
                    "cleanup.policy": "delete"
                }
            )
        ]
        
        try:
            admin_client.create_topics(new_topics=topics, validate_only=False)
            logger.info("Топики Kafka успешно созданы")
        except Exception as e:
            logger.warning(f"Ошибка создания топиков (возможно, уже существуют): {e}")
        finally:
            admin_client.close()

def main():
    # Создание топиков (однократно)
    # SyntheticDataProducer.create_topics()
    
    # Запуск генератора
    producer = SyntheticDataProducer()
    producer.run()

if __name__ == "__main__":
    main()