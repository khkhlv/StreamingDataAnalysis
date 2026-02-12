import requests
import json
import time
from confluent_kafka import Producer

# Конфигурация Kafka
config = {
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest'
}

producer = Producer(config)

def send_to_kafka(topic, data):
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()
    print(f"Sent to Kafka: {data}")

# # Функция получения котировок индекса IMOEX с MOEX API
# def get_moex_index():
#     url = "https://iss.moex.com/iss/engines/stock/markets/index/securities/IMOEX.json"
    
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
        
#         # Разбираем JSON, достаем последнюю цену индекса
#         marketdata = data["marketdata"]["data"]
#         columns = data["marketdata"]["columns"]

#         if marketdata:
#             last_price_index = columns.index("LAST")
#             last_price = marketdata[0][last_price_index]

#             timestamp_index = columns.index("UPDATETIME")
#             timestamp = marketdata[0][timestamp_index]

#             payload = {
#                 "ticker": "IMOEX",
#                 "price": last_price,
#                 "timestamp": timestamp
#             }

#             send_to_kafka(KAFKA_TOPIC, payload)

#     except requests.exceptions.RequestException as e:
#         print(f"Error fetching data: {e}")

# # Основной цикл
# if __name__ == "__main__":
#     while True:
#         get_moex_index()
#         time.sleep(10)  # Запрос каждые 10 секунд
