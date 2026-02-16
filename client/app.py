from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from kafka import KafkaConsumer
import json
import asyncio
from threading import Thread
import time

app = FastAPI(title="Moex Stream Analyzer")
templates = Jinja2Templates(directory="client")
connections = set()
event_loop = None

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.add(websocket)
    print(f"\n✅ WebSocket подключен. Всего клиентов: {len(connections)}")
    
    try:
        if hasattr(app.state, 'latest_data'):
            for ticker, data in app.state.latest_data.items():
                await websocket.send_json(data)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections.remove(websocket)
        print(f"🔌 WebSocket отключен. Осталось клиентов: {len(connections)}")

def kafka_reader():
    """Читатель с исправленной асинхронностью и парсингом"""
    print("\n" + "="*70)
    print("📡 Запуск читателя Kafka (исправленная версия)")
    print("="*70)
    
    # Уникальная группа для гарантии чтения с начала
    group_id = f"web-client-{int(time.time())}"
    
    try:
        consumer = KafkaConsumer(
            'moex_indicators',
            bootstrap_servers='localhost:9093',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"✅ Подключено к Kafka (группа: {group_id})")
        print("⏳ Чтение сообщений из топика 'moex_indicators'...\n")
        
        if not hasattr(app.state, 'latest_data'):
            app.state.latest_data = {}
        
        message_count = 0
        
        for msg in consumer:
            try:
                # === КРИТИЧЕСКИ ВАЖНЫЙ ПАРСИНГ ВЛОЖЕННОЙ СТРУКТУРЫ ===
                data = msg.value
                
                # Извлечение данных из вложенных полей
                ohlcv = data.get("ohlcv", {})
                indicators = data.get("indicators", {})
                sma = indicators.get("sma", {})
                rsi_data = indicators.get("rsi", {})
                signals = data.get("signals", {})
                
                # Формирование плоской структуры для фронтенда
                formatted = {
                    "ticker": data.get("ticker", "UNKNOWN"),
                    "price": ohlcv.get("close", 0),
                    "sma_5": sma.get("sma_5", 0),
                    "sma_20": sma.get("sma_20", 0),
                    "rsi": rsi_data.get("value", 50),
                    "volatility": indicators.get("volatility", 0),
                    "volume": ohlcv.get("volume", 0),
                    "signals": {
                        "summary": signals.get("summary", [])
                    }
                }
                
                # Сохранение в кэш
                app.state.latest_data[formatted["ticker"]] = formatted
                
                # Рассылка клиентам (с правильной асинхронностью)
                if connections and event_loop:
                    asyncio.run_coroutine_threadsafe(
                        broadcast(formatted),
                        event_loop
                    )
                
                message_count += 1
                if message_count == 1:
                    print("🎉 ПЕРВЫЕ ДАННЫЕ ПОЛУЧЕНЫ И ОТОБРАЖЕНЫ В БРАУЗЕРЕ!")
                    print(f"   Тикер: {formatted['ticker']} | Цена: {formatted['price']:.2f} ₽ | RSI: {formatted['rsi']:.1f}")
                elif message_count % 20 == 0:
                    print(f"📊 Обработано {message_count} сообщений. Последний тикер: {formatted['ticker']} @ {formatted['price']:.2f} ₽")
                
            except Exception as e:
                print(f"❌ Ошибка обработки сообщения: {e}")
                # Не прерываем цикл — продолжаем читать
        
        consumer.close()
        
    except Exception as e:
        print(f"\n❌ Критическая ошибка Kafka: {e}")
        import traceback
        traceback.print_exc()

async def broadcast(data):
    """Рассылка с обработкой ошибок"""
    disconnected = set()
    for ws in connections:
        try:
            await ws.send_json(data)
        except:
            disconnected.add(ws)
    
    for ws in disconnected:
        connections.discard(ws)

@app.on_event("startup")
async def startup_event():
    global event_loop
    event_loop = asyncio.get_running_loop()  # Сохраняем цикл основного потока
    
    print("\n" + "="*70)
    print("🚀 ВЕБ-СЕРВЕР ЗАПУЩЕН")
    print("   • Адрес: http://localhost:8000")
    print("   • WebSocket: ws://localhost:8000/ws")
    print("   • Данные: читаются из топика 'moex_indicators'")
    print("="*70)
    
    Thread(target=kafka_reader, daemon=True).start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")