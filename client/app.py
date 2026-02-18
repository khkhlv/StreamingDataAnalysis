#!/usr/bin/env python3
"""
Веб-сервер с кластерным анализом (сохранена рабочая структура данных)
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from kafka import KafkaConsumer
import json
import asyncio
from threading import Thread, Lock
import time
from collections import defaultdict, deque
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

# ИСПРАВЛЕНО: папка шаблонов "client" вместо "templates" (как в вашем рабочем коде)
app = FastAPI(title="Moex Stream Analyzer")
templates = Jinja2Templates(directory=".")  # ← КРИТИЧЕСКИ ВАЖНО: "client" а не "templates"
connections = {"main": set(), "clusters": set()}
event_loop = None
lock = Lock()

# Кэш для кластеризации (отдельно от основного кэша)
cluster_cache = defaultdict(lambda: {
    "prices": deque(maxlen=60),
    "volatilities": deque(maxlen=60),
    "volumes": deque(maxlen=60),
    "rsi": deque(maxlen=60),
    "sma_ratio": deque(maxlen=60)
})
cluster_results = {"clusters": {}, "last_update": 0}

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# НОВАЯ СТРАНИЦА: кластерный анализ
@app.get("/clusters", response_class=HTMLResponse)
async def clusters_page(request: Request):
    return templates.TemplateResponse("cluster.html", {"request": request})

@app.websocket("/ws")
async def websocket_main(websocket: WebSocket):
    """Основной веб-сокет (как в вашем рабочем коде)"""
    await websocket.accept()
    connections["main"].add(websocket)
    print(f"\n✅ WebSocket подключен. Всего клиентов: {len(connections['main'])}")
    
    try:
        if hasattr(app.state, 'latest_data'):
            for ticker, data in app.state.latest_data.items():
                await websocket.send_json(data)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections["main"].discard(websocket)
        print(f"🔌 WebSocket отключен. Осталось клиентов: {len(connections['main'])}")

# НОВЫЙ ВЕБ-СОКЕТ: для кластеров
@app.websocket("/ws/clusters")
async def websocket_clusters(websocket: WebSocket):
    await websocket.accept()
    connections["clusters"].add(websocket)
    print(f"✅ Кластерный WebSocket подключен. Клиентов: {len(connections['clusters'])}")
    
    try:
        if cluster_results["clusters"]:
            await websocket.send_json({
                "type": "clusters",
                "data": cluster_results["clusters"],
                "timestamp": cluster_results["last_update"]
            })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections["clusters"].discard(websocket)
        print(f"🔌 Кластерный WebSocket отключен. Осталось: {len(connections['clusters'])}")

def kafka_reader():
    """Читатель с ВАШЕЙ рабочей структурой данных (вложенная структура)"""
    print("\n" + "="*70)
    print("📡 Запуск читателя Kafka (ВАША РАБОЧАЯ СТРУКТУРА)")
    print("="*70)
    
    group_id = f"web-client-main-{int(time.time())}"
    
    try:
        consumer = KafkaConsumer(
            'moex_indicators',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"✅ Подключено к Kafka (группа: {group_id})")
        print("⏳ Чтение сообщений из топика 'moex_indicators'...\n")
        
        if not hasattr(app.state, 'latest_data'):
            app.state.latest_data = {}
        
        # Для кластеризации: отдельный потребитель в другом потоке
        Thread(target=cluster_kafka_reader, daemon=True).start()
        
        message_count = 0
        last_cluster_time = time.time()
        
        for msg in consumer:
            try:
                data = msg.value
                
                ohlcv = data.get("ohlcv", {})
                indicators = data.get("indicators", {})
                sma = indicators.get("sma", {})
                rsi_data = indicators.get("rsi", {})
                signals = data.get("signals", {})
                
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
                
                # Накопление данных для кластеризации (потокобезопасно)
                with lock:
                    ticker = formatted["ticker"]
                    cluster_cache[ticker]["prices"].append(formatted["price"])
                    cluster_cache[ticker]["volatilities"].append(formatted["volatility"])
                    cluster_cache[ticker]["volumes"].append(formatted["volume"])
                    cluster_cache[ticker]["rsi"].append(formatted["rsi"])
                    cluster_cache[ticker]["sma_ratio"].append(
                        formatted["sma_5"] / formatted["sma_20"] if formatted["sma_20"] > 0 else 1.0
                    )
                
                # Рассылка основным клиентам
                if connections["main"] and event_loop:
                    asyncio.run_coroutine_threadsafe(
                        broadcast_main(formatted),
                        event_loop
                    )
                trend_signal = None
                for signal in signals.get("summary", []):
                    if "Восходящий тренд" in signal:
                        trend_signal = "up"
                        break
                    elif "Нисходящий тренд" in signal:
                        trend_signal = "down"
                        break
                
                if trend_signal and len(connections.get("trend", [])) > 0:
                    with lock:
                        # Сохраняем цену в историю
                        trend_history[ticker].append(formatted["price"])
                        
                        # Рассчитываем прогноз при достаточном количестве данных
                        if len(trend_history[ticker]) >= 10:
                            history_list = list(trend_history[ticker])
                            display_history = history_list[-20:] if len(history_list) >= 20 else history_list
                            
                            # Линейная регрессия для прогноза
                            x = np.arange(len(history_list))
                            y = np.array(history_list)
                            coeffs = np.polyfit(x, y, 1)
                            poly = np.poly1d(coeffs)
                            
                            # Прогноз на 5 секунд вперед
                            forecast_x = np.arange(len(history_list), len(history_list) + 5)
                            forecast_y = poly(forecast_x).tolist()
                            
                            # Отправка данных клиенту
                            trend_data = {
                                "type": "trend_update",
                                "ticker": ticker,
                                "current_price": formatted["price"],
                                "trend_type": trend_signal,
                                "history": display_history,
                                "forecast": forecast_y,
                                "timestamp": data.get("timestamp", "")
                            }
                            
                            if event_loop:
                                asyncio.run_coroutine_threadsafe(
                                    broadcast_trend(trend_data),
                                    event_loop
                                )
                
                # Запуск кластеризации каждые 30 сек
                now = time.time()
                if now - last_cluster_time >= 30 and len(cluster_cache) >= 3:
                    last_cluster_time = now
                    Thread(target=perform_clustering, daemon=True).start()
                
                message_count += 1
                if message_count == 1:
                    print("🎉 ПЕРВЫЕ ДАННЫЕ ПОЛУЧЕНЫ (как в вашем рабочем коде)!")
                    print(f"   Тикер: {formatted['ticker']} | Цена: {formatted['price']:.2f} ₽ | RSI: {formatted['rsi']:.1f}")
                elif message_count % 20 == 0:
                    print(f"📊 Обработано {message_count} сообщений. Последний: {formatted['ticker']} @ {formatted['price']:.2f} ₽")
                
            except Exception as e:
                print(f"❌ Ошибка обработки: {e}")
        
        consumer.close()
        
    except Exception as e:
        print(f"\n❌ Критическая ошибка Kafka: {e}")
        import traceback
        traceback.print_exc()

def cluster_kafka_reader():
    """Отдельный потребитель ТОЛЬКО для кластеризации (не влияет на основной поток)"""
    # Этот поток не нужен - кластеризация использует данные из основного потока
    # Оставлен для будущего расширения
    pass

async def broadcast_main(data):
    """Рассылка основным клиентам"""
    disconnected = set()
    for ws in connections["main"]:
        try:
            await ws.send_json(data)
        except:
            disconnected.add(ws)
    
    for ws in disconnected:
        connections["main"].discard(ws)

async def broadcast_clusters(data):
    """Рассылка кластерным клиентам"""
    disconnected = set()
    for ws in connections["clusters"]:
        try:
            await ws.send_json(data)
        except:
            disconnected.add(ws)
    
    for ws in disconnected:
        connections["clusters"].discard(ws)

def perform_clustering():
    """Кластеризация на основе накопленных данных"""
    try:
        with lock:
            tickers = list(cluster_cache.keys())
            if len(tickers) < 3:
                return
            
            features = []
            valid_tickers = []
            
            for ticker in tickers:
                t = cluster_cache[ticker]
                if len(t["prices"]) < 10:  # Минимум 10 точек
                    continue
                
                features.append([
                    np.mean(t["prices"]),
                    np.std(t["prices"]),
                    np.mean(t["volatilities"]),
                    np.mean(t["volumes"]),
                    np.mean(t["rsi"]),
                    np.mean(t["sma_ratio"])
                ])
                valid_tickers.append(ticker)
            
            if len(features) < 2:
                return
            
            features = np.array(features)
            
            # Нормализация
            scaler = StandardScaler()
            features_scaled = scaler.fit_transform(features)
            
            # Определение числа кластеров
            n_clusters = min(max(2, len(valid_tickers) // 3), 5)
            
            # Кластеризация
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(features_scaled)
            
            # Снижение размерности для визуализации
            pca = PCA(n_components=2, random_state=42)
            features_2d = pca.fit_transform(features_scaled)
            
            # Формирование результатов
            cluster_results["clusters"] = {
                "tickers": valid_tickers,
                "labels": cluster_labels.tolist(),
                "features_2d": features_2d.tolist(),
                "centroids_2d": pca.transform(kmeans.cluster_centers_).tolist(),
                "n_clusters": n_clusters
            }
            cluster_results["last_update"] = time.time()
            
            # Отладочный вывод
            print(f"\n📊 КЛАСТЕРИЗАЦИЯ ВЫПОЛНЕНА ({len(valid_tickers)} тикеров, {n_clusters} кластеров)")
            for i in range(n_clusters):
                members = [valid_tickers[j] for j, label in enumerate(cluster_labels) if label == i]
                print(f"   Кластер {i}: {', '.join(members[:5])}{'...' if len(members) > 5 else ''} ({len(members)} тикеров)")
            print()
            
            # Рассылка результатов
            if event_loop and connections["clusters"]:
                payload = {
                    "type": "clusters",
                    "data": cluster_results["clusters"],
                    "timestamp": cluster_results["last_update"]
                }
                asyncio.run_coroutine_threadsafe(broadcast_clusters(payload), event_loop)
                
    except Exception as e:
        print(f"❌ Ошибка кластеризации: {e}")
        import traceback
        traceback.print_exc()

# =============== ТРЕНДОВЫЙ ПРОГНОЗ ===============
trend_history = defaultdict(lambda: deque(maxlen=60))  # История цен для прогноза

@app.websocket("/ws/trend-forecast")
async def websocket_trend(websocket: WebSocket):
    """Веб-сокет для страницы прогноза тренда"""
    await websocket.accept()
    connections["trend"] = connections.get("trend", set())
    connections["trend"].add(websocket)
    print(f"✅ Трендовый WebSocket подключен. Клиентов: {len(connections['trend'])}")
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections["trend"].discard(websocket)
        print(f"🔌 Трендовый WebSocket отключен. Осталось: {len(connections['trend'])}")

@app.get("/trend-forecast", response_class=HTMLResponse)
async def trend_forecast_page(request: Request):
    """Страница с прогнозом тренда"""
    return templates.TemplateResponse("trend_forecast.html", {"request": request})

async def broadcast_trend(data):
    """Рассылка данных о тренде"""
    disconnected = set()
    for ws in connections.get("trend", []):
        try:
            await ws.send_json(data)
        except:
            disconnected.add(ws)
    
    for ws in disconnected:
        connections["trend"].discard(ws)

@app.on_event("startup")
async def startup_event():
    global event_loop
    event_loop = asyncio.get_running_loop()
    
    print("\n" + "="*70)
    print("🚀 ВЕБ-СЕРВЕР ЗАПУЩЕН (сохранена ваша рабочая структура)")
    print("="*70)
    print("   • Основной дашборд: http://localhost:8000")
    print("   • Кластерный анализ: http://localhost:8000/clusters")
    print("   • Анализ тренда: http://localhost:8000/trend-forecast")
    print("   • Структура данных: ВЛОЖЕННАЯ (как в вашем рабочем коде)")
    print("   • auto_offset_reset: 'latest' (только новые данные)")
    print("   • Кластеризация: каждые 30 секунд")
    print("="*70 + "\n")
    
    Thread(target=kafka_reader, daemon=True).start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")