# flink-job/indicators.py
"""
PyFlink job для расширенного потокового анализа котировок Московской биржи
Лабораторная работа №3: Потоковая обработка с расчетом полного набора индикаторов

Реализованные функции (без состояния между окнами и без внешних зависимостей):
✅ Технические индикаторы: SMA, EMA, RSI, MACD, Bollinger Bands, VWAP
✅ Агрегированные метрики: объемы, волатильность
✅ Сигналы трейдинга: тренды внутри окна, зоны перекупленности/перепроданности
✅ Обнаружение аномалий: резкие движения цены, необычные объемы
✅ Статистика ликвидности: спред, глубина рынка
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
import json
import math
from datetime import datetime
from typing import Iterable, List, Tuple

class AdvancedIndicators(ProcessWindowFunction):
    """
    Расширенный расчет технических индикаторов без сохранения состояния между окнами
    Все сигналы рассчитываются на основе данных внутри текущего окна
    """
    
    def calculate_ema(self, prices: List[float], period: int) -> float:
        """Расчет экспоненциальной скользящей средней (без сохранения состояния)"""
        if len(prices) < period:
            return prices[-1] if prices else 0.0
        
        # Начинаем с SMA для первых значений
        ema = sum(prices[:period]) / period
        multiplier = 2 / (period + 1)
        
        # Рассчитываем EMA для оставшихся значений
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Tuple[float, str]:
        """Расчет RSI с определением состояния рынка"""
        if len(prices) < period + 1:
            return 50.0, "neutral"
        
        # Расчет изменений цены
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        recent_deltas = deltas[-period:]
        
        # Средние значения роста и падения
        avg_gain = sum(d for d in recent_deltas if d > 0) / period
        avg_loss = abs(sum(d for d in recent_deltas if d < 0)) / period
        
        if avg_loss == 0:
            return 100.0, "overbought"
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # Определение состояния
        if rsi > 70:
            state = "overbought"
        elif rsi < 30:
            state = "oversold"
        else:
            state = "neutral"
        
        return rsi, state
    
    def calculate_macd(self, prices: List[float]) -> Tuple[float, float, float]:
        """Расчет MACD (12, 26, 9) без сохранения состояния"""
        ema_12 = self.calculate_ema(prices, 12)
        ema_26 = self.calculate_ema(prices, 26)
        macd_line = ema_12 - ema_26
        
        # Для сигнальной линии используем упрощенный подход:
        # если данных достаточно, рассчитываем EMA(9) от MACD значений
        if len(prices) >= 35:  # 26 + 9
            # Генерируем историю MACD для расчёта сигнальной линии
            macd_history = []
            for i in range(26, len(prices)):
                short_ema = self.calculate_ema(prices[:i+1], 12)
                long_ema = self.calculate_ema(prices[:i+1], 26)
                macd_history.append(short_ema - long_ema)
            
            if len(macd_history) >= 9:
                signal_line = self.calculate_ema(macd_history[-9:], 9)
            else:
                signal_line = macd_line
        else:
            signal_line = macd_line
        
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, std_dev: int = 2) -> Tuple[float, float, float]:
        """Расчет полос Боллинджера"""
        if len(prices) < period:
            sma = sum(prices) / len(prices) if prices else 0.0
            return sma, sma, sma
        
        recent_prices = prices[-period:]
        sma = sum(recent_prices) / period
        std = math.sqrt(sum((p - sma) ** 2 for p in recent_prices) / period)
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        return upper_band, sma, lower_band
    
    def calculate_vwap(self, prices: List[float], volumes: List[int]) -> float:
        """Расчет VWAP (Volume Weighted Average Price)"""
        if not prices or not volumes or sum(volumes) == 0:
            return prices[-1] if prices else 0.0
        
        weighted_sum = sum(p * v for p, v in zip(prices, volumes))
        total_volume = sum(volumes)
        return weighted_sum / total_volume if total_volume > 0 else prices[-1]
    
    def detect_anomalies(self, prices: List[float], volumes: List[int]) -> dict:
        """Обнаружение аномалий в данных внутри окна"""
        anomalies = {
            "price_spike": False,
            "price_spike_percent": 0.0,
            "unusual_volume": False,
            "volume_deviation": 0.0,
            "volatility_spike": False,
            "trend_strength": "neutral"  # "strong_up", "strong_down", "neutral"
        }
        
        if len(prices) < 3:
            return anomalies
        
        # 1. Резкое движение цены (>2% за окно)
        price_change = ((prices[-1] - prices[0]) / prices[0]) * 100 if prices[0] != 0 else 0
        if abs(price_change) > 2.0:
            anomalies["price_spike"] = True
            anomalies["price_spike_percent"] = round(price_change, 2)
        
        # 2. Определение силы тренда внутри окна
        if price_change > 1.5:
            anomalies["trend_strength"] = "strong_up"
        elif price_change < -1.5:
            anomalies["trend_strength"] = "strong_down"
        
        # 3. Необычные объемы (последний объем > 2σ от среднего)
        if len(volumes) >= 5:
            avg_volume = sum(volumes) / len(volumes)
            if avg_volume > 0:
                std_volume = math.sqrt(sum((v - avg_volume) ** 2 for v in volumes) / len(volumes))
                if std_volume > 0 and volumes[-1] > avg_volume + (2 * std_volume):
                    anomalies["unusual_volume"] = True
                    anomalies["volume_deviation"] = round((volumes[-1] - avg_volume) / std_volume, 2)
        
        # 4. Всплеск волатильности (сравнение первой и второй половины окна)
        if len(prices) >= 10:
            mid = len(prices) // 2
            first_half_vol = math.sqrt(sum((p - sum(prices[:mid])/mid) ** 2 for p in prices[:mid]) / mid)
            second_half_vol = math.sqrt(sum((p - sum(prices[mid:])/len(prices[mid:])) ** 2 for p in prices[mid:]) / len(prices[mid:]))
            
            if first_half_vol > 0 and second_half_vol > first_half_vol * 1.8:
                anomalies["volatility_spike"] = True
        
        return anomalies
    
    def generate_trading_signals(self, 
                                prices: List[float],
                                sma_5: float, sma_20: float,
                                rsi: float,
                                bb_upper: float, bb_lower: float, close: float) -> dict:
        """Генерация торговых сигналов на основе данных внутри окна"""
        signals = {
            "trend_direction": "neutral",   # "up", "down", "neutral"
            "trend_strength": "weak",       # "strong", "moderate", "weak"
            "rsi_signal": "neutral",        # "overbought", "oversold", "neutral"
            "bb_signal": "neutral",         # "upper_band", "lower_band", "neutral"
            "summary": []
        }
        
        # 1. Направление тренда (сравнение начала и конца окна)
        if len(prices) >= 5:
            start_avg = sum(prices[:2]) / 2
            end_avg = sum(prices[-2:]) / 2
            trend_change = ((end_avg - start_avg) / start_avg) * 100
            
            if trend_change > 0.8:
                signals["trend_direction"] = "up"
                signals["trend_strength"] = "strong" if trend_change > 1.5 else "moderate"
                signals["summary"].append(f"📈 Восходящий тренд (+{trend_change:.1f}%)")
            elif trend_change < -0.8:
                signals["trend_direction"] = "down"
                signals["trend_strength"] = "strong" if trend_change < -1.5 else "moderate"
                signals["summary"].append(f"📉 Нисходящий тренд ({trend_change:.1f}%)")
        
        # 2. Сигналы по RSI
        if rsi > 70:
            signals["rsi_signal"] = "overbought"
            signals["summary"].append("⚠️ RSI > 70: зона перекупленности")
        elif rsi < 30:
            signals["rsi_signal"] = "oversold"
            signals["summary"].append("⚠️ RSI < 30: зона перепроданности")
        
        # 3. Сигналы по полосам Боллинджера
        if close >= bb_upper:
            signals["bb_signal"] = "upper_band"
            signals["summary"].append("🔝 Цена у верхней полосы Боллинджера")
        elif close <= bb_lower:
            signals["bb_signal"] = "lower_band"
            signals["summary"].append("🔻 Цена у нижней полосы Боллинджера")
        
        # 4. Пересечение средних (внутри окна)
        # Если 5-периодная средняя в конце окна выше 20-периодной — бычий сигнал
        if sma_5 > sma_20:
            if signals["trend_direction"] == "up":
                signals["summary"].append("✅ Подтверждение тренда: SMA(5) > SMA(20)")
        else:
            if signals["trend_direction"] == "down":
                signals["summary"].append("✅ Подтверждение тренда: SMA(5) < SMA(20)")
        
        return signals
    
    def process(self, key: str, context, elements: Iterable[str]) -> Iterable[str]:
        # Парсинг всех сообщений в окне
        quotes = []
        for element in elements:
            try:
                if element.strip():
                    quote = json.loads(element)
                    if quote.get("price", 0) > 0:
                        quotes.append(quote)
            except:
                continue  # Пропускаем некорректные сообщения
        
        if not quotes:
            return
        
        # Извлечение временных рядов
        prices = [q["price"] for q in quotes]
        volumes = [q.get("volume", 0) for q in quotes]
        bids = [q.get("bid", 0) for q in quotes if q.get("bid", 0) > 0]
        asks = [q.get("offer", 0) for q in quotes if q.get("offer", 0) > 0]
        bid_depths = [q.get("bid_depth", 0) for q in quotes]
        ask_depths = [q.get("offer_depth", 0) for q in quotes]
        
        # OHLCV
        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]
        total_volume = sum(volumes)
        
        # === Расчет базовых индикаторов ===
        
        # SMA
        sma_5 = sum(prices[-5:]) / min(5, len(prices))
        sma_20 = sum(prices[-20:]) / min(20, len(prices)) if len(prices) >= 20 else sma_5
        
        # EMA
        ema_12 = self.calculate_ema(prices, 12)
        ema_26 = self.calculate_ema(prices, 26)
        
        # RSI
        rsi, rsi_state = self.calculate_rsi(prices, 14)
        
        # MACD
        macd_line, signal_line, histogram = self.calculate_macd(prices)
        
        # Bollinger Bands
        bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(prices, 20, 2)
        
        # VWAP
        vwap = self.calculate_vwap(prices, volumes)
        
        # Волатильность (стандартное отклонение)
        mean_price = sum(prices) / len(prices)
        volatility = math.sqrt(sum((p - mean_price) ** 2 for p in prices) / len(prices)) if len(prices) > 1 else 0
        
        # === Метрики ликвидности ===
        avg_bid = sum(bids) / len(bids) if bids else close_price * 0.9995
        avg_ask = sum(asks) / len(asks) if asks else close_price * 1.0005
        avg_spread = avg_ask - avg_bid
        avg_spread_percent = (avg_spread / close_price * 100) if close_price > 0 else 0
        avg_bid_depth = sum(bid_depths) / len(bid_depths) if bid_depths else 0
        avg_ask_depth = sum(ask_depths) / len(ask_depths) if ask_depths else 0
        market_depth = avg_bid_depth + avg_ask_depth
        
        # === Обнаружение аномалий ===
        anomalies = self.detect_anomalies(prices, volumes)
        
        # === Торговые сигналы ===
        signals = self.generate_trading_signals(
            prices, sma_5, sma_20, rsi, bb_upper, bb_lower, close_price
        )
        
        # === Формирование результата ===
        result = {
            "ticker": key,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "timestamp": datetime.utcnow().isoformat(),
            
            # OHLCV
            "ohlcv": {
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": total_volume
            },
            
            # Технические индикаторы
            "indicators": {
                "sma": {
                    "sma_5": round(sma_5, 2),
                    "sma_20": round(sma_20, 2)
                },
                "ema": {
                    "ema_12": round(ema_12, 2),
                    "ema_26": round(ema_26, 2)
                },
                "rsi": {
                    "value": round(rsi, 1),
                    "state": rsi_state
                },
                "macd": {
                    "line": round(macd_line, 4),
                    "signal": round(signal_line, 4),
                    "histogram": round(histogram, 4)
                },
                "bollinger_bands": {
                    "upper": round(bb_upper, 2),
                    "middle": round(bb_middle, 2),
                    "lower": round(bb_lower, 2),
                    "width": round((bb_upper - bb_lower) / bb_middle * 100, 2)  # в процентах
                },
                "vwap": round(vwap, 2),
                "volatility": round(volatility, 4)
            },
            
            # Метрики ликвидности
            "liquidity": {
                "spread": round(avg_spread, 4),
                "spread_percent": round(avg_spread_percent, 2),
                "bid_depth": round(avg_bid_depth, 0),
                "ask_depth": round(avg_ask_depth, 0),
                "market_depth": round(market_depth, 0)
            },
            
            # Аномалии
            "anomalies": anomalies,
            
            # Торговые сигналы
            "signals": signals,
            
            # Статистика окна
            "window_stats": {
                "data_points": len(prices),
                "price_change_percent": round(((close_price - open_price) / open_price) * 100, 2),
                "volume": total_volume,
                "market_regime": quotes[-1].get("market_regime", "normal")
            }
        }
        
        yield json.dumps(result)

def run_flink_job():
    # Создание среды выполнения
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Включение checkpointing для отказоустойчивости
    env.enable_checkpointing(10000)  # 10 секунд
    
    # === ИСТОЧНИК ИЗ KAFKA (совместимый синтаксис для 1.18 без внешних зависимостей) ===
    kafka_properties = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-consumer-group',
        'auto.offset.reset': 'latest'
    }
    
    consumer = FlinkKafkaConsumer(
        topics='moex_raw_quotes',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_properties
    )
    
    stream = env.add_source(consumer)
    
    # Фильтрация и ключирование по тикеру
    keyed_stream = stream \
        .filter(lambda x: len(x.strip()) > 0 and json.loads(x).get("price", 0) > 0) \
        .key_by(lambda x: json.loads(x)["ticker"])
    
    # Оконная обработка (10-секундные окна)
    windowed_stream = keyed_stream.window(
        TumblingProcessingTimeWindows.of(Time.seconds(10))
    ).process(AdvancedIndicators(), Types.STRING())
    
    # === ПРИЕМНИК В KAFKA (совместимый синтаксис) ===
    producer = FlinkKafkaProducer(
        topic='moex_indicators',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    
    windowed_stream.add_sink(producer)
    
    # Запуск задания
    print("=" * 70)
    print("🚀 ЗАПУСК РАСШИРЕННОГО PYFLINK JOB (полностью совместимый с 1.18)")
    print("=" * 70)
    print("📊 Параметры окон: 10 секунд")
    print("📈 Индикаторы: SMA(5/20), EMA(12/26), RSI(14), MACD, Bollinger Bands, VWAP")
    print("💡 Сигналы: тренды внутри окна, зоны перекупленности/перепроданности")
    print("⚠️  Аномалии: резкие движения цены (>2%), необычные объемы")
    print("💧 Ликвидность: спред, глубина рынка")
    print("📡 Поток: moex_raw_quotes → Flink → moex_indicators")
    print("=" * 70)
    print()
    
    env.execute("Moex Advanced Streaming Analysis")

if __name__ == "__main__":
    run_flink_job()