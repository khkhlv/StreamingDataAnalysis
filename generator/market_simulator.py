import random
import math
from datetime import datetime, timedelta
from typing import Dict, List, Generator
import numpy as np
from config import TickerConfig, MarketRegime, GeneratorConfig

class MarketSimulator:
    """Симулятор рыночного поведения с реалистичной динамикой"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.current_prices = {t.symbol: t.base_price for t in config.tickers}
        self.current_volumes = {t.symbol: t.avg_volume for t in config.tickers}
        self.market_regime = MarketRegime.NORMAL
        self.regime_start_time = datetime.utcnow()
        self.regime_duration = 0
        self.correlation_matrix = self._build_correlation_matrix()
        self._schedule_next_regime_change()
        
        # История цен для расчета технических индикаторов
        self.price_history: Dict[str, List[float]] = {
            t.symbol: [t.base_price] * 50 for t in config.tickers
        }
    
    def _build_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """Построение матрицы корреляции между тикерами по секторам"""
        correlation = {}
        sectors = {}
        
        # Группировка по секторам
        for ticker in self.config.tickers:
            sectors.setdefault(ticker.correlation_group, []).append(ticker.symbol)
        
        # Заполнение матрицы
        for ticker1 in self.config.tickers:
            correlation[ticker1.symbol] = {}
            for ticker2 in self.config.tickers:
                if ticker1.symbol == ticker2.symbol:
                    correlation[ticker1.symbol][ticker2.symbol] = 1.0
                elif ticker1.correlation_group == ticker2.correlation_group:
                    # Высокая корреляция внутри сектора (0.6-0.9)
                    correlation[ticker1.symbol][ticker2.symbol] = random.uniform(0.6, 0.9)
                else:
                    # Низкая/отрицательная корреляция между секторами
                    correlation[ticker1.symbol][ticker2.symbol] = random.uniform(-0.3, 0.3)
        
        return correlation
    
    def _schedule_next_regime_change(self):
        """Планирование следующей смены рыночного режима"""
        # Длительность режима: 2-10 минут
        self.regime_duration = random.randint(
            self.config.regime_change_interval // 2,
            self.config.regime_change_interval * 2
        )
        self.regime_start_time = datetime.utcnow()
        
        # Выбор нового режима с учетом вероятностей
        regimes = [
            (MarketRegime.CALM, 0.15),
            (MarketRegime.NORMAL, 0.40),
            (MarketRegime.VOLATILE, 0.20),
            (MarketRegime.TREND_UP, 0.10),
            (MarketRegime.TREND_DOWN, 0.10),
            (MarketRegime.PANIC, 0.05),
        ]
        
        rand = random.random()
        cumulative = 0.0
        for regime, prob in regimes:
            cumulative += prob
            if rand <= cumulative:
                self.market_regime = regime
                break
        
        print(f"[MARKET] Новый режим: {self.market_regime.value} (длительность: {self.regime_duration} сек)")
    
    def _get_regime_multiplier(self) -> Dict[str, float]:
        """Получение мультипликаторов для текущего рыночного режима"""
        multipliers = {
            "price_volatility": 1.0,
            "volume_multiplier": 1.0,
            "trend_strength": 0.0,
            "spread_multiplier": 1.0
        }
        
        if self.market_regime == MarketRegime.CALM:
            multipliers["price_volatility"] = 0.3
            multipliers["volume_multiplier"] = 0.5
            multipliers["spread_multiplier"] = 0.7
            
        elif self.market_regime == MarketRegime.NORMAL:
            multipliers["price_volatility"] = 1.0
            multipliers["volume_multiplier"] = 1.0
            multipliers["spread_multiplier"] = 1.0
            
        elif self.market_regime == MarketRegime.VOLATILE:
            multipliers["price_volatility"] = 2.5
            multipliers["volume_multiplier"] = 1.8
            multipliers["spread_multiplier"] = 1.5
            
        elif self.market_regime == MarketRegime.TREND_UP:
            multipliers["price_volatility"] = 1.5
            multipliers["volume_multiplier"] = 1.5
            multipliers["trend_strength"] = 0.05  # +0.05% за шаг
            multipliers["spread_multiplier"] = 0.9
            
        elif self.market_regime == MarketRegime.TREND_DOWN:
            multipliers["price_volatility"] = 1.5
            multipliers["volume_multiplier"] = 1.5
            multipliers["trend_strength"] = -0.05  # -0.05% за шаг
            multipliers["spread_multiplier"] = 1.2
            
        elif self.market_regime == MarketRegime.PANIC:
            multipliers["price_volatility"] = 5.0
            multipliers["volume_multiplier"] = 3.0
            multipliers["trend_strength"] = -0.2  # Резкое падение
            multipliers["spread_multiplier"] = 3.0
        
        return multipliers
    
    def _apply_market_impact(self, symbol: str, base_change: float) -> float:
        """Применение рыночного воздействия с учетом корреляции"""
        # Основное изменение цены
        change = base_change
        
        # Добавление корреляции с другими активами (упрощенно)
        # В реальной системе здесь был бы многомерный гауссовский процесс
        if random.random() < 0.3:  # 30% шанс корреляционного движения
            # Выбор случайного "лидера" из той же группы
            ticker_config = next(t for t in self.config.tickers if t.symbol == symbol)
            correlated = [
                t for t in self.config.tickers 
                if t.correlation_group == ticker_config.correlation_group and t.symbol != symbol
            ]
            if correlated:
                leader = random.choice(correlated)
                correlation = self.correlation_matrix[symbol][leader.symbol]
                leader_change = (self.current_prices[leader.symbol] - self.price_history[leader.symbol][-2]) \
                              / self.price_history[leader.symbol][-2] if len(self.price_history[leader.symbol]) > 1 else 0
                change += leader_change * correlation * 0.7  # 70% влияния корреляции
        
        return change
    
    def _generate_quote(self, ticker: TickerConfig) -> Dict:
        """Генерация одной котировки для тикера"""
        # Проверка смены рыночного режима
        if (datetime.utcnow() - self.regime_start_time).total_seconds() > self.regime_duration:
            self._schedule_next_regime_change()
        
        # Мультипликаторы текущего режима
        regime_mult = self._get_regime_multiplier()
        
        # Базовое случайное изменение цены (нормальное распределение)
        base_volatility = ticker.volatility / 100  # конвертация % в долю
        random_change = np.random.normal(0, base_volatility * regime_mult["price_volatility"])
        
        # Добавление тренда
        trend_component = regime_mult["trend_strength"] / 100
        
        # Итоговое изменение с рыночным воздействием
        total_change = self._apply_market_impact(ticker.symbol, random_change + trend_component)
        
        # Новая цена
        new_price = self.current_prices[ticker.symbol] * (1 + total_change)
        new_price = max(new_price, ticker.base_price * 0.5)  # Ограничение падения до 50% от базы
        
        # Расчет объема (логнормальное распределение для реалистичности)
        volume_multiplier = regime_mult["volume_multiplier"] * random.uniform(0.5, 2.0)
        volume = int(np.random.lognormal(
            mean=math.log(ticker.avg_volume),
            sigma=0.7
        ) * volume_multiplier)
        
        # Расчет стакана (bid/ask)
        spread_pct = (0.05 + random.random() * 0.1) / 100 * regime_mult["spread_multiplier"]  # 0.05%-0.15% спред
        bid = new_price * (1 - spread_pct / 2)
        ask = new_price * (1 + spread_pct / 2)
        bid_depth = int(volume * random.uniform(0.8, 1.5))
        ask_depth = int(volume * random.uniform(0.8, 1.5))
        
        # Расчет изменения относительно предыдущей цены
        prev_price = self.current_prices[ticker.symbol]
        change = new_price - prev_price
        change_percent = (change / prev_price) * 100 if prev_price > 0 else 0
        
        # Обновление состояния
        self.current_prices[ticker.symbol] = new_price
        self.current_volumes[ticker.symbol] = volume
        self.price_history[ticker.symbol].append(new_price)
        if len(self.price_history[ticker.symbol]) > 100:
            self.price_history[ticker.symbol].pop(0)
        
        # Формирование котировки
        quote = {
            "ticker": ticker.symbol,
            "board": "TQBR",
            "price": round(new_price, 2),
            "volume": volume,
            "change": round(change, 2),
            "change_percent": round(change_percent, 2),
            "bid": round(bid, 2),
            "offer": round(ask, 2),
            "bid_depth": bid_depth,
            "offer_depth": ask_depth,
            "timestamp": datetime.utcnow().isoformat(),
            "event_time": datetime.utcnow().isoformat(),
            "market_regime": self.market_regime.value,
            "trades_count": random.randint(1, 20)  # Количество сделок в этом сообщении
        }
        
        return quote
    
    def stream_quotes(self) -> Generator[Dict, None, None]:
        """Генератор бесконечного потока котировок"""
        print(f"[GENERATOR] Запуск симулятора рынка с {len(self.config.tickers)} тикерами")
        
        # Циклическая генерация котировок
        ticker_cycle = self.config.tickers * 3  # Увеличение частоты для активных тикеров
        random.shuffle(ticker_cycle)
        cycle_index = 0
        
        while True:
            # Выбор тикера по циклу
            ticker = ticker_cycle[cycle_index % len(ticker_cycle)]
            cycle_index += 1
            
            # Генерация котировки
            quote = self._generate_quote(ticker)
            yield quote
            
            # Динамическая задержка для достижения целевой частоты
            # Более частые котировки для крупных тикеров
            base_delay = 1.0 / self.config.messages_per_second
            cap_multiplier = 0.7 if ticker.market_cap_category == "large" else 1.0
            delay = base_delay * cap_multiplier * random.uniform(0.8, 1.2)
            
            # Имитация пауз в торговле (ночное время)
            if random.random() < 0.01:  # 1% шанс длинной паузы
                delay *= random.uniform(5, 20)
            
            # Ограничение задержки
            delay = max(0.001, min(delay, 1.0))
            
            # Имитация времени через управление скоростью генерации
            # (в реальном времени использовался бы asyncio.sleep(delay))
            yield {"_delay": delay, "_quote": quote}