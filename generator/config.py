from dataclasses import dataclass
from typing import Dict, List
from enum import Enum

class MarketRegime(Enum):
    """Режимы рыночной активности"""
    CALM = "calm"           # Спокойный рынок (низкая волатильность)
    NORMAL = "normal"       # Нормальная торговля
    VOLATILE = "volatile"   # Высокая волатильность
    TREND_UP = "trend_up"   # Восходящий тренд
    TREND_DOWN = "trend_down"  # Нисходящий тренд
    PANIC = "panic"         # Паника/резкие движения

@dataclass
class TickerConfig:
    """Конфигурация для отдельного тикера"""
    symbol: str
    base_price: float       # Базовая цена в рублях
    volatility: float       # Базовая волатильность (% от цены)
    avg_volume: int         # Средний объем сделки
    market_cap_category: str  # "large", "mid", "small"
    correlation_group: str  # Группа для корреляции (энергетика, финансы и т.д.)

@dataclass
class GeneratorConfig:
    """Глобальная конфигурация генератора"""
    tickers: List[TickerConfig]
    messages_per_second: int = 50  # Частота генерации сообщений
    regime_change_interval: int = 300  # Смена режима каждые N секунд
    market_hours: Dict[str, str] = None  # Режим работы рынка
    
    def __post_init__(self):
        if self.market_hours is None:
            self.market_hours = {
                "open": "10:00",
                "close": "18:40",
                "pre_market": "09:50",
                "after_market": "19:00"
            }

# Преднастроенные тикеры (голубые фишки Мосбиржи)
DEFAULT_TICKERS = [
    TickerConfig("SBER", 280.50, 0.8, 50000, "large", "finance"),
    TickerConfig("GAZP", 175.30, 1.2, 80000, "large", "energy"),
    TickerConfig("LKOH", 7200.00, 0.6, 5000, "large", "energy"),
    TickerConfig("MTSS", 320.80, 1.0, 30000, "large", "telecom"),
    TickerConfig("TATN", 650.40, 1.5, 20000, "mid", "energy"),
    TickerConfig("VTBR", 0.055, 2.0, 5000000, "large", "finance"),
    TickerConfig("ROSN", 480.20, 1.3, 40000, "large", "energy"),
    TickerConfig("NVTK", 2450.00, 1.8, 8000, "mid", "metals"),
]

DEFAULT_CONFIG = GeneratorConfig(
    tickers=DEFAULT_TICKERS,
    messages_per_second=50,
    regime_change_interval=300
)