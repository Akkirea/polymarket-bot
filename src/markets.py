"""Market catalog helpers for Polymarket dashboards."""

import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


FIVE_MINUTE_MARKETS = [
    {
        "symbol": "BTC",
        "pair": "BTC/USD",
        "slug_prefix": "btc-updown-5m",
        "binance_symbol": "BTCUSDT",
        "category": "crypto",
        "timeframe": "5m",
        "trade_enabled": True,
        "status": "live",
    },
    {
        "symbol": "ETH",
        "pair": "ETH/USD",
        "slug_prefix": "eth-updown-5m",
        "binance_symbol": "ETHUSDT",
        "category": "crypto",
        "timeframe": "5m",
        "trade_enabled": False,
        "status": "watch",
    },
    {
        "symbol": "XRP",
        "pair": "XRP/USD",
        "slug_prefix": "xrp-updown-5m",
        "binance_symbol": "XRPUSDT",
        "category": "crypto",
        "timeframe": "5m",
        "trade_enabled": False,
        "status": "watch",
    },
]


FIFTEEN_MINUTE_MARKETS = [
    {
        "symbol": "BTC15",
        "pair": "BTC/USD",
        "slug_prefix": "btc-updown-15m",
        "binance_symbol": "BTCUSDT",
        "category": "crypto",
        "timeframe": "15m",
        "trade_enabled": False,
        "status": "paper-shadow",
    },
]

DAILY_MARKETS = [
    {
        "symbol": "BTCD",
        "pair": "BTC/USD",
        "slug_prefix": "bitcoin-up-or-down-on-",
        "binance_symbol": "BTCUSDT",
        "category": "crypto",
        "timeframe": "daily",
        "trade_enabled": False,
        "status": "shadow-watch",
    },
]


PLANNED_MARKETS = [
    {
        "symbol": "SOL",
        "pair": "SOL/USD",
        "slug_prefix": "sol-updown-5m",
        "binance_symbol": "SOLUSDT",
        "category": "crypto",
        "timeframe": "5m",
        "trade_enabled": False,
        "status": "pending",
    },
    {
        "symbol": "USDCAD",
        "pair": "USD/CAD",
        "slug_prefix": "usd-cad-up-or-down",
        "binance_symbol": None,
        "category": "fx",
        "timeframe": "daily",
        "trade_enabled": False,
        "status": "daily-only",
    },
]


def current_five_minute_start() -> int:
    now = int(time.time())
    return (now // 300) * 300


def current_five_minute_slugs(market: dict) -> list[str]:
    start = current_five_minute_start()
    return [
        f"{market['slug_prefix']}-{start}",
        f"{market['slug_prefix']}-{start + 300}",
    ]


def current_interval_slugs(market: dict) -> list[str]:
    interval = 900 if market.get("timeframe") == "15m" else 300
    now = int(time.time())
    start = (now // interval) * interval
    return [
        f"{market['slug_prefix']}-{start}",
        f"{market['slug_prefix']}-{start + interval}",
    ]


def current_daily_slugs(market: dict) -> list[str]:
    """Return likely current/next daily Polymarket slugs for date-named markets."""
    now_et = datetime.now(ZoneInfo("America/New_York"))
    slugs = []
    for offset in (0, 1):
        market_date = now_et + timedelta(days=offset)
        month = market_date.strftime("%B").lower()
        slugs.append(f"{market['slug_prefix']}{month}-{market_date.day}")
    return slugs


def catalog() -> list[dict]:
    return FIVE_MINUTE_MARKETS + FIFTEEN_MINUTE_MARKETS + DAILY_MARKETS + PLANNED_MARKETS
