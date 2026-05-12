"""Market catalog helpers for short-horizon Polymarket dashboards."""

import time


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


def catalog() -> list[dict]:
    return FIVE_MINUTE_MARKETS + FIFTEEN_MINUTE_MARKETS + PLANNED_MARKETS
