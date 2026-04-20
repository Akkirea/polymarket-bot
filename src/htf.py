"""
HTF (Higher-Timeframe) BTC Polymarket analysis.

Log-normal edge model — no ML.
P(S_T >= K) = N(d2)
d2 = (ln(S/K) - σ²·T/2) / (σ·√T)
"""

import asyncio
import json
import math
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

GAMMA           = "https://gamma-api.polymarket.com"
BINANCE_PRICE   = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
BINANCE_KLINES  = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=72"
COINGECKO_PRICE = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
BYBIT_PRICE     = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT"

# ── Probability model ──────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Abramowitz & Stegun approximation, error < 7.5e-8."""
    a = [0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429]
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    poly = t * (a[0] + t * (a[1] + t * (a[2] + t * (a[3] + t * a[4]))))
    cdf = 1.0 - poly * math.exp(-x * x / 2.0) / math.sqrt(2 * math.pi)
    return 0.5 * (1.0 + sign * (2 * cdf - 1))


def btc_up_prob(spot: float, strike: float, sigma_ann: float, seconds_left: float) -> dict:
    """
    Compute log-normal P(S_T >= K).

    Returns dict with probUp, probDown, d2, sigma, T.
    """
    T = max(seconds_left / (365.25 * 24 * 3600), 1e-9)
    sigma = max(sigma_ann, 0.01)
    d2 = (math.log(spot / strike) - 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    prob_up = min(max(_norm_cdf(d2), 0.001), 0.999)
    return {
        "prob_up":   round(prob_up, 4),
        "prob_down": round(min(max(1 - prob_up, 0.001), 0.999), 4),
        "d2":        round(d2, 4),
        "sigma":     round(sigma, 4),
        "T":         T,
    }


def estimate_vol(prices: list[float], interval_secs: int = 3600) -> float:
    """Annualised realised vol from list of close prices."""
    if len(prices) < 2:
        return 0.80
    log_rets = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0 and prices[i] > 0
    ]
    if len(log_rets) < 2:
        return 0.80
    mean = sum(log_rets) / len(log_rets)
    var  = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
    periods_per_year = (365.25 * 24 * 3600) / interval_secs
    return round(math.sqrt(var * periods_per_year), 4)


def half_kelly(est_prob: float, entry_price: float) -> float:
    """Half-Kelly stake as fraction of bankroll (capped at 10%)."""
    if entry_price <= 0:
        return 0.0
    b = 1.0 / entry_price - 1.0
    q = 1 - est_prob
    kelly = (est_prob * b - q) / b if b > 0 else 0.0
    return round(min(max(kelly / 2, 0.0), 0.10), 4)


# ── Polymarket fetchers ────────────────────────────────────────────────────────

def _classify_timeframe(question: str) -> str:
    q = question.lower()
    if any(w in q for w in ["week", "7-day", "7 day"]):
        return "1w"
    if any(w in q for w in ["24-hour", "24 hour", "1-day", "1 day", "daily", "end of day", "eod"]):
        return "1d"
    if "4-hour" in q or "4 hour" in q or "4h" in q:
        return "4h"
    if "2-hour" in q or "2 hour" in q or "2h" in q:
        return "2h"
    if "1-hour" in q or "1 hour" in q or "1h" in q or "hourly" in q:
        return "1h"
    return "other"


def _parse_json_field(field, fallback):
    if isinstance(field, list):
        return field
    if isinstance(field, str):
        try:
            return json.loads(field)
        except Exception:
            return fallback
    return fallback


def _parse_market(raw: dict) -> Optional[dict]:
    try:
        question = raw.get("question", "")
        outcomes = _parse_json_field(raw.get("outcomes"), ["Up", "Down"])
        prices   = _parse_json_field(raw.get("outcomePrices"), ["0.5", "0.5"])
        tokens   = _parse_json_field(raw.get("clobTokenIds"), ["", ""])

        prob_up = prob_down = 0.5
        for i, o in enumerate(outcomes):
            p = float(prices[i]) if i < len(prices) else 0.5
            if str(o).lower() == "up":
                prob_up = p
            elif str(o).lower() == "down":
                prob_down = p

        end_date   = raw.get("endDate", "")
        end_ms     = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp() * 1000 if end_date else 0
        secs_left  = max(0, (end_ms / 1000) - time.time())

        events = raw.get("events") or []
        meta   = (events[0].get("eventMetadata") or {}) if events else {}
        price_to_beat = float(meta["priceToBeat"]) if meta.get("priceToBeat") is not None else None

        # For "above $X" markets, extract the strike from the question if not in metadata
        if price_to_beat is None:
            import re
            # Match $70,000 / $70k / $1m / $1.5M patterns
            m_price = re.search(r'\$([0-9,]+(?:\.[0-9]+)?)([kKmM]?)', question)
            if m_price:
                raw_num   = m_price.group(1).replace(',', '')
                suffix    = m_price.group(2).lower()
                try:
                    val = float(raw_num)
                    if suffix == 'k':   val *= 1_000
                    elif suffix == 'm': val *= 1_000_000
                    price_to_beat = val
                except Exception:
                    pass

        timeframe = _classify_timeframe(question)

        return {
            "id":            raw.get("id", ""),
            "slug":          raw.get("slug", ""),
            "question":      question,
            "timeframe":     timeframe,
            "prob_up":       round(prob_up, 4),
            "prob_down":     round(prob_down, 4),
            "volume":        float(raw.get("volume") or 0),
            "liquidity":     float(raw.get("liquidityNum") or 0),
            "end_date":      end_date,
            "seconds_left":  round(secs_left),
            "price_to_beat": price_to_beat,
        }
    except Exception:
        return None


async def fetch_htf_markets(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch active Polymarket BTC Up/Down markets with timeframe > 5 min."""
    params = {"active": "true", "closed": "false", "limit": "50",
              "order": "endDate", "ascending": "false"}
    try:
        async with session.get(f"{GAMMA}/markets", params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception as e:
        print(f"[htf] fetch_htf_markets error: {e}")
        return []

    markets = []
    for raw in data:
        q = (raw.get("question") or "").lower()
        is_btc     = "bitcoin" in q or "btc" in q
        is_binary  = (
            "up or down" in q or "up/down" in q   # 5-min style
            or "above" in q or "below" in q         # price-level markets
            or "higher" in q or "lower" in q
            or "exceed" in q or "reach" in q
            or "hit $" in q or "hit " in q          # "hit $1m" style
        )
        if not (is_btc and is_binary):
            continue

        m = _parse_market(raw)
        if not m:
            continue
        # Skip nearly-closed markets
        if m["seconds_left"] < 60:
            continue
        markets.append(m)

    return markets


async def fetch_btc_price(session: aiohttp.ClientSession) -> float:
    """
    BTC/USD spot — tries Bybit → CoinGecko → Binance via aiohttp.
    """
    errors = []
    for url, extractor in [
        (BYBIT_PRICE,     lambda d: float(d["result"]["list"][0]["lastPrice"])),
        (COINGECKO_PRICE, lambda d: float(d["bitcoin"]["usd"])),
        (BINANCE_PRICE,   lambda d: float(d["price"])),
    ]:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json(content_type=None)
            return extractor(data)
        except Exception as e:
            errors.append(f"{url.split('/')[2]}: {e}")

    raise RuntimeError(f"All BTC price sources failed: {'; '.join(errors)}")


async def fetch_recent_prices(session: aiohttp.ClientSession) -> list[float]:
    """72 hours of 1h BTC close prices for vol estimation."""
    try:
        async with session.get(BINANCE_KLINES, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
        return [float(k[4]) for k in data]
    except Exception:
        return []


async def fetch_resolution(session: aiohttp.ClientSession, slug: str) -> tuple:
    """
    Returns (winner, final_price, price_to_beat) for a closed market.
    winner is 'up' or 'down', or None if not yet resolved.
    """
    params = {"slug": slug, "closed": "true"}
    try:
        async with session.get(f"{GAMMA}/markets", params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None, None, None
            data = await resp.json()
    except Exception:
        return None, None, None

    if not data:
        return None, None, None

    m = data[0]
    events = m.get("events") or []
    meta   = (events[0].get("eventMetadata") or {}) if events else {}

    try:
        fp  = float(meta["finalPrice"])   if meta.get("finalPrice")   is not None else None
        ptb = float(meta["priceToBeat"])  if meta.get("priceToBeat")  is not None else None
        if fp is not None and ptb is not None:
            return ("up" if fp >= ptb else "down"), fp, ptb
    except Exception:
        pass

    # Fallback: outcomePrices settled to 0/1
    try:
        outcomes = _parse_json_field(m.get("outcomes"), [])
        prices   = _parse_json_field(m.get("outcomePrices"), [])
        for i, p in enumerate(prices):
            if float(p) >= 0.99 and i < len(outcomes):
                return str(outcomes[i]).lower(), None, None
    except Exception:
        pass

    return None, None, None
