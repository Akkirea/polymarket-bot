"""
SIGNAL/ZERO Phase 1 — Binance WebSocket Client

Connects to Binance's public trade stream for BTC/USDT.
Maintains a rolling window of recent prices for momentum calculation.
No API key needed — this is a public endpoint.
"""

import asyncio
import json
import time
from collections import deque
from typing import Optional

import websockets

from . import config


class BinancePriceFeed:
    """Real-time BTC price via Binance WebSocket."""

    _KLINE_URL = "https://api.binance.com/api/v3/klines"

    def __init__(self):
        self.current_price: Optional[float] = None
        self.last_update: Optional[float] = None
        self.source: str = "binance"
        self.change_24h: Optional[float] = None  # not provided by trade stream
        self._price_history: deque = deque(maxlen=600)  # ~10 min at 1/sec
        self._kline_cache: dict = {}  # start_ts → open price
        self._running = False
        self._ws = None

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    def get_momentum(self, window_seconds: int = None) -> Optional[float]:
        """
        Calculate short-term price momentum as a probability estimate.

        Returns a value between 0 and 1 representing estimated probability
        that BTC will be higher at end of current window vs start.

        Uses exponentially weighted price change direction over the window.
        """
        if window_seconds is None:
            window_seconds = config.MOMENTUM_WINDOW

        if len(self._price_history) < 5:
            return None

        now = time.time()
        cutoff = now - window_seconds

        # Get prices within our window
        window_prices = [
            (ts, price)
            for ts, price in self._price_history
            if ts >= cutoff
        ]

        if len(window_prices) < 3:
            return None

        # Calculate weighted directional moves
        up_weight = 0.0
        down_weight = 0.0
        decay = config.MOMENTUM_DECAY

        for i in range(1, len(window_prices)):
            weight = decay ** (len(window_prices) - 1 - i)  # recent = higher weight
            change = window_prices[i][1] - window_prices[i - 1][1]

            if change > 0:
                up_weight += weight
            elif change < 0:
                down_weight += weight
            else:
                # flat — split evenly
                up_weight += weight * 0.5
                down_weight += weight * 0.5

        total = up_weight + down_weight
        if total == 0:
            return 0.5

        # Raw directional probability
        raw_prob = up_weight / total

        # Pull toward 0.5 slightly — we shouldn't be too confident
        # on short-term momentum alone
        confidence_damper = 0.7
        adjusted = 0.5 + (raw_prob - 0.5) * confidence_damper

        return max(0.01, min(0.99, adjusted))

    def get_price_at_window_start(self, window_start_ts: float) -> Optional[float]:
        """Find the price closest to a given timestamp."""
        if not self._price_history:
            return None

        closest = None
        closest_diff = float("inf")

        for ts, price in self._price_history:
            diff = abs(ts - window_start_ts)
            if diff < closest_diff:
                closest_diff = diff
                closest = price

        # Only use if within 5 seconds of target
        if closest_diff > 5.0:
            return None
        return closest

    async def get_kline_open(self, start_ts: float) -> Optional[float]:
        """
        Return the Binance 5m kline open price at start_ts (Unix seconds).

        Binance klines align with clock boundaries (:00, :05, :10 ...) so the
        candle whose openTime == start_ts gives the BTC price at the exact
        moment the Polymarket window opened — same reference Chainlink uses.
        Result is cached per start_ts so only one HTTP call is made per market.
        """
        if start_ts in self._kline_cache:
            return self._kline_cache[start_ts]

        import aiohttp as _aio
        start_ms = int(start_ts * 1000)
        try:
            async with _aio.ClientSession(timeout=_aio.ClientTimeout(total=5)) as session:
                async with session.get(
                    self._KLINE_URL,
                    params={
                        "symbol": "BTCUSDT",
                        "interval": "5m",
                        "startTime": start_ms,
                        "limit": 1,
                    },
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    if not data or len(data[0]) < 2:
                        return None
                    # Verify the returned candle actually starts at our timestamp
                    candle_open_ms = int(data[0][0])
                    if abs(candle_open_ms - start_ms) > 1000:
                        return None
                    price = float(data[0][1])  # index 1 = open price
                    self._kline_cache[start_ts] = price
                    return price
        except Exception:
            return None

    # Port 9443 is blocked on some cloud providers; 443 is the fallback.
    _WS_URLS = [
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
        "wss://stream.binance.com:443/ws/btcusdt@trade",
    ]
    _REST_URL = "https://api.binance.com/api/v3/ticker/price"

    async def connect(self):
        """
        Connect to Binance and stream prices.
        Falls back to REST polling when both WebSocket URLs are blocked
        (common on Railway and other cloud providers).
        """
        self._running = True
        url_index = 0
        ws_fail_streak = 0

        while self._running:
            url = self._WS_URLS[url_index % len(self._WS_URLS)]
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    ws_fail_streak = 0
                    url_index = 0
                    print(f"[binance] WebSocket connected: {url}", flush=True)
                    while self._running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            price = float(data["p"])
                            now = time.time()
                            self.current_price = price
                            self.last_update = now
                            self.source = "binance"
                            self._price_history.append((now, price))
                        except asyncio.TimeoutError:
                            continue
                        except (json.JSONDecodeError, KeyError):
                            continue

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                self._ws = None
                ws_fail_streak += 1
                print(f"[binance] WS failed ({type(exc).__name__}), streak={ws_fail_streak}", flush=True)

            except Exception as exc:
                self._ws = None
                ws_fail_streak += 1
                print(f"[binance] WS error ({type(exc).__name__}: {exc}), streak={ws_fail_streak}", flush=True)

            if not self._running:
                break

            # After both WS URLs fail, REST-poll for 60s before retrying WS.
            if ws_fail_streak >= 2:
                await self._rest_poll(60)
                ws_fail_streak = 0
                url_index = 0
            else:
                url_index += 1
                await asyncio.sleep(3)

    async def _rest_poll(self, duration: float = 60.0) -> None:
        """Poll Binance REST API for BTC price when WebSocket is unavailable."""
        import aiohttp as _aio
        print(f"[binance] REST fallback active for {duration:.0f}s", flush=True)
        deadline = time.time() + duration
        async with _aio.ClientSession(timeout=_aio.ClientTimeout(total=5)) as session:
            while self._running and time.time() < deadline:
                try:
                    async with session.get(
                        self._REST_URL, params={"symbol": "BTCUSDT"}
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            price = float(data["price"])
                            now = time.time()
                            self.current_price = price
                            self.last_update = now
                            self.source = "binance-rest"
                            self._price_history.append((now, price))
                except Exception:
                    pass
                await asyncio.sleep(2)
        self.source = "binance"

    def stop(self):
        self._running = False
