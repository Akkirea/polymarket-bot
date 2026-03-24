import asyncio
import time
from collections import deque
from typing import Optional
import aiohttp

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_PARAMS = {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"}
POLL_INTERVAL = 3

class PriceFeed:
    def __init__(self):
        self.current_price = None
        self.last_update = None
        self.source = "connecting"
        self.change_24h = None
        self._price_history = deque(maxlen=600)
        self._running = False
        self._session = None
        self._errors = 0

    @property
    def connected(self):
        return self.current_price is not None and self._running

    def _record_price(self, price):
        now = time.time()
        self.current_price = price
        self.last_update = now
        self._price_history.append((now, price))

    def get_momentum(self, window_seconds=60):
        if len(self._price_history) < 5:
            return None
        now = time.time()
        cutoff = now - window_seconds
        window_prices = [(ts, p) for ts, p in self._price_history if ts >= cutoff]
        if len(window_prices) < 3:
            return None
        up_weight = 0.0
        down_weight = 0.0
        decay = 0.95
        for i in range(1, len(window_prices)):
            weight = decay ** (len(window_prices) - 1 - i)
            change = window_prices[i][1] - window_prices[i - 1][1]
            if change > 0:
                up_weight += weight
            elif change < 0:
                down_weight += weight
            else:
                up_weight += weight * 0.5
                down_weight += weight * 0.5
        total = up_weight + down_weight
        if total == 0:
            return 0.5
        raw_prob = up_weight / total
        adjusted = 0.5 + (raw_prob - 0.5) * 0.7
        return max(0.01, min(0.99, adjusted))

    def get_price_at_window_start(self, window_start_ts):
        if not self._price_history:
            return None
        closest = None
        closest_diff = float("inf")
        for ts, price in self._price_history:
            diff = abs(ts - window_start_ts)
            if diff < closest_diff:
                closest_diff = diff
                closest = price
        if closest_diff > 5.0:
            return None
        return closest

    async def connect(self):
        self._running = True
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        try:
            while self._running:
                try:
                    async with self._session.get(COINGECKO_URL, params=COINGECKO_PARAMS) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            btc = data.get("bitcoin", {})
                            price = btc.get("usd")
                            if price and price > 0:
                                self._record_price(float(price))
                                self.source = "coingecko"
                                self._errors = 0
                                change = btc.get("usd_24h_change")
                                if change is not None:
                                    self.change_24h = float(change)
                        elif resp.status == 429:
                            await asyncio.sleep(15)
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    self._errors += 1
                    if self._errors > 5:
                        await asyncio.sleep(10)
                await asyncio.sleep(POLL_INTERVAL)
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    def stop(self):
        self._running = False
