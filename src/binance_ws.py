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

    def __init__(self):
        self.current_price: Optional[float] = None
        self.last_update: Optional[float] = None
        self._price_history: deque = deque(maxlen=600)  # ~10 min at 1/sec
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

    async def connect(self):
        """Connect to Binance and start streaming prices."""
        self._running = True

        while self._running:
            try:
                async with websockets.connect(
                    config.BINANCE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    while self._running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            price = float(data["p"])
                            now = time.time()

                            self.current_price = price
                            self.last_update = now
                            self._price_history.append((now, price))

                        except asyncio.TimeoutError:
                            # Send a ping to keep alive
                            continue
                        except (json.JSONDecodeError, KeyError):
                            continue

            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                self._ws = None
                if self._running:
                    # Reconnect after brief pause
                    await asyncio.sleep(2)

            except Exception as e:
                self._ws = None
                if self._running:
                    await asyncio.sleep(5)

    def stop(self):
        self._running = False
