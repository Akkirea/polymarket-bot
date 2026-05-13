"""
Polymarket RTDS client for BTC price ticks.

This feed is used for shadow research only. It lets us measure whether
Polymarket's own real-time stream can recover a BTC 5m price-to-beat before
Gamma exposes eventMetadata.priceToBeat.
"""

import asyncio
import json
import time
from collections import deque
from typing import Optional

import websockets


class PolymarketRtdsPriceFeed:
    """Real-time BTC price via Polymarket RTDS."""

    _WS_URL = "wss://ws-live-data.polymarket.com"

    def __init__(self):
        self.current_price: Optional[float] = None
        self.last_update: Optional[float] = None
        self.source: str = "polymarket-rtds"
        self.current_symbol: Optional[str] = None
        self._price_history: deque = deque(maxlen=1200)  # ~10 min at up to a few ticks/sec
        self._running = False
        self._ws = None

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    def get_price_at_window_start(self, window_start_ts: float) -> Optional[float]:
        """Find the RTDS BTC/USD tick closest to the market start timestamp."""
        if not self._price_history:
            return None

        closest_ts, closest_price = min(
            self._price_history,
            key=lambda item: abs(item[0] - window_start_ts),
        )
        if abs(closest_ts - window_start_ts) > 5.0:
            return None
        return float(closest_price)

    async def connect(self):
        """Connect to Polymarket RTDS and stream BTC prices."""
        self._running = True
        fail_streak = 0
        subscription = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices",
                    "type": "update",
                }
            ],
        }

        while self._running:
            try:
                async with websockets.connect(
                    self._WS_URL,
                    ping_interval=None,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    fail_streak = 0
                    await ws.send(json.dumps(subscription))
                    print("[rtds] connected: crypto_prices", flush=True)

                    ping_task = asyncio.create_task(self._heartbeat(ws))
                    try:
                        while self._running:
                            msg = await asyncio.wait_for(ws.recv(), timeout=15)
                            await self._handle_message(msg)
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except asyncio.CancelledError:
                break
            except Exception as exc:
                fail_streak += 1
                print(f"[rtds] WS error ({type(exc).__name__}: {exc}), streak={fail_streak}", flush=True)
            finally:
                self._ws = None

            if self._running:
                await asyncio.sleep(min(30, 2 + fail_streak * 2))

    async def _heartbeat(self, ws) -> None:
        while self._running:
            await asyncio.sleep(5)
            try:
                await ws.send("PING")
            except Exception:
                return

    async def _handle_message(self, msg: str) -> None:
        if msg in {"PONG", "pong"}:
            return
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            return

        payload = data.get("payload") if isinstance(data, dict) else None
        if not isinstance(payload, dict):
            return

        symbol = str(payload.get("symbol") or "").lower()
        if symbol not in {"btc/usd", "btcusdt"}:
            return
        value = payload.get("value")
        if value is None:
            return

        try:
            price = float(value)
        except (TypeError, ValueError):
            return

        payload_ts = payload.get("timestamp") or data.get("timestamp")
        ts = time.time()
        try:
            if payload_ts:
                raw_ts = float(payload_ts)
                ts = raw_ts / 1000.0 if raw_ts > 10_000_000_000 else raw_ts
        except (TypeError, ValueError):
            pass

        now = time.time()
        self.current_price = price
        self.current_symbol = symbol
        self.last_update = now
        self._price_history.append((ts, price))

        cutoff = now - 600
        while self._price_history and self._price_history[0][0] < cutoff:
            self._price_history.popleft()

    def stop(self):
        self._running = False
