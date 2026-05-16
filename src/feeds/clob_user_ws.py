"""ClobUserFeed — websocket client for Polymarket CLOB user-channel fills.

Buffers recent fill events keyed by order_id. Supports:
  - subscribe(condition_id) for dynamic market additions
  - wait_fill(order_id, timeout) async event when that order is filled
  - poll_fill(order_id) non-blocking lookup

Reconnects on error. Auth via env vars; if missing the feed stays idle.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import websockets


WS_URL = os.getenv(
    "POLY_CLOB_USER_WS",
    "wss://ws-subscriptions-clob.polymarket.com/ws/user",
)
FILL_TTL_SEC = float(os.getenv("EXEC_FILL_BUFFER_TTL_SEC", "600"))


@dataclass
class FillEvent:
    order_id: str
    price: float
    size_matched_shares: float
    size_matched_usdc: float
    side: str
    asset_id: Optional[str]
    timestamp: float


class ClobUserFeed:
    def __init__(self):
        self._fills: dict = {}
        self._fill_events: dict = {}
        self._markets: set = set()
        self._ws = None
        self._running = False
        self._auth: dict = {}

    # ── Public API ───────────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    def subscribe(self, condition_id: str) -> None:
        condition_id = str(condition_id)
        if not condition_id or condition_id in self._markets:
            return
        self._markets.add(condition_id)
        if self._ws is not None:
            asyncio.create_task(self._send_subscribe())

    def poll_fill(self, order_id: str) -> Optional[FillEvent]:
        return self._fills.get(str(order_id))

    async def wait_fill(self, order_id: str, *, timeout: float = 1.0) -> dict:
        oid = str(order_id)
        if oid in self._fills:
            return self._payload(self._fills[oid])
        evt = self._fill_events.setdefault(oid, asyncio.Event())
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return {"kind": "fill_timeout", "order_id": oid}
        f = self._fills.get(oid)
        if f is None:
            return {"kind": "fill_timeout", "order_id": oid}
        return self._payload(f)

    def stop(self) -> None:
        self._running = False

    async def connect(self) -> None:
        api_key = os.getenv("API_KEY") or os.getenv("CLOB_API_KEY")
        api_secret = os.getenv("API_SECRET") or os.getenv("SECRET") or os.getenv("CLOB_SECRET")
        api_passphrase = (
            os.getenv("API_PASSPHRASE")
            or os.getenv("PASSPHRASE")
            or os.getenv("CLOB_PASS_PHRASE")
        )
        if not (api_key and api_secret and api_passphrase):
            print("[clob-user] auth env missing — feed disabled", flush=True)
            return
        self._auth = {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        }

        self._running = True
        fail_streak = 0
        while self._running:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    fail_streak = 0
                    await self._send_subscribe()
                    print(f"[clob-user] connected: {WS_URL}", flush=True)
                    while self._running:
                        msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                fail_streak += 1
                print(
                    f"[clob-user] WS error ({type(exc).__name__}: {exc}), streak={fail_streak}",
                    flush=True,
                )
            finally:
                self._ws = None
            if self._running:
                await asyncio.sleep(min(30, 2 + fail_streak * 2))

    # ── Internals ────────────────────────────────────────────────────────────

    async def _send_subscribe(self) -> None:
        if self._ws is None:
            return
        payload = {
            "type": "user",
            "auth": self._auth,
            "markets": sorted(self._markets),
        }
        try:
            await self._ws.send(json.dumps(payload))
        except Exception as exc:
            print(f"[clob-user] subscribe send failed: {exc}", flush=True)

    async def _handle_message(self, raw) -> None:
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                return
        if not isinstance(raw, str) or not raw:
            return
        if raw in {"PONG", "pong", "PING", "ping"}:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        events = data if isinstance(data, list) else [data]
        for evt in events:
            if not isinstance(evt, dict):
                continue
            self._apply_event(evt)
        self._gc()

    def _apply_event(self, evt: dict) -> None:
        evt_type = (evt.get("event_type") or evt.get("type") or "").lower()
        if evt_type not in {"trade", "fill", "order", "order_update"}:
            return
        order_id = (
            evt.get("order_id")
            or evt.get("orderID")
            or evt.get("maker_order_id")
            or evt.get("taker_order_id")
        )
        if not order_id:
            return
        order_id = str(order_id)
        try:
            price = float(evt.get("price") or evt.get("matched_price") or 0)
            size_shares = float(
                evt.get("size_matched")
                or evt.get("matched_size")
                or evt.get("size")
                or 0
            )
        except (TypeError, ValueError):
            return
        if size_shares <= 0 or price <= 0:
            return
        size_usdc = round(price * size_shares, 4)
        side = str(evt.get("side") or "BUY")
        asset_id = evt.get("asset_id") or evt.get("token_id") or evt.get("market")
        existing = self._fills.get(order_id)
        if existing is not None:
            size_shares = max(size_shares, existing.size_matched_shares)
            size_usdc = max(size_usdc, existing.size_matched_usdc)
        self._fills[order_id] = FillEvent(
            order_id=order_id,
            price=price,
            size_matched_shares=size_shares,
            size_matched_usdc=size_usdc,
            side=side,
            asset_id=str(asset_id) if asset_id is not None else None,
            timestamp=time.time(),
        )
        evt_obj = self._fill_events.get(order_id)
        if evt_obj is not None:
            evt_obj.set()

    def _gc(self) -> None:
        cutoff = time.time() - FILL_TTL_SEC
        stale = [oid for oid, f in self._fills.items() if f.timestamp < cutoff]
        for oid in stale:
            self._fills.pop(oid, None)
            evt = self._fill_events.pop(oid, None)
            if evt is not None and not evt.is_set():
                evt.set()

    @staticmethod
    def _payload(f: FillEvent) -> dict:
        return {
            "kind": "fill",
            "order_id": f.order_id,
            "price": f.price,
            "size_matched_shares": f.size_matched_shares,
            "size_matched_usdc": f.size_matched_usdc,
            "side": f.side,
            "asset_id": f.asset_id,
        }
