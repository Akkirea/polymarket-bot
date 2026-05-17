"""ClobBookFeed — websocket client for Polymarket CLOB market data + trade prints.

Maintains in-memory top-of-book per token_id. Supports:
  - subscribe(token_id) for dynamic subscriptions
  - top(token_id, max_age_sec) for fresh top-of-book
  - wait_update(token_id, timeout) async event for any book change
  - set_trade_callback(fn) / set_book_callback(fn) for ShadowFillSimulator

Reconnects on error with exponential backoff. Stale-tolerant.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Callable, Optional

import websockets


WS_URL = os.getenv(
    "POLY_CLOB_MARKET_WS",
    "wss://ws-subscriptions-clob.polymarket.com/ws/market",
)


@dataclass
class TopOfBook:
    token_id: str
    bid_price: Optional[float]
    bid_size: Optional[float]
    ask_price: Optional[float]
    ask_size: Optional[float]
    last_update: float


class ClobBookFeed:
    def __init__(self):
        self._tops: dict = {}
        self._subscriptions: set = set()
        self._update_events: dict = {}
        self._lock = asyncio.Lock()
        self._ws = None
        self._running = False
        self._connected_evt = asyncio.Event()
        self._trade_callback: Optional[Callable] = None
        self._book_callback: Optional[Callable] = None
        # Per-token freshness (Fix 1): each subscribed token has its own update timestamp
        # so the freshness gate cannot be satisfied by activity on a different token.
        self._last_book_update_by_token: dict[str, float] = {}
        self._last_trade_event_by_token: dict[str, float] = {}
        # Aggregate timestamps retained for status/back-compat; do NOT use for gating.
        self.last_trade_event_ts: float = 0.0
        self.last_book_update_ts: float = 0.0

    # ── Public API ───────────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    def set_trade_callback(self, fn: Callable) -> None:
        self._trade_callback = fn

    def set_book_callback(self, fn: Callable) -> None:
        self._book_callback = fn

    def subscribe(self, token_id: str) -> None:
        token_id = str(token_id)
        if not token_id or token_id in self._subscriptions:
            return
        self._subscriptions.add(token_id)
        if self._ws is not None:
            asyncio.create_task(self._send_subscribe([token_id]))

    def is_subscribed(self, token_id: str) -> bool:
        return str(token_id) in self._subscriptions

    def get_token_age(self, token_id: str) -> Optional[float]:
        """Seconds since last book update for this token, or None if never updated."""
        ts = self._last_book_update_by_token.get(str(token_id), 0.0)
        if ts <= 0:
            return None
        return time.time() - ts

    def is_token_fresh(self, token_id: str, max_age: float = 30.0) -> bool:
        age = self.get_token_age(token_id)
        return age is not None and age < max_age

    def freshness_snapshot(self) -> dict:
        """Per-token age in seconds for the status endpoint."""
        now = time.time()
        book_ages: dict = {}
        trade_ages: dict = {}
        for tok in self._subscriptions:
            tok_s = str(tok)
            b = self._last_book_update_by_token.get(tok_s, 0.0)
            t = self._last_trade_event_by_token.get(tok_s, 0.0)
            book_ages[tok_s[:12]] = round(now - b, 1) if b > 0 else None
            trade_ages[tok_s[:12]] = round(now - t, 1) if t > 0 else None
        return {"book_ages_sec": book_ages, "trade_ages_sec": trade_ages}

    def top(self, token_id: str, *, max_age_sec: float = 1.5) -> Optional[TopOfBook]:
        t = self._tops.get(str(token_id))
        if t is None:
            return None
        if time.time() - t.last_update > max_age_sec:
            return None
        return t

    async def wait_update(self, token_id: str, *, timeout: float = 1.0) -> dict:
        evt = self._update_events.setdefault(str(token_id), asyncio.Event())
        evt.clear()
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
            return {"kind": "book_update", "token_id": token_id}
        except asyncio.TimeoutError:
            return {"kind": "book_timeout", "token_id": token_id}

    def stop(self) -> None:
        self._running = False

    async def connect(self) -> None:
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
                    self._connected_evt.set()
                    print(f"[clob-book] connected: {WS_URL}", flush=True)
                    if self._subscriptions:
                        await self._send_subscribe(sorted(self._subscriptions))
                    while self._running:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                fail_streak += 1
                print(
                    f"[clob-book] WS error ({type(exc).__name__}: {exc}), streak={fail_streak}",
                    flush=True,
                )
            finally:
                self._ws = None
                self._connected_evt.clear()
            if self._running:
                await asyncio.sleep(min(30, 2 + fail_streak * 2))

    # ── Internals ────────────────────────────────────────────────────────────

    async def _send_subscribe(self, token_ids) -> None:
        if self._ws is None:
            return
        payload = {
            "type": "market",
            "assets_ids": list(token_ids),
        }
        try:
            await self._ws.send(json.dumps(payload))
        except Exception as exc:
            print(f"[clob-book] subscribe send failed: {exc}", flush=True)

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

    def _apply_event(self, evt: dict) -> None:
        token_id = (
            evt.get("asset_id")
            or evt.get("token_id")
            or evt.get("market")
            or evt.get("assetId")
        )
        if not token_id:
            return
        token_id = str(token_id)
        evt_type = (evt.get("event_type") or evt.get("type") or "").lower()

        if evt_type in {"book", "order_book"}:
            bid_price, bid_size = self._best_level(evt.get("bids"))
            ask_price, ask_size = self._best_level(evt.get("asks"))
            now_ts = time.time()
            self._tops[token_id] = TopOfBook(
                token_id=token_id,
                bid_price=bid_price,
                bid_size=bid_size,
                ask_price=ask_price,
                ask_size=ask_size,
                last_update=now_ts,
            )
            self._last_book_update_by_token[token_id] = now_ts
            self.last_book_update_ts = now_ts
            if self._book_callback is not None:
                try:
                    self._book_callback(token_id, bid_price, bid_size)
                except Exception as exc:
                    print(f"[clob-book] book_callback error: {exc}", flush=True)
        elif evt_type in {"price_change", "tick_size_change", "level_update"}:
            current = self._tops.get(token_id)
            if current is None:
                return
            side = (evt.get("side") or "").lower()
            try:
                price = float(evt.get("price")) if evt.get("price") is not None else None
                size = float(evt.get("size")) if evt.get("size") is not None else None
            except (TypeError, ValueError):
                return
            new_bid_price = current.bid_price
            new_bid_size = current.bid_size
            new_ask_price = current.ask_price
            new_ask_size = current.ask_size
            if side in {"buy", "bid"}:
                new_bid_price, new_bid_size = price, size
            elif side in {"sell", "ask"}:
                new_ask_price, new_ask_size = price, size
            now_ts = time.time()
            self._tops[token_id] = TopOfBook(
                token_id=token_id,
                bid_price=new_bid_price,
                bid_size=new_bid_size,
                ask_price=new_ask_price,
                ask_size=new_ask_size,
                last_update=now_ts,
            )
            self._last_book_update_by_token[token_id] = now_ts
            self.last_book_update_ts = now_ts
            if self._book_callback is not None:
                try:
                    self._book_callback(token_id, new_bid_price, new_bid_size)
                except Exception as exc:
                    print(f"[clob-book] book_callback error: {exc}", flush=True)
        elif evt_type in {"trade", "last_trade_price", "matched", "trades"}:
            try:
                price = float(evt.get("price") or evt.get("matched_price") or 0)
                size = float(
                    evt.get("size")
                    or evt.get("matched_size")
                    or evt.get("size_matched")
                    or 0
                )
            except (TypeError, ValueError):
                return
            if price <= 0 or size <= 0:
                return
            ts_raw = evt.get("timestamp") or evt.get("ts") or time.time()
            try:
                ts = float(ts_raw)
                if ts > 10_000_000_000:
                    ts = ts / 1000.0
            except (TypeError, ValueError):
                ts = time.time()
            # Fix 2: extract aggressor side. Polymarket WS may publish under
            # different keys; normalise to BUY/SELL or None when missing.
            raw_side = (
                evt.get("side")
                or evt.get("taker_side")
                or evt.get("taker_order_side")
                or evt.get("aggressor_side")
                or evt.get("maker_side")  # if maker_side present, invert below
            )
            aggressor_side: Optional[str] = None
            if raw_side:
                s = str(raw_side).upper().strip()
                if s in {"BUY", "B", "BID"}:
                    aggressor_side = "BUY"
                elif s in {"SELL", "S", "ASK"}:
                    aggressor_side = "SELL"
                # If event used maker_side semantics, the aggressor is the opposite.
                if evt.get("maker_side") and not (
                    evt.get("side") or evt.get("taker_side")
                    or evt.get("taker_order_side") or evt.get("aggressor_side")
                ):
                    aggressor_side = "SELL" if aggressor_side == "BUY" else (
                        "BUY" if aggressor_side == "SELL" else None
                    )
            now_ts = time.time()
            self._last_trade_event_by_token[token_id] = now_ts
            self.last_trade_event_ts = now_ts
            if self._trade_callback is not None:
                try:
                    self._trade_callback(token_id, price, size, ts, aggressor_side)
                except Exception as exc:
                    print(f"[clob-book] trade_callback error: {exc}", flush=True)
            return
        else:
            return

        evt_obj = self._update_events.get(token_id)
        if evt_obj is not None:
            evt_obj.set()

    @staticmethod
    def _best_level(levels):
        if not isinstance(levels, list) or not levels:
            return None, None
        try:
            best = levels[0]
            if isinstance(best, dict):
                price = float(best.get("price"))
                size = float(best.get("size"))
            elif isinstance(best, (list, tuple)) and len(best) >= 2:
                price = float(best[0])
                size = float(best[1])
            else:
                return None, None
            return price, size
        except (TypeError, ValueError):
            return None, None
