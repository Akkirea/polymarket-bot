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
        # WS instrumentation (observability-only): distinguishes silent-market vs
        # heartbeat-only / parse-failure / schema-drift / subscribe-failure modes.
        self.ws_messages_received_total: int = 0
        self.ws_text_pong_total: int = 0
        self.ws_messages_unparseable_total: int = 0
        self.ws_events_by_type: dict[str, int] = {}
        self.parsed_book_updates_total: int = 0
        self.parsed_price_change_total: int = 0
        self.parsed_trade_total: int = 0
        self.ws_events_unrouted_total: int = 0
        self.ws_reconnects_total: int = 0
        self.last_ws_message_ts: float = 0.0
        self.last_ws_subscribe_sent_ts: float = 0.0
        # Debug capture (time-limited): first N raw event samples per evt_type and
        # ring buffer of reconnect markers with book-counter snapshots. Used to
        # disambiguate ID mismatch / nested-changes / unknown failure modes.
        self._raw_sample_limit: int = 5
        self._raw_samples: dict[str, list] = {}
        self._reconnect_log_limit: int = 16
        self._reconnect_log: list = []
        # Reconnect-cause classifier. Distinguishes TimeoutError (our 30s recv
        # timeout) from ConnectionClosedError (the websockets-lib protocol-pong
        # timeout / server-initiated close) from other failure modes. Lets us
        # verify whether the existing protocol-level ping_interval=20 +
        # ping_timeout=10 keepalive is actually getting Pong responses before
        # adding a parallel application-level text-PING heartbeat.
        self.ws_reconnect_causes: dict[str, int] = {}
        self.last_close_exc_type: Optional[str] = None
        self.last_close_exc_msg: Optional[str] = None
        self.last_close_ts: float = 0.0
        # Text-PING keepalive experiment. Polymarket CLOB docs require the
        # client to send text "PING" every 10s on the market channel; their
        # official real-time-data-client (src/client.ts) sends text "ping"
        # every 5s. Our implementation previously relied solely on the
        # websockets-library protocol-level pings (ping_interval=20,
        # ping_timeout=10, unchanged here). This branch adds a parallel
        # application-level text-ping task to test whether the post-burst
        # silence / 30s recv-timeout reconnect spiral is caused by the
        # missing client-initiated heartbeat. Pure addition; no other
        # connect / recv-timeout / reconnect-policy changes.
        self._text_ping_interval_sec: float = 5.0
        self._text_ping_payload: str = "ping"
        self.text_pings_sent_total: int = 0
        self.text_pings_send_errors_total: int = 0
        self.last_text_ping_ts: float = 0.0
        self.text_pong_received_total: int = 0

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
        iteration = 0
        last_exc_type: Optional[str] = None
        while self._running:
            ping_task: Optional[asyncio.Task] = None
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    fail_streak = 0
                    self._connected_evt.set()
                    if iteration > 0:
                        self.ws_reconnects_total += 1
                        try:
                            self._reconnect_log.append({
                                "ts": time.time(),
                                "reconnect_n": self.ws_reconnects_total,
                                "book_total_at_reconnect": self.parsed_book_updates_total,
                                "ws_msgs_at_reconnect": self.ws_messages_received_total,
                                "by_type_snapshot": dict(self.ws_events_by_type),
                                # Attribution: which exception ended the
                                # previous connection. None on the very first
                                # connect; populated thereafter.
                                "prev_close_cause": last_exc_type,
                            })
                            if len(self._reconnect_log) > self._reconnect_log_limit:
                                self._reconnect_log.pop(0)
                        except Exception:
                            pass
                    print(f"[clob-book] connected: {WS_URL}", flush=True)
                    if self._subscriptions:
                        await self._send_subscribe(sorted(self._subscriptions))
                    # Spawn the text-PING keepalive task scoped to this connection.
                    # Cancelled in finally; lifetime bounded by this `async with`.
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    while self._running:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                exc_type = type(exc).__name__
                last_exc_type = exc_type
                self.ws_reconnect_causes[exc_type] = (
                    self.ws_reconnect_causes.get(exc_type, 0) + 1
                )
                self.last_close_exc_type = exc_type
                try:
                    self.last_close_exc_msg = str(exc)[:200]
                except Exception:
                    self.last_close_exc_msg = None
                self.last_close_ts = time.time()
                fail_streak += 1
                print(
                    f"[clob-book] WS error ({exc_type}: {exc}), streak={fail_streak}",
                    flush=True,
                )
            finally:
                if ping_task is not None and not ping_task.done():
                    ping_task.cancel()
                    try:
                        await ping_task
                    except (asyncio.CancelledError, Exception):
                        pass
                self._ws = None
                self._connected_evt.clear()
                iteration += 1
            if self._running:
                await asyncio.sleep(min(30, 2 + fail_streak * 2))

    # ── Internals ────────────────────────────────────────────────────────────

    async def _ping_loop(self, ws) -> None:
        """Send an application-level text "ping" every ``_text_ping_interval_sec``
        for the lifetime of the supplied connection.

        Lifetime is bounded by the connection: the task is spawned inside the
        ``async with websockets.connect(...)`` block in ``connect`` and is
        cancelled in the ``finally``. The captured ``ws`` reference ensures we
        never send on a successor connection.

        Errors from ``ws.send`` are counted but do NOT raise — recv-timeout /
        the existing reconnect loop handles connection death. Cancellation is
        the normal exit path on reconnect.
        """
        try:
            while True:
                await asyncio.sleep(self._text_ping_interval_sec)
                try:
                    await ws.send(self._text_ping_payload)
                    self.text_pings_sent_total += 1
                    self.last_text_ping_ts = time.time()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.text_pings_send_errors_total += 1
                    print(
                        f"[clob-book] text-ping send failed: {type(exc).__name__}: {exc}",
                        flush=True,
                    )
                    return
        except asyncio.CancelledError:
            return

    async def _send_subscribe(self, token_ids) -> None:
        if self._ws is None:
            return
        payload = {
            "type": "market",
            "assets_ids": list(token_ids),
        }
        try:
            await self._ws.send(json.dumps(payload))
            self.last_ws_subscribe_sent_ts = time.time()
        except Exception as exc:
            print(f"[clob-book] subscribe send failed: {exc}", flush=True)

    async def _handle_message(self, raw) -> None:
        self.ws_messages_received_total += 1
        self.last_ws_message_ts = time.time()
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                return
        if not isinstance(raw, str) or not raw:
            return
        if raw in {"PONG", "pong", "PING", "ping"}:
            self.ws_text_pong_total += 1
            if raw in {"PONG", "pong"}:
                # Specific counter for incoming PONG (server's response to our
                # text PING) so we can distinguish it from any unsolicited PING
                # the server might initiate. ws_text_pong_total is preserved
                # unchanged for back-compat.
                self.text_pong_received_total += 1
            return
        try:
            data = json.loads(raw)
        except Exception:
            self.ws_messages_unparseable_total += 1
            return
        events = data if isinstance(data, list) else [data]
        for evt in events:
            if not isinstance(evt, dict):
                continue
            self._apply_event(evt)

    def _apply_event(self, evt: dict) -> None:
        evt_type = (evt.get("event_type") or evt.get("type") or "").lower()
        # by-type telemetry: count every event regardless of token_id presence so we can
        # detect schema drift (unknown types) and "events arrive but lack token_id" cases.
        bucket = evt_type if evt_type else "<no_evt_type>"
        self.ws_events_by_type[bucket] = self.ws_events_by_type.get(bucket, 0) + 1
        # Debug capture: first N samples per evt_type, key-and-shape preview only.
        try:
            samples = self._raw_samples.setdefault(bucket, [])
            if len(samples) < self._raw_sample_limit:
                preview: dict = {}
                for k, v in evt.items():
                    if isinstance(v, list):
                        first = v[0] if v else None
                        if isinstance(first, dict):
                            first_repr = {"_keys": sorted(first.keys()), "_first": {kk: str(vv)[:60] for kk, vv in list(first.items())[:6]}}
                        else:
                            first_repr = str(first)[:120] if first is not None else None
                        preview[k] = {"_type": "list", "len": len(v), "first": first_repr}
                    elif isinstance(v, dict):
                        preview[k] = {"_type": "dict", "keys": sorted(v.keys())[:12]}
                    else:
                        s = str(v)
                        preview[k] = s[:200] if len(s) > 200 else v
                samples.append({"ts": time.time(), "evt": preview})
        except Exception:
            pass
        # Route by evt_type FIRST. Identifier extraction differs between event
        # types: book / last_trade_price carry the per-token id at top level as
        # `asset_id`, while price_change nests one record per affected asset
        # inside `price_changes[].asset_id` (the top-level `market` field is the
        # condition_id and is NOT a valid `_tops` key). Routing first avoids the
        # condition_id-vs-asset_id mismatch that previously dropped every
        # price_change event silently.
        if evt_type in {"book", "order_book"}:
            token_id = (
                evt.get("asset_id")
                or evt.get("token_id")
                or evt.get("assetId")
            )
            if not token_id:
                return
            token_id = str(token_id)
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
            self.parsed_book_updates_total += 1
            if self._book_callback is not None:
                try:
                    self._book_callback(token_id, bid_price, bid_size)
                except Exception as exc:
                    print(f"[clob-book] book_callback error: {exc}", flush=True)
            evt_obj = self._update_events.get(token_id)
            if evt_obj is not None:
                evt_obj.set()
            return

        if evt_type == "price_change":
            changes = evt.get("price_changes") or evt.get("changes") or []
            if not isinstance(changes, list):
                return
            for change in changes:
                if not isinstance(change, dict):
                    continue
                inner_id = (
                    change.get("asset_id")
                    or change.get("token_id")
                    or change.get("assetId")
                )
                if not inner_id:
                    continue
                inner_id = str(inner_id)
                current = self._tops.get(inner_id)
                if current is None:
                    # Wait for a `book` snapshot for this asset before applying
                    # incremental level updates; partial state would otherwise
                    # mis-represent the opposite side.
                    continue
                side = (change.get("side") or "").lower()
                try:
                    price = float(change.get("price")) if change.get("price") is not None else None
                    size = float(change.get("size")) if change.get("size") is not None else None
                except (TypeError, ValueError):
                    continue
                new_bid_price = current.bid_price
                new_bid_size = current.bid_size
                new_ask_price = current.ask_price
                new_ask_size = current.ask_size
                if side in {"buy", "bid"}:
                    new_bid_price, new_bid_size = price, size
                elif side in {"sell", "ask"}:
                    new_ask_price, new_ask_size = price, size
                now_ts = time.time()
                self._tops[inner_id] = TopOfBook(
                    token_id=inner_id,
                    bid_price=new_bid_price,
                    bid_size=new_bid_size,
                    ask_price=new_ask_price,
                    ask_size=new_ask_size,
                    last_update=now_ts,
                )
                self._last_book_update_by_token[inner_id] = now_ts
                self.last_book_update_ts = now_ts
                if self._book_callback is not None:
                    try:
                        self._book_callback(inner_id, new_bid_price, new_bid_size)
                    except Exception as exc:
                        print(f"[clob-book] book_callback error: {exc}", flush=True)
                inner_evt = self._update_events.get(inner_id)
                if inner_evt is not None:
                    inner_evt.set()
            self.parsed_price_change_total += 1
            return

        if evt_type in {"tick_size_change", "level_update"}:
            # Preserve prior flat-shape handling for event types not observed in
            # the validation window. If the server starts emitting these, they
            # will appear via ws_events_by_type for follow-up classification.
            token_id = (
                evt.get("asset_id")
                or evt.get("token_id")
                or evt.get("assetId")
            )
            if not token_id:
                return
            token_id = str(token_id)
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
            evt_obj = self._update_events.get(token_id)
            if evt_obj is not None:
                evt_obj.set()
            return

        if evt_type in {"trade", "last_trade_price", "matched", "trades"}:
            token_id = (
                evt.get("asset_id")
                or evt.get("token_id")
                or evt.get("assetId")
            )
            if not token_id:
                return
            token_id = str(token_id)
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
            self.parsed_trade_total += 1
            if self._trade_callback is not None:
                try:
                    self._trade_callback(token_id, price, size, ts, aggressor_side)
                except Exception as exc:
                    print(f"[clob-book] trade_callback error: {exc}", flush=True)
            return

        self.ws_events_unrouted_total += 1
        return

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
