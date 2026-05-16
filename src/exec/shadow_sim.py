"""ShadowFillSimulator — evaluates resting shadow BUY limits against real CLOB trade prints.

Quacks like ClobUserFeed: exposes wait_fill(order_id, timeout) and poll_fill(order_id).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .. import db_shadow


EPS = 1e-9
ELIGIBILITY_DELAY_SEC = 0.5


@dataclass
class _ShadowOrder:
    order_id: str
    token_id: str
    side: str
    limit_price: float
    size_usdc: float
    placed_at: float
    last_price_seen_at_limit: float = 0.0
    last_size_at_limit: Optional[float] = None
    filled_aggressive: bool = False
    filled_conservative: bool = False
    fill_price: Optional[float] = None
    fill_at: Optional[float] = None
    size_filled_shares: float = 0.0
    size_filled_usdc: float = 0.0
    cancelled: bool = False
    cancel_reason: Optional[str] = None
    trade_prints_during_window: int = 0
    book_updates_during_window: int = 0
    fill_event: asyncio.Event = field(default_factory=asyncio.Event)


class ShadowFillSimulator:
    def __init__(self):
        self._orders: dict[str, _ShadowOrder] = {}
        self._orders_by_token: dict[str, set[str]] = {}

    # ── lifecycle ────────────────────────────────────────────────────────────

    def register(
        self,
        *,
        order_id: str,
        token_id: str,
        side: str,
        limit_price: float,
        size_usdc: float,
        placed_at: float,
    ) -> None:
        o = _ShadowOrder(
            order_id=order_id,
            token_id=str(token_id),
            side=side,
            limit_price=float(limit_price),
            size_usdc=float(size_usdc),
            placed_at=float(placed_at),
        )
        self._orders[order_id] = o
        self._orders_by_token.setdefault(o.token_id, set()).add(order_id)

    def cancel(self, order_id: str, *, reason: str) -> None:
        o = self._orders.get(order_id)
        if o is None or o.cancelled or o.filled_aggressive:
            return
        o.cancelled = True
        o.cancel_reason = reason
        time_resting = max(0.0, time.time() - o.placed_at)
        try:
            db_shadow.update_attempt(order_id, {
                "cancel_reason": reason,
                "final_resting_price": o.limit_price,
                "time_resting_sec": time_resting,
                "trade_prints_during_window": o.trade_prints_during_window,
                "book_updates_during_window": o.book_updates_during_window,
            })
        except Exception as exc:
            print(f"[shadow-sim] db update failed for {order_id}: {exc}", flush=True)
        if not o.fill_event.is_set():
            o.fill_event.set()
        self._orders_by_token.get(o.token_id, set()).discard(order_id)

    # ── feed callbacks ───────────────────────────────────────────────────────

    def on_book_update(
        self,
        token_id: str,
        bid_price: Optional[float],
        bid_size: Optional[float],
    ) -> None:
        ids = self._orders_by_token.get(str(token_id))
        if not ids:
            return
        for oid in list(ids):
            o = self._orders.get(oid)
            if o is None or o.cancelled or o.filled_aggressive:
                continue
            o.book_updates_during_window += 1
            if bid_price is None:
                continue
            if abs(bid_price - o.limit_price) < EPS:
                if (
                    o.last_size_at_limit is not None
                    and bid_size is not None
                    and bid_size < o.last_size_at_limit - EPS
                ):
                    o.last_price_seen_at_limit = time.time()
                if bid_size is not None:
                    o.last_size_at_limit = float(bid_size)

    def on_trade(
        self,
        token_id: str,
        price: float,
        size_shares: float,
        ts: Optional[float] = None,
    ) -> None:
        ids = self._orders_by_token.get(str(token_id))
        if not ids:
            return
        t_trade = float(ts) if ts is not None else time.time()
        try:
            price = float(price)
            size_shares = float(size_shares)
        except (TypeError, ValueError):
            return
        if price <= 0 or size_shares <= 0:
            return
        for oid in list(ids):
            o = self._orders.get(oid)
            if o is None or o.cancelled or o.filled_aggressive:
                continue
            o.trade_prints_during_window += 1
            if t_trade < o.placed_at + ELIGIBILITY_DELAY_SEC:
                continue
            if price > o.limit_price + EPS:
                continue
            if not o.filled_aggressive:
                self._record_fill(o, t_trade, size_shares, kind="aggressive")
            if (
                not o.filled_conservative
                and o.last_price_seen_at_limit >= o.placed_at
            ):
                self._record_fill(o, t_trade, size_shares, kind="conservative")

    # ── fill recording ───────────────────────────────────────────────────────

    def _record_fill(
        self,
        o: _ShadowOrder,
        ts: float,
        size_shares: float,
        *,
        kind: str,
    ) -> None:
        # Set the flag the moment the rule fires; share accounting is shared
        # across both rules so a fully-filled order does not block the second
        # flag from being recorded.
        if kind == "aggressive":
            o.filled_aggressive = True
        else:
            o.filled_conservative = True

        max_shares = (o.size_usdc / o.limit_price) if o.limit_price > 0 else 0.0
        remaining_shares = max(0.0, max_shares - o.size_filled_shares)
        fill_shares = min(remaining_shares, size_shares)
        if fill_shares > 0:
            o.size_filled_shares += fill_shares
            o.size_filled_usdc = round(o.size_filled_shares * o.limit_price, 4)
            if o.fill_price is None:
                o.fill_price = o.limit_price
                o.fill_at = ts

        fully = o.size_filled_shares + EPS >= max_shares

        try:
            db_shadow.update_attempt(o.order_id, {
                "hypothetical_filled_aggressive": 1 if o.filled_aggressive else 0,
                "hypothetical_filled_conservative": 1 if o.filled_conservative else 0,
                "hypothetical_fill_price": o.fill_price,
                "hypothetical_fill_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "hypothetical_size_filled_usdc": o.size_filled_usdc,
                "time_resting_sec": max(0.0, ts - o.placed_at),
                "final_resting_price": o.limit_price,
                "trade_prints_during_window": o.trade_prints_during_window,
                "book_updates_during_window": o.book_updates_during_window,
            })
        except Exception as exc:
            print(f"[shadow-sim] db update failed for {o.order_id}: {exc}", flush=True)

        if o.filled_aggressive and fully and not o.fill_event.is_set():
            o.fill_event.set()
            self._orders_by_token.get(o.token_id, set()).discard(o.order_id)

    # ── ClobUserFeed-compatible API ──────────────────────────────────────────

    def subscribe(self, condition_id: str) -> None:
        return None

    def poll_fill(self, order_id: str) -> Optional[dict]:
        o = self._orders.get(order_id)
        if o is None or not o.filled_aggressive:
            return None
        return self._fill_payload(o)

    async def wait_fill(self, order_id: str, *, timeout: float = 1.0) -> dict:
        o = self._orders.get(order_id)
        if o is None:
            return {"kind": "fill_timeout", "order_id": order_id}
        if o.filled_aggressive:
            return self._fill_payload(o)
        try:
            await asyncio.wait_for(o.fill_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return {"kind": "fill_timeout", "order_id": order_id}
        if o.cancelled or not o.filled_aggressive:
            return {"kind": "fill_timeout", "order_id": order_id}
        return self._fill_payload(o)

    def _fill_payload(self, o: _ShadowOrder) -> dict:
        return {
            "kind": "fill",
            "order_id": o.order_id,
            "price": o.fill_price or o.limit_price,
            "size_matched_shares": o.size_filled_shares,
            "size_matched_usdc": o.size_filled_usdc,
            "side": o.side,
            "asset_id": o.token_id,
            "shadow": True,
        }

    # ── telemetry ────────────────────────────────────────────────────────────

    def status_snapshot(self) -> dict:
        open_count = sum(
            1 for o in self._orders.values()
            if not o.cancelled and not o.filled_aggressive
        )
        return {
            "tracked_orders": len(self._orders),
            "open_orders": open_count,
        }
