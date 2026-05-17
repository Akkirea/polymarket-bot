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
    # Fix 2: queue + direction tracking
    side_is_buy: bool = True                          # this codebase only places BUYs in shadow
    queue_ahead_at_placement: float = 0.0             # shares queued ahead at placement time
    queue_ahead_remaining: float = 0.0                # decremented as same-side trades print
    queue_cleared_before_fill: float = 0.0            # shares of queue consumed before our fill
    direction_rejections: int = 0                     # count of wrong-aggressor prints ignored
    trade_aggressor_side_on_fill: Optional[str] = None
    fill_event: asyncio.Event = field(default_factory=asyncio.Event)


class ShadowFillSimulator:
    def __init__(self):
        self._orders: dict[str, _ShadowOrder] = {}
        self._orders_by_token: dict[str, set[str]] = {}
        # Latest known top-of-book per token (bid_price, bid_size), updated from
        # on_book_update. Used to snapshot queue depth at register() time.
        self._latest_top: dict[str, tuple[Optional[float], Optional[float]]] = {}
        # Diagnostics counters (exposed via status_snapshot)
        self._direction_rejections_total: int = 0
        self._missing_direction_total: int = 0

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
        tok = str(token_id)
        lp = float(limit_price)
        # Fix 2: snapshot queue ahead of us at this price level from the cached
        # top-of-book. We only place BUY shadow orders, so we care about bids.
        # If we're at the best bid → queued behind existing bid size.
        # If we're below the best bid → still use best-bid size as a conservative
        # proxy (we'd actually queue behind that AND every level above us).
        # If we're above the best bid → we're the new best, queue ahead = 0.
        queue_ahead = 0.0
        top = self._latest_top.get(tok)
        if top is not None:
            bid_price, bid_size = top
            if bid_price is not None and bid_size is not None:
                if lp + EPS < bid_price:
                    queue_ahead = float(bid_size)        # below best: conservative proxy
                elif abs(bid_price - lp) < EPS:
                    queue_ahead = float(bid_size)        # at best: queue behind existing
                # else: lp > bid → new best → queue_ahead stays 0
        o = _ShadowOrder(
            order_id=order_id,
            token_id=tok,
            side=side,
            limit_price=lp,
            size_usdc=float(size_usdc),
            placed_at=float(placed_at),
            side_is_buy=True,                            # shadow path is BUY-only
            queue_ahead_at_placement=queue_ahead,
            queue_ahead_remaining=queue_ahead,
        )
        self._orders[order_id] = o
        self._orders_by_token.setdefault(o.token_id, set()).add(order_id)
        # Persist the queue snapshot immediately so the row reflects what we
        # observed at placement, even if the order is cancelled before any fill.
        try:
            db_shadow.update_attempt(order_id, {
                "queue_ahead_at_placement": queue_ahead,
            })
        except Exception as exc:
            print(f"[shadow-sim] db queue-snapshot write failed for {order_id}: {exc}", flush=True)

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
        tok = str(token_id)
        # Fix 2: cache latest top-of-book for register() to snapshot queue depth.
        self._latest_top[tok] = (
            float(bid_price) if bid_price is not None else None,
            float(bid_size) if bid_size is not None else None,
        )
        ids = self._orders_by_token.get(tok)
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
        aggressor_side: Optional[str] = None,
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
        # Fix 2: direction validation. A SELL aggressor hits resting bids; a BUY
        # aggressor lifts asks. Our shadow orders are BUY-only, so a BUY aggressor
        # cannot fill us — record the rejection and move on.
        agg = aggressor_side.upper() if isinstance(aggressor_side, str) else None
        if agg is None:
            # No direction info on this print; we log it once-per-bucket via the
            # counter and proceed (otherwise an unparsed WS schema halts all fills).
            self._missing_direction_total += 1
            if self._missing_direction_total in (1, 100, 1000):
                print(
                    f"[shadow-sim] WARN: trade event missing aggressor_side "
                    f"(total={self._missing_direction_total}); proceeding without direction check",
                    flush=True,
                )

        for oid in list(ids):
            o = self._orders.get(oid)
            if o is None or o.cancelled or o.filled_aggressive:
                continue
            o.trade_prints_during_window += 1
            if t_trade < o.placed_at + ELIGIBILITY_DELAY_SEC:
                continue
            if price > o.limit_price + EPS:
                continue
            # Direction gate: skip if aggressor is on the same side as our resting order.
            # (Resting BUY must be filled by a SELL aggressor.)
            if agg is not None:
                same_side = (o.side_is_buy and agg == "BUY") or (not o.side_is_buy and agg == "SELL")
                if same_side:
                    o.direction_rejections += 1
                    self._direction_rejections_total += 1
                    continue

            # Queue clearing: same-side flow that arrives ahead of us in the queue
            # must be consumed first. Only the leftover (after the queue ahead is
            # drained) becomes available to fill our order.
            remaining_queue = max(0.0, o.queue_ahead_remaining)
            if remaining_queue > 0:
                consumed = min(size_shares, remaining_queue)
                o.queue_ahead_remaining = remaining_queue - consumed
                leftover = size_shares - consumed
                if leftover <= 0:
                    continue
                effective_size = leftover
            else:
                effective_size = size_shares
            if agg is not None:
                o.trade_aggressor_side_on_fill = agg

            if not o.filled_aggressive:
                # queue_cleared_before_fill is set once, at the first fill
                if o.queue_cleared_before_fill == 0.0:
                    o.queue_cleared_before_fill = max(
                        0.0, o.queue_ahead_at_placement - o.queue_ahead_remaining
                    )
                self._record_fill(o, t_trade, effective_size, kind="aggressive")
            if (
                not o.filled_conservative
                and o.last_price_seen_at_limit >= o.placed_at
            ):
                self._record_fill(o, t_trade, effective_size, kind="conservative")

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
                # Fix 2 audit fields
                "queue_ahead_at_placement": o.queue_ahead_at_placement,
                "queue_cleared_before_fill": o.queue_cleared_before_fill,
                "trade_aggressor_side": o.trade_aggressor_side_on_fill,
                "direction_rejections": o.direction_rejections,
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
        # Distribution of queue_ahead at placement, for measurement-phase validation.
        qa_zero = sum(1 for o in self._orders.values() if o.queue_ahead_at_placement <= EPS)
        qa_nonzero = len(self._orders) - qa_zero
        return {
            "tracked_orders": len(self._orders),
            "open_orders": open_count,
            "queue_ahead_zero_count": qa_zero,
            "queue_ahead_nonzero_count": qa_nonzero,
            "direction_rejections_total": self._direction_rejections_total,
            "trades_missing_aggressor_total": self._missing_direction_total,
        }
