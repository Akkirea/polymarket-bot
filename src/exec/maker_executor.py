"""MakerExecutor — single-rung MVP.

Resting GTC limit BUY on signal; repegs toward best_bid+tick; cancels on diff decay,
time decay, hard kill, or fill. Emits a fill event via the on_fill callback.

Dependency-injected `place_fn` / `cancel_fn` / `fill_source` so the same code runs
in maker_shadow (stubs) and maker_live (live_clob).
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional


BOOK_STALE_SEC = float(os.getenv("EXEC_BOOK_STALE_SEC", "1.5"))
REPEG_MIN_INTERVAL_SEC = float(os.getenv("EXEC_REPEG_MIN_INTERVAL_SEC", "0.25"))
CANCEL_TIMEOUT_SEC = float(os.getenv("EXEC_CANCEL_TIMEOUT_SEC", "1.5"))
PLACE_TIMEOUT_SEC = float(os.getenv("EXEC_PLACE_TIMEOUT_SEC", "3.0"))


@dataclass
class LadderRung:
    price: float
    size_usdc: float
    client_id: str
    order_id: Optional[str] = None
    placed_at: Optional[float] = None
    last_repeg_at: float = 0.0


@dataclass
class MakerPlan:
    token_id: str
    side: str
    rungs: list
    cancel_threshold_diff: float
    repeg_interval_s: float
    hard_kill_sec: float
    late_aggressive_sec: float
    end_ts: float
    diff_at_entry: Optional[float]
    tick: float
    min_price: float
    max_price: float


class MakerExecutor:
    def __init__(
        self,
        *,
        market: dict,
        slug: str,
        plan: MakerPlan,
        regime: str,
        book,
        fill_source,
        diff_now: Callable[[str], Optional[float]],
        on_fill: Callable[[dict], Awaitable[None]],
        on_attempt_log: Callable[..., None],
        price_to_beat: Optional[float],
        strategy: Optional[str],
        place_fn: Optional[Callable[..., Awaitable[dict]]] = None,
        cancel_fn: Optional[Callable[[str], Awaitable[bool]]] = None,
        exec_mode: str = "maker_live",
    ):
        self.market = market
        self.slug = slug
        self.plan = plan
        self.regime = regime
        self.book = book
        self.fill_source = fill_source
        self._diff_now = diff_now
        self._on_fill = on_fill
        self._on_attempt_log = on_attempt_log
        self._price_to_beat = price_to_beat
        self._strategy = strategy or "maker_v1"
        self._exec_mode = (exec_mode or "maker_live").lower()
        if place_fn is None and cancel_fn is None:
            from .. import live_clob
            self._place_fn = live_clob.place_gtc_buy
            self._cancel_fn = live_clob.cancel_order
        elif place_fn is not None and cancel_fn is not None:
            self._place_fn = place_fn
            self._cancel_fn = cancel_fn
        else:
            raise ValueError(
                "MakerExecutor: place_fn and cancel_fn must be provided together "
                "or not at all — partial injection would silently mix live/shadow calls"
            )
        self._closed = False
        self._fill_emitted = False
        self._diff_seed = abs(plan.diff_at_entry) if plan.diff_at_entry is not None else None
        self._reprices_count = 0

    # ── Public lifecycle ─────────────────────────────────────────────────────

    async def run(self) -> None:
        try:
            await self._place_initial_rung()
            if self._closed:
                return
            await self._watch_loop()
        except asyncio.CancelledError:
            await self.cancel_all(reason="task_cancelled")
            raise
        except Exception as exc:
            self._log_failure(f"executor_error: {exc}", self.plan.rungs[0].price)
            await self.cancel_all(reason=f"error:{exc}")
        finally:
            self._closed = True

    async def cancel_all(self, *, reason: str) -> None:
        for rung in self.plan.rungs:
            oid = rung.order_id
            if not oid:
                continue
            try:
                await asyncio.wait_for(self._cancel_fn(oid), timeout=CANCEL_TIMEOUT_SEC)
                if self._exec_mode == "maker_shadow":
                    try:
                        from .. import db_shadow
                        db_shadow.update_attempt(oid, {"cancel_reason": reason})
                    except Exception:
                        pass
                print(
                    f"[maker:{self._exec_mode}] CANCEL {self.slug} {self.plan.side} "
                    f"order_id={oid} reason={reason}",
                    flush=True,
                )
            except asyncio.TimeoutError:
                print(
                    f"[maker:{self._exec_mode}] CANCEL TIMEOUT {self.slug} "
                    f"order_id={oid} (presumed cancelled)",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"[maker:{self._exec_mode}] CANCEL ERROR {self.slug} "
                    f"order_id={oid}: {exc}",
                    flush=True,
                )
            finally:
                rung.order_id = None

    # ── Placement ────────────────────────────────────────────────────────────

    async def _place_initial_rung(self) -> None:
        rung = self.plan.rungs[0]
        target = self._target_price()
        if target is None:
            self._log_failure("no_book_top_at_dispatch", rung.price)
            self._closed = True
            return
        rung.price = target
        await self._place(rung)

    async def _place(self, rung: LadderRung) -> None:
        try:
            resp = await asyncio.wait_for(
                self._place_fn(
                    self.market,
                    self.plan.side,
                    rung.size_usdc,
                    rung.price,
                ),
                timeout=PLACE_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            self._log_failure(f"place_timeout @ {rung.price:.4f}", rung.price)
            self._closed = True
            return
        except Exception as exc:
            self._log_failure(f"place_error: {exc}", rung.price)
            self._closed = True
            return
        rung.order_id = resp["order_id"]
        rung.placed_at = time.time()
        rung.last_repeg_at = rung.placed_at
        print(
            f"[maker:{self._exec_mode}] PLACED {self.slug} {self.plan.side} "
            f"price={rung.price:.4f} stake=${rung.size_usdc:.2f} "
            f"order_id={rung.order_id} regime={self.regime}",
            flush=True,
        )

    # ── Watch loop ───────────────────────────────────────────────────────────

    async def _watch_loop(self) -> None:
        rung = self.plan.rungs[0]
        while not self._closed and rung.order_id is not None:
            timeout = self.plan.repeg_interval_s
            book_evt = self.book.wait_update(self.plan.token_id, timeout=timeout)
            fill_evt = self.fill_source.wait_fill(rung.order_id, timeout=timeout)
            done, pending = await asyncio.wait(
                {asyncio.ensure_future(book_evt), asyncio.ensure_future(fill_evt)},
                return_when=asyncio.FIRST_COMPLETED,
                timeout=timeout,
            )
            for p in pending:
                p.cancel()

            if self._handle_fill_results(done):
                return

            if self._should_kill_for_time():
                await self.cancel_all(reason="hard_kill_time")
                return

            if self._should_kill_for_diff_decay():
                await self.cancel_all(reason="diff_decay")
                return

            await self._maybe_reprice(rung)

    def _handle_fill_results(self, done_set) -> bool:
        rung = self.plan.rungs[0]
        for fut in done_set:
            try:
                result = fut.result()
            except Exception:
                continue
            if isinstance(result, dict) and result.get("kind") == "fill":
                if self._fill_emitted:
                    continue
                self._fill_emitted = True
                fill = {
                    "order_id": rung.order_id,
                    "stake": float(result.get("size_matched_usdc") or rung.size_usdc),
                    "fill_price": float(result.get("price") or rung.price),
                    "side": self.plan.side,
                    "token_id": self.plan.token_id,
                    "strategy": self._strategy,
                    "shadow": bool(result.get("shadow")),
                }
                rung.order_id = None
                self._closed = True
                asyncio.create_task(self._on_fill(fill))
                return True
        return False

    def _should_kill_for_time(self) -> bool:
        sec_rem = max(0.0, self.plan.end_ts - time.time())
        return sec_rem < self.plan.hard_kill_sec

    def _should_kill_for_diff_decay(self) -> bool:
        diff_now = self._diff_now(self.slug)
        if diff_now is None or self._diff_seed is None or self._diff_seed <= 0:
            return False
        if (self.plan.side == "Up" and diff_now < 0) or (self.plan.side == "Down" and diff_now > 0):
            return True
        if abs(diff_now) < self.plan.cancel_threshold_diff:
            return True
        return False

    # ── Repricing ────────────────────────────────────────────────────────────

    async def _maybe_reprice(self, rung: LadderRung) -> None:
        now = time.time()
        if now - rung.last_repeg_at < REPEG_MIN_INTERVAL_SEC:
            return
        target = self._target_price()
        if target is None:
            return
        if abs(target - rung.price) < self.plan.tick - 1e-9:
            return
        old_id = rung.order_id
        old_price = rung.price
        rung.last_repeg_at = now
        self._reprices_count += 1
        await self.cancel_all(reason="reprice")
        rung.price = target
        await self._place(rung)
        if self._exec_mode == "maker_shadow" and rung.order_id:
            try:
                from .. import db_shadow
                db_shadow.update_attempt(rung.order_id, {"reprices_count": self._reprices_count})
            except Exception:
                pass
        print(
            f"[maker:{self._exec_mode}] REPRICED {self.slug} {self.plan.side} "
            f"{old_price:.4f} -> {rung.price:.4f} old_id={old_id} new_id={rung.order_id}",
            flush=True,
        )

    def _target_price(self) -> Optional[float]:
        top = self.book.top(self.plan.token_id, max_age_sec=BOOK_STALE_SEC)
        if top is None or top.bid_price is None:
            return None
        sec_rem = max(0.0, self.plan.end_ts - time.time())
        if self.regime == "LATE" and sec_rem < self.plan.late_aggressive_sec:
            target = min(
                top.ask_price if top.ask_price is not None else top.bid_price + self.plan.tick,
                top.bid_price + self.plan.tick,
            )
        else:
            target = top.bid_price + self.plan.tick if top.bid_price is not None else None
        if target is None:
            return None
        if top.ask_price is not None and target >= top.ask_price:
            target = top.ask_price - self.plan.tick
        target = max(self.plan.min_price, min(self.plan.max_price, target))
        return round(target, 4)

    # ── Failure logging ──────────────────────────────────────────────────────

    def _log_failure(self, reason: str, cap_price: float) -> None:
        try:
            self._on_attempt_log(
                self.slug,
                self.plan.side,
                self.plan.rungs[0].size_usdc,
                cap_price,
                cap_price,
                reason,
                seconds_remaining=max(0.0, self.plan.end_ts - time.time()),
                strategy=self._strategy,
                diff_at_entry=self.plan.diff_at_entry,
                price_to_beat=self._price_to_beat,
            )
        except Exception as exc:
            print(f"[maker:{self._exec_mode}] failure-log error for {self.slug}: {exc}", flush=True)
