"""ExecutionRouter — entry point from the signal engine into live execution.

Picks a regime per signal, builds a MakerPlan, and hands off to a MakerExecutor
task per (slug, side). Holds dependency-injected place_fn/cancel_fn/fill_source so
maker_shadow and maker_live share one code path.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from .maker_executor import LadderRung, MakerExecutor, MakerPlan


EXEC_MODE = os.getenv("EXEC_MODE", "shadow_only").lower()

REGIME_DEAD = "DEAD"
REGIME_LATE = "LATE"
REGIME_MID = "MID"
REGIME_EARLY = "EARLY"

HARD_KILL_SEC = float(os.getenv("EXEC_HARD_KILL_SEC", "30"))
LATE_AGGRESSIVE_SEC = float(os.getenv("EXEC_LATE_AGGRESSIVE_SEC", "45"))
LATE_MAX_SEC = float(os.getenv("EXEC_LATE_MAX_SEC", "70"))
MID_MAX_SEC = float(os.getenv("EXEC_MID_MAX_SEC", "120"))

TICK = float(os.getenv("EXEC_TICK_SIZE", "0.01"))
MIN_LIVE_PRICE = float(os.getenv("MIN_LIVE_PRICE", "0.30"))
MAX_LIVE_PRICE = float(os.getenv("MAX_LIVE_PRICE", "0.70"))


@dataclass
class DispatchHandle:
    slug: str
    side: str
    task: asyncio.Task
    executor: MakerExecutor


def _regime_for(seconds_remaining: float) -> str:
    if seconds_remaining < HARD_KILL_SEC:
        return REGIME_DEAD
    if seconds_remaining < LATE_MAX_SEC:
        return REGIME_LATE
    if seconds_remaining < MID_MAX_SEC:
        return REGIME_MID
    return REGIME_EARLY


def _build_plan(
    *,
    token_id: str,
    side: str,
    stake_usdc: float,
    paper_entry_price: float,
    seconds_remaining: float,
    end_ts: float,
    diff_at_entry: Optional[float],
) -> MakerPlan:
    initial_price = round(
        max(MIN_LIVE_PRICE, min(MAX_LIVE_PRICE, paper_entry_price - TICK)),
        4,
    )
    client_id = f"sz-{int(time.time() * 1000)}-{side[:1]}"
    rung = LadderRung(
        price=initial_price,
        size_usdc=round(stake_usdc, 2),
        client_id=client_id,
        order_id=None,
    )
    diff_seed = abs(diff_at_entry) if diff_at_entry is not None else 0.0
    return MakerPlan(
        token_id=token_id,
        side=side,
        rungs=[rung],
        cancel_threshold_diff=max(8.0, 0.5 * diff_seed),
        repeg_interval_s=0.5 if seconds_remaining < LATE_MAX_SEC else 1.5,
        hard_kill_sec=HARD_KILL_SEC,
        late_aggressive_sec=LATE_AGGRESSIVE_SEC,
        end_ts=end_ts,
        diff_at_entry=diff_at_entry,
        tick=TICK,
        min_price=MIN_LIVE_PRICE,
        max_price=MAX_LIVE_PRICE,
    )


def _token_for_side(market: dict, side: str) -> str:
    from ..live_clob import token_for_side as _tfs
    return _tfs(market, side)


def _condition_id(market: dict) -> str:
    return str(
        market.get("conditionId")
        or market.get("condition_id")
        or market.get("id")
        or ""
    )


class ExecutionRouter:
    def __init__(
        self,
        *,
        book_feed,
        fill_source,
        on_fill: Callable[[dict], Awaitable[None]],
        log_attempt: Callable[..., None],
        place_fn: Optional[Callable[..., Awaitable[dict]]] = None,
        cancel_fn: Optional[Callable[[str], Awaitable[bool]]] = None,
        exec_mode: Optional[str] = None,
    ):
        self._book = book_feed
        self._fill_source = fill_source
        self._on_fill = on_fill
        self._log_attempt = log_attempt
        self._exec_mode = (exec_mode or EXEC_MODE).lower()
        if place_fn is None or cancel_fn is None:
            from .. import live_clob
            self._place_fn = place_fn or live_clob.place_gtc_buy
            self._cancel_fn = cancel_fn or live_clob.cancel_order
        else:
            self._place_fn = place_fn
            self._cancel_fn = cancel_fn
        self._active: dict[tuple, DispatchHandle] = {}
        self._lock = asyncio.Lock()

    async def dispatch(
        self,
        *,
        market: dict,
        slug: str,
        side: str,
        stake: float,
        paper_entry_price: float,
        seconds_remaining: float,
        end_ts: float,
        diff_at_entry: Optional[float],
        price_to_beat: Optional[float],
        strategy: Optional[str],
        diff_now_getter: Callable[[str], Optional[float]],
    ) -> Optional[DispatchHandle]:
        regime = _regime_for(seconds_remaining)
        key = (slug, side)
        if regime == REGIME_DEAD:
            self._log_attempt(
                slug, side, stake, paper_entry_price, paper_entry_price,
                f"DEAD regime — sec_rem={seconds_remaining:.1f}s < {HARD_KILL_SEC:.0f}s",
                seconds_remaining=seconds_remaining,
                strategy=strategy,
                diff_at_entry=diff_at_entry,
                price_to_beat=price_to_beat,
            )
            return None

        async with self._lock:
            existing = self._active.get(key)
            if existing is not None and not existing.task.done():
                return existing

            token_id = _token_for_side(market, side)
            self._book.subscribe(token_id)
            self._fill_source.subscribe(_condition_id(market))

            market["_sz_regime"] = regime
            market["_sz_diff_at_dispatch"] = diff_at_entry
            market["_sz_sec_rem_at_dispatch"] = seconds_remaining

            plan = _build_plan(
                token_id=token_id,
                side=side,
                stake_usdc=stake,
                paper_entry_price=paper_entry_price,
                seconds_remaining=seconds_remaining,
                end_ts=end_ts,
                diff_at_entry=diff_at_entry,
            )
            executor = MakerExecutor(
                market=market,
                slug=slug,
                plan=plan,
                regime=regime,
                book=self._book,
                fill_source=self._fill_source,
                diff_now=diff_now_getter,
                on_fill=self._wrap_on_fill(slug),
                on_attempt_log=self._log_attempt,
                price_to_beat=price_to_beat,
                strategy=strategy,
                place_fn=self._place_fn,
                cancel_fn=self._cancel_fn,
                exec_mode=self._exec_mode,
            )
            task = asyncio.create_task(executor.run(), name=f"maker:{slug}:{side}")
            handle = DispatchHandle(slug=slug, side=side, task=task, executor=executor)
            self._active[key] = handle
            task.add_done_callback(lambda _t: self._active.pop(key, None))
            return handle

    def _wrap_on_fill(self, slug: str) -> Callable[[dict], Awaitable[None]]:
        async def _wrapped(fill: dict) -> None:
            fill = dict(fill)
            fill.setdefault("market_slug", slug)
            await self._on_fill(fill)
        return _wrapped

    async def shutdown(self) -> None:
        async with self._lock:
            handles = list(self._active.values())
            self._active.clear()
        for h in handles:
            await h.executor.cancel_all(reason="shutdown")
            h.task.cancel()
            try:
                await h.task
            except (asyncio.CancelledError, Exception):
                pass
