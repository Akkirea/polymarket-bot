"""Shadow stubs replacing live_clob.place_gtc_buy / cancel_order under EXEC_MODE=maker_shadow.

These NEVER call the SDK. They register/cancel against a process-global ShadowFillSimulator.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from .. import db_shadow
from .shadow_sim import ShadowFillSimulator


_SIM: Optional[ShadowFillSimulator] = None


def set_simulator(sim: ShadowFillSimulator) -> None:
    global _SIM
    _SIM = sim


def get_simulator() -> ShadowFillSimulator:
    if _SIM is None:
        raise RuntimeError("shadow simulator not initialised — call set_simulator() first")
    return _SIM


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


async def place_gtc_buy_shadow(
    market: dict,
    side: str,
    stake_usdc: float,
    limit_price: float,
) -> dict:
    sim = get_simulator()
    order_id = f"shadow-{uuid.uuid4().hex[:12]}"
    token_id = _token_for_side(market, side)
    price = round(float(limit_price), 4)
    stake = round(float(stake_usdc), 2)
    shares = round(stake / price, 4) if price > 0 else 0.0

    sim.register(
        order_id=order_id,
        token_id=token_id,
        side=side,
        limit_price=price,
        size_usdc=stake,
        placed_at=time.time(),
    )

    try:
        db_shadow.insert_attempt({
            "order_id": order_id,
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
            "market_slug": market.get("slug"),
            "condition_id": _condition_id(market),
            "token_id": token_id,
            "side": side,
            "regime": market.get("_sz_regime"),
            "intended_initial_price": price,
            "intended_size_usdc": stake,
            "diff_at_dispatch": market.get("_sz_diff_at_dispatch"),
            "sec_rem_at_dispatch": market.get("_sz_sec_rem_at_dispatch"),
            "exec_mode": os.getenv("EXEC_MODE", "maker_shadow"),
        })
    except Exception as exc:
        print(f"[shadow-clob] db insert failed for {order_id}: {exc}", flush=True)

    return {
        "order_id": order_id,
        "token_id": token_id,
        "side": side,
        "limit_price": price,
        "stake_usdc": stake,
        "shares": shares,
        "response": {"shadow": True},
        "shadow": True,
    }


async def cancel_order_shadow(order_id: str) -> bool:
    sim = get_simulator()
    sim.cancel(order_id, reason="caller_cancel")
    return True
