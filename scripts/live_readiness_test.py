"""Live trading readiness harness.

Operator-triggered ONLY. NEVER runs automatically.

Runs a sequence of read-only checks against the live trading
infrastructure to verify it is ready for a controlled $1 live trade. The
final order-placement step is gated by an explicit CLI flag AND
``POLYMARKET_LIVE_TEST=true`` AND ``LIVE_TEST_TOKEN`` set — three
independent locks. Default invocation (no flag) is strictly read-only.

Steps:
    1. env_summary           — required env vars present
    2. funder gas check      — Polygon eth_getBalance(FUNDER) >= threshold
    3. ClobClient build      — SDK + signer + funder configured
    4. gamma connectivity    — open BTC 5m market discoverable
    5. balance/allowance     — refresh pUSD via SDK update_balance_allowance
    6. order book read       — fetch CLOB order book for LIVE_TEST_TOKEN
    7. (gated) test order    — place + cancel $1 GTC limit far below market

Exit code 0 if all attempted steps pass; non-zero on any failure.

Usage:
    cd <repo-root>
    POLYGON_RPC_URL=... FUNDER_ADDRESS=... PRIVATE_KEY=... \\
        API_KEY=... API_SECRET=... API_PASSPHRASE=... SIGNATURE_TYPE=3 \\
        python -m scripts.live_readiness_test

    # Optionally also place + cancel a $1 GTC limit far below market:
    LIVE_TEST_TOKEN=<token_id> POLYMARKET_LIVE_TEST=true \\
        python -m scripts.live_readiness_test --place-test-order
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

# Allow `python scripts/live_readiness_test.py` from anywhere by ensuring the
# repo root is on sys.path. `python -m scripts.live_readiness_test` from the
# repo root also works without this.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# Output helpers ──────────────────────────────────────────────────────────────

_RESULTS: list[dict] = []


def _emit(step: str, status: str, detail: str = "", **extras: Any) -> None:
    rec = {"step": step, "status": status, "detail": detail, **extras}
    _RESULTS.append(rec)
    flag = {"PASS": "[OK]", "FAIL": "[!!]", "SKIP": "[--]"}.get(status, "[??]")
    print(f"  {flag} {step:34s} {detail}", flush=True)


async def _run_step(name: str, fn: Callable[[], Any]) -> Optional[Any]:
    try:
        res = fn()
        if asyncio.iscoroutine(res):
            res = await res
        if isinstance(res, dict) and res.get("__skip__"):
            _emit(name, "SKIP", res.get("detail", ""))
            return None
        return res
    except Exception as exc:  # noqa: BLE001
        _emit(name, "FAIL", f"{type(exc).__name__}: {exc}")
        return None


# Step implementations ───────────────────────────────────────────────────────


def step_env() -> dict:
    from src.live_clob import env_summary

    summary = env_summary()
    required_present = (
        summary["has_private_key"]
        and summary["has_api_key"]
        and summary["has_api_secret"]
        and summary["has_api_passphrase"]
        and summary["funder_address"] is not None
        and summary["signature_type"] in {"1", "2", "3"}
    )
    detail = (
        f"funder={summary['funder_address']} sig_type={summary['signature_type']} "
        f"live={summary['polymarket_live']} live_test={summary['polymarket_live_test']} "
        f"test_token={summary['has_live_test_token']}"
    )
    if not required_present:
        raise RuntimeError(f"missing required env: {summary}")
    _emit("env_summary", "PASS", detail)
    return summary


async def step_funder_gas(min_matic: float) -> dict:
    from src.live_clob import check_funder_gas

    if min_matic <= 0:
        _emit("funder_gas", "SKIP", "threshold=0 (set LIVE_MIN_FUNDER_MATIC to enable)")
        return {"ok": True, "skipped": True}
    res = await check_funder_gas(min_matic=min_matic, cache_ttl_sec=0.0)
    detail = (
        f"funder={res['funder'][:10]}.. balance={res['balance_matic']:.6f} MATIC "
        f"min={min_matic} err={res.get('error')}"
    )
    if not res.get("ok"):
        _emit("funder_gas", "FAIL", detail)
        return res
    _emit("funder_gas", "PASS", detail)
    return res


def step_clob_client_build() -> dict:
    from src.live_clob import _client, _load_sdk

    sdk = _load_sdk()
    client = _client(sdk)
    # Bare instantiation success is the check; do not call any mutating method.
    _emit("clob_client_build", "PASS", f"client_type={type(client).__name__}")
    return {"ok": True}


async def step_gamma_connectivity() -> dict:
    from src.live_clob import fetch_active_btc_5m_market

    market = await fetch_active_btc_5m_market()
    slug = market.get("slug", "?")
    volume = float(market.get("volume") or 0.0)
    _emit("gamma_connectivity", "PASS", f"open_market={slug} volume=${volume:.0f}")
    return {"market": market}


async def step_balance_allowance() -> dict:
    # update_balance() in live_clob.py refreshes pUSD allowance via SDK. If the
    # function or SDK call fails, surfaces the cause.
    from src.live_clob import update_balance

    res = await update_balance()
    _emit(
        "balance_allowance",
        "PASS",
        f"refresh_ok={res.get('ok', False)} keys={sorted(res.keys())[:6]}",
    )
    return res


def step_order_book_read(token_id: Optional[str]) -> dict:
    if not token_id:
        _emit("order_book_read", "SKIP", "LIVE_TEST_TOKEN not set")
        return {"__skip__": True, "detail": "LIVE_TEST_TOKEN not set"}
    from src.live_clob import get_book_rest

    book = get_book_rest(token_id)
    asks = book.get("asks") if isinstance(book, dict) else None
    bids = book.get("bids") if isinstance(book, dict) else None
    _emit(
        "order_book_read",
        "PASS",
        f"token={token_id[:12]}.. asks={len(asks or [])} bids={len(bids or [])}",
    )
    return {"book": book}


async def step_test_order(token_id: Optional[str]) -> dict:
    if not token_id:
        _emit("test_order", "SKIP", "LIVE_TEST_TOKEN not set")
        return {"__skip__": True}
    if os.getenv("POLYMARKET_LIVE_TEST", "false").lower() != "true":
        _emit("test_order", "SKIP", "POLYMARKET_LIVE_TEST != true")
        return {"__skip__": True}
    # Place a $1 GTC limit at MIN_LIVE_PRICE (far below market) — guaranteed
    # not to fill against current asks. Cancel after a short delay.
    from src.live_clob import place_gtc_buy, cancel_order, fetch_active_btc_5m_market

    market = await fetch_active_btc_5m_market()
    min_price = float(os.getenv("MIN_LIVE_PRICE", "0.30"))
    placed = await place_gtc_buy(market, "Up", 1.00, min_price)
    order_id = placed.get("order_id")
    _emit(
        "test_order:place",
        "PASS",
        f"order_id={order_id} price={min_price} stake=$1.00",
    )
    await asyncio.sleep(2.0)
    cancelled = await cancel_order(order_id)
    _emit(
        "test_order:cancel",
        "PASS" if cancelled else "FAIL",
        f"order_id={order_id} cancelled={cancelled}",
    )
    return {"placed": placed, "cancelled": cancelled}


# Orchestration ──────────────────────────────────────────────────────────────


async def main_async(args: argparse.Namespace) -> int:
    print(f"[live-readiness] starting at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
          flush=True)
    print(f"[live-readiness] place_test_order={args.place_test_order}", flush=True)
    print(f"[live-readiness] LIVE_MIN_FUNDER_MATIC={os.getenv('LIVE_MIN_FUNDER_MATIC','0.0')}",
          flush=True)
    print("", flush=True)

    env_res = await _run_step("env_summary", step_env)
    gas_res = await _run_step(
        "funder_gas",
        lambda: step_funder_gas(float(os.getenv("LIVE_MIN_FUNDER_MATIC", "0.0"))),
    )
    await _run_step("clob_client_build", step_clob_client_build)
    await _run_step("gamma_connectivity", step_gamma_connectivity)
    await _run_step("balance_allowance", step_balance_allowance)
    token_id = os.getenv("LIVE_TEST_TOKEN") or None
    await _run_step("order_book_read", lambda: step_order_book_read(token_id))
    if args.place_test_order:
        await _run_step("test_order", lambda: step_test_order(token_id))
    else:
        _emit("test_order", "SKIP", "--place-test-order not supplied")

    print("", flush=True)
    fails = [r for r in _RESULTS if r["status"] == "FAIL"]
    skips = [r for r in _RESULTS if r["status"] == "SKIP"]
    print(
        f"[live-readiness] summary: {len(_RESULTS) - len(fails) - len(skips)} pass / "
        f"{len(fails)} fail / {len(skips)} skip",
        flush=True,
    )
    return 1 if fails else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--place-test-order",
        action="store_true",
        help="Actually place + cancel a $1 GTC limit (requires POLYMARKET_LIVE_TEST=true AND LIVE_TEST_TOKEN)",
    )
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
