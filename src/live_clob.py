"""
Live Polymarket CLOB execution helpers.

This module is deliberately test-first. It exposes read-only health checks and a
manual $1 FOK buy path; automated bot execution should only call this after the
manual path has been proven with production credentials and allowances.
"""

import json
import os
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Optional

import aiohttp

from .bot import GAMMA_API


CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


class LiveClobError(RuntimeError):
    pass


def _env(*names: str, required: bool = False) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    if required:
        raise LiveClobError(f"Missing required env var: {names[0]}")
    return None


def _mask(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if len(value) <= 10:
        return "***"
    return f"{value[:6]}...{value[-4:]}"


def _to_plain(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    if hasattr(value, "__dict__"):
        return _to_plain(vars(value))
    return value


def _load_sdk():
    try:
        from py_clob_client_v2 import (  # type: ignore
            ApiCreds,
            AssetType,
            BalanceAllowanceParams,
            ClobClient,
            OrderArgs,
            OrderType,
            PartialCreateOrderOptions,
            Side,
        )
    except Exception as exc:
        raise LiveClobError(
            "py_clob_client_v2 is not installed. Deploy with py_clob_client_v2 in requirements.txt."
        ) from exc
    return {
        "ApiCreds": ApiCreds,
        "AssetType": AssetType,
        "BalanceAllowanceParams": BalanceAllowanceParams,
        "ClobClient": ClobClient,
        "OrderArgs": OrderArgs,
        "OrderType": OrderType,
        "PartialCreateOrderOptions": PartialCreateOrderOptions,
        "Side": Side,
    }


def _api_creds(sdk: dict):
    return sdk["ApiCreds"](
        api_key=_env("API_KEY", "CLOB_API_KEY", required=True),
        api_secret=_env("API_SECRET", "SECRET", "CLOB_SECRET", required=True),
        api_passphrase=_env("API_PASSPHRASE", "PASSPHRASE", "CLOB_PASS_PHRASE", required=True),
    )


def _client(sdk: dict):
    funder = _env("FUNDER_ADDRESS")
    signature_type = int(_env("SIGNATURE_TYPE") or 0)
    return sdk["ClobClient"](
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=_env("PRIVATE_KEY", "PK", required=True),
        creds=_api_creds(sdk),
        signature_type=signature_type,
        funder=funder,
    )


def env_summary() -> dict:
    """Return non-secret live config status for diagnostics."""
    private_key = _env("PRIVATE_KEY", "PK")
    api_key = _env("API_KEY", "CLOB_API_KEY")
    api_secret = _env("API_SECRET", "SECRET", "CLOB_SECRET")
    api_passphrase = _env("API_PASSPHRASE", "PASSPHRASE", "CLOB_PASS_PHRASE")
    signature_type = _env("SIGNATURE_TYPE")
    return {
        "polymarket_live": os.getenv("POLYMARKET_LIVE", "false").lower() == "true",
        "polymarket_live_test": os.getenv("POLYMARKET_LIVE_TEST", "false").lower() == "true",
        "has_private_key": bool(private_key),
        "has_api_key": bool(api_key),
        "has_api_secret": bool(api_secret),
        "has_api_passphrase": bool(api_passphrase),
        "api_key": _mask(api_key),
        "funder_address": _env("FUNDER_ADDRESS"),
        "funder_required": signature_type not in {None, "0"},
        "signature_type": signature_type,
        "live_bet_size": float(os.getenv("LIVE_BET_SIZE", "1.0")),
        "max_live_price": float(os.getenv("MAX_LIVE_PRICE", "0.70")),
        "min_live_price": float(os.getenv("MIN_LIVE_PRICE", "0.30")),
        "live_order_type": os.getenv("LIVE_ORDER_TYPE", "FOK"),
        "has_live_test_token": bool(os.getenv("LIVE_TEST_TOKEN")),
    }


async def fetch_active_btc_5m_market() -> dict:
    now = int(time.time())
    candidates = [(now // 300) * 300, (now // 300 + 1) * 300]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        for start_ts in candidates:
            slug = f"btc-updown-5m-{start_ts}"
            async with session.get(f"{GAMMA_API}/markets", params={"slug": slug}) as resp:
                if resp.status != 200:
                    continue
                markets = await resp.json()
            if markets and not markets[0].get("closed", True):
                return markets[0]
    raise LiveClobError("No open BTC 5m market found")


def _json_field(raw: Any, fallback: list) -> list:
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return fallback
    return raw if isinstance(raw, list) else fallback


def token_for_side(market: dict, side: str) -> str:
    outcomes = _json_field(market.get("outcomes"), ["Up", "Down"])
    tokens = _json_field(market.get("clobTokenIds"), [])
    for idx, outcome in enumerate(outcomes):
        if str(outcome).lower() == side.lower() and idx < len(tokens):
            token_id = str(tokens[idx])
            if token_id:
                return token_id
    raise LiveClobError(f"Could not find CLOB token id for side={side}")


async def health(side: str = "Up") -> dict:
    sdk = _load_sdk()
    client = _client(sdk)
    market = await fetch_active_btc_5m_market()
    token_id = token_for_side(market, side)

    ok = client.get_ok()
    book = client.get_order_book(token_id)
    tick_size = client.get_tick_size(token_id)
    neg_risk = client.get_neg_risk(token_id)
    collateral = client.get_balance_allowance(
        sdk["BalanceAllowanceParams"](asset_type=sdk["AssetType"].COLLATERAL)
    )
    conditional = client.get_balance_allowance(
        sdk["BalanceAllowanceParams"](asset_type=sdk["AssetType"].CONDITIONAL, token_id=token_id)
    )

    return {
        "ok": True,
        "env": env_summary(),
        "clob_ok": _to_plain(ok),
        "market": {
            "slug": market.get("slug"),
            "question": market.get("question"),
            "condition_id": market.get("conditionId"),
        },
        "side": side,
        "token_id": token_id,
        "tick_size": tick_size,
        "neg_risk": bool(neg_risk),
        "orderbook": {
            "bid_count": len((book or {}).get("bids", [])) if isinstance(book, dict) else len(getattr(book, "bids", []) or []),
            "ask_count": len((book or {}).get("asks", [])) if isinstance(book, dict) else len(getattr(book, "asks", []) or []),
        },
        "balance_allowance": {
            "collateral": _to_plain(collateral),
            "conditional": _to_plain(conditional),
        },
    }


async def test_buy(side: str, amount: float) -> dict:
    if os.getenv("POLYMARKET_LIVE_TEST", "false").lower() != "true":
        raise LiveClobError("POLYMARKET_LIVE_TEST must be true for manual test orders")
    max_amount = float(os.getenv("LIVE_TEST_MAX_USDC", "1.0"))
    if amount <= 0 or amount > max_amount:
        raise LiveClobError(f"Test amount must be > 0 and <= {max_amount:.2f} USDC")
    if side not in {"Up", "Down"}:
        raise LiveClobError("side must be Up or Down")

    sdk = _load_sdk()
    client = _client(sdk)
    market = await fetch_active_btc_5m_market()
    token_id = token_for_side(market, side)
    tick_size = client.get_tick_size(token_id)
    neg_risk = client.get_neg_risk(token_id)

    order_type_name = os.getenv("LIVE_ORDER_TYPE", "FOK").upper()
    if order_type_name not in {"FOK", "FAK"}:
        raise LiveClobError("LIVE_ORDER_TYPE must be FOK or FAK for manual test buys")
    order_type = getattr(sdk["OrderType"], order_type_name)

    estimated_price = float(client.calculate_market_price(token_id, sdk["Side"].BUY, amount, order_type))
    min_price = float(os.getenv("MIN_LIVE_PRICE", "0.30"))
    max_price = float(os.getenv("MAX_LIVE_PRICE", "0.70"))
    if estimated_price < min_price or estimated_price > max_price:
        raise LiveClobError(
            f"Estimated price {estimated_price:.4f} outside [{min_price:.2f}, {max_price:.2f}]"
        )

    size = round(amount / estimated_price, 4)
    response = client.create_and_post_order(
        order_args=sdk["OrderArgs"](
            token_id=token_id,
            price=estimated_price,
            size=size,
            side=sdk["Side"].BUY,
        ),
        options=sdk["PartialCreateOrderOptions"](tick_size=tick_size, neg_risk=bool(neg_risk)),
        order_type=order_type,
    )

    return {
        "ok": True,
        "market": {
            "slug": market.get("slug"),
            "question": market.get("question"),
            "condition_id": market.get("conditionId"),
        },
        "side": side,
        "token_id": token_id,
        "amount": amount,
        "size": size,
        "order_type": order_type_name,
        "estimated_price": estimated_price,
        "tick_size": tick_size,
        "neg_risk": bool(neg_risk),
        "response": _to_plain(response),
    }
