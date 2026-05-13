"""
Live Polymarket CLOB execution helpers (V2 SDK / sig_type=3 POLY_1271).

This module is deliberately test-first. It exposes read-only health checks and a
manual $1 FOK/FAK buy path; automated bot execution should only call this after the
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
        from py_clob_client_v2.client import ClobClient  # type: ignore
        from py_clob_client_v2.clob_types import (  # type: ignore
            ApiCreds,
            AssetType,
            BalanceAllowanceParams,
            MarketOrderArgs,
            OrderArgs,
            OrderType,
            PartialCreateOrderOptions,
        )
    except Exception as exc:
        raise LiveClobError(
            "py-clob-client-v2 is not installed. Add py-clob-client-v2>=1.0.1rc1 to requirements.txt."
        ) from exc
    return {
        "ApiCreds": ApiCreds,
        "AssetType": AssetType,
        "BalanceAllowanceParams": BalanceAllowanceParams,
        "ClobClient": ClobClient,
        "MarketOrderArgs": MarketOrderArgs,
        "OrderArgs": OrderArgs,
        "OrderType": OrderType,
        "PartialCreateOrderOptions": PartialCreateOrderOptions,
        "BUY": "BUY",
        "SELL": "SELL",
    }


def _api_creds(sdk: dict):
    return sdk["ApiCreds"](
        api_key=_env("API_KEY", "CLOB_API_KEY", required=True),
        api_secret=_env("API_SECRET", "SECRET", "CLOB_SECRET", required=True),
        api_passphrase=_env("API_PASSPHRASE", "PASSPHRASE", "CLOB_PASS_PHRASE", required=True),
    )


def _client(sdk: dict):
    from eth_account import Account
    pk = _env("PRIVATE_KEY", "PK", required=True)
    pk = pk if pk.startswith("0x") else "0x" + pk
    eoa = Account.from_key(pk).address

    sig_type_raw = _env("SIGNATURE_TYPE")
    funder = _env("FUNDER_ADDRESS")
    kwargs = dict(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=pk,
        creds=_api_creds(sdk),
    )
    if sig_type_raw and sig_type_raw != "0":
        if not funder:
            raise LiveClobError(
                f"SIGNATURE_TYPE={sig_type_raw} requires FUNDER_ADDRESS to be set"
            )
        kwargs["signature_type"] = int(sig_type_raw)
        kwargs["funder"] = funder
        print(f"[identity] EOA={eoa} funder={funder} sig_type={sig_type_raw}", flush=True)
    else:
        print(f"[identity] EOA={eoa} (EOA-only mode)", flush=True)
    return sdk["ClobClient"](**kwargs)

CODE_VERSION = "v2-poly1271"


def env_summary() -> dict:
    """Return non-secret live config status for diagnostics."""
    private_key = _env("PRIVATE_KEY", "PK")
    api_key = _env("API_KEY", "CLOB_API_KEY")
    api_secret = _env("API_SECRET", "SECRET", "CLOB_SECRET")
    api_passphrase = _env("API_PASSPHRASE", "PASSPHRASE", "CLOB_PASS_PHRASE")
    signature_type = _env("SIGNATURE_TYPE")
    return {
        "code_version": CODE_VERSION,
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
        "live_order_type": os.getenv("LIVE_ORDER_TYPE", "FAK"),
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


def _book_side(book: Any, side: str) -> list:
    if isinstance(book, dict):
        return book.get(side, []) or []
    return getattr(book, side, []) or []


def _level_value(level: Any, key: str) -> Optional[float]:
    raw = level.get(key) if isinstance(level, dict) else getattr(level, key, None)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _inspect_buy_liquidity(book: Any, max_price: float) -> dict:
    asks = []
    for level in _book_side(book, "asks"):
        price = _level_value(level, "price")
        size = _level_value(level, "size")
        if price is None or size is None or size <= 0:
            continue
        asks.append({"price": price, "size": size, "usdc": price * size})

    asks.sort(key=lambda item: item["price"])
    fillable = [item for item in asks if item["price"] <= max_price]
    return {
        "ask_count": len(asks),
        "best_ask": asks[0]["price"] if asks else None,
        "fillable_ask_count": len(fillable),
        "fillable_shares": sum(item["size"] for item in fillable),
        "fillable_usdc": sum(item["usdc"] for item in fillable),
        "max_price": max_price,
    }


async def diagnose() -> dict:
    """
    Try every known signature_type and funder so we can see
    where the CLOB thinks our funds live.
    """
    sdk = _load_sdk()
    pk = _env("PRIVATE_KEY", "PK", required=True)
    pk = pk if pk.startswith("0x") else "0x" + pk
    from eth_account import Account
    eoa = Account.from_key(pk).address

    funder_env = _env("FUNDER_ADDRESS")
    sig_env = _env("SIGNATURE_TYPE")

    OLD_PROXY = "0x53Be71805A26b3e5cb9440Dc7e41D3e47868EED7"

    results = {
        "signer_eoa": eoa,
        "funder_env": funder_env,
        "sig_type_env": sig_env,
        "attempts": [],
    }

    attempts = [
        (0,  None,        "EOA only"),
        (1,  funder_env,  "POLY_PROXY with env funder"),
        (2,  funder_env,  "POLY_GNOSIS_SAFE with env funder"),
        (3,  funder_env,  "POLY_1271 with env funder"),
        (3,  OLD_PROXY,   "POLY_1271 with old proxy (has pUSD)"),
    ]

    for sig_type, funder, label in attempts:
        attempt: dict = {"label": label, "sig_type": sig_type, "funder": funder}
        try:
            kwargs = dict(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk, creds=_api_creds(sdk))
            if sig_type and sig_type != 0:
                kwargs["signature_type"] = sig_type
                if funder:
                    kwargs["funder"] = funder
            client = sdk["ClobClient"](**kwargs)
            attempt["sdk_address"] = getattr(client, "get_address", lambda: None)()
            collateral = client.get_balance_allowance(
                sdk["BalanceAllowanceParams"](asset_type=sdk["AssetType"].COLLATERAL)
            )
            attempt["collateral"] = _to_plain(collateral)
        except Exception as exc:
            attempt["error"] = f"{type(exc).__name__}: {exc}"
        results["attempts"].append(attempt)

    return results


async def place_order(
    market: dict,
    side: str,
    stake: float,
    max_fill_price: float | None = None,
) -> dict:
    """
    Place a real live buy on the CLOB for the given market/side/stake.
    Returns fill details. Raises LiveClobError if order not filled or env not set.
    Only callable when POLYMARKET_LIVE=true.
    """
    if os.getenv("POLYMARKET_LIVE", "false").lower() != "true":
        raise LiveClobError("POLYMARKET_LIVE must be true to place live orders")

    sdk = _load_sdk()
    client = _client(sdk)
    token_id = token_for_side(market, side)
    tick_size = client.get_tick_size(token_id)
    neg_risk = client.get_neg_risk(token_id)

    # CLOB requires maker amount to have at most 2 decimal places ($0.01 precision)
    stake = round(stake, 2)
    if stake < 0.01:
        raise LiveClobError(f"Stake ${stake:.4f} is below $0.01 minimum")

    order_type_name = os.getenv("LIVE_ORDER_TYPE", "FAK").upper()
    if order_type_name not in {"FOK", "FAK"}:
        raise LiveClobError("LIVE_ORDER_TYPE must be FOK or FAK for live orders")
    order_type = getattr(sdk["OrderType"], order_type_name)
    min_price = float(os.getenv("MIN_LIVE_PRICE", "0.30"))
    max_price = float(os.getenv("MAX_LIVE_PRICE", "0.70"))
    effective_max_price = min(max_price, max_fill_price) if max_fill_price is not None else max_price
    min_fill_usdc = float(os.getenv("LIVE_MIN_FILL_USDC", "1.00"))

    book = client.get_order_book(token_id)
    liquidity = _inspect_buy_liquidity(book, effective_max_price)
    if liquidity["best_ask"] is None:
        raise LiveClobError(f"No asks in order book for {side} token")
    if liquidity["best_ask"] > effective_max_price:
        raise LiveClobError(
            f"Best ask {liquidity['best_ask']:.4f} above live cap={effective_max_price:.4f}; "
            f"ask_count={liquidity['ask_count']}"
        )
    required_fill = min(stake, min_fill_usdc) if order_type_name == "FAK" else stake
    if liquidity["fillable_usdc"] + 1e-9 < required_fill:
        raise LiveClobError(
            f"Only ${liquidity['fillable_usdc']:.2f} fillable at cap={effective_max_price:.4f}; "
            f"need ${required_fill:.2f} minimum fill "
            f"(best_ask={liquidity['best_ask']:.4f}, fillable_asks={liquidity['fillable_ask_count']})"
        )

    estimated_price = float(
        client.calculate_market_price(token_id, "BUY", stake, order_type)
    )
    if estimated_price < min_price:
        raise LiveClobError(
            f"Estimated price {estimated_price:.4f} below MIN_LIVE_PRICE={min_price:.2f}"
        )
    if order_type_name == "FOK" and estimated_price > effective_max_price:
        raise LiveClobError(
            f"Estimated price {estimated_price:.4f} above live cap={effective_max_price:.2f}"
        )

    order_price = estimated_price if order_type_name == "FOK" else effective_max_price

    order = client.create_market_order(
        order_args=sdk["MarketOrderArgs"](
            token_id=token_id,
            amount=stake,
            side="BUY",
            price=order_price,
            order_type=order_type,
        ),
        options=sdk["PartialCreateOrderOptions"](tick_size=tick_size, neg_risk=bool(neg_risk)),
    )
    response = client.post_order(order, order_type)
    resp = _to_plain(response)

    if not isinstance(resp, dict):
        raise LiveClobError(f"{order_type_name} order returned unexpected response: {resp}")

    # Derive actual filled stake and fill price from taking/making amounts.
    # FAK may partially fill, so success=true alone is not enough context.
    fill_price = estimated_price
    making = resp.get("makingAmount")
    taking = resp.get("takingAmount")
    filled_stake = stake
    if making is not None and taking is not None:
        making_float = float(making)
        taking_float = float(taking)
        if taking_float > 0:
            fill_price = making_float / taking_float
        filled_stake = making_float

    success = resp.get("success") is True and filled_stake > 0
    if not success:
        raise LiveClobError(f"{order_type_name} order not filled: {resp}")

    return {
        "token_id": token_id,
        "stake": round(filled_stake, 2),
        "requested_stake": stake,
        "order_type": order_type_name,
        "estimated_price": estimated_price,
        "order_price": order_price,
        "max_fill_price": effective_max_price,
        "fill_price": fill_price,
        "orderbook": liquidity,
        "order_id": resp.get("orderID"),
        "tx_hash": (resp.get("transactionsHashes") or [None])[0],
        "response": resp,
    }


async def update_balance() -> dict:
    """Tell the CLOB to re-read on-chain pUSD balance + allowances for the funder."""
    sdk = _load_sdk()
    client = _client(sdk)

    results = {}
    for label, asset_type in [
        ("collateral",  sdk["AssetType"].COLLATERAL),
        ("conditional", sdk["AssetType"].CONDITIONAL),
    ]:
        try:
            params = sdk["BalanceAllowanceParams"](asset_type=asset_type)
            before = client.get_balance_allowance(params)
            after  = client.update_balance_allowance(params)
            results[label] = {"before": _to_plain(before), "after": _to_plain(after)}
        except Exception as exc:
            results[label] = {"error": str(exc)}

    return {"ok": True, "results": results}


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

    order_type_name = os.getenv("LIVE_ORDER_TYPE", "FAK").upper()
    if order_type_name not in {"FOK", "FAK"}:
        raise LiveClobError("LIVE_ORDER_TYPE must be FOK or FAK for manual test buys")
    order_type = getattr(sdk["OrderType"], order_type_name)

    estimated_price = float(client.calculate_market_price(token_id, sdk["BUY"], amount, order_type))
    min_price = float(os.getenv("MIN_LIVE_PRICE", "0.30"))
    max_price = float(os.getenv("MAX_LIVE_PRICE", "0.70"))
    if estimated_price < min_price or estimated_price > max_price:
        raise LiveClobError(
            f"Estimated price {estimated_price:.4f} outside [{min_price:.2f}, {max_price:.2f}]"
        )

    # Use create_market_order so makerAmount = amount exactly (preserves $0.01 precision)
    order = client.create_market_order(
        order_args=sdk["MarketOrderArgs"](
            token_id=token_id,
            amount=amount,
            side=sdk["BUY"],
            price=estimated_price,
            order_type=order_type,
        ),
        options=sdk["PartialCreateOrderOptions"](tick_size=tick_size, neg_risk=bool(neg_risk)),
    )
    response = client.post_order(order, order_type)

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
        "order_type": order_type_name,
        "estimated_price": estimated_price,
        "tick_size": tick_size,
        "neg_risk": bool(neg_risk),
        "response": _to_plain(response),
    }
