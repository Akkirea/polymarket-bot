"""
SIGNAL/ZERO — Chainlink BTC/USD price feed reader.

Two interfaces:
  - Sync (web3.py): get_btc_price() — latest price, used by legacy callers
  - Async (aiohttp raw JSON-RPC): get_price_at_ts() — historical round lookup,
    used by the bot to get the exact priceToBeat at market open

Feed address: 0xc907E116054Ad103354f2D350FD2514433D57F6f (BTC/USD, Polygon)
Decimals: 8
"""

import os
import time
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from web3 import Web3

from . import config

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

FEED_ADDRESS = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
_DECIMALS    = 8
_MAX_WALK    = 30   # max rounds to walk back; Chainlink BTC/USD ~1-5 min/round

# Raw function selectors (no ABI needed for async path)
_SEL_LATEST    = "0xfeaf968c"   # latestRoundData()
_SEL_GET_ROUND = "0x9a6fc8f5"   # getRoundData(uint80)

# ── Sync path (web3.py) ───────────────────────────────────────────────────────

ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId",         "type": "uint80"},
            {"name": "answer",          "type": "int256"},
            {"name": "startedAt",       "type": "uint256"},
            {"name": "updatedAt",       "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"name": "roundId",         "type": "uint80"},
            {"name": "answer",          "type": "int256"},
            {"name": "startedAt",       "type": "uint256"},
            {"name": "updatedAt",       "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

_cached_rpc_url: Optional[str] = None
_w3 = None
_contract      = None
_decimals_cache: Optional[int] = None


def _get_contract():
    global _cached_rpc_url, _w3, _contract, _decimals_cache
    rpc_url = os.environ.get("POLYGON_RPC_URL")
    if not rpc_url:
        raise EnvironmentError("POLYGON_RPC_URL is not set")
    if _contract is None or rpc_url != _cached_rpc_url:
        _w3 = Web3(Web3.HTTPProvider(rpc_url))
        _contract = _w3.eth.contract(
            address=Web3.to_checksum_address(FEED_ADDRESS), abi=ABI
        )
        _decimals_cache = _contract.functions.decimals().call()
        _cached_rpc_url = rpc_url
    return _contract, _decimals_cache


def get_btc_price() -> float:
    """Latest Chainlink BTC/USD price (sync). Raises on stale data."""
    contract, decimals = _get_contract()
    _, answer, _, updated_at, _ = contract.functions.latestRoundData().call()
    if answer <= 0:
        raise ValueError(f"Chainlink returned non-positive answer: {answer}")
    age_secs = time.time() - updated_at
    if age_secs > config.CHAINLINK_MAX_STALE_SECS:
        raise ValueError(f"Chainlink price is stale: {age_secs:.0f}s old")
    return answer / 10 ** decimals


def get_price_at_ts_sync(target_ts: float) -> Optional[tuple]:
    """
    Sync version of get_price_at_ts — for scripts and validation.
    Returns (price_usd, round_id, started_at) or None.
    """
    contract, _ = _get_contract()

    rd_id, answer, started_at, _, _ = contract.functions.latestRoundData().call()
    if answer / 10 ** _DECIMALS > 0 and started_at <= target_ts:
        return answer / 10 ** _DECIMALS, rd_id, started_at

    phase_id  = rd_id >> 64
    agg_round = rd_id & 0xFFFF_FFFF_FFFF_FFFF

    for _ in range(_MAX_WALK):
        agg_round -= 1
        if agg_round <= 0:
            break
        prev_id = (phase_id << 64) | agg_round
        try:
            _, ans, s_at, _, _ = contract.functions.getRoundData(prev_id).call()
        except Exception:
            break
        if s_at <= target_ts:
            return ans / 10 ** _DECIMALS, prev_id, s_at

    return None


# ── Async path (aiohttp raw JSON-RPC) ────────────────────────────────────────

def _decode_round(hex_result: str) -> Optional[tuple]:
    """Decode latestRoundData / getRoundData hex response."""
    if not hex_result or len(hex_result) < 2 + 5 * 64:
        return None
    d          = hex_result[2:]
    round_id   = int(d[0:64],    16)
    answer_raw = int(d[64:128],  16)
    started_at = int(d[128:192], 16)
    updated_at = int(d[192:256], 16)
    if answer_raw >= 2 ** 255:
        answer_raw -= 2 ** 256
    return round_id, answer_raw / 10 ** _DECIMALS, started_at, updated_at


async def get_price_at_ts(
    target_ts: float,
    session: aiohttp.ClientSession,
    rpc_url: Optional[str] = None,
) -> Optional[tuple]:
    """
    Async. Return (price_usd, round_id, started_at) for the Chainlink round
    whose startedAt <= target_ts — the round active when a market opened.

    Walks back from latestRoundData up to _MAX_WALK rounds.
    Returns None on any failure.
    """
    rpc = rpc_url or os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

    async def eth_call(data: str) -> Optional[str]:
        payload = {
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": FEED_ADDRESS, "data": data}, "latest"],
            "id": 1,
        }
        try:
            async with session.post(
                rpc, json=payload, timeout=aiohttp.ClientTimeout(total=6)
            ) as resp:
                result = (await resp.json()).get("result", "0x")
                return result if result and result != "0x" else None
        except Exception:
            return None

    latest_hex = await eth_call(_SEL_LATEST)
    if not latest_hex:
        return None
    decoded = _decode_round(latest_hex)
    if not decoded:
        return None

    round_id, price, started_at, _ = decoded
    if started_at <= target_ts:
        return price, round_id, started_at

    phase_id  = round_id >> 64
    agg_round = round_id & 0xFFFF_FFFF_FFFF_FFFF

    for _ in range(_MAX_WALK):
        agg_round -= 1
        if agg_round <= 0:
            break
        prev_id  = (phase_id << 64) | agg_round
        data     = _SEL_GET_ROUND + format(prev_id, "064x")
        hex_res  = await eth_call(data)
        if not hex_res:
            break
        decoded = _decode_round(hex_res)
        if not decoded:
            break
        _, price, started_at, _ = decoded
        if started_at <= target_ts:
            return price, prev_id, started_at

    return None


if __name__ == "__main__":
    price = get_btc_price()
    print(f"BTC/USD (Chainlink, Polygon): ${price:,.2f}")
