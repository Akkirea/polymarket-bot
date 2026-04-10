"""
SIGNAL/ZERO — Chainlink BTC/USD price feed reader.

Reads the latest BTC/USD price from Chainlink's aggregator contract
on Polygon using web3.py.

Feed address: 0xc907E116054Ad103354f2D350FD2514433D57F6f
"""

import os
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from web3 import Web3

from . import config

# Load .env from the project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

FEED_ADDRESS = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

# Minimal AggregatorV3Interface ABI — only the functions we need
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
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ── Module-level connection cache ─────────────────────────────────────────────
# Recreating Web3 + contract on every call adds ~100–300 ms RPC overhead.
# Cache the contract instance; reconnect only if the RPC URL changes.
_cached_rpc_url: Optional[str] = None
_w3: Optional[Web3] = None
_contract = None
_decimals: Optional[int] = None


def _get_contract():
    """Return (and lazily create) a cached Chainlink contract instance."""
    global _cached_rpc_url, _w3, _contract, _decimals

    rpc_url = os.environ.get("POLYGON_RPC_URL")
    if not rpc_url:
        raise EnvironmentError("POLYGON_RPC_URL is not set")

    if _contract is None or rpc_url != _cached_rpc_url:
        _w3 = Web3(Web3.HTTPProvider(rpc_url))
        _contract = _w3.eth.contract(
            address=Web3.to_checksum_address(FEED_ADDRESS),
            abi=ABI,
        )
        _decimals = _contract.functions.decimals().call()
        _cached_rpc_url = rpc_url

    return _contract, _decimals


def get_btc_price() -> float:
    """
    Return the latest BTC/USD price from the Chainlink feed on Polygon.

    Raises:
        EnvironmentError: if POLYGON_RPC_URL is not set.
        ValueError: if the answer is non-positive or the price is stale
                    (older than CHAINLINK_MAX_STALE_SECS).
    """
    contract, decimals = _get_contract()
    _, answer, _, updated_at, _ = contract.functions.latestRoundData().call()

    if answer <= 0:
        raise ValueError(f"Chainlink returned non-positive answer: {answer}")

    age_secs = time.time() - updated_at
    if age_secs > config.CHAINLINK_MAX_STALE_SECS:
        raise ValueError(
            f"Chainlink price is stale: {age_secs:.0f}s old "
            f"(max {config.CHAINLINK_MAX_STALE_SECS}s)"
        )

    return answer / 10 ** decimals


if __name__ == "__main__":
    price = get_btc_price()
    print(f"BTC/USD (Chainlink, Polygon): ${price:,.2f}")
