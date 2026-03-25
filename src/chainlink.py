"""
SIGNAL/ZERO — Chainlink BTC/USD price feed reader.

Reads the latest BTC/USD price from Chainlink's aggregator contract
on Polygon using web3.py.

Feed address: 0xc907E116054Ad103354f2D350FD2514433D57F6f
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# Load .env from the project root (two levels up from this file: src/chainlink.py → src/ → project/)
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


def get_btc_price() -> float:
    """
    Return the latest BTC/USD price from the Chainlink feed on Polygon.
    Raises if POLYGON_RPC_URL is not set or the call fails.
    """
    rpc_url = os.environ.get("POLYGON_RPC_URL")
    if not rpc_url:
        raise EnvironmentError("POLYGON_RPC_URL is not set")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    feed = w3.eth.contract(
        address=Web3.to_checksum_address(FEED_ADDRESS),
        abi=ABI,
    )

    decimals = feed.functions.decimals().call()
    _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()

    if answer <= 0:
        raise ValueError(f"Chainlink returned non-positive answer: {answer}")

    return answer / 10 ** decimals


if __name__ == "__main__":
    price = get_btc_price()
    print(f"BTC/USD (Chainlink, Polygon): ${price:,.2f}")
