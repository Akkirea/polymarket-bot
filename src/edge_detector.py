"""
SIGNAL/ZERO Phase 1 — Edge Detector

Compares our momentum-based probability estimate against Polymarket's
implied probability. When they diverge beyond a threshold, that's a
potential edge — the market is mispricing the outcome relative to
what current price action suggests.

This is the core hypothesis being tested in Phase 1:
  "Can real-time price momentum predict 5-min BTC direction
   better than the Polymarket crowd, after fees?"

If the answer is no (likely for most thresholds), the data still
tells us something about how these markets behave — which informs
the strategy for Phase 2.
"""

from dataclasses import dataclass
from typing import Optional

from . import config


@dataclass
class Signal:
    """A detected divergence between momentum and market odds."""
    market_id: str
    btc_price: float
    momentum: float          # our estimated P(up), 0-1
    implied_up_prob: float   # market's implied P(up), 0-1
    divergence: float        # momentum - implied (signed)
    abs_divergence: float    # |divergence|
    direction: str           # "up" or "down" — which side to bet
    confidence: str          # "low", "medium", "high"
    fee_estimate: float      # estimated Polymarket fee


def estimate_fee(price: float) -> float:
    """
    Estimate Polymarket taker fee for 5-min markets.

    Fee = C * 0.25 * (p * (1 - p))^2
    where C is a constant and p is the share price.

    Fee is highest at p=0.5 (max uncertainty) and drops
    toward 0 or 1 (high certainty).
    """
    c = config.EST_FEE_CONSTANT
    p = max(0.01, min(0.99, price))
    fee = c * 0.25 * (p * (1 - p)) ** 2
    return fee


def detect_edge(
    momentum: float,
    market_up_price: float,
    market_down_price: float,
    btc_price: float,
    market_id: str = "",
) -> Optional[Signal]:
    """
    Compare momentum estimate to market-implied probability.

    Returns a Signal if divergence exceeds threshold, None otherwise.

    Args:
        momentum: Our estimated P(BTC up) from price history, 0-1
        market_up_price: Current price of "Up" shares on Polymarket
        market_down_price: Current price of "Down" shares on Polymarket
        btc_price: Current BTC/USDT price
        market_id: Polymarket market identifier
    """
    if momentum is None:
        return None

    # Market-implied probability of "Up"
    # Prices should sum to ~1.0; normalize if they don't
    total = market_up_price + market_down_price
    if total <= 0:
        return None

    implied_up = market_up_price / total

    # Divergence: positive = we think Up is more likely than market does
    divergence = momentum - implied_up

    # Check if divergence exceeds threshold
    if abs(divergence) < config.EDGE_THRESHOLD:
        return None

    # Determine which side to bet
    if divergence > 0:
        # We think Up is underpriced
        direction = "up"
        entry_price = market_up_price
    else:
        # We think Down is underpriced
        direction = "down"
        entry_price = market_down_price

    # Estimate fee
    fee = estimate_fee(entry_price)

    # Check if edge survives fees
    net_edge = abs(divergence) - fee
    if net_edge <= 0:
        # Edge exists but doesn't survive fees — log but don't trade
        confidence = "low"
    elif net_edge < 0.05:
        confidence = "medium"
    else:
        confidence = "high"

    return Signal(
        market_id=market_id,
        btc_price=btc_price,
        momentum=momentum,
        implied_up_prob=implied_up,
        divergence=divergence,
        abs_divergence=abs(divergence),
        direction=direction,
        confidence=confidence,
        fee_estimate=fee,
    )
