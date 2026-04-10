"""
SIGNAL/ZERO Phase 1 — Paper Trader

Simulates trade execution against detected signals.
Tracks P&L including estimated fees. Enforces risk limits.

Every trade is a binary bet:
  - Pay entry_price per share (e.g., $0.45)
  - If you win: receive $1.00 per share → profit = 1 - entry_price - fee
  - If you lose: receive $0.00 → loss = entry_price + fee
"""

import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from . import config
from .edge_detector import Signal
from . import db


@dataclass
class PaperPosition:
    """A simulated open position."""
    market_id: str
    condition_id: str
    side: str              # "up" or "down"
    entry_price: float     # price paid per share
    shares: float          # number of shares (size / entry_price)
    size: float            # total USDC deployed
    fee: float             # estimated fee
    opened_at: str
    signal_divergence: float
    price_to_beat: float = 0.0   # BTC price at window open (resolution reference)
    end_ts: float = 0.0          # unix timestamp when the 5-min window closes


@dataclass
class PaperTrader:
    """Manages paper trading simulation with risk controls."""

    balance: float = config.INITIAL_BANKROLL
    positions: list = field(default_factory=list)
    trade_count: int = 0
    win_count: int = 0
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    _last_reset_date: str = ""

    def can_trade(self) -> bool:
        """Check if we're allowed to open a new position."""
        # Reset daily P&L at midnight UTC
        today = datetime.now(timezone.utc).date().isoformat()
        if today != self._last_reset_date:
            self.daily_pnl = 0.0
            self._last_reset_date = today

        if len(self.positions) >= config.MAX_CONCURRENT_BETS:
            return False

        if self.balance < config.PAPER_BET_SIZE:
            return False

        if self.daily_pnl <= -config.MAX_DAILY_LOSS:
            return False

        return True

    def open_position(self, signal: Signal, market: dict) -> Optional[PaperPosition]:
        """
        Open a simulated position based on a detected signal.
        Returns the position if opened, None if blocked by risk controls.
        """
        if not self.can_trade():
            return None

        # Only trade medium+ confidence signals
        if signal.confidence == "low":
            return None

        size = config.PAPER_BET_SIZE

        # Entry price is the current share price for our side
        if signal.direction == "up":
            entry_price = market["up_price"]
        else:
            entry_price = market["down_price"]

        # Sanity check: don't buy shares priced above 0.95 or below 0.05
        if entry_price > 0.95 or entry_price < 0.05:
            return None

        shares = size / entry_price
        fee = signal.fee_estimate * size

        # Parse window expiry from market end_date (ISO string from Polymarket)
        end_date_str = market.get("end_date", "")
        end_ts = 0.0
        if end_date_str:
            try:
                end_ts = datetime.fromisoformat(
                    end_date_str.replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                end_ts = _time.time() + 300  # fallback: 5 min from now
        if end_ts == 0.0:
            end_ts = _time.time() + 300

        pos = PaperPosition(
            market_id=market.get("id", ""),
            condition_id=market.get("condition_id", ""),
            side=signal.direction,
            entry_price=entry_price,
            shares=shares,
            size=size,
            fee=fee,
            opened_at=datetime.now(timezone.utc).isoformat(),
            signal_divergence=signal.divergence,
            price_to_beat=signal.btc_price,
            end_ts=end_ts,
        )

        self.balance -= size
        self.positions.append(pos)

        return pos

    def get_expired_positions(self, now_ts: float = None) -> list:
        """Return positions whose 5-min window has expired."""
        if now_ts is None:
            now_ts = _time.time()
        return [p for p in self.positions if p.end_ts > 0 and now_ts >= p.end_ts]

    def resolve_position(self, pos: PaperPosition, outcome: str) -> dict:
        """
        Resolve a position when the market settles.

        Args:
            pos: The open position
            outcome: "up" or "down" — the actual market result

        Returns:
            Trade record dict with P&L
        """
        won = pos.side == outcome

        if won:
            # Receive $1.00 per share, minus fees
            payout = pos.shares * 1.0
            pnl = payout - pos.size - pos.fee
        else:
            # Shares are worthless
            payout = 0.0
            pnl = -pos.size - pos.fee

        self.balance += pos.size + pnl  # return capital + P&L
        self.total_pnl += pnl
        self.daily_pnl += pnl
        self.trade_count += 1
        if won:
            self.win_count += 1

        # Remove this specific position by identity
        self.positions = [p for p in self.positions if p is not pos]

        trade_record = {
            "opened_at": pos.opened_at,
            "closed_at": datetime.now(timezone.utc).isoformat(),
            "market_id": pos.market_id,
            "condition_id": pos.condition_id,
            "side": pos.side,
            "entry_price": pos.entry_price,
            "size": pos.size,
            "fee": pos.fee,
            "outcome": outcome,
            "pnl": pnl,
            "balance_after": self.balance,
        }

        # Persist to database
        db.log_paper_trade(trade_record)

        return trade_record

    @property
    def win_rate(self) -> float:
        if self.trade_count == 0:
            return 0.0
        return self.win_count / self.trade_count

    @property
    def total_value(self) -> float:
        """Balance + value of open positions (at entry price)."""
        open_value = sum(p.size for p in self.positions)
        return self.balance + open_value

    def summary(self) -> dict:
        return {
            "balance": self.balance,
            "open_positions": len(self.positions),
            "total_trades": self.trade_count,
            "win_rate": self.win_rate,
            "total_pnl": self.total_pnl,
            "daily_pnl": self.daily_pnl,
            "total_value": self.total_value,
        }
