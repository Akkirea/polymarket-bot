"""
SIGNAL/ZERO — Timing-based signal bot.

Monitors active BTC 5-minute markets and enters a $500 paper position
in the last 5-12 seconds before close when momentum, funding rate,
and market price range all agree on direction.

All paper only — no real money.

Usage (started automatically by api.py endpoints):
    POST http://localhost:8000/api/bot/start
    POST http://localhost:8000/api/bot/stop
    GET  http://localhost:8000/api/bot/status
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from . import db

# ── Constants ──────────────────────────────────────────────────────────────────
GAMMA_API    = "https://gamma-api.polymarket.com"
COINGECKO    = "https://api.coingecko.com/api/v3"

INITIAL_BALANCE  = 10_000.0
BET_SIZE         = 500.0
POLL_INTERVAL    = 3     # seconds between ticks
ENTRY_WINDOW_LO  = 5     # enter when seconds_remaining >= this
ENTRY_WINDOW_HI  = 12    # enter when seconds_remaining <= this
FUNDING_THRESHOLD = 0.02  # % — above = bullish, below negative = bearish

STRATEGY_TAG = "SIGNAL_STRATEGY"  # stored in whale_address column (NOT NULL)


# ── PaperBot ───────────────────────────────────────────────────────────────────
class PaperBot:
    """
    Enters BTC 5-min markets in the last 5-12 s before close when
    momentum + funding rate agree and market price is in the 0.40-0.70 range.
    """

    def __init__(self):
        self.balance:     float          = INITIAL_BALANCE
        self.position:    Optional[dict] = None
        self.wins:        int            = 0
        self.losses:      int            = 0
        self.total_pnl:   float          = 0.0
        self.running:     bool           = False
        self._task:       Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        self._entered_slugs: set = set()  # avoid re-entering the same market
        db.init_db()

    # ── Session ────────────────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self):
        """Launch the background loop. Must be called from an async context."""
        if self.running:
            return
        self.running = True
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._loop())
        print("[bot] Started")

    async def stop(self):
        """Cancel the background loop and close the HTTP session."""
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._session and not self._session.closed:
            await self._session.close()
        print("[bot] Stopped")

    def get_status(self) -> dict:
        total = self.wins + self.losses
        return {
            "running":          self.running,
            "balance":          round(self.balance, 2),
            "total_trades":     total,
            "wins":             self.wins,
            "losses":           self.losses,
            "win_rate":         round(self.wins / total, 4) if total else 0.0,
            "pnl":              round(self.total_pnl, 2),
            "current_position": self.position,
        }

    def get_recent_trades(self, n: int = 5) -> list:
        conn = db.get_connection()
        rows = conn.execute(
            "SELECT * FROM bot_trades ORDER BY opened_at DESC LIMIT %s", (n,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def _loop(self):
        while self.running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[bot] Tick error: {exc}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _tick(self):
        # If holding a position, try to resolve once the market window has closed
        if self.position:
            end_ts = self.position.get("end_ts", 0)
            if time.time() >= end_ts:
                await self._try_resolve()
            return  # one position at a time

        # Scan active BTC 5m markets for an entry window
        markets = await self._fetch_active_markets()
        now = time.time()

        for market in markets:
            slug = market.get("slug", "")
            if not slug or slug in self._entered_slugs:
                continue

            end_ts = _extract_end_ts(slug)
            if end_ts is None:
                continue

            seconds_remaining = end_ts - now
            if not (ENTRY_WINDOW_LO <= seconds_remaining <= ENTRY_WINDOW_HI):
                continue

            print(f"[bot] Entry window: {slug}  {seconds_remaining:.1f}s remaining — evaluating signals")
            direction, signals = await self._evaluate_signals(market)

            # Mark slug as seen regardless of outcome so we don't re-evaluate
            self._entered_slugs.add(slug)

            if direction is None:
                print(f"[bot] SKIP: signals disagree — {signals}")
                continue

            entry_price = _side_price(market, direction)
            print(
                f"[bot] SIGNAL ENTRY: {direction} on {slug} — "
                f"momentum={signals['momentum']} funding={signals['funding']:.4f} "
                f"price={entry_price:.3f}"
            )
            await self._open_position(slug, direction, entry_price, end_ts)
            break  # one position at a time

    # ── Active markets ─────────────────────────────────────────────────────────

    async def _fetch_active_markets(self) -> list:
        session = await self._get_session()
        try:
            async with session.get(
                f"{GAMMA_API}/markets",
                params={"slug": "btc-updown-5m", "active": "true", "limit": 20},
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
        except Exception as exc:
            print(f"[bot] Failed to fetch markets: {exc}")
            return []

        # Guard: keep only BTC 5m slugs in case the API returns unrelated markets
        return [m for m in data if "btc-updown-5m" in (m.get("slug") or "")]

    # ── Signals ────────────────────────────────────────────────────────────────

    async def _evaluate_signals(self, market: dict) -> tuple[Optional[str], dict]:
        """
        Returns (direction, signals_dict).
        direction is 'Up', 'Down', or None if signals don't agree.
        """
        momentum = await self._signal_momentum()
        funding  = await self._signal_funding()

        signals = {"momentum": momentum, "funding": funding}

        # Both must agree and neither can be None/neutral
        if momentum is None or funding == "neutral":
            return None, signals
        if momentum != funding:
            return None, signals

        direction = momentum

        # Price range filter: our target side must be priced 0.40-0.70
        price = _side_price(market, direction)
        signals["price"] = price
        if not (0.40 <= price <= 0.70):
            print(f"[bot] SKIP: {direction} price {price:.3f} outside 0.40-0.70 range")
            return None, signals

        return direction, signals

    async def _signal_momentum(self) -> Optional[str]:
        """
        Fetch BTC 1-day minute chart from CoinGecko.
        Take the last 3 candles (4 price points → 3 moves).
        2 of 3 moves up → 'Up', 2 of 3 down → 'Down', else None.
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{COINGECKO}/coins/bitcoin/market_chart",
                params={"vs_currency": "usd", "days": 1, "interval": "minute"},
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        except Exception as exc:
            print(f"[bot] Momentum fetch error: {exc}")
            return None

        prices = [p[1] for p in data.get("prices", [])]
        if len(prices) < 4:
            return None

        last4 = prices[-4:]
        moves = ["Up" if last4[i + 1] > last4[i] else "Down" for i in range(3)]

        ups   = moves.count("Up")
        downs = moves.count("Down")

        if ups >= 2:
            return "Up"
        if downs >= 2:
            return "Down"
        return None

    async def _signal_funding(self) -> str:
        """
        Fetch BTC funding rate from CoinGecko derivatives.
        Rate > 0.02% → 'Up' (bullish), < -0.02% → 'Down' (bearish), else 'neutral'.
        Returns the rate direction string, not a float.
        """
        session = await self._get_session()
        try:
            async with session.get(f"{COINGECKO}/derivatives") as resp:
                if resp.status != 200:
                    return "neutral"
                data = await resp.json()
        except Exception as exc:
            print(f"[bot] Funding fetch error: {exc}")
            return "neutral"

        btc = [d for d in data if d.get("base") == "BTC" and d.get("funding_rate") is not None]
        if not btc:
            return "neutral"

        # Use the contract with the largest open interest as the representative rate
        btc.sort(key=lambda d: float(d.get("open_interest_usd") or 0), reverse=True)
        rate = float(btc[0]["funding_rate"]) * 100  # convert to %

        if rate > FUNDING_THRESHOLD:
            return "Up"
        if rate < -FUNDING_THRESHOLD:
            return "Down"
        return "neutral"

    # ── Position lifecycle ─────────────────────────────────────────────────────

    async def _open_position(self, slug: str, side: str, entry_price: float, end_ts: float):
        if self.balance < BET_SIZE:
            print(f"[bot] Insufficient balance (${self.balance:.2f}) — skipping")
            return

        self.balance -= BET_SIZE
        self.position = {
            "market_slug": slug,
            "side":        side,
            "size":        BET_SIZE,
            "entry_price": entry_price,
            "end_ts":      end_ts,
            "opened_at":   datetime.now(timezone.utc).isoformat(),
        }
        print(
            f"[bot] OPEN  {side:>4}  ${BET_SIZE:.0f}  {slug}"
            f"  entry={entry_price:.3f}  balance=${self.balance:.2f}"
        )

    async def _try_resolve(self):
        slug = self.position["market_slug"]
        winner = await self._resolve_market(slug)
        if winner is None:
            print(f"[bot] {slug} not resolved yet — will retry")
            return
        await self._close_position(winner)

    async def _resolve_market(self, slug: str) -> Optional[str]:
        """Return 'Up' or 'Down' when outcomePrices reaches 0.99, else None."""
        session = await self._get_session()
        try:
            async with session.get(
                f"{GAMMA_API}/markets", params={"slug": slug}
            ) as resp:
                if resp.status != 200:
                    return None
                markets = await resp.json()
        except Exception:
            return None

        if not markets:
            return None

        m = markets[0]
        try:
            outcomes_raw = m.get("outcomes",      '["Up","Down"]')
            prices_raw   = m.get("outcomePrices", "[0.5,0.5]")
            outcomes = eval(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            prices   = eval(prices_raw)   if isinstance(prices_raw,   str) else prices_raw
        except Exception:
            return None

        print(f"[bot] outcomePrices for {slug}: {list(zip(outcomes, prices))}")
        for i, p in enumerate(prices):
            if float(p) >= 0.99:
                return str(outcomes[i])
        return None

    async def _close_position(self, winner: str):
        pos         = self.position
        won         = pos["side"] == winner
        size        = pos["size"]
        entry_price = pos.get("entry_price", 0.5)
        if entry_price <= 0:
            entry_price = 0.5

        if won:
            pnl = size * (1.0 / entry_price - 1.0)
            self.balance += size + pnl
            self.wins += 1
        else:
            pnl = -size
            self.losses += 1

        self.total_pnl += pnl
        closed_at = datetime.now(timezone.utc).isoformat()

        print(
            f"[bot] CLOSE {pos['side']:>4}  winner={winner}"
            f"  pnl=${pnl:+.2f}  balance=${self.balance:.2f}"
        )

        conn = db.get_connection()
        conn.execute(
            """INSERT INTO bot_trades
               (whale_address, market_slug, side, size, entry_price,
                outcome, pnl, opened_at, closed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                STRATEGY_TAG,
                pos["market_slug"],
                pos["side"],
                pos["size"],
                round(entry_price, 4),
                winner,
                round(pnl, 2),
                pos["opened_at"],
                closed_at,
            ),
        )
        conn.commit()
        conn.close()
        self.position = None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_end_ts(slug: str) -> Optional[float]:
    """Extract Unix end timestamp from 'btc-updown-5m-1774383000' → 1774383000.0"""
    try:
        return float(slug.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return None


def _side_price(market: dict, side: str) -> float:
    """
    Return the current market price for 'Up' or 'Down'.
    outcomePrices and outcomes come back as JSON strings from the Gamma API.
    """
    try:
        outcomes_raw = market.get("outcomes",      '["Up","Down"]')
        prices_raw   = market.get("outcomePrices", "[0.5,0.5]")
        outcomes = eval(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        prices   = eval(prices_raw)   if isinstance(prices_raw,   str) else prices_raw
        return float(prices[outcomes.index(side)])
    except Exception:
        return 0.5


# ── Module-level singleton used by api.py ─────────────────────────────────────
bot = PaperBot()
