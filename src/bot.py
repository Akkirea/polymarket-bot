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
from .chainlink import get_btc_price

# ── Constants ──────────────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
BYBIT_API = "https://api.bybit.com/v5/market"

INITIAL_BALANCE      = db.INITIAL_BALANCE  # keep in sync with db.py
BET_SIZE             = 500.0
POLL_INTERVAL        = 3     # seconds between ticks
ENTRY_WINDOW_LO      = 2     # enter when seconds_remaining >= this
ENTRY_WINDOW_HI      = 15    # enter when seconds_remaining <= this
FUNDING_THRESHOLD    = 0.02  # % — above = bullish, below negative = bearish
PRICE_DIFF_THRESHOLD = 10.0  # USD — minimum chainlink vs start-price diff to act

STRATEGY_TAG = "SIGNAL_STRATEGY"  # stored in whale_address column (NOT NULL)


# ── PaperBot ───────────────────────────────────────────────────────────────────
class PaperBot:
    """
    Enters BTC 5-min markets in the last 5-12 s before close when
    momentum + funding rate agree and market price is in the 0.40-0.70 range.
    """

    def __init__(self):
        db.init_db()
        state = db.load_bot_state()
        self.balance:     float          = state["balance"]
        self.position:    Optional[dict] = None
        self.wins:        int            = 0
        self.losses:      int            = 0
        self.total_pnl:   float          = 0.0
        self.running:     bool           = False
        self._task:       Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        self._entered_slugs: set = set()         # avoid re-entering the same market
        self._market_start_prices: dict = {}    # slug → chainlink price at market open
        print(f"[bot] Loaded balance from DB: ${self.balance:.2f}")

    # ── Session ────────────────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10, connect=5)
            )
        return self._session

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self):
        """Launch the background loop. Must be called from an async context."""
        if self.running:
            return
        self.running = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop())
        print("[bot] Started — task created")

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
        print("[bot] loop starting")
        while self.running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[bot] Tick error: {exc}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _tick(self):
        print("[bot] _tick called")
        # If holding a position, try to resolve once the market window has closed
        if self.position:
            end_ts = self.position.get("end_ts", 0)
            if time.time() >= end_ts:
                await self._try_resolve()
            return  # one position at a time

        # Scan active BTC 5m markets for an entry window
        markets = await self._fetch_active_markets()
        now = time.time()

        if not markets:
            print("[bot] no active markets found")

        print(f"[bot] found {len(markets)} active BTC 5m markets")

        for market in markets:
            slug = market.get("slug", "")
            if not slug or slug in self._entered_slugs:
                continue

            end_ts = _extract_end_ts(slug)
            if end_ts is None:
                continue

            seconds_remaining = end_ts - now
            print(f"[bot] market {slug} — {seconds_remaining:.1f}s remaining")

            # Cache Chainlink price on first sight — this becomes price_to_beat
            if slug not in self._market_start_prices:
                start_price = await self._get_chainlink_price()
                if start_price is not None:
                    self._market_start_prices[slug] = start_price
                    print(f"[bot] cached start price for {slug}: ${start_price:,.2f}")

            if not (ENTRY_WINDOW_LO <= seconds_remaining <= ENTRY_WINDOW_HI):
                continue

            print(f"[bot] Entry window: {slug}  {seconds_remaining:.1f}s remaining — evaluating signals")
            price_to_beat = self._market_start_prices.get(slug)
            direction, signals = await self._evaluate_signals(market, price_to_beat)

            # Mark slug as seen regardless of outcome so we don't re-evaluate
            self._entered_slugs.add(slug)
            self._market_start_prices.pop(slug, None)  # free memory

            if direction is None:
                print(f"[bot] SKIP: no clear signal — {signals}")
                continue

            entry_price = _side_price(market, direction)
            print(
                f"[bot] SIGNAL ENTRY: {direction} on {slug} — "
                f"source={signals['source']} momentum={signals['momentum']} "
                f"price={entry_price:.3f}"
            )
            await self._open_position(slug, direction, entry_price, end_ts)
            break  # one position at a time

    # ── Active markets ─────────────────────────────────────────────────────────

    async def _fetch_active_markets(self) -> list:
        """
        Gamma API only supports exact slug lookups — a prefix query returns nothing.
        BTC 5m market slugs are 'btc-updown-5m-{ts}' where ts is always a
        multiple of 300, so we compute the current and next windows ourselves
        and fetch each by exact slug.
        """
        now = int(time.time())
        # Current window closes at the next 5-min boundary; fetch that plus the
        # following window so we never miss a market that opens while we're polling
        candidates = [
            (now // 300 + 1) * 300,
            (now // 300 + 2) * 300,
        ]
        slugs = [f"btc-updown-5m-{ts}" for ts in candidates]
        print(f"[bot] fetching slugs: {slugs}")

        session = await self._get_session()
        markets = []
        for slug in slugs:
            try:
                async with session.get(
                    f"{GAMMA_API}/markets", params={"slug": slug}
                ) as resp:
                    if resp.status != 200:
                        print(f"[bot] gamma returned {resp.status} for {slug}")
                        continue
                    data = await resp.json()
                    if data and not data[0].get("closed", True):
                        markets.append(data[0])
                        print(f"[bot] found open market: {slug}  closed={data[0].get('closed')}")
                    elif data:
                        print(f"[bot] market {slug} is closed — skipping")
                    else:
                        print(f"[bot] no gamma result for {slug}")
            except Exception as exc:
                print(f"[bot] failed to fetch {slug}: {exc}")

        return markets

    # ── Signals ────────────────────────────────────────────────────────────────

    async def _get_chainlink_price(self) -> Optional[float]:
        """Run the synchronous web3 call in a thread so it doesn't block the loop."""
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, get_btc_price)
        except Exception as exc:
            print(f"[bot] Chainlink error: {exc}")
            return None

    async def _evaluate_signals(
        self, market: dict, price_to_beat: Optional[float]
    ) -> tuple[Optional[str], dict]:
        """
        Primary: Chainlink latency arb — compare live price to cached start price.
        Fallback: outcomePrices >= 0.55 if Chainlink is unavailable or diff too small.
        """
        # ── Primary: Chainlink ──────────────────────────────────────────────────
        if price_to_beat is not None:
            chainlink_price = await self._get_chainlink_price()
            if chainlink_price is not None:
                diff = chainlink_price - price_to_beat
                if abs(diff) >= PRICE_DIFF_THRESHOLD:
                    direction = "Up" if diff > 0 else "Down"
                    print(
                        f"[bot] CHAINLINK: current=${chainlink_price:,.2f} "
                        f"vs beat=${price_to_beat:,.2f} → {direction}"
                    )
                    return direction, {
                        "source": "chainlink",
                        "momentum": direction,
                        "chainlink": chainlink_price,
                        "price_to_beat": price_to_beat,
                    }
                print(
                    f"[bot] CHAINLINK: diff ${abs(diff):.2f} < "
                    f"${PRICE_DIFF_THRESHOLD} threshold — falling back"
                )
            else:
                print("[bot] CHAINLINK: unavailable — falling back to market signal")
        else:
            print("[bot] CHAINLINK: no start price cached — falling back to market signal")

        # ── Fallback: outcomePrices ─────────────────────────────────────────────
        direction = self._signal_market(market)
        return direction, {"source": "market", "momentum": direction}

    def _signal_market(self, market: dict) -> Optional[str]:
        """
        Polymarket-native fallback: outcomePrices >= 0.55.
        Up price >= 0.55 → 'Up', Down price >= 0.55 → 'Down', else None.
        """
        up_price   = _side_price(market, "Up")
        down_price = _side_price(market, "Down")

        if up_price >= 0.55:
            return "Up"
        if down_price >= 0.55:
            return "Down"
        return None

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

        db.save_bot_state(self.balance)
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
