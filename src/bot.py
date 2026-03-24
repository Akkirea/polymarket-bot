"""
SIGNAL/ZERO — Whale-copy paper trading bot.

Watches the #1 ranked whale wallet (highest win rate in whale_wallets),
mirrors their BTC 5-minute Up/Down trades with $500 paper positions,
and resolves each position after the market closes (≥5 min).

All paper only — no real money, no API keys.

Usage (started automatically by api.py endpoints):
    POST http://localhost:8000/api/bot/start
    POST http://localhost:8000/api/bot/stop
    GET  http://localhost:8000/api/bot/status
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from . import db

# ── Constants ──────────────────────────────────────────────────────────────────
DATA_API        = "https://data-api.polymarket.com"
GAMMA_API       = "https://gamma-api.polymarket.com"

INITIAL_BALANCE = 10_000.0
BET_SIZE        = 500.0
POLL_INTERVAL   = 60    # seconds between whale checks
RESOLVE_AFTER   = 300   # wait 5 min before first resolution attempt


# ── PaperBot ───────────────────────────────────────────────────────────────────
class PaperBot:
    """
    Mirrors the top whale wallet's BTC 5-min trades with paper money.

    One open position at a time. Resolves via the Gamma API once the
    5-minute market window has closed and outcomePrices shows a winner.
    """

    def __init__(self):
        self.balance:     float          = INITIAL_BALANCE
        self.position:    Optional[dict] = None   # single open position
        self.wins:        int            = 0
        self.losses:      int            = 0
        self.total_pnl:   float          = 0.0
        self.running:     bool           = False
        self._task:       Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        # tx hashes we've already processed so we don't re-mirror old trades
        self._seen_tx:    set            = set()
        self._seeded:     bool           = False   # first-poll seed done?
        db.init_db()

    # ── Session ────────────────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
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

    # ── Loop ───────────────────────────────────────────────────────────────────

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
        # ── Step 1: try to resolve open position once old enough ────────────
        if self.position:
            opened = datetime.fromisoformat(self.position["opened_at"])
            age = (datetime.now(timezone.utc) - opened).total_seconds()
            if age >= RESOLVE_AFTER:
                await self._try_resolve()

        # ── Step 2: if still no open position, look for a trade to mirror ───
        if not self.position:
            whale = self._get_top_whale()
            if not whale:
                print("[bot] No ranked whale wallets yet — run the whale tracker first")
                return
            addr = whale["address"]
            print(f"[bot] Checking whale {addr[:8]}…{addr[-4:]} "
                  f"(win rate {whale['win_rate']:.0%})")
            await self._check_for_new_trade(addr)

    # ── Whale DB lookup ────────────────────────────────────────────────────────

    def _get_top_whale(self) -> Optional[dict]:
        conn = db.get_connection()
        row = conn.execute(
            """SELECT address, win_rate, total_trades, resolved_trades
                 FROM whale_wallets
                WHERE resolved_trades >= 2
                ORDER BY win_rate DESC, resolved_trades DESC
                LIMIT 1"""
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    # ── Trade fetching & mirroring ─────────────────────────────────────────────

    async def _check_for_new_trade(self, address: str):
        """
        Fetch the whale's 10 most recent trades.
        On the very first call (self._seeded is False) we just record all
        tx hashes without acting — this prevents mirroring stale trades
        that predate the bot starting.
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{DATA_API}/trades",
                params={"user": address, "limit": 10},
            ) as resp:
                if resp.status != 200:
                    print(f"[bot] data-api returned HTTP {resp.status}")
                    return
                trades = await resp.json()
        except Exception as exc:
            print(f"[bot] Failed to fetch trades for {address[:10]}: {exc}")
            return

        print(f"[bot] Fetched {len(trades)} trades from data-api")
        if trades:
            t0 = trades[0]
            print(f"[bot] Most recent trade: slug={t0.get('slug', '?')}  "
                  f"ts={t0.get('timestamp', '?')}  side={t0.get('side', '?')}  "
                  f"outcome={t0.get('outcome', '?')}")

        if not self._seeded:
            # First call: seed seen set without acting
            for t in trades:
                tx = t.get("transactionHash") or str(t.get("id", ""))
                if tx:
                    self._seen_tx.add(tx)
            self._seeded = True
            print(f"[bot] Seeded {len(self._seen_tx)} existing tx hashes — watching for new trades")
            return

        # Subsequent calls: look for a new BTC 5-min BUY we haven't seen
        new_count = 0
        for trade in trades:
            tx = trade.get("transactionHash") or str(trade.get("id", ""))
            slug    = trade.get("slug", "")
            side    = trade.get("side", "")
            outcome = trade.get("outcome", "")
            price   = float(trade.get("price") or 0.5)

            if not tx or tx in self._seen_tx:
                print(f"[bot] SKIP (already seen): {slug}  side={side}  tx={tx[:12] if tx else '?'}")
                continue

            new_count += 1
            self._seen_tx.add(tx)
            print(f"[bot] NEW tx: {tx[:16]}  slug={slug}  side={side}  outcome={outcome}  price={price:.4f}")

            if "btc-updown-5m" not in slug:
                print(f"[bot] SKIP: not a BTC 5m slug ({slug})")
                continue
            if side != "BUY":
                print(f"[bot] SKIP: side is {side!r}, not BUY")
                continue
            if outcome not in ("Up", "Down"):
                print(f"[bot] SKIP: outcome {outcome!r} not Up/Down")
                continue

            print(f"[bot] ✓ Mirroring whale trade — {outcome} on {slug} @ {price:.4f}")
            await self._open_position(address, slug, outcome, price)
            break  # one position at a time

        if new_count == 0:
            print(f"[bot] No new trades (all {len(trades)} already seen)")

    # ── Position lifecycle ─────────────────────────────────────────────────────

    async def _open_position(
        self, whale_address: str, slug: str, side: str, entry_price: float
    ):
        if self.balance < BET_SIZE:
            print(f"[bot] Insufficient balance (${self.balance:.2f}) — skipping")
            return

        self.balance -= BET_SIZE
        self.position = {
            "whale_address": whale_address,
            "market_slug":   slug,
            "side":          side,
            "size":          BET_SIZE,
            "entry_price":   entry_price,
            "opened_at":     datetime.now(timezone.utc).isoformat(),
        }
        print(
            f"[bot] OPEN  {side:>4}  ${BET_SIZE:.0f}  {slug}"
            f"  entry={entry_price:.3f}  balance=${self.balance:.2f}"
        )

    async def _try_resolve(self):
        slug = self.position["market_slug"]
        print(f"[bot] Attempting resolution of {slug}")
        winner = await self._resolve_market(slug)
        print(f"[bot] Resolution result: {winner!r}")
        if winner is None:
            print(f"[bot] Not resolved yet — will retry next tick")
            return
        await self._close_position(winner)

    async def _resolve_market(self, slug: str) -> Optional[str]:
        """Return 'Up' or 'Down' when the market settles, else None."""
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
            print(f"[bot] Gamma returned 0 markets for slug={slug}")
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
        return None  # still live

    async def _close_position(self, winner: str):
        pos         = self.position
        won         = pos["side"] == winner
        size        = pos["size"]
        entry_price = pos.get("entry_price", 0.5)
        if entry_price <= 0:
            entry_price = 0.5

        if won:
            # Shares * $1 payout, minus cost = size*(1/entry_price - 1)
            pnl = size * (1.0 / entry_price - 1.0)
            self.balance += size + pnl   # return stake + profit
            self.wins += 1
        else:
            pnl = -size
            # balance was already deducted at open
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
                pos["whale_address"],
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


# ── Module-level singleton used by api.py ─────────────────────────────────────
bot = PaperBot()
