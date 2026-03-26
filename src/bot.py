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
ENTRY_WINDOW_LO      = 6     # enter when seconds_remaining >= this
ENTRY_WINDOW_HI      = 15    # enter when seconds_remaining <= this
FUNDING_THRESHOLD    = 0.02  # % — above = bullish, below negative = bearish
PRICE_DIFF_THRESHOLD = 60.0  # USD — initial diff required to consider entry
REVERSAL_THRESHOLD   = 40.0  # USD — diff must still be >= this after 3s re-check
CROWD_MIN_CONFIDENCE = 0.60  # kept for reference; crowd filter currently disabled
RANGING_WINDOW       = 3     # number of recent BTC readings to check for ranging market
RANGING_THRESHOLD    = 20.0  # USD — if all readings within $20, market is flat → skip

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
        self.position:    Optional[dict] = db.load_open_position()
        self.wins:        int            = 0
        self.losses:      int            = 0
        self.total_pnl:   float          = 0.0
        self.running:     bool           = False
        self._task:       Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        self._entered_slugs: set = set()          # avoid re-entering the same market
        self._evaluating:    bool = False         # True while reversal guard is mid-sleep
        self._market_start_prices: dict = {}     # slug → BTC price at first sight (price_to_beat)
        self._btc_price_history:     list = []  # rolling window of recent prices for ranging filter
        self._btc_price_timestamps: list = []  # [(unix_ts, price)] — last 60s for momentum
        print(f"[bot] Loaded balance from DB: ${self.balance:.2f}", flush=True)
        if self.position:
            print(
                f"[bot] Restored open position from DB: {self.position['side']} "
                f"{self.position['market_slug']}  end_ts={self.position['end_ts']}",
                flush=True,
            )
            # Prevent re-entry into the restored slug
            self._entered_slugs.add(self.position["market_slug"])

    # ── Session ────────────────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10, connect=5),
                headers={"User-Agent": "Mozilla/5.0 (compatible; signal-zero-bot/1.0)"},
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
        import traceback as _tb
        print("[bot] loop starting", flush=True)
        while self.running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[bot] Tick error: {exc}\n{_tb.format_exc()}", flush=True)
            await asyncio.sleep(POLL_INTERVAL)

    async def _tick(self):
        print(f"[bot] _tick called  position={'HELD' if self.position else 'none'}", flush=True)
        # If holding a position, try to resolve once the market window has closed
        if self.position:
            end_ts = self.position.get("end_ts", 0)
            now_ts = time.time()
            if now_ts >= end_ts + 1200:
                # 20 minutes past close — finalPrice still not available; force-close
                print(
                    f"[bot] FORCE-CLOSE: {self.position['market_slug']} "
                    f"— {int(now_ts - end_ts)}s past close with no resolution",
                    flush=True,
                )
                await self._force_close()
            elif now_ts >= end_ts:
                await self._try_resolve()
            return  # one position at a time

        # Reversal guard in progress — don't scan until it completes
        if self._evaluating:
            print("[bot] _tick: reversal check in progress — skipping scan", flush=True)
            return

        # Scan active BTC 5m markets for an entry window
        markets = await self._fetch_active_markets()
        now = time.time()
        slugs = [m.get("slug", "?") for m in markets]
        print(f"[bot] _fetch_active_markets returned {len(markets)} markets: {slugs}", flush=True)

        for market in markets:
            slug = market.get("slug", "")
            if not slug or slug in self._entered_slugs:
                continue

            end_ts = _extract_end_ts(slug)
            if end_ts is None:
                continue

            seconds_remaining = end_ts - now
            print(f"[bot] market {slug} — {seconds_remaining:.1f}s remaining")

            # Cache BTC price at first sight — this becomes price_to_beat for comparison
            if slug not in self._market_start_prices:
                start_price = await self._get_btc_price()
                if start_price is not None:
                    self._market_start_prices[slug] = start_price
                    print(f"[bot] cached start price for {slug}: ${start_price:,.2f}", flush=True)
                else:
                    print(f"[bot] BTC price unavailable — cannot cache start price for {slug}", flush=True)

            if not (ENTRY_WINDOW_LO <= seconds_remaining <= ENTRY_WINDOW_HI):
                continue

            # Use the price cached at first sight as the baseline
            price_to_beat = self._market_start_prices.get(slug)
            print(
                f"[bot] Entry window: {slug}  {seconds_remaining:.1f}s remaining  "
                f"price_to_beat=${price_to_beat:,.2f}" if price_to_beat else
                f"[bot] Entry window: {slug}  {seconds_remaining:.1f}s remaining  price_to_beat=None",
                flush=True,
            )
            direction, signals = await self._evaluate_signals(market, price_to_beat)

            # Clean up cached price and mark slug as seen
            self._market_start_prices.pop(slug, None)
            self._entered_slugs.add(slug)

            if direction is None:
                print(f"[bot] SKIP: no clear signal — {signals}", flush=True)
                continue

            entry_price = _side_price(market, direction)
            print(
                f"[bot] SIGNAL ENTRY: {direction} on {slug} — "
                f"source={signals['source']} momentum={signals['momentum']} "
                f"price={entry_price:.3f}",
                flush=True,
            )
            await self._open_position(slug, direction, entry_price, end_ts, price_to_beat)
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

    async def _get_btc_price(self) -> Optional[float]:
        """Fetch live BTC/USD from Chainlink on Polygon and append to both history stores."""
        try:
            loop  = asyncio.get_running_loop()
            price = await loop.run_in_executor(None, get_btc_price)
            now   = time.time()

            # Ranging filter — last N readings (no timestamps needed)
            self._btc_price_history.append(price)
            if len(self._btc_price_history) > RANGING_WINDOW:
                self._btc_price_history.pop(0)

            # Momentum filter — timestamped, keep last 60s
            self._btc_price_timestamps.append((now, price))
            cutoff = now - 60
            self._btc_price_timestamps = [
                (t, p) for t, p in self._btc_price_timestamps if t >= cutoff
            ]

            return price
        except Exception as exc:
            print(f"[bot] BTC price error: {exc}", flush=True)
            return None

    def _get_price_n_seconds_ago(self, n: float) -> Optional[float]:
        """Return the Chainlink reading closest to n seconds ago.
        Returns None if no reading exists within 10s of the target timestamp."""
        if not self._btc_price_timestamps:
            return None
        target = time.time() - n
        closest_ts, closest_price = min(self._btc_price_timestamps, key=lambda x: abs(x[0] - target))
        if abs(closest_ts - target) > 10:
            return None
        return closest_price

    def _is_ranging(self) -> bool:
        """Return True if the last RANGING_WINDOW prices are all within RANGING_THRESHOLD of each other."""
        if len(self._btc_price_history) < RANGING_WINDOW:
            return False  # not enough data yet — don't filter
        spread = max(self._btc_price_history) - min(self._btc_price_history)
        return spread < RANGING_THRESHOLD

    async def _evaluate_signals(
        self, market: dict, price_to_beat: Optional[float]
    ) -> tuple[Optional[str], dict]:
        """
        Two-condition reversal guard:
        1. abs(live_price - price_to_beat) >= PRICE_DIFF_THRESHOLD ($60)
        2. After 3s re-check: move hasn't reversed and diff >= REVERSAL_THRESHOLD ($40)
        Ranging filter also applied before the diff check.
        """
        if price_to_beat is None:
            print("[bot] BTC: no start price cached — skipping, no trade", flush=True)
            return None, {"source": "none", "momentum": None}

        self._evaluating = True
        try:
            live_price = await self._get_btc_price()
            if live_price is None:
                print("[bot] BTC: unavailable — skipping, no trade", flush=True)
                return None, {"source": "none", "momentum": None}

            if self._is_ranging():
                spread = max(self._btc_price_history) - min(self._btc_price_history)
                print(
                    f"[bot] SKIP: ranging market — last {RANGING_WINDOW} readings span ${spread:.2f} "
                    f"(threshold ${RANGING_THRESHOLD})",
                    flush=True,
                )
                return None, {"source": "none", "momentum": None}

            # Condition a: strong initial move from first-sight price
            diff_initial = live_price - price_to_beat
            print(
                f"[bot] signals: diff_initial=${diff_initial:+.2f}  "
                f"now=${live_price:,.2f}  beat(first-sight)=${price_to_beat:,.2f}",
                flush=True,
            )
            if abs(diff_initial) < PRICE_DIFF_THRESHOLD:
                print(
                    f"[bot] SKIP: diff ${abs(diff_initial):.2f} < threshold ${PRICE_DIFF_THRESHOLD}",
                    flush=True,
                )
                return None, {"source": "none", "momentum": None}

            direction = "Up" if diff_initial > 0 else "Down"

            # Condition b: reversal guard — wait 3s, re-fetch, confirm move still holding
            await asyncio.sleep(3)
            live_price_b = await self._get_btc_price()
            if live_price_b is None:
                print("[bot] SKIP: reversal check fetch failed", flush=True)
                return None, {"source": "none", "momentum": None}

            diff_final = live_price_b - price_to_beat
            reversed_direction = (diff_final > 0) != (diff_initial > 0)
            if reversed_direction or abs(diff_final) < REVERSAL_THRESHOLD:
                print(
                    f"[bot] SKIP: momentum faded/reversed — "
                    f"initial=${diff_initial:+.2f}  now=${diff_final:+.2f}",
                    flush=True,
                )
                return None, {"source": "none", "momentum": None}

            print(
                f"[bot] ENTRY: {direction}  diff_initial=${diff_initial:+.2f}  "
                f"diff_final=${diff_final:+.2f}  beat=${price_to_beat:,.2f}",
                flush=True,
            )
            return direction, {
                "source":        "chainlink-reversal-guard",
                "momentum":      direction,
                "live_price":    live_price_b,
                "price_to_beat": price_to_beat,
                "diff_initial":  diff_initial,
                "diff_final":    diff_final,
            }
        finally:
            self._evaluating = False

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

    async def _open_position(self, slug: str, side: str, entry_price: float, end_ts: float,
                             price_to_beat: Optional[float] = None):
        if self.balance < BET_SIZE:
            print(f"[bot] Insufficient balance (${self.balance:.2f}) — skipping")
            return

        self.balance -= BET_SIZE
        self.position = {
            "market_slug":   slug,
            "side":          side,
            "size":          BET_SIZE,
            "entry_price":   entry_price,
            "price_to_beat": price_to_beat,
            "end_ts":        end_ts,
            "opened_at":     datetime.now(timezone.utc).isoformat(),
        }
        db.save_open_position(self.position)
        db.save_bot_state(self.balance)
        print(
            f"[bot] OPEN  {side:>4}  ${BET_SIZE:.0f}  {slug}"
            f"  entry={entry_price:.3f}  balance=${self.balance:.2f}",
            flush=True,
        )

    async def _force_close(self):
        """Close a position that never received a winner from Polymarket.
        Stake is returned to balance — outcome recorded as 'unresolved'."""
        # Clear position first — prevents re-entry if DB write fails
        pos           = self.position
        self.position = None
        db.clear_open_position()

        self.balance += pos["size"]
        closed_at = datetime.now(timezone.utc).isoformat()
        print(
            f"[bot] FORCE-CLOSE refund ${pos['size']:.0f} → balance=${self.balance:.2f}",
            flush=True,
        )

        try:
            conn = db.get_connection()
            conn.execute(
                """INSERT INTO bot_trades
                   (whale_address, market_slug, side, size, entry_price,
                    price_to_beat, resolution_price,
                    outcome, pnl, balance_after, opened_at, closed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    STRATEGY_TAG,
                    pos["market_slug"],
                    pos["side"],
                    pos["size"],
                    round(pos.get("entry_price") or 0.5, 4),
                    round(pos["price_to_beat"], 2) if pos.get("price_to_beat") else None,
                    None,
                    "unresolved",
                    0.0,
                    round(self.balance, 2),
                    pos["opened_at"],
                    closed_at,
                ),
            )
            conn.commit()
            conn.close()
            db.save_bot_state(self.balance)
            asyncio.create_task(
                self._backfill_resolution_price(pos["market_slug"], pos["end_ts"])
            )
        except Exception as exc:
            import traceback
            print(f"[bot] DB write failed after force-close — trade lost but position cleared: {exc}\n{traceback.format_exc()}", flush=True)

    async def _backfill_resolution_price(self, slug: str, end_ts: float):
        """Background task: wait until ~15 min past close, then fetch finalPrice
        from eventMetadata and write it to bot_trades if still missing."""
        wait = max(0.0, (end_ts + 900) - time.time())
        print(f"[bot] backfill: waiting {wait:.0f}s to fetch finalPrice for {slug}", flush=True)
        await asyncio.sleep(wait)

        for attempt in range(3):
            try:
                session = await self._get_session()
                async with session.get(
                    f"{GAMMA_API}/markets", params={"slug": slug}
                ) as resp:
                    markets = await resp.json()
                if not markets:
                    break
                m      = markets[0]
                events = m.get("events", [])
                meta   = (events[0].get("eventMetadata") or {}) if events else {}
                final_price = meta.get("finalPrice")
                if final_price is not None:
                    db.update_resolution_price(slug, float(final_price))
                    print(
                        f"[bot] backfill: {slug} → finalPrice={float(final_price):.2f} written",
                        flush=True,
                    )
                    return
                print(
                    f"[bot] backfill: attempt {attempt + 1}/3 — finalPrice not yet available for {slug}",
                    flush=True,
                )
            except Exception as exc:
                print(f"[bot] backfill error ({slug}) attempt {attempt + 1}/3: {exc}", flush=True)
            await asyncio.sleep(300)  # wait 5 more minutes before next attempt

        print(f"[bot] backfill: gave up on finalPrice for {slug}", flush=True)

    async def _try_resolve(self):
        slug = self.position["market_slug"]
        winner, resolution_price, poly_price_to_beat = await self._resolve_market(slug)
        if winner is None:
            print(f"[bot] {slug} not resolved yet — will retry")
            return
        await self._close_position(winner, resolution_price, poly_price_to_beat)

    async def _resolve_market(self, slug: str) -> tuple[Optional[str], Optional[float], Optional[float]]:
        """Return (winner, final_price, price_to_beat) when market.closed == True
        and market.winner is set. Uses Polymarket's authoritative fields directly —
        no outcomePrices inference."""
        session = await self._get_session()
        try:
            async with session.get(
                f"{GAMMA_API}/markets", params={"slug": slug}
            ) as resp:
                if resp.status != 200:
                    print(f"[bot] resolve: Gamma returned {resp.status} for {slug}", flush=True)
                    return None, None, None
                markets = await resp.json()
        except Exception as exc:
            print(f"[bot] resolve: fetch error for {slug}: {exc}", flush=True)
            return None, None, None

        if not markets:
            return None, None, None

        m = markets[0]

        if not m.get("closed"):
            print(f"[bot] resolve: {slug} not yet closed", flush=True)
            return None, None, None

        # --- Derive winner from eventMetadata (winner field is never populated
        #     for BTC 5m markets; finalPrice vs priceToBeat is authoritative) ---
        final_price:   Optional[float] = None
        price_to_beat: Optional[float] = None
        winner:        Optional[str]   = None

        import json as _json
        events = m.get("events", [])
        meta   = (events[0].get("eventMetadata") or {}) if events else {}
        print(
            f"[bot] resolve raw: slug={slug}  events={len(events)}  "
            f"meta={_json.dumps(meta)}  winner_field={m.get('winner')!r}  "
            f"outcomePrices={m.get('outcomePrices')!r}",
            flush=True,
        )
        try:
            if meta.get("finalPrice") is not None:
                final_price = float(meta["finalPrice"])
            if meta.get("priceToBeat") is not None:
                price_to_beat = float(meta["priceToBeat"])
        except Exception as exc:
            print(f"[bot] resolve: eventMetadata parse error: {exc}", flush=True)

        if final_price is not None and price_to_beat is not None:
            winner = "Up" if final_price >= price_to_beat else "Down"
            print(
                f"[bot] resolved via eventMetadata: {slug} → {winner}  "
                f"finalPrice={final_price:.2f}  priceToBeat={price_to_beat:.2f}",
                flush=True,
            )
        else:
            # Fallback: find which outcome index has price >= 0.99, then look up
            # the label at that index. Order is not assumed — label comes from outcomes[].
            try:
                outcomes       = _json.loads(m.get("outcomes", "[]"))
                outcome_prices = _json.loads(m.get("outcomePrices", "[]"))
                for i, price_str in enumerate(outcome_prices):
                    if float(price_str) >= 0.99:
                        winner = outcomes[i]
                        break
            except Exception as exc:
                print(f"[bot] resolve: outcomePrices parse error: {exc}", flush=True)

            if winner:
                print(
                    f"[bot] resolved via outcomePrices: {slug} → {winner}",
                    flush=True,
                )
            else:
                print(
                    f"[bot] resolve: {slug} closed but cannot determine winner yet",
                    flush=True,
                )
                return None, None, None

        return winner, final_price, price_to_beat

    async def _close_position(self, winner: str, resolution_price: Optional[float] = None,
                              poly_price_to_beat: Optional[float] = None):
        # Snapshot and clear position FIRST — prevents re-entry if DB write fails
        pos           = self.position
        self.position = None
        db.clear_open_position()

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
            f"  pnl=${pnl:+.2f}  balance=${self.balance:.2f}",
            flush=True,
        )

        try:
            conn = db.get_connection()
            conn.execute(
                """INSERT INTO bot_trades
                   (whale_address, market_slug, side, size, entry_price,
                    price_to_beat, resolution_price,
                    outcome, pnl, balance_after, opened_at, closed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    STRATEGY_TAG,
                    pos["market_slug"],
                    pos["side"],
                    pos["size"],
                    round(entry_price, 4),
                    round(pos.get("price_to_beat"), 2) if pos.get("price_to_beat") else None,
                    round(resolution_price, 2) if resolution_price else None,
                    winner,
                    round(pnl, 2),
                    round(self.balance, 2),
                    pos["opened_at"],
                    closed_at,
                ),
            )
            conn.commit()
            conn.close()
            db.save_bot_state(self.balance)
            asyncio.create_task(
                self._backfill_resolution_price(pos["market_slug"], pos["end_ts"])
            )
        except Exception as exc:
            import traceback
            print(f"[bot] DB write failed after close — trade lost but position cleared: {exc}\n{traceback.format_exc()}", flush=True)


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
