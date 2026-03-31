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
BET_SIZE             = 500.0  # Kelly base cap — 2x hours can go up to $1,000
WIN_PROB             = 0.60   # conservative win rate estimate — update after 200 trades
MIN_PREV_MOVE        = 30.0   # USD — skip if the reference window moved less than this
# Data-validated allowed hours (ET = UTC-4). All others blocked.
# Hours 18+19 statistically significant (p=0.009 combined). Hours 0,4 solid (n=13-15).
ALLOWED_HOURS        = {0, 1, 4, 5, 18, 19}
# Hour 19: 2x (70.6% WR, n=17, p<0.01). Hour 18: 1x until n≥20 (77.8% WR but n=9).
HOUR_MULTIPLIER      = {19: 2.0, 0: 1.0, 4: 1.0, 5: 1.0, 1: 0.75, 18: 1.0}
POLL_INTERVAL        = 3     # seconds between ticks
ENTRY_WINDOW_LO      = 20    # enter when seconds_remaining >= this
ENTRY_WINDOW_HI      = 45    # enter when seconds_remaining <= this
MIN_MOMENTUM_MOVE    = 20.0  # USD — chop filter: abs(live - price_10s_ago) must exceed this
FUNDING_THRESHOLD    = 0.02  # % — above = bullish, below negative = bearish
PRICE_DIFF_THRESHOLD = 30.0  # USD — minimum diff from reference price to enter
REVERSAL_THRESHOLD   = 20.0  # USD — diff must still be >= this after 3s re-check
CROWD_MIN            = 0.20  # outcomePrices lower bound — below this crowd is 80%+ against us
CROWD_MAX            = 0.70  # outcomePrices upper bound — above this move is fully priced in

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
        self.balance:     float = state["balance"]
        self.positions:   list  = db.load_open_positions()   # up to 2 concurrent positions
        self.wins:        int   = 0
        self.losses:      int   = 0
        self.total_pnl:   float = 0.0
        self.running:     bool  = False
        self._task:       Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        self._entered_slugs: set = set()           # avoid re-entering the same market
        self._evaluating:    bool = False          # True while reversal guard is mid-sleep
        self._final_price_cache:    dict = {}      # {end_ts: finalPrice} for recent closed markets
        self._market_start_prices: dict = {}      # slug → BTC price at first sight (price_to_beat)
        self._btc_price_timestamps: list = []  # [(unix_ts, price)] — last 60s for momentum
        print(f"[bot] Loaded balance from DB: ${self.balance:.2f}", flush=True)
        for pos in self.positions:
            print(
                f"[bot] Restored open position from DB: {pos['side']} "
                f"{pos['market_slug']}  end_ts={pos['end_ts']}",
                flush=True,
            )
            self._entered_slugs.add(pos["market_slug"])

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
            "open_positions":   self.positions,
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
        print(f"[bot] _tick called  positions={len(self.positions)}", flush=True)
        # Continuously collect finalPrices for recent closed markets, independent of position state
        await self._collect_final_prices()

        # Resolve / force-close all held positions
        now_ts = time.time()
        for pos in list(self.positions):  # snapshot — list mutates during iteration
            end_ts = pos.get("end_ts", 0)
            if now_ts >= end_ts + 1200:
                print(
                    f"[bot] FORCE-CLOSE: {pos['market_slug']} "
                    f"— {int(now_ts - end_ts)}s past close with no resolution",
                    flush=True,
                )
                await self._force_close(pos)
            elif now_ts >= end_ts:
                await self._try_resolve(pos)

        # Entry: skip if at capacity
        if len(self.positions) >= 3:
            return

        # Trading hours filter — only enter during data-validated ET hours
        hour_et = (datetime.now(timezone.utc).hour - 4) % 24
        if hour_et not in ALLOWED_HOURS:
            print(f"[bot] entry blocked: ET hour={hour_et} not in ALLOWED_HOURS", flush=True)
            return

        # Scan active BTC 5m markets
        markets = await self._fetch_active_markets()
        now = time.time()
        slugs = [m.get("slug", "?") for m in markets]
        print(f"[bot] _fetch_active_markets returned {len(markets)} markets: {slugs}", flush=True)

        for market in markets:
            slug = market.get("slug", "")
            if not slug:
                continue

            end_ts = _extract_end_ts(slug)
            if end_ts is None:
                continue

            seconds_remaining = end_ts - now
            print(f"[bot] market {slug} — {seconds_remaining:.1f}s remaining")

            # Cache BTC price at first sight — used by reversal-guard as baseline
            if slug not in self._market_start_prices and slug not in self._entered_slugs:
                start_price = await self._get_btc_price()
                if start_price is not None:
                    self._market_start_prices[slug] = start_price
                    print(f"[bot] cached start price for {slug}: ${start_price:,.2f}", flush=True)
                else:
                    print(f"[bot] BTC price unavailable — cannot cache start price for {slug}", flush=True)

            # ── chainlink-reversal-guard (last 6-15s of market)
            if (ENTRY_WINDOW_LO <= seconds_remaining <= ENTRY_WINDOW_HI
                    and slug not in self._entered_slugs):
                if self._evaluating:
                    print("[bot] _tick: reversal check in progress — skipping reversal-guard", flush=True)
                    continue
                price_to_beat = self._market_start_prices.get(slug)
                print(
                    f"[bot] reversal-guard window: {slug}  {seconds_remaining:.1f}s  "
                    f"ref=${price_to_beat:,.2f}" if price_to_beat else
                    f"[bot] reversal-guard window: {slug}  {seconds_remaining:.1f}s  ref=None",
                    flush=True,
                )
                direction, signals = await self._evaluate_signals(market, price_to_beat)
                self._market_start_prices.pop(slug, None)
                self._entered_slugs.add(slug)

                if direction is None:
                    print(f"[bot] SKIP: no clear signal — {signals}", flush=True)
                    continue

                entry_price = _side_price(market, direction)
                if not (CROWD_MIN <= entry_price <= CROWD_MAX):
                    print(
                        f"[bot] SKIP: {direction} price={entry_price:.3f} outside "
                        f"profitability range [{CROWD_MIN}, {CROWD_MAX}]",
                        flush=True,
                    )
                    continue

                diff_at_entry = signals.get("diff_initial")
                print(
                    f"[bot] reversal-guard ENTRY: {direction} on {slug}  "
                    f"price={entry_price:.3f}  diff=${diff_at_entry:+.2f}  secs={seconds_remaining:.1f}",
                    flush=True,
                )
                await self._open_position(
                    slug, direction, entry_price, end_ts,
                    price_to_beat=price_to_beat,
                    diff_at_entry=diff_at_entry,
                    seconds_remaining=seconds_remaining,
                    strategy="chainlink-reversal-guard",
                    hour_et=hour_et,
                )
                break

    # ── Active markets ─────────────────────────────────────────────────────────

    async def _fetch_active_markets(self) -> list:
        """
        Gamma API only supports exact slug lookups — a prefix query returns nothing.
        BTC 5m market slugs are 'btc-updown-5m-{ts}' where ts is always a
        multiple of 300, so we compute the current and next windows ourselves
        and fetch each by exact slug.
        """
        now = int(time.time())
        # Current window closes at the current 5-min boundary floor; fetch that plus
        # the next window as fallback in case the current has already closed
        candidates = [
            (now // 300) * 300,
            (now // 300 + 1) * 300,
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

    async def _collect_final_prices(self) -> None:
        """Each tick: fetch eventMetadata.finalPrice for the last 6 closed BTC 5m markets
        and cache by end_ts. Runs regardless of whether we traded those markets."""
        now_ts   = int(time.time())
        boundary = (now_ts // 300) * 300  # end_ts of the most recently closed market
        print(f"[bot] finalPrice cache size={len(self._final_price_cache)}  keys={sorted(self._final_price_cache)}", flush=True)
        session  = await self._get_session()
        for i in range(1, 7):
            end_ts = boundary - (i - 1) * 300
            if end_ts in self._final_price_cache:
                continue
            slug = f"btc-updown-5m-{end_ts}"
            try:
                async with session.get(
                    f"{GAMMA_API}/markets", params={"slug": slug}
                ) as resp:
                    if resp.status != 200:
                        continue
                    markets = await resp.json()
                if not markets:
                    continue
                m      = markets[0]
                events = m.get("events", [])
                meta   = (events[0].get("eventMetadata") or {}) if events else {}
                print(
                    f"[bot] cache probe {slug}: meta_keys={list(meta.keys())}  "
                    f"market_keys_sample={[k for k in m.keys() if 'price' in k.lower() or 'beat' in k.lower()]}",
                    flush=True,
                )
                if meta.get("finalPrice") is not None and end_ts not in self._final_price_cache:
                    fp = float(meta["finalPrice"])
                    self._final_price_cache[end_ts] = fp
                    print(f"[bot] finalPrice cache: {slug} end_ts={end_ts} → ${fp:,.2f}", flush=True)

            except Exception as exc:
                print(f"[bot] _collect_final_prices error for {slug}: {exc}", flush=True)

    async def _get_btc_price(self) -> Optional[float]:
        """Fetch live BTC/USD from Chainlink on Polygon and append to both history stores."""
        try:
            loop  = asyncio.get_running_loop()
            price = await loop.run_in_executor(None, get_btc_price)
            now   = time.time()

            # Timestamped history — keep last 60s
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

    async def _evaluate_signals(
        self, market: dict, price_to_beat: Optional[float]
    ) -> tuple[Optional[str], dict]:
        """
        Two-condition entry:
        1. abs(live_price - reference_price) >= PRICE_DIFF_THRESHOLD
        2. After 3s re-check: move hasn't reversed and diff >= REVERSAL_THRESHOLD
        Reference price is previous window's finalPrice if cached, else Chainlink first-sight.
        """
        # Use previous market's finalPrice as reference if cached, else fall back to Chainlink first-sight.
        # Slug number = end_ts of the current window (confirmed by _fetch_active_markets and _extract_end_ts).
        # _final_price_cache is keyed by end_ts, so look up (end_ts - 300) to get the PREVIOUS window.
        end_ts      = int(market["slug"].split("-")[-1])
        prev_final  = self._final_price_cache.get(end_ts - 300)
        reference_price = prev_final if prev_final is not None else price_to_beat
        ref_source  = "prev-finalPrice" if prev_final is not None else "chainlink-first-sight"

        if reference_price is None:
            print("[bot] SKIP: no reference price (no finalPrice cache, no Chainlink cache)", flush=True)
            return None, {"source": "none", "momentum": None}

        # Condition 0: minimum prev-window move — only enter if the reference window had momentum
        if prev_final is not None:
            prev_prev_final = self._final_price_cache.get(end_ts - 600)
            if prev_prev_final is not None:
                prev_move = abs(reference_price - prev_prev_final)
                if prev_move < MIN_PREV_MOVE:
                    print(
                        f"[bot] SKIP: prev_move ${prev_move:.2f} < MIN_PREV_MOVE ${MIN_PREV_MOVE:.0f} (flat reference window)",
                        flush=True,
                    )
                    return None, {"source": "none", "momentum": None}

        self._evaluating = True
        try:
            live_price = await self._get_btc_price()
            if live_price is None:
                print("[bot] BTC: unavailable — skipping, no trade", flush=True)
                return None, {"source": "none", "momentum": None}

            # Condition a: strong initial move from reference price
            diff_initial = live_price - reference_price
            print(
                f"[bot] signals: diff_initial=${diff_initial:+.2f}  "
                f"now=${live_price:,.2f}  ref=${reference_price:,.2f} ({ref_source})",
                flush=True,
            )
            direction = "Up" if diff_initial > 0 else "Down"
            if abs(diff_initial) < PRICE_DIFF_THRESHOLD:
                print(
                    f"[bot] SKIP: {direction} diff ${abs(diff_initial):.2f} < threshold ${PRICE_DIFF_THRESHOLD:.0f}",
                    flush=True,
                )
                return None, {"source": "none", "momentum": None}

            # Condition b1: chop filter — 10s price range must show real movement
            price_10s_ago = self._get_price_n_seconds_ago(10)
            if price_10s_ago is not None:
                chop_range = abs(live_price - price_10s_ago)
                if chop_range < MIN_MOMENTUM_MOVE:
                    print(
                        f"[bot] SKIP (chop): 10s range=${chop_range:.2f} < ${MIN_MOMENTUM_MOVE:.0f}  "
                        f"now=${live_price:,.2f}  10s_ago=${price_10s_ago:,.2f}",
                        flush=True,
                    )
                    return None, {"source": "none", "momentum": None}
            else:
                print("[bot] chop filter: no 10s reading, continuing", flush=True)

            # Condition b2: momentum filter — short-term momentum must agree with direction
            price_5s_ago = self._get_price_n_seconds_ago(5)
            if price_5s_ago is not None:
                momentum = live_price - price_5s_ago
                if direction == "Up" and momentum < 0:
                    print(
                        f"[bot] SKIP (momentum): signal=Up but 5s momentum=${momentum:+.2f} (falling)",
                        flush=True,
                    )
                    return None, {"source": "none", "momentum": None}
                if direction == "Down" and momentum > 0:
                    print(
                        f"[bot] SKIP (momentum): signal=Down but 5s momentum=${momentum:+.2f} (rising)",
                        flush=True,
                    )
                    return None, {"source": "none", "momentum": None}
                print(f"[bot] momentum OK: 5s=${momentum:+.2f}  direction={direction}", flush=True)
            else:
                print("[bot] momentum filter: no 5s reading, continuing", flush=True)

            # Condition c: reversal guard — wait 3s, re-fetch, confirm move still holding
            await asyncio.sleep(3)
            live_price_b = await self._get_btc_price()
            if live_price_b is None:
                print("[bot] SKIP: reversal check fetch failed", flush=True)
                return None, {"source": "none", "momentum": None}

            diff_final = live_price_b - reference_price
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
                f"diff_final=${diff_final:+.2f}  ref=${reference_price:,.2f} ({ref_source})",
                flush=True,
            )
            return direction, {
                "source":        f"chainlink-reversal-guard/{ref_source}",
                "momentum":      direction,
                "live_price":    live_price_b,
                "price_to_beat": reference_price,
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
                             price_to_beat: Optional[float] = None,
                             diff_at_entry: Optional[float] = None,
                             seconds_remaining: Optional[float] = None,
                             strategy: Optional[str] = None,
                             hour_et: int = 0):
        # Half-Kelly sizing with data-driven hour multiplier
        loss_prob      = 1.0 - WIN_PROB
        b              = (1.0 / entry_price) - 1.0   # net payout per dollar risked
        kelly_fraction = max(0.0, (WIN_PROB * b - loss_prob) / b) if b > 0 else 0.0
        multiplier     = HOUR_MULTIPLIER.get(hour_et, 1.0)
        max_stake      = BET_SIZE * multiplier         # $500 base, $1000 for 2x hours
        stake          = self.balance * kelly_fraction * 0.5 * multiplier
        stake          = max(50.0, min(max_stake, stake))  # floor $50, cap by hour tier

        if self.balance < stake:
            print(f"[bot] Insufficient balance (${self.balance:.2f}) — skipping")
            return

        self.balance -= stake
        pos = {
            "market_slug":       slug,
            "side":              side,
            "size":              stake,
            "entry_price":       entry_price,
            "price_to_beat":     price_to_beat,
            "end_ts":            end_ts,
            "opened_at":         datetime.now(timezone.utc).isoformat(),
            "diff_at_entry":     diff_at_entry,
            "seconds_remaining": seconds_remaining,
            "strategy":          strategy,
        }
        self.positions.append(pos)
        db.save_open_position(pos)
        db.save_bot_state(self.balance)
        slug_ts   = int(slug.rsplit("-", 1)[-1])
        win_start = datetime.utcfromtimestamp(slug_ts - 300).strftime("%H:%M")
        win_end   = datetime.utcfromtimestamp(slug_ts).strftime("%H:%M")
        print(
            f"[bot] OPEN  {side:>4}  ${stake:.0f} (kelly={kelly_fraction:.3f} ×{multiplier})  slug={slug}"
            f"  window={win_start}→{win_end} UTC  end_ts={slug_ts}"
            f"  entry={entry_price:.3f}  balance=${self.balance:.2f}  "
            f"positions={len(self.positions)}",
            flush=True,
        )

    async def _force_close(self, pos: dict):
        """Close a position that never received a winner from Polymarket.
        Stake is returned to balance — outcome recorded as 'unresolved'."""
        # Remove from list first — prevents re-entry if DB write fails
        self.positions = [p for p in self.positions if p["market_slug"] != pos["market_slug"]]
        db.clear_open_position(pos["market_slug"])

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
                    price_to_beat, poly_price_to_beat, resolution_price,
                    outcome, pnl, balance_after, diff_at_entry, seconds_remaining,
                    strategy, opened_at, closed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    STRATEGY_TAG,
                    pos["market_slug"],
                    pos["side"],
                    pos["size"],
                    round(pos.get("entry_price") or 0.5, 4),
                    round(pos["price_to_beat"], 2) if pos.get("price_to_beat") else None,
                    None,
                    None,
                    "unresolved",
                    0.0,
                    round(self.balance, 2),
                    round(pos["diff_at_entry"], 2) if pos.get("diff_at_entry") is not None else None,
                    round(pos["seconds_remaining"], 1) if pos.get("seconds_remaining") is not None else None,
                    pos.get("strategy"),
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

    async def _try_resolve(self, pos: dict):
        slug   = pos["market_slug"]
        end_ts = pos["end_ts"]
        winner, resolution_price, poly_price_to_beat = await self._resolve_market(slug, end_ts)
        if winner is None:
            print(f"[bot] {slug} not resolved yet — will retry")
            return
        await self._close_position(pos, winner, resolution_price, poly_price_to_beat)

    async def _resolve_market(self, slug: str, end_ts: float) -> tuple[Optional[str], Optional[float], Optional[float]]:
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
            minutes_past = (time.time() - end_ts) / 60
            if minutes_past < 25:
                print(
                    f"[bot] resolve: {slug} — finalPrice not yet available "
                    f"({minutes_past:.1f} min past close, waiting for eventMetadata)",
                    flush=True,
                )
                return None, None, None

            # 25+ min elapsed — finalPrice isn't coming; fall back to outcomePrices
            # Order is not assumed — label comes from outcomes[i].
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
                    f"[bot] resolved via outcomePrices (25+ min fallback): {slug} → {winner}",
                    flush=True,
                )
            else:
                print(
                    f"[bot] resolve: {slug} closed but cannot determine winner yet",
                    flush=True,
                )
                return None, None, None

        return winner, final_price, price_to_beat

    async def _close_position(self, pos: dict, winner: str, resolution_price: Optional[float] = None,
                              poly_price_to_beat: Optional[float] = None):
        # Remove from list FIRST — prevents re-entry if DB write fails
        self.positions = [p for p in self.positions if p["market_slug"] != pos["market_slug"]]
        db.clear_open_position(pos["market_slug"])

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
                    price_to_beat, poly_price_to_beat, resolution_price,
                    outcome, pnl, balance_after, diff_at_entry, seconds_remaining,
                    strategy, opened_at, closed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    STRATEGY_TAG,
                    pos["market_slug"],
                    pos["side"],
                    pos["size"],
                    round(entry_price, 4),
                    round(pos.get("price_to_beat"), 2) if pos.get("price_to_beat") else None,
                    round(poly_price_to_beat, 2) if poly_price_to_beat else None,
                    round(resolution_price, 2) if resolution_price else None,
                    winner,
                    round(pnl, 2),
                    round(self.balance, 2),
                    round(pos["diff_at_entry"], 2) if pos.get("diff_at_entry") is not None else None,
                    round(pos["seconds_remaining"], 1) if pos.get("seconds_remaining") is not None else None,
                    pos.get("strategy"),
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
