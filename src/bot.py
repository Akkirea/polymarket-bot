"""
SIGNAL/ZERO — Timing-based signal bot.

Monitors active BTC 5-minute markets and enters a $500 paper position
in the last 5-12 seconds before close when momentum and market price
range agree on direction. Meaningful non-neutral funding must agree too.

All paper only — no real money.

Usage (started automatically by api.py endpoints):
    POST http://localhost:8000/api/bot/start
    POST http://localhost:8000/api/bot/stop
    GET  http://localhost:8000/api/bot/status
"""

import asyncio
import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from . import db
from .binance_ws import BinancePriceFeed
from .chainlink import get_btc_price

# ── Constants ──────────────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
BYBIT_API = "https://api.bybit.com/v5/market"

INITIAL_BALANCE      = db.INITIAL_BALANCE  # keep in sync with db.py
INITIAL_WALLET_SIZE  = 97.0
MAX_STAKE_PCT        = 0.02   # half-Kelly at 52% win prob and 0.50 entry
MIN_STAKE_PCT        = 0.01   # skip smaller signals instead of forcing oversize bets
LIVE_MIN_ORDER_SIZE  = 1.00   # Polymarket marketable BUY minimum
ORDER_SIZE_INCREMENT = 1.00   # Send whole-dollar USDC order sizes only
MAX_STAKE            = INITIAL_WALLET_SIZE * MAX_STAKE_PCT
MIN_STAKE            = max(LIVE_MIN_ORDER_SIZE, INITIAL_WALLET_SIZE * MIN_STAKE_PCT)
WIN_PROB             = 0.60   # conservative win rate estimate — update after 200 trades
MIN_PREV_MOVE        = 25.0   # USD — skip if the reference window moved less than this
# Data-validated allowed hours (ET = UTC-4). All others blocked.
# Hours 18+19 statistically significant (p=0.009 combined). Hours 0,4 solid (n=13-15).
ALLOWED_HOURS        = {0, 1, 4, 5, 18, 19}
# Hour 19: 2x (70.6% WR, n=17, p<0.01). Hour 18: 1x until n≥20 (77.8% WR but n=9).
HOUR_MULTIPLIER      = {19: 2.0, 0: 1.0, 4: 1.0, 5: 1.0, 1: 0.75, 18: 1.0}
POLL_INTERVAL        = 3     # seconds between ticks
ENTRY_WINDOW_LO      = 75    # enter when seconds_remaining >= this
ENTRY_WINDOW_HI      = 150   # enter when seconds_remaining <= this
VOLUME_RETRY_LO      = 30    # late retry only after a volume skip in the main window
VOLUME_RETRY_HI      = ENTRY_WINDOW_LO
MIN_MOMENTUM_MOVE    = 10.0  # USD — chop filter: abs(live - price_10s_ago) must exceed this
FUNDING_THRESHOLD    = 0.02  # % — outside this band, funding must agree with direction
PRICE_DIFF_THRESHOLD = 10.0  # USD — minimum diff from reference price to enter
REVERSAL_THRESHOLD   = 8.0   # USD — diff must still be >= this after 3s re-check
CROWD_MIN            = 0.30  # outcomePrices lower bound — below this crowd is 70%+ against us
CROWD_MAX            = 0.70  # outcomePrices upper bound — above this move is fully priced in
BINANCE_STALE_AFTER  = 65.0  # seconds — keep Binance history usable for 15m's 30s/60s checks
MIN_MARKET_VOLUME    = 2000.0  # USDC — only enforced during the entry window

STRATEGY_TAG = "SIGNAL_STRATEGY"  # stored in whale_address column (NOT NULL)
LIVE_INITIAL_BALANCE = float(os.getenv("LIVE_INITIAL_BALANCE", "8.45"))  # starting pUSD on-chain


def _round_order_size(amount: float) -> float:
    """Round up to the next whole-dollar order size."""
    increment = max(0.01, ORDER_SIZE_INCREMENT)
    return round(math.ceil(amount / increment) * increment, 2)

MARKET_FAMILIES = [
    {
        "label": "BTC 5m",
        "slug_prefix": "btc-updown-5m",
        "interval": 300,
        "strategy": "chainlink-reversal-guard",
        "live_enabled": True,
        "entry_window": (ENTRY_WINDOW_LO, ENTRY_WINDOW_HI),
        "diff_threshold": PRICE_DIFF_THRESHOLD,
        "reversal_threshold": REVERSAL_THRESHOLD,
        "chop_window": 10,
        "momentum_window": 5,
        "chop_min_move": MIN_MOMENTUM_MOVE,
    },
    {
        "label": "BTC 15m",
        "slug_prefix": "btc-updown-15m",
        "interval": 900,
        "strategy": "btc-15m-paper-shadow",
        "live_enabled": False,
        "mode": "shadow",
        "entry_window": (180, 300),
        "diff_threshold": 25.0,
        "reversal_threshold": 20.0,
        "chop_window": 60,
        "momentum_window": 30,
        "chop_min_move": 15.0,
    },
]


# ── PaperBot ───────────────────────────────────────────────────────────────────
class PaperBot:
    """
    Enters BTC 5-min markets in the last 5-12 s before close when
    momentum agrees and market price is in the 0.40-0.70 range.
    Meaningful non-neutral funding must agree too.
    """

    def __init__(self):
        db.init_db()
        state = db.load_bot_state()
        performance = db.load_bot_performance()
        self.balance:     float = state["balance"]
        self.positions:   list  = db.load_open_positions()
        self.wins:        int   = performance["wins"]
        self.losses:      int   = performance["losses"]
        self.total_pnl:   float = performance["total_pnl"]
        live_perf = db.load_live_performance()
        live_state = db.load_live_state(LIVE_INITIAL_BALANCE)
        self.live_balance: float = live_state["balance"]
        self.live_wins:    int   = live_perf["wins"]
        self.live_losses:  int   = live_perf["losses"]
        self.live_pnl:     float = live_perf["total_pnl"]
        self.running:     bool  = False
        self._task:       Optional[asyncio.Task] = None
        self._binance_task: Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        self._binance_feed = BinancePriceFeed()
        self._entered_slugs: set = set()           # avoid re-entering the same market
        self._volume_retry_slugs: set = set()      # slugs that can retry after main-window low volume
        self._live_retry_slugs: set = set()        # slugs with a background live-fill worker
        self._shadow_entered_slugs: set = set()    # avoid duplicate shadow rows for one market
        self._evaluating:    bool = False          # True while reversal guard is mid-sleep
        self._final_price_cache:    dict = {}      # {market_slug: finalPrice} for recent closed markets
        self._market_start_prices: dict = {}      # slug → BTC price at first sight (price_to_beat)
        self._market_start_sources: dict = {}     # slug → source name for cached first-sight price
        self._btc_price_timestamps: list = []  # [(unix_ts, price)] — last 60s for momentum
        self._funding_rate: Optional[float] = None
        self._funding_rate_updated_at: float = 0.0
        self._funding_rate_source: Optional[str] = None
        print(f"[bot] Loaded balance from DB: ${self.balance:.2f}", flush=True)
        print(
            f"[bot] Loaded stats from DB: wins={self.wins} losses={self.losses} "
            f"pnl=${self.total_pnl:.2f}",
            flush=True,
        )
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
        self._binance_task = loop.create_task(self._binance_feed.connect())
        loop.create_task(self._reconcile_unresolved_trades())
        loop.create_task(self._reconcile_unresolved_shadow_trades())
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
        self._binance_feed.stop()
        if self._binance_task:
            self._binance_task.cancel()
            try:
                await self._binance_task
            except asyncio.CancelledError:
                pass
            self._binance_task = None
        if self._session and not self._session.closed:
            await self._session.close()
        print("[bot] Stopped")

    def get_status(self) -> dict:
        total = self.wins + self.losses
        live_total = self.live_wins + self.live_losses
        return {
            "running":          self.running,
            "balance":          round(self.balance, 2),
            "total_trades":     total,
            "wins":             self.wins,
            "losses":           self.losses,
            "win_rate":         round(self.wins / total, 4) if total else 0.0,
            "pnl":              round(self.total_pnl, 2),
            "live": {
                "balance":   round(self.live_balance, 4),
                "wins":      self.live_wins,
                "losses":    self.live_losses,
                "total":     live_total,
                "win_rate":  round(self.live_wins / live_total, 4) if live_total else 0.0,
                "pnl":       round(self.live_pnl, 4),
            },
            "open_positions":   self.positions,
            "price_feed": {
                "source":       self._binance_feed.source,
                "connected":    self._binance_feed.connected,
                "current_price": round(self._binance_feed.current_price, 2) if self._binance_feed.current_price else None,
                "last_update":  self._binance_feed.last_update,
            },
            "funding_rate": self._funding_rate,
            "funding_rate_updated_at": self._funding_rate_updated_at or None,
            "funding_rate_source": self._funding_rate_source,
        }

    def get_recent_trades(self, n: int = 20, mode: str = "paper") -> list:
        conn = db.get_connection()
        rows = conn.execute(
            "SELECT * FROM bot_trades WHERE COALESCE(mode,'paper')=%s ORDER BY opened_at DESC LIMIT %s",
            (mode, n),
        ).fetchall()
        conn.close()
        trades = [dict(r) for r in rows]

        if mode in {"paper", "live"}:
            for pos in db.load_open_positions():
                if mode == "live" and not pos.get("live_order_id"):
                    continue
                size = pos.get("live_stake") if mode == "live" else pos.get("size")
                entry_price = pos.get("live_fill_price") if mode == "live" else pos.get("entry_price")
                trades.append(
                    {
                        "id": f"open:{mode}:{pos['market_slug']}",
                        "whale_address": STRATEGY_TAG,
                        "market_slug": pos["market_slug"],
                        "side": pos["side"],
                        "size": size,
                        "entry_price": entry_price,
                        "price_to_beat": pos.get("price_to_beat"),
                        "poly_price_to_beat": None,
                        "resolution_price": None,
                        "outcome": "open",
                        "pnl": None,
                        "balance_after": None,
                        "diff_at_entry": pos.get("diff_at_entry"),
                        "seconds_remaining": pos.get("seconds_remaining"),
                        "strategy": pos.get("strategy"),
                        "opened_at": pos.get("opened_at"),
                        "closed_at": None,
                        "mode": mode,
                    }
                )

        trades.sort(key=lambda r: r.get("opened_at") or "", reverse=True)
        return trades[:n]

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

        # Keep fallback history warm when Binance is stale/disconnected so the
        # 5s/10s momentum checks still have usable samples.
        if not self._binance_is_fresh():
            fallback_price, fallback_source = await self._get_signal_price()
            if fallback_price is not None:
                print(
                    f"[bot] warmed fallback price history: ${fallback_price:,.2f} via {fallback_source}",
                    flush=True,
                )

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

        # ALLOWED_HOURS filter is disabled until we accumulate enough trades
        # (≥200) to validate per-hour edge with statistical confidence.
        # Re-enable once sample sizes support it.
        hour_et = (datetime.now(timezone.utc).hour - 4) % 24
        # if hour_et not in ALLOWED_HOURS:
        #     print(f"[bot] entry blocked: ET hour={hour_et} not in ALLOWED_HOURS")
        #     return

        # Scan active BTC short-horizon markets.
        markets = await self._fetch_active_markets()
        now = time.time()
        slugs = [m.get("slug", "?") for m in markets]
        print(f"[bot] _fetch_active_markets returned {len(markets)} markets: {slugs}", flush=True)

        for market in markets:
            slug = market.get("slug", "")
            if not slug:
                continue

            start_ts = _extract_start_ts(slug)
            end_ts = _market_end_ts(market)
            if start_ts is None or end_ts is None:
                continue

            seconds_remaining = end_ts - now
            print(f"[bot] market {slug} — {seconds_remaining:.1f}s remaining")
            if seconds_remaining < VOLUME_RETRY_LO:
                self._volume_retry_slugs.discard(slug)

            # Cache BTC price at first sight — used by reversal-guard as baseline
            if slug not in self._market_start_prices and slug not in self._entered_slugs:
                start_price, start_source = await self._get_signal_price()
                if start_price is not None:
                    self._market_start_prices[slug] = start_price
                    self._market_start_sources[slug] = start_source
                    print(
                        f"[bot] cached start price for {slug}: ${start_price:,.2f} via {start_source}",
                        flush=True,
                    )
                else:
                    print(f"[bot] BTC price unavailable — cannot cache start price for {slug}", flush=True)

            entry_lo, entry_hi = market.get("_sz_entry_window", (ENTRY_WINDOW_LO, ENTRY_WINDOW_HI))
            in_main_window = entry_lo <= seconds_remaining <= entry_hi
            volume_retry_enabled = bool(market.get("_sz_live_enabled", True))
            in_volume_retry_window = (
                volume_retry_enabled
                and VOLUME_RETRY_LO <= seconds_remaining < VOLUME_RETRY_HI
                and slug in self._volume_retry_slugs
            )
            # ── configured reversal window per market family
            if ((in_main_window or in_volume_retry_window)
                    and slug not in self._entered_slugs):
                strategy = market.get("_sz_strategy", "chainlink-reversal-guard")
                shadow_mode = market.get("_sz_mode") == "shadow"
                shadow_already_recorded = (
                    slug in self._shadow_entered_slugs
                    or db.shadow_trade_exists(slug, strategy if shadow_mode else "funding-neutral-shadow")
                )
                # Minimum volume filter — thin markets cause large slippage.
                # Only enforce/log this inside the entry window; early-window
                # volume is often low and can make normal markets look blocked.
                volume = float(market.get("volume") or 0)
                if volume < MIN_MARKET_VOLUME:
                    if in_main_window and volume_retry_enabled:
                        self._volume_retry_slugs.add(slug)
                    print(
                        f"[bot] SKIP: {slug} volume=${volume:.0f} < "
                        f"${MIN_MARKET_VOLUME:,.0f} minimum  secs={seconds_remaining:.1f}"
                        f"{' — eligible for late retry' if in_main_window and volume_retry_enabled else ''}",
                        flush=True,
                    )
                    continue
                if in_volume_retry_window:
                    print(
                        f"[bot] LATE RETRY: {slug} volume=${volume:.0f} now clears "
                        f"${MIN_MARKET_VOLUME:,.0f} minimum  secs={seconds_remaining:.1f}",
                        flush=True,
                    )

                if self._evaluating:
                    print("[bot] _tick: reversal check in progress — skipping reversal-guard", flush=True)
                    continue
                price_to_beat = self._market_start_prices.get(slug)
                price_source = self._market_start_sources.get(slug)
                print(
                    f"[bot] reversal-guard window: {slug}  {seconds_remaining:.1f}s "
                    f"(target {entry_lo}-{entry_hi}s)  "
                    f"ref=${price_to_beat:,.2f} ({price_source})" if price_to_beat else
                    f"[bot] reversal-guard window: {slug}  {seconds_remaining:.1f}s "
                    f"(target {entry_lo}-{entry_hi}s)  ref=None",
                    flush=True,
                )
                direction, signals = await self._evaluate_signals(market, price_to_beat, price_source)

                if direction is None:
                    print(f"[bot] SKIP: {slug} no clear signal — {signals}", flush=True)
                    continue

                entry_price = _side_price(market, direction)
                if not (CROWD_MIN <= entry_price <= CROWD_MAX):
                    print(
                        f"[bot] SKIP: {direction} price={entry_price:.3f} outside "
                        f"profitability range [{CROWD_MIN}, {CROWD_MAX}]",
                        flush=True,
                    )
                    db.log_bot_signal(slug, filter_hit="crowd_price", outcome="skipped",
                                      direction=direction, diff=signals.get("diff_initial"))
                    continue

                if shadow_mode or signals.get("shadow_only"):
                    if shadow_already_recorded:
                        print(f"[bot] SHADOW: {slug} already recorded — no duplicate", flush=True)
                    else:
                        self._record_shadow_entry(
                            slug=slug,
                            side=direction,
                            entry_price=entry_price,
                            end_ts=end_ts,
                            price_to_beat=signals.get("price_to_beat") or price_to_beat,
                            diff_at_entry=signals.get("diff_initial"),
                            seconds_remaining=seconds_remaining,
                            hour_et=hour_et,
                            strategy=strategy if shadow_mode else "funding-neutral-shadow",
                        )
                    continue

                self._market_start_prices.pop(slug, None)
                self._market_start_sources.pop(slug, None)
                self._entered_slugs.add(slug)
                self._volume_retry_slugs.discard(slug)
                diff_at_entry = signals.get("diff_initial")
                print(
                    f"[bot] reversal-guard SIGNAL PASSED: {direction} on {slug}  "
                    f"price={entry_price:.3f}  diff=${diff_at_entry:+.2f}  secs={seconds_remaining:.1f}",
                    flush=True,
                )
                await self._open_position(
                    slug, direction, entry_price, end_ts,
                    start_ts=start_ts,
                    price_to_beat=price_to_beat,
                    diff_at_entry=diff_at_entry,
                    seconds_remaining=seconds_remaining,
                    strategy=strategy,
                    hour_et=hour_et,
                    market=market,
                    live_enabled=bool(market.get("_sz_live_enabled", True)),
                )
                break

    def _get_measured_win_prob(self) -> float:
        """
        Return the win probability to use for Kelly sizing.

        Uses the measured win rate from resolved trades once we have at least
        MIN_KELLY_TRADES samples, shrunk toward 0.5 with a Bayesian prior of
        weight 10 (equivalent to 10 virtual 50/50 observations).

        Falls back to a conservative 0.52 (barely above breakeven) until we
        have enough data — avoids over-sizing on unvalidated edge.
        """
        total = self.wins + self.losses
        if total < 20:  # config.MIN_KELLY_TRADES
            print(
                f"[bot] Kelly: only {total} resolved trades — "
                f"using conservative WIN_PROB=0.52",
                flush=True,
            )
            return 0.52
        # Bayesian shrinkage toward 0.5 with prior weight 10
        prior_weight = 10
        shrunk = (self.wins + prior_weight * 0.5) / (total + prior_weight)
        print(
            f"[bot] Kelly: measured win_prob={self.wins}/{total}={self.wins/total:.3f} "
            f"→ shrunk={shrunk:.3f}",
            flush=True,
        )
        return shrunk

    # ── Active markets ─────────────────────────────────────────────────────────

    async def _fetch_active_markets(self) -> list:
        """
        Gamma API only supports exact slug lookups — a prefix query returns nothing.
        Short-horizon BTC market slugs are '{prefix}-{ts}' where ts is the
        market start timestamp. We compute the current and next window starts
        for each configured interval and fetch each by exact slug.
        """
        now = int(time.time())
        slugs: list[tuple[str, dict]] = []
        for spec in MARKET_FAMILIES:
            interval = spec["interval"]
            start = (now // interval) * interval
            slugs.extend([
                (f"{spec['slug_prefix']}-{start}", spec),
                (f"{spec['slug_prefix']}-{start + interval}", spec),
            ])
        print(f"[bot] fetching slugs: {[slug for slug, _ in slugs]}")

        session = await self._get_session()
        markets = []
        for slug, spec in slugs:
            try:
                async with session.get(
                    f"{GAMMA_API}/markets", params={"slug": slug}
                ) as resp:
                    if resp.status != 200:
                        print(f"[bot] gamma returned {resp.status} for {slug}")
                        continue
                    data = await resp.json()
                    if data and not data[0].get("closed", True):
                        market = data[0]
                        market["_sz_interval"] = spec["interval"]
                        market["_sz_strategy"] = spec["strategy"]
                        market["_sz_live_enabled"] = spec["live_enabled"]
                        market["_sz_label"] = spec["label"]
                        market["_sz_entry_window"] = spec["entry_window"]
                        market["_sz_mode"] = spec.get("mode", "paper")
                        markets.append(market)
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
        """Each tick: fetch finalPrice for recent closed BTC short-horizon markets.
        Cache is keyed by market slug because 5m and 15m markets can share a
        start timestamp but settle at different times."""
        now_ts   = int(time.time())
        print(f"[bot] finalPrice cache size={len(self._final_price_cache)}  keys={sorted(self._final_price_cache)}", flush=True)
        session  = await self._get_session()
        for spec in MARKET_FAMILIES:
            interval = spec["interval"]
            boundary = (now_ts // interval) * interval
            for i in range(1, 7):
                start_ts = boundary - i * interval
                slug = f"{spec['slug_prefix']}-{start_ts}"
                if slug in self._final_price_cache:
                    continue
                try:
                    async with session.get(
                        f"{GAMMA_API}/markets", params={"slug": slug, "closed": "true"}
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
                    if meta.get("finalPrice") is not None and slug not in self._final_price_cache:
                        fp = float(meta["finalPrice"])
                        self._final_price_cache[slug] = fp
                        print(f"[bot] finalPrice cache: {slug} start_ts={start_ts} → ${fp:,.2f}", flush=True)

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
        """Return the nearest recent BTC reading, preferring Binance history."""
        target = time.time() - n

        if self._binance_feed.connected:
            price = self._find_closest_price(self._binance_feed._price_history, target, max_gap=2.0)
            if price is not None:
                return price

        if not self._btc_price_timestamps:
            return None
        return self._find_closest_price(self._btc_price_timestamps, target, max_gap=10.0)

    def _find_closest_price(self, history, target: float, max_gap: float) -> Optional[float]:
        if not history:
            return None
        closest_ts, closest_price = min(history, key=lambda x: abs(x[0] - target))
        if abs(closest_ts - target) > max_gap:
            return None
        return closest_price

    def _binance_is_fresh(self) -> bool:
        if self._binance_feed.current_price is None or self._binance_feed.last_update is None:
            return False
        return (time.time() - self._binance_feed.last_update) <= BINANCE_STALE_AFTER

    async def _get_signal_price(self) -> tuple[Optional[float], str]:
        """Prefer Binance for fast signal reads, fall back to Chainlink when cold."""
        if self._binance_is_fresh():
            return self._binance_feed.current_price, "binance"

        chainlink_price = await self._get_btc_price()
        if chainlink_price is not None:
            return chainlink_price, "chainlink"
        return None, "none"

    async def _get_btc_funding_rate(self) -> Optional[float]:
        """Fetch BTC funding rate from public derivatives APIs and cache it briefly."""
        now = time.time()
        if self._funding_rate is not None and (now - self._funding_rate_updated_at) <= 30:
            return self._funding_rate

        session = await self._get_session()
        sources = (
            (
                "bybit",
                f"{BYBIT_API}/tickers",
                {"category": "linear", "symbol": "BTCUSDT"},
                self._parse_bybit_funding_rate,
            ),
            (
                "binance",
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                {"symbol": "BTCUSDT"},
                self._parse_binance_funding_rate,
            ),
        )

        for source_name, url, params, parser in sources:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        print(f"[bot] funding: {source_name} returned {resp.status}", flush=True)
                        continue
                    data = await resp.json()
                funding_rate = parser(data)
                if funding_rate is None:
                    print(f"[bot] funding: {source_name} returned no usable funding rate", flush=True)
                    continue
                self._funding_rate = funding_rate
                self._funding_rate_updated_at = now
                self._funding_rate_source = source_name
                return funding_rate
            except Exception as exc:
                print(f"[bot] funding fetch error from {source_name}: {exc}", flush=True)

        self._funding_rate = None
        self._funding_rate_source = None
        return None

    def _parse_bybit_funding_rate(self, data: dict) -> Optional[float]:
        try:
            tickers = data.get("result", {}).get("list", [])
            if not tickers:
                return None
            return float(tickers[0]["fundingRate"]) * 100
        except Exception as exc:
            print(f"[bot] funding parse error from bybit: {exc}", flush=True)
            return None

    def _parse_binance_funding_rate(self, data: dict) -> Optional[float]:
        try:
            return float(data["lastFundingRate"]) * 100
        except Exception as exc:
            print(f"[bot] funding parse error from binance: {exc}", flush=True)
            return None

    async def _evaluate_signals(
        self, market: dict, price_to_beat: Optional[float], price_source: Optional[str]
    ) -> tuple[Optional[str], dict]:
        """
        Two-condition entry:
        1. abs(live_price - reference_price) >= PRICE_DIFF_THRESHOLD
        2. After 3s re-check: move hasn't reversed and diff >= REVERSAL_THRESHOLD
        Reference price is previous window's finalPrice if cached, else Chainlink first-sight.
        """
        # Use previous same-interval market's finalPrice as reference if cached,
        # else fall back to first-sight price. The slug number is the market start_ts.
        slug = market["slug"]
        start_ts = _extract_start_ts(slug)
        if start_ts is None:
            db.log_bot_signal(slug, filter_hit="bad_slug_ts", outcome="skipped")
            return None, {"source": "none", "momentum": None}
        spec = _market_spec_for_slug(slug)
        interval = spec["interval"] if spec else int(market.get("_sz_interval") or 300)
        diff_threshold = float(spec.get("diff_threshold", PRICE_DIFF_THRESHOLD)) if spec else PRICE_DIFF_THRESHOLD
        reversal_threshold = float(spec.get("reversal_threshold", REVERSAL_THRESHOLD)) if spec else REVERSAL_THRESHOLD
        chop_window = float(spec.get("chop_window", 10)) if spec else 10.0
        momentum_window = float(spec.get("momentum_window", 5)) if spec else 5.0
        chop_min_move = float(spec.get("chop_min_move", MIN_MOMENTUM_MOVE)) if spec else MIN_MOMENTUM_MOVE
        prefix = spec["slug_prefix"] if spec else slug.rsplit("-", 1)[0]
        prev_slug = f"{prefix}-{int(start_ts - interval)}"
        prev_final  = self._final_price_cache.get(prev_slug)
        reference_price = prev_final if prev_final is not None else price_to_beat
        ref_source  = "prev-finalPrice" if prev_final is not None else f"{price_source or 'unknown'}-first-sight"

        if reference_price is None:
            print(f"[bot] SKIP: {slug} no reference price (no finalPrice cache, no Chainlink cache)", flush=True)
            db.log_bot_signal(slug, filter_hit="no_reference", outcome="skipped")
            return None, {"source": "none", "momentum": None}

        # Condition 0: minimum prev-window move — only enter if the reference window had momentum
        if prev_final is not None:
            prev_prev_slug = f"{prefix}-{int(start_ts - interval * 2)}"
            prev_prev_final = self._final_price_cache.get(prev_prev_slug)
            if prev_prev_final is not None:
                prev_move = abs(reference_price - prev_prev_final)
                if prev_move < MIN_PREV_MOVE:
                    print(
                        f"[bot] SKIP: {slug} prev_move ${prev_move:.2f} < "
                        f"MIN_PREV_MOVE ${MIN_PREV_MOVE:.0f} (flat reference window)",
                        flush=True,
                    )
                    db.log_bot_signal(slug, filter_hit="prev_move", outcome="skipped")
                    return None, {"source": "none", "momentum": None}

        self._evaluating = True
        try:
            live_price, live_source = await self._get_signal_price()
            if live_price is None:
                print(f"[bot] BTC: unavailable for {slug} — skipping, no trade", flush=True)
                db.log_bot_signal(slug, filter_hit="no_btc_price", outcome="skipped")
                return None, {"source": "none", "momentum": None}

            # Condition a: strong initial move from reference price
            diff_initial = live_price - reference_price
            print(
                f"[bot] signals: diff_initial=${diff_initial:+.2f}  "
                f"now=${live_price:,.2f} ({live_source})  ref=${reference_price:,.2f} ({ref_source})",
                flush=True,
            )
            direction = "Up" if diff_initial > 0 else "Down"
            if abs(diff_initial) < diff_threshold:
                print(
                    f"[bot] SKIP: {slug} {direction} diff ${abs(diff_initial):.2f} "
                    f"< threshold ${diff_threshold:.0f}",
                    flush=True,
                )
                db.log_bot_signal(slug, filter_hit="diff_threshold", outcome="skipped",
                                  direction=direction, diff=diff_initial)
                return None, {"source": "none", "momentum": None}

            funding_rate = await self._get_btc_funding_rate()
            if funding_rate is None:
                print(f"[bot] funding unavailable for {slug} — proceeding without funding filter", flush=True)
            else:
                if abs(funding_rate) < FUNDING_THRESHOLD:
                    print(
                        f"[bot] funding neutral: {slug} {funding_rate:+.4f}% inside ±{FUNDING_THRESHOLD:.2f}% — "
                        "proceeding without funding direction filter",
                        flush=True,
                    )
                    db.log_bot_signal(slug, filter_hit="funding_neutral_ignored", outcome="continued",
                                      direction=direction, diff=diff_initial)
                else:
                    funding_direction = "Up" if funding_rate > 0 else "Down"
                    if funding_direction != direction:
                        print(
                            f"[bot] SKIP (funding): {slug} signal={direction} "
                            f"but funding={funding_rate:+.4f}% ({funding_direction})",
                            flush=True,
                        )
                        db.log_bot_signal(slug, filter_hit="funding_conflict", outcome="skipped",
                                          direction=direction, diff=diff_initial)
                        return None, {"source": "none", "momentum": None}
                    print(f"[bot] funding OK: {slug} {funding_rate:+.4f}%  direction={direction}", flush=True)

            # Condition b1: chop filter — configured price range must show real movement
            price_chop_window_ago = self._get_price_n_seconds_ago(chop_window)
            chop_range = abs(live_price - price_chop_window_ago) if price_chop_window_ago is not None else None
            if price_chop_window_ago is not None:
                if chop_range < chop_min_move:
                    print(
                        f"[bot] SKIP (chop): {slug} {chop_window:.0f}s range=${chop_range:.2f} < ${chop_min_move:.0f}  "
                        f"now=${live_price:,.2f}  {chop_window:.0f}s_ago=${price_chop_window_ago:,.2f}",
                        flush=True,
                    )
                    db.log_bot_signal(slug, filter_hit="chop", outcome="skipped",
                                      direction=direction, diff=diff_initial, chop_range=chop_range)
                    return None, {"source": "none", "momentum": None}
            else:
                print(f"[bot] SKIP (chop): {slug} no {chop_window:.0f}s reading", flush=True)
                db.log_bot_signal(slug, filter_hit="no_chop_history", outcome="skipped",
                                  direction=direction, diff=diff_initial)
                return None, {"source": "none", "momentum": None}

            # Condition b2: momentum filter — configured momentum must agree with direction
            price_momentum_window_ago = self._get_price_n_seconds_ago(momentum_window)
            momentum = (live_price - price_momentum_window_ago) if price_momentum_window_ago is not None else None
            if price_momentum_window_ago is not None:
                if direction == "Up" and momentum < 0:
                    print(
                        f"[bot] SKIP (momentum): {slug} signal=Up but "
                        f"{momentum_window:.0f}s momentum=${momentum:+.2f} (falling)",
                        flush=True,
                    )
                    db.log_bot_signal(slug, filter_hit="momentum", outcome="skipped",
                                      direction=direction, diff=diff_initial,
                                      momentum=momentum, chop_range=chop_range)
                    return None, {"source": "none", "momentum": None}
                if direction == "Down" and momentum > 0:
                    print(
                        f"[bot] SKIP (momentum): {slug} signal=Down but "
                        f"{momentum_window:.0f}s momentum=${momentum:+.2f} (rising)",
                        flush=True,
                    )
                    db.log_bot_signal(slug, filter_hit="momentum", outcome="skipped",
                                      direction=direction, diff=diff_initial,
                                      momentum=momentum, chop_range=chop_range)
                    return None, {"source": "none", "momentum": None}
                print(f"[bot] momentum OK: {slug} {momentum_window:.0f}s=${momentum:+.2f}  direction={direction}", flush=True)
            else:
                print(f"[bot] SKIP (momentum): {slug} no {momentum_window:.0f}s reading", flush=True)
                db.log_bot_signal(slug, filter_hit="no_momentum_history", outcome="skipped",
                                  direction=direction, diff=diff_initial, chop_range=chop_range)
                return None, {"source": "none", "momentum": None}

            # Condition c: reversal guard — wait 3s, re-fetch, confirm move still holding
            await asyncio.sleep(3)
            live_price_b, confirm_source = await self._get_signal_price()
            if live_price_b is None:
                print(f"[bot] SKIP: {slug} reversal check fetch failed", flush=True)
                db.log_bot_signal(slug, filter_hit="reversal_fetch_failed", outcome="skipped",
                                  direction=direction, diff=diff_initial,
                                  momentum=momentum, chop_range=chop_range)
                return None, {"source": "none", "momentum": None}

            diff_final = live_price_b - reference_price
            reversed_direction = (diff_final > 0) != (diff_initial > 0)
            if reversed_direction or abs(diff_final) < reversal_threshold:
                print(
                    f"[bot] SKIP: {slug} momentum faded/reversed — "
                    f"initial=${diff_initial:+.2f}  now=${diff_final:+.2f}",
                    flush=True,
                )
                db.log_bot_signal(slug, filter_hit="reversal", outcome="skipped",
                                  direction=direction, diff=diff_initial,
                                  momentum=momentum, chop_range=chop_range)
                return None, {"source": "none", "momentum": None}

            print(
                f"[bot] ENTRY: {direction}  diff_initial=${diff_initial:+.2f}  "
                f"diff_final=${diff_final:+.2f}  ref=${reference_price:,.2f} ({ref_source})",
                flush=True,
            )
            db.log_bot_signal(slug, filter_hit="none", outcome="entered",
                              direction=direction, diff=diff_initial,
                              momentum=momentum, chop_range=chop_range)
            return direction, {
                "source":        f"{live_source}-reversal-guard/{ref_source}",
                "confirm_source": confirm_source,
                "funding_rate":  funding_rate,
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

    def _shadow_stake(self, entry_price: float, hour_et: int = 0) -> tuple[float, float, float]:
        """Mirror paper sizing without reserving balance or placing orders."""
        win_prob       = self._get_measured_win_prob()
        loss_prob      = 1.0 - win_prob
        b              = (1.0 / entry_price) - 1.0
        kelly_fraction = max(0.0, (win_prob * b - loss_prob) / b) if b > 0 else 0.0
        multiplier     = min(1.0, HOUR_MULTIPLIER.get(hour_et, 1.0))
        stake          = min(MAX_STAKE * multiplier, self.balance * kelly_fraction * 0.5 * multiplier)
        return stake, kelly_fraction, multiplier

    def _record_shadow_entry(
        self,
        slug: str,
        side: str,
        entry_price: float,
        end_ts: float,
        price_to_beat: Optional[float] = None,
        diff_at_entry: Optional[float] = None,
        seconds_remaining: Optional[float] = None,
        hour_et: int = 0,
        strategy: str = "funding-neutral-shadow",
    ) -> None:
        """Record a hypothetical shadow trade without touching paper/live accounting."""
        stake, kelly_fraction, multiplier = self._shadow_stake(entry_price, hour_et)
        if stake < MIN_STAKE:
            print(
                f"[bot] SHADOW SKIP: Kelly stake ${stake:.2f} below ${MIN_STAKE:.2f} minimum",
                flush=True,
            )
            db.log_bot_signal(slug, filter_hit="funding_neutral_shadow_size", outcome="skipped",
                              direction=side, diff=diff_at_entry)
            return

        inserted = db.log_shadow_trade(
            {
                "whale_address": STRATEGY_TAG,
                "market_slug": slug,
                "side": side,
                "size": stake,
                "entry_price": entry_price,
                "price_to_beat": price_to_beat,
                "diff_at_entry": diff_at_entry,
                "seconds_remaining": seconds_remaining,
                "strategy": strategy,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._shadow_entered_slugs.add(slug)
        self._market_start_prices.pop(slug, None)
        self._market_start_sources.pop(slug, None)

        if not inserted:
            print(f"[bot] SHADOW: {slug} already exists in DB — no duplicate", flush=True)
            return

        print(
            f"[bot] SHADOW OPEN {side:>4} ${stake:.2f} "
            f"(kelly={kelly_fraction:.3f} ×{multiplier}) strategy={strategy} "
            f"slug={slug} entry={entry_price:.3f}",
            flush=True,
        )
        asyncio.create_task(self._backfill_shadow_trade(slug, end_ts))

    async def _backfill_shadow_trade(self, slug: str, end_ts: float) -> None:
        """Background task: resolve a shadow trade without touching accounting."""
        wait = max(0.0, (end_ts + 900) - time.time())
        print(f"[bot] shadow backfill: waiting {wait:.0f}s to fetch finalPrice for {slug}", flush=True)
        await asyncio.sleep(wait)

        for attempt in range(3):
            try:
                winner, final_price, poly_price_to_beat = await self._resolve_market(slug, end_ts)
                if winner is not None:
                    settled = db.settle_unresolved_shadow_trade(
                        slug,
                        winner,
                        resolution_price=final_price,
                        poly_price_to_beat=poly_price_to_beat,
                    )
                    if settled:
                        print(
                            f"[bot] shadow backfill: {slug} settled → {winner} "
                            f"pnl=${settled['pnl']:+.2f}",
                            flush=True,
                        )
                    return
                print(
                    f"[bot] shadow backfill: attempt {attempt + 1}/3 — "
                    f"resolution not yet available for {slug}",
                    flush=True,
                )
            except Exception as exc:
                print(f"[bot] shadow backfill error ({slug}) attempt {attempt + 1}/3: {exc}", flush=True)
            await asyncio.sleep(300)

        print(f"[bot] shadow backfill: gave up on finalPrice for {slug}", flush=True)

    async def _open_position(self, slug: str, side: str, entry_price: float, end_ts: float,
                             start_ts: Optional[float] = None,
                             price_to_beat: Optional[float] = None,
                             diff_at_entry: Optional[float] = None,
                             seconds_remaining: Optional[float] = None,
                             strategy: Optional[str] = None,
                             hour_et: int = 0,
                             market: Optional[dict] = None,
                             live_enabled: bool = True):
        # Half-Kelly sizing with data-driven hour multiplier.
        # WIN_PROB is derived from measured trade history (or conservative
        # default of 0.52 when sample size is too small).
        win_prob       = self._get_measured_win_prob()
        loss_prob      = 1.0 - win_prob
        b              = (1.0 / entry_price) - 1.0   # net payout per dollar risked
        kelly_fraction = max(0.0, (win_prob * b - loss_prob) / b) if b > 0 else 0.0
        multiplier     = min(1.0, HOUR_MULTIPLIER.get(hour_et, 1.0))
        max_stake      = MAX_STAKE * multiplier
        raw_stake      = self.balance * kelly_fraction * 0.5 * multiplier
        kelly_stake    = min(max_stake, raw_stake)
        stake          = _round_order_size(max(MIN_STAKE, kelly_stake))

        if max_stake < MIN_STAKE:
            print(
                f"[bot] OPEN SKIP: {slug} {side} entry={entry_price:.3f} "
                f"max stake ${max_stake:.2f} below ${MIN_STAKE:.2f} minimum "
                f"({MIN_STAKE_PCT:.0%} of ${INITIAL_WALLET_SIZE:.0f})",
                flush=True,
            )
            return
        if kelly_stake < MIN_STAKE:
            print(
                f"[bot] OPEN: {slug} {side} entry={entry_price:.3f} "
                f"using minimum stake ${stake:.2f}; Kelly suggested ${kelly_stake:.2f}",
                flush=True,
            )

        if self.balance < stake:
            print(f"[bot] OPEN SKIP: {slug} insufficient balance (${self.balance:.2f})", flush=True)
            return

        # ── Live order (best-effort alongside paper) ───────────────────────────
        live_order = None
        live_should_retry = False
        if os.getenv("POLYMARKET_LIVE", "false").lower() == "true" and live_enabled:
            if market is None:
                print(f"[bot] LIVE: no market dict for {slug} — paper only", flush=True)
            else:
                try:
                    from . import live_clob
                    live_slippage = float(os.getenv("LIVE_MAX_SLIPPAGE", "0.05"))
                    live_cap = min(CROWD_MAX, entry_price + live_slippage)
                    live_retries = max(0, int(os.getenv("LIVE_FAK_RETRIES", "1")))
                    live_retry_delay = max(0.0, float(os.getenv("LIVE_FAK_RETRY_DELAY_SEC", "1.0")))

                    for attempt in range(live_retries + 1):
                        try:
                            live_order = await live_clob.place_order(
                                market,
                                side,
                                stake,
                                max_fill_price=live_cap,
                            )
                            break
                        except Exception as exc:
                            order_type = os.getenv("LIVE_ORDER_TYPE", "FAK").upper()
                            no_fak_match = "no orders found to match with FAK order" in str(exc)
                            can_retry = order_type == "FAK" and no_fak_match and attempt < live_retries
                            if not can_retry:
                                raise
                            print(
                                f"[bot] LIVE: FAK no-fill for {slug}; retrying "
                                f"{attempt + 1}/{live_retries} in {live_retry_delay:.1f}s "
                                f"at cap={live_cap:.4f}",
                                flush=True,
                            )
                            if live_retry_delay:
                                await asyncio.sleep(live_retry_delay)

                    if live_order is None:
                        raise RuntimeError("live retry loop exited without an order")

                    self.live_balance -= live_order["stake"]
                    print(
                        f"[bot] LIVE: filled orderID={live_order['order_id']}  "
                        f"stake=${live_order['stake']:.2f}  price={live_order['fill_price']:.4f}  "
                        f"cap={live_order.get('max_fill_price', live_cap):.4f}  "
                        f"tx={live_order['tx_hash']}",
                        flush=True,
                    )
                except Exception as exc:
                    print(f"[bot] LIVE: order failed for {slug} ({exc}) — paper only", flush=True)
                    live_should_retry = True
        elif os.getenv("POLYMARKET_LIVE", "false").lower() == "true" and not live_enabled:
            print(f"[bot] LIVE: disabled for {slug} strategy={strategy} — paper/shadow only", flush=True)

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
            "live_order_id":     live_order["order_id"] if live_order else None,
            "live_stake":        live_order["stake"]    if live_order else None,
            "live_fill_price":   live_order["fill_price"] if live_order else None,
        }
        self.positions.append(pos)
        db.save_open_position(pos)
        db.save_bot_state(self.balance)
        if live_should_retry:
            asyncio.create_task(self._retry_live_fill(pos, market, side, stake, entry_price))
        if start_ts is None:
            start_ts = _extract_start_ts(slug)
        win_start = datetime.utcfromtimestamp(start_ts).strftime("%H:%M") if start_ts is not None else "?"
        win_end   = datetime.utcfromtimestamp(end_ts).strftime("%H:%M")
        print(
            f"[bot] OPEN  {side:>4}  ${stake:.0f} (kelly={kelly_fraction:.3f} ×{multiplier})  slug={slug}"
            f"  window={win_start}→{win_end} UTC  end_ts={int(end_ts)}"
            f"  entry={entry_price:.3f}  balance=${self.balance:.2f}  "
            f"positions={len(self.positions)}",
            flush=True,
        )

    async def _retry_live_fill(
        self,
        pos: dict,
        market: Optional[dict],
        side: str,
        stake: float,
        paper_entry_price: float,
    ) -> None:
        """Keep trying to attach a live fill to an already-open paper position."""
        slug = pos["market_slug"]
        if market is None or slug in self._live_retry_slugs:
            return

        max_profit_price = float(os.getenv("LIVE_RETRY_MAX_PRICE", "0.65"))
        live_slippage = float(os.getenv("LIVE_MAX_SLIPPAGE", "0.05"))
        retry_interval = max(0.5, float(os.getenv("LIVE_RETRY_INTERVAL_SEC", "3.0")))
        stop_before_close = max(0.0, float(os.getenv("LIVE_RETRY_STOP_BEFORE_CLOSE_SEC", "20.0")))
        max_attempts = max(0, int(os.getenv("LIVE_RETRY_MAX_ATTEMPTS", "12")))
        live_cap = min(CROWD_MAX, max_profit_price, paper_entry_price + live_slippage)

        if live_cap < CROWD_MIN:
            print(
                f"[bot] LIVE RETRY: disabled for {slug}; cap={live_cap:.4f} below CROWD_MIN={CROWD_MIN:.2f}",
                flush=True,
            )
            return

        self._live_retry_slugs.add(slug)
        try:
            for attempt in range(1, max_attempts + 1):
                if time.time() >= (float(pos["end_ts"]) - stop_before_close):
                    print(f"[bot] LIVE RETRY: stop for {slug}; too close to close", flush=True)
                    return

                current_pos = next((p for p in self.positions if p["market_slug"] == slug), None)
                if current_pos is None or current_pos.get("live_order_id"):
                    return

                try:
                    from . import live_clob
                    live_order = await live_clob.place_order(
                        market,
                        side,
                        stake,
                        max_fill_price=live_cap,
                    )
                except Exception as exc:
                    print(
                        f"[bot] LIVE RETRY: {slug} attempt {attempt}/{max_attempts} failed "
                        f"cap={live_cap:.4f}: {exc}",
                        flush=True,
                    )
                    await asyncio.sleep(retry_interval)
                    continue

                current_pos["live_order_id"] = live_order["order_id"]
                current_pos["live_stake"] = live_order["stake"]
                current_pos["live_fill_price"] = live_order["fill_price"]
                db.save_open_position(current_pos)
                self.live_balance -= live_order["stake"]
                print(
                    f"[bot] LIVE RETRY FILLED {side:>4} {slug} "
                    f"stake=${live_order['stake']:.2f} price={live_order['fill_price']:.4f} "
                    f"cap={live_cap:.4f} tx={live_order['tx_hash']}",
                    flush=True,
                )
                return

            print(f"[bot] LIVE RETRY: gave up on {slug} after {max_attempts} attempts", flush=True)
        finally:
            self._live_retry_slugs.discard(slug)

    async def _force_close(self, pos: dict):
        """Close a position that never received a winner from Polymarket.
        Stake is returned to balance — outcome recorded as 'unresolved'."""
        self.positions = [p for p in self.positions if p["market_slug"] != pos["market_slug"]]
        db.clear_open_position(pos["market_slug"])

        self.balance += pos["size"]
        if pos.get("live_order_id") and pos.get("live_stake"):
            self.live_balance += pos["live_stake"]
        closed_at = datetime.now(timezone.utc).isoformat()
        print(
            f"[bot] FORCE-CLOSE refund ${pos['size']:.0f} → balance=${self.balance:.2f}",
            flush=True,
        )

        try:
            conn = db.get_connection()
            _INSERT = """INSERT INTO bot_trades
                   (whale_address, market_slug, side, size, entry_price,
                    price_to_beat, outcome, pnl, balance_after, diff_at_entry,
                    seconds_remaining, strategy, opened_at, closed_at, mode)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            base = (
                STRATEGY_TAG, pos["market_slug"], pos["side"],
                round(pos.get("entry_price") or 0.5, 4),
                round(pos["price_to_beat"], 2) if pos.get("price_to_beat") else None,
                "unresolved", 0.0,
                round(pos["diff_at_entry"], 2) if pos.get("diff_at_entry") is not None else None,
                round(pos["seconds_remaining"], 1) if pos.get("seconds_remaining") is not None else None,
                pos.get("strategy"), pos["opened_at"], closed_at,
            )
            conn.execute(_INSERT, base[:2] + (pos["side"], pos["size"],) + base[3:] + (round(self.balance, 2), "paper",))
            if pos.get("live_order_id") and pos.get("live_stake"):
                conn.execute(_INSERT, base[:2] + (pos["side"], pos["live_stake"],) + base[3:] + (round(self.live_balance, 4), "live",))
            conn.commit()
            conn.close()
            db.save_bot_state(self.balance)
            asyncio.create_task(
                self._backfill_resolution_price(pos["market_slug"], pos["end_ts"])
            )
        except Exception as exc:
            import traceback
            print(f"[bot] DB write failed after force-close: {exc}\n{traceback.format_exc()}", flush=True)

    async def _backfill_resolution_price(self, slug: str, end_ts: float):
        """Background task: wait until resolution data is available, then settle."""
        wait = max(0.0, (end_ts + 900) - time.time())
        print(f"[bot] backfill: waiting {wait:.0f}s to fetch finalPrice for {slug}", flush=True)
        await asyncio.sleep(wait)

        for attempt in range(3):
            try:
                winner, final_price, poly_price_to_beat = await self._resolve_market(slug, end_ts)
                if winner is not None:
                    settled = db.settle_unresolved_bot_trade(
                        slug,
                        winner,
                        resolution_price=final_price,
                        poly_price_to_beat=poly_price_to_beat,
                    )
                    if settled:
                        self._reload_accounting_state()
                        print(
                            f"[bot] backfill: {slug} settled → {winner} pnl=${settled['pnl']:+.2f}",
                            flush=True,
                        )
                        return
                    if final_price is not None:
                        db.update_resolution_price(slug, float(final_price))
                    print(
                        f"[bot] backfill: {slug} → finalPrice={float(final_price):.2f} written",
                        flush=True,
                    )
                    return
                print(
                    f"[bot] backfill: attempt {attempt + 1}/3 — resolution not yet available for {slug}",
                    flush=True,
                )
            except Exception as exc:
                print(f"[bot] backfill error ({slug}) attempt {attempt + 1}/3: {exc}", flush=True)
            await asyncio.sleep(300)  # wait 5 more minutes before next attempt

        print(f"[bot] backfill: gave up on finalPrice for {slug}", flush=True)

    def _reload_accounting_state(self) -> None:
        state = db.load_bot_state()
        performance = db.load_bot_performance()
        self.balance = state["balance"]
        self.wins = performance["wins"]
        self.losses = performance["losses"]
        self.total_pnl = performance["total_pnl"]
        db.save_bot_state(self.balance)
        print(
            f"[bot] accounting reload: balance=${self.balance:.2f} "
            f"wins={self.wins} losses={self.losses} pnl=${self.total_pnl:.2f}",
            flush=True,
        )

    async def _reconcile_unresolved_trades(self) -> None:
        unresolved = db.load_unresolved_bot_trades()
        if not unresolved:
            return
        print(f"[bot] reconcile: checking {len(unresolved)} unresolved trades", flush=True)
        settled_count = 0
        for row in unresolved:
            slug = row["market_slug"]
            start_ts = _extract_start_ts(slug)
            if start_ts is None:
                continue
            end_ts = start_ts + float(_market_interval(slug))
            winner, final_price, poly_price_to_beat = await self._resolve_market(slug, end_ts)
            if winner is None:
                continue
            settled = db.settle_unresolved_bot_trade(
                slug,
                winner,
                resolution_price=final_price,
                poly_price_to_beat=poly_price_to_beat,
            )
            if settled:
                settled_count += 1
                print(
                    f"[bot] reconcile: {slug} settled → {winner} pnl=${settled['pnl']:+.2f}",
                    flush=True,
                )
        if settled_count:
            self._reload_accounting_state()

    async def _reconcile_unresolved_shadow_trades(self) -> None:
        unresolved = db.load_unresolved_shadow_trades()
        if not unresolved:
            return
        print(f"[bot] shadow reconcile: checking {len(unresolved)} unresolved shadow trades", flush=True)
        settled_count = 0
        for row in unresolved:
            slug = row["market_slug"]
            self._shadow_entered_slugs.add(slug)
            start_ts = _extract_start_ts(slug)
            if start_ts is None:
                continue
            end_ts = start_ts + float(_market_interval(slug))
            winner, final_price, poly_price_to_beat = await self._resolve_market(slug, end_ts)
            if winner is None:
                continue
            settled = db.settle_unresolved_shadow_trade(
                slug,
                winner,
                resolution_price=final_price,
                poly_price_to_beat=poly_price_to_beat,
            )
            if settled:
                settled_count += 1
                print(
                    f"[bot] shadow reconcile: {slug} settled → {winner} "
                    f"pnl=${settled['pnl']:+.2f}",
                    flush=True,
                )
        if settled_count:
            print(f"[bot] shadow reconcile: settled {settled_count} trades", flush=True)

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
                f"{GAMMA_API}/markets", params={"slug": slug, "closed": "true"}
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

        events = m.get("events", [])
        meta   = (events[0].get("eventMetadata") or {}) if events else {}
        print(
            f"[bot] resolve raw: slug={slug}  events={len(events)}  "
            f"meta={json.dumps(meta)}  winner_field={m.get('winner')!r}  "
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

            # 25+ min elapsed — finalPrice isn't coming; fall back to outcomePrices.
            # Order is not assumed — label comes from outcomes[i].
            try:
                outcomes       = json.loads(m.get("outcomes", "[]"))
                outcome_prices = json.loads(m.get("outcomePrices", "[]"))
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

            # outcomePrices gives us the winner but not the actual settlement BTC price.
            # Pull it from _final_price_cache if _collect_final_prices() already fetched it.
            if final_price is None:
                cached_fp = self._final_price_cache.get(slug)
                if cached_fp is not None:
                    final_price = cached_fp
                    print(
                        f"[bot] resolve: pulled finalPrice=${final_price:,.2f} "
                        f"from _final_price_cache for {slug}",
                        flush=True,
                    )

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

        # ── Paper P&L ─────────────────────────────────────────────────────────
        if won:
            pnl = size * (1.0 / entry_price - 1.0)
            self.balance += size + pnl
            self.wins += 1
        else:
            pnl = -size
            self.losses += 1
        self.total_pnl += pnl

        # ── Live P&L (if this position had a real order) ───────────────────────
        live_pnl = None
        if pos.get("live_order_id"):
            live_size  = pos["live_stake"]
            live_price = pos.get("live_fill_price") or entry_price
            if live_price <= 0:
                live_price = entry_price
            if won:
                live_pnl = live_size * (1.0 / live_price - 1.0)
                self.live_balance += live_size + live_pnl
                self.live_wins += 1
            else:
                live_pnl = -live_size
                self.live_losses += 1
            self.live_pnl += live_pnl

        closed_at = datetime.now(timezone.utc).isoformat()

        print(
            f"[bot] CLOSE {pos['side']:>4}  winner={winner}"
            f"  paper=${pnl:+.2f} (bal=${self.balance:.2f})"
            + (f"  live=${live_pnl:+.4f} (bal=${self.live_balance:.4f})" if live_pnl is not None else ""),
            flush=True,
        )

        try:
            conn = db.get_connection()
            common = (
                pos["market_slug"], pos["side"],
                round(pos.get("price_to_beat"), 2) if pos.get("price_to_beat") else None,
                round(poly_price_to_beat, 2) if poly_price_to_beat else None,
                round(resolution_price, 2) if resolution_price else None,
                winner,
                round(pos["diff_at_entry"], 2) if pos.get("diff_at_entry") is not None else None,
                round(pos["seconds_remaining"], 1) if pos.get("seconds_remaining") is not None else None,
                pos.get("strategy"),
                pos["opened_at"],
                closed_at,
            )
            _INSERT = """INSERT INTO bot_trades
                   (market_slug, side, price_to_beat, poly_price_to_beat, resolution_price,
                    outcome, diff_at_entry, seconds_remaining, strategy, opened_at, closed_at,
                    whale_address, size, entry_price, pnl, balance_after, mode)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            # Paper row
            conn.execute(_INSERT, common + (
                STRATEGY_TAG, round(size, 4), round(entry_price, 4),
                round(pnl, 2), round(self.balance, 2), "paper",
            ))
            # Live row (only if a real order was placed)
            if pos.get("live_order_id") and live_pnl is not None:
                live_size  = pos["live_stake"]
                live_price = pos.get("live_fill_price") or entry_price
                conn.execute(_INSERT, common + (
                    STRATEGY_TAG, round(live_size, 4), round(live_price, 4),
                    round(live_pnl, 4), round(self.live_balance, 4), "live",
                ))
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

def _extract_start_ts(slug: str) -> Optional[float]:
    """Extract Unix market start timestamp from 'btc-updown-5m-1774383000'."""
    try:
        return float(slug.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return None


def _market_spec_for_slug(slug: str) -> Optional[dict]:
    for spec in MARKET_FAMILIES:
        if slug.startswith(f"{spec['slug_prefix']}-"):
            return spec
    return None


def _market_interval(slug: str) -> int:
    spec = _market_spec_for_slug(slug)
    return int(spec["interval"]) if spec else 300


def _market_end_ts(market: dict) -> Optional[float]:
    """Return market close timestamp from API payload, falling back to start+interval."""
    end_date = market.get("endDate")
    if isinstance(end_date, str) and end_date:
        try:
            return datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    start_ts = _extract_start_ts(market.get("slug", ""))
    interval = int(market.get("_sz_interval") or _market_interval(market.get("slug", "")))
    return (start_ts + float(interval)) if start_ts is not None else None


def _side_price(market: dict, side: str) -> float:
    """
    Return the current market price for 'Up' or 'Down'.
    outcomePrices and outcomes come back as JSON strings from the Gamma API.
    """
    try:
        outcomes_raw = market.get("outcomes",      '["Up","Down"]')
        prices_raw   = market.get("outcomePrices", "[0.5,0.5]")
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        prices   = json.loads(prices_raw)   if isinstance(prices_raw,   str) else prices_raw
        return float(prices[outcomes.index(side)])
    except Exception:
        return 0.5


# ── Module-level singleton used by api.py ─────────────────────────────────────
bot = PaperBot()
