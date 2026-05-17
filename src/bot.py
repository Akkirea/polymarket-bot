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
from . import db_shadow
from .instrumentation.attribution import attribution  # observability-only; removable
from .binance_ws import BinancePriceFeed, OrderBookFeed
from .chainlink import get_btc_price, get_price_at_ts as chainlink_price_at_ts
from .rtds_ws import PolymarketRtdsPriceFeed
from .feeds.clob_book_ws import ClobBookFeed
from .feeds.clob_user_ws import ClobUserFeed
from .exec.router import ExecutionRouter
from .exec.shadow_sim import ShadowFillSimulator
from .exec import shadow_clob

# ── Constants ──────────────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
BYBIT_API = "https://api.bybit.com/v5/market"

INITIAL_BALANCE      = db.INITIAL_BALANCE  # keep in sync with db.py
INITIAL_WALLET_SIZE  = 97.0
MAX_STAKE_PCT        = 0.02   # half-Kelly at 52% win prob and 0.50 entry
MIN_STAKE_PCT        = 0.01   # skip smaller signals instead of forcing oversize bets
LIVE_MIN_ORDER_SIZE  = 1.00   # Polymarket marketable BUY minimum
CLOB_MIN_SHARES      = 5      # Polymarket CLOB rejects orders with fewer than 5 shares
ORDER_SIZE_INCREMENT = 0.01   # Keep cent precision; MIN_STAKE protects Polymarket's $1 floor
MAX_STAKE            = INITIAL_WALLET_SIZE * MAX_STAKE_PCT  # legacy fallback; runtime uses self.balance
MIN_STAKE            = max(LIVE_MIN_ORDER_SIZE, INITIAL_WALLET_SIZE * MIN_STAKE_PCT)  # legacy fallback
WIN_PROB             = 0.60   # conservative win rate estimate — update after 200 trades
MIN_PREV_MOVE        = 25.0   # USD — skip if the reference window moved less than this
# Data-validated allowed hours (ET = UTC-4). All others blocked.
# Hours 18+19 statistically significant (p=0.009 combined). Hours 0,4 solid (n=13-15).
ALLOWED_HOURS        = {0, 1, 4, 5, 18, 19}
# Hour 19: 2x (70.6% WR, n=17, p<0.01). Hour 18: 1x until n≥20 (77.8% WR but n=9).
HOUR_MULTIPLIER      = {19: 2.0, 0: 1.0, 4: 1.0, 5: 1.0, 1: 0.75, 18: 1.0}
# Hours blocked from live execution (shadow continues at all hours for data collection).
# Derived from shadow WR with n≥5: hour 0 (55% WR n=20), 3 (62.5% n=8), 11 (57.1% n=7).
LIVE_BLOCKED_HOURS   = {0, 3, 11}
# Minimum HTF trend (10-min BTC move) that blocks a trade if it opposes the signal direction.
HTF_TREND_THRESHOLD  = 25.0
POLL_INTERVAL        = 3     # seconds between ticks
ENTRY_WINDOW_LO      = 75    # enter when seconds_remaining >= this
ENTRY_WINDOW_HI      = 100   # enter when seconds_remaining <= this
EARLY_LIVE_WINDOW_LO = 150   # stricter live-only detector starts here
EARLY_LIVE_WINDOW_HI = 240
VOLUME_RETRY_LO      = 60    # late retry only while entries still have enough time before close
VOLUME_RETRY_HI      = ENTRY_WINDOW_LO
MIN_MOMENTUM_MOVE    = 10.0  # USD — chop filter: abs(live - price_10s_ago) must exceed this
FUNDING_THRESHOLD    = 0.02  # % — outside this band, funding must agree with direction
PRICE_DIFF_THRESHOLD      = 10.0  # USD — shadow/paper entry gate (kept low for data)
LIVE_DIFF_THRESHOLD_BASE  = 35.0  # USD — live entry base: $35 at 60s, +$0.25/s after
LIVE_DIFF_THRESHOLD_MAX   = 75.0  # USD — cap so very early entries aren't blocked forever
REVERSAL_THRESHOLD        = 8.0   # USD — diff must still be >= this after 3s re-check
CROWD_MIN            = 0.30  # outcomePrices lower bound — below this crowd is 70%+ against us
CROWD_MAX            = 0.70  # outcomePrices upper bound — above this move is fully priced in
EARLY_LIVE_MAX_PRICE = 0.60  # early entries need better risk/reward than regular entries
LIVE_MIN_SECONDS_BEFORE_CLOSE = 60.0  # avoid last-minute live fills with collapsed upside
PREV_FINAL_MAX_AGE_SEC = 600.0  # previous exact market finalPrice can seed the next 5m beat
BINANCE_STALE_AFTER  = 65.0  # seconds — keep Binance history usable for 15m's 30s/60s checks
MIN_MARKET_VOLUME    = 2000.0  # USDC — only enforced during the entry window
MIN_SHADOW_VOLUME    = 500.0   # USDC — minimum volume for strategy research shadows
SHADOW_MAX_FOLLOW_PRICE = 0.67
LOCAL_PREVCLOSE_SHADOW_STRATEGY = "btc5-local-prevclose-shadow"
RTDS_PREVCLOSE_SHADOW_STRATEGY = "btc5-rtds-prevclose-shadow"
RTDS_LIVE_FALLBACK_SOURCE = "rtds-live-fallback"
RTDS_LIVE_FALLBACK_ENABLED = os.getenv("RTDS_LIVE_FALLBACK_ENABLED", "true").lower() == "true"
# Kline (Binance 5m open) and RTDS prices are consistently ~$33/$30 ABOVE the actual
# Chainlink Data Streams price Polymarket uses for settlement. Subtract this correction
# so the reference used for diff and strategy tag reflects the real expected beat.
KLINE_BIAS_CORRECTION = float(os.getenv("KLINE_BIAS_CORRECTION", "33.0"))
RTDS_BIAS_CORRECTION  = float(os.getenv("RTDS_BIAS_CORRECTION",  "42.0"))
RTDS_LIVE_UP_DIFF        = float(os.getenv("RTDS_LIVE_UP_DIFF",          "30"))
RTDS_LIVE_DOWN_DIFF      = float(os.getenv("RTDS_LIVE_DOWN_DIFF",        "40"))
RTDS_LIVE_UP_CROWD_CAP   = float(os.getenv("RTDS_LIVE_UP_CROWD_CAP",    "0.55"))
RTDS_LIVE_DOWN_CROWD_FLOOR = float(os.getenv("RTDS_LIVE_DOWN_CROWD_FLOOR", "0.45"))
PRE_SIGNAL_ENABLED     = os.getenv("PRE_SIGNAL_ENABLED", "false").lower() == "true"
PRE_SIGNAL_DIFF        = float(os.getenv("PRE_SIGNAL_DIFF",        "22"))
PRE_SIGNAL_LIMIT_PRICE = float(os.getenv("PRE_SIGNAL_LIMIT_PRICE", "0.50"))
PRE_SIGNAL_CANCEL_DIFF = float(os.getenv("PRE_SIGNAL_CANCEL_DIFF", "13"))
LAG_FOLLOW_LIVE_ENABLED = os.getenv("LAG_FOLLOW_LIVE_ENABLED", "false").lower() == "true"
LAG_FOLLOW_LIVE_MAX_PRICE = float(os.getenv("LAG_FOLLOW_LIVE_MAX_PRICE", "0.62"))
EARLY_SHADOW_WINDOW  = (150, 240)
RTDS_PREVCLOSE_LIVE_ENABLED = os.getenv("RTDS_PREVCLOSE_LIVE_ENABLED", "false").lower() == "true"
HEDGED_SHADOW_WINDOW = (150, 300)
HEDGED_DOMINANT_MIN  = 0.45
HEDGED_DOMINANT_MAX  = 0.67
HEDGED_HEDGE_MAX     = 0.10
OB_IMBALANCE_THRESHOLD = float(os.getenv("OB_IMBALANCE_THRESHOLD", "0.5"))
OB_SUSTAINED_SECONDS   = float(os.getenv("OB_SUSTAINED_SECONDS",   "5.0"))
OB_SHADOW_WINDOW       = (60, 210)  # wider than lag-follow; leading signal fires early

# ── Execution mode ────────────────────────────────────────────────────────────
# Values: shadow_only | maker_shadow | maker_live
EXEC_MODE = os.getenv("EXEC_MODE", "shadow_only").lower()
if EXEC_MODE.startswith("maker_"):
    # Pre-signal limit path is not migrated; force-disable when in maker modes.
    os.environ["PRE_SIGNAL_ENABLED"] = "false"

STRATEGY_TAG = "SIGNAL_STRATEGY"  # stored in whale_address column (NOT NULL)
LIVE_INITIAL_BALANCE = float(os.getenv("LIVE_INITIAL_BALANCE", "8.45"))  # starting pUSD on-chain


def _SHADOW_HARD_STOP(reason: str) -> None:
    """Emit a loud alert and abort the process immediately.

    Called when a maker_shadow invariant is violated. os.abort() generates a
    core-dump signal (SIGABRT) so Railway restarts the container and the incident
    is visible in logs and exit-code monitoring.
    """
    msg = f"\n{'='*70}\n[SHADOW HARD STOP] {reason}\n{'='*70}\n"
    print(msg, flush=True)
    import sys
    sys.stderr.write(msg)
    sys.stderr.flush()
    os.abort()


def _round_order_size(amount: float) -> float:
    """Round up to the configured order-size increment."""
    increment = max(0.01, ORDER_SIZE_INCREMENT)
    return round(math.ceil(amount / increment) * increment, 2)


def _crowd_floor(abs_diff: float) -> float:
    """Minimum acceptable Polymarket side price given BTC diff strength.

    At $75+ diff (81% WR in shadow) the signal overrides crowd skepticism —
    allows the symmetric payout zone (0.40–0.55) where wins ≈ losses.
    Below $75, only enter when crowd moderately agrees (0.58+).
    """
    if abs_diff >= 100:
        return 0.38
    if abs_diff >= 75:
        return 0.40
    if abs_diff >= 50:
        return 0.58
    return 1.0


def _live_diff_threshold(seconds_remaining: float) -> float:
    """
    Seconds-aware live entry threshold.

    Starts at $35 at 60 s remaining and grows by $0.25 per additional second,
    capped at $75. More time left = more room for reversal = larger diff needed.
    """
    extra = max(0.0, seconds_remaining - 60.0)
    return min(LIVE_DIFF_THRESHOLD_MAX, LIVE_DIFF_THRESHOLD_BASE + extra * 0.25)


def _extract_official_price_to_beat(market: dict) -> Optional[float]:
    """Return Polymarket's authoritative beat price when Gamma exposes it."""
    candidates = [
        market.get("priceToBeat"),
        market.get("price_to_beat"),
    ]
    events = market.get("events") or []
    if events:
        meta = events[0].get("eventMetadata") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if isinstance(meta, dict):
            candidates.append(meta.get("priceToBeat"))

    for value in candidates:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None

MARKET_FAMILIES = [
    {
        "label": "BTC 5m",
        "slug_prefix": "btc-updown-5m",
        "interval": 300,
        "strategy": "chainlink-reversal-guard",
        "live_enabled": False,
        "mode": "shadow",
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
        self._rtds_task: Optional[asyncio.Task] = None
        self._ob_task: Optional[asyncio.Task] = None
        self._clob_book_task: Optional[asyncio.Task] = None
        self._clob_user_task: Optional[asyncio.Task] = None
        self._shadow_selfcheck_task: Optional[asyncio.Task] = None
        self._session:    Optional[aiohttp.ClientSession] = None
        self._binance_feed = BinancePriceFeed()
        self._rtds_feed = PolymarketRtdsPriceFeed()
        self._ob_feed = OrderBookFeed()
        self._clob_book_feed = ClobBookFeed()
        self._clob_user_feed = ClobUserFeed()
        self._exec_router: Optional[ExecutionRouter] = None
        self._shadow_sim: Optional[ShadowFillSimulator] = None
        self._shadow_ready: bool = False
        self._shadow_watchdog_task: Optional[asyncio.Task] = None
        self._shadow_dispatch_count: int = 0       # incremented by router dispatch
        self._live_balance_at_shadow_boot: Optional[float] = None
        self._entered_slugs: set = set()           # avoid re-entering the same market
        self._volume_retry_slugs: set = set()      # slugs that can retry after main-window low volume
        self._live_retry_slugs: set = set()        # slugs with a background live-fill worker
        self._pre_signal_orders: dict = {          # slug → pre-signal order dict
            row["market_slug"]: row
            for row in db.load_pre_signal_orders()
        }
        # Startup safety net: pre-signal management ONLY allowed in maker_live.
        # If shadow_only/maker_shadow boots with leftover rows, log + disable
        # so even a regression in the runtime guard cannot reach the live SDK.
        self._pre_signal_disabled: bool = False
        if EXEC_MODE != "maker_live" and len(self._pre_signal_orders) > 0:
            print(
                f"[bot] STARTUP SAFETY: EXEC_MODE={EXEC_MODE} but found "
                f"{len(self._pre_signal_orders)} leftover pre_signal_orders row(s); "
                f"_manage_pre_signal_orders is permanently disabled this run "
                f"(slugs: {', '.join(self._pre_signal_orders.keys())})",
                flush=True,
            )
            self._pre_signal_disabled = True
        self._shadow_entered_slugs: set = set()    # avoid duplicate shadow rows per (slug, strategy)
        self._evaluating:    bool = False          # True while reversal guard is mid-sleep
        self._final_price_cache:    dict = {}      # {market_slug: finalPrice} for recent closed markets
        self._final_price_cache_at: dict = {}      # {market_slug: unix_ts when finalPrice was fetched}
        self._ptb_cache:            dict = {}      # {market_slug: priceToBeat} refreshed independently
        self._ptb_cache_at:         dict = {}      # {market_slug: unix_ts when priceToBeat was last fetched}
        self._market_start_prices: dict = {}      # slug → BTC price at first sight (price_to_beat)
        self._market_start_sources: dict = {}     # slug → source name for cached first-sight price
        self._market_diff_history: dict = {}      # slug → [(ts, seconds_remaining, diff), ...]
        self._btc_price_timestamps: list = []  # [(unix_ts, price)] — last 60s for momentum
        self._funding_rate: Optional[float] = None
        self._funding_rate_updated_at: float = 0.0
        self._funding_rate_source: Optional[str] = None
        if self._pre_signal_orders:
            print(
                f"[bot] Restored {len(self._pre_signal_orders)} resting pre-signal order(s) from DB: "
                + ", ".join(self._pre_signal_orders.keys()),
                flush=True,
            )
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
        self._rtds_task = loop.create_task(self._rtds_feed.connect())
        self._ob_task = loop.create_task(self._ob_feed.connect())
        if EXEC_MODE in {"maker_shadow", "maker_live"}:
            self._clob_book_task = loop.create_task(self._clob_book_feed.connect())
            if EXEC_MODE == "maker_shadow":
                self._shadow_sim = ShadowFillSimulator()
                shadow_clob.set_simulator(self._shadow_sim)
                self._clob_book_feed.set_trade_callback(self._shadow_sim.on_trade)
                self._clob_book_feed.set_book_callback(self._shadow_sim.on_book_update)
                fill_source = self._shadow_sim
                place_fn = shadow_clob.place_gtc_buy_shadow
                cancel_fn = shadow_clob.cancel_order_shadow
            else:
                self._clob_user_task = loop.create_task(self._clob_user_feed.connect())
                fill_source = self._clob_user_feed
                place_fn = None  # router defaults to live_clob.place_gtc_buy
                cancel_fn = None
            self._exec_router = ExecutionRouter(
                book_feed=self._clob_book_feed,
                fill_source=fill_source,
                on_fill=self._on_maker_fill,
                log_attempt=self._log_live_attempt_failed,
                place_fn=place_fn,
                cancel_fn=cancel_fn,
                exec_mode=EXEC_MODE,
            )
            self._shadow_selfcheck_task = loop.create_task(self._shadow_selfcheck())
            if EXEC_MODE == "maker_shadow":
                self._install_shadow_canaries()
                self._live_balance_at_shadow_boot = self.live_balance
                self._shadow_watchdog_task = loop.create_task(self._shadow_watchdog())
            print(f"[bot] EXEC_MODE={EXEC_MODE} — router initialised", flush=True)
        else:
            print(f"[bot] EXEC_MODE={EXEC_MODE} — no live/shadow execution layer", flush=True)
        loop.create_task(self._reconcile_unresolved_trades())
        loop.create_task(self._reconcile_unresolved_shadow_trades())
        loop.create_task(self._reconcile_unresolved_live_attempts())
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
        self._rtds_feed.stop()
        if self._rtds_task:
            self._rtds_task.cancel()
            try:
                await self._rtds_task
            except asyncio.CancelledError:
                pass
            self._rtds_task = None
        self._ob_feed.stop()
        if self._ob_task:
            self._ob_task.cancel()
            try:
                await self._ob_task
            except asyncio.CancelledError:
                pass
            self._ob_task = None
        if self._exec_router is not None:
            try:
                await self._exec_router.shutdown()
            except Exception as exc:
                print(f"[bot] router shutdown error: {exc}", flush=True)
            self._exec_router = None
        if self._shadow_watchdog_task is not None:
            self._shadow_watchdog_task.cancel()
            try:
                await self._shadow_watchdog_task
            except asyncio.CancelledError:
                pass
            self._shadow_watchdog_task = None
        if self._shadow_selfcheck_task is not None:
            self._shadow_selfcheck_task.cancel()
            try:
                await self._shadow_selfcheck_task
            except asyncio.CancelledError:
                pass
            self._shadow_selfcheck_task = None
        self._clob_book_feed.stop()
        if self._clob_book_task:
            self._clob_book_task.cancel()
            try:
                await self._clob_book_task
            except asyncio.CancelledError:
                pass
            self._clob_book_task = None
        self._clob_user_feed.stop()
        if self._clob_user_task:
            self._clob_user_task.cancel()
            try:
                await self._clob_user_task
            except asyncio.CancelledError:
                pass
            self._clob_user_task = None
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
            "rtds_price_feed": {
                "source":       self._rtds_feed.source,
                "connected":    self._rtds_feed.connected,
                "current_price": round(self._rtds_feed.current_price, 2) if self._rtds_feed.current_price else None,
                "last_update":  self._rtds_feed.last_update,
            },
            "funding_rate": self._funding_rate,
            "funding_rate_updated_at": self._funding_rate_updated_at or None,
            "funding_rate_source": self._funding_rate_source,
            "shadow": self._shadow_status_block(),
        }

    def _shadow_status_block(self) -> dict:
        counts = {"open": 0, "filled_aggressive_24h": 0, "filled_conservative_24h": 0, "cancelled_24h": 0}
        if EXEC_MODE == "maker_shadow":
            try:
                counts = db_shadow.counts_24h()
            except Exception:
                pass
        sim_snap: dict = {}
        if self._shadow_sim is not None:
            try:
                sim_snap = self._shadow_sim.status_snapshot()
            except Exception:
                sim_snap = {}
        # Fix 1: per-token freshness map for the measurement-phase smoke test.
        freshness: dict = {"book_ages_sec": {}, "trade_ages_sec": {}}
        try:
            if hasattr(self._clob_book_feed, "freshness_snapshot"):
                freshness = self._clob_book_feed.freshness_snapshot()
        except Exception:
            pass
        return {
            "exec_mode": EXEC_MODE,
            "ready": self._shadow_ready,
            "orders_open_in_memory": sim_snap.get("open_orders", 0),
            "orders_open_db": counts.get("open", 0),
            "orders_filled_aggressive_24h": counts.get("filled_aggressive_24h", 0),
            "orders_filled_conservative_24h": counts.get("filled_conservative_24h", 0),
            "orders_cancelled_24h": counts.get("cancelled_24h", 0),
            "last_book_update_ts": getattr(self._clob_book_feed, "last_book_update_ts", 0.0),
            "last_trade_event_ts": getattr(self._clob_book_feed, "last_trade_event_ts", 0.0),
            "ws_book_connected": getattr(self._clob_book_feed, "connected", False),
            "book_subscriptions": len(getattr(self._clob_book_feed, "_subscriptions", set()) or set()),
            # Fix 1 telemetry
            "last_book_update_by_token": freshness.get("book_ages_sec", {}),
            "last_trade_event_by_token": freshness.get("trade_ages_sec", {}),
            # Fix 2 telemetry
            "queue_ahead_zero_count": sim_snap.get("queue_ahead_zero_count", 0),
            "queue_ahead_nonzero_count": sim_snap.get("queue_ahead_nonzero_count", 0),
            "direction_rejections_total": sim_snap.get("direction_rejections_total", 0),
            "trades_missing_aggressor_total": sim_snap.get("trades_missing_aggressor_total", 0),
            # Dispatch attribution (observability-only; removable)
            "dispatch_attribution": attribution.snapshot(),
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

            if bool(market.get("_sz_live_enabled", True)) and slug.startswith("btc-updown-5m-"):
                self._record_rtds_reference_samples(market)

            entry_lo, entry_hi = market.get("_sz_entry_window", (ENTRY_WINDOW_LO, ENTRY_WINDOW_HI))
            in_main_window = entry_lo <= seconds_remaining <= entry_hi
            early_live_enabled = (
                bool(market.get("_sz_live_enabled", True))
                and slug.startswith("btc-updown-5m-")
            )
            in_early_live_window = (
                early_live_enabled
                and EARLY_LIVE_WINDOW_LO <= seconds_remaining <= EARLY_LIVE_WINDOW_HI
            )
            volume_retry_enabled = bool(market.get("_sz_live_enabled", True))
            in_volume_retry_window = (
                volume_retry_enabled
                and VOLUME_RETRY_LO <= seconds_remaining < VOLUME_RETRY_HI
                and slug in self._volume_retry_slugs
            )
            if market.get("_sz_live_enabled", True):
                await self._record_research_shadows(market, end_ts, seconds_remaining, hour_et, markets)
            if slug.startswith("btc-updown-15m-"):
                await self._record_15m_shadow(market, end_ts, seconds_remaining, hour_et)

            # ── Pre-signal limit order management (5m live markets only) ──────
            if (
                bool(market.get("_sz_live_enabled", True))
                and slug.startswith("btc-updown-5m-")
            ):
                # Compute reference price once — reused by pre-signal and lag-follow paths
                _pre_ref_price: Optional[float] = _extract_official_price_to_beat(market)
                _pre_ref_source: Optional[str] = "polymarket-official" if _pre_ref_price is not None else None
                if _pre_ref_price is None:
                    _ptb = await self._fetch_official_price_to_beat(slug)
                    if _ptb is not None:
                        _pre_ref_price, _pre_ref_source = _ptb, "polymarket-official"
                if _pre_ref_price is None:
                    _rtds_raw, _rtds_src = self._rtds_live_fallback_reference(market)
                    if _rtds_raw is not None:
                        _pre_ref_price = _rtds_raw - RTDS_BIAS_CORRECTION
                        _pre_ref_source = _rtds_src
                if _pre_ref_price is None:
                    _pre_ref_price = self._market_start_prices.get(slug)
                    _pre_ref_source = self._market_start_sources.get(slug)

                if _pre_ref_price is not None:
                    _pre_live_price, _pre_live_source = await self._get_signal_price()
                    if _pre_live_price is not None:
                        _pre_diff = _pre_live_price - _pre_ref_price
                        self._record_diff_snapshot(slug, seconds_remaining, _pre_diff)

                        # 1. Manage existing resting orders (check fill / cancel on reversal)
                        pre_filled = await self._manage_pre_signal_orders(
                            market=market,
                            current_diff=_pre_diff,
                            seconds_remaining=seconds_remaining,
                            end_ts=end_ts,
                            start_ts=start_ts,
                            hour_et=hour_et,
                        )
                        if pre_filled:
                            break

                        # 2. Place a new pre-signal limit — main window only, quality reference only
                        if (
                            in_main_window
                            and _pre_ref_source in ("polymarket-official", "prev-finalPrice")
                        ) and slug not in self._entered_slugs:
                            await self._maybe_place_pre_signal_limit(
                                market=market,
                                diff=_pre_diff,
                                reference_price=_pre_ref_price,
                                reference_source=_pre_ref_source,
                                seconds_remaining=seconds_remaining,
                                hour_et=hour_et,
                            )

            if (
                LAG_FOLLOW_LIVE_ENABLED
                and market.get("_sz_live_enabled", True)
                and slug.startswith("btc-updown-5m-")
                and in_main_window
                and slug not in self._entered_slugs
            ):
                opened_lag_follow = await self._maybe_open_lag_follow_live(
                    market=market,
                    end_ts=end_ts,
                    start_ts=start_ts,
                    seconds_remaining=seconds_remaining,
                    hour_et=hour_et,
                )
                if opened_lag_follow:
                    break

                if (
                    RTDS_PREVCLOSE_LIVE_ENABLED
                    and market.get("_sz_live_enabled", True)
                    and slug not in self._entered_slugs
                ):
                    opened_rtds = await self._maybe_open_rtds_prevclose_live(
                        market=market,
                        end_ts=end_ts,
                        seconds_remaining=seconds_remaining,
                        hour_et=hour_et,
                    )
                    if opened_rtds:
                        break

            # ── configured reversal window per market family
            if ((in_early_live_window or in_main_window or in_volume_retry_window)
                    and slug not in self._entered_slugs):
                if seconds_remaining < LIVE_MIN_SECONDS_BEFORE_CLOSE:
                    print(
                        f"[bot] SKIP: {slug} {seconds_remaining:.1f}s before close "
                        f"< {LIVE_MIN_SECONDS_BEFORE_CLOSE:.0f}s no-entry floor",
                        flush=True,
                    )
                    self._volume_retry_slugs.discard(slug)
                    continue

                strategy = market.get("_sz_strategy", "chainlink-reversal-guard")
                if in_early_live_window:
                    strategy = "btc5-early-lag-live"
                shadow_mode = market.get("_sz_mode") == "shadow"
                shadow_strategy = strategy
                shadow_already_recorded = (
                    (slug, shadow_strategy) in self._shadow_entered_slugs
                    or db.shadow_trade_exists(slug, shadow_strategy)
                )
                # Minimum volume filter — thin markets cause large slippage.
                # Only enforce/log this inside the entry window; early-window
                # volume is often low and can make normal markets look blocked.
                volume = float(market.get("volume") or 0)
                volume_blocks_paper = volume < MIN_MARKET_VOLUME
                if volume_blocks_paper and not bool(market.get("_sz_live_enabled", True)):
                    if in_main_window and volume_retry_enabled:
                        self._volume_retry_slugs.add(slug)
                    print(
                        f"[bot] SKIP: {slug} volume=${volume:.0f} < "
                        f"${MIN_MARKET_VOLUME:,.0f} minimum  secs={seconds_remaining:.1f}"
                        f"{' — eligible for late retry' if in_main_window and volume_retry_enabled else ''}",
                        flush=True,
                    )
                    continue
                if volume_blocks_paper:
                    print(
                        f"[bot] LOW VOLUME: {slug} volume=${volume:.0f} < "
                        f"${MIN_MARKET_VOLUME:,.0f}; live may continue via orderbook preflight",
                        flush=True,
                    )
                elif in_volume_retry_window:
                    print(
                        f"[bot] LATE RETRY: {slug} volume=${volume:.0f} now clears "
                        f"${MIN_MARKET_VOLUME:,.0f} minimum  secs={seconds_remaining:.1f}",
                        flush=True,
                    )

                if self._evaluating:
                    print("[bot] _tick: reversal check in progress — skipping reversal-guard", flush=True)
                    continue
                official_price_to_beat = _extract_official_price_to_beat(market)
                if official_price_to_beat is None:
                    official_price_to_beat = await self._fetch_official_price_to_beat(slug)
                # current priceToBeat == previous window's finalPrice at the boundary —
                # seed the cache now so prev-finalPrice is available immediately without
                # waiting for _collect_final_prices() to poll Gamma
                if official_price_to_beat is not None and start_ts is not None:
                    _spec = _market_spec_for_slug(slug)
                    if _spec:
                        _prev_seed_slug = f"{_spec['slug_prefix']}-{int(start_ts - _spec['interval'])}"
                        if _prev_seed_slug not in self._final_price_cache:
                            self._final_price_cache[_prev_seed_slug] = official_price_to_beat
                            self._final_price_cache_at[_prev_seed_slug] = time.time()
                            print(
                                f"[bot] prev-finalPrice seeded: {_prev_seed_slug} → ${official_price_to_beat:,.2f}",
                                flush=True,
                            )
                prev_final_price_to_beat, prev_final_source = self._previous_final_reference(market)
                # Priority: official Gamma → prev-final → RTDS → local start price
                rtds_price_to_beat, rtds_source = (None, None)
                if official_price_to_beat is None and prev_final_price_to_beat is None:
                    _rtds_raw, rtds_source = self._rtds_live_fallback_reference(market)
                    if _rtds_raw is not None:
                        rtds_price_to_beat = _rtds_raw - RTDS_BIAS_CORRECTION
                local_price_to_beat = self._market_start_prices.get(slug)
                local_price_source = self._market_start_sources.get(slug)
                if (
                    bool(market.get("_sz_live_enabled", True))
                    and official_price_to_beat is None
                    and prev_final_price_to_beat is None
                    and rtds_price_to_beat is None
                    and local_price_to_beat is None
                ):
                    print(
                        f"[bot] SKIP: {slug} all price-to-beat sources unavailable "
                        "(official/prev-final/RTDS/local); not opening entry",
                        flush=True,
                    )
                    continue
                if bool(market.get("_sz_live_enabled", True)):
                    price_to_beat = official_price_to_beat or prev_final_price_to_beat or rtds_price_to_beat or local_price_to_beat
                    price_source = (
                        "polymarket-official"
                        if official_price_to_beat is not None
                        else prev_final_source
                        if prev_final_price_to_beat is not None
                        else rtds_source
                        if rtds_price_to_beat is not None
                        else local_price_source
                    )
                else:
                    price_to_beat = official_price_to_beat or prev_final_price_to_beat or local_price_to_beat
                    price_source = (
                        "polymarket-official"
                        if official_price_to_beat is not None
                        else prev_final_source
                        if prev_final_price_to_beat is not None
                        else local_price_source
                    )
                live_reference_ok = (
                    official_price_to_beat is not None
                    or prev_final_price_to_beat is not None
                    or rtds_price_to_beat is not None
                )
                target_label = (
                    f"{EARLY_LIVE_WINDOW_LO}-{EARLY_LIVE_WINDOW_HI}s early-live"
                    if in_early_live_window else f"{entry_lo}-{entry_hi}s"
                )
                ref_label = f"${price_to_beat:,.2f} ({price_source})" if price_to_beat else "None"
                print(
                    f"[bot] reversal-guard window: {slug}  {seconds_remaining:.1f}s "
                    f"(target {target_label})  ref={ref_label}",
                    flush=True,
                )
                if price_source == RTDS_LIVE_FALLBACK_SOURCE and not shadow_mode:
                    strategy = f"{strategy}-rtds-fallback"
                direction, signals = await self._evaluate_signals(market, price_to_beat, price_source)

                if direction is None:
                    print(f"[bot] SKIP: {slug} no clear signal — {signals}", flush=True)
                    attribution.bump("no_clear_signal", strategy=strategy)
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

                if in_early_live_window:
                    early_ok, early_reason = self._early_live_confirmation(market, direction, signals)
                    if not early_ok:
                        print(f"[bot] SKIP EARLY LIVE: {slug} {early_reason}", flush=True)
                        db.log_bot_signal(slug, filter_hit="early_live_confirmation", outcome="skipped",
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
                            strategy=shadow_strategy,
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
                if not live_reference_ok:
                    print(
                        f"[bot] LIVE DISABLED: {slug} official priceToBeat unavailable; "
                        "paper only to avoid local-reference mismatch",
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
                    live_enabled=bool(market.get("_sz_live_enabled", True)) and live_reference_ok,
                )
                break

    def _get_measured_win_prob(self, live: bool = False) -> float:
        """
        Return the win probability to use for Kelly sizing.

        Uses the measured win rate from resolved trades once we have at least
        MIN_KELLY_TRADES samples, shrunk toward 0.5 with a Bayesian prior of
        weight 10 (equivalent to 10 virtual 50/50 observations).

        Falls back to a conservative 0.52 (barely above breakeven) until we
        have enough data — avoids over-sizing on unvalidated edge.

        Pass live=True to use live trade history instead of paper history.
        """
        if live:
            wins, losses, label = self.live_wins, self.live_losses, "live"
        else:
            wins, losses, label = self.wins, self.losses, "paper"
        total = wins + losses
        if total < 20:  # config.MIN_KELLY_TRADES
            print(
                f"[bot] Kelly ({label}): only {total} resolved trades — "
                f"using conservative WIN_PROB=0.52",
                flush=True,
            )
            return 0.52
        # Bayesian shrinkage toward 0.5 with prior weight 10
        prior_weight = 10
        shrunk = (wins + prior_weight * 0.5) / (total + prior_weight)
        print(
            f"[bot] Kelly ({label}): measured win_prob={wins}/{total}={wins/total:.3f} "
            f"→ shrunk={shrunk:.3f}",
            flush=True,
        )
        return shrunk

    def _log_live_attempt_failed(
        self,
        pos_or_slug,
        side: str,
        stake: float,
        paper_entry_price: float,
        live_cap: float,
        reason: str,
        *,
        seconds_remaining: Optional[float] = None,
        strategy: Optional[str] = None,
        diff_at_entry: Optional[float] = None,
        price_to_beat: Optional[float] = None,
    ) -> None:
        """Persist a failed live attempt so we can later score direction quality."""
        if isinstance(pos_or_slug, dict):
            slug = pos_or_slug["market_slug"]
            seconds_remaining = seconds_remaining if seconds_remaining is not None else (
                max(0.0, float(pos_or_slug["end_ts"]) - time.time()) if pos_or_slug.get("end_ts") else None
            )
            strategy = strategy if strategy is not None else pos_or_slug.get("strategy")
            diff_at_entry = diff_at_entry if diff_at_entry is not None else pos_or_slug.get("diff_at_entry")
            price_to_beat = price_to_beat if price_to_beat is not None else pos_or_slug.get("price_to_beat")
        else:
            slug = str(pos_or_slug)

        try:
            db.log_live_order_attempt({
                "market_slug": slug,
                "side": side,
                "intended_stake": stake,
                "paper_entry_price": paper_entry_price,
                "max_fill_price": live_cap,
                "reason": reason,
                "attempted_at": datetime.now(timezone.utc).isoformat(),
                "seconds_remaining": seconds_remaining,
                "strategy": strategy,
                "diff_at_entry": diff_at_entry,
                "price_to_beat": price_to_beat,
            })
        except Exception as exc:
            print(f"[bot] LIVE ATTEMPT LOG failed for {slug}: {exc}", flush=True)

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
                        volume = float(market.get("volume") or 0)
                        print(
                            f"[bot] found open market: {slug}  closed={data[0].get('closed')}  "
                            f"label={spec['label']}  volume=${volume:.0f}  "
                            f"live_enabled={spec['live_enabled']}  mode={spec.get('mode','paper')}"
                        )
                    elif data:
                        print(f"[bot] market {slug} is closed — skipping")
                    else:
                        print(f"[bot] no gamma result for {slug}")
            except Exception as exc:
                print(f"[bot] failed to fetch {slug}: {exc}")

        return markets

    # ── Signals ────────────────────────────────────────────────────────────────

    def _previous_final_reference(self, market: dict) -> tuple[Optional[float], Optional[str]]:
        """Use the exact previous interval's Polymarket finalPrice as a live-safe beat fallback."""
        slug = market.get("slug", "")
        start_ts = _extract_start_ts(slug)
        spec = _market_spec_for_slug(slug)
        if start_ts is None or spec is None:
            return None, None

        interval = int(spec["interval"])
        prev_slug = f"{spec['slug_prefix']}-{int(start_ts - interval)}"
        prev_final = self._final_price_cache.get(prev_slug)
        cached_at = self._final_price_cache_at.get(prev_slug)
        if prev_final is None or cached_at is None:
            return None, None

        age = time.time() - float(cached_at)
        if age > PREV_FINAL_MAX_AGE_SEC:
            print(
                f"[bot] prev-finalPrice stale for {slug}: {prev_slug} age={age:.1f}s",
                flush=True,
            )
            return None, None

        return float(prev_final), "prev-finalPrice"

    async def _fetch_official_price_to_beat(self, slug: str) -> Optional[float]:
        """Re-fetch priceToBeat directly from Gamma for a specific slug. Cached for 10s."""
        now = time.time()
        cached_at = self._ptb_cache_at.get(slug)
        if cached_at is not None and now - cached_at < 10.0:
            return self._ptb_cache.get(slug)

        try:
            session = await self._get_session()
            async with session.get(
                f"{GAMMA_API}/markets", params={"slug": slug}
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data:
                    return None
                ptb = _extract_official_price_to_beat(data[0])
                self._ptb_cache_at[slug] = now
                if ptb is not None:
                    self._ptb_cache[slug] = ptb
                    print(f"[bot] priceToBeat refreshed: {slug} ${ptb:,.2f}", flush=True)
                return ptb
        except Exception:
            return None

    def _local_previous_close_reference(self, market: dict) -> tuple[Optional[float], Optional[str]]:
        """Estimate current market's beat from local BTC price at previous close.

        This is intentionally shadow-only. It can arrive earlier than Polymarket
        metadata, but it is not authoritative enough for live execution.
        """
        slug = market.get("slug", "")
        start_ts = _extract_start_ts(slug)
        if start_ts is None:
            return None, None

        if self._binance_feed.connected:
            price = self._binance_feed.get_price_at_window_start(float(start_ts))
            if price is not None:
                return float(price), "binance-prev-close"

        first_sight = self._market_start_prices.get(slug)
        first_sight_source = self._market_start_sources.get(slug)
        if first_sight is not None:
            return float(first_sight), f"{first_sight_source or 'local'}-first-sight"

        return None, None

    def _rtds_previous_close_reference(self, market: dict) -> tuple[Optional[float], Optional[str]]:
        """Estimate current market's beat from Polymarket RTDS BTC price."""
        slug = market.get("slug", "")
        start_ts = _extract_start_ts(slug)
        if start_ts is None:
            return None, None

        price = self._rtds_feed.get_price_at_window_start(float(start_ts))
        if price is not None:
            symbol = self._rtds_feed.current_symbol or "btc"
            return float(price), f"polymarket-rtds-{symbol}-prev-close"
        return None, None

    def _rtds_live_fallback_reference(self, market: dict) -> tuple[Optional[float], Optional[str]]:
        """Use RTDS as a live fallback only when the later edge gate is large."""
        if not RTDS_LIVE_FALLBACK_ENABLED:
            return None, None
        if not bool(market.get("_sz_live_enabled", True)):
            return None, None
        slug = market.get("slug", "")
        if not slug.startswith("btc-updown-5m-"):
            return None, None
        price, _source = self._rtds_previous_close_reference(market)
        if price is None:
            return None, None
        return float(price), RTDS_LIVE_FALLBACK_SOURCE

    def _rtds_live_edge_ok(self, diff: float, crowd_price: float) -> tuple[bool, str]:
        """BTC-vs-crowd divergence gate.

        Fires when BTC has moved meaningfully AND the crowd hasn't fully
        priced it in yet — i.e. the crowd is still lagging the actual move.
        Lower diff thresholds than the old pure-diff gate because we now
        confirm the crowd hasn't caught up before entering.
        """
        if diff >= RTDS_LIVE_UP_DIFF and crowd_price <= RTDS_LIVE_UP_CROWD_CAP:
            return True, (
                f"Up divergence ${diff:+.2f} >= ${RTDS_LIVE_UP_DIFF:.0f} "
                f"crowd={crowd_price:.3f} <= {RTDS_LIVE_UP_CROWD_CAP:.2f}"
            )
        if diff <= -RTDS_LIVE_DOWN_DIFF and crowd_price >= RTDS_LIVE_DOWN_CROWD_FLOOR:
            return True, (
                f"Down divergence ${diff:+.2f} <= -${RTDS_LIVE_DOWN_DIFF:.0f} "
                f"crowd={crowd_price:.3f} >= {RTDS_LIVE_DOWN_CROWD_FLOOR:.2f}"
            )
        if diff >= 0:
            if diff < RTDS_LIVE_UP_DIFF:
                return False, f"Up diff ${diff:+.2f} < ${RTDS_LIVE_UP_DIFF:.0f}"
            return False, f"Up crowd={crowd_price:.3f} > cap {RTDS_LIVE_UP_CROWD_CAP:.2f} (already priced in)"
        else:
            if diff > -RTDS_LIVE_DOWN_DIFF:
                return False, f"Down diff ${diff:+.2f} > -${RTDS_LIVE_DOWN_DIFF:.0f}"
            return False, f"Down crowd={crowd_price:.3f} < floor {RTDS_LIVE_DOWN_CROWD_FLOOR:.2f} (already priced in)"

    def _record_rtds_reference_samples(self, market: dict) -> None:
        """Store RTDS ticks around 5m open so settlement can reveal best timing offset."""
        slug = market.get("slug", "")
        start_ts = _extract_start_ts(slug)
        if start_ts is None:
            return
        samples = self._rtds_feed.get_ticks_around(float(start_ts), radius=10.0)
        inserted = db.log_rtds_reference_samples(slug, float(start_ts), samples)
        if inserted:
            print(
                f"[bot] RTDS samples: stored {inserted} tick(s) around open for {slug}",
                flush=True,
            )

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
                        self._final_price_cache_at[slug] = time.time()
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

        if self._binance_feed._price_history:
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

    def _record_diff_snapshot(self, slug: str, seconds_remaining: float, diff: float) -> None:
        """Record a (ts, seconds_remaining, diff) snapshot for signal maturity tracking."""
        history = self._market_diff_history.setdefault(slug, [])
        history.append((time.time(), seconds_remaining, diff))
        # Keep last 6 minutes; prune closed-market entries lazily
        cutoff = time.time() - 360
        self._market_diff_history[slug] = [e for e in history if e[0] >= cutoff]

    def _signal_maturity(self, slug: str, threshold: float = 50.0) -> dict:
        """Analyse diff history for a market.

        Returns:
          sustained_secs  — continuous seconds diff has been above threshold in same direction
          trend           — 'rising', 'falling', or 'stable' based on last 15s vs prior 15s
          peak_diff       — max abs(diff) seen across the whole window
          is_fresh        — True if signal only just crossed threshold (< 20s sustained)
        """
        history = self._market_diff_history.get(slug, [])
        if not history:
            return {"sustained_secs": 0, "trend": "unknown", "peak_diff": 0, "is_fresh": True}

        now = time.time()
        _, _, latest_diff = history[-1]
        sign = 1 if latest_diff >= 0 else -1

        # Walk history forward; find the start of the most recent unbroken run above threshold
        above_since: Optional[float] = None
        for ts, _, diff in history:
            if sign * diff >= threshold:
                if above_since is None:
                    above_since = ts
            else:
                above_since = None  # dipped below — reset streak
        sustained_secs = (now - above_since) if above_since else 0.0

        # Trend: compare avg abs(diff) last 15s vs the 15s before that
        recent = [abs(d) for t, _, d in history if now - t <= 15]
        older  = [abs(d) for t, _, d in history if 30 >= now - t > 15]
        if recent and older:
            delta = sum(recent) / len(recent) - sum(older) / len(older)
            trend = "rising" if delta > 5 else "falling" if delta < -5 else "stable"
        else:
            trend = "unknown"

        peak_diff = max((abs(d) for _, _, d in history), default=0)

        return {
            "sustained_secs": sustained_secs,
            "trend": trend,
            "peak_diff": peak_diff,
            "is_fresh": sustained_secs < 20,
        }

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

    def _research_shadow_exists(self, slug: str, strategy: str) -> bool:
        return (slug, strategy) in self._shadow_entered_slugs or db.shadow_trade_exists(slug, strategy)

    def _early_live_confirmation(self, market: dict, direction: str, signals: dict) -> tuple[bool, str]:
        """
        Stricter early-live gate: only enter before the normal 75-150s window
        when the lag-follow direction and early-momentum direction are aligned.
        """
        diff = signals.get("diff_final")
        if diff is None:
            diff = signals.get("diff_initial")
        if diff is None:
            return False, "missing confirmed diff"

        lag_side = "Up" if diff > 0 else "Down"
        if lag_side != direction:
            return False, f"lag side {lag_side} disagrees with signal {direction}"
        if abs(diff) < PRICE_DIFF_THRESHOLD:
            return False, f"lag diff ${abs(diff):.2f} below ${PRICE_DIFF_THRESHOLD:.0f}"

        side_price = _side_price(market, direction)
        early_cap = min(EARLY_LIVE_MAX_PRICE, SHADOW_MAX_FOLLOW_PRICE, CROWD_MAX)
        if not (CROWD_MIN <= side_price <= early_cap):
            return False, f"{direction} price={side_price:.3f} outside early range [{CROWD_MIN}, {early_cap}]"

        live_price = signals.get("live_price")
        if live_price is None:
            return False, "missing live confirmation price"

        price_10s_ago = self._get_price_n_seconds_ago(10)
        price_30s_ago = self._get_price_n_seconds_ago(30)
        if price_10s_ago is None or price_30s_ago is None:
            return False, "missing 10s/30s momentum history"

        momentum_10 = live_price - price_10s_ago
        momentum_30 = live_price - price_30s_ago
        if direction == "Up" and not (momentum_10 > 0 and momentum_30 > 0):
            return False, f"Up momentum not aligned: 10s=${momentum_10:+.2f}, 30s=${momentum_30:+.2f}"
        if direction == "Down" and not (momentum_10 < 0 and momentum_30 < 0):
            return False, f"Down momentum not aligned: 10s=${momentum_10:+.2f}, 30s=${momentum_30:+.2f}"

        return True, (
            f"early live confirmed: price={side_price:.3f}, diff=${diff:+.2f}, "
            f"10s=${momentum_10:+.2f}, 30s=${momentum_30:+.2f}"
        )

    async def _maybe_place_pre_signal_limit(
        self,
        market: dict,
        diff: float,
        reference_price: float,
        reference_source: str,
        seconds_remaining: float,
        hour_et: int,
    ) -> None:
        """At PRE_SIGNAL_DIFF threshold, place a GTC limit order before the book reprices."""
        if EXEC_MODE.startswith("maker_"):
            return
        if not PRE_SIGNAL_ENABLED:
            return
        if os.getenv("POLYMARKET_LIVE", "false").lower() != "true":
            return

        slug = market.get("slug", "")
        if slug in self._entered_slugs or slug in self._pre_signal_orders:
            return
        if seconds_remaining < LIVE_MIN_SECONDS_BEFORE_CLOSE:
            return

        side = "Up" if diff > 0 else "Down"
        if abs(diff) < PRE_SIGNAL_DIFF:
            return

        crowd_price = _side_price(market, side)
        if not (CROWD_MIN <= crowd_price <= CROWD_MAX):
            return

        win_prob = self._get_measured_win_prob(live=True)
        loss_prob = 1.0 - win_prob
        b = (1.0 / PRE_SIGNAL_LIMIT_PRICE) - 1.0
        kelly_fraction = max(0.0, (win_prob * b - loss_prob) / b) if b > 0 else 0.0
        multiplier = min(1.0, HOUR_MULTIPLIER.get(hour_et, 1.0))
        max_profit_price = float(os.getenv("LIVE_RETRY_MAX_PRICE", "0.62"))
        _live_bal = self.live_balance
        _dyn_max = _live_bal * MAX_STAKE_PCT * multiplier
        _min_shares_stake = _round_order_size(CLOB_MIN_SHARES * PRE_SIGNAL_LIMIT_PRICE)
        _dyn_min = max(LIVE_MIN_ORDER_SIZE, _live_bal * MIN_STAKE_PCT, _min_shares_stake)
        stake = _round_order_size(max(_dyn_min, min(_dyn_max, _live_bal * kelly_fraction * 0.5 * multiplier)))

        if _live_bal < stake:
            return

        try:
            from . import live_clob
            limit_price = min(PRE_SIGNAL_LIMIT_PRICE, max_profit_price, CROWD_MAX)
            result = await live_clob.place_limit_order(market, side, stake, limit_price)
            order_rec = {
                "market_slug": slug,
                "order_id": result["order_id"],
                "side": side,
                "stake": stake,
                "limit_price": limit_price,
                "placed_at": time.time(),
                "market": market,
                "price_to_beat": reference_price,
                "reference_source": reference_source,
                "diff_at_placement": diff,
                "crowd_at_placement": crowd_price,
                "end_ts": market.get("endDateIso") or market.get("end_ts"),
                "hour_et": hour_et,
            }
            self._pre_signal_orders[slug] = order_rec
            db.save_pre_signal_order(order_rec)
            print(
                f"[bot] PRE-SIGNAL LIMIT: {side} {slug} "
                f"stake=${stake:.2f} limit={limit_price:.3f} diff=${diff:+.2f} "
                f"order_id={result['order_id']}",
                flush=True,
            )
        except Exception as exc:
            print(f"[bot] PRE-SIGNAL LIMIT failed for {slug}: {exc}", flush=True)

    async def _manage_pre_signal_orders(
        self,
        market: dict,
        current_diff: float,
        seconds_remaining: float,
        end_ts: float,
        start_ts: Optional[float],
        hour_et: int,
    ) -> bool:
        """
        Check resting pre-signal orders for this market.
        - If filled: promote to open position, return True (skip further entry logic)
        - If diff reversed below cancel threshold: cancel the order
        - Returns True if a pre-signal fill was processed (caller should skip normal entry).
        """
        # ── Permanent runtime guard ────────────────────────────────────────────
        # The pre-signal path uses live_clob.cancel_limit_order / get_order_status
        # which have NO POLYMARKET_LIVE gate inside live_clob, and the fill branch
        # mutates self.live_balance + writes live fields to open_positions.
        # Only maker_live is permitted to traverse this code.
        if EXEC_MODE != "maker_live":
            _SHADOW_HARD_STOP(
                f"_manage_pre_signal_orders reached in EXEC_MODE={EXEC_MODE} — "
                f"pre-signal path must never run outside maker_live"
            )
            return False
        if self._pre_signal_disabled:
            return False
        slug = market.get("slug", "")
        order_info = self._pre_signal_orders.get(slug)
        if order_info is None:
            return False

        side = order_info["side"]
        order_id = order_info["order_id"]

        # Cancel if direction reversed
        if (side == "Up" and current_diff < PRE_SIGNAL_CANCEL_DIFF) or \
           (side == "Down" and current_diff > -PRE_SIGNAL_CANCEL_DIFF):
            print(
                f"[bot] PRE-SIGNAL CANCEL: {slug} diff reversed to ${current_diff:+.2f} "
                f"— cancelling order {order_id}",
                flush=True,
            )
            try:
                from . import live_clob
                await live_clob.cancel_limit_order(order_id)
            except Exception as exc:
                print(f"[bot] PRE-SIGNAL cancel error {slug}: {exc}", flush=True)
            self._pre_signal_orders.pop(slug, None)
            db.delete_pre_signal_order(slug)
            return False

        # Check fill status
        try:
            from . import live_clob
            status = await live_clob.get_order_status(order_id)
        except Exception as exc:
            print(f"[bot] PRE-SIGNAL status check failed {slug}: {exc}", flush=True)
            return False

        size_matched = float(status.get("size_matched") or status.get("sizeMatched") or 0)
        size_total   = float(status.get("original_size") or status.get("size") or (order_info["stake"] / order_info["limit_price"]))
        filled_usdc  = size_matched * order_info["limit_price"]

        if filled_usdc < 0.50:
            # Not meaningfully filled yet
            return False

        # Filled — promote to open position
        self._pre_signal_orders.pop(slug, None)
        db.delete_pre_signal_order(slug)
        self._entered_slugs.add(slug)
        self.live_balance -= filled_usdc

        strategy = "btc5-pre-signal-limit"
        if order_info.get("reference_source") == RTDS_LIVE_FALLBACK_SOURCE:
            strategy = "btc5-pre-signal-limit-rtds"

        print(
            f"[bot] PRE-SIGNAL FILLED: {side} {slug} "
            f"filled_usdc=${filled_usdc:.2f} price={order_info['limit_price']:.3f} "
            f"order_id={order_id}",
            flush=True,
        )

        pos = {
            "market_slug":       slug,
            "side":              side,
            "size":              order_info["stake"],
            "entry_price":       order_info["limit_price"],
            "price_to_beat":     order_info["price_to_beat"],
            "end_ts":            end_ts,
            "opened_at":         datetime.now(timezone.utc).isoformat(),
            "diff_at_entry":     order_info["diff_at_placement"],
            "seconds_remaining": seconds_remaining,
            "strategy":          strategy,
            "live_order_id":     order_id,
            "live_stake":        round(filled_usdc, 2),
            "live_fill_price":   order_info["limit_price"],
        }
        self.balance -= order_info["stake"]
        self.positions.append(pos)
        db.save_open_position(pos)
        db.save_bot_state(self.balance)
        return True

    async def _maybe_open_lag_follow_live(
        self,
        market: dict,
        end_ts: float,
        start_ts: Optional[float],
        seconds_remaining: float,
        hour_et: int,
    ) -> bool:
        """Promote the validated btc5-lag-follow-shadow rule into conservative live execution."""
        slug = market["slug"]

        # ── Hour gate: skip live entry in statistically bad hours ────────────
        if hour_et in LIVE_BLOCKED_HOURS:
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} hour {hour_et} ET blocked "
                f"(LIVE_BLOCKED_HOURS={LIVE_BLOCKED_HOURS})",
                flush=True,
            )
            return False

        reference_price = _extract_official_price_to_beat(market)
        reference_source = "polymarket-official" if reference_price is not None else None
        if reference_price is None:
            ptb = await self._fetch_official_price_to_beat(slug)
            if ptb is not None:
                reference_price, reference_source = ptb, "polymarket-official"
        if reference_price is None:
            rtds_raw, rtds_src = self._rtds_live_fallback_reference(market)
            if rtds_raw is not None:
                reference_price = rtds_raw - RTDS_BIAS_CORRECTION
                reference_source = rtds_src
        if reference_price is None:
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} all reference sources unavailable",
                flush=True,
            )
            return False

        live_price, live_source = await self._get_signal_price()
        if live_price is None:
            return False

        diff = live_price - reference_price
        side = "Up" if diff > 0 else "Down"
        side_price = _side_price(market, side)
        live_max_price = min(LAG_FOLLOW_LIVE_MAX_PRICE, SHADOW_MAX_FOLLOW_PRICE, CROWD_MAX)
        if not (CROWD_MIN <= side_price <= live_max_price):
            return False
        if reference_source == RTDS_LIVE_FALLBACK_SOURCE:
            edge_ok, edge_reason = self._rtds_live_edge_ok(diff, side_price)
            if not edge_ok:
                print(f"[bot] LAG-FOLLOW RTDS LIVE SKIP: {slug} {edge_reason}", flush=True)
                return False
            print(f"[bot] LAG-FOLLOW RTDS LIVE edge OK: {slug} {edge_reason}", flush=True)
        else:
            if abs(diff) < 50.0:
                print(
                    f"[bot] LAG-FOLLOW LIVE SKIP: {slug} diff ${abs(diff):.2f} "
                    f"< threshold $50.00 ({seconds_remaining:.0f}s remaining)",
                    flush=True,
                )
                return False

        # ── Symmetric-zone gate: crowd skeptical (< 0.55) needs $75+ diff ──
        # At 0.40–0.55 the payout is symmetric; below 0.55 the crowd disagrees,
        # so only enter when the BTC signal is very strong ($75+, 81% WR in shadow).
        if side_price < 0.55 and abs(diff) < 75.0:
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} symmetric-zone requires diff >= $75 "
                f"(side_price={side_price:.3f} diff=${abs(diff):.2f})",
                flush=True,
            )
            return False

        # ── Crowd floor: reject if crowd disagrees too strongly with signal ──
        floor = _crowd_floor(abs(diff))
        if side_price < floor:
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} crowd {side_price:.3f} "
                f"< floor {floor:.2f} for diff ${abs(diff):.2f}",
                flush=True,
            )
            return False

        # ── Signal maturity: reject fresh spikes, penalise falling diffs ────
        maturity = self._signal_maturity(slug, threshold=50.0)
        print(
            f"[bot] LAG-FOLLOW LIVE maturity: {slug} sustained={maturity['sustained_secs']:.0f}s "
            f"trend={maturity['trend']} peak=${maturity['peak_diff']:.0f} fresh={maturity['is_fresh']}",
            flush=True,
        )
        if maturity["is_fresh"]:
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} signal fresh spike — "
                f"only {maturity['sustained_secs']:.0f}s above threshold",
                flush=True,
            )
            return False
        if maturity["trend"] == "falling":
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} diff trending down — "
                f"peak=${maturity['peak_diff']:.0f} now=${abs(diff):.0f}",
                flush=True,
            )
            return False

        # Recheck disabled: continuous diff-decay cancel is enforced inside MakerExecutor.

        price_10s_ago = self._get_price_n_seconds_ago(10)
        price_30s_ago = self._get_price_n_seconds_ago(30)
        if price_10s_ago is None or price_30s_ago is None:
            return False
        momentum_10 = live_price - price_10s_ago
        momentum_30 = live_price - price_30s_ago

        # Both windows must agree — guards against adverse selection where
        # informed sellers dump the losing side right as it begins to reverse.
        if side == "Up" and not (momentum_10 > 0 and momentum_30 > 0):
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} Up momentum not aligned "
                f"10s=${momentum_10:+.2f} 30s=${momentum_30:+.2f}",
                flush=True,
            )
            return False
        if side == "Down" and not (momentum_10 < 0 and momentum_30 < 0):
            print(
                f"[bot] LAG-FOLLOW LIVE SKIP: {slug} Down momentum not aligned "
                f"10s=${momentum_10:+.2f} 30s=${momentum_30:+.2f}",
                flush=True,
            )
            return False

        # ── HTF trend filter: 10-minute BTC trend must not strongly oppose signal ──
        # Primary cause of UP losses during sustained downtrends: bounces look
        # bullish intrawindow but the 10m context is bearish. Blocks signals where
        # the HTF trend exceeds $25 in the opposite direction.
        btc_history = list(self._binance_feed._price_history)
        if len(btc_history) >= 60:
            price_10m_ago_target = time.time() - 600
            _htf_closest = min(btc_history, key=lambda x: abs(x[0] - price_10m_ago_target))
            _htf_price = _htf_closest[1] if abs(_htf_closest[0] - price_10m_ago_target) < 60.0 else None
            if _htf_price is not None:
                htf_trend = live_price - _htf_price
                if side == "Up" and htf_trend < -HTF_TREND_THRESHOLD:
                    print(
                        f"[bot] LAG-FOLLOW LIVE SKIP: {slug} HTF bearish ({htf_trend:+.2f} over 10m) "
                        f"but signal=Up — sustained downtrend blocks entry",
                        flush=True,
                    )
                    return False
                if side == "Down" and htf_trend > HTF_TREND_THRESHOLD:
                    print(
                        f"[bot] LAG-FOLLOW LIVE SKIP: {slug} HTF bullish ({htf_trend:+.2f} over 10m) "
                        f"but signal=Down — sustained uptrend blocks entry",
                        flush=True,
                    )
                    return False
                print(
                    f"[bot] LAG-FOLLOW LIVE HTF OK: {slug} 10m trend={htf_trend:+.2f} side={side}",
                    flush=True,
                )

        self._market_start_prices.pop(slug, None)
        self._market_start_sources.pop(slug, None)
        self._entered_slugs.add(slug)
        self._volume_retry_slugs.discard(slug)
        print(
            f"[bot] LAG-FOLLOW LIVE SIGNAL PASSED: {side} on {slug} "
            f"price={side_price:.3f} diff=${diff:+.2f} 10s=${momentum_10:+.2f} 30s=${momentum_30:+.2f} "
            f"secs={seconds_remaining:.1f} source={live_source} ref={reference_source}",
            flush=True,
        )
        if side_price < 0.55:
            strategy = "btc5-lag-follow-live-symmetric"
        elif reference_source == RTDS_LIVE_FALLBACK_SOURCE:
            strategy = "btc5-lag-follow-live-rtds-fallback"
        else:
            strategy = "btc5-lag-follow-live"
        await self._open_position(
            slug,
            side,
            side_price,
            end_ts,
            start_ts=start_ts,
            price_to_beat=reference_price,
            diff_at_entry=diff,
            seconds_remaining=seconds_remaining,
            strategy=strategy,
            hour_et=hour_et,
            market=market,
            live_enabled=True,
        )
        return True

    async def _maybe_open_rtds_prevclose_live(
        self,
        market: dict,
        end_ts: float,
        seconds_remaining: float,
        hour_et: int,
    ) -> bool:
        """Live execution of the RTDS prev-close strategy (75.8% WR shadow, n=150).

        Uses the RTDS tick at window open as the reference (same as prevclose shadow).
        Only fires when diff clears the RTDS live edge gate AND both 10s+30s momentum agree.
        """
        if not RTDS_PREVCLOSE_LIVE_ENABLED:
            return False

        slug = market.get("slug", "")
        if not slug.startswith("btc-updown-5m-"):
            return False

        official_reference = _extract_official_price_to_beat(market)
        prev_final_reference, _ = self._previous_final_reference(market)
        if official_reference is not None or prev_final_reference is not None:
            return False

        if seconds_remaining < LIVE_MIN_SECONDS_BEFORE_CLOSE:
            return False

        reference_price, reference_source = self._rtds_previous_close_reference(market)
        if reference_price is None:
            return False

        live_price, live_source = await self._get_signal_price()
        if live_price is None:
            return False

        diff = live_price - reference_price
        side = "Up" if diff > 0 else "Down"
        side_price = _side_price(market, side)

        live_max_price = min(LAG_FOLLOW_LIVE_MAX_PRICE, SHADOW_MAX_FOLLOW_PRICE, CROWD_MAX)
        if not (CROWD_MIN <= side_price <= live_max_price):
            return False

        edge_ok, edge_reason = self._rtds_live_edge_ok(diff, side_price)
        if not edge_ok:
            print(f"[bot] RTDS-PREVCLOSE LIVE SKIP: {slug} {edge_reason}", flush=True)
            return False

        price_10s_ago = self._get_price_n_seconds_ago(10)
        price_30s_ago = self._get_price_n_seconds_ago(30)
        if price_10s_ago is None or price_30s_ago is None:
            return False
        momentum_10 = live_price - price_10s_ago
        momentum_30 = live_price - price_30s_ago

        if side == "Up" and not (momentum_10 > 0 and momentum_30 > 0):
            print(
                f"[bot] RTDS-PREVCLOSE LIVE SKIP: {slug} Up momentum not aligned "
                f"10s=${momentum_10:+.2f} 30s=${momentum_30:+.2f}", flush=True,
            )
            return False
        if side == "Down" and not (momentum_10 < 0 and momentum_30 < 0):
            print(
                f"[bot] RTDS-PREVCLOSE LIVE SKIP: {slug} Down momentum not aligned "
                f"10s=${momentum_10:+.2f} 30s=${momentum_30:+.2f}", flush=True,
            )
            return False

        self._entered_slugs.add(slug)
        self._volume_retry_slugs.discard(slug)
        print(
            f"[bot] RTDS-PREVCLOSE LIVE SIGNAL: {side} on {slug} "
            f"diff=${diff:+.2f} 10s=${momentum_10:+.2f} 30s=${momentum_30:+.2f} "
            f"secs={seconds_remaining:.1f} source={live_source} ref={reference_source}",
            flush=True,
        )
        await self._open_position(
            slug,
            side,
            side_price,
            end_ts,
            start_ts=_extract_start_ts(slug),
            price_to_beat=reference_price,
            diff_at_entry=diff,
            seconds_remaining=seconds_remaining,
            strategy="btc5-rtds-prevclose-live",
            hour_et=hour_et,
            market=market,
            live_enabled=True,
        )
        return True

    async def _record_research_shadows(
        self,
        market: dict,
        end_ts: float,
        seconds_remaining: float,
        hour_et: int,
        active_markets: Optional[list] = None,
    ) -> None:
        """Record shadow-only BTC 5m strategy variants for tradability research."""
        slug = market["slug"]
        if not slug.startswith("btc-updown-5m-"):
            return

        volume = float(market.get("volume") or 0.0)
        if volume < MIN_SHADOW_VOLUME:
            return

        await self._record_local_prevclose_shadow(
            market=market,
            end_ts=end_ts,
            seconds_remaining=seconds_remaining,
            hour_et=hour_et,
        )
        await self._record_rtds_prevclose_shadow(
            market=market,
            end_ts=end_ts,
            seconds_remaining=seconds_remaining,
            hour_et=hour_et,
        )

        reference_price = self._market_start_prices.get(slug)
        if reference_price is None:
            return

        live_price, _source = await self._get_signal_price()
        if live_price is None:
            return

        diff = live_price - reference_price
        momentum_10_price = self._get_price_n_seconds_ago(10)
        momentum_30_price = self._get_price_n_seconds_ago(30)
        momentum_10 = live_price - momentum_10_price if momentum_10_price is not None else None
        momentum_30 = live_price - momentum_30_price if momentum_30_price is not None else None
        up_price = _side_price(market, "Up")
        down_price = _side_price(market, "Down")

        def record(strategy: str, side: str) -> None:
            if self._research_shadow_exists(slug, strategy):
                return
            self._record_shadow_entry(
                slug=slug,
                side=side,
                entry_price=_side_price(market, side),
                end_ts=end_ts,
                price_to_beat=reference_price,
                diff_at_entry=diff,
                seconds_remaining=seconds_remaining,
                hour_et=hour_et,
                strategy=strategy,
                preserve_reference=True,
                force_min_stake=True,
            )

        # 1) Lag-follow: current direction is strong and still executable.
        if 75 <= seconds_remaining <= 150 and abs(diff) >= PRICE_DIFF_THRESHOLD:
            side = "Up" if diff > 0 else "Down"
            side_price = _side_price(market, side)
            momentum_ok = (
                momentum_10 is not None
                and ((side == "Up" and momentum_10 >= 0) or (side == "Down" and momentum_10 <= 0))
            )
            if momentum_ok and CROWD_MIN <= side_price <= SHADOW_MAX_FOLLOW_PRICE:
                record("btc5-lag-follow-shadow", side)
            # $50 threshold variant: simulate live $50 gate, cap at live max price
            if momentum_ok and abs(diff) >= 50.0 and CROWD_MIN <= side_price <= LAG_FOLLOW_LIVE_MAX_PRICE:
                record("btc5-lag-follow-50-shadow", side)
            # Symmetric-zone variant: mirrors btc5-lag-follow-live-symmetric exactly.
            # diff $75+, crowd skeptical (0.40–0.55) — symmetric payout, strong signal.
            if momentum_ok and abs(diff) >= 75.0 and CROWD_MIN <= side_price <= 0.55:
                record("btc5-lag-follow-75-symmetric-shadow", side)

        # 1b) Early GTC shadow: first crossing of $50 diff at 150-200s with tradable price.
        # Tests whether placing a GTC limit early (before the main window) would win.
        # Paper-only — live GTC not implemented; this builds the evidence base.
        if 150 <= seconds_remaining <= 200 and abs(diff) >= 50.0:
            side = "Up" if diff > 0 else "Down"
            side_price = _side_price(market, side)
            if CROWD_MIN <= side_price <= LAG_FOLLOW_LIVE_MAX_PRICE:
                record("btc5-early-gtc-shadow", side)

        # 2) Early momentum: enter before the obvious late signal if price is still tradable.
        early_lo, early_hi = EARLY_SHADOW_WINDOW
        if early_lo <= seconds_remaining <= early_hi and abs(diff) >= 8.0:
            side = "Up" if diff > 0 else "Down"
            side_price = _side_price(market, side)
            momentum_ok = (
                momentum_10 is not None
                and momentum_30 is not None
                and ((side == "Up" and momentum_10 > 0 and momentum_30 > 0)
                     or (side == "Down" and momentum_10 < 0 and momentum_30 < 0))
            )
            if momentum_ok and CROWD_MIN <= side_price <= SHADOW_MAX_FOLLOW_PRICE:
                record("btc5-early-momentum-shadow", side)

        # 3) Order book imbalance: leading signal — fires before price confirms.
        ob_lo, ob_hi = OB_SHADOW_WINDOW
        if ob_lo <= seconds_remaining <= ob_hi:
            imbalance = self._ob_feed.get_sustained_imbalance(OB_SUSTAINED_SECONDS)
            if imbalance is not None and abs(imbalance) >= OB_IMBALANCE_THRESHOLD:
                ob_side = "Up" if imbalance > 0 else "Down"
                ob_price = _side_price(market, ob_side)
                if CROWD_MIN <= ob_price <= SHADOW_MAX_FOLLOW_PRICE:
                    if not self._research_shadow_exists(slug, "btc5-ob-imbalance-shadow"):
                        self._record_shadow_entry(
                            slug=slug,
                            side=ob_side,
                            entry_price=ob_price,
                            end_ts=end_ts,
                            price_to_beat=reference_price,
                            diff_at_entry=round(imbalance, 4),
                            seconds_remaining=seconds_remaining,
                            hour_et=hour_et,
                            strategy="btc5-ob-imbalance-shadow",
                            preserve_reference=True,
                            force_min_stake=True,
                        )

        # 4) Bonereaper-style research: dominant directional leg with cheap
        # opposite insurance. This is shadow-only because 0.90+ chasing has
        # blow-up risk; we only test sane dominant prices.
        hedge_lo, hedge_hi = HEDGED_SHADOW_WINDOW
        if hedge_lo <= seconds_remaining <= hedge_hi and abs(diff) >= 8.0:
            side = "Up" if diff > 0 else "Down"
            opposite = "Down" if side == "Up" else "Up"
            side_price = _side_price(market, side)
            hedge_price = _side_price(market, opposite)
            momentum_ok = (
                momentum_10 is not None
                and momentum_30 is not None
                and ((side == "Up" and momentum_10 > 0 and momentum_30 > 0)
                     or (side == "Down" and momentum_10 < 0 and momentum_30 < 0))
            )
            if momentum_ok and HEDGED_DOMINANT_MIN <= side_price <= HEDGED_DOMINANT_MAX:
                agreement = self._cross_market_agreement(side, active_markets or [], slug)
                suffix = f"{agreement}x"
                record(f"btc5-hedged-dominant-shadow-{suffix}", side)
                if hedge_price <= HEDGED_HEDGE_MAX:
                    record(f"btc5-cheap-hedge-shadow-{suffix}", opposite)

    async def _record_15m_shadow(
        self,
        market: dict,
        end_ts: float,
        seconds_remaining: float,
        hour_et: int,
    ) -> None:
        """Record shadow research entries for BTC 15m markets."""
        slug = market.get("slug", "")
        if not slug.startswith("btc-updown-15m-"):
            return
        if self._research_shadow_exists(slug, "btc-15m-paper-shadow"):
            return

        volume = float(market.get("volume") or 0.0)
        if volume < MIN_SHADOW_VOLUME:
            return

        official_ptb = _extract_official_price_to_beat(market)
        prev_final, _ = self._previous_final_reference(market)
        reference_price = official_ptb or prev_final
        if reference_price is None:
            return

        live_price, _ = await self._get_signal_price()
        if live_price is None:
            return

        diff = live_price - reference_price
        spec = _market_spec_for_slug(slug)
        diff_threshold = float(spec["diff_threshold"]) if spec else 25.0

        if abs(diff) < diff_threshold:
            return

        side = "Up" if diff > 0 else "Down"
        side_price = _side_price(market, side)
        if not (CROWD_MIN <= side_price <= CROWD_MAX):
            return

        self._record_shadow_entry(
            slug=slug,
            side=side,
            entry_price=side_price,
            end_ts=end_ts,
            price_to_beat=reference_price,
            diff_at_entry=diff,
            seconds_remaining=seconds_remaining,
            hour_et=hour_et,
            strategy="btc-15m-paper-shadow",
            preserve_reference=True,
            force_min_stake=True,
        )

    async def _record_local_prevclose_shadow(
        self,
        market: dict,
        end_ts: float,
        seconds_remaining: float,
        hour_et: int,
    ) -> None:
        """Shadow-test local previous-close reference when trusted Polymarket refs are missing."""
        slug = market.get("slug", "")
        if not slug.startswith("btc-updown-5m-"):
            return
        if self._research_shadow_exists(slug, LOCAL_PREVCLOSE_SHADOW_STRATEGY):
            return

        official_reference = _extract_official_price_to_beat(market)
        prev_final_reference, _prev_final_source = self._previous_final_reference(market)
        if official_reference is not None or prev_final_reference is not None:
            return

        if not (ENTRY_WINDOW_LO <= seconds_remaining <= EARLY_LIVE_WINDOW_HI):
            return

        reference_price, reference_source = self._local_previous_close_reference(market)
        if reference_price is None:
            return

        live_price, live_source = await self._get_signal_price()
        if live_price is None:
            return

        diff = live_price - reference_price
        if abs(diff) < PRICE_DIFF_THRESHOLD:
            return

        side = "Up" if diff > 0 else "Down"
        side_price = _side_price(market, side)
        if not (CROWD_MIN <= side_price <= SHADOW_MAX_FOLLOW_PRICE):
            return

        price_10s_ago = self._get_price_n_seconds_ago(10)
        if price_10s_ago is None:
            return
        momentum_10 = live_price - price_10s_ago
        if side == "Up" and momentum_10 < 0:
            return
        if side == "Down" and momentum_10 > 0:
            return

        print(
            f"[bot] LOCAL-PREVCLOSE SHADOW: {side} on {slug} "
            f"diff=${diff:+.2f} 10s=${momentum_10:+.2f} "
            f"ref=${reference_price:,.2f} ({reference_source}) live={live_source}",
            flush=True,
        )
        self._record_shadow_entry(
            slug=slug,
            side=side,
            entry_price=side_price,
            end_ts=end_ts,
            price_to_beat=reference_price,
            diff_at_entry=diff,
            seconds_remaining=seconds_remaining,
            hour_et=hour_et,
            strategy=LOCAL_PREVCLOSE_SHADOW_STRATEGY,
            preserve_reference=True,
            force_min_stake=True,
        )

    async def _record_rtds_prevclose_shadow(
        self,
        market: dict,
        end_ts: float,
        seconds_remaining: float,
        hour_et: int,
    ) -> None:
        """Shadow-test Polymarket RTDS previous-close reference."""
        slug = market.get("slug", "")
        if not slug.startswith("btc-updown-5m-"):
            return
        if self._research_shadow_exists(slug, RTDS_PREVCLOSE_SHADOW_STRATEGY):
            return

        official_reference = _extract_official_price_to_beat(market)
        prev_final_reference, _prev_final_source = self._previous_final_reference(market)
        if official_reference is not None or prev_final_reference is not None:
            return

        if not (ENTRY_WINDOW_LO <= seconds_remaining <= EARLY_LIVE_WINDOW_HI):
            return

        reference_price, reference_source = self._rtds_previous_close_reference(market)
        if reference_price is None:
            return

        live_price, live_source = await self._get_signal_price()
        if live_price is None:
            return

        diff = live_price - reference_price
        if abs(diff) < PRICE_DIFF_THRESHOLD:
            return

        side = "Up" if diff > 0 else "Down"
        side_price = _side_price(market, side)
        if not (CROWD_MIN <= side_price <= SHADOW_MAX_FOLLOW_PRICE):
            return

        price_10s_ago = self._get_price_n_seconds_ago(10)
        if price_10s_ago is None:
            return
        momentum_10 = live_price - price_10s_ago
        if side == "Up" and momentum_10 < 0:
            return
        if side == "Down" and momentum_10 > 0:
            return

        print(
            f"[bot] RTDS-PREVCLOSE SHADOW: {side} on {slug} "
            f"diff=${diff:+.2f} 10s=${momentum_10:+.2f} "
            f"ref=${reference_price:,.2f} ({reference_source}) live={live_source}",
            flush=True,
        )
        self._record_shadow_entry(
            slug=slug,
            side=side,
            entry_price=side_price,
            end_ts=end_ts,
            price_to_beat=reference_price,
            diff_at_entry=diff,
            seconds_remaining=seconds_remaining,
            hour_et=hour_et,
            strategy=RTDS_PREVCLOSE_SHADOW_STRATEGY,
            preserve_reference=True,
            force_min_stake=True,
        )

    def _cross_market_agreement(self, side: str, active_markets: list, current_slug: str) -> int:
        """Count related open markets whose crowd-leading side agrees."""
        agreement = 1
        for related in active_markets:
            related_slug = related.get("slug", "")
            if related_slug == current_slug:
                continue
            if not (
                related_slug.startswith("btc-updown-15m-")
                or related_slug.startswith("btc-updown-5m-")
            ):
                continue
            up_price = _side_price(related, "Up")
            down_price = _side_price(related, "Down")
            if max(up_price, down_price) < 0.55:
                continue
            leading_side = "Up" if up_price > down_price else "Down"
            if leading_side == side:
                agreement += 1
        return min(agreement, 3)

    async def _evaluate_signals(
        self, market: dict, price_to_beat: Optional[float], price_source: Optional[str]
    ) -> tuple[Optional[str], dict]:
        """
        Two-condition entry:
        1. abs(live_price - reference_price) >= PRICE_DIFF_THRESHOLD
        2. After 3s re-check: move hasn't reversed and diff >= REVERSAL_THRESHOLD
        Live-enabled markets require Polymarket official priceToBeat or a fresh
        previous-window finalPrice. Shadow-only markets may still use local first-sight.
        """
        slug = market["slug"]
        start_ts = _extract_start_ts(slug)
        if start_ts is None:
            db.log_bot_signal(slug, filter_hit="bad_slug_ts", outcome="skipped")
            return None, {"source": "none", "momentum": None}
        live_enabled = bool(market.get("_sz_live_enabled", True))
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
        has_official_reference = price_source == "polymarket-official" and price_to_beat is not None
        has_prev_final_reference = price_source == "prev-finalPrice" and price_to_beat is not None
        has_rtds_live_fallback = price_source == RTDS_LIVE_FALLBACK_SOURCE and price_to_beat is not None
        if live_enabled and not (has_official_reference or has_prev_final_reference or has_rtds_live_fallback):
            print(
                f"[bot] SKIP: {slug} live-enabled market has no official/prev-final/RTDS-fallback reference; "
                "local proxy disabled",
                flush=True,
            )
            db.log_bot_signal(slug, filter_hit="no_trusted_reference", outcome="skipped")
            return None, {"source": "none", "momentum": None}

        if has_official_reference:
            reference_price = price_to_beat
            ref_source = "polymarket-official"
        elif has_prev_final_reference:
            reference_price = price_to_beat
            ref_source = "prev-finalPrice"
        elif has_rtds_live_fallback:
            reference_price = price_to_beat
            ref_source = RTDS_LIVE_FALLBACK_SOURCE
        elif prev_final is not None:
            reference_price = prev_final
            ref_source = "prev-finalPrice"
        else:
            reference_price = price_to_beat
            ref_source = f"{price_source or 'unknown'}-first-sight"

        if reference_price is None:
            print(f"[bot] SKIP: {slug} no reference price", flush=True)
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
            if has_rtds_live_fallback:
                crowd_price_now = _side_price(market, direction)
                edge_ok, edge_reason = self._rtds_live_edge_ok(diff_initial, crowd_price_now)
                if not edge_ok:
                    print(f"[bot] SKIP RTDS LIVE: {slug} {edge_reason}", flush=True)
                    db.log_bot_signal(slug, filter_hit="rtds_live_edge", outcome="skipped",
                                      direction=direction, diff=diff_initial)
                    return None, {"source": "none", "momentum": None}
                print(f"[bot] RTDS LIVE edge OK: {slug} {edge_reason}", flush=True)
            elif abs(diff_initial) < diff_threshold:
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
                    attribution.bump("chop_filter")
                    return None, {"source": "none", "momentum": None}
            else:
                print(f"[bot] SKIP (chop): {slug} no {chop_window:.0f}s reading", flush=True)
                db.log_bot_signal(slug, filter_hit="no_chop_history", outcome="skipped",
                                  direction=direction, diff=diff_initial)
                attribution.bump("chop_filter")
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
            if has_rtds_live_fallback:
                crowd_price_post = _side_price(market, direction)
                edge_ok, edge_reason = self._rtds_live_edge_ok(diff_final, crowd_price_post)
                if not edge_ok:
                    print(f"[bot] SKIP RTDS LIVE: {slug} post-guard {edge_reason}", flush=True)
                    db.log_bot_signal(slug, filter_hit="rtds_live_reversal_edge", outcome="skipped",
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
        _dyn_max = self.balance * MAX_STAKE_PCT * multiplier
        stake    = min(_dyn_max, self.balance * kelly_fraction * 0.5 * multiplier)
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
        strategy: str = "shadow",
        preserve_reference: bool = False,
        force_min_stake: bool = False,
    ) -> None:
        """Record a hypothetical shadow trade without touching paper/live accounting."""
        stake, kelly_fraction, multiplier = self._shadow_stake(entry_price, hour_et)
        _dyn_min = max(LIVE_MIN_ORDER_SIZE, self.balance * MIN_STAKE_PCT)
        if force_min_stake:
            stake = _round_order_size(max(_dyn_min, stake))
        if stake < _dyn_min:
            print(
                f"[bot] SHADOW SKIP: Kelly stake ${stake:.2f} below ${_dyn_min:.2f} minimum",
                flush=True,
            )
            db.log_bot_signal(slug, filter_hit="shadow_size", outcome="skipped",
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
        self._shadow_entered_slugs.add((slug, strategy))
        if not preserve_reference:
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
                            f"rows={settled.get('count', 1)} pnl=${settled['pnl']:+.2f}",
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
        win_prob       = self._get_measured_win_prob(live=True)
        loss_prob      = 1.0 - win_prob
        b              = (1.0 / entry_price) - 1.0   # net payout per dollar risked
        kelly_fraction = max(0.0, (win_prob * b - loss_prob) / b) if b > 0 else 0.0
        multiplier     = min(1.0, HOUR_MULTIPLIER.get(hour_et, 1.0))
        _live_bal      = self.live_balance
        max_stake      = _live_bal * MAX_STAKE_PCT * multiplier
        min_shares_stake = _round_order_size(CLOB_MIN_SHARES * entry_price)
        min_stake      = max(LIVE_MIN_ORDER_SIZE, _live_bal * MIN_STAKE_PCT, min_shares_stake)
        raw_stake      = _live_bal * kelly_fraction * 0.5 * multiplier
        kelly_stake    = min(max_stake, raw_stake)
        stake          = _round_order_size(max(min_stake, kelly_stake))

        if kelly_stake < min_stake:
            print(
                f"[bot] OPEN: {slug} {side} entry={entry_price:.3f} "
                f"using minimum stake ${stake:.2f}; Kelly suggested ${kelly_stake:.2f}",
                flush=True,
            )

        if _live_bal < stake:
            print(f"[bot] OPEN SKIP: {slug} insufficient live balance (${_live_bal:.2f})", flush=True)
            return

        # ── Live order (best-effort alongside paper) ───────────────────────────
        live_order = None
        execution_enabled = (
            live_enabled
            and EXEC_MODE in {"maker_shadow", "maker_live"}
            and (EXEC_MODE == "maker_shadow" or os.getenv("POLYMARKET_LIVE", "false").lower() == "true")
        )
        if EXEC_MODE == "maker_shadow" and not self._shadow_ready:
            print(f"[bot] SHADOW: dispatch skipped for {slug} — shadow_ready=False", flush=True)
            execution_enabled = False
            attribution.bump("ws_unhealthy", strategy=strategy)
        # Fix 1: per-token freshness gate. The selfcheck-driven _shadow_ready flag
        # is one-shot; this gate is checked on every dispatch and uses per-token
        # last-update timestamps. First dispatch for a never-seen token is allowed
        # to pass (router will subscribe inside dispatch); subsequent dispatches
        # require recent book data for the specific token.
        if EXEC_MODE == "maker_shadow" and execution_enabled and market is not None:
            try:
                from .live_clob import token_for_side as _tfs_check
                _tok = _tfs_check(market, side)
            except Exception:
                _tok = None
            if _tok and self._clob_book_feed.is_subscribed(_tok):
                age = self._clob_book_feed.get_token_age(_tok)
                if age is None or age > 30.0:
                    age_str = "never" if age is None else f"{age:.1f}s"
                    print(
                        f"[bot] SKIP: token={str(_tok)[:12]}.. stale (age={age_str}, max=30s) "
                        f"slug={slug}",
                        flush=True,
                    )
                    execution_enabled = False
                    attribution.bump("token_stale", strategy=strategy)
        if execution_enabled:
            if seconds_remaining is not None and seconds_remaining < LIVE_MIN_SECONDS_BEFORE_CLOSE:
                print(
                    f"[bot] LIVE: skipped for {slug}; {seconds_remaining:.1f}s before close "
                    f"< {LIVE_MIN_SECONDS_BEFORE_CLOSE:.0f}s minimum",
                    flush=True,
                )
                self._log_live_attempt_failed(
                    slug,
                    side,
                    stake,
                    entry_price,
                    entry_price,
                    f"Skipped live: {seconds_remaining:.1f}s before close < {LIVE_MIN_SECONDS_BEFORE_CLOSE:.0f}s minimum",
                    seconds_remaining=seconds_remaining,
                    strategy=strategy,
                    diff_at_entry=diff_at_entry,
                    price_to_beat=price_to_beat,
                )
                attribution.bump("market_closed", strategy=strategy)
            elif not (CROWD_MIN <= entry_price <= CROWD_MAX):
                reason = (
                    f"entry_price {entry_price:.3f} outside crowd band "
                    f"[{CROWD_MIN},{CROWD_MAX}] at signal time — crowd slipped"
                )
                print(f"[bot] LIVE CANCEL: {slug} {reason}", flush=True)
                self._log_live_attempt_failed(
                    slug, side, stake, entry_price, entry_price, reason,
                    seconds_remaining=seconds_remaining, strategy=strategy,
                    diff_at_entry=diff_at_entry, price_to_beat=price_to_beat,
                )
                attribution.bump("execution_disabled", strategy=strategy)
            elif market is None:
                print(f"[bot] LIVE: no market dict for {slug} — paper only", flush=True)
                attribution.bump("no_market", strategy=strategy)
            elif self._exec_router is None:
                print(f"[bot] LIVE: exec router not initialised — paper only", flush=True)
                attribution.bump("execution_disabled", strategy=strategy)
            else:
                try:
                    await self._exec_router.dispatch(
                        market=market,
                        slug=slug,
                        side=side,
                        stake=stake,
                        paper_entry_price=entry_price,
                        seconds_remaining=seconds_remaining or 0.0,
                        end_ts=end_ts,
                        diff_at_entry=diff_at_entry,
                        price_to_beat=price_to_beat,
                        strategy=strategy,
                        diff_now_getter=self._current_diff_for_slug,
                    )
                    self._shadow_dispatch_count += 1
                    attribution.bump("dispatch_called", strategy=strategy)
                except Exception as exc:
                    print(f"[bot] LIVE: maker dispatch failed for {slug} ({exc}) — paper only", flush=True)
                    self._log_live_attempt_failed(
                        slug, side, stake, entry_price, entry_price, f"maker_dispatch_error: {exc}",
                        seconds_remaining=seconds_remaining, strategy=strategy,
                        diff_at_entry=diff_at_entry, price_to_beat=price_to_beat,
                    )
                    attribution.bump("execution_disabled", strategy=strategy)
        else:
            if EXEC_MODE == "shadow_only":
                attribution.bump("execution_disabled", strategy=strategy)
                pass  # paper/shadow accounting only
            elif not live_enabled:
                print(f"[bot] LIVE: disabled for {slug} strategy={strategy} — paper/shadow only", flush=True)
                attribution.bump("execution_disabled", strategy=strategy)

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

    async def _retry_live_fill(self, *args, **kwargs) -> None:
        """Deprecated: FAK retry loop replaced by MakerExecutor diff-decay cancel."""
        return None

    def _current_diff_for_slug(self, slug: str) -> Optional[float]:
        """Return latest BTC-vs-reference diff for a slug, or None if unknown."""
        hist = self._market_diff_history.get(slug)
        if not hist:
            return None
        try:
            return float(hist[-1][2])
        except Exception:
            return None

    async def _on_maker_fill(self, fill: dict) -> None:
        """Callback invoked by MakerExecutor when a resting order matches."""
        if EXEC_MODE != "maker_live":
            print(
                f"[bot] SHADOW FILL (no state mutation): {fill.get('market_slug')} "
                f"order_id={fill.get('order_id')} price={fill.get('fill_price')} "
                f"stake={fill.get('stake')}",
                flush=True,
            )
            return
        slug = fill.get("market_slug")
        if not slug:
            return
        pos = next((p for p in self.positions if p["market_slug"] == slug), None)
        if pos is None:
            print(f"[bot] MAKER FILL ignored: no open position for {slug}", flush=True)
            return
        pos["live_order_id"] = fill.get("order_id")
        pos["live_stake"] = float(fill.get("stake", 0.0))
        pos["live_fill_price"] = float(fill.get("fill_price", 0.0))
        try:
            db.save_open_position(pos)
        except Exception as exc:
            print(f"[bot] MAKER FILL: db save_open_position failed for {slug}: {exc}", flush=True)
        self.live_balance -= float(fill.get("stake", 0.0))
        print(
            f"[bot] MAKER FILLED {pos['side']:>4} {slug} "
            f"stake=${pos['live_stake']:.2f} price={pos['live_fill_price']:.4f} "
            f"order_id={pos['live_order_id']}",
            flush=True,
        )

    # ── Shadow safety harness ──────────────────────────────────────────────────

    def _install_shadow_canaries(self) -> None:
        """Overwrite live execution entry-points with hard-stop traps.

        Called once at startup in maker_shadow. Any call to these functions
        means a code path reached the live CLOB when it must not have.
        """
        from . import live_clob as _lc

        def _trap(name: str):
            async def _canary(*args, **kwargs):
                _SHADOW_HARD_STOP(
                    f"live_clob.{name} called in EXEC_MODE=maker_shadow — "
                    f"real order would have been placed"
                )
            return _canary

        _lc.place_order    = _trap("place_order")
        _lc.place_gtc_buy  = _trap("place_gtc_buy")
        print("[bot] shadow canaries installed on live_clob.place_order + place_gtc_buy", flush=True)

    async def _shadow_watchdog(self) -> None:
        """Periodic integrity checks while running in maker_shadow.

        Hard-stops the process if any invariant is violated:
          1. live_balance must not change from its boot value
          2. After first dispatch, db_shadow must contain at least one row
        """
        await asyncio.sleep(10)  # let feeds settle first
        dispatch_db_checked = False

        while self.running:
            # ── Check 1: live_balance must be frozen ──────────────────────────
            if (
                self._live_balance_at_shadow_boot is not None
                and self.live_balance != self._live_balance_at_shadow_boot
            ):
                _SHADOW_HARD_STOP(
                    f"live_balance mutated in maker_shadow: "
                    f"boot={self._live_balance_at_shadow_boot:.4f} "
                    f"now={self.live_balance:.4f}"
                )

            # ── Check 2: after first dispatch, DB must have a row ─────────────
            if self._shadow_dispatch_count > 0 and not dispatch_db_checked:
                try:
                    counts = db_shadow.counts_24h()
                    total = sum(counts.values())
                    if total == 0:
                        _SHADOW_HARD_STOP(
                            f"dispatch count={self._shadow_dispatch_count} but "
                            f"db_shadow shows 0 rows — shadow_clob may not be writing"
                        )
                    else:
                        print(
                            f"[shadow-watchdog] DB integrity OK after first dispatch: "
                            f"{counts}",
                            flush=True,
                        )
                        dispatch_db_checked = True
                except Exception as exc:
                    print(f"[shadow-watchdog] db_shadow.counts_24h failed: {exc}", flush=True)

            await asyncio.sleep(5)

    async def _shadow_selfcheck(self) -> None:
        """After 60 s, verify book WS produces a valid top-of-book for at least one
        active 5 m token. Sets self._shadow_ready when satisfied."""
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        try:
            markets = await self._fetch_active_markets()
        except Exception as exc:
            print(f"[bot] shadow self-check: fetch_active_markets failed: {exc}", flush=True)
            return
        token_id: Optional[str] = None
        for m in markets:
            try:
                from .live_clob import token_for_side
                token_id = token_for_side(m, "Up")
                self._clob_book_feed.subscribe(token_id)
                break
            except Exception:
                continue
        if token_id is None:
            print("[bot] shadow self-check: no active 5m market token resolved", flush=True)
            return
        deadline = time.time() + 120
        while time.time() < deadline and self.running:
            top = self._clob_book_feed.top(token_id, max_age_sec=2.0)
            if (
                top is not None
                and top.bid_price is not None
                and top.ask_price is not None
                and top.bid_price < top.ask_price
            ):
                self._shadow_ready = True
                print(
                    f"[bot] shadow self-check PASSED token={token_id[:10]} "
                    f"bid={top.bid_price:.4f} ask={top.ask_price:.4f}",
                    flush=True,
                )
                return
            await asyncio.sleep(2)
        print("[bot] shadow self-check FAILED — refusing shadow dispatch", flush=True)

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
                    attempt_count = db.settle_live_order_attempts(
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
                        if attempt_count:
                            print(
                                f"[bot] live attempts: settled {attempt_count} failed attempt(s) for {slug}",
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
            attempt_count = db.settle_live_order_attempts(
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
            if attempt_count:
                print(
                    f"[bot] live attempts: settled {attempt_count} failed attempt(s) for {slug}",
                    flush=True,
                )
        if settled_count:
            self._reload_accounting_state()

    async def _reconcile_unresolved_live_attempts(self) -> None:
        unresolved = db.load_unresolved_live_order_attempts()
        if not unresolved:
            return
        print(f"[bot] live attempts reconcile: checking {len(unresolved)} unresolved attempts", flush=True)
        settled_count = 0
        checked_slugs = set()
        for row in unresolved:
            slug = row["market_slug"]
            if slug in checked_slugs:
                continue
            checked_slugs.add(slug)
            start_ts = _extract_start_ts(slug)
            if start_ts is None:
                continue
            end_ts = start_ts + float(_market_interval(slug))
            winner, final_price, poly_price_to_beat = await self._resolve_market(slug, end_ts)
            if winner is None:
                continue
            settled_count += db.settle_live_order_attempts(
                slug,
                winner,
                resolution_price=final_price,
                poly_price_to_beat=poly_price_to_beat,
            )
        if settled_count:
            print(f"[bot] live attempts reconcile: settled {settled_count} failed attempt(s)", flush=True)

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
                settled_count += int(settled.get("count", 1))
                print(
                    f"[bot] shadow reconcile: {slug} settled → {winner} "
                    f"rows={settled.get('count', 1)} pnl=${settled['pnl']:+.2f}",
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
            sample_count = db.settle_rtds_reference_samples(slug, price_to_beat)
            if sample_count:
                print(
                    f"[bot] RTDS samples: compared {sample_count} tick(s) for {slug} "
                    f"against priceToBeat=${price_to_beat:,.2f}",
                    flush=True,
                )
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
            attempt_count = db.settle_live_order_attempts(
                pos["market_slug"],
                winner,
                resolution_price=resolution_price,
                poly_price_to_beat=poly_price_to_beat,
            )
            if attempt_count:
                print(
                    f"[bot] live attempts: settled {attempt_count} failed attempt(s) for {pos['market_slug']}",
                    flush=True,
                )
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
