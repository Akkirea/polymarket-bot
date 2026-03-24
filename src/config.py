"""
SIGNAL/ZERO Phase 1 — Configuration

All tunable parameters in one place. Adjust these as you observe
how the market behaves before moving to Phase 2.
"""

# ── Polymarket ──────────────────────────────────────────────
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# Search terms to find active 5-min BTC markets
MARKET_SEARCH_SLUG = "bitcoin-up-or-down"
MARKET_TIMEFRAME = "5"  # minutes

# How often to poll Polymarket for updated odds (seconds)
POLL_INTERVAL = 10

# ── Binance ─────────────────────────────────────────────────
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BINANCE_KLINE_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

# ── Edge Detection ──────────────────────────────────────────
# Minimum divergence (percentage points) between our momentum
# estimate and Polymarket implied probability to flag a signal.
# Start conservative — you can tighten this after collecting data.
EDGE_THRESHOLD = 0.08  # 8 percentage points

# How many seconds of price history to use for momentum calc
MOMENTUM_WINDOW = 60  # seconds

# Weight recent prices more heavily in momentum calculation
MOMENTUM_DECAY = 0.95  # exponential decay factor

# ── Paper Trading ───────────────────────────────────────────
PAPER_BET_SIZE = 5.0        # USDC per simulated trade
INITIAL_BANKROLL = 100.0    # starting paper balance
MAX_DAILY_LOSS = 25.0       # stop trading if daily loss exceeds this
MAX_CONCURRENT_BETS = 1     # only 1 bet at a time for Phase 1

# Estimated fees for realistic P&L simulation
# Polymarket taker fee formula for 5-min markets:
#   fee = C * 0.25 * (p * (1-p))^2
# where C is a constant (~2.0 based on recent data) and p is the price
EST_FEE_CONSTANT = 2.0

# ── Database ────────────────────────────────────────────────
DB_PATH = "signal_zero.db"

# ── Display ─────────────────────────────────────────────────
# Use rich terminal UI
ENABLE_RICH_UI = True
