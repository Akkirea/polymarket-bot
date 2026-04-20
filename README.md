# SIGNAL/ZERO — Phase 1: Polymarket 5-Min Monitor

A local Python tool that monitors Polymarket's 5-minute BTC Up/Down markets
alongside real-time Binance price data, looking for edge signals.

**Phase 1 is observation only — no wallet, no trades, no capital at risk.**

## What it does

1. Connects to Binance WebSocket for real-time BTC/USDT price
2. Polls Polymarket Gamma API for active 5-min market odds
3. Calculates momentum vs. market-implied probability divergence
4. Simulates paper trades when divergence exceeds threshold
5. Logs everything to SQLite for later analysis

## Setup

```bash
# Python 3.10+ required
cd polymarket-bot

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the monitor
python -m src.monitor
```

## Railway process split

For production, run the latency/API stack and HTF worker as separate processes:

```bash
web: uvicorn src.api:app --host 0.0.0.0 --port $PORT
htf: python -m src.htf_worker
```

This keeps the HTF paper-trading loop isolated from the latency-sensitive bot.
Both processes can share the same `DATABASE_URL`.

## Configuration

Edit `src/config.py` to adjust:
- `EDGE_THRESHOLD` — minimum divergence to trigger a simulated trade (default: 8%)
- `PAPER_BET_SIZE` — simulated bet size in USDC (default: 5.0)
- `MAX_DAILY_LOSS` — daily loss circuit breaker (default: 25.0)
- `POLL_INTERVAL` — seconds between Polymarket polls (default: 10)

## Analyzing results

```bash
# After running for a while, check your paper P&L
python -m src.analyze
```

## Project structure

```
polymarket-bot/
├── src/
│   ├── __init__.py
│   ├── config.py          # All constants and settings
│   ├── monitor.py         # Main loop — ties everything together
│   ├── binance_ws.py      # Real-time BTC price via WebSocket
│   ├── polymarket_api.py  # Polymarket market discovery + odds
│   ├── edge_detector.py   # Divergence calculation logic
│   ├── paper_trader.py    # Simulated trade execution + P&L
│   ├── db.py              # SQLite persistence layer
│   └── analyze.py         # Post-session analysis script
├── requirements.txt
└── README.md
```

## Phase roadmap

- **Phase 1 (this):** Monitor + paper trade + collect data
- **Phase 2:** Connect to Polymarket CLOB API with read-only keys
- **Phase 3:** Live trading with micro stakes ($25-50 max)

---
PAPER TRADING ONLY · NO REAL CAPITAL · FOR EDUCATION AND RESEARCH
SIGNAL/ZERO v0.2 · PHASE 1 MONITOR · BUILT BY ERIKKA
