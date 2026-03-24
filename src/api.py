"""
SIGNAL/ZERO — FastAPI bridge
Exposes whale wallet data and the paper trading bot to the React dashboard.

Run with:
    cd ~/Downloads/polymarket-bot
    uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
"""

import sqlite3
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .bot import bot

DB_PATH = Path(__file__).parent.parent / "signal_zero.db"

app = FastAPI(title="SIGNAL/ZERO API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Whale endpoints ────────────────────────────────────────────────────────────

@app.get("/api/whales")
def get_whales():
    """Return top 5 whale wallets ranked by win rate."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT address, win_rate, total_trades, pnl
                 FROM whale_wallets
                WHERE resolved_trades >= 2
                ORDER BY win_rate DESC, total_trades DESC
                LIMIT 5"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Bot endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/bot/status")
def get_bot_status():
    """Current bot state: running, balance, stats, open position."""
    return bot.get_status()


@app.get("/api/bot/trades")
def get_bot_trades():
    """Last 5 completed bot trades."""
    return bot.get_recent_trades(n=5)


@app.get("/api/bot/stats")
def get_bot_stats():
    """Aggregate performance stats across all resolved bot_trades."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT pnl, outcome, side, opened_at FROM bot_trades WHERE pnl IS NOT NULL ORDER BY opened_at"
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "total_pnl": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
            "best_trade": 0.0, "worst_trade": 0.0, "trades_by_day": [],
        }

    pnls   = [r["pnl"] for r in rows]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    total  = len(pnls)

    by_day: dict = {}
    for r in rows:
        day = r["opened_at"][:10]
        if day not in by_day:
            by_day[day] = {"date": day, "trades": 0, "wins": 0, "pnl": 0.0}
        by_day[day]["trades"] += 1
        by_day[day]["pnl"]    = round(by_day[day]["pnl"] + r["pnl"], 2)
        if r["pnl"] > 0:
            by_day[day]["wins"] += 1

    return {
        "total_trades": total,
        "wins":         len(wins),
        "losses":       len(losses),
        "win_rate":     round(len(wins) / total, 4),
        "total_pnl":    round(sum(pnls), 2),
        "avg_win":      round(sum(wins)   / len(wins)   if wins   else 0.0, 2),
        "avg_loss":     round(sum(losses) / len(losses) if losses else 0.0, 2),
        "best_trade":   round(max(pnls), 2),
        "worst_trade":  round(min(pnls), 2),
        "trades_by_day": list(by_day.values()),
    }


@app.post("/api/bot/start")
async def start_bot():
    """Start the whale-copy bot loop."""
    if bot.running:
        return {"ok": True, "message": "Already running"}
    bot.start()
    return {"ok": True, "message": "Bot started"}


@app.post("/api/bot/stop")
async def stop_bot():
    """Stop the whale-copy bot loop."""
    if not bot.running:
        return {"ok": True, "message": "Already stopped"}
    await bot.stop()
    return {"ok": True, "message": "Bot stopped"}
