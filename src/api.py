"""
SIGNAL/ZERO — FastAPI bridge
Exposes whale wallet data and the paper trading bot to the React dashboard.

Run with:
    cd ~/Downloads/polymarket-bot
    uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

import aiohttp
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .bot import bot
from . import db
from .whale_tracker import WhaleTracker
from . import htf as htf_lib


async def _backfill_resolution_prices():
    """
    For trades that are settled (outcome known) but missing resolution_price,
    fetch the Polymarket finalPrice and write it to the DB.
    Runs once at startup in the background.
    """
    import aiohttp as _aiohttp
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT market_slug, outcome
                 FROM bot_trades
                WHERE resolution_price IS NULL
                  AND outcome NOT IN ('', 'unresolved')
                  AND outcome IS NOT NULL"""
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("[backfill] no settled trades missing resolution_price")
        return

    print(f"[backfill] {len(rows)} settled trade(s) missing resolution_price")
    GAMMA = "https://gamma-api.polymarket.com"

    async with _aiohttp.ClientSession(timeout=_aiohttp.ClientTimeout(total=15)) as session:
        for row in rows:
            slug = row["market_slug"]
            try:
                async with session.get(f"{GAMMA}/markets", params={"slug": slug, "closed": "true"}) as resp:
                    if resp.status != 200:
                        continue
                    markets = await resp.json()
                if not markets:
                    continue
                m = markets[0]
                events = m.get("events", [])
                meta   = (events[0].get("eventMetadata") or {}) if events else {}
                final_price    = meta.get("finalPrice")
                price_to_beat  = meta.get("priceToBeat")
                if final_price is not None:
                    db.update_resolution_price(slug, float(final_price))
                    print(
                        f"[backfill] {slug} → finalPrice=${float(final_price):,.2f}"
                        + (f"  priceToBeat=${float(price_to_beat):,.2f}" if price_to_beat else ""),
                        flush=True,
                    )
                else:
                    print(f"[backfill] {slug} — finalPrice not in eventMetadata yet", flush=True)
            except Exception as exc:
                print(f"[backfill] error for {slug}: {exc}", flush=True)
            await asyncio.sleep(0.5)   # gentle rate-limit


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[startup] lifespan begin")
    db.init_db()
    print("[startup] db.init_db() done")

    tracker = WhaleTracker()
    try:
        print("[startup] running whale_tracker.run_once() ...")
        results = await tracker.run_once(db_module=db)
        print(f"[startup] whale_tracker finished — {len(results)} wallets saved")
    except Exception as exc:
        print(f"[startup] whale_tracker ERROR: {exc}")
    finally:
        await tracker.close()

    # Auto-start the trading bot so it survives Railway redeploys without
    # needing a manual POST /api/bot/start each time.
    bot.start()
    print("[startup] bot auto-started")

    # Backfill resolution_price for any settled trades that are still NULL
    asyncio.create_task(_backfill_resolution_prices())

    print("[startup] lifespan ready")
    yield
    print("[shutdown] lifespan end")
    await bot.stop()


app = FastAPI(title="SIGNAL/ZERO API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "https://signal-zero-dashboard.vercel.app",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Whale endpoints ────────────────────────────────────────────────────────────

@app.get("/api/whales")
def get_whales():
    """Return top 5 whale wallets ranked by win rate."""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT address, win_rate, total_trades, pnl
                 FROM whale_wallets
                WHERE resolved_trades >= 5
                ORDER BY win_rate DESC, total_trades DESC
                LIMIT 5"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/run-whales")
async def run_whales():
    """Manually trigger whale_tracker.run_once() and return results."""
    print("[run-whales] manual trigger")
    tracker = WhaleTracker()
    try:
        results = await tracker.run_once(db_module=db)
        print(f"[run-whales] done — {len(results)} wallets saved")
        return {"ok": True, "wallets_saved": len(results), "wallets": results}
    except Exception as exc:
        print(f"[run-whales] ERROR: {exc}")
        return {"ok": False, "error": str(exc), "wallets_saved": 0}
    finally:
        await tracker.close()


# ── Bot endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/bot/status")
def get_bot_status():
    """Current bot state: running, balance, stats, open position."""
    return bot.get_status()


@app.get("/api/bot/trades")
def get_bot_trades():
    """Last 5 completed bot trades."""
    return bot.get_recent_trades(n=20)


@app.get("/api/bot/stats")
def get_bot_stats():
    """Aggregate performance stats across all resolved bot_trades."""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT pnl, outcome, side, opened_at FROM bot_trades "
            "WHERE pnl IS NOT NULL AND COALESCE(outcome, '') != 'unresolved' "
            "ORDER BY opened_at"
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
    losses = [p for p in pnls if p < 0]
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


@app.get("/api/bot/stats/hourly")
def get_hourly_stats():
    """Per-ET-hour breakdown of win rate and P&L across all resolved trades."""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT pnl, opened_at FROM bot_trades "
            "WHERE pnl IS NOT NULL AND COALESCE(outcome, '') != 'unresolved' "
            "ORDER BY opened_at"
        ).fetchall()
    finally:
        conn.close()

    by_hour: dict = {}
    for r in rows:
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(r["opened_at"].replace("Z", "+00:00"))
            hour_et = (dt.hour - 4) % 24
        except Exception:
            continue
        if hour_et not in by_hour:
            by_hour[hour_et] = {"hour_et": hour_et, "trades": 0, "wins": 0, "pnl": 0.0}
        by_hour[hour_et]["trades"] += 1
        by_hour[hour_et]["pnl"] = round(by_hour[hour_et]["pnl"] + r["pnl"], 2)
        if r["pnl"] > 0:
            by_hour[hour_et]["wins"] += 1

    result = []
    for h in sorted(by_hour):
        d = by_hour[h]
        d["win_rate"] = round(d["wins"] / d["trades"], 4) if d["trades"] else 0.0
        result.append(d)
    return result


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


@app.post("/api/bot/set-balance")
async def set_balance(body: dict):
    """Sync in-memory balance and optionally P&L without a full reset.
    Body: { "balance": 10000, "pnl": -29.13 }  — pnl is optional.
    """
    if "balance" not in body:
        raise HTTPException(status_code=400, detail="balance is required")
    new_balance = float(body["balance"])
    if new_balance < 0:
        raise HTTPException(status_code=400, detail="Balance must be non-negative")
    bot.balance = new_balance
    db.save_bot_state(new_balance)
    if "pnl" in body:
        bot.total_pnl = float(body["pnl"])
    return {"ok": True, "balance": bot.balance, "pnl": bot.total_pnl}


@app.post("/api/bot/reset")
async def reset_bot():
    """Reset all in-memory stats to a clean slate and persist to DB.
    Does not stop the bot or clear open positions.
    """
    bot.balance   = db.INITIAL_BALANCE
    bot.wins      = 0
    bot.losses    = 0
    bot.total_pnl = 0.0
    db.save_bot_state(bot.balance)
    return {"ok": True, "balance": bot.balance}

@app.get("/api/btc-price")
async def btc_price():
    """Return the latest BTC/USD price from the Chainlink feed on Polygon."""
    try:
        loop = asyncio.get_running_loop()
        price = await loop.run_in_executor(None, get_btc_price)
        return {
            "price": round(price, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/funding-rates")
async def get_funding_rates():
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear"},
            )
            r.raise_for_status()
            data = r.json()
        tickers = data.get("result", {}).get("list", [])
        filtered = [
            {
                "symbol": d["symbol"].replace("USDT", ""),
                "fundingRate": float(d["fundingRate"]) * 100,
                "markPrice": float(d["lastPrice"]),
            }
            for d in tickers
            if d["symbol"].endswith("USDT") and "_" not in d["symbol"]
            and d.get("fundingRate")
        ]
        filtered.sort(key=lambda x: abs(x["fundingRate"]), reverse=True)
        return filtered[:15]
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── HTF endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/htf/markets")
async def htf_markets():
    """
    Live HTF market data: fetch from Polymarket, enrich with log-normal edge.
    Returns enriched market list sorted by |edge| descending.
    """
    try:
        async with aiohttp.ClientSession() as session:
            spot, markets, prices = await asyncio.gather(
                htf_lib.fetch_btc_price(session),
                htf_lib.fetch_htf_markets(session),
                htf_lib.fetch_recent_prices(session),
            )
        sigma = htf_lib.estimate_vol(prices, 3600) if prices else 0.80

        enriched = []
        for m in markets:
            strike   = m["price_to_beat"] or spot
            prob     = htf_lib.btc_up_prob(spot, strike, sigma, m["seconds_left"])
            edge     = round(prob["prob_up"] - m["prob_up"], 4)
            enriched.append({
                **m,
                "spot":       round(spot, 2),
                "est_prob_up":   prob["prob_up"],
                "est_prob_down": prob["prob_down"],
                "edge":       edge,
                "d2":         prob["d2"],
                "sigma":      sigma,
            })

        enriched.sort(key=lambda x: abs(x["edge"]), reverse=True)
        return {"ok": True, "markets": enriched, "spot": round(spot, 2)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/htf/latest")
def htf_latest():
    """DB snapshots, recent trades, and paper bot state."""
    try:
        db.init_htf_tables()  # idempotent — creates tables if missing
    except Exception:
        pass
    try:
        return {
            "ok":            True,
            "snapshots":     db.htf_get_latest_snapshots(),
            "recent_trades": db.htf_get_recent_trades(20),
            "bot_state":     db.htf_get_state(),
        }
    except Exception as e:
        return {
            "ok":            False,
            "error":         str(e),
            "snapshots":     [],
            "recent_trades": [],
            "bot_state":     {"balance": 10000.0, "total_pnl": 0.0},
        }


@app.get("/api/htf/history")
def htf_history(slug: str, hours: int = 6):
    """Snapshot history for one market (for charting)."""
    return {"ok": True, "history": db.htf_get_history(slug, min(hours, 168))}
