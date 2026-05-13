"""
SIGNAL/ZERO — FastAPI bridge
Exposes whale wallet data and the paper trading bot to the React dashboard.

Run with:
    cd ~/Downloads/polymarket-bot
    uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aiohttp
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .bot import bot
from . import db
from .whale_tracker import WhaleTracker
from . import live_clob
from .markets import (
    DAILY_MARKETS,
    FIVE_MINUTE_MARKETS,
    FIFTEEN_MINUTE_MARKETS,
    catalog,
    current_daily_slugs,
    current_interval_slugs,
)


SHADOW_FAMILIES = {
    "btc5": {
        "label": "BTC 5m Shadow",
        "slug_like": "btc-updown-5m-%",
        "strategy": None,
    },
    "btc5_lag_follow": {
        "label": "BTC 5m Lag Follow",
        "slug_like": "btc-updown-5m-%",
        "strategy": "btc5-lag-follow-shadow",
    },
    "btc5_early_momentum": {
        "label": "BTC 5m Early Momentum",
        "slug_like": "btc-updown-5m-%",
        "strategy": "btc5-early-momentum-shadow",
    },
    "btc5_overpriced_fade": {
        "label": "BTC 5m Overpriced Fade",
        "slug_like": "btc-updown-5m-%",
        "strategy": "btc5-overpriced-fade-shadow",
    },
    "btc5_hedged_dominant": {
        "label": "BTC 5m Hedged Dominant",
        "slug_like": "btc-updown-5m-%",
        "strategy_like": "btc5-hedged-dominant-shadow-%",
    },
    "btc5_cheap_hedge": {
        "label": "BTC 5m Cheap Hedge",
        "slug_like": "btc-updown-5m-%",
        "strategy_like": "btc5-cheap-hedge-shadow-%",
    },
    "btc5_local_prevclose": {
        "label": "BTC 5m Local Prev Close",
        "slug_like": "btc-updown-5m-%",
        "strategy": "btc5-local-prevclose-shadow",
    },
    "btc15": {
        "label": "BTC 15m Shadow",
        "slug_like": "btc-updown-15m-%",
        "strategy": "btc-15m-paper-shadow",
    },
    "btcdaily": {
        "label": "BTC Daily Shadow",
        "slug_like": "bitcoin-up-or-down-on-%",
        "strategy": "btc-daily-shadow",
    },
}


def _family_filter_sql(family: str | None) -> tuple[str, tuple]:
    if not family:
        return "", ()
    spec = SHADOW_FAMILIES.get(family)
    if not spec:
        raise HTTPException(status_code=400, detail=f"Unknown family: {family}")
    clauses = ["market_slug LIKE %s"]
    params: list = [spec["slug_like"]]
    if spec.get("strategy"):
        clauses.append("strategy = %s")
        params.append(spec["strategy"])
    if spec.get("strategy_like"):
        clauses.append("strategy LIKE %s")
        params.append(spec["strategy_like"])
    return " AND " + " AND ".join(clauses), tuple(params)


async def _backfill_resolution_prices():
    """
    For trades that are settled (outcome known) but missing resolution_price,
    fetch the Polymarket finalPrice and write it to the DB.
    Returns a small summary so it can be run from startup, a periodic task,
    or a manual API endpoint.
    """
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT market_slug, outcome, poly_price_to_beat
                 FROM bot_trades
                WHERE resolution_price IS NULL
                  AND outcome NOT IN ('', 'unresolved')
                  AND outcome IS NOT NULL"""
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("[backfill] no settled trades missing resolution_price")
        return {"checked": 0, "updated": 0}

    print(f"[backfill] {len(rows)} settled trade(s) missing resolution_price")
    GAMMA = "https://gamma-api.polymarket.com"
    updated = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
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
                    conn = db.get_connection()
                    try:
                        conn.execute(
                            """UPDATE bot_trades
                                  SET resolution_price = %s,
                                      poly_price_to_beat = COALESCE(poly_price_to_beat, %s)
                                WHERE market_slug = %s
                                  AND resolution_price IS NULL""",
                            (
                                round(float(final_price), 2),
                                round(float(price_to_beat), 2) if price_to_beat is not None else None,
                                slug,
                            ),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    updated += 1
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

    return {"checked": len(rows), "updated": updated}


async def _periodic_resolution_backfill():
    """Keep retrying settled trades whose Polymarket finalPrice arrived late."""
    while True:
        try:
            await _backfill_resolution_prices()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[backfill] periodic task error: {exc}", flush=True)
        await asyncio.sleep(600)


async def _run_startup_whale_tracker():
    tracker = WhaleTracker()
    try:
        print("[startup] running whale_tracker.run_once() ...")
        results = await tracker.run_once(db_module=db)
        print(f"[startup] whale_tracker finished — {len(results)} wallets saved")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[startup] whale_tracker ERROR: {exc}", flush=True)
    finally:
        await tracker.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[startup] lifespan begin")

    # Auto-start the trading bot so it survives Railway redeploys without
    # needing a manual POST /api/bot/start each time.
    bot.start()
    print("[startup] bot auto-started")

    whale_tracker_task = None
    if os.getenv("RUN_STARTUP_WHALE_TRACKER", "false").lower() == "true":
        whale_tracker_task = asyncio.create_task(_run_startup_whale_tracker())
    else:
        print("[startup] whale_tracker startup run disabled", flush=True)

    # Keep retrying resolution_price for settled trades whose finalPrice was late.
    resolution_backfill_task = asyncio.create_task(_periodic_resolution_backfill())

    print("[startup] lifespan ready")
    yield
    print("[shutdown] lifespan end")
    if whale_tracker_task:
        whale_tracker_task.cancel()
    resolution_backfill_task.cancel()
    if whale_tracker_task:
        try:
            await whale_tracker_task
        except asyncio.CancelledError:
            pass
    try:
        await resolution_backfill_task
    except asyncio.CancelledError:
        pass
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
def get_bot_trades(mode: str = "paper", family: str | None = None, strategy: str | None = None):
    """Last 20 bot trades. mode=paper|live|shadow; family can split shadow buckets."""
    if not family and not strategy:
        return bot.get_recent_trades(n=20, mode=mode)

    family_sql, family_params = _family_filter_sql(family)
    strategy_sql = " AND strategy = %s" if strategy else ""
    params = (mode,) + family_params + ((strategy,) if strategy else ()) + (20,)
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM bot_trades "
            "WHERE COALESCE(mode,'paper')=%s "
            f"{family_sql}{strategy_sql} "
            "ORDER BY opened_at DESC LIMIT %s",
            params,
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


@app.get("/api/bot/stats")
def get_bot_stats(mode: str = "paper", family: str | None = None, strategy: str | None = None):
    """Aggregate performance stats. mode=paper|live|shadow; family can split shadow buckets."""
    family_sql, family_params = _family_filter_sql(family)
    strategy_sql = "AND strategy = %s " if strategy else ""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT pnl, outcome, side, opened_at FROM bot_trades "
            "WHERE pnl IS NOT NULL AND COALESCE(outcome, '') != 'unresolved' "
            "AND COALESCE(mode,'paper')=%s "
            f"{family_sql} {strategy_sql}"
            "ORDER BY opened_at",
            (mode,) + family_params + ((strategy,) if strategy else ()),
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


@app.get("/api/bot/shadow/summary")
def get_shadow_summary():
    """Dashboard-ready shadow stats split by BTC market family."""
    conn = db.get_connection()
    try:
        summaries = []
        for key, spec in SHADOW_FAMILIES.items():
            strategy_clause = ""
            params = ["shadow", spec["slug_like"]]
            if spec.get("strategy"):
                strategy_clause = "AND strategy = %s"
                params.append(spec["strategy"])
            elif spec.get("strategy_like"):
                strategy_clause = "AND strategy LIKE %s"
                params.append(spec["strategy_like"])
            params = tuple(params)
            row = conn.execute(
                """SELECT
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome = 'unresolved' THEN 1 ELSE 0 END) AS unresolved,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS losses,
                       COALESCE(SUM(pnl), 0) AS pnl
                     FROM bot_trades
                    WHERE mode = %s
                      AND market_slug LIKE %s """
                + strategy_clause,
                params,
            ).fetchone()
            recent = conn.execute(
                """SELECT *
                     FROM bot_trades
                    WHERE mode = %s
                      AND market_slug LIKE %s """
                + strategy_clause
                + " ORDER BY opened_at DESC LIMIT 10",
                params,
            ).fetchall()
            resolved = int(row["wins"] or 0) + int(row["losses"] or 0)
            summaries.append({
                "family": key,
                "label": spec["label"],
                "strategy": spec.get("strategy"),
                "total_trades": int(row["total"] or 0),
                "unresolved": int(row["unresolved"] or 0),
                "wins": int(row["wins"] or 0),
                "losses": int(row["losses"] or 0),
                "win_rate": round((int(row["wins"] or 0) / resolved), 4) if resolved else 0.0,
                "total_pnl": round(float(row["pnl"] or 0.0), 2),
                "recent_trades": [dict(r) for r in recent],
            })
    finally:
        conn.close()
    return {"ok": True, "families": summaries}


@app.get("/api/bot/live-attempts")
def get_live_attempts(limit: int = 100):
    """Recent failed live order attempts with eventual would-have-won annotation."""
    limit = max(1, min(int(limit), 500))
    return {"ok": True, "attempts": db.get_live_order_attempts(limit=limit)}


@app.get("/api/bot/live-attempts/summary")
def get_live_attempt_summary():
    """Aggregate failed live attempts for dashboard cards."""
    return {"ok": True, "summary": db.get_live_order_attempt_summary()}


@app.get("/api/markets")
async def get_markets():
    """Configured markets plus current/next short-horizon Polymarket windows when available."""
    results = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        for item in catalog():
            row = dict(item)
            row["markets"] = []
            if item in FIVE_MINUTE_MARKETS or item in FIFTEEN_MINUTE_MARKETS or item in DAILY_MARKETS:
                slug_candidates = (
                    current_daily_slugs(item)
                    if item in DAILY_MARKETS
                    else current_interval_slugs(item)
                )
                for slug in slug_candidates:
                    try:
                        async with session.get(
                            "https://gamma-api.polymarket.com/markets",
                            params={"slug": slug},
                        ) as resp:
                            if resp.status != 200:
                                continue
                            data = await resp.json()
                    except Exception as exc:
                        row["error"] = str(exc)
                        continue
                    if not data:
                        continue
                    market = data[0]
                    row["markets"].append(
                        {
                            "slug": market.get("slug"),
                            "question": market.get("question"),
                            "closed": market.get("closed"),
                            "active": market.get("active"),
                            "volume": float(market.get("volume") or 0),
                            "liquidity": float(market.get("liquidity") or 0),
                            "outcomePrices": market.get("outcomePrices"),
                            "bestBid": market.get("bestBid"),
                            "bestAsk": market.get("bestAsk"),
                            "lastTradePrice": market.get("lastTradePrice"),
                        }
                    )
            results.append(row)
    return results


@app.get("/api/bot/stats/hourly")
def get_hourly_stats(mode: str = "paper"):
    """Per-ET-hour breakdown. mode=paper|live"""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT pnl, opened_at FROM bot_trades "
            "WHERE pnl IS NOT NULL AND COALESCE(outcome, '') != 'unresolved' "
            "AND COALESCE(mode,'paper')=%s "
            "ORDER BY opened_at",
            (mode,),
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


@app.get("/api/live/health")
async def live_health(side: str = "Up"):
    """Read-only CLOB auth, allowance, orderbook, and active market check."""
    try:
        return await live_clob.health(side=side)
    except live_clob.LiveClobError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/live/update-balance")
async def live_update_balance():
    """Tell the CLOB to re-read on-chain USDC balance + allowances for the funder."""
    try:
        return await live_clob.update_balance()
    except live_clob.LiveClobError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/api/live/diagnose")
async def live_diagnose():
    """Probe all signature_type combinations to locate the funder."""
    try:
        return await live_clob.diagnose()
    except live_clob.LiveClobError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/live/update-balance")
async def live_update_balance(x_live_test_token: str = Header(default="")):
    """Force the CLOB to re-read on-chain USDC balance + allowances."""
    expected_token = os.getenv("LIVE_TEST_TOKEN")
    if not expected_token or x_live_test_token != expected_token:
        raise HTTPException(status_code=403, detail="Invalid or missing live test token")
    try:
        return await live_clob.update_balance()
    except live_clob.LiveClobError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/live/test-buy")
async def live_test_buy(body: dict, x_live_test_token: str = Header(default="")):
    """
    Manual protected $1-ish FOK/FAK CLOB test buy.

    Requires Railway env:
      POLYMARKET_LIVE_TEST=true
      LIVE_TEST_TOKEN=<secret>
    and request body:
      {"confirm": "BUY_TEST", "side": "Up", "amount": 1.0}
    """
    expected_token = os.getenv("LIVE_TEST_TOKEN")
    if not expected_token or x_live_test_token != expected_token:
        raise HTTPException(status_code=403, detail="Invalid or missing live test token")
    if body.get("confirm") != "BUY_TEST":
        raise HTTPException(status_code=400, detail='Body must include confirm="BUY_TEST"')

    side = str(body.get("side", ""))
    amount = float(body.get("amount", os.getenv("LIVE_TEST_MAX_USDC", "1.0")))
    try:
        return await live_clob.test_buy(side=side, amount=amount)
    except live_clob.LiveClobError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/admin/archive-paper-legacy")
async def archive_paper_legacy(body: dict, x_live_test_token: str = Header(default="")):
    """Archive pre-current-era paper rows so current stats stay comparable."""
    expected_token = os.getenv("LIVE_TEST_TOKEN")
    if not expected_token or x_live_test_token != expected_token:
        raise HTTPException(status_code=403, detail="Invalid or missing live test token")
    if body.get("confirm") != "ARCHIVE_PAPER_LEGACY":
        raise HTTPException(status_code=400, detail="confirm must be ARCHIVE_PAPER_LEGACY")

    cutoff = body.get("cutoff") or "2026-05-11T14:14:00+00:00"
    conn = db.get_connection()
    try:
        before = conn.execute(
            """SELECT COUNT(*) AS trades, COALESCE(SUM(pnl), 0) AS pnl
                 FROM bot_trades
                WHERE COALESCE(mode, 'paper') = 'paper'
                  AND opened_at < %s""",
            (cutoff,),
        ).fetchone()
        conn.execute(
            """UPDATE bot_trades
                  SET mode = 'paper_legacy'
                WHERE COALESCE(mode, 'paper') = 'paper'
                  AND opened_at < %s""",
            (cutoff,),
        )
        current = conn.execute(
            """SELECT COUNT(*) AS trades, COALESCE(SUM(pnl), 0) AS pnl
                 FROM bot_trades
                WHERE COALESCE(mode, 'paper') = 'paper'
                  AND pnl IS NOT NULL
                  AND COALESCE(outcome, '') != 'unresolved'""",
        ).fetchone()
        conn.commit()
    finally:
        conn.close()

    bot._reload_accounting_state()
    return {
        "ok": True,
        "cutoff": cutoff,
        "archived": {
            "trades": int(before["trades"] or 0),
            "pnl": round(float(before["pnl"] or 0.0), 2),
        },
        "current_paper": {
            "trades": int(current["trades"] or 0),
            "pnl": round(float(current["pnl"] or 0.0), 2),
            "balance": round(bot.balance, 2),
        },
    }


@app.post("/api/admin/delete-shadow-btc5")
async def delete_shadow_btc5(body: dict, x_live_test_token: str = Header(default="")):
    """Delete old BTC 5m shadow rows so shadow stats can start clean for BTC15."""
    expected_token = os.getenv("LIVE_TEST_TOKEN")
    if not expected_token or x_live_test_token != expected_token:
        raise HTTPException(status_code=403, detail="Invalid or missing live test token")
    if body.get("confirm") != "DELETE_SHADOW_BTC5":
        raise HTTPException(status_code=400, detail="confirm must be DELETE_SHADOW_BTC5")

    conn = db.get_connection()
    try:
        before = conn.execute(
            """SELECT COUNT(*) AS trades, COALESCE(SUM(pnl), 0) AS pnl
                 FROM bot_trades
                WHERE mode = 'shadow'
                  AND market_slug LIKE 'btc-updown-5m-%'"""
        ).fetchone()
        conn.execute(
            """DELETE FROM bot_trades
                WHERE mode = 'shadow'
                  AND market_slug LIKE 'btc-updown-5m-%'"""
        )
        remaining = conn.execute(
            """SELECT
                    SUM(CASE WHEN market_slug LIKE 'btc-updown-5m-%' THEN 1 ELSE 0 END) AS btc5,
                    SUM(CASE WHEN market_slug LIKE 'btc-updown-15m-%' THEN 1 ELSE 0 END) AS btc15,
                    COUNT(*) AS total
                 FROM bot_trades
                WHERE mode = 'shadow'"""
        ).fetchone()
        conn.commit()
    finally:
        conn.close()

    return {
        "ok": True,
        "deleted": {
            "trades": int(before["trades"] or 0),
            "pnl": round(float(before["pnl"] or 0.0), 2),
        },
        "remaining_shadow": {
            "btc5": int(remaining["btc5"] or 0),
            "btc15": int(remaining["btc15"] or 0),
            "total": int(remaining["total"] or 0),
        },
    }


@app.post("/api/admin/reconcile-live-balance")
async def reconcile_live_balance(body: dict, x_live_test_token: str = Header(default="")):
    """Adjust the latest live ledger row so derived live balance matches Polymarket cash."""
    expected_token = os.getenv("LIVE_TEST_TOKEN")
    if not expected_token or x_live_test_token != expected_token:
        raise HTTPException(status_code=403, detail="Invalid or missing live test token")

    actual_balance = round(float(body.get("actual_balance")), 4)
    current_balance = round(float(bot.live_balance), 4)
    delta = round(actual_balance - current_balance, 4)
    if abs(delta) < 0.0001:
        return {
            "ok": True,
            "changed": False,
            "balance": actual_balance,
            "delta": 0.0,
        }

    conn = db.get_connection()
    try:
        row = conn.execute(
            """SELECT id, pnl, balance_after
                 FROM bot_trades
                WHERE mode = 'live'
                  AND pnl IS NOT NULL
                ORDER BY opened_at DESC
                LIMIT 1"""
        ).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="No live trade row to reconcile")

        new_pnl = round(float(row["pnl"] or 0.0) + delta, 4)
        conn.execute(
            """UPDATE bot_trades
                  SET pnl = %s,
                      balance_after = %s
                WHERE id = %s""",
            (new_pnl, actual_balance, row["id"]),
        )
        conn.commit()
    finally:
        conn.close()

    bot.live_balance = actual_balance
    bot.live_pnl = round(bot.live_pnl + delta, 4)
    return {
        "ok": True,
        "changed": True,
        "trade_id": row["id"],
        "old_balance": current_balance,
        "new_balance": actual_balance,
        "delta": delta,
        "old_pnl": round(float(row["pnl"] or 0.0), 4),
        "new_pnl": new_pnl,
    }


@app.get("/api/live/signer")
def live_signer():
    """Return the public address derived from PRIVATE_KEY/PK — no secrets exposed."""
    from eth_account import Account

    pk = os.getenv("PRIVATE_KEY") or os.getenv("PK") or ""
    if not pk:
        raise HTTPException(status_code=503, detail="PRIVATE_KEY/PK not set")
    pk = pk if pk.startswith("0x") else "0x" + pk
    address = Account.from_key(pk).address
    return {"signer": address}


@app.post("/api/bot/backfill-resolutions")
async def backfill_bot_resolutions():
    """Manually retry finalPrice backfill for settled trades that still show FINAL —."""
    return {"ok": True, **await _backfill_resolution_prices()}


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
    """Return the latest BTC/USD price — Chainlink preferred, Binance REST fallback."""
    ts = datetime.now(timezone.utc).isoformat()

    try:
        loop = asyncio.get_running_loop()
        price = await loop.run_in_executor(None, get_btc_price)
        return {"price": round(price, 2), "source": "chainlink", "timestamp": ts}
    except Exception:
        pass

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "price": round(float(data["price"]), 2),
                        "source": "binance-rest",
                        "timestamp": ts,
                    }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"All price sources failed: {e}")


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


@app.post("/api/claude-analysis")
async def claude_analysis(body: dict):
    """Run Claude analysis server-side so the Anthropic key never reaches the browser."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY is not configured")

    funding_rates = body.get("fundingRates") or []
    onchain_signals = body.get("onchainSignals") or []
    if not isinstance(funding_rates, list) or not isinstance(onchain_signals, list):
        raise HTTPException(status_code=400, detail="fundingRates and onchainSignals must be lists")

    def format_rate(rate: dict) -> str:
        symbol = str(rate.get("symbol", "UNKNOWN"))[:24]
        try:
            funding_rate = float(rate.get("fundingRate", 0))
        except (TypeError, ValueError):
            funding_rate = 0.0
        return f"{symbol}: {funding_rate:+.4f}%"

    def format_signal(signal: dict) -> str:
        symbol = str(signal.get("symbol", "UNKNOWN"))[:24]
        sentiment = str(signal.get("sentiment", "unknown"))[:24]
        strength = signal.get("strength", "n/a")
        return f"{symbol}: {sentiment} ({strength})"

    rates_ctx = ", ".join(format_rate(rate) for rate in funding_rates[:10])
    signals_ctx = ", ".join(format_signal(signal) for signal in onchain_signals[:20])

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 512,
        "system": "You are a sharp crypto analyst. Be concise, direct, and actionable.",
        "messages": [{
            "role": "user",
            "content": (
                "Analyze these live funding rates and onchain signals. "
                "Give 2-3 specific trading insights.\n\n"
                f"Funding Rates (top 10 by abs value):\n{rates_ctx}\n\n"
                f"Onchain Signals:\n{signals_ctx}\n\n"
                "What are the best opportunities and key risks right now?"
            ),
        }],
    }

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                },
                json=payload,
            ) as resp:
                data = await resp.json()
                if resp.status >= 400:
                    detail = data.get("error", {}).get("message", "Claude request failed")
                    raise HTTPException(status_code=resp.status, detail=detail)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    return {"ok": True, "analysis": text or "No response."}
