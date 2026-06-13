"""maker_shadow_attempts table helpers (separate from db.py to keep refactor surgical)."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import db


_init_lock = threading.Lock()
_initialised = False
_REPAIR_PNL_HOURS = 48


def _pk_type() -> str:
    return "SERIAL PRIMARY KEY" if getattr(db, "_USE_PG", False) else "INTEGER PRIMARY KEY AUTOINCREMENT"


def _ensure_table() -> None:
    global _initialised
    if _initialised:
        return
    with _init_lock:
        if _initialised:
            return
        pk = _pk_type()
        ddl = f"""
        CREATE TABLE IF NOT EXISTS maker_shadow_attempts (
            id {pk},
            order_id TEXT UNIQUE NOT NULL,
            dispatched_at TEXT NOT NULL,
            market_slug TEXT NOT NULL,
            condition_id TEXT,
            token_id TEXT,
            side TEXT NOT NULL,
            regime TEXT,
            intended_initial_price REAL,
            intended_size_usdc REAL,
            diff_at_dispatch REAL,
            sec_rem_at_dispatch REAL,
            reprices_count INTEGER DEFAULT 0,
            final_resting_price REAL,
            time_resting_sec REAL,
            hypothetical_filled_conservative INTEGER DEFAULT 0,
            hypothetical_filled_aggressive INTEGER DEFAULT 0,
            hypothetical_fill_price REAL,
            hypothetical_fill_at TEXT,
            hypothetical_size_filled_usdc REAL,
            cancel_reason TEXT,
            settlement_outcome TEXT,
            hypothetical_pnl_conservative REAL,
            hypothetical_pnl_aggressive REAL,
            book_updates_during_window INTEGER DEFAULT 0,
            trade_prints_during_window INTEGER DEFAULT 0,
            book_ws_uptime_pct REAL,
            code_version TEXT DEFAULT 'maker_shadow_v1',
            exec_mode TEXT,
            queue_ahead_at_placement REAL,
            queue_cleared_before_fill REAL DEFAULT 0,
            trade_aggressor_side TEXT,
            direction_rejections INTEGER DEFAULT 0,
            post_fill_mark_1s REAL,
            post_fill_delta_1s REAL,
            post_fill_mark_3s REAL,
            post_fill_delta_3s REAL,
            post_fill_mark_5s REAL,
            post_fill_delta_5s REAL,
            post_fill_mark_10s REAL,
            post_fill_delta_10s REAL
        );
        CREATE INDEX IF NOT EXISTS idx_maker_shadow_slug ON maker_shadow_attempts(market_slug);
        CREATE INDEX IF NOT EXISTS idx_maker_shadow_dispatched ON maker_shadow_attempts(dispatched_at);
        """
        conn = db.get_connection()
        try:
            conn.executescript(ddl)
            try:
                conn.commit()
            except Exception:
                pass
            # Fix 2 migration: ALTER existing deployments to add the new columns.
            # Postgres supports IF NOT EXISTS; SQLite errors on duplicate so we
            # swallow per-statement failures.
            add_col = "ADD COLUMN IF NOT EXISTS" if getattr(db, "_USE_PG", False) else "ADD COLUMN"
            migrations = [
                f"ALTER TABLE maker_shadow_attempts {add_col} queue_ahead_at_placement REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} queue_cleared_before_fill REAL DEFAULT 0",
                f"ALTER TABLE maker_shadow_attempts {add_col} trade_aggressor_side TEXT",
                f"ALTER TABLE maker_shadow_attempts {add_col} direction_rejections INTEGER DEFAULT 0",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_mark_1s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_delta_1s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_mark_3s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_delta_3s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_mark_5s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_delta_5s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_mark_10s REAL",
                f"ALTER TABLE maker_shadow_attempts {add_col} post_fill_delta_10s REAL",
            ]
            for stmt in migrations:
                try:
                    conn.execute(stmt)
                    try:
                        conn.commit()
                    except Exception:
                        pass
                except Exception:
                    # Column already exists (or unsupported syntax) — ignore.
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    pass
            _repair_recent_settled_pnl(conn)
        finally:
            conn.close()
        _initialised = True


def _pnl_for_fill(side: str, winner: str, fill_notional: float, fill_price: float) -> float:
    if fill_notional <= 0:
        return 0.0
    if fill_price <= 0:
        fill_price = 0.5
    if side == winner:
        return fill_notional * (1.0 / fill_price - 1.0)
    return -fill_notional


def _repair_recent_settled_pnl(conn) -> None:
    """Backfill old settled rows to use actual filled notional instead of intended stake."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=_REPAIR_PNL_HOURS)).isoformat()
    try:
        rows = conn.execute(
            """SELECT id, side, settlement_outcome, hypothetical_fill_price,
                      hypothetical_size_filled_usdc,
                      hypothetical_filled_aggressive, hypothetical_filled_conservative,
                      hypothetical_pnl_aggressive, hypothetical_pnl_conservative
                 FROM maker_shadow_attempts
                WHERE dispatched_at >= %s
                  AND settlement_outcome IS NOT NULL""",
            (cutoff,),
        ).fetchall()
    except Exception:
        return
    for row in rows:
        fill_notional = float(row["hypothetical_size_filled_usdc"] or 0.0)
        fill_price = float(row["hypothetical_fill_price"] or 0.0)
        winner = row["settlement_outcome"]
        side = row["side"]
        aggr_filled = int(row["hypothetical_filled_aggressive"] or 0) == 1
        cons_filled = int(row["hypothetical_filled_conservative"] or 0) == 1
        aggr_pnl = _pnl_for_fill(side, winner, fill_notional, fill_price) if aggr_filled else 0.0
        cons_pnl = _pnl_for_fill(side, winner, fill_notional, fill_price) if cons_filled else 0.0
        if (
            round(float(row["hypothetical_pnl_aggressive"] or 0.0), 4) == round(aggr_pnl, 4)
            and round(float(row["hypothetical_pnl_conservative"] or 0.0), 4) == round(cons_pnl, 4)
        ):
            continue
        conn.execute(
            """UPDATE maker_shadow_attempts
                  SET hypothetical_pnl_aggressive = %s,
                      hypothetical_pnl_conservative = %s
                WHERE id = %s""",
            (round(aggr_pnl, 4), round(cons_pnl, 4), row["id"]),
        )
    try:
        conn.commit()
    except Exception:
        pass


_INSERT_COLS = [
    "order_id", "dispatched_at", "market_slug", "condition_id", "token_id",
    "side", "regime", "intended_initial_price", "intended_size_usdc",
    "diff_at_dispatch", "sec_rem_at_dispatch", "exec_mode",
]


def insert_attempt(row: dict) -> None:
    _ensure_table()
    cols = ", ".join(_INSERT_COLS)
    placeholders = ", ".join(["%s"] * len(_INSERT_COLS))
    values = tuple(row.get(c) for c in _INSERT_COLS)
    conn = db.get_connection()
    try:
        conn.execute(
            f"INSERT INTO maker_shadow_attempts ({cols}) VALUES ({placeholders})",
            values,
        )
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


_UPDATE_COLS = {
    "reprices_count", "final_resting_price", "time_resting_sec",
    "hypothetical_filled_conservative", "hypothetical_filled_aggressive",
    "hypothetical_fill_price", "hypothetical_fill_at",
    "hypothetical_size_filled_usdc", "cancel_reason",
    "settlement_outcome", "hypothetical_pnl_conservative",
    "hypothetical_pnl_aggressive", "book_updates_during_window",
    "trade_prints_during_window", "book_ws_uptime_pct",
    # Fix 2 audit columns
    "queue_ahead_at_placement", "queue_cleared_before_fill",
    "trade_aggressor_side", "direction_rejections",
    # Post-fill adverse-selection audit columns
    "post_fill_mark_1s", "post_fill_delta_1s",
    "post_fill_mark_3s", "post_fill_delta_3s",
    "post_fill_mark_5s", "post_fill_delta_5s",
    "post_fill_mark_10s", "post_fill_delta_10s",
}


def update_attempt(order_id: str, fields: dict) -> None:
    _ensure_table()
    sanitised = {k: v for k, v in fields.items() if k in _UPDATE_COLS}
    if not sanitised:
        return
    set_clause = ", ".join(f"{k} = %s" for k in sanitised)
    values = tuple(sanitised.values()) + (order_id,)
    conn = db.get_connection()
    try:
        conn.execute(
            f"UPDATE maker_shadow_attempts SET {set_clause} WHERE order_id = %s",
            values,
        )
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def counts_24h() -> dict:
    _ensure_table()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """WITH logical AS (
                   SELECT *
                     FROM maker_shadow_attempts msa
                    WHERE dispatched_at >= %s
                      AND (
                           hypothetical_filled_aggressive = 1
                        OR hypothetical_filled_conservative = 1
                        OR (
                             NOT EXISTS (
                                 SELECT 1
                                   FROM maker_shadow_attempts f
                                  WHERE f.market_slug = msa.market_slug
                                    AND f.side = msa.side
                                    AND (f.hypothetical_filled_aggressive = 1
                                      OR f.hypothetical_filled_conservative = 1)
                             )
                             AND msa.id = (
                                 SELECT MAX(last_row.id)
                                   FROM maker_shadow_attempts last_row
                                  WHERE last_row.market_slug = msa.market_slug
                                    AND last_row.side = msa.side
                             )
                           )
                      )
               )
               SELECT
                   SUM(CASE WHEN cancel_reason IS NULL
                              AND hypothetical_filled_conservative=0
                              AND hypothetical_filled_aggressive=0 THEN 1 ELSE 0 END) AS open_count,
                   SUM(CASE WHEN hypothetical_filled_aggressive=1 THEN 1 ELSE 0 END) AS filled_aggr,
                   SUM(CASE WHEN hypothetical_filled_conservative=1 THEN 1 ELSE 0 END) AS filled_cons,
                   SUM(CASE WHEN cancel_reason IS NOT NULL THEN 1 ELSE 0 END) AS cancelled
                 FROM logical""",
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return {"open": 0, "filled_aggressive_24h": 0, "filled_conservative_24h": 0, "cancelled_24h": 0}
    r = rows[0]

    def _g(key, idx):
        try:
            return int((r[key] if isinstance(r, dict) or hasattr(r, "keys") else r[idx]) or 0)
        except Exception:
            try:
                return int(r[idx] or 0)
            except Exception:
                return 0

    return {
        "open": _g("open_count", 0),
        "filled_aggressive_24h": _g("filled_aggr", 1),
        "filled_conservative_24h": _g("filled_cons", 2),
        "cancelled_24h": _g("cancelled", 3),
    }


def recent_attempts(limit: int = 100) -> list[dict]:
    """Return recent maker-shadow execution attempts for dashboard/replay audit."""
    _ensure_table()
    limit = max(1, min(int(limit), 500))
    conn = db.get_connection()
    try:
        _repair_recent_settled_pnl(conn)
        rows = conn.execute(
            """WITH logical AS (
                   SELECT *
                     FROM maker_shadow_attempts msa
                    WHERE hypothetical_filled_aggressive = 1
                       OR hypothetical_filled_conservative = 1
                       OR (
                            NOT EXISTS (
                                SELECT 1
                                  FROM maker_shadow_attempts f
                                 WHERE f.market_slug = msa.market_slug
                                   AND f.side = msa.side
                                   AND (f.hypothetical_filled_aggressive = 1
                                     OR f.hypothetical_filled_conservative = 1)
                            )
                            AND msa.id = (
                                SELECT MAX(last_row.id)
                                  FROM maker_shadow_attempts last_row
                                 WHERE last_row.market_slug = msa.market_slug
                                   AND last_row.side = msa.side
                            )
                          )
               )
               SELECT *
                 FROM logical
                ORDER BY dispatched_at DESC
                LIMIT %s""",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def load_unresolved_attempt_markets(limit: int = 200) -> list[dict]:
    """Return distinct maker-shadow markets that still need settlement annotation."""
    _ensure_table()
    limit = max(1, min(int(limit), 500))
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT market_slug, MAX(dispatched_at) AS latest_at
                 FROM maker_shadow_attempts
                WHERE settlement_outcome IS NULL
                GROUP BY market_slug
                ORDER BY latest_at DESC
                LIMIT %s""",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def settle_attempts_for_market(
    market_slug: str,
    winner: str,
) -> dict:
    """Annotate maker-shadow attempts for a resolved market and compute model P&L."""
    _ensure_table()
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT id, side, intended_size_usdc, hypothetical_fill_price,
                      hypothetical_size_filled_usdc,
                      hypothetical_filled_aggressive, hypothetical_filled_conservative
                 FROM maker_shadow_attempts
                WHERE market_slug = %s
                  AND settlement_outcome IS NULL""",
            (market_slug,),
        ).fetchall()
        updated = 0
        aggr_pnl_total = 0.0
        cons_pnl_total = 0.0
        for row in rows:
            side = row["side"]
            fill_notional = float(row["hypothetical_size_filled_usdc"] or 0.0)
            fallback_stake = float(row["intended_size_usdc"] or 0.0)
            fill_price = float(row["hypothetical_fill_price"] or 0.0)
            if fill_price <= 0:
                fill_price = 0.5
            won = side == winner

            aggr_filled = int(row["hypothetical_filled_aggressive"] or 0) == 1
            cons_filled = int(row["hypothetical_filled_conservative"] or 0) == 1
            aggr_notional = fill_notional if fill_notional > 0 else fallback_stake
            cons_notional = fill_notional if fill_notional > 0 else fallback_stake
            aggr_pnl = _pnl_for_fill(side, winner, aggr_notional, fill_price) if aggr_filled else 0.0
            cons_pnl = _pnl_for_fill(side, winner, cons_notional, fill_price) if cons_filled else 0.0
            aggr_pnl_total += aggr_pnl
            cons_pnl_total += cons_pnl

            conn.execute(
                """UPDATE maker_shadow_attempts
                      SET settlement_outcome = %s,
                          hypothetical_pnl_aggressive = %s,
                          hypothetical_pnl_conservative = %s
                    WHERE id = %s""",
                (
                    winner,
                    round(aggr_pnl, 4),
                    round(cons_pnl, 4),
                    row["id"],
                ),
            )
            updated += 1
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()
    return {
        "count": updated,
        "hypothetical_pnl_aggressive": round(aggr_pnl_total, 4),
        "hypothetical_pnl_conservative": round(cons_pnl_total, 4),
    }


def summary_24h() -> dict:
    """Aggregate maker-shadow execution attempt health over the last 24h."""
    _ensure_table()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    conn = db.get_connection()
    try:
        _repair_recent_settled_pnl(conn)
        row = conn.execute(
            """WITH logical AS (
                   SELECT *
                     FROM maker_shadow_attempts msa
                    WHERE dispatched_at >= %s
                      AND (
                           hypothetical_filled_aggressive = 1
                        OR hypothetical_filled_conservative = 1
                        OR (
                             NOT EXISTS (
                                 SELECT 1
                                   FROM maker_shadow_attempts f
                                  WHERE f.market_slug = msa.market_slug
                                    AND f.side = msa.side
                                    AND (f.hypothetical_filled_aggressive = 1
                                      OR f.hypothetical_filled_conservative = 1)
                             )
                             AND msa.id = (
                                 SELECT MAX(last_row.id)
                                   FROM maker_shadow_attempts last_row
                                  WHERE last_row.market_slug = msa.market_slug
                                    AND last_row.side = msa.side
                             )
                           )
                      )
               )
               SELECT
                   COUNT(*) AS total,
                   SUM(CASE WHEN cancel_reason IS NULL
                              AND hypothetical_filled_aggressive=0
                              AND hypothetical_filled_conservative=0 THEN 1 ELSE 0 END) AS open_count,
                   SUM(CASE WHEN hypothetical_filled_aggressive=1 THEN 1 ELSE 0 END) AS filled_aggr,
                   SUM(CASE WHEN hypothetical_filled_conservative=1 THEN 1 ELSE 0 END) AS filled_cons,
                   SUM(CASE WHEN cancel_reason IS NOT NULL THEN 1 ELSE 0 END) AS cancelled,
                   SUM(CASE WHEN settlement_outcome IS NOT NULL THEN 1 ELSE 0 END) AS settled,
                   SUM(CASE WHEN settlement_outcome IS NOT NULL
                              AND (hypothetical_filled_aggressive=1 OR hypothetical_filled_conservative=1)
                            THEN 1 ELSE 0 END) AS filled_settled,
                   SUM(CASE WHEN settlement_outcome IS NOT NULL
                              AND (hypothetical_filled_aggressive=1 OR hypothetical_filled_conservative=1)
                              AND side=settlement_outcome
                            THEN 1 ELSE 0 END) AS filled_won,
                   SUM(CASE WHEN settlement_outcome IS NOT NULL
                              AND hypothetical_filled_aggressive=0
                              AND hypothetical_filled_conservative=0
                            THEN 1 ELSE 0 END) AS missed_settled,
                   SUM(CASE WHEN settlement_outcome IS NOT NULL
                              AND hypothetical_filled_aggressive=0
                              AND hypothetical_filled_conservative=0
                              AND side=settlement_outcome
                            THEN 1 ELSE 0 END) AS missed_won,
                   COALESCE(SUM(hypothetical_pnl_aggressive), 0) AS pnl_aggr,
                   COALESCE(SUM(hypothetical_pnl_conservative), 0) AS pnl_cons,
                   COALESCE(AVG(queue_ahead_at_placement), 0) AS avg_queue_ahead,
                   COALESCE(AVG(reprices_count), 0) AS avg_reprices,
                   COALESCE(SUM(direction_rejections), 0) AS direction_rejections,
                   AVG(post_fill_delta_1s) AS avg_post_fill_delta_1s,
                   AVG(post_fill_delta_3s) AS avg_post_fill_delta_3s,
                   AVG(post_fill_delta_5s) AS avg_post_fill_delta_5s,
                   AVG(post_fill_delta_10s) AS avg_post_fill_delta_10s
                 FROM logical""",
            (cutoff,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {
            "total": 0,
            "open": 0,
            "filled_aggressive": 0,
            "filled_conservative": 0,
            "cancelled": 0,
            "fill_rate_aggressive": 0.0,
            "fill_rate_conservative": 0.0,
            "settled": 0,
            "won": 0,
            "filled_settled": 0,
            "filled_won": 0,
            "win_rate": 0.0,
            "missed_settled": 0,
            "missed_won": 0,
            "missed_win_rate": 0.0,
            "hypothetical_pnl_aggressive": 0.0,
            "hypothetical_pnl_conservative": 0.0,
            "avg_queue_ahead": 0.0,
            "avg_reprices": 0.0,
            "direction_rejections": 0,
            "avg_post_fill_delta_1s": None,
            "avg_post_fill_delta_3s": None,
            "avg_post_fill_delta_5s": None,
            "avg_post_fill_delta_10s": None,
        }

    total = int(row["total"] or 0)
    filled_aggr = int(row["filled_aggr"] or 0)
    filled_cons = int(row["filled_cons"] or 0)
    settled = int(row["settled"] or 0)
    filled_settled = int(row["filled_settled"] or 0)
    filled_won = int(row["filled_won"] or 0)
    missed_settled = int(row["missed_settled"] or 0)
    missed_won = int(row["missed_won"] or 0)
    return {
        "total": total,
        "open": int(row["open_count"] or 0),
        "filled_aggressive": filled_aggr,
        "filled_conservative": filled_cons,
        "cancelled": int(row["cancelled"] or 0),
        "fill_rate_aggressive": round(filled_aggr / total, 4) if total else 0.0,
        "fill_rate_conservative": round(filled_cons / total, 4) if total else 0.0,
        "settled": settled,
        "won": filled_won,
        "filled_settled": filled_settled,
        "filled_won": filled_won,
        "win_rate": round(filled_won / filled_settled, 4) if filled_settled else 0.0,
        "missed_settled": missed_settled,
        "missed_won": missed_won,
        "missed_win_rate": round(missed_won / missed_settled, 4) if missed_settled else 0.0,
        "hypothetical_pnl_aggressive": round(float(row["pnl_aggr"] or 0.0), 4),
        "hypothetical_pnl_conservative": round(float(row["pnl_cons"] or 0.0), 4),
        "avg_queue_ahead": round(float(row["avg_queue_ahead"] or 0.0), 4),
        "avg_reprices": round(float(row["avg_reprices"] or 0.0), 4),
        "direction_rejections": int(row["direction_rejections"] or 0),
        "avg_post_fill_delta_1s": (
            round(float(row["avg_post_fill_delta_1s"]), 4)
            if row["avg_post_fill_delta_1s"] is not None else None
        ),
        "avg_post_fill_delta_3s": (
            round(float(row["avg_post_fill_delta_3s"]), 4)
            if row["avg_post_fill_delta_3s"] is not None else None
        ),
        "avg_post_fill_delta_5s": (
            round(float(row["avg_post_fill_delta_5s"]), 4)
            if row["avg_post_fill_delta_5s"] is not None else None
        ),
        "avg_post_fill_delta_10s": (
            round(float(row["avg_post_fill_delta_10s"]), 4)
            if row["avg_post_fill_delta_10s"] is not None else None
        ),
    }
