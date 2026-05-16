"""maker_shadow_attempts table helpers (separate from db.py to keep refactor surgical)."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import db


_init_lock = threading.Lock()
_initialised = False


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
            exec_mode TEXT
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
        finally:
            conn.close()
        _initialised = True


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
            "SELECT "
            " SUM(CASE WHEN cancel_reason IS NULL "
            "          AND hypothetical_filled_conservative=0 "
            "          AND hypothetical_filled_aggressive=0 THEN 1 ELSE 0 END) AS open_count, "
            " SUM(CASE WHEN hypothetical_filled_aggressive=1 THEN 1 ELSE 0 END) AS filled_aggr, "
            " SUM(CASE WHEN hypothetical_filled_conservative=1 THEN 1 ELSE 0 END) AS filled_cons, "
            " SUM(CASE WHEN cancel_reason IS NOT NULL THEN 1 ELSE 0 END) AS cancelled "
            "FROM maker_shadow_attempts WHERE dispatched_at >= %s",
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
