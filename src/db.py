"""
SIGNAL/ZERO Phase 1 — Database Layer

Automatically selects the backend at startup:
  - DATABASE_URL set  → psycopg2 (PostgreSQL / Railway)
  - DATABASE_URL unset → sqlite3  (local development)

All SQL is written with %s placeholders. The _Connection wrapper
converts them to ? for SQLite so no query needs two versions.
"""

import os
import sqlite3
from datetime import datetime
from . import config

# ── Backend detection ──────────────────────────────────────────────────────────
_DATABASE_URL = os.environ.get("DATABASE_URL")
_USE_PG       = bool(_DATABASE_URL)

if _USE_PG:
    import psycopg2
    import psycopg2.extras

# Primary-key column definition differs between the two engines
_PK = "SERIAL PRIMARY KEY" if _USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"

# INSERT-ignore syntax differs; resolved once at module load
_INSERT_WHALE_TRADE = (
    """INSERT INTO whale_trades
           (wallet_address, market_slug, condition_id, side, outcome,
            size, price, timestamp, transaction_hash)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
       ON CONFLICT (transaction_hash) DO NOTHING"""
    if _USE_PG else
    """INSERT OR IGNORE INTO whale_trades
           (wallet_address, market_slug, condition_id, side, outcome,
            size, price, timestamp, transaction_hash)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
)


# ── Unified connection wrapper ─────────────────────────────────────────────────
class _Connection:
    """
    Wraps sqlite3 or psycopg2 behind one interface.

    • execute(sql, params) — %s placeholders work for both engines.
    • executescript(sql)   — handles PG's lack of executescript.
    • commit() / close()   — pass straight through.
    • execute() returns a cursor-like object with fetchone/fetchall.
    """

    def __init__(self, raw, is_pg: bool):
        self._raw   = raw
        self._is_pg = is_pg

    def execute(self, sql: str, params=()):
        if self._is_pg:
            cur = self._raw.cursor()
            cur.execute(sql, params if params else None)
            return cur
        # SQLite expects ? not %s
        return self._raw.execute(sql.replace("%s", "?"), params)

    def executescript(self, script: str):
        if self._is_pg:
            cur = self._raw.cursor()
            for stmt in (s.strip() for s in script.split(";") if s.strip()):
                cur.execute(stmt)
        else:
            self._raw.executescript(script)

    def commit(self):
        self._raw.commit()

    def close(self):
        self._raw.close()


def get_connection() -> _Connection:
    if _USE_PG:
        raw = psycopg2.connect(_DATABASE_URL)
        raw.cursor_factory = psycopg2.extras.RealDictCursor
        return _Connection(raw, is_pg=True)
    raw = sqlite3.connect(config.DB_PATH)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    return _Connection(raw, is_pg=False)


# ── Schema ─────────────────────────────────────────────────────────────────────
def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    conn.executescript(f"""
        CREATE TABLE IF NOT EXISTS price_ticks (
            id          {_PK},
            ts          TEXT NOT NULL,
            price       REAL NOT NULL,
            source      TEXT DEFAULT 'binance'
        );

        CREATE TABLE IF NOT EXISTS market_snapshots (
            id           {_PK},
            ts           TEXT NOT NULL,
            market_id    TEXT,
            condition_id TEXT,
            question     TEXT,
            up_price     REAL,
            down_price   REAL,
            volume       REAL,
            window_start TEXT,
            window_end   TEXT,
            resolved     INTEGER DEFAULT 0,
            outcome      TEXT
        );

        CREATE TABLE IF NOT EXISTS signals (
            id              {_PK},
            ts              TEXT NOT NULL,
            market_id       TEXT,
            btc_price       REAL,
            momentum        REAL,
            implied_up_prob REAL,
            our_up_prob     REAL,
            divergence      REAL,
            direction       TEXT,
            acted_on        INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS paper_trades (
            id            {_PK},
            opened_at     TEXT NOT NULL,
            closed_at     TEXT,
            market_id     TEXT,
            condition_id  TEXT,
            side          TEXT,
            entry_price   REAL,
            size          REAL,
            fee           REAL DEFAULT 0,
            outcome       TEXT,
            pnl           REAL,
            balance_after REAL
        );

        CREATE INDEX IF NOT EXISTS idx_ticks_ts      ON price_ticks(ts);
        CREATE INDEX IF NOT EXISTS idx_signals_ts     ON signals(ts);
        CREATE INDEX IF NOT EXISTS idx_trades_opened  ON paper_trades(opened_at);

        CREATE TABLE IF NOT EXISTS whale_wallets (
            id              {_PK},
            address         TEXT NOT NULL UNIQUE,
            win_rate        REAL NOT NULL,
            total_trades    INTEGER NOT NULL,
            resolved_trades INTEGER NOT NULL,
            wins            INTEGER NOT NULL,
            losses          INTEGER NOT NULL,
            pnl             REAL NOT NULL,
            last_updated    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS whale_trades (
            id               {_PK},
            wallet_address   TEXT NOT NULL,
            market_slug      TEXT,
            condition_id     TEXT,
            side             TEXT,
            outcome          TEXT,
            size             REAL,
            price            REAL,
            timestamp        INTEGER,
            transaction_hash TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS bot_trades (
            id            {_PK},
            whale_address TEXT NOT NULL,
            market_slug   TEXT NOT NULL,
            side          TEXT NOT NULL,
            size          REAL NOT NULL,
            entry_price   REAL,
            outcome       TEXT,
            pnl           REAL,
            opened_at     TEXT NOT NULL,
            closed_at     TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_whale_wallets_winrate ON whale_wallets(win_rate DESC);
        CREATE INDEX IF NOT EXISTS idx_whale_trades_wallet   ON whale_trades(wallet_address)
    """)
    conn.commit()
    conn.close()


# ── Write helpers ──────────────────────────────────────────────────────────────
def log_tick(price: float):
    conn = get_connection()
    conn.execute(
        "INSERT INTO price_ticks (ts, price) VALUES (%s, %s)",
        (datetime.utcnow().isoformat(), price),
    )
    conn.commit()
    conn.close()


def log_market_snapshot(market: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO market_snapshots
           (ts, market_id, condition_id, question, up_price, down_price,
            volume, window_start, window_end)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            datetime.utcnow().isoformat(),
            market.get("id", ""),
            market.get("condition_id", ""),
            market.get("question", ""),
            market.get("up_price", 0),
            market.get("down_price", 0),
            market.get("volume", 0),
            market.get("window_start", ""),
            market.get("window_end", ""),
        ),
    )
    conn.commit()
    conn.close()


def log_signal(signal: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO signals
           (ts, market_id, btc_price, momentum, implied_up_prob,
            our_up_prob, divergence, direction)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            datetime.utcnow().isoformat(),
            signal.get("market_id", ""),
            signal["btc_price"],
            signal["momentum"],
            signal["implied_up_prob"],
            signal.get("our_up_prob"),
            signal["divergence"],
            signal["direction"],
        ),
    )
    conn.commit()
    conn.close()


def log_paper_trade(trade: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO paper_trades
           (opened_at, market_id, condition_id, side, entry_price,
            size, fee, outcome, pnl, balance_after, closed_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            trade["opened_at"],
            trade.get("market_id", ""),
            trade.get("condition_id", ""),
            trade["side"],
            trade["entry_price"],
            trade["size"],
            trade.get("fee", 0),
            trade.get("outcome"),
            trade.get("pnl"),
            trade.get("balance_after"),
            trade.get("closed_at"),
        ),
    )
    conn.commit()
    conn.close()


def upsert_whale_wallet(stats: dict):
    """Insert or update a whale wallet record."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO whale_wallets
               (address, win_rate, total_trades, resolved_trades, wins, losses, pnl, last_updated)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT(address) DO UPDATE SET
               win_rate        = excluded.win_rate,
               total_trades    = excluded.total_trades,
               resolved_trades = excluded.resolved_trades,
               wins            = excluded.wins,
               losses          = excluded.losses,
               pnl             = excluded.pnl,
               last_updated    = excluded.last_updated""",
        (
            stats["address"],
            stats["win_rate"],
            stats["total_trades"],
            stats["resolved_trades"],
            stats["wins"],
            stats["losses"],
            stats["pnl"],
            stats["last_updated"],
        ),
    )
    conn.commit()
    conn.close()


def log_whale_trade(trade: dict):
    """Insert a whale trade (skip if transaction_hash already stored)."""
    tx_hash = trade.get("transactionHash")
    if not tx_hash:
        return
    conn = get_connection()
    conn.execute(
        _INSERT_WHALE_TRADE,
        (
            trade.get("proxyWallet", ""),
            trade.get("slug", ""),
            trade.get("conditionId", ""),
            trade.get("side", ""),
            trade.get("outcome", ""),
            float(trade.get("size") or 0),
            float(trade.get("price") or 0),
            trade.get("timestamp"),
            tx_hash,
        ),
    )
    conn.commit()
    conn.close()


# ── Read helpers ───────────────────────────────────────────────────────────────
def get_today_pnl() -> float:
    """Sum of P&L for trades closed today."""
    conn = get_connection()
    today = datetime.utcnow().date().isoformat()
    row = conn.execute(
        "SELECT COALESCE(SUM(pnl), 0) AS total FROM paper_trades WHERE closed_at LIKE %s",
        (f"{today}%",),
    ).fetchone()
    conn.close()
    return float(row["total"])


def get_stats() -> dict:
    """Aggregate stats for the analyze script."""
    conn = get_connection()
    stats = {}
    row = conn.execute("SELECT COUNT(*) AS n FROM paper_trades WHERE pnl IS NOT NULL").fetchone()
    stats["total_trades"] = row["n"]
    row = conn.execute("SELECT COUNT(*) AS n FROM paper_trades WHERE pnl > 0").fetchone()
    stats["wins"] = row["n"]
    row = conn.execute("SELECT COUNT(*) AS n FROM paper_trades WHERE pnl <= 0").fetchone()
    stats["losses"] = row["n"]
    row = conn.execute("SELECT COALESCE(SUM(pnl), 0) AS total FROM paper_trades").fetchone()
    stats["total_pnl"] = float(row["total"])
    row = conn.execute("SELECT COALESCE(SUM(fee), 0) AS total FROM paper_trades").fetchone()
    stats["total_fees"] = float(row["total"])
    row = conn.execute("SELECT COUNT(*) AS n FROM signals").fetchone()
    stats["total_signals"] = row["n"]
    row = conn.execute("SELECT COUNT(*) AS n FROM signals WHERE acted_on = 1").fetchone()
    stats["signals_traded"] = row["n"]
    conn.close()
    return stats


def get_top_whale_wallets(limit: int = 5) -> list:
    """Return top wallets ranked by win rate (requires at least 2 resolved trades)."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT address, win_rate, total_trades, resolved_trades, pnl
             FROM whale_wallets
            WHERE resolved_trades >= 2
            ORDER BY win_rate DESC, resolved_trades DESC
            LIMIT %s""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
