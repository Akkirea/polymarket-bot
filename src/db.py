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
from typing import Optional
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

    def rollback(self):
        self._raw.rollback()

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
    print(f"[db] DB: {'PostgreSQL' if _USE_PG else 'SQLite'}")
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
            id                 {_PK},
            whale_address      TEXT NOT NULL,
            market_slug        TEXT NOT NULL,
            side               TEXT NOT NULL,
            size               REAL NOT NULL,
            entry_price        REAL,
            price_to_beat      REAL,
            poly_price_to_beat REAL,
            resolution_price   REAL,
            outcome            TEXT,
            pnl                REAL,
            balance_after      REAL,
            diff_at_entry      REAL,
            seconds_remaining  REAL,
            strategy           TEXT,
            opened_at          TEXT NOT NULL,
            closed_at          TEXT
        );

        CREATE TABLE IF NOT EXISTS bot_state (
            id         INTEGER PRIMARY KEY,
            balance    REAL NOT NULL DEFAULT 10000,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bot_open_position (
            id            INTEGER PRIMARY KEY,
            market_slug   TEXT NOT NULL,
            side          TEXT NOT NULL,
            size          REAL NOT NULL,
            entry_price   REAL NOT NULL,
            price_to_beat REAL,
            end_ts        REAL NOT NULL,
            opened_at     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bot_open_positions (
            market_slug       TEXT PRIMARY KEY,
            side              TEXT NOT NULL,
            size              REAL NOT NULL,
            entry_price       REAL NOT NULL,
            price_to_beat     REAL,
            end_ts            REAL NOT NULL,
            opened_at         TEXT NOT NULL,
            diff_at_entry     REAL,
            seconds_remaining REAL,
            strategy          TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_whale_wallets_winrate ON whale_wallets(win_rate DESC);
        CREATE INDEX IF NOT EXISTS idx_whale_trades_wallet   ON whale_trades(wallet_address);

        CREATE TABLE IF NOT EXISTS bot_signals (
            id          {_PK},
            ts          TEXT NOT NULL,
            slug        TEXT NOT NULL,
            direction   TEXT,
            diff        REAL,
            momentum    REAL,
            chop_range  REAL,
            filter_hit  TEXT NOT NULL,
            outcome     TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_bot_signals_ts   ON bot_signals(ts);
        CREATE INDEX IF NOT EXISTS idx_bot_signals_slug ON bot_signals(slug)
    """)
    conn.commit()

    # Migrate existing bot_trades tables that predate these columns.
    # PostgreSQL: ADD COLUMN IF NOT EXISTS is idempotent — no exception, no broken transaction.
    # SQLite:     IF NOT EXISTS in ALTER TABLE requires 3.37+; use try/except instead.
    for col, typ in [("price_to_beat", "REAL"), ("poly_price_to_beat", "REAL"), ("resolution_price", "REAL"), ("balance_after", "REAL"), ("diff_at_entry", "REAL"), ("seconds_remaining", "REAL"), ("strategy", "TEXT")]:
        if _USE_PG:
            conn.execute(f"ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS {col} {typ}")
            conn.commit()
        else:
            try:
                conn.execute(f"ALTER TABLE bot_trades ADD COLUMN {col} {typ}")
                conn.commit()
            except Exception:
                pass  # column already exists

    # Migrate bot_open_positions (new table — may have been created before these columns were added)
    for col, typ in [("diff_at_entry", "REAL"), ("seconds_remaining", "REAL"), ("strategy", "TEXT")]:
        if _USE_PG:
            conn.execute(f"ALTER TABLE bot_open_positions ADD COLUMN IF NOT EXISTS {col} {typ}")
            conn.commit()
        else:
            try:
                conn.execute(f"ALTER TABLE bot_open_positions ADD COLUMN {col} {typ}")
                conn.commit()
            except Exception:
                pass  # column already exists

    conn.close()


def log_bot_signal(
    slug: str,
    filter_hit: str,
    outcome: str,
    direction: Optional[str] = None,
    diff: Optional[float] = None,
    momentum: Optional[float] = None,
    chop_range: Optional[float] = None,
):
    """Record one evaluated signal — whether it was blocked by a filter or entered."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO bot_signals (ts, slug, direction, diff, momentum, chop_range, filter_hit, outcome)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            datetime.utcnow().isoformat(),
            slug,
            direction,
            round(diff, 4) if diff is not None else None,
            round(momentum, 4) if momentum is not None else None,
            round(chop_range, 4) if chop_range is not None else None,
            filter_hit,
            outcome,
        ),
    )
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


INITIAL_BALANCE = 10_000.0


def load_bot_state() -> dict:
    """Return balance derived from the trade ledger and open positions."""
    conn = get_connection()
    realized = conn.execute(
        """SELECT COALESCE(SUM(pnl), 0) AS total
             FROM bot_trades
            WHERE pnl IS NOT NULL
              AND COALESCE(outcome, '') != 'unresolved'"""
    ).fetchone()
    reserved = conn.execute(
        "SELECT COALESCE(SUM(size), 0) AS total FROM bot_open_positions"
    ).fetchone()
    conn.close()
    balance = INITIAL_BALANCE + float(realized["total"] or 0.0) - float(reserved["total"] or 0.0)
    return {"balance": balance}


def load_bot_performance() -> dict:
    """Return aggregate win/loss/P&L from resolved bot trades."""
    conn = get_connection()
    row = conn.execute(
        """SELECT
               COUNT(*) AS total_trades,
               COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
               COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), 0) AS losses,
               COALESCE(SUM(pnl), 0) AS total_pnl
           FROM bot_trades
           WHERE pnl IS NOT NULL
             AND COALESCE(outcome, '') != 'unresolved'"""
    ).fetchone()
    conn.close()
    return {
        "total_trades": int(row["total_trades"] or 0),
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "total_pnl": float(row["total_pnl"] or 0.0),
    }


def save_bot_state(balance: float):
    """Upsert the single bot_state row."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO bot_state (id, balance, updated_at)
               VALUES (1, %s, %s)
           ON CONFLICT(id) DO UPDATE SET
               balance    = excluded.balance,
               updated_at = excluded.updated_at"""
        if _USE_PG else
        """INSERT OR REPLACE INTO bot_state (id, balance, updated_at)
               VALUES (1, %s, %s)""",
        (balance, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def save_open_position(pos: dict):
    """Upsert a position row keyed by market_slug (supports up to 2 concurrent positions)."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO bot_open_positions
               (market_slug, side, size, entry_price, price_to_beat, end_ts, opened_at,
                diff_at_entry, seconds_remaining, strategy)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT(market_slug) DO UPDATE SET
               side              = excluded.side,
               size              = excluded.size,
               entry_price       = excluded.entry_price,
               price_to_beat     = excluded.price_to_beat,
               end_ts            = excluded.end_ts,
               opened_at         = excluded.opened_at,
               diff_at_entry     = excluded.diff_at_entry,
               seconds_remaining = excluded.seconds_remaining,
               strategy          = excluded.strategy"""
        if _USE_PG else
        """INSERT OR REPLACE INTO bot_open_positions
               (market_slug, side, size, entry_price, price_to_beat, end_ts, opened_at,
                diff_at_entry, seconds_remaining, strategy)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            pos["market_slug"],
            pos["side"],
            pos["size"],
            pos["entry_price"],
            pos.get("price_to_beat"),
            pos["end_ts"],
            pos["opened_at"],
            pos.get("diff_at_entry"),
            pos.get("seconds_remaining"),
            pos.get("strategy"),
        ),
    )
    conn.commit()
    conn.close()


def load_open_positions() -> list:
    """Return all persisted open positions (up to 2)."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT market_slug, side, size, entry_price, price_to_beat, end_ts, opened_at, "
        "diff_at_entry, seconds_remaining, strategy FROM bot_open_positions"
    ).fetchall()
    conn.close()
    positions = []
    for row in rows:
        market_slug = row["market_slug"]
        end_ts = float(row["end_ts"])
        try:
            slug_start_ts = float(str(market_slug).rsplit("-", 1)[-1])
            if abs(end_ts - slug_start_ts) < 1:
                end_ts += 300.0
        except (ValueError, IndexError):
            pass
        positions.append(
            {
                "market_slug":       market_slug,
                "side":              row["side"],
                "size":              float(row["size"]),
                "entry_price":       float(row["entry_price"]),
                "price_to_beat":     float(row["price_to_beat"]) if row["price_to_beat"] is not None else None,
                "end_ts":            end_ts,
                "opened_at":         row["opened_at"],
                "diff_at_entry":     float(row["diff_at_entry"]) if row["diff_at_entry"] is not None else None,
                "seconds_remaining": float(row["seconds_remaining"]) if row["seconds_remaining"] is not None else None,
                "strategy":          row["strategy"],
            }
        )
    return positions


def clear_open_position(market_slug: str):
    """Remove a specific open position by market_slug."""
    conn = get_connection()
    conn.execute("DELETE FROM bot_open_positions WHERE market_slug = %s", (market_slug,))
    conn.commit()
    conn.close()


def update_resolution_price(market_slug: str, resolution_price: float):
    """Backfill resolution_price on a closed trade where it was not available at close time."""
    conn = get_connection()
    conn.execute(
        "UPDATE bot_trades SET resolution_price = %s "
        "WHERE market_slug = %s AND resolution_price IS NULL",
        (round(resolution_price, 2), market_slug),
    )
    conn.commit()
    conn.close()


def load_unresolved_bot_trades(limit: int = 200) -> list:
    """Return unresolved placeholder trades that still need settlement."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT market_slug, side, size, entry_price, opened_at
             FROM bot_trades
            WHERE outcome = 'unresolved'
            ORDER BY opened_at DESC
            LIMIT %s""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def settle_unresolved_bot_trade(
    market_slug: str,
    winner: str,
    resolution_price: Optional[float] = None,
    poly_price_to_beat: Optional[float] = None,
) -> Optional[dict]:
    """Convert an unresolved placeholder row into a settled trade."""
    conn = get_connection()
    row = conn.execute(
        """SELECT side, size, entry_price
             FROM bot_trades
            WHERE market_slug = %s
              AND outcome = 'unresolved'
            ORDER BY opened_at DESC
            LIMIT 1""",
        (market_slug,),
    ).fetchone()
    if not row:
        conn.close()
        return None

    side = row["side"]
    size = float(row["size"] or 0.0)
    entry_price = float(row["entry_price"] or 0.5)
    won = side == winner
    pnl = size * (1.0 / entry_price - 1.0) if won else -size

    conn.execute(
        """UPDATE bot_trades
              SET outcome = %s,
                  pnl = %s,
                  resolution_price = COALESCE(%s, resolution_price),
                  poly_price_to_beat = COALESCE(%s, poly_price_to_beat)
            WHERE market_slug = %s
              AND outcome = 'unresolved'""",
        (
            winner,
            round(pnl, 2),
            round(resolution_price, 2) if resolution_price is not None else None,
            round(poly_price_to_beat, 2) if poly_price_to_beat is not None else None,
            market_slug,
        ),
    )
    conn.commit()
    conn.close()
    return {"won": won, "pnl": round(pnl, 2)}


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
