"""
SIGNAL/ZERO Phase 1 — Database Layer

Automatically selects the backend at startup:
  - DATABASE_URL set  → psycopg2 (PostgreSQL / Railway)
  - DATABASE_URL unset → sqlite3  (local development)

All SQL is written with %s placeholders. The _Connection wrapper
converts them to ? for SQLite so no query needs two versions.
"""

import os
import json
import sqlite3
from datetime import datetime
from typing import Optional
from . import config

# ── Backend detection ──────────────────────────────────────────────────────────
_DATABASE_URL = os.environ.get("DATABASE_URL")
_USE_PG       = bool(_DATABASE_URL)
_MAKER_SHADOW_SAMPLE_STRATEGY = "btc5-maker-shadow-sample-rtds-nearmiss"

if _USE_PG:
    import psycopg2
    import psycopg2.extras

# Primary-key column definition differs between the two engines
_PK = "SERIAL PRIMARY KEY" if _USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
_FLOAT = "DOUBLE PRECISION" if _USE_PG else "REAL"

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
            closed_at          TEXT,
            mode               TEXT NOT NULL DEFAULT 'paper'
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
            strategy          TEXT,
            live_order_id     TEXT,
            live_stake        REAL,
            live_fill_price   REAL
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
        CREATE INDEX IF NOT EXISTS idx_bot_signals_slug ON bot_signals(slug);

        CREATE TABLE IF NOT EXISTS live_order_attempts (
            id                 {_PK},
            market_slug        TEXT NOT NULL,
            side               TEXT NOT NULL,
            intended_stake     REAL,
            paper_entry_price  REAL,
            max_fill_price     REAL,
            reason             TEXT,
            status             TEXT NOT NULL DEFAULT 'failed',
            attempted_at       TEXT NOT NULL,
            seconds_remaining  REAL,
            strategy           TEXT,
            diff_at_entry      REAL,
            price_to_beat      REAL,
            orderbook          TEXT,
            outcome            TEXT,
            resolution_price   REAL,
            poly_price_to_beat REAL,
            would_have_won     INTEGER,
            hypothetical_pnl   REAL,
            settled_at         TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_live_attempts_slug ON live_order_attempts(market_slug);
        CREATE INDEX IF NOT EXISTS idx_live_attempts_status ON live_order_attempts(status);
        CREATE INDEX IF NOT EXISTS idx_live_attempts_attempted ON live_order_attempts(attempted_at);

        CREATE TABLE IF NOT EXISTS rtds_reference_samples (
            id                   {_PK},
            market_slug          TEXT NOT NULL,
            market_start_ts      {_FLOAT} NOT NULL,
            sample_ts            {_FLOAT} NOT NULL,
            offset_sec           {_FLOAT} NOT NULL,
            price                {_FLOAT} NOT NULL,
            symbol               TEXT,
            official_price_to_beat {_FLOAT},
            error                {_FLOAT},
            created_at           TEXT NOT NULL,
            resolved_at          TEXT,
            UNIQUE(market_slug, sample_ts, symbol)
        );

        CREATE INDEX IF NOT EXISTS idx_rtds_samples_slug ON rtds_reference_samples(market_slug);
        CREATE INDEX IF NOT EXISTS idx_rtds_samples_error ON rtds_reference_samples(error);

        CREATE TABLE IF NOT EXISTS pre_signal_orders (
            market_slug       TEXT PRIMARY KEY,
            order_id          TEXT NOT NULL,
            side              TEXT NOT NULL,
            stake             REAL NOT NULL,
            limit_price       REAL NOT NULL,
            placed_at         REAL NOT NULL,
            price_to_beat     REAL,
            reference_source  TEXT,
            diff_at_placement REAL,
            crowd_at_placement REAL,
            end_ts            REAL,
            hour_et           INTEGER,
            created_at        TEXT NOT NULL
        )
    """)
    conn.commit()

    # Migrate existing bot_trades tables that predate these columns.
    # PostgreSQL: ADD COLUMN IF NOT EXISTS is idempotent — no exception, no broken transaction.
    # SQLite:     IF NOT EXISTS in ALTER TABLE requires 3.37+; use try/except instead.
    for col, typ in [
        ("price_to_beat", "REAL"), ("poly_price_to_beat", "REAL"), ("resolution_price", "REAL"),
        ("balance_after", "REAL"), ("diff_at_entry", "REAL"), ("seconds_remaining", "REAL"),
        ("strategy", "TEXT"), ("mode", "TEXT NOT NULL DEFAULT 'paper'"),
    ]:
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
    for col, typ in [
        ("diff_at_entry", "REAL"), ("seconds_remaining", "REAL"), ("strategy", "TEXT"),
        ("live_order_id", "TEXT"), ("live_stake", "REAL"), ("live_fill_price", "REAL"),
    ]:
        if _USE_PG:
            conn.execute(f"ALTER TABLE bot_open_positions ADD COLUMN IF NOT EXISTS {col} {typ}")
            conn.commit()
        else:
            try:
                conn.execute(f"ALTER TABLE bot_open_positions ADD COLUMN {col} {typ}")
                conn.commit()
            except Exception:
                pass  # column already exists

    if _USE_PG:
        for col in [
            "market_start_ts",
            "sample_ts",
            "offset_sec",
            "price",
            "official_price_to_beat",
            "error",
        ]:
            conn.execute(
                f"ALTER TABLE rtds_reference_samples "
                f"ALTER COLUMN {col} TYPE DOUBLE PRECISION USING {col}::double precision"
            )
        conn.commit()

    conn.close()


def log_rtds_reference_samples(market_slug: str, market_start_ts: float, samples: list[tuple[float, float, str]]) -> int:
    """Persist RTDS ticks around a market open for later priceToBeat comparison."""
    if not samples:
        return 0
    conn = get_connection()
    inserted = 0
    sql = (
        """INSERT INTO rtds_reference_samples
              (market_slug, market_start_ts, sample_ts, offset_sec, price, symbol, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (market_slug, sample_ts, symbol) DO NOTHING"""
        if _USE_PG else
        """INSERT OR IGNORE INTO rtds_reference_samples
              (market_slug, market_start_ts, sample_ts, offset_sec, price, symbol, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    )
    now = datetime.utcnow().isoformat()
    for sample_ts, price, symbol in samples:
        cur = conn.execute(
            sql,
            (
                market_slug,
                float(market_start_ts),
                float(sample_ts),
                round(float(sample_ts) - float(market_start_ts), 3),
                round(float(price), 8),
                symbol,
                now,
            ),
        )
        if getattr(cur, "rowcount", 0) and cur.rowcount > 0:
            inserted += 1
    conn.commit()
    conn.close()
    return inserted


def settle_rtds_reference_samples(market_slug: str, official_price_to_beat: Optional[float]) -> int:
    """Attach official priceToBeat to RTDS samples and calculate sample error."""
    if official_price_to_beat is None:
        return 0
    conn = get_connection()
    cur = conn.execute(
        """UPDATE rtds_reference_samples
              SET official_price_to_beat = %s,
                  error = price - %s,
                  resolved_at = %s
            WHERE market_slug = %s
              AND official_price_to_beat IS NULL""",
        (
            round(float(official_price_to_beat), 8),
            round(float(official_price_to_beat), 8),
            datetime.utcnow().isoformat(),
            market_slug,
        ),
    )
    count = getattr(cur, "rowcount", 0) or 0
    conn.commit()
    conn.close()
    return int(count)


def get_rtds_reference_comparisons(limit: int = 200, market_slug: Optional[str] = None) -> list:
    """Return RTDS samples with official priceToBeat/error, closest-to-open first per slug."""
    limit = max(1, min(int(limit), 1000))
    conn = get_connection()
    where = ""
    params: list = []
    if market_slug:
        where = "WHERE market_slug = %s"
        params.append(market_slug)
    rows = conn.execute(
        f"""SELECT *
              FROM rtds_reference_samples
              {where}
             ORDER BY market_start_ts DESC, ABS(offset_sec) ASC, sample_ts ASC
             LIMIT %s""",
        tuple(params + [limit]),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def log_live_order_attempt(attempt: dict):
    """Record a failed live execution attempt for later resolution analysis."""
    orderbook = attempt.get("orderbook")
    conn = get_connection()
    conn.execute(
        """INSERT INTO live_order_attempts
              (market_slug, side, intended_stake, paper_entry_price, max_fill_price,
               reason, status, attempted_at, seconds_remaining, strategy,
               diff_at_entry, price_to_beat, orderbook, outcome, would_have_won,
               hypothetical_pnl)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'unresolved', NULL, NULL)""",
        (
            attempt["market_slug"],
            attempt["side"],
            round(float(attempt.get("intended_stake") or 0.0), 4),
            round(float(attempt.get("paper_entry_price") or 0.0), 4),
            round(float(attempt.get("max_fill_price") or 0.0), 4),
            str(attempt.get("reason") or "")[:1000],
            attempt.get("status", "failed"),
            attempt.get("attempted_at") or datetime.utcnow().isoformat(),
            round(float(attempt["seconds_remaining"]), 1) if attempt.get("seconds_remaining") is not None else None,
            attempt.get("strategy"),
            round(float(attempt["diff_at_entry"]), 2) if attempt.get("diff_at_entry") is not None else None,
            round(float(attempt["price_to_beat"]), 2) if attempt.get("price_to_beat") is not None else None,
            json.dumps(orderbook, sort_keys=True) if orderbook is not None else None,
        ),
    )
    conn.commit()
    conn.close()


def load_unresolved_live_order_attempts(limit: int = 500) -> list:
    """Return unresolved failed live attempts that still need winner annotation."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT market_slug, side, paper_entry_price
             FROM live_order_attempts
            WHERE outcome = 'unresolved'
              AND COALESCE(strategy, '') != %s
            ORDER BY attempted_at DESC
            LIMIT %s""",
        (_MAKER_SHADOW_SAMPLE_STRATEGY, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def settle_live_order_attempts(
    market_slug: str,
    winner: str,
    resolution_price: Optional[float] = None,
    poly_price_to_beat: Optional[float] = None,
) -> int:
    """Annotate failed live attempts with whether the direction would have won."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, side, intended_stake, paper_entry_price
             FROM live_order_attempts
            WHERE market_slug = %s
              AND outcome = 'unresolved'
              AND COALESCE(strategy, '') != %s""",
        (market_slug, _MAKER_SHADOW_SAMPLE_STRATEGY),
    ).fetchall()
    updated = 0
    for row in rows:
        side = row["side"]
        stake = float(row["intended_stake"] or 0.0)
        entry_price = float(row["paper_entry_price"] or 0.5)
        if entry_price <= 0:
            entry_price = 0.5
        won = side == winner
        hypothetical_pnl = stake * (1.0 / entry_price - 1.0) if won else -stake
        conn.execute(
            """UPDATE live_order_attempts
                  SET outcome = %s,
                      resolution_price = COALESCE(%s, resolution_price),
                      poly_price_to_beat = COALESCE(%s, poly_price_to_beat),
                      would_have_won = %s,
                      hypothetical_pnl = %s,
                      settled_at = %s
                WHERE id = %s""",
            (
                winner,
                round(resolution_price, 2) if resolution_price is not None else None,
                round(poly_price_to_beat, 2) if poly_price_to_beat is not None else None,
                1 if won else 0,
                round(hypothetical_pnl, 4),
                datetime.utcnow().isoformat(),
                row["id"],
            ),
        )
        updated += 1
    conn.commit()
    conn.close()
    return updated


def get_live_order_attempts(limit: int = 100) -> list:
    """Return recent failed live opportunities, collapsed to the latest attempt per market/side/strategy."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT loa.*
             FROM live_order_attempts loa
             JOIN (
                   SELECT market_slug, side, COALESCE(strategy, '') AS strategy_key, MAX(attempted_at) AS latest_at
                     FROM live_order_attempts
                    WHERE COALESCE(strategy, '') != %s
                    GROUP BY market_slug, side, COALESCE(strategy, '')
                  ) latest
               ON latest.market_slug = loa.market_slug
              AND latest.side = loa.side
              AND latest.strategy_key = COALESCE(loa.strategy, '')
              AND latest.latest_at = loa.attempted_at
            ORDER BY loa.attempted_at DESC
            LIMIT %s""",
        (_MAKER_SHADOW_SAMPLE_STRATEGY, limit),
    ).fetchall()
    conn.close()
    attempts = []
    for row in rows:
        item = dict(row)
        if item.get("orderbook"):
            try:
                item["orderbook"] = json.loads(item["orderbook"])
            except Exception:
                pass
        attempts.append(item)
    return attempts


def get_live_order_attempt_summary() -> dict:
    """Aggregate missed live opportunities by latest attempt per market/side/strategy."""
    conn = get_connection()
    deduped_sql = """
        WITH latest AS (
            SELECT market_slug, side, COALESCE(strategy, '') AS strategy_key, MAX(attempted_at) AS latest_at
              FROM live_order_attempts
             WHERE COALESCE(strategy, '') != '{_MAKER_SHADOW_SAMPLE_STRATEGY}'
             GROUP BY market_slug, side, COALESCE(strategy, '')
        )
        SELECT loa.*
          FROM live_order_attempts loa
          JOIN latest
            ON latest.market_slug = loa.market_slug
           AND latest.side = loa.side
           AND latest.strategy_key = COALESCE(loa.strategy, '')
           AND latest.latest_at = loa.attempted_at
    """
    total = conn.execute(f"SELECT COUNT(*) AS n FROM ({deduped_sql}) deduped").fetchone()
    resolved = conn.execute(
        f"""SELECT
               COUNT(*) AS n,
               COALESCE(SUM(CASE WHEN would_have_won = 1 THEN 1 ELSE 0 END), 0) AS wins,
               COALESCE(SUM(CASE WHEN would_have_won = 0 THEN 1 ELSE 0 END), 0) AS losses,
               COALESCE(SUM(hypothetical_pnl), 0) AS hypothetical_pnl
             FROM ({deduped_sql}) deduped
            WHERE outcome IS NOT NULL
              AND outcome != 'unresolved'"""
    ).fetchone()
    by_reason = conn.execute(
        f"""SELECT reason, COUNT(*) AS count
             FROM ({deduped_sql}) deduped
            GROUP BY reason
            ORDER BY count DESC
            LIMIT 10"""
    ).fetchall()
    conn.close()
    resolved_n = int(resolved["n"] or 0)
    wins = int(resolved["wins"] or 0)
    return {
        "total_attempts": int(total["n"] or 0),
        "resolved_attempts": resolved_n,
        "would_have_won": wins,
        "would_have_lost": int(resolved["losses"] or 0),
        "would_have_win_rate": round(wins / resolved_n, 4) if resolved_n else 0.0,
        "hypothetical_pnl": round(float(resolved["hypothetical_pnl"] or 0.0), 4),
        "by_reason": [dict(r) for r in by_reason],
    }


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


INITIAL_BALANCE = float(os.getenv("PAPER_INITIAL_BALANCE", "97.0"))


def load_bot_state() -> dict:
    """Return paper balance derived from the paper trade ledger and open positions."""
    conn = get_connection()
    realized = conn.execute(
        """SELECT COALESCE(SUM(pnl), 0) AS total
             FROM bot_trades
            WHERE pnl IS NOT NULL
              AND COALESCE(outcome, '') != 'unresolved'
              AND COALESCE(mode, 'paper') = 'paper'"""
    ).fetchone()
    reserved = conn.execute(
        "SELECT COALESCE(SUM(size), 0) AS total FROM bot_open_positions"
    ).fetchone()
    conn.close()
    balance = INITIAL_BALANCE + float(realized["total"] or 0.0) - float(reserved["total"] or 0.0)
    return {"balance": balance}


def load_live_state(initial_balance: float) -> dict:
    """Return live balance derived from settled live trades and open live stakes."""
    conn = get_connection()
    realized = conn.execute(
        """SELECT COALESCE(SUM(pnl), 0) AS total
             FROM bot_trades
            WHERE pnl IS NOT NULL
              AND COALESCE(outcome, '') != 'unresolved'
              AND mode = 'live'"""
    ).fetchone()
    reserved = conn.execute(
        "SELECT COALESCE(SUM(live_stake), 0) AS total FROM bot_open_positions WHERE live_order_id IS NOT NULL"
    ).fetchone()
    conn.close()
    balance = initial_balance + float(realized["total"] or 0.0) - float(reserved["total"] or 0.0)
    return {"balance": balance}


def today_live_pnl(now=None) -> float:
    """Sum of realized LIVE-mode PnL for trades closed since UTC midnight today.

    Used by the daily-loss circuit breaker. Closed_at values are ISO-8601 UTC
    strings; string `>=` comparison against a midnight-prefix is sufficient as
    long as the format is stable, which load_live_state / save_open_position
    paths guarantee (datetime.now(timezone.utc).isoformat()).
    """
    from datetime import datetime, timezone
    if now is None:
        now = datetime.now(timezone.utc)
    midnight_iso = now.strftime("%Y-%m-%dT00:00:00")
    conn = get_connection()
    row = conn.execute(
        """SELECT COALESCE(SUM(pnl), 0) AS total
             FROM bot_trades
            WHERE pnl IS NOT NULL
              AND mode = 'live'
              AND COALESCE(outcome, '') != 'unresolved'
              AND closed_at >= %s""",
        (midnight_iso,),
    ).fetchone()
    conn.close()
    return float(row["total"] or 0.0)


def load_live_performance() -> dict:
    """Return aggregate win/loss/P&L from resolved live trades."""
    conn = get_connection()
    row = conn.execute(
        """SELECT
               COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
               COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), 0) AS losses,
               COALESCE(SUM(pnl), 0) AS total_pnl
           FROM bot_trades
           WHERE pnl IS NOT NULL
             AND COALESCE(outcome, '') != 'unresolved'
             AND mode = 'live'"""
    ).fetchone()
    conn.close()
    return {
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "total_pnl": float(row["total_pnl"] or 0.0),
    }


def load_bot_performance() -> dict:
    """Return aggregate win/loss/P&L from resolved paper trades."""
    conn = get_connection()
    row = conn.execute(
        """SELECT
               COUNT(*) AS total_trades,
               COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
               COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END), 0) AS losses,
               COALESCE(SUM(pnl), 0) AS total_pnl
           FROM bot_trades
           WHERE pnl IS NOT NULL
             AND COALESCE(outcome, '') != 'unresolved'
             AND COALESCE(mode, 'paper') = 'paper'"""
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
    """Upsert a position row keyed by market_slug (supports up to 3 concurrent positions)."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO bot_open_positions
               (market_slug, side, size, entry_price, price_to_beat, end_ts, opened_at,
                diff_at_entry, seconds_remaining, strategy,
                live_order_id, live_stake, live_fill_price)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT(market_slug) DO UPDATE SET
               side              = excluded.side,
               size              = excluded.size,
               entry_price       = excluded.entry_price,
               price_to_beat     = excluded.price_to_beat,
               end_ts            = excluded.end_ts,
               opened_at         = excluded.opened_at,
               diff_at_entry     = excluded.diff_at_entry,
               seconds_remaining = excluded.seconds_remaining,
               strategy          = excluded.strategy,
               live_order_id     = excluded.live_order_id,
               live_stake        = excluded.live_stake,
               live_fill_price   = excluded.live_fill_price"""
        if _USE_PG else
        """INSERT OR REPLACE INTO bot_open_positions
               (market_slug, side, size, entry_price, price_to_beat, end_ts, opened_at,
                diff_at_entry, seconds_remaining, strategy,
                live_order_id, live_stake, live_fill_price)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
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
            pos.get("live_order_id"),
            pos.get("live_stake"),
            pos.get("live_fill_price"),
        ),
    )
    conn.commit()
    conn.close()


def load_open_positions() -> list:
    """Return all persisted open positions."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT market_slug, side, size, entry_price, price_to_beat, end_ts, opened_at, "
        "diff_at_entry, seconds_remaining, strategy, "
        "live_order_id, live_stake, live_fill_price FROM bot_open_positions"
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
                "live_order_id":     row["live_order_id"],
                "live_stake":        float(row["live_stake"]) if row["live_stake"] is not None else None,
                "live_fill_price":   float(row["live_fill_price"]) if row["live_fill_price"] is not None else None,
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
              AND COALESCE(mode, 'paper') IN ('paper', 'live')
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
              AND COALESCE(mode, 'paper') IN ('paper', 'live')
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
              AND outcome = 'unresolved'
              AND COALESCE(mode, 'paper') IN ('paper', 'live')""",
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


def shadow_trade_exists(market_slug: str, strategy: str = "shadow") -> bool:
    """Return True if a shadow entry was already recorded for this slug."""
    conn = get_connection()
    row = conn.execute(
        """SELECT 1
             FROM bot_trades
            WHERE market_slug = %s
              AND mode = 'shadow'
              AND strategy = %s
            LIMIT 1""",
        (market_slug, strategy),
    ).fetchone()
    conn.close()
    return row is not None


def log_shadow_trade(trade: dict) -> bool:
    """Insert a hypothetical trade that never affects paper/live balance."""
    strategy = trade.get("strategy", "shadow")
    if shadow_trade_exists(trade["market_slug"], strategy):
        return False

    conn = get_connection()
    conn.execute(
        """INSERT INTO bot_trades
              (whale_address, market_slug, side, size, entry_price,
               price_to_beat, outcome, pnl, balance_after, diff_at_entry,
               seconds_remaining, strategy, opened_at, closed_at, mode)
           VALUES (%s, %s, %s, %s, %s, %s, 'unresolved', NULL, NULL, %s, %s, %s, %s, NULL, 'shadow')""",
        (
            trade.get("whale_address", "SHADOW_STRATEGY"),
            trade["market_slug"],
            trade["side"],
            round(float(trade.get("size") or 0.0), 4),
            round(float(trade.get("entry_price") or 0.5), 4),
            round(float(trade["price_to_beat"]), 2) if trade.get("price_to_beat") is not None else None,
            round(float(trade["diff_at_entry"]), 2) if trade.get("diff_at_entry") is not None else None,
            round(float(trade["seconds_remaining"]), 1) if trade.get("seconds_remaining") is not None else None,
            strategy,
            trade.get("opened_at") or datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    return True


def load_unresolved_shadow_trades(limit: int = 200) -> list:
    """Return unresolved shadow trades only."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT market_slug, side, size, entry_price, opened_at
             FROM bot_trades
            WHERE outcome = 'unresolved'
              AND mode = 'shadow'
            ORDER BY opened_at DESC
            LIMIT %s""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def settle_unresolved_shadow_trade(
    market_slug: str,
    winner: str,
    resolution_price: Optional[float] = None,
    poly_price_to_beat: Optional[float] = None,
) -> Optional[dict]:
    """Settle all unresolved shadow rows for a slug without touching paper/live accounting."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, side, size, entry_price
             FROM bot_trades
            WHERE market_slug = %s
              AND outcome = 'unresolved'
              AND mode = 'shadow'
            ORDER BY opened_at DESC""",
        (market_slug,),
    ).fetchall()
    if not rows:
        conn.close()
        return None

    total_pnl = 0.0
    won_count = 0
    closed_at = datetime.utcnow().isoformat()
    for row in rows:
        side = row["side"]
        size = float(row["size"] or 0.0)
        entry_price = float(row["entry_price"] or 0.5)
        won = side == winner
        pnl = size * (1.0 / entry_price - 1.0) if won else -size
        total_pnl += pnl
        if won:
            won_count += 1
        conn.execute(
            """UPDATE bot_trades
                  SET outcome = %s,
                      pnl = %s,
                      resolution_price = COALESCE(%s, resolution_price),
                      poly_price_to_beat = COALESCE(%s, poly_price_to_beat),
                      closed_at = %s
                WHERE id = %s
                  AND mode = 'shadow'""",
            (
                winner,
                round(pnl, 2),
                round(resolution_price, 2) if resolution_price is not None else None,
                round(poly_price_to_beat, 2) if poly_price_to_beat is not None else None,
                closed_at,
                row["id"],
            ),
        )
    conn.commit()
    conn.close()
    return {"count": len(rows), "won": won_count, "pnl": round(total_pnl, 2)}


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


# ── Pre-signal order persistence ───────────────────────────────────────────────

def save_pre_signal_order(order_info: dict) -> None:
    """Persist a resting GTC pre-signal order so it survives bot restarts."""
    from datetime import timezone
    conn = get_connection()
    conn.execute(
        """INSERT INTO pre_signal_orders
               (market_slug, order_id, side, stake, limit_price, placed_at,
                price_to_beat, reference_source, diff_at_placement,
                crowd_at_placement, end_ts, hour_et, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (market_slug) DO UPDATE SET
               order_id          = EXCLUDED.order_id,
               side              = EXCLUDED.side,
               stake             = EXCLUDED.stake,
               limit_price       = EXCLUDED.limit_price,
               placed_at         = EXCLUDED.placed_at,
               price_to_beat     = EXCLUDED.price_to_beat,
               reference_source  = EXCLUDED.reference_source,
               diff_at_placement = EXCLUDED.diff_at_placement,
               crowd_at_placement = EXCLUDED.crowd_at_placement,
               end_ts            = EXCLUDED.end_ts,
               hour_et           = EXCLUDED.hour_et,
               created_at        = EXCLUDED.created_at"""
        if _USE_PG else
        """INSERT OR REPLACE INTO pre_signal_orders
               (market_slug, order_id, side, stake, limit_price, placed_at,
                price_to_beat, reference_source, diff_at_placement,
                crowd_at_placement, end_ts, hour_et, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            order_info["market_slug"],
            order_info["order_id"],
            order_info["side"],
            order_info["stake"],
            order_info["limit_price"],
            order_info["placed_at"],
            order_info.get("price_to_beat"),
            order_info.get("reference_source"),
            order_info.get("diff_at_placement"),
            order_info.get("crowd_at_placement"),
            order_info.get("end_ts"),
            order_info.get("hour_et"),
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def delete_pre_signal_order(market_slug: str) -> None:
    conn = get_connection()
    conn.execute("DELETE FROM pre_signal_orders WHERE market_slug = %s", (market_slug,))
    conn.commit()
    conn.close()


def load_pre_signal_orders() -> list[dict]:
    """Load all resting pre-signal orders from DB (called on bot startup)."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM pre_signal_orders ORDER BY placed_at ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
