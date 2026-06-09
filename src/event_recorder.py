"""Durable CLOB websocket event recorder.

Opt-in via CLOB_EVENT_RECORDER_ENABLED=true. The recorder is intentionally
best-effort: it never blocks websocket parsing, batches inserts, and reports
dropped rows when the queue is full.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Optional

from . import db


def _truthy(value: str) -> bool:
    return str(value or "").lower() in {"1", "true", "yes", "on"}


class EventRecorder:
    def __init__(
        self,
        *,
        enabled: bool,
        event_types: Optional[set[str]] = None,
        queue_max: int = 20000,
        batch_size: int = 500,
        flush_interval_sec: float = 1.0,
    ):
        self.enabled = enabled
        self.event_types = event_types or {"book", "price_change", "last_trade_price", "trade"}
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, queue_max))
        self.batch_size = max(1, batch_size)
        self.flush_interval_sec = max(0.1, flush_interval_sec)
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.enqueued_total = 0
        self.inserted_total = 0
        self.dropped_total = 0
        self.flush_errors_total = 0
        self.last_insert_ts = 0.0
        self.last_error: Optional[str] = None

    @classmethod
    def from_env(cls) -> "EventRecorder":
        types_raw = os.getenv(
            "CLOB_EVENT_RECORDER_TYPES",
            "book,price_change,last_trade_price,trade",
        )
        event_types = {x.strip().lower() for x in types_raw.split(",") if x.strip()}
        return cls(
            enabled=_truthy(os.getenv("CLOB_EVENT_RECORDER_ENABLED", "false")),
            event_types=event_types,
            queue_max=int(os.getenv("CLOB_EVENT_RECORDER_QUEUE_MAX", "20000")),
            batch_size=int(os.getenv("CLOB_EVENT_RECORDER_BATCH_SIZE", "500")),
            flush_interval_sec=float(os.getenv("CLOB_EVENT_RECORDER_FLUSH_SEC", "1.0")),
        )

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        await asyncio.to_thread(_ensure_table)
        self._running = True
        self._task = asyncio.create_task(self._run(), name="clob-event-recorder")
        print("[event-recorder] enabled for CLOB websocket events", flush=True)

    async def stop(self) -> None:
        self._running = False
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    def record(self, evt: dict, *, received_ts: Optional[float] = None) -> None:
        if not self.enabled:
            return
        evt_type = (evt.get("event_type") or evt.get("type") or "").lower()
        if self.event_types and evt_type not in self.event_types:
            return
        rows = _rows_for_event(evt, received_ts=received_ts or time.time(), evt_type=evt_type)
        for row in rows:
            try:
                self.queue.put_nowait(row)
                self.enqueued_total += 1
            except asyncio.QueueFull:
                self.dropped_total += 1

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "queue_size": self.queue.qsize() if self.enabled else 0,
            "enqueued_total": self.enqueued_total,
            "inserted_total": self.inserted_total,
            "dropped_total": self.dropped_total,
            "flush_errors_total": self.flush_errors_total,
            "last_insert_ts": self.last_insert_ts,
            "last_error": self.last_error,
            "event_types": sorted(self.event_types),
        }

    async def _run(self) -> None:
        batch: list[dict] = []
        while self._running or not self.queue.empty():
            try:
                row = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval_sec)
                batch.append(row)
                while len(batch) < self.batch_size:
                    try:
                        batch.append(self.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if batch:
                await self._flush(batch)
                batch = []

        if batch:
            await self._flush(batch)

    async def _flush(self, batch: list[dict]) -> None:
        try:
            inserted = await asyncio.to_thread(_insert_rows, list(batch))
            self.inserted_total += inserted
            self.last_insert_ts = time.time()
            self.last_error = None
        except Exception as exc:
            self.flush_errors_total += 1
            self.last_error = f"{type(exc).__name__}: {exc}"[:200]
            print(f"[event-recorder] flush failed: {self.last_error}", flush=True)


def _event_ts(evt: dict) -> Optional[float]:
    raw = evt.get("timestamp") or evt.get("ts")
    try:
        ts = float(raw)
        return ts / 1000.0 if ts > 10_000_000_000 else ts
    except (TypeError, ValueError):
        return None


def _rows_for_event(evt: dict, *, received_ts: float, evt_type: str) -> list[dict]:
    market = evt.get("market") or evt.get("condition_id") or evt.get("conditionId")
    event_hash = evt.get("hash") or evt.get("transaction_hash")
    payload = json.dumps(evt, separators=(",", ":"), sort_keys=True)
    base = {
        "received_at": datetime.fromtimestamp(received_ts, tz=timezone.utc).isoformat(),
        "received_ts": received_ts,
        "event_ts": _event_ts(evt),
        "event_type": evt_type,
        "market": str(market) if market is not None else None,
        "event_hash": str(event_hash) if event_hash is not None else None,
        "payload": payload,
    }

    token_ids: list[str] = []
    if evt_type == "price_change":
        changes = evt.get("price_changes") or evt.get("changes") or []
        if isinstance(changes, list):
            for change in changes:
                if not isinstance(change, dict):
                    continue
                tok = change.get("asset_id") or change.get("token_id") or change.get("assetId")
                if tok is not None:
                    token_ids.append(str(tok))
    else:
        tok = evt.get("asset_id") or evt.get("token_id") or evt.get("assetId")
        if tok is not None:
            token_ids.append(str(tok))

    if not token_ids:
        return [{**base, "token_id": None}]
    seen = set()
    rows = []
    for tok in token_ids:
        if tok in seen:
            continue
        seen.add(tok)
        rows.append({**base, "token_id": tok})
    return rows


def _ensure_table() -> None:
    pk = "SERIAL PRIMARY KEY" if getattr(db, "_USE_PG", False) else "INTEGER PRIMARY KEY AUTOINCREMENT"
    float_type = "DOUBLE PRECISION" if getattr(db, "_USE_PG", False) else "REAL"
    conn = db.get_connection()
    try:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS clob_ws_events (
                id {pk},
                received_at TEXT NOT NULL,
                received_ts {float_type} NOT NULL,
                event_ts {float_type},
                event_type TEXT NOT NULL,
                market TEXT,
                token_id TEXT,
                event_hash TEXT,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_clob_events_token_ts ON clob_ws_events(token_id, received_ts);
            CREATE INDEX IF NOT EXISTS idx_clob_events_type_ts ON clob_ws_events(event_type, received_ts);
            CREATE INDEX IF NOT EXISTS idx_clob_events_market_ts ON clob_ws_events(market, received_ts);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _insert_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    _ensure_table()
    conn = db.get_connection()
    try:
        for row in rows:
            conn.execute(
                """INSERT INTO clob_ws_events
                   (received_at, received_ts, event_ts, event_type, market, token_id, event_hash, payload)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    row["received_at"],
                    row["received_ts"],
                    row["event_ts"],
                    row["event_type"],
                    row["market"],
                    row["token_id"],
                    row["event_hash"],
                    row["payload"],
                ),
            )
        conn.commit()
        return len(rows)
    finally:
        conn.close()
