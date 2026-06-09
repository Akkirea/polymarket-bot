#!/usr/bin/env python3
"""Replay recorded CLOB websocket events against maker-shadow attempts.

This is an audit tool, not a trading component. It reads clob_ws_events and
maker_shadow_attempts, reconstructs top-of-book from raw payloads, and compares
a simple resting BUY fill replay against the shadow simulator's recorded result.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import db  # noqa: E402
from src.event_recorder import _ensure_table  # noqa: E402


EPS = 1e-9
ELIGIBILITY_DELAY_SEC = 0.5


def _row_dict(row) -> dict:
    return dict(row)


def _parse_ts(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    try:
        return float(text)
    except ValueError:
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _best_level(levels):
    if not isinstance(levels, list) or not levels:
        return None, None
    try:
        level = levels[0]
        if isinstance(level, dict):
            return float(level.get("price")), float(level.get("size"))
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return float(level[0]), float(level[1])
    except (TypeError, ValueError):
        return None, None
    return None, None


def _trade_aggressor(evt: dict) -> Optional[str]:
    raw_side = (
        evt.get("side")
        or evt.get("taker_side")
        or evt.get("taker_order_side")
        or evt.get("aggressor_side")
        or evt.get("maker_side")
    )
    if not raw_side:
        return None
    side = str(raw_side).upper().strip()
    if side in {"BUY", "B", "BID"}:
        agg = "BUY"
    elif side in {"SELL", "S", "ASK"}:
        agg = "SELL"
    else:
        return None
    if evt.get("maker_side") and not (
        evt.get("side") or evt.get("taker_side") or evt.get("taker_order_side") or evt.get("aggressor_side")
    ):
        return "SELL" if agg == "BUY" else "BUY"
    return agg


def _load_attempts(limit: int, since_hours: Optional[float]) -> list[dict]:
    where = []
    params: list = []
    if since_hours is not None:
        cutoff = datetime.fromtimestamp(time.time() - since_hours * 3600, tz=timezone.utc).isoformat()
        where.append("dispatched_at >= %s")
        params.append(cutoff)
    sql = "SELECT * FROM maker_shadow_attempts"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY dispatched_at DESC LIMIT %s"
    params.append(limit)
    conn = db.get_connection()
    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
    finally:
        conn.close()
    return [_row_dict(r) for r in rows]


def _load_events(token_id: str, start_ts: float, end_ts: float) -> list[dict]:
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT received_ts, event_type, payload
                 FROM clob_ws_events
                WHERE token_id = %s
                  AND received_ts >= %s
                  AND received_ts <= %s
                ORDER BY received_ts ASC, id ASC""",
            (token_id, start_ts, end_ts),
        ).fetchall()
    finally:
        conn.close()
    out = []
    for row in rows:
        item = _row_dict(row)
        try:
            item["payload"] = json.loads(item["payload"])
        except Exception:
            continue
        out.append(item)
    return out


def _price_change_schema_stats(limit: int) -> dict:
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT received_ts, event_hash, payload
                 FROM clob_ws_events
                WHERE event_type = 'price_change'
                ORDER BY received_ts DESC
                LIMIT %s""",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    seen = set()
    changes_total = 0
    with_best = 0
    top_matches = 0
    key_shapes: dict[str, int] = {}
    for row in rows:
        item = _row_dict(row)
        key = (item.get("received_ts"), item.get("event_hash"), item.get("payload"))
        if key in seen:
            continue
        seen.add(key)
        try:
            evt = json.loads(item["payload"])
        except Exception:
            continue
        changes = evt.get("price_changes") or evt.get("changes") or []
        if not isinstance(changes, list):
            continue
        for change in changes:
            if not isinstance(change, dict):
                continue
            changes_total += 1
            keys = ",".join(sorted(change.keys()))
            key_shapes[keys] = key_shapes.get(keys, 0) + 1
            has = "best_bid" in change and "best_ask" in change
            with_best += int(has)
            price = str(change.get("price"))
            top_matches += int(has and (price == str(change.get("best_bid")) or price == str(change.get("best_ask"))))
    return {
        "sampled_unique_price_change_events": len(seen),
        "changes_total": changes_total,
        "with_best_bid_ask_pct": round(100.0 * with_best / changes_total, 2) if changes_total else None,
        "price_eq_best_bid_or_ask_pct": round(100.0 * top_matches / changes_total, 2) if changes_total else None,
        "key_shapes": key_shapes,
    }


def _apply_book(evt: dict) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    bid_price, bid_size = _best_level(evt.get("bids"))
    ask_price, ask_size = _best_level(evt.get("asks"))
    return bid_price, bid_size, ask_price, ask_size


def _apply_price_change(evt: dict, token_id: str, top: dict, *, top_filter: bool) -> None:
    changes = evt.get("price_changes") or evt.get("changes") or []
    if not isinstance(changes, list):
        return
    for change in changes:
        if not isinstance(change, dict):
            continue
        tok = change.get("asset_id") or change.get("token_id") or change.get("assetId")
        if str(tok) != str(token_id):
            continue
        try:
            price = float(change.get("price")) if change.get("price") is not None else None
            size = float(change.get("size")) if change.get("size") is not None else None
        except (TypeError, ValueError):
            continue

        best_bid = change.get("best_bid")
        best_ask = change.get("best_ask")
        try:
            best_bid_f = float(best_bid) if best_bid is not None else None
            best_ask_f = float(best_ask) if best_ask is not None else None
        except (TypeError, ValueError):
            best_bid_f = None
            best_ask_f = None

        # Preferred reconstruction: if the payload publishes best_bid/best_ask,
        # use those as top-of-book and avoid treating every level update as top.
        if best_bid_f is not None:
            top["bid_price"] = best_bid_f
        if best_ask_f is not None:
            top["ask_price"] = best_ask_f

        if top_filter and price is not None:
            is_top = (
                (best_bid_f is not None and math.isclose(price, best_bid_f, abs_tol=EPS))
                or (best_ask_f is not None and math.isclose(price, best_ask_f, abs_tol=EPS))
            )
            if not is_top:
                continue

        side = str(change.get("side") or "").lower()
        if side in {"buy", "bid"}:
            top["bid_price"] = price
            top["bid_size"] = size
        elif side in {"sell", "ask"}:
            top["ask_price"] = price
            top["ask_size"] = size


def _replay_attempt(attempt: dict, *, window_sec: float, top_filter: bool) -> dict:
    token_id = str(attempt.get("token_id") or "")
    placed_ts = _parse_ts(attempt.get("dispatched_at"))
    limit_price = float(attempt.get("intended_initial_price") or 0.0)
    stake = float(attempt.get("intended_size_usdc") or 0.0)
    if not token_id or placed_ts is None or limit_price <= 0 or stake <= 0:
        return {"ok": False, "reason": "missing_attempt_fields"}

    recorded_resting = float(attempt.get("time_resting_sec") or 0.0)
    end_ts = placed_ts + (recorded_resting if recorded_resting > 0 else window_sec)
    events = _load_events(token_id, placed_ts - 10.0, end_ts)
    top = {"bid_price": None, "bid_size": None, "ask_price": None, "ask_size": None}
    queue_ahead = 0.0
    queue_remaining = 0.0
    queue_snapshotted = False
    filled = False
    fill_ts = None
    fill_usdc = 0.0
    direction_rejections = 0

    for event in events:
        evt = event["payload"]
        evt_type = str(event["event_type"] or "").lower()
        ts = float(event["received_ts"])
        if evt_type in {"book", "order_book"}:
            bid_price, bid_size, ask_price, ask_size = _apply_book(evt)
            top.update(
                {
                    "bid_price": bid_price,
                    "bid_size": bid_size,
                    "ask_price": ask_price,
                    "ask_size": ask_size,
                }
            )
        elif evt_type == "price_change":
            _apply_price_change(evt, token_id, top, top_filter=top_filter)

        if not queue_snapshotted and ts >= placed_ts:
            bid_price = top.get("bid_price")
            bid_size = top.get("bid_size")
            if bid_price is not None and bid_size is not None:
                if limit_price + EPS < bid_price or math.isclose(limit_price, bid_price, abs_tol=EPS):
                    queue_ahead = float(bid_size)
            queue_remaining = queue_ahead
            queue_snapshotted = True

        if evt_type not in {"trade", "last_trade_price", "matched", "trades"}:
            continue
        trade_ts = _parse_ts(evt.get("timestamp") or evt.get("ts")) or ts
        if trade_ts < placed_ts + ELIGIBILITY_DELAY_SEC:
            continue
        try:
            price = float(evt.get("price") or evt.get("matched_price") or 0)
            shares = float(evt.get("size") or evt.get("matched_size") or evt.get("size_matched") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0 or shares <= 0 or price > limit_price + EPS:
            continue
        agg = _trade_aggressor(evt)
        if agg == "BUY":
            direction_rejections += 1
            continue
        if queue_remaining > 0:
            consumed = min(shares, queue_remaining)
            queue_remaining -= consumed
            shares -= consumed
            if shares <= 0:
                continue
        max_shares = stake / limit_price
        fill_shares = min(max_shares, shares)
        if fill_shares > 0:
            filled = True
            fill_ts = trade_ts
            fill_usdc = round(fill_shares * limit_price, 4)
            break

    recorded_filled = bool(int(attempt.get("hypothetical_filled_aggressive") or 0))
    return {
        "ok": True,
        "order_id": attempt.get("order_id"),
        "market_slug": attempt.get("market_slug"),
        "token_id": token_id,
        "events": len(events),
        "replay_filled": filled,
        "recorded_filled": recorded_filled,
        "matches_recorded": filled == recorded_filled,
        "fill_ts": fill_ts,
        "fill_usdc": fill_usdc,
        "queue_ahead": round(queue_ahead, 4),
        "queue_remaining": round(queue_remaining, 4),
        "direction_rejections": direction_rejections,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200, help="max maker_shadow_attempts to replay")
    parser.add_argument("--since-hours", type=float, default=None)
    parser.add_argument("--window-sec", type=float, default=300.0)
    parser.add_argument("--schema-sample", type=int, default=1000)
    parser.add_argument("--no-top-filter", action="store_true", help="apply every price_change level update as top")
    args = parser.parse_args()

    _ensure_table()
    attempts = _load_attempts(args.limit, args.since_hours)
    schema = _price_change_schema_stats(args.schema_sample)
    results = [
        _replay_attempt(a, window_sec=args.window_sec, top_filter=not args.no_top_filter)
        for a in attempts
    ]
    valid = [r for r in results if r.get("ok")]
    mismatches = [r for r in valid if not r.get("matches_recorded")]
    report = {
        "db_path": os.getenv("DB_PATH", "signal_zero.db"),
        "attempts_loaded": len(attempts),
        "attempts_replayed": len(valid),
        "replay_filled": sum(1 for r in valid if r.get("replay_filled")),
        "recorded_filled": sum(1 for r in valid if r.get("recorded_filled")),
        "mismatches": len(mismatches),
        "top_filter": not args.no_top_filter,
        "price_change_schema": schema,
        "mismatch_examples": mismatches[:10],
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
