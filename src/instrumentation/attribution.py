"""DispatchAttribution — observability counters for why maker_shadow dispatch
fires or does not fire.

Pure observability. NO behavior change. To remove cleanly:
  1. Delete this file and its parent package (src/instrumentation/)
  2. Delete the import line in src/bot.py
  3. Delete the attribution.bump(...) calls in src/bot.py (each is one line)
  4. Delete the snapshot wire-in in src/bot.py:_shadow_status_block

The bump() call is the only side-effect; it can be replaced with a no-op for
A/B comparison without affecting any other logic.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Optional


# The exact, fixed set of reasons the operator asked for. Unknown reasons are
# silently dropped so a typo cannot raise from a hot path.
REASONS: tuple = (
    "no_clear_signal",
    "chop_filter",
    "no_market",
    "execution_disabled",
    "token_stale",
    "ws_unhealthy",
    "market_closed",
    "exposure_cap",
    "dispatch_called",
)


class _DispatchAttribution:
    """Thread-safe counter map with per-strategy breakdown and periodic summary.

    Summary cadence is event-driven (checked on every bump), not on a timer,
    to avoid spawning a background task purely for logging.
    """

    def __init__(self, summary_interval_sec: float = 300.0):
        self._lock = threading.Lock()
        self._totals: dict = {r: 0 for r in REASONS}
        self._per_strategy: dict = {r: defaultdict(int) for r in REASONS}
        self._last_summary_ts: float = time.time()
        self._summary_interval: float = float(summary_interval_sec)
        self._last_event: Optional[dict] = None

    def bump(self, reason: str, strategy: Optional[str] = None) -> None:
        if reason not in self._totals:
            return
        s = strategy or "_unspecified_"
        emit_snapshot: Optional[dict] = None
        with self._lock:
            self._totals[reason] += 1
            self._per_strategy[reason][s] += 1
            self._last_event = {"reason": reason, "strategy": s, "ts": time.time()}
            if (time.time() - self._last_summary_ts) >= self._summary_interval:
                self._last_summary_ts = time.time()
                emit_snapshot = dict(self._totals)
        if emit_snapshot is not None:
            self._emit_summary(emit_snapshot)

    def _emit_summary(self, totals: dict) -> None:
        nonzero = {r: n for r, n in totals.items() if n > 0}
        if not nonzero:
            return
        ordered = sorted(nonzero.items(), key=lambda kv: -kv[1])
        body = "  ".join(f"{r}={n}" for r, n in ordered)
        print(f"[attribution] 5m-summary  {body}", flush=True)

    def snapshot(self) -> dict:
        """Read-only export for the status endpoint."""
        with self._lock:
            totals = dict(self._totals)
            per_strategy = {
                r: dict(s) for r, s in self._per_strategy.items() if s
            }
            last = dict(self._last_event) if self._last_event else None
        return {
            "totals": totals,
            "per_strategy": per_strategy,
            "last_event": last,
        }


# Module-level singleton. All bump() calls go through this.
attribution = _DispatchAttribution()
