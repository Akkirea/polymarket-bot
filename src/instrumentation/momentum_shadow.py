"""Shadow experiment for the momentum gate — observability-only.

Records every signal that reaches the momentum check (post chop + diff +
funding), classified by the magnitude of momentum *opposing* the signal
direction. Lets us estimate the would-have-passed pass-rate under relaxed
momentum tolerances without any change to live behavior.

Pass logic mirrors the live gate:
  direction=Up    rejects if momentum <  0   → opposing_mag = max(0, -momentum)
  direction=Down  rejects if momentum >  0   → opposing_mag = max(0,  momentum)
  direction=*     "no-history" if momentum is None — tallied separately.

Bucketed opposing_mag lets a reader compute pass_rate(T) for arbitrary T as
cumulative bucket-count divided by `n_evaluable`. `cum_pass_at_tolerance`
and `pass_rate_at_tolerance` precompute a fixed shortlist for quick reads.

To remove cleanly:
  1. Delete this file and its single line of state.
  2. Delete the import line in src/bot.py.
  3. Delete the momentum_shadow.record(...) call in src/bot.py.
  4. Delete the momentum_shadow.snapshot() wire-in in src/bot.py:_shadow_status_block.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Optional


# Buckets are (name, upper_bound_exclusive). First match wins.
# "0" captures the currently-passing case (opposing_mag effectively 0).
BUCKETS: tuple = (
    ("0",       1e-9),
    ("0to5",    5.0),
    ("5to10",   10.0),
    ("10to20",  20.0),
    ("20to50",  50.0),
    ("50plus",  float("inf")),
)

# Tolerance thresholds (in $) for the shadow pass-rate readout.
TOLERANCES: tuple = (0.0, 5.0, 10.0, 20.0, 50.0)


class _MomentumShadow:
    def __init__(self):
        self._lock = threading.Lock()
        self._n_total = 0
        self._n_no_history = 0
        self._buckets: dict = {name: 0 for name, _ in BUCKETS}
        self._by_strategy: dict = defaultdict(
            lambda: {name: 0 for name, _ in BUCKETS}
        )

    def record(
        self,
        strategy: Optional[str],
        direction: Optional[str],
        momentum: Optional[float],
    ) -> None:
        with self._lock:
            self._n_total += 1
            if momentum is None:
                self._n_no_history += 1
                return
            if direction == "Up":
                opposing = max(0.0, -float(momentum))
            elif direction == "Down":
                opposing = max(0.0, float(momentum))
            else:
                opposing = 0.0  # defensive; live gate only fires with Up/Down
            s = strategy or "_unspecified_"
            for name, cap in BUCKETS:
                if opposing < cap:
                    self._buckets[name] += 1
                    self._by_strategy[s][name] += 1
                    return

    def snapshot(self) -> dict:
        with self._lock:
            buckets = dict(self._buckets)
            by_strat = {k: dict(v) for k, v in self._by_strategy.items()}
            n_total = self._n_total
            n_no_history = self._n_no_history
        evaluable = n_total - n_no_history
        cum_passes: dict = {}
        for T in TOLERANCES:
            count = 0
            for name, cap in BUCKETS:
                if cap <= T + 1e-9:
                    count += buckets[name]
            cum_passes[f"T<={T:g}"] = count
        pass_rate = {
            k: (v / evaluable) if evaluable > 0 else 0.0
            for k, v in cum_passes.items()
        }
        return {
            "n_total_evals": n_total,
            "n_no_history": n_no_history,
            "n_evaluable": evaluable,
            "buckets": buckets,
            "by_strategy": by_strat,
            "cum_pass_at_tolerance": cum_passes,
            "pass_rate_at_tolerance": pass_rate,
        }


momentum_shadow = _MomentumShadow()
