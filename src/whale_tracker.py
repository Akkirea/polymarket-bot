"""
SIGNAL/ZERO — Whale Tracker

Fetches recent BTC 5-min market trades from the Polymarket data API,
ranks wallets by win rate and P&L, and logs the top performers to
signal_zero.db.

Note: clob.polymarket.com/trades requires an API key (returns 401).
The equivalent public endpoint is data-api.polymarket.com/trades.

Standalone usage:
    python -m src.whale_tracker
"""

import asyncio
import sys
import os
from collections import defaultdict
from datetime import datetime
from typing import Optional

import aiohttp

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

FETCH_LIMIT  = 500
FETCH_PAGES  = 3    # pages of FETCH_LIMIT trades to fetch
TOP_WALLETS  = 20


class WhaleTracker:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Data fetching ────────────────────────────────────────────────────────

    async def _fetch_page(self, limit: int, offset: int) -> list:
        """Fetch one page of trades from the data API."""
        session = await self._get_session()
        try:
            async with session.get(
                f"{DATA_API}/trades",
                params={"limit": limit, "offset": offset},
            ) as resp:
                if resp.status != 200:
                    print(f"[whale] data-api returned {resp.status} (offset={offset})")
                    return []
                return await resp.json()
        except Exception as exc:
            print(f"[whale] fetch error (offset={offset}): {exc}")
            return []

    async def fetch_btc5m_trades(self, limit: int = FETCH_LIMIT, pages: int = FETCH_PAGES) -> list:
        """
        Pull trades from the public data API across `pages` pages and filter
        to BTC 5-minute up/down markets (slug contains btc-updown-5m).
        """
        all_trades: list = []
        for page in range(pages):
            offset = page * limit
            print(f"[whale] Fetching page {page + 1}/{pages} (offset={offset}, limit={limit}) ...")
            page_trades = await self._fetch_page(limit, offset)
            print(f"[whale]   → {len(page_trades)} raw trades")
            all_trades.extend(page_trades)
            if len(page_trades) < limit:
                print(f"[whale]   → fewer than limit returned, stopping pagination")
                break

        btc5m = [t for t in all_trades if "btc-updown-5m" in (t.get("slug") or "")]
        print(f"[whale] Total: {len(all_trades)} raw trades → {len(btc5m)} BTC 5m trades")
        return btc5m

    async def resolve_market_by_slug(self, slug: str) -> Optional[str]:
        """
        Ask the Gamma API for a market's resolution using its event slug.
        Returns the winning outcome string ("Up" or "Down"), or None if
        the market hasn't resolved yet.

        Resolution signal: outcomePrices driven to ["1","0"] or ["0","1"].
        Slug lookup is used (not conditionId) because conditionIds can be
        shared across multiple Gamma markets.
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{GAMMA_API}/markets",
                params={"slug": slug},
            ) as resp:
                if resp.status != 200:
                    return None
                markets = await resp.json()
        except Exception:
            return None

        if not markets:
            return None

        m = markets[0]
        try:
            outcomes_raw = m.get("outcomes", '["Up","Down"]')
            prices_raw = m.get("outcomePrices", "[0.5,0.5]")
            outcomes = eval(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            prices = eval(prices_raw) if isinstance(prices_raw, str) else prices_raw
        except Exception:
            return None

        for i, p in enumerate(prices):
            if float(p) >= 0.99:
                return str(outcomes[i])

        return None  # Still live / unresolved

    # ── Stats computation ─────────────────────────────────────────────────────

    async def compute_wallet_stats(self, trades: list) -> list:
        """
        Group trades by wallet, resolve each market, then score each wallet.

        Only BUY trades on resolved markets count toward win rate and P&L.
        SELL trades are counted in total_trades but excluded from P&L since
        we don't track original cost basis.

        P&L model (size = USDC cost):
          WIN : +size * (1/price - 1)   [you get size/price back]
          LOSS: -size
        """
        # Resolve all unique slugs in one batch.
        # We key on slug (not conditionId) because conditionId is non-unique
        # in the Gamma API — multiple markets can share the same conditionId.
        unique_slugs = {t["slug"] for t in trades if t.get("slug")}
        print(f"[whale] Resolving {len(unique_slugs)} unique slugs ...")
        resolutions: dict[str, Optional[str]] = {}
        for slug in unique_slugs:
            resolutions[slug] = await self.resolve_market_by_slug(slug)

        resolved_slugs   = {s for s, w in resolutions.items() if w is not None}
        unresolved_slugs = {s for s, w in resolutions.items() if w is None}
        print(f"[whale] Slugs resolved: {len(resolved_slugs)}  unresolved/live: {len(unresolved_slugs)}")
        if resolved_slugs:
            for s in sorted(resolved_slugs):
                print(f"[whale]   ✓ {s} → {resolutions[s]}")
        else:
            print("[whale] No resolved BTC 5-min markets found in this batch.")

        # Group trades by wallet
        by_wallet: dict[str, list] = defaultdict(list)
        for t in trades:
            wallet = t.get("proxyWallet")
            if wallet:
                by_wallet[wallet].append(t)

        stats = []
        for wallet, wallet_trades in by_wallet.items():
            wins = 0
            losses = 0
            total_pnl = 0.0

            for t in wallet_trades:
                if t.get("side") != "BUY":
                    continue
                winner = resolutions.get(t.get("slug", ""))
                if winner is None:
                    continue  # Unresolved — skip

                outcome = t.get("outcome", "")
                size = float(t.get("size") or 0)
                price = float(t.get("price") or 0.5)
                if price <= 0:
                    price = 0.5

                if outcome == winner:
                    wins += 1
                    total_pnl += size * (1.0 / price - 1.0)
                else:
                    losses += 1
                    total_pnl -= size

            resolved_count = wins + losses
            if resolved_count == 0:
                continue  # Nothing to rank on

            stats.append({
                "address": wallet,
                "total_trades": len(wallet_trades),
                "resolved_trades": resolved_count,
                "wins": wins,
                "losses": losses,
                "win_rate": wins / resolved_count,
                "pnl": round(total_pnl, 4),
                "last_updated": datetime.utcnow().isoformat(),
                "_raw_trades": wallet_trades,
            })

        # Primary sort: win rate desc; secondary: total resolved trades desc
        stats.sort(key=lambda x: (-x["win_rate"], -x["resolved_trades"]))
        return stats

    # ── Main entry ───────────────────────────────────────────────────────────

    async def run_once(self, db_module=None) -> list:
        """
        Fetch → resolve → compute → log to DB.
        Returns the top-N wallet stat dicts (without _raw_trades).
        """
        trades = await self.fetch_btc5m_trades()

        if not trades:
            print("[whale] No BTC 5m trades found — aborting")
            return []

        wallet_stats = await self.compute_wallet_stats(trades)
        print(f"[whale] {len(wallet_stats)} wallets with at least one resolved trade")

        top = wallet_stats[:TOP_WALLETS]

        if db_module is not None:
            for entry in top:
                db_module.upsert_whale_wallet(entry)
                for raw_t in entry["_raw_trades"]:
                    db_module.log_whale_trade(raw_t)

        # Strip internal key before returning
        return [{k: v for k, v in e.items() if k != "_raw_trades"} for e in top]


# ── Standalone runner ─────────────────────────────────────────────────────────

async def _standalone():
    # Adjust path so relative package imports work
    src_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if src_parent not in sys.path:
        sys.path.insert(0, src_parent)

    from src import db

    db.init_db()

    tracker = WhaleTracker()
    try:
        results = await tracker.run_once(db_module=db)
    finally:
        await tracker.close()

    if not results:
        print("\nNo ranked wallets yet — markets may still be resolving.")
        print("Try again in ~5 minutes once the current window closes.\n")
        return

    print(f"\n{'─'*70}")
    print(f"{'RANK':<5} {'ADDRESS':<14} {'WIN%':>6} {'TRADES':>7} {'RESOLVED':>9} {'P&L':>9}")
    print(f"{'─'*70}")
    for i, w in enumerate(results, 1):
        addr = w["address"][:6] + "…" + w["address"][-4:]
        print(
            f"{i:<5} {addr:<14} {w['win_rate']:>5.0%}  "
            f"{w['total_trades']:>6}  {w['resolved_trades']:>8}  "
            f"${w['pnl']:>8.2f}"
        )
    print(f"{'─'*70}\n")


if __name__ == "__main__":
    asyncio.run(_standalone())
