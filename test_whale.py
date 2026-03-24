"""
Quick test: run whale_tracker against live API and print results.
Usage: venv/bin/python test_whale.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src import db
from src.whale_tracker import WhaleTracker


async def main():
    db.init_db()

    tracker = WhaleTracker()
    try:
        results = await tracker.run_once(db_module=db)
    finally:
        await tracker.close()

    if not results:
        print("\nNo ranked wallets yet — no resolved markets in this batch.")
        print("Markets resolve every 5 minutes. Try again shortly.\n")

        # Show raw trade count so we know the fetch works
        tracker2 = WhaleTracker()
        try:
            trades = await tracker2.fetch_btc5m_trades(limit=200)
        finally:
            await tracker2.close()

        print(f"Confirmed: fetched {len(trades)} BTC 5-min trades from data-api.")
        if trades:
            slugs = sorted({t.get("slug", "") for t in trades})
            print(f"Slugs in batch ({len(slugs)}):")
            for s in slugs:
                print(f"  {s}")
        return

    print(f"\n{'─'*72}")
    print(f"{'RANK':<5} {'ADDRESS':<15} {'WIN%':>6} {'TRADES':>7} {'RESOLVED':>9} {'P&L':>10}")
    print(f"{'─'*72}")
    for i, w in enumerate(results, 1):
        addr = w["address"]
        short = addr[:6] + "…" + addr[-4:]
        pnl = w["pnl"]
        pnl_str = f"${pnl:+.2f}"
        print(
            f"{i:<5} {short:<15} {w['win_rate']:>5.0%}  "
            f"{w['total_trades']:>6}  {w['resolved_trades']:>8}  {pnl_str:>9}"
        )
    print(f"{'─'*72}\n")
    print(f"Logged top {len(results)} wallets to signal_zero.db (whale_wallets table).\n")


if __name__ == "__main__":
    asyncio.run(main())
