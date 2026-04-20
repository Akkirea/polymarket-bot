"""
Dedicated HTF paper-trading worker.

Run separately from the FastAPI web process so the HTF strategy cannot
interfere with latency-sensitive API and bot work.
"""

import asyncio

import aiohttp

from . import db
from . import htf as htf_lib

HTF_SIGMA_TTL = 30 * 60
HTF_INTERVAL = 20
HTF_MIN_EDGE = 0.05
DEFAULT_SIGMA = 0.80


async def run_htf_pipeline():
    """Background loop for HTF market discovery, paper entries, and resolution."""
    sigma = DEFAULT_SIGMA
    sigma_ts = 0.0

    db.init_db()
    db.init_htf_tables()
    await asyncio.sleep(5)

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                now = asyncio.get_running_loop().time()

                if now - sigma_ts > HTF_SIGMA_TTL:
                    prices = await htf_lib.fetch_recent_prices(session)
                    if prices:
                        sigma = htf_lib.estimate_vol(prices, 3600)
                        sigma_ts = now
                        print(f"[htf-worker] vol refreshed -> sigma={sigma*100:.1f}%/yr", flush=True)

                spot, markets = await asyncio.gather(
                    htf_lib.fetch_btc_price(session),
                    htf_lib.fetch_htf_markets(session),
                )
                print(f"[htf-worker] spot=${spot:,.0f} sigma={sigma*100:.1f}% markets={len(markets)}", flush=True)

                state = db.htf_get_state()
                balance = state["balance"]

                for market in markets:
                    slug = market["slug"]
                    strike = market["price_to_beat"] or spot
                    secs_left = market["seconds_left"]
                    market_prob = market["prob_up"]

                    prob = htf_lib.btc_up_prob(spot, strike, sigma, secs_left)
                    est_prob = prob["prob_up"]
                    edge = round(est_prob - market_prob, 4)

                    db.htf_save_snapshot(slug, spot, market_prob, est_prob, edge, sigma)

                    if abs(edge) >= HTF_MIN_EDGE and not db.htf_has_open_trade(slug):
                        side = "UP" if edge > 0 else "DOWN"
                        entry_prob = market_prob if side == "UP" else market["prob_down"]
                        fraction = htf_lib.half_kelly(est_prob, entry_prob)
                        stake = max(1.0, round(balance * fraction, 2))
                        db.htf_open_trade(slug, side, stake, entry_prob, spot, edge)
                        print(
                            f"[htf-worker] TRADE {side} {slug} stake=${stake} edge={edge:.4f}",
                            flush=True,
                        )

                open_trades = [trade for trade in db.htf_get_recent_trades(50) if trade.get("closed_at") is None]
                for trade in open_trades:
                    winner, final_price, price_to_beat = await htf_lib.fetch_resolution(
                        session, trade["market_slug"]
                    )
                    if winner:
                        db.htf_close_trades(trade["market_slug"], winner, final_price, price_to_beat)
                        print(f"[htf-worker] RESOLVED {trade['market_slug']} -> {winner.upper()}", flush=True)

            except Exception as exc:
                print(f"[htf-worker] pipeline error: {exc}", flush=True)

            await asyncio.sleep(HTF_INTERVAL)


def main():
    asyncio.run(run_htf_pipeline())


if __name__ == "__main__":
    main()
