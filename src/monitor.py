"""
SIGNAL/ZERO Phase 1 — Main Monitor

Run with: python -m src.monitor

This is the main loop that:
1. Starts the Binance price feed (WebSocket)
2. Polls Polymarket for the active 5-min BTC market
3. Runs edge detection every poll cycle
4. Paper trades when signals are strong enough
5. Displays everything in a live terminal dashboard

Ctrl+C to stop. Data persists in signal_zero.db.
"""

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from . import config
from . import db
from .price_feed import PriceFeed
from .polymarket_api import PolymarketClient
from .edge_detector import detect_edge
from .paper_trader import PaperTrader
from .whale_tracker import WhaleTracker


console = Console()


def build_dashboard(
    price_feed: PriceFeed,
    poly_client: PolymarketClient,
    trader: PaperTrader,
    last_signal,
    signal_log: list,
    status: str,
    top_wallets: list = None,
) -> Panel:
    """Build the terminal UI using Rich."""

    # ── Header
    header = Text()
    header.append("SIGNAL / ZERO", style="bold white")
    header.append("  ·  Phase 1 Monitor  ·  ", style="dim")
    header.append("PAPER TRADING", style="bold yellow")

    # ── Price + Connection Status
    info_table = Table(show_header=False, box=None, padding=(0, 3))
    info_table.add_column(style="dim")
    info_table.add_column(style="bold")

    btc_str = f"${price_feed.current_price:,.2f}" if price_feed.current_price else "connecting..."
    source_label = price_feed.source.upper()
    feed_status = f"[green]● LIVE · {source_label}[/green]" if price_feed.connected else "[yellow]● CONNECTING...[/yellow]"
    poly_status = "[green]● ACTIVE[/green]" if poly_client.current_market else "[yellow]● SEARCHING[/yellow]"

    info_table.add_row("BTC/USD", btc_str)
    info_table.add_row("Price Feed", feed_status)
    info_table.add_row("Polymarket", poly_status)

    # ── Current Market
    market_table = Table(show_header=True, box=None, padding=(0, 2))
    market_table.add_column("Market", style="dim")
    market_table.add_column("Up $", style="green")
    market_table.add_column("Down $", style="red")
    market_table.add_column("Volume", style="dim")
    market_table.add_column("Window", style="dim")

    m = poly_client.current_market
    if m:
        market_table.add_row(
            m["question"][:60],
            f"{m['up_price']:.3f}",
            f"{m['down_price']:.3f}",
            f"${m['volume']:,.0f}",
            f"{m['window_start']} – {m['window_end']}",
        )
    else:
        market_table.add_row("searching for active market...", "—", "—", "—", "—")

    # ── Edge Detection
    edge_table = Table(show_header=True, box=None, padding=(0, 2))
    edge_table.add_column("Momentum P(Up)", style="cyan")
    edge_table.add_column("Market P(Up)", style="yellow")
    edge_table.add_column("Divergence", style="bold")
    edge_table.add_column("Confidence", style="dim")

    momentum = price_feed.get_momentum()
    if momentum is not None and m:
        implied_up = m["up_price"] / max(m["up_price"] + m["down_price"], 0.01)
        div = momentum - implied_up
        div_style = "green" if abs(div) > config.EDGE_THRESHOLD else "dim"
        conf = "—"
        if last_signal:
            conf = last_signal.confidence

        edge_table.add_row(
            f"{momentum:.1%}",
            f"{implied_up:.1%}",
            Text(f"{div:+.1%}", style=div_style),
            conf,
        )
    else:
        edge_table.add_row("calculating...", "—", "—", "—")

    # ── Paper Trading Stats
    stats = trader.summary()
    stats_table = Table(show_header=False, box=None, padding=(0, 3))
    stats_table.add_column(style="dim")
    stats_table.add_column(style="bold")

    pnl_style = "green" if stats["total_pnl"] >= 0 else "red"
    daily_style = "green" if stats["daily_pnl"] >= 0 else "red"

    stats_table.add_row("Balance", f"${stats['balance']:.2f}")
    stats_table.add_row("Total Value", f"${stats['total_value']:.2f}")
    stats_table.add_row("Total P&L", Text(f"${stats['total_pnl']:+.3f}", style=pnl_style))
    stats_table.add_row("Daily P&L", Text(f"${stats['daily_pnl']:+.3f}", style=daily_style))
    stats_table.add_row("Trades", f"{stats['total_trades']} ({stats['win_rate']:.0%} win)")
    stats_table.add_row("Open", f"{stats['open_positions']} position(s)")

    # ── Open Positions Detail
    pos_table = Table(show_header=True, box=None, padding=(0, 2))
    pos_table.add_column("Side", style="bold", width=6)
    pos_table.add_column("Size $", style="dim", justify="right")
    pos_table.add_column("Entry", justify="right")
    pos_table.add_column("Open BTC", justify="right")
    pos_table.add_column("Cur BTC", justify="right")
    pos_table.add_column("Expires in", justify="right")

    now_ts = time.time()
    current_btc = price_feed.current_price

    if trader.positions:
        for pos in trader.positions:
            secs_left = max(0.0, pos.end_ts - now_ts) if pos.end_ts else 0.0
            mins_left = int(secs_left // 60)
            secs_rem  = int(secs_left % 60)
            exp_str   = f"{mins_left}m{secs_rem:02d}s" if pos.end_ts else "—"

            cur_str = f"${current_btc:,.2f}" if current_btc else "—"
            open_str = f"${pos.price_to_beat:,.2f}" if pos.price_to_beat else "—"
            side_style = "green" if pos.side == "up" else "red"

            # Colour expiry red when < 30s
            exp_style = "red" if secs_left < 30 else "yellow" if secs_left < 90 else "dim"

            pos_table.add_row(
                Text(pos.side.upper(), style=side_style),
                f"${pos.size:.2f}",
                f"{pos.entry_price:.3f}",
                open_str,
                cur_str,
                Text(exp_str, style=exp_style),
            )
    else:
        pos_table.add_row("—", "—", "—", "—", "—", "—")

    # ── Signal Log
    log_table = Table(show_header=True, box=None, padding=(0, 1))
    log_table.add_column("Time", style="dim", width=10)
    log_table.add_column("Event", style="white")

    for entry in signal_log[-8:]:
        log_table.add_row(entry["time"], entry["msg"])

    if not signal_log:
        log_table.add_row("—", "waiting for data...")

    # ── Top Wallets
    whale_table = Table(show_header=True, box=None, padding=(0, 2))
    whale_table.add_column("Wallet", style="cyan", width=14)
    whale_table.add_column("Win%", style="green", justify="right")
    whale_table.add_column("Trades", style="dim", justify="right")
    whale_table.add_column("Resolved", style="dim", justify="right")
    whale_table.add_column("P&L", justify="right")

    if top_wallets:
        for w in top_wallets[:5]:
            addr = w["address"]
            short = addr[:6] + "…" + addr[-4:]
            pnl_val = w.get("pnl", 0)
            pnl_style = "green" if pnl_val >= 0 else "red"
            whale_table.add_row(
                short,
                f"{w['win_rate']:.0%}",
                str(w["total_trades"]),
                str(w["resolved_trades"]),
                Text(f"${pnl_val:+.2f}", style=pnl_style),
            )
    else:
        whale_table.add_row("fetching...", "—", "—", "—", "—")

    # ── Assemble
    output = Table(show_header=False, box=None, padding=(1, 0))
    output.add_column()

    output.add_row(header)
    output.add_row("")
    output.add_row(Panel(info_table, title="[dim]CONNECTION[/dim]", border_style="dim"))
    output.add_row(Panel(market_table, title="[dim]ACTIVE MARKET[/dim]", border_style="dim"))
    output.add_row(Panel(edge_table, title="[dim]EDGE DETECTION[/dim]", border_style="cyan"))
    output.add_row(Panel(stats_table, title="[dim]PAPER TRADING[/dim]", border_style="yellow"))
    output.add_row(Panel(pos_table, title="[dim]OPEN POSITIONS[/dim]", border_style="blue"))
    output.add_row(Panel(whale_table, title="[dim]TOP WALLETS · BTC 5M[/dim]", border_style="magenta"))
    output.add_row(Panel(log_table, title="[dim]LOG[/dim]", border_style="dim"))
    output.add_row(Text(f"  {status}", style="dim"))

    return Panel(
        output,
        border_style="bright_black",
        title="[bold]SIGNAL/ZERO[/bold]",
        subtitle="[dim]Ctrl+C to stop · data saved to signal_zero.db[/dim]",
    )


async def main():
    # Initialize
    db.init_db()
    price_feed = PriceFeed()
    poly_client = PolymarketClient()
    trader = PaperTrader()
    whale_tracker = WhaleTracker()

    last_signal = None
    signal_log = []
    status = "initializing..."
    top_wallets: list = []
    whale_last_run: float = 0

    def add_log(msg: str, style: str = ""):
        signal_log.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "msg": msg,
        })
        if len(signal_log) > 50:
            signal_log.pop(0)

    add_log("SIGNAL/ZERO Phase 1 starting...")
    add_log(f"Paper bankroll: ${config.INITIAL_BANKROLL:.2f}")
    add_log(f"Edge threshold: {config.EDGE_THRESHOLD:.0%}")

    # Seed top_wallets from any previously stored records
    top_wallets = db.get_top_whale_wallets(limit=5)

    # Start CoinGecko price feed in background
    price_feed_task = asyncio.create_task(price_feed.connect())

    # Handle Ctrl+C
    shutdown = asyncio.Event()

    def handle_signal(*args):
        shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        with Live(
            build_dashboard(price_feed, poly_client, trader, last_signal, signal_log, status),
            console=console,
            refresh_per_second=2,
            screen=False,
        ) as live:

            poll_counter = 0

            while not shutdown.is_set():
                try:
                    # ── Poll Polymarket every POLL_INTERVAL cycles
                    if poll_counter % config.POLL_INTERVAL == 0:
                        status = "polling polymarket..."
                        market = await poly_client.poll_market()

                        if market:
                            if not poly_client.current_market or poly_client.current_market.get("id") != market.get("id"):
                                add_log(f"Market found: {market['question'][:50]}")
                            db.log_market_snapshot(market)
                        else:
                            add_log("No active 5-min BTC market found")

                    # ── Run edge detection
                    market = poly_client.current_market
                    if market and price_feed.current_price:
                        momentum = price_feed.get_momentum()

                        if momentum is not None:
                            edge_signal = detect_edge(
                                momentum=momentum,
                                market_up_price=market["up_price"],
                                market_down_price=market["down_price"],
                                btc_price=price_feed.current_price,
                                market_id=market.get("id", ""),
                            )

                            if edge_signal:
                                last_signal = edge_signal

                                # Log to database
                                db.log_signal({
                                    "market_id": edge_signal.market_id,
                                    "btc_price": edge_signal.btc_price,
                                    "momentum": edge_signal.momentum,
                                    "implied_up_prob": edge_signal.implied_up_prob,
                                    "divergence": edge_signal.divergence,
                                    "direction": edge_signal.direction,
                                })

                                add_log(
                                    f"SIGNAL: {edge_signal.direction.upper()} "
                                    f"div={edge_signal.divergence:+.1%} "
                                    f"[{edge_signal.confidence}]"
                                )

                                # Paper trade if conditions met
                                if trader.can_trade() and edge_signal.confidence != "low":
                                    pos = trader.open_position(edge_signal, market)
                                    if pos:
                                        add_log(
                                            f"PAPER OPEN: {pos.side.upper()} "
                                            f"@{pos.entry_price:.3f} "
                                            f"${pos.size:.2f}"
                                        )

                            # Log price tick periodically
                            if poll_counter % 5 == 0:
                                db.log_tick(price_feed.current_price)

                        status = f"monitoring · tick #{poll_counter}"
                    else:
                        status = "waiting for data..."

                    # ── Resolve expired positions
                    expired = trader.get_expired_positions()
                    for pos in expired:
                        if price_feed.current_price:
                            # "Up" wins if final BTC >= price at window open
                            outcome = "up" if price_feed.current_price >= pos.price_to_beat else "down"
                            trade = trader.resolve_position(pos, outcome)
                            won = trade["pnl"] > 0
                            add_log(
                                f"{'✓ WIN' if won else '✗ LOSS'}: {pos.side.upper()} "
                                f"open={pos.price_to_beat:,.2f} final={price_feed.current_price:,.2f} "
                                f"PnL={trade['pnl']:+.2f}"
                            )

                    # ── Refresh whale data every 60 seconds
                    import time as _time
                    if _time.time() - whale_last_run >= 60:
                        whale_last_run = _time.time()
                        try:
                            fresh = await whale_tracker.run_once(db_module=db)
                            if fresh:
                                top_wallets = db.get_top_whale_wallets(limit=5)
                                add_log(f"Whale update: {len(fresh)} wallets ranked")
                        except Exception as we:
                            add_log(f"Whale err: {str(we)[:50]}")

                    # ── Update display
                    live.update(
                        build_dashboard(
                            price_feed, poly_client, trader,
                            last_signal, signal_log, status,
                            top_wallets=top_wallets,
                        )
                    )

                    poll_counter += 1
                    await asyncio.sleep(1)

                except Exception as e:
                    add_log(f"Error: {str(e)[:60]}")
                    await asyncio.sleep(2)

    except asyncio.CancelledError:
        pass
    finally:
        # Cleanup
        price_feed.stop()
        await poly_client.close()
        await whale_tracker.close()
        price_feed_task.cancel()

        console.print("\n[yellow]SIGNAL/ZERO shutting down...[/yellow]")

        # Print final stats
        stats = trader.summary()
        console.print(f"  Final balance: ${stats['balance']:.2f}")
        console.print(f"  Total P&L: ${stats['total_pnl']:+.3f}")
        console.print(f"  Trades: {stats['total_trades']} ({stats['win_rate']:.0%} win rate)")
        console.print(f"  Data saved to {config.DB_PATH}")
        console.print()


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
