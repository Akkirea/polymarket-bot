"""
SIGNAL/ZERO Phase 1 — Analysis Script

Run with: python -m src.analyze

Reads the SQLite database and generates a summary of:
- Paper trading performance
- Signal quality (did signals predict correctly?)
- Optimal threshold analysis
- Fee impact analysis
"""

import sqlite3
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from . import config
from . import db


console = Console()


def analyze():
    db.init_db()
    conn = db.get_connection()

    console.print()
    console.print(
        Panel(
            "[bold]SIGNAL/ZERO[/bold]  ·  Phase 1 Analysis",
            border_style="cyan",
        )
    )

    # ── Overall Stats
    stats = db.get_stats()
    console.print()
    console.print("[bold]Paper Trading Summary[/bold]")

    stats_table = Table(show_header=False, box=None, padding=(0, 3))
    stats_table.add_column(style="dim")
    stats_table.add_column(style="bold")

    pnl_style = "green" if stats["total_pnl"] >= 0 else "red"
    win_rate = stats["wins"] / max(stats["total_trades"], 1)

    stats_table.add_row("Total trades", str(stats["total_trades"]))
    stats_table.add_row("Wins / Losses", f"{stats['wins']} / {stats['losses']}")
    stats_table.add_row("Win rate", f"{win_rate:.1%}")
    stats_table.add_row("Total P&L", Text(f"${stats['total_pnl']:+.3f}", style=pnl_style))
    stats_table.add_row("Total fees paid", f"${stats['total_fees']:.3f}")
    stats_table.add_row("P&L after fees", Text(f"${stats['total_pnl']:+.3f}", style=pnl_style))
    stats_table.add_row("Signals detected", str(stats["total_signals"]))
    stats_table.add_row("Signals traded", str(stats["signals_traded"]))

    console.print(stats_table)

    # ── Signal Quality Analysis
    console.print()
    console.print("[bold]Signal Quality[/bold]")

    signals = conn.execute("""
        SELECT direction, divergence, ABS(divergence) as abs_div
        FROM signals
        ORDER BY ts DESC
        LIMIT 100
    """).fetchall()

    if signals:
        avg_div = sum(abs(s["divergence"]) for s in signals) / len(signals)
        up_signals = sum(1 for s in signals if s["direction"] == "up")
        down_signals = sum(1 for s in signals if s["direction"] == "down")

        sig_table = Table(show_header=False, box=None, padding=(0, 3))
        sig_table.add_column(style="dim")
        sig_table.add_column(style="bold")

        sig_table.add_row("Recent signals (last 100)", str(len(signals)))
        sig_table.add_row("Avg divergence", f"{avg_div:.1%}")
        sig_table.add_row("Up / Down split", f"{up_signals} / {down_signals}")

        console.print(sig_table)
    else:
        console.print("[dim]  No signals recorded yet. Run the monitor first.[/dim]")

    # ── Trade Detail
    console.print()
    console.print("[bold]Recent Trades[/bold]")

    trades = conn.execute("""
        SELECT opened_at, closed_at, side, entry_price, size, fee, outcome, pnl
        FROM paper_trades
        ORDER BY opened_at DESC
        LIMIT 20
    """).fetchall()

    if trades:
        trade_table = Table(box=None, padding=(0, 1))
        trade_table.add_column("Opened", style="dim")
        trade_table.add_column("Side")
        trade_table.add_column("Entry $")
        trade_table.add_column("Size")
        trade_table.add_column("Fee")
        trade_table.add_column("Outcome")
        trade_table.add_column("P&L")

        for t in trades:
            pnl = t["pnl"] or 0
            pnl_text = Text(f"${pnl:+.3f}", style="green" if pnl > 0 else "red")
            outcome_text = Text(
                t["outcome"] or "open",
                style="green" if t["outcome"] == t["side"] else "red" if t["outcome"] else "yellow",
            )
            trade_table.add_row(
                (t["opened_at"] or "")[:19],
                t["side"].upper(),
                f"{t['entry_price']:.3f}",
                f"${t['size']:.2f}",
                f"${t['fee']:.4f}",
                outcome_text,
                pnl_text,
            )

        console.print(trade_table)
    else:
        console.print("[dim]  No trades yet.[/dim]")

    # ── Price Data Summary
    console.print()
    tick_count = conn.execute("SELECT COUNT(*) as n FROM price_ticks").fetchone()["n"]
    market_count = conn.execute("SELECT COUNT(*) as n FROM market_snapshots").fetchone()["n"]

    console.print(f"[dim]  Price ticks logged: {tick_count:,}[/dim]")
    console.print(f"[dim]  Market snapshots: {market_count:,}[/dim]")
    console.print(f"[dim]  Database: {config.DB_PATH}[/dim]")

    # ── Verdict
    console.print()
    if stats["total_trades"] >= 20:
        if stats["total_pnl"] > 0 and win_rate > 0.55:
            console.print(
                Panel(
                    "[green]Edge appears viable after fees. "
                    "Consider tightening threshold and extending observation "
                    "before Phase 2.[/green]",
                    title="[bold]Verdict[/bold]",
                    border_style="green",
                )
            )
        elif stats["total_pnl"] > 0:
            console.print(
                Panel(
                    "[yellow]Marginal edge detected. Win rate is thin. "
                    "Continue monitoring — this could be variance.[/yellow]",
                    title="[bold]Verdict[/bold]",
                    border_style="yellow",
                )
            )
        else:
            console.print(
                Panel(
                    "[red]No edge detected at current threshold. "
                    "Try adjusting EDGE_THRESHOLD or MOMENTUM_WINDOW "
                    "in config.py before concluding.[/red]",
                    title="[bold]Verdict[/bold]",
                    border_style="red",
                )
            )
    else:
        console.print(
            Panel(
                f"[dim]Need at least 20 trades for a verdict. "
                f"Currently: {stats['total_trades']}. Keep running.[/dim]",
                title="[bold]Verdict[/bold]",
                border_style="dim",
            )
        )

    console.print()
    conn.close()


if __name__ == "__main__":
    analyze()
