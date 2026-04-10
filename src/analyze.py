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

    # ── Signal Accuracy (direction vs actual outcome)
    console.print()
    console.print("[bold]Signal Accuracy[/bold]")

    # For each closed trade, check what direction the most recent signal
    # fired before that trade and whether it agreed with the outcome.
    trade_rows = conn.execute(
        "SELECT market_id, side, outcome FROM paper_trades "
        "WHERE outcome IS NOT NULL AND outcome != 'open'"
    ).fetchall()

    correct = 0
    incorrect = 0
    for t in trade_rows:
        sig_row = conn.execute(
            "SELECT direction FROM signals WHERE market_id = %s "
            "ORDER BY ts DESC LIMIT 1",
            (t["market_id"],),
        ).fetchone()
        if sig_row:
            if sig_row["direction"] == t["outcome"]:
                correct += 1
            else:
                incorrect += 1

    acc_table = Table(show_header=False, box=None, padding=(0, 3))
    acc_table.add_column(style="dim")
    acc_table.add_column(style="bold")

    if correct + incorrect > 0:
        accuracy = correct / (correct + incorrect)
        acc_style = "green" if accuracy > 0.55 else "yellow" if accuracy > 0.50 else "red"
        acc_table.add_row("Signal→outcome pairs", str(correct + incorrect))
        acc_table.add_row(
            "Direction accuracy",
            Text(f"{accuracy:.1%}  ({correct} correct, {incorrect} wrong)", style=acc_style),
        )
        # Breakeven (ignoring fees) for binary market with 50c shares = 50%.
        # With ~1% total fees, need ~51% to cover fees at entry_price=0.50.
        acc_table.add_row(
            "Breakeven (after fees ~1%)",
            "≈ 51%",
        )
    else:
        acc_table.add_row("Signal→outcome pairs", "0  (need closed trades with logged signals)")

    console.print(acc_table)

    # ── Per-Hour Performance (bot_trades)
    console.print()
    console.print("[bold]Bot Trades — Per Hour (ET)[/bold]")

    hour_rows = conn.execute(
        """SELECT
               CAST(((CAST(strftime('%H', opened_at) AS INTEGER) - 4 + 24) % 24) AS TEXT) AS hour_et,
               COUNT(*) AS total,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
               COALESCE(SUM(pnl), 0) AS total_pnl
           FROM bot_trades
           WHERE pnl IS NOT NULL AND COALESCE(outcome, '') NOT IN ('unresolved', '')
           GROUP BY hour_et
           ORDER BY hour_et"""
    ).fetchall()

    if hour_rows:
        hour_table = Table(box=None, padding=(0, 2))
        hour_table.add_column("Hour (ET)", style="dim")
        hour_table.add_column("Trades", justify="right")
        hour_table.add_column("Win%", justify="right")
        hour_table.add_column("Total P&L", justify="right")
        hour_table.add_column("Note", style="dim")

        for r in hour_rows:
            total_h = int(r["total"] or 0)
            wins_h  = int(r["wins"]  or 0)
            wr      = wins_h / total_h if total_h else 0.0
            pnl_h   = float(r["total_pnl"] or 0.0)
            wr_style = "green" if wr > 0.55 else "yellow" if wr > 0.50 else "red"
            note = ""
            if total_h < 10:
                note = f"[dim]n={total_h} — insufficient[/dim]"
            elif total_h < 20:
                note = f"[dim]n={total_h} — caution[/dim]"
            hour_table.add_row(
                r["hour_et"],
                str(total_h),
                Text(f"{wr:.0%}", style=wr_style),
                Text(f"${pnl_h:+.2f}", style="green" if pnl_h >= 0 else "red"),
                note,
            )
        console.print(hour_table)
    else:
        console.print("[dim]  No resolved bot trades yet.[/dim]")

    # Kelly sizing sanity check
    console.print()
    all_bot = conn.execute(
        "SELECT COUNT(*) AS n FROM bot_trades "
        "WHERE pnl IS NOT NULL AND COALESCE(outcome,'') NOT IN ('unresolved','')"
    ).fetchone()
    n_bot = int(all_bot["n"] or 0)
    if n_bot < 20:
        console.print(
            Panel(
                f"[yellow]Kelly sizing is using the conservative default (WIN_PROB=0.52) "
                f"because only {n_bot} resolved trades exist.\n"
                f"Need ≥ 20 resolved trades for measured win rate to take effect.[/yellow]",
                title="[bold]Kelly Warning[/bold]",
                border_style="yellow",
            )
        )

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
