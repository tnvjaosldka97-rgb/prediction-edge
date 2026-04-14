"""
Shadow-live performance report.

Usage:
  python -m shadow.report                 # all-time
  python -m shadow.report --strategy closing_convergence
  python -m shadow.report --days 7

Outputs:
  - Per-strategy: trade count, realized + unrealized PnL, win rate,
    avg slippage, adverse selection (drift at 5s/60s/5min)
  - Per-category breakdown
  - Comparison: realized PnL vs realistic_engine backtest prediction
"""
from __future__ import annotations
import argparse
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass

from core import db


def fmt_usd(v) -> str:
    if v is None:
        return "  n/a"
    return f"${v:+.2f}"


def report(strategy: str | None, days: int | None):
    conn = db.get_conn()
    clauses = ["1=1"]
    params: list = []
    if strategy:
        clauses.append("strategy = ?")
        params.append(strategy)
    if days:
        clauses.append("fill_ts > ?")
        params.append(time.time() - days * 86400)
    where = " AND ".join(clauses)

    rows = conn.execute(
        f"""SELECT id, strategy, category, fill_ts, fill_price, size_usd,
                   slippage, levels_touched, mid_at_signal, ask_at_signal,
                   mid_after_5s, mid_after_60s, mid_after_300s,
                   exit_price, realized_pnl, unrealized_pnl, resolved_at
            FROM virtual_trades WHERE {where}
            ORDER BY fill_ts ASC""",
        params,
    ).fetchall()

    if not rows:
        print("No virtual trades yet.")
        return

    print("=" * 78)
    print(f"Shadow-Live Report  ({len(rows)} trades)")
    print("=" * 78)

    # Per-strategy aggregates
    by_strat: dict = {}
    for r in rows:
        s = r["strategy"] or "unknown"
        by_strat.setdefault(s, []).append(r)

    print(f"\n{'strategy':>22}  {'n':>4}  {'realized':>10}  {'unreal':>10}  "
          f"{'winR':>6}  {'avg slip':>9}  {'avg drift 5m':>13}")
    print(f"{'-'*22}  {'-'*4}  {'-'*10}  {'-'*10}  {'-'*6}  {'-'*9}  {'-'*13}")

    for s, trades in sorted(by_strat.items()):
        resolved = [t for t in trades if t["resolved_at"]]
        unresolved = [t for t in trades if not t["resolved_at"]]
        realized = sum(t["realized_pnl"] or 0 for t in resolved)
        unreal = sum(t["unrealized_pnl"] or 0 for t in unresolved)
        if resolved:
            wins = sum(1 for t in resolved if (t["realized_pnl"] or 0) > 0)
            winR = wins / len(resolved)
        else:
            winR = 0.0
        slip_vals = [t["slippage"] or 0 for t in trades]
        avg_slip = sum(slip_vals) / len(slip_vals) if slip_vals else 0
        drift_vals = [
            (t["mid_after_300s"] - t["fill_price"])
            for t in trades
            if t["mid_after_300s"] is not None and t["fill_price"] is not None
        ]
        avg_drift = sum(drift_vals) / len(drift_vals) if drift_vals else None
        drift_str = f"{avg_drift*100:+.2f}¢" if avg_drift is not None else "  n/a"
        print(f"{s:>22}  {len(trades):>4}  {fmt_usd(realized):>10}  "
              f"{fmt_usd(unreal):>10}  {winR*100:>5.1f}%  {avg_slip*100:>7.2f}¢  {drift_str:>13}")

    # Per-category breakdown
    by_cat: dict = {}
    for r in rows:
        c = r["category"] or "unknown"
        by_cat.setdefault(c, []).append(r)

    print(f"\n  [category breakdown]")
    print(f"  {'category':>15}  {'n':>4}  {'realized':>10}  {'unreal':>10}  {'winR':>6}")
    print(f"  {'-'*15}  {'-'*4}  {'-'*10}  {'-'*10}  {'-'*6}")
    for c, trades in sorted(by_cat.items(), key=lambda x: -len(x[1])):
        resolved = [t for t in trades if t["resolved_at"]]
        realized = sum(t["realized_pnl"] or 0 for t in resolved)
        unreal = sum(t["unrealized_pnl"] or 0 for t in trades if not t["resolved_at"])
        wins = sum(1 for t in resolved if (t["realized_pnl"] or 0) > 0)
        winR = wins / len(resolved) if resolved else 0
        print(f"  {c[:15]:>15}  {len(trades):>4}  {fmt_usd(realized):>10}  "
              f"{fmt_usd(unreal):>10}  {winR*100:>5.1f}%")

    # Overall stats
    resolved = [r for r in rows if r["resolved_at"]]
    total_realized = sum(r["realized_pnl"] or 0 for r in resolved)
    total_unrealized = sum(r["unrealized_pnl"] or 0 for r in rows if not r["resolved_at"])
    total_deployed = sum(r["size_usd"] or 0 for r in rows)
    print(f"\n  realized:    {fmt_usd(total_realized)}  on {len(resolved)} resolved trades")
    print(f"  unrealized:  {fmt_usd(total_unrealized)}  on {len(rows) - len(resolved)} open trades")
    print(f"  deployed:    ${total_deployed:.2f} total")
    if total_deployed:
        print(f"  return:      {(total_realized + total_unrealized) / total_deployed * 100:+.2f}%")
    print("=" * 78)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default=None)
    ap.add_argument("--days", type=int, default=None)
    args = ap.parse_args()
    report(args.strategy, args.days)


if __name__ == "__main__":
    main()
