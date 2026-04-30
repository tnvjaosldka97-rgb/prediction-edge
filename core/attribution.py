"""
Performance Attribution — PnL 기여도 분해.

질문: "이번 주 +$50 수익 — 어디서 왔나?"
대답:
  - closing_convergence: +$30 (60%)
  - claude_oracle: +$15 (30%)
  - news_lag: +$8 (16%)
  - dispute_premium: -$3 (-6%)

그리고:
  - 카테고리별: politics +$25 / economy +$20 / tech +$5
  - 시간대별: US daytime +$45 / Asian night +$5
  - 포지션 사이즈별: 작은 트레이드 +$30 (vs 큰 +$20)

이 분석으로 어디 더 투자하고 어디 줄여야 하는지 결정.
"""
from __future__ import annotations
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import config


def attribution_by_strategy(window_days: float = 30) -> dict[str, dict]:
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT strategy, pnl, fill_price, size_shares, fee_paid FROM trades "
        "WHERE timestamp >= ? AND strategy IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    by_strategy = defaultdict(lambda: {"pnl": 0, "n": 0, "fees": 0, "volume": 0})
    for s, pnl, fp, ss, fee in rows:
        by_strategy[s]["pnl"] += (pnl or 0)
        by_strategy[s]["n"] += 1
        by_strategy[s]["fees"] += (fee or 0)
        by_strategy[s]["volume"] += (fp or 0) * (ss or 0)

    total_pnl = sum(v["pnl"] for v in by_strategy.values())
    out = {}
    for s, v in by_strategy.items():
        out[s] = {
            "pnl": v["pnl"],
            "pnl_pct_of_total": v["pnl"] / total_pnl * 100 if total_pnl != 0 else 0,
            "n_trades": v["n"],
            "avg_pnl_per_trade": v["pnl"] / v["n"] if v["n"] else 0,
            "fees_paid": v["fees"],
            "volume": v["volume"],
        }
    return out


def attribution_by_category(window_days: float = 30) -> dict[str, dict]:
    """카테고리별 — virtual_trades.category 사용 (DRY_RUN 데이터)
    + 라이브에서는 trades에 strategy로만 있어서 그걸 카테고리로 매핑."""
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)

    # virtual_trades에 category 있음
    rows = conn.execute(
        "SELECT category, COALESCE(realized_pnl, unrealized_pnl, 0) FROM virtual_trades "
        "WHERE fill_ts >= ? AND category IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    by_cat = defaultdict(lambda: {"pnl": 0, "n": 0})
    for cat, pnl in rows:
        c = cat or "unknown"
        by_cat[c]["pnl"] += pnl
        by_cat[c]["n"] += 1

    total = sum(v["pnl"] for v in by_cat.values())
    out = {}
    for c, v in by_cat.items():
        out[c] = {
            "pnl": v["pnl"],
            "pnl_pct_of_total": v["pnl"] / total * 100 if total != 0 else 0,
            "n_trades": v["n"],
        }
    return out


def attribution_by_size_bucket(window_days: float = 30) -> dict[str, dict]:
    """포지션 사이즈 별 — 작은 / 중간 / 큰 트레이드 분류."""
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT fill_price * size_shares as size_usd, pnl FROM trades "
        "WHERE timestamp >= ? AND pnl IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    buckets = {
        "small (< $10)": (0, 10),
        "medium ($10-50)": (10, 50),
        "large ($50-200)": (50, 200),
        "xlarge (> $200)": (200, float("inf")),
    }
    out = {}
    for name, (lo, hi) in buckets.items():
        pnls = [r[1] for r in rows if r[0] is not None and lo <= r[0] < hi]
        out[name] = {
            "pnl": sum(pnls),
            "n_trades": len(pnls),
            "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
            "win_rate": sum(1 for p in pnls if p > 0) / len(pnls) if pnls else 0,
        }
    return out


def attribution_by_hour(window_days: float = 30) -> dict[int, dict]:
    """UTC 시간대별 PnL."""
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT timestamp, pnl FROM trades WHERE timestamp >= ? AND pnl IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    by_hour = defaultdict(lambda: {"pnl": 0, "n": 0})
    for ts, pnl in rows:
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        by_hour[h]["pnl"] += pnl or 0
        by_hour[h]["n"] += 1

    return {str(h): dict(v) for h, v in sorted(by_hour.items())}


def full_attribution_report(window_days: float = 30) -> dict:
    """대시보드용 전체 보고서."""
    return {
        "window_days": window_days,
        "by_strategy": attribution_by_strategy(window_days),
        "by_category": attribution_by_category(window_days),
        "by_size_bucket": attribution_by_size_bucket(window_days),
        "by_hour_utc": attribution_by_hour(window_days),
    }
