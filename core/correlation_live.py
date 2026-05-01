"""
동적 전략 상관 행렬.

매시간 전략별 P&L의 상관계수 계산.
0.7+ 상관 → 사실상 같은 전략 → 한쪽 사이즈 축소.
음의 상관 → 자연스러운 헤지 → 둘 다 유지.

기존 sizing/portfolio_optimizer는 정적. 이건 실시간 갱신.
"""
from __future__ import annotations
import math
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass

import config


@dataclass
class CorrelationPair:
    strategy_a: str
    strategy_b: str
    correlation: float
    n_observations: int
    severity: str        # "ok" / "redundant" / "anti_hedge"


def compute_strategy_correlation(window_days: float = 7) -> list[CorrelationPair]:
    """전략 쌍 별 상관."""
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT strategy, timestamp, COALESCE(pnl, 0) FROM trades "
        "WHERE timestamp >= ? AND strategy IS NOT NULL ORDER BY timestamp",
        (since_ts,)
    ).fetchall()
    conn.close()

    # 시간 버킷 (1시간 단위)
    bucket_size = 3600
    bucketed: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for s, ts, pnl in rows:
        b = int(ts // bucket_size)
        bucketed[s][b] += pnl or 0

    strategies = sorted(bucketed.keys())
    if len(strategies) < 2:
        return []

    # 모든 시간 버킷
    all_buckets = sorted({b for v in bucketed.values() for b in v.keys()})
    if len(all_buckets) < 5:
        return []    # 표본 부족

    # 매트릭스
    series = {}
    for s in strategies:
        series[s] = [bucketed[s].get(b, 0.0) for b in all_buckets]

    out = []
    for i, sa in enumerate(strategies):
        for sb in strategies[i + 1:]:
            a, b = series[sa], series[sb]
            n = len(a)
            if n < 5:
                continue
            mean_a = sum(a) / n
            mean_b = sum(b) / n
            cov = sum((a[k] - mean_a) * (b[k] - mean_b) for k in range(n)) / max(1, n - 1)
            var_a = sum((x - mean_a) ** 2 for x in a) / max(1, n - 1)
            var_b = sum((x - mean_b) ** 2 for x in b) / max(1, n - 1)
            denom = math.sqrt(var_a * var_b)
            if denom == 0:
                continue
            corr = cov / denom

            severity = "ok"
            if corr > 0.7:
                severity = "redundant"        # 같은 전략 effectively
            elif corr < -0.5:
                severity = "anti_hedge"       # 자연 헤지

            out.append(CorrelationPair(
                strategy_a=sa, strategy_b=sb,
                correlation=corr, n_observations=n,
                severity=severity,
            ))
    return out


def get_redundant_pairs(window_days: float = 7) -> list[dict]:
    """중복 전략 쌍 — 한쪽 사이즈 축소 권고."""
    pairs = compute_strategy_correlation(window_days)
    return [
        {
            "a": p.strategy_a, "b": p.strategy_b,
            "correlation": p.correlation, "n": p.n_observations,
        }
        for p in pairs
        if p.severity == "redundant"
    ]


async def correlation_loop(interval_sec: int = 3600):
    """매시간 상관 행렬 갱신."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            redundant = get_redundant_pairs()
            if redundant:
                log.warning(f"[correlation] {len(redundant)} redundant pairs: {redundant[:3]}")
                try:
                    from notifications.telegram import notify
                    notify("INFO", f"전략 중복 {len(redundant)}쌍", {
                        "top": f"{redundant[0]['a']} ↔ {redundant[0]['b']} (r={redundant[0]['correlation']:.2f})",
                    })
                except Exception:
                    pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[correlation] {e}")
            await asyncio.sleep(600)
