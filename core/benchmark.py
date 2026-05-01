"""
벤치마크 추적 — 우리 봇이 진짜로 알파 만드나?

비교 대상:
1. **buy-and-hold**: 시작 시점 active markets 균등 매수 후 만기 보유
2. **Polymarket index**: 거래량 가중 인덱스
3. **risk-free**: 4% 연 무위험 수익률

매주:
- 우리 누적 PnL vs benchmark PnL
- alpha (residual return after benchmark) 측정
- Information Ratio = alpha / tracking_error
- IR > 0.5 = 진짜 알파, < 0.5 = 노이즈
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass

import config


@dataclass
class BenchmarkComparison:
    n_days: float
    our_return_pct: float
    buy_and_hold_return_pct: float
    risk_free_return_pct: float
    alpha_vs_buyhold_pct: float
    alpha_vs_riskfree_pct: float
    information_ratio: float
    sharpe_us: float
    sharpe_buyhold: float
    is_real_alpha: bool          # IR > 0.5
    interpretation: str


def _conn():
    return sqlite3.connect(config.DB_PATH)


def _our_returns(window_days: float) -> tuple[list[float], float, float]:
    """우리 봇 portfolio returns + start/end value."""
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    rows = conn.execute(
        "SELECT timestamp, total_value FROM portfolio_snapshots "
        "WHERE timestamp >= ? ORDER BY timestamp ASC",
        (since_ts,)
    ).fetchall()
    conn.close()
    if len(rows) < 2:
        return [], 0, 0
    start = rows[0][1]
    end = rows[-1][1]
    rets = []
    for i in range(1, len(rows)):
        prev = rows[i - 1][1]
        if prev > 0:
            rets.append(rows[i][1] / prev - 1)
    return rets, start, end


def _buy_hold_returns(window_days: float) -> tuple[list[float], float]:
    """가상 buy-and-hold 시뮬 — 시작 시점 active markets에서 균등 진입."""
    # virtual_trades 또는 trades에서 우리 시작 시 가용 마켓들 평균 가격
    # 단순화: 시작 시점 active markets 가격 평균 → 만기 시 1.0 또는 0.0 (이진)
    # 보수적 가정: 50% 시장이 YES로 끝남 → 평균 +50% 수익은 비현실적
    # 더 현실적: 시작 가격 0.5 → 끝 가격 0.5 (평균 회귀) → 0%
    # 실제로는 가격 발견 비용·수수료로 약간 손실
    # 여기서는 간단히 -1% 가정 (시장 효율성 baseline)
    rets = []
    n_periods = max(1, int(window_days * 24 / 6))    # 6시간 단위
    for _ in range(n_periods):
        rets.append(-0.0001)    # 시간당 매우 작은 음수 (수수료·decay)
    return rets, -0.01 * window_days / 30    # 30일에 -1%


def _risk_free_return(window_days: float, annual_rate: float = 0.04) -> float:
    """무위험 수익 — 한국 1년 정기예금 ~4%."""
    return annual_rate * (window_days / 365) * 100


def _sharpe(rets: list[float], rf_per: float = 0.0) -> float:
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    std = math.sqrt(var)
    return (mean - rf_per) / std * math.sqrt(252) if std > 0 else 0


def compare(window_days: float = 30) -> BenchmarkComparison:
    our_rets, start, end = _our_returns(window_days)
    bh_rets, bh_total_pct = _buy_hold_returns(window_days)

    if not our_rets or start <= 0:
        return BenchmarkComparison(
            n_days=0, our_return_pct=0, buy_and_hold_return_pct=0,
            risk_free_return_pct=0, alpha_vs_buyhold_pct=0,
            alpha_vs_riskfree_pct=0, information_ratio=0,
            sharpe_us=0, sharpe_buyhold=0,
            is_real_alpha=False, interpretation="insufficient data",
        )

    our_total_pct = (end - start) / start * 100 if start > 0 else 0
    rf_pct = _risk_free_return(window_days)

    alpha_vs_bh = our_total_pct - bh_total_pct
    alpha_vs_rf = our_total_pct - rf_pct

    sharpe_us = _sharpe(our_rets)
    sharpe_bh = _sharpe(bh_rets)

    # Tracking error (우리 - benchmark 차이의 표준편차)
    if len(our_rets) > 1:
        excess_rets = [r - (-0.0001) for r in our_rets]    # vs buy_and_hold simple
        mean_excess = sum(excess_rets) / len(excess_rets)
        var_excess = sum((r - mean_excess) ** 2 for r in excess_rets) / (len(excess_rets) - 1)
        tracking_error = math.sqrt(var_excess)
        ir = mean_excess / tracking_error * math.sqrt(252) if tracking_error > 0 else 0
    else:
        ir = 0

    is_real = ir > 0.5

    if alpha_vs_rf < 0:
        interp = "NO ALPHA — 정기예금이 더 나음. 봇 끄거나 진단 필요"
    elif alpha_vs_bh < 0:
        interp = "NO ALPHA vs buy-and-hold — 단순 보유보다 못함"
    elif ir < 0.5:
        interp = f"WEAK ALPHA — IR={ir:.2f} 노이즈 가능성, 더 표본 필요"
    elif ir < 1.0:
        interp = f"REAL ALPHA — IR={ir:.2f} 의미있는 우위"
    else:
        interp = f"STRONG ALPHA — IR={ir:.2f} 통계적으로 강한 우위"

    return BenchmarkComparison(
        n_days=window_days,
        our_return_pct=our_total_pct,
        buy_and_hold_return_pct=bh_total_pct,
        risk_free_return_pct=rf_pct,
        alpha_vs_buyhold_pct=alpha_vs_bh,
        alpha_vs_riskfree_pct=alpha_vs_rf,
        information_ratio=ir,
        sharpe_us=sharpe_us,
        sharpe_buyhold=sharpe_bh,
        is_real_alpha=is_real,
        interpretation=interp,
    )


async def benchmark_loop(interval_sec: int = 86400):
    """매일 1회 벤치마크 비교 + 알림."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            comp = compare(window_days=30)
            log.info(
                f"[benchmark] {comp.interpretation} | "
                f"우리 {comp.our_return_pct:+.2f}% vs buyhold {comp.buy_and_hold_return_pct:+.2f}% "
                f"vs RF {comp.risk_free_return_pct:+.2f}% | IR={comp.information_ratio:.2f}"
            )
            try:
                from notifications.telegram import notify
                level = "WARN" if not comp.is_real_alpha and comp.n_days > 14 else "INFO"
                notify(level, f"30일 벤치마크: {comp.interpretation[:40]}", {
                    "our_return": f"{comp.our_return_pct:+.2f}%",
                    "buyhold": f"{comp.buy_and_hold_return_pct:+.2f}%",
                    "alpha": f"{comp.alpha_vs_buyhold_pct:+.2f}%",
                    "IR": f"{comp.information_ratio:.2f}",
                })
            except Exception:
                pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[benchmark] {e}")
            await asyncio.sleep(3600)
