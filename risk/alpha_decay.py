"""
Alpha Decay Tracker — 전략별 엣지가 시간에 따라 줄어드는지 감지.

원리:
- 새 알파 발견 후 시장이 적응해서 엣지 줄어듦 (디케이)
- 우리 전략별 30일 vs 7일 vs 1일 Sharpe 비교
- 30일 > 7일 > 1일 순으로 작아지면 디케이 진행 중
- 자동 사이즈 축소 또는 비활성화 권고

자동 비활성화는 strategy_disabler.py가 담당. 여기는 monitoring + 알림.
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass

import config


@dataclass
class StrategyDecayReport:
    strategy: str
    sharpe_30d: float
    sharpe_7d: float
    sharpe_1d: float
    decay_score: float        # 양수 = 디케이 진행, 음수 = 향상
    n_trades_30d: int
    recommendation: str       # "scale_down" / "monitor" / "scale_up" / "disable"


def _sharpe(pnls: list[float]) -> float:
    if len(pnls) < 3:
        return 0.0
    mean = sum(pnls) / len(pnls)
    var = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
    std = math.sqrt(var)
    return mean / std if std > 0 else 0.0


def evaluate_decay(strategy: str) -> StrategyDecayReport:
    now = time.time()
    conn = sqlite3.connect(config.DB_PATH)

    rows_30 = conn.execute(
        "SELECT pnl FROM trades WHERE strategy=? AND timestamp >= ? AND pnl IS NOT NULL",
        (strategy, now - 30 * 86400)
    ).fetchall()
    rows_7 = conn.execute(
        "SELECT pnl FROM trades WHERE strategy=? AND timestamp >= ? AND pnl IS NOT NULL",
        (strategy, now - 7 * 86400)
    ).fetchall()
    rows_1 = conn.execute(
        "SELECT pnl FROM trades WHERE strategy=? AND timestamp >= ? AND pnl IS NOT NULL",
        (strategy, now - 86400)
    ).fetchall()
    conn.close()

    pnls_30 = [r[0] for r in rows_30]
    pnls_7 = [r[0] for r in rows_7]
    pnls_1 = [r[0] for r in rows_1]

    s30 = _sharpe(pnls_30)
    s7 = _sharpe(pnls_7)
    s1 = _sharpe(pnls_1)

    # Decay score: (30d sharpe - 7d sharpe) + (7d - 1d), 양수 = 디케이 가속
    decay = max(0, s30 - s7) + max(0, s7 - s1)

    # 권고
    if len(pnls_7) < 5:
        rec = "monitor"        # 데이터 부족
    elif s7 < -0.5:
        rec = "disable"        # 7일에 강한 음수
    elif decay > 1.5 and s1 < 0:
        rec = "disable"        # 빠른 디케이 + 1일 음수
    elif decay > 0.8:
        rec = "scale_down"     # 명확한 디케이
    elif s7 > 1.5 and s1 > s7:
        rec = "scale_up"       # 강한 양수 + 가속
    else:
        rec = "monitor"

    return StrategyDecayReport(
        strategy=strategy,
        sharpe_30d=s30,
        sharpe_7d=s7,
        sharpe_1d=s1,
        decay_score=decay,
        n_trades_30d=len(pnls_30),
        recommendation=rec,
    )


def evaluate_all_strategies() -> list[StrategyDecayReport]:
    """모든 활성 전략 평가."""
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT strategy FROM trades WHERE strategy IS NOT NULL"
    ).fetchall()
    conn.close()

    return [evaluate_decay(r[0]) for r in rows if r[0]]


async def alpha_decay_loop(interval_sec: int = 86400):
    """매일 1회 평가 + 알림."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            reports = evaluate_all_strategies()
            decaying = [r for r in reports if r.recommendation in ("scale_down", "disable")]
            if decaying:
                log.warning(
                    f"[alpha_decay] {len(decaying)} strategies showing decay: "
                    f"{[(r.strategy, r.recommendation) for r in decaying]}"
                )
                try:
                    from notifications.telegram import notify_async
                    for r in decaying:
                        level = "CRITICAL" if r.recommendation == "disable" else "WARN"
                        await notify_async(level, f"알파 디케이: {r.strategy}", {
                            "30d_sharpe": f"{r.sharpe_30d:.2f}",
                            "7d_sharpe": f"{r.sharpe_7d:.2f}",
                            "1d_sharpe": f"{r.sharpe_1d:.2f}",
                            "rec": r.recommendation,
                        })
                except Exception:
                    pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[alpha_decay] {e}")
            await asyncio.sleep(3600)
