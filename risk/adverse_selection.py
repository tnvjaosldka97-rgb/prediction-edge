"""
Adverse Selection Monitor.

우리 주문 체결 후 N초/분 가격이 우리한테 불리한 방향으로 계속 가면
"우린 정보가 부족한 측에 있음" — 즉 더 똑똑한 사람이 우리 주문 받아간 것.

지표:
- markout_5s: 체결 후 5초 mid - 체결가
- markout_60s: 체결 후 60초 mid - 체결가
- markout_300s: 체결 후 5분 mid - 체결가

BUY: markout 음수 (가격 떨어짐) = 적자 = 우리가 비싸게 산 것
SELL: markout 양수 (가격 올라감) = 적자 = 우리가 싸게 판 것

평균 markout이 일관되게 음수 → 우리 시그널이 노이즈 또는 후행 정보.
"""
from __future__ import annotations
import asyncio
import math
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass

import config


@dataclass
class MarkoutStats:
    strategy: str
    n_trades: int
    avg_markout_5s_pct: float
    avg_markout_60s_pct: float
    avg_markout_300s_pct: float
    median_markout_60s_pct: float
    win_rate_at_60s: float       # markout이 우리한테 유리한 비율
    severity: str                # "ok" / "warn" / "critical"


def _signed_markout(side: str, fill_price: float, future_mid: float) -> float:
    """우리한테 유리한 방향이면 양수, 불리하면 음수 (% 단위)."""
    if fill_price <= 0:
        return 0
    raw = (future_mid - fill_price) / fill_price
    return raw if side == "BUY" else -raw


def compute_markout_stats(strategy: str = None, window_days: float = 7) -> MarkoutStats:
    """virtual_trades + price_history에서 markout 계산.

    DRY_RUN의 virtual_trades는 mid_after_5s/60s/300s 컬럼을 drift_tracker가 채움.
    """
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    if strategy:
        rows = conn.execute(
            "SELECT side, fill_price, mid_after_5s, mid_after_60s, mid_after_300s "
            "FROM virtual_trades WHERE fill_ts >= ? AND strategy = ? "
            "AND mid_after_60s IS NOT NULL",
            (since_ts, strategy)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT side, fill_price, mid_after_5s, mid_after_60s, mid_after_300s "
            "FROM virtual_trades WHERE fill_ts >= ? AND mid_after_60s IS NOT NULL",
            (since_ts,)
        ).fetchall()
    conn.close()

    if not rows:
        return MarkoutStats(strategy or "all", 0, 0, 0, 0, 0, 0, "ok")

    m5 = []
    m60 = []
    m300 = []
    for side, fp, m5_v, m60_v, m300_v in rows:
        if m5_v is not None:
            m5.append(_signed_markout(side, fp, m5_v))
        if m60_v is not None:
            m60.append(_signed_markout(side, fp, m60_v))
        if m300_v is not None:
            m300.append(_signed_markout(side, fp, m300_v))

    avg5 = sum(m5) / len(m5) * 100 if m5 else 0
    avg60 = sum(m60) / len(m60) * 100 if m60 else 0
    avg300 = sum(m300) / len(m300) * 100 if m300 else 0
    median60 = sorted(m60)[len(m60) // 2] * 100 if m60 else 0
    win_rate = sum(1 for x in m60 if x > 0) / len(m60) if m60 else 0

    # Severity 판정
    severity = "ok"
    if avg60 < -2.0:    # -2% adverse
        severity = "critical"
    elif avg60 < -0.5:
        severity = "warn"

    return MarkoutStats(
        strategy=strategy or "all",
        n_trades=len(rows),
        avg_markout_5s_pct=avg5,
        avg_markout_60s_pct=avg60,
        avg_markout_300s_pct=avg300,
        median_markout_60s_pct=median60,
        win_rate_at_60s=win_rate,
        severity=severity,
    )


def evaluate_all_strategies() -> list[MarkoutStats]:
    """모든 전략 평가."""
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT strategy FROM virtual_trades "
        "WHERE strategy IS NOT NULL AND strategy != ''"
    ).fetchall()
    conn.close()
    return [compute_markout_stats(r[0]) for r in rows if r[0]]


async def adverse_selection_loop(interval_sec: int = 21600):
    """6시간마다 평가."""
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            stats = evaluate_all_strategies()
            critical = [s for s in stats if s.severity == "critical" and s.n_trades >= 10]
            if critical:
                log.warning(f"[adverse_selection] CRITICAL: {[(s.strategy, s.avg_markout_60s_pct) for s in critical]}")
                try:
                    from notifications.telegram import notify_async
                    for s in critical:
                        await notify_async("WARN", f"Adverse selection: {s.strategy}", {
                            "n_trades": s.n_trades,
                            "avg_markout_60s": f"{s.avg_markout_60s_pct:.2f}%",
                            "win_rate_60s": f"{s.win_rate_at_60s*100:.1f}%",
                            "interpretation": "우리 주문이 픽되고 있음. 시그널 재검토 필요",
                        })
                except Exception:
                    pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[adverse_selection] {e}")
            await asyncio.sleep(3600)
