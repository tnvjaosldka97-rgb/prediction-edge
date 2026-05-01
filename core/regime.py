"""
매크로 regime 감지 — bull/bear/sideways/volatile.

Polymarket 자체는 BTC/주식처럼 trending 안 하지만,
우리 portfolio_snapshots 변동성·방향성으로 우리 봇의 regime 분류.

기준 (30일 윈도우):
- 변동성 < 5% + 양수 추세 → bull (calm uptrend)
- 변동성 > 15% + 양수 → volatile_up (위험한 상승)
- 변동성 < 5% + 음수 → bear (calm decline)
- 변동성 > 15% + 음수 → crisis (위험한 하락)
- 추세 평탄 → sideways

regime별 자동 전략 가중치 조정:
- bull: closing_convergence + claude_oracle 강화
- bear: 모든 전략 50% 사이즈 축소
- volatile_up: maker 우선, taker 줄이기
- crisis: killswitch trip + 비상정지 권고
- sideways: 균형 유지
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Literal

import config


Regime = Literal["bull", "bear", "sideways", "volatile_up", "crisis", "unknown"]


@dataclass
class RegimeState:
    regime: Regime
    volatility_pct: float
    trend_pct: float
    n_observations: int
    confidence: float
    recommended_size_multiplier: float    # 1.0 = normal
    notes: str = ""


def detect_regime(window_days: float = 30) -> RegimeState:
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT timestamp, total_value FROM portfolio_snapshots "
        "WHERE timestamp >= ? ORDER BY timestamp ASC",
        (since_ts,)
    ).fetchall()
    conn.close()

    if len(rows) < 10:
        return RegimeState(
            regime="unknown",
            volatility_pct=0,
            trend_pct=0,
            n_observations=len(rows),
            confidence=0,
            recommended_size_multiplier=0.5,    # 데이터 부족 = 보수적
            notes="insufficient data",
        )

    values = [r[1] for r in rows]
    rets = [(values[i] - values[i - 1]) / values[i - 1]
            for i in range(1, len(values)) if values[i - 1] > 0]

    if not rets:
        return RegimeState("unknown", 0, 0, 0, 0, 0.5, "no returns")

    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    std = math.sqrt(var)

    # 연환산 변동성
    avg_interval_sec = (rows[-1][0] - rows[0][0]) / max(1, len(rows) - 1)
    n_per_year = 365 * 86400 / max(1, avg_interval_sec)
    vol_annual = std * math.sqrt(n_per_year) * 100

    # 추세 — start to end
    trend = (values[-1] - values[0]) / values[0] * 100

    # Regime classification
    if vol_annual > 50:
        if trend < -5:
            regime = "crisis"
            mult = 0.0    # 거의 정지
            notes = "변동성 + 하락 — killswitch 권고"
        elif trend > 10:
            regime = "volatile_up"
            mult = 0.5    # 사이즈 절반
            notes = "변동성 큰 상승 — maker 우선"
        else:
            regime = "sideways"
            mult = 0.7
            notes = "큰 변동성, 방향성 X"
    elif vol_annual > 15:
        if trend > 5:
            regime = "volatile_up"
            mult = 0.7
            notes = "중변동 상승"
        elif trend < -5:
            regime = "bear"
            mult = 0.5
            notes = "하락 + 변동"
        else:
            regime = "sideways"
            mult = 1.0
    else:
        if trend > 3:
            regime = "bull"
            mult = 1.2    # 좋은 환경 → 약간 더 공격적
            notes = "낮은 변동성 + 상승"
        elif trend < -3:
            regime = "bear"
            mult = 0.7
            notes = "낮은 변동성 + 하락"
        else:
            regime = "sideways"
            mult = 1.0

    confidence = min(1.0, len(rets) / 100)    # 100표본 이상 = 1.0

    return RegimeState(
        regime=regime,
        volatility_pct=vol_annual,
        trend_pct=trend,
        n_observations=len(rows),
        confidence=confidence,
        recommended_size_multiplier=mult,
        notes=notes,
    )


async def regime_loop(interval_sec: int = 21600):
    """6시간마다 regime 평가 + runtime_state에 반영."""
    import asyncio
    import json
    from pathlib import Path
    from core.logger import log

    state_file = Path(__file__).resolve().parent.parent / "runtime_state.json"
    last_regime = None

    while True:
        try:
            await asyncio.sleep(interval_sec)
            state = detect_regime(window_days=30)

            # runtime_state.json에 저장 (sizing이 읽음)
            try:
                if state_file.exists():
                    rstate = json.loads(state_file.read_text(encoding="utf-8"))
                else:
                    rstate = {}
                rstate["regime"] = state.regime
                rstate["regime_size_multiplier"] = state.recommended_size_multiplier
                rstate["regime_volatility_pct"] = state.volatility_pct
                rstate["regime_trend_pct"] = state.trend_pct
                rstate["regime_updated_at"] = time.time()
                state_file.write_text(json.dumps(rstate, indent=2, ensure_ascii=False), encoding="utf-8")
            except Exception:
                pass

            # regime 변경 시 알림
            if state.regime != last_regime:
                last_regime = state.regime
                log.info(f"[regime] {state.regime} | vol {state.volatility_pct:.1f}% trend {state.trend_pct:+.1f}% mult={state.recommended_size_multiplier}")
                try:
                    from notifications.telegram import notify
                    level = "CRITICAL" if state.regime == "crisis" else "INFO"
                    notify(level, f"Regime 변경: {state.regime}", {
                        "volatility": f"{state.volatility_pct:.1f}%",
                        "trend": f"{state.trend_pct:+.1f}%",
                        "size_multiplier": state.recommended_size_multiplier,
                        "notes": state.notes,
                    })
                except Exception:
                    pass
                if state.regime == "crisis":
                    try:
                        from risk import killswitch
                        killswitch.trip("regime_crisis_detected",
                                          vol=state.volatility_pct, trend=state.trend_pct)
                    except Exception:
                        pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[regime] {e}")
            await asyncio.sleep(3600)
