"""
Realtime PnL Monitor — 임계 도달 시 즉시 알림.

5분 단위 자산 곡선 변동 감지:
- -3% 단일 5분 → WARN
- -5% 1시간 → WARN
- -10% 일 → CRITICAL + 권장 비상정지
- +5% 단일 5분 → INFO (확인용)

자동 비상정지 트리거 옵션도 있음 (기본 OFF — 사람 판단 우선).
"""
from __future__ import annotations
import asyncio
import time
from collections import deque
from dataclasses import dataclass


@dataclass
class PnLSnapshot:
    ts: float
    total_value: float


@dataclass
class PnLAlert:
    severity: str             # INFO / WARN / CRITICAL
    window: str               # "5min" / "1hour" / "24hour"
    pct_change: float
    abs_change_usd: float
    detail: str


class RealtimePnLMonitor:
    """싱글턴 — 자산 변동 추적."""

    _instance: "RealtimePnLMonitor | None" = None

    def __init__(self):
        self._history: deque[PnLSnapshot] = deque(maxlen=2000)    # ~7일치 5분 단위
        self._last_alerts: dict[str, float] = {}    # alert_key → ts (1시간 dedupe)

    @classmethod
    def get(cls) -> "RealtimePnLMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def record(self, total_value: float):
        self._history.append(PnLSnapshot(time.time(), total_value))

    def _value_at(self, seconds_ago: float) -> float | None:
        if not self._history:
            return None
        target = time.time() - seconds_ago
        for s in reversed(self._history):
            if s.ts <= target:
                return s.total_value
        return self._history[0].total_value

    def _send_alert(self, alert: PnLAlert):
        key = f"{alert.window}_{alert.severity}"
        if time.time() - self._last_alerts.get(key, 0) < 3600:
            return
        self._last_alerts[key] = time.time()
        try:
            from notifications.telegram import notify
            notify(alert.severity, f"PnL {alert.window}: {alert.pct_change:+.1%}", {
                "abs_change": f"${alert.abs_change_usd:+.2f}",
                "detail": alert.detail,
            })
        except Exception:
            pass
        try:
            from dashboard.realtime import broadcast_event
            broadcast_event("pnl_alert", {
                "severity": alert.severity,
                "window": alert.window,
                "pct": alert.pct_change,
            })
        except Exception:
            pass

    def check_alerts(self) -> list[PnLAlert]:
        if len(self._history) < 2:
            return []
        current = self._history[-1].total_value
        alerts = []

        # 5분
        v5 = self._value_at(300)
        if v5 and v5 > 0:
            pct = (current - v5) / v5
            if pct <= -0.03:
                alerts.append(PnLAlert("WARN", "5min", pct, current - v5,
                                        f"5분 안에 {pct*100:.1f}% 하락"))
            elif pct >= 0.05:
                alerts.append(PnLAlert("INFO", "5min", pct, current - v5,
                                        f"5분 안에 {pct*100:+.1f}%"))

        # 1시간
        v1h = self._value_at(3600)
        if v1h and v1h > 0:
            pct = (current - v1h) / v1h
            if pct <= -0.05:
                alerts.append(PnLAlert("WARN", "1hour", pct, current - v1h,
                                        f"1시간 안에 {pct*100:.1f}% 하락"))

        # 24시간
        v24h = self._value_at(86400)
        if v24h and v24h > 0:
            pct = (current - v24h) / v24h
            if pct <= -0.10:
                alerts.append(PnLAlert("CRITICAL", "24hour", pct, current - v24h,
                                        f"24시간 안에 {pct*100:.1f}% 하락 — 비상정지 검토"))

        for a in alerts:
            self._send_alert(a)
        return alerts

    def summary(self) -> dict:
        if not self._history:
            return {}
        current = self._history[-1].total_value
        return {
            "current": current,
            "5min_change": (current - (self._value_at(300) or current)) / max(0.01, self._value_at(300) or 1),
            "1hour_change": (current - (self._value_at(3600) or current)) / max(0.01, self._value_at(3600) or 1),
            "24hour_change": (current - (self._value_at(86400) or current)) / max(0.01, self._value_at(86400) or 1),
            "n_snapshots": len(self._history),
        }


def get_monitor() -> RealtimePnLMonitor:
    return RealtimePnLMonitor.get()


async def pnl_monitor_loop(portfolio_state, interval_sec: int = 300):
    """5분 간격 자동 알림."""
    from core.logger import log
    monitor = get_monitor()
    while True:
        try:
            await asyncio.sleep(interval_sec)
            if portfolio_state:
                monitor.record(portfolio_state.total_value)
                alerts = monitor.check_alerts()
                if alerts:
                    log.warning(f"[realtime_pnl] {len(alerts)} alerts: {[a.severity for a in alerts]}")
        except Exception as e:
            log.warning(f"[realtime_pnl] {e}")
            await asyncio.sleep(60)
