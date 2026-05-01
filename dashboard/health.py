"""
Health check endpoint + Watchdog.

/health: Railway probe + uptime 모니터링 (인증 X, 공개)
- 200 OK: 봇 정상 가동
- 503: 한 가지 이상 컴포넌트 죽음

Watchdog: 5분간 시그널·snapshot 없으면 텔레그램 CRITICAL.
"""
from __future__ import annotations
import asyncio
import sqlite3
import time
from typing import Optional

from fastapi import APIRouter, Response

import config


router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    """간단한 살아있음 체크. Railway probe + 외부 모니터링용."""
    issues = []
    health_data = {"status": "ok", "ts": time.time()}

    # DB 살아있는지
    try:
        conn = sqlite3.connect(config.DB_PATH, timeout=2.0)
        conn.execute("SELECT 1").fetchone()
        conn.close()
        health_data["db"] = "ok"
    except Exception as e:
        health_data["db"] = f"error: {str(e)[:60]}"
        issues.append("db")

    # 최근 5분 내 portfolio_snapshot 있는지 (봇 살아있는 지표)
    try:
        conn = sqlite3.connect(config.DB_PATH, timeout=2.0)
        row = conn.execute(
            "SELECT timestamp, total_value FROM portfolio_snapshots ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            age = time.time() - row[0]
            health_data["last_snapshot_age_sec"] = age
            health_data["last_total_value"] = row[1]
            if age > 600:    # 10분 이상 → 봇 stuck 의심
                issues.append("stale_snapshot")
        else:
            health_data["last_snapshot_age_sec"] = None
            issues.append("no_snapshot")
    except Exception as e:
        issues.append(f"snapshot_query: {e}")

    # 모드 표시
    try:
        from pathlib import Path
        import json
        _root = Path(__file__).resolve().parent.parent
        state_file = (Path("/data") if Path("/data").exists() else _root) / "runtime_state.json"
        if state_file.exists():
            state = json.loads(state_file.read_text(encoding="utf-8"))
            health_data["mode"] = state.get("mode", "unknown")
            health_data["bankroll_cap_usd"] = state.get("bankroll_cap_usd")
    except Exception:
        pass

    if issues:
        health_data["status"] = "degraded"
        health_data["issues"] = issues
        return Response(content=str(health_data), status_code=503, media_type="application/json")

    return health_data


# ── Watchdog ─────────────────────────────────────────────────────────────────

class Watchdog:
    """봇 활동 모니터. DB의 portfolio_snapshots/signals 직접 조회 (heartbeat 의존 X).

    이전에는 heartbeat_signal/snapshot 호출에 의존했으나, 호출하는 곳이 없어서
    항상 false alarm 발생 → DB 직접 조회로 변경 (self-correcting).
    """

    def __init__(self):
        self._last_alert_ts = 0

    def heartbeat_signal(self):
        """Backward compat — 더 이상 사용 X (DB 직접 조회)."""
        pass

    def heartbeat_snapshot(self):
        """Backward compat — 더 이상 사용 X (DB 직접 조회)."""
        pass

    def is_stuck(self) -> tuple[bool, str]:
        now = time.time()
        try:
            conn = sqlite3.connect(config.DB_PATH, timeout=2.0)
            snap_row = conn.execute(
                "SELECT MAX(timestamp) FROM portfolio_snapshots"
            ).fetchone()
            sig_row = conn.execute(
                "SELECT MAX(created_at) FROM signals"
            ).fetchone()
            conn.close()
        except Exception:
            return False, ""    # DB 접근 자체가 실패하면 다른 모니터가 잡음

        snap_ts = snap_row[0] if snap_row else None
        sig_ts = sig_row[0] if sig_row else None

        if snap_ts:
            snap_age = now - snap_ts
            if snap_age > 600:    # 10분
                return True, f"snapshot_stale_{int(snap_age)}s"

        # signal stale은 더 관대 (시그널 조건 안 맞으면 자연스럽게 안 생성)
        if sig_ts:
            sig_age = now - sig_ts
            if sig_age > 7200:    # 2시간 — 더 보수적
                return True, f"signal_stale_{int(sig_age)}s"

        return False, ""


_watchdog: Optional[Watchdog] = None


def get_watchdog() -> Watchdog:
    global _watchdog
    if _watchdog is None:
        _watchdog = Watchdog()
    return _watchdog


async def watchdog_loop(interval_sec: int = 60):
    from core.logger import log
    wd = get_watchdog()
    while True:
        try:
            await asyncio.sleep(interval_sec)
            stuck, reason = wd.is_stuck()
            if stuck and time.time() - wd._last_alert_ts > 1800:
                log.error(f"[watchdog] BOT STUCK: {reason}")
                wd._last_alert_ts = time.time()
                try:
                    from notifications.telegram import notify
                    notify("CRITICAL", "봇 stuck 감지", {
                        "reason": reason,
                        "action": "Railway 재시작 또는 로그 확인 필요",
                    })
                except Exception:
                    pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[watchdog] {e}")
            await asyncio.sleep(60)
