"""
Dashboard 컨트롤 엔드포인트 — 모드 토글, 비상정지, 전략 ON/OFF.

State machine:
  DRY_RUN     : 시뮬만, 실거래 X (기본)
  SHADOW      : 페이퍼 트레이딩 (현재 라이브 페이퍼와 동일)
  LIVE_PILOT  : 실거래 + 자본 cap 적용 + 손실 -X% 자동 정지
  LIVE_FULL   : 실거래 본격 (캘리브레이션 검증 후)

운영 상태는 runtime_state.json 파일에 저장 → main.py·gateway가 읽어 분기.
변경 시 audit_log에 기록.
"""
from __future__ import annotations
import json
import os
import time
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import db
from dashboard import auth
from risk import killswitch


ROOT = Path(__file__).resolve().parent.parent
# Railway 영구 볼륨 (/data) 사용. 로컬 dev는 ROOT 사용.
_DATA_DIR = Path("/data") if Path("/data").exists() else ROOT
STATE_FILE = _DATA_DIR / "runtime_state.json"


Mode = Literal["DRY_RUN", "SHADOW", "LIVE_PILOT", "LIVE_FULL"]
DEFAULT_STATE = {
    "mode": "DRY_RUN",
    "bankroll_cap_usd": 50.0,
    "max_loss_usd": 20.0,
    "strategies_enabled": {
        "closing_convergence": True,
        "claude_oracle": False,
        "fee_arbitrage": False,
        "correlated_arb": False,
        "cross_platform": False,
        "limitless_arb": False,
        "base_rate": False,
        "exit_signal": True,
    },
    "last_modified": 0.0,
    "modified_by": "init",
}


def load_state() -> dict:
    if not STATE_FILE.exists():
        save_state(DEFAULT_STATE)
        return dict(DEFAULT_STATE)
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_STATE)


def save_state(state: dict) -> None:
    state["last_modified"] = time.time()
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


# ── Request/response schemas ────────────────────────────────────────────────

class LoginRequest(BaseModel):
    password: str


class ModeChangeRequest(BaseModel):
    mode: Mode
    confirm_token: Optional[str] = None  # LIVE 토글 시 추가 확인 (LIVE_PILOT/FULL은 "PROMOTE" 입력 요구)


class BankrollCapRequest(BaseModel):
    bankroll_cap_usd: float = Field(gt=0, le=10_000)


class StrategyToggleRequest(BaseModel):
    strategy: str
    enabled: bool


# ── Router ───────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/control", tags=["control"])
auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.post("/login")
async def auth_login(req: LoginRequest, request: Request, response: Response):
    ip = request.client.host if request.client else ""
    ua = request.headers.get("user-agent", "")[:200]
    if not auth.is_ip_allowed(ip):
        raise HTTPException(403, f"IP {ip} not in allowlist")
    token = auth.login(req.password, ip=ip, ua=ua)
    if not token:
        raise HTTPException(401, "Invalid credentials")
    response.set_cookie(
        key="admin_session", value=token,
        httponly=True, samesite="strict", secure=False,  # secure=True in production HTTPS
        max_age=86400,
    )
    return {"ok": True, "expires_in_sec": 86400}


@auth_router.post("/logout")
async def auth_logout(response: Response, sess: dict = Depends(auth.require_admin), request: Request = None):
    ip = request.client.host if request and request.client else ""
    auth.logout("admin", ip)
    response.delete_cookie("admin_session")
    return {"ok": True}


@router.get("/state")
async def get_state(_: dict = Depends(auth.require_admin)):
    return load_state()


@router.post("/mode")
async def change_mode(
    req: ModeChangeRequest,
    request: Request,
    sess: dict = Depends(auth.require_recent_auth),  # LIVE 변경은 최근 인증 필요
):
    state = load_state()
    before = dict(state)

    # LIVE 진입 추가 확인
    if req.mode in ("LIVE_PILOT", "LIVE_FULL"):
        if req.confirm_token != "PROMOTE":
            raise HTTPException(400, 'LIVE 진입은 confirm_token="PROMOTE" 필수')

    # 보호 모드 체크 — LIVE_FULL 진입 첫 30일 차단
    try:
        from risk.protection_mode import can_change_mode, activate_protection
        ok, reason = can_change_mode(req.mode)
        if not ok:
            raise HTTPException(403, reason)
        # LIVE_PILOT 첫 진입 시 보호 모드 자동 활성화 (30일)
        if req.mode == "LIVE_PILOT" and before.get("mode") == "DRY_RUN":
            activate_protection()
    except ImportError:
        pass

    state["mode"] = req.mode
    state["modified_by"] = "admin"
    save_state(state)

    ip = request.client.host if request.client else ""
    db.insert_audit_log("admin", "mode_change", before, state, ip, "")

    # 모드 전환 알림 — LIVE 진입은 CRITICAL, 그 외 INFO
    try:
        from notifications.telegram import notify_async
        level = "CRITICAL" if req.mode in ("LIVE_PILOT", "LIVE_FULL") else "INFO"
        await notify_async(level, f"모드 전환: {before.get('mode')} → {req.mode}", {
            "actor": "admin",
            "ip": ip,
            "bankroll_cap": state.get("bankroll_cap_usd"),
        })
    except Exception:
        pass
    # WebSocket 즉시 브로드캐스트
    try:
        from dashboard.realtime import broadcast_event
        broadcast_event("mode_change", {"from": before.get("mode"), "to": req.mode})
    except Exception:
        pass

    return {"ok": True, "state": state}


@router.post("/bankroll_cap")
async def change_bankroll_cap(
    req: BankrollCapRequest,
    request: Request,
    sess: dict = Depends(auth.require_admin),
):
    state = load_state()
    before = dict(state)

    # 보호 모드 체크 — 첫 30일은 즉시 변경 X
    try:
        from risk.protection_mode import can_change_bankroll_cap, log_override_attempt
        ok, reason = can_change_bankroll_cap()
        if not ok:
            # 즉시 변경 X — 24h cooling-off 큐에 등록
            state.setdefault("pending_bankroll_changes", []).append({
                "requested_at": time.time(),
                "apply_at": time.time() + 86400,
                "new_value": req.bankroll_cap_usd,
                "current_value": state.get("bankroll_cap_usd"),
            })
            save_state(state)
            log_override_attempt("bankroll_cap_change", reason)
            ip = request.client.host if request.client else ""
            db.insert_audit_log("admin", "bankroll_cap_change_pending", before, state, ip, "")
            return {
                "ok": False,
                "queued": True,
                "reason": reason,
                "applies_at": time.time() + 86400,
                "message": "변경 요청 큐에 등록 — 24시간 후 자동 적용. 즉시 적용 불가.",
            }
    except ImportError:
        pass

    state["bankroll_cap_usd"] = req.bankroll_cap_usd
    state["modified_by"] = "admin"
    save_state(state)
    ip = request.client.host if request.client else ""
    db.insert_audit_log("admin", "bankroll_cap_change", before, state, ip, "")
    return {"ok": True, "state": state}


@router.post("/protection_unlock")
async def protection_unlock(
    request: Request,
    sess: dict = Depends(auth.require_recent_auth),
):
    """보호 모드 강제 해제 — confirmation 'I_UNDERSTAND_THE_RISK' 필요."""
    body = await request.json()
    confirmation = body.get("confirmation", "")
    try:
        from risk.protection_mode import force_unlock
        ok = force_unlock(confirmation)
        if ok:
            ip = request.client.host if request.client else ""
            db.insert_audit_log("admin", "protection_force_unlock", None, {}, ip, "")
            return {"ok": True, "message": "보호 모드 해제됨"}
        return {"ok": False, "message": "confirmation 'I_UNDERSTAND_THE_RISK' 입력 필요"}
    except ImportError:
        return {"ok": False, "message": "protection_mode 모듈 없음"}


@router.post("/strategy_toggle")
async def toggle_strategy(
    req: StrategyToggleRequest,
    request: Request,
    sess: dict = Depends(auth.require_admin),
):
    state = load_state()
    before = dict(state)
    state["strategies_enabled"][req.strategy] = req.enabled
    state["modified_by"] = "admin"
    save_state(state)
    ip = request.client.host if request.client else ""
    db.insert_audit_log("admin", "strategy_toggle", before, state, ip, "")
    return {"ok": True, "state": state}


@router.post("/emergency_stop")
async def emergency_stop(
    request: Request,
    sess: dict = Depends(auth.require_admin),
):
    """1클릭 비상정지 — 모드를 DRY_RUN으로 + killswitch trip + 모든 미체결 주문 취소 (시도)."""
    state = load_state()
    before = dict(state)

    state["mode"] = "DRY_RUN"
    state["modified_by"] = "admin_emergency"
    save_state(state)

    # killswitch 강제 트립 — 재시작에도 유지
    try:
        killswitch.trip("manual_emergency_stop_from_dashboard")
    except Exception as e:
        # killswitch 없어도 모드는 바뀜
        pass

    ip = request.client.host if request.client else ""
    db.insert_audit_log("admin", "emergency_stop", before, state, ip, "")

    # 텔레그램 알림 — 비상정지는 무조건 CRITICAL
    try:
        from notifications.telegram import notify_async
        await notify_async("CRITICAL", "비상정지 발동", {
            "trigger": "dashboard manual",
            "previous_mode": before.get("mode"),
            "ip": ip,
        })
    except Exception:
        pass

    return {"ok": True, "state": state, "killswitch_tripped": True}


@router.get("/audit_log")
async def get_audit(_: dict = Depends(auth.require_admin), limit: int = 100):
    return {"entries": db.get_audit_log(limit=limit)}


@router.post("/calibrate")
async def trigger_calibration(_: dict = Depends(auth.require_admin)):
    """수동 캘리브레이션 트리거."""
    from friction.calibrate import calibrate
    from friction.orchestrator import FrictionOrchestrator
    orch = FrictionOrchestrator()
    report = calibrate(orch, min_traces=10)  # 수동은 임계 낮춰
    return {
        "ok": True,
        "report": {
            "n_traces": report.n_traces_used,
            "n_filled": report.n_filled,
            "n_rejected": report.n_rejected,
            "rejection_breakdown": report.rejection_breakdown,
            "latency_mu_ms": report.latency_mu_ms,
            "saved": report.saved,
            "skip_reason": report.skip_reason,
        },
    }
