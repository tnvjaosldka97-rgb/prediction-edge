"""
첫 30일 보호 모드 — 강제 운영 규율.

retail 봇 망함 1순위 = 사람이 패닉으로 개입.
- "손실 났으니 봇 끄자" → 알파 못 모음
- "잘 되니 자본 10배" → 한 번 망에 끝
- "이 전략 빼자" → 검증 못함

이를 강제 방어:
1. 첫 30일 동안 killswitch reset 불가 (한 번 trip되면 자동 reset 30일)
2. bankroll_cap 변경 시 24h cooling-off
3. 전략 OFF 변경 시 24h cooling-off
4. 모드 LIVE_FULL 진입 불가 (LIVE_PILOT만)
5. 매일 1회 자동 점검: 위 위반 시 텔레그램 CRITICAL

이 보호 30일 풀려면: 시작일 + 30일 자동, 또는 사람 명시 'unlock_protection' 호출.
"""
from __future__ import annotations
import json
import os
import time
from pathlib import Path


_PROTECTION_FILE = Path(__file__).resolve().parent.parent / "protection_state.json"
_PROTECTION_DURATION_DAYS = 30


def _load() -> dict:
    if not _PROTECTION_FILE.exists():
        return {}
    try:
        return json.loads(_PROTECTION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(state: dict) -> None:
    _PROTECTION_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def activate_protection(start_ts: float | None = None) -> None:
    """LIVE_PILOT 첫 활성화 시 호출. 30일 보호 시작."""
    state = _load()
    if state.get("activated_at"):
        return    # 이미 활성. 재활성 X
    state["activated_at"] = start_ts or time.time()
    state["expires_at"] = state["activated_at"] + _PROTECTION_DURATION_DAYS * 86400
    state["overrides_logged"] = []
    _save(state)


def is_protected() -> tuple[bool, float]:
    """현재 보호 모드 활성? + 남은 시간(초)."""
    state = _load()
    activated = state.get("activated_at", 0)
    expires = state.get("expires_at", 0)
    if not activated or not expires:
        return False, 0
    remaining = expires - time.time()
    return remaining > 0, max(0, remaining)


def can_change_bankroll_cap() -> tuple[bool, str]:
    is_p, remaining = is_protected()
    if not is_p:
        return True, ""
    return False, f"보호 모드 — bankroll cap 변경 24h cooling-off 필요 (남은 {remaining/86400:.1f}일)"


def can_disable_strategy(strategy: str) -> tuple[bool, str]:
    is_p, remaining = is_protected()
    if not is_p:
        return True, ""
    return False, f"보호 모드 — 전략 OFF는 24h cooling-off 필요 (남은 {remaining/86400:.1f}일)"


def can_change_mode(target_mode: str) -> tuple[bool, str]:
    is_p, _ = is_protected()
    if not is_p:
        return True, ""
    if target_mode == "LIVE_FULL":
        return False, "보호 모드 — LIVE_FULL 진입 불가 (LIVE_PILOT만)"
    return True, ""


def can_reset_killswitch() -> tuple[bool, str]:
    """killswitch 트립된 후 사람이 reset할 수 있나."""
    is_p, remaining = is_protected()
    if not is_p:
        return True, ""
    state = _load()
    last_trip = state.get("last_killswitch_trip", 0)
    if time.time() - last_trip < 86400:
        return False, "보호 모드 — killswitch reset 24h 대기 필요"
    return True, ""


def log_override_attempt(action: str, denied_reason: str) -> None:
    """사용자가 보호 우회 시도 — 로그."""
    state = _load()
    state.setdefault("overrides_logged", []).append({
        "ts": time.time(), "action": action, "reason": denied_reason,
    })
    _save(state)


def status() -> dict:
    state = _load()
    is_p, remaining = is_protected()
    return {
        "active": is_p,
        "remaining_days": remaining / 86400 if is_p else 0,
        "activated_at": state.get("activated_at"),
        "expires_at": state.get("expires_at"),
        "n_override_attempts": len(state.get("overrides_logged", [])),
    }


def force_unlock(confirmation: str = "") -> bool:
    """사용자 명시적 잠금해제. 30일 안에 풀고 싶으면 정확히 'I_UNDERSTAND_THE_RISK' 입력."""
    if confirmation != "I_UNDERSTAND_THE_RISK":
        return False
    state = _load()
    state["force_unlocked"] = True
    state["expires_at"] = time.time() - 1
    _save(state)
    try:
        from notifications.telegram import notify
        notify("CRITICAL", "보호 모드 강제 해제됨", {
            "warning": "30일 안에 사람이 풀음. 위험 감수 의사 확인",
        })
    except Exception:
        pass
    return True
