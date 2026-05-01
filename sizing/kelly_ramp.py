"""
Kelly fraction ramp-up — 자동 점진 증액.

원리:
- 시작: Kelly의 10% (매우 보수적)
- N개 trade 안전 통과 시: 20%, 30%, ... 100% 까지 증액
- 손실 발생 시: 즉시 50% 축소
- 음수 영역 진입 시: 10%로 리셋

이 방식이 retail 봇의 가장 흔한 망함 패턴 — "잘 되니까 자본 10배" — 방어.

실제 Kelly 함수에 호출되어 multiplier 적용.
"""
from __future__ import annotations
import json
import math
import sqlite3
import time
from pathlib import Path

import config


_STATE_FILE = Path(__file__).resolve().parent.parent / "kelly_ramp_state.json"

# Ramp-up schedule — n_safe_trades → multiplier
SCHEDULE = [
    (0, 0.10),       # 시작
    (10, 0.20),
    (30, 0.30),
    (60, 0.50),
    (100, 0.70),
    (200, 0.85),
    (500, 1.00),
]


def _load_state() -> dict:
    if not _STATE_FILE.exists():
        return {"n_safe_trades": 0, "n_loss_trades": 0, "current_multiplier": 0.10, "last_loss_ts": 0}
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"n_safe_trades": 0, "n_loss_trades": 0, "current_multiplier": 0.10, "last_loss_ts": 0}


def _save_state(state: dict) -> None:
    _STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _multiplier_from_safe_count(n_safe: int) -> float:
    for n, m in reversed(SCHEDULE):
        if n_safe >= n:
            return m
    return 0.10


def get_current_multiplier() -> float:
    """현재 Kelly multiplier — main.py / sizing/kelly.py가 호출."""
    state = _load_state()
    return state.get("current_multiplier", 0.10)


def record_trade_result(pnl_usd: float) -> dict:
    """매 거래 종료 시 호출. PnL 기반으로 ramp 갱신."""
    state = _load_state()
    if pnl_usd > 0:
        state["n_safe_trades"] += 1
        # 손실 후 회복은 5건 안전 거래 후 ramp 재개
        if state.get("n_loss_trades", 0) >= 3:
            state["n_loss_trades"] = max(0, state["n_loss_trades"] - 1)
        new_mult = _multiplier_from_safe_count(state["n_safe_trades"])
        state["current_multiplier"] = new_mult
    else:
        # 손실 — 즉시 50% 축소
        state["n_loss_trades"] = state.get("n_loss_trades", 0) + 1
        state["n_safe_trades"] = max(0, state["n_safe_trades"] - 5)    # safe count 5 깎음
        state["current_multiplier"] = max(0.10, state["current_multiplier"] * 0.5)
        state["last_loss_ts"] = time.time()
        # 연속 5 손실 시 risk reset
        if state["n_loss_trades"] >= 5:
            state["current_multiplier"] = 0.10
            state["n_safe_trades"] = 0

    _save_state(state)
    return state


def reset() -> None:
    """수동 리셋 — 비상정지 후 사용."""
    _save_state({
        "n_safe_trades": 0, "n_loss_trades": 0,
        "current_multiplier": 0.10, "last_loss_ts": 0,
    })


def status() -> dict:
    return _load_state()
