"""
Profit Sweeper — 수익 자동 인출 to cold storage.

핫월렛(MetaMask)에 자본 누적되면 도난·키 노출 위험 ↑.
임계값 초과 분 자동으로 cold wallet으로 송금.

설정 (.env):
  COLD_WALLET_ADDRESS=0x... (별도 콜드 지갑 주소)
  PROFIT_SWEEP_THRESHOLD_USD=200    (이 값 초과 시 sweep)
  PROFIT_SWEEP_KEEP_USD=100         (운영 자본 이만큼 유지)

매시간 체크. 수동 트리거도 가능 (대시보드 버튼).
실거래는 사람 확인 후 (위험한 자동화 방지).
"""
from __future__ import annotations
import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class SweepRequest:
    requested_at: float
    amount_to_sweep_usdc: float
    cold_wallet: str
    reason: str
    approved: bool = False
    executed: bool = False
    tx_hash: Optional[str] = None


_PENDING_SWEEPS: list[SweepRequest] = []


def get_threshold_usd() -> float:
    return float(os.getenv("PROFIT_SWEEP_THRESHOLD_USD", "200"))


def get_keep_usd() -> float:
    return float(os.getenv("PROFIT_SWEEP_KEEP_USD", "100"))


def get_cold_wallet() -> str:
    return os.getenv("COLD_WALLET_ADDRESS", "")


def evaluate_sweep_need(portfolio_state) -> Optional[SweepRequest]:
    """현재 자산 상태에서 sweep 필요한지."""
    if not portfolio_state:
        return None
    cold = get_cold_wallet()
    if not cold:
        return None    # cold wallet 미설정 시 sweep 자동화 안 함

    threshold = get_threshold_usd()
    keep = get_keep_usd()

    cash_usdc = portfolio_state.bankroll
    if cash_usdc <= threshold:
        return None

    sweep_amount = cash_usdc - keep
    if sweep_amount <= 0:
        return None

    return SweepRequest(
        requested_at=time.time(),
        amount_to_sweep_usdc=sweep_amount,
        cold_wallet=cold,
        reason=f"cash ${cash_usdc:.2f} > threshold ${threshold:.0f}, sweeping ${sweep_amount:.2f}",
        approved=False,
    )


def request_sweep(req: SweepRequest) -> int:
    """sweep 요청 등록 → 사용자 확인 대기."""
    _PENDING_SWEEPS.append(req)
    # 텔레그램 알림 — CRITICAL (자금 이동)
    try:
        from notifications.telegram import notify
        notify("CRITICAL", "Profit Sweep 요청", {
            "amount_usdc": f"${req.amount_to_sweep_usdc:.2f}",
            "to": req.cold_wallet[:10] + "...",
            "reason": req.reason,
            "action": "대시보드에서 승인 필요",
        })
    except Exception:
        pass
    try:
        from dashboard.realtime import broadcast_event
        broadcast_event("sweep_requested", {
            "amount": req.amount_to_sweep_usdc,
            "cold_wallet": req.cold_wallet[:10] + "...",
        })
    except Exception:
        pass
    return len(_PENDING_SWEEPS) - 1


def list_pending() -> list[dict]:
    return [
        {
            "id": i,
            "requested_at": r.requested_at,
            "amount_usdc": r.amount_to_sweep_usdc,
            "cold_wallet": r.cold_wallet,
            "reason": r.reason,
            "approved": r.approved,
            "executed": r.executed,
            "tx_hash": r.tx_hash,
        }
        for i, r in enumerate(_PENDING_SWEEPS)
        if not r.executed
    ]


def approve_sweep(sweep_id: int, actor: str = "admin") -> bool:
    if sweep_id < 0 or sweep_id >= len(_PENDING_SWEEPS):
        return False
    req = _PENDING_SWEEPS[sweep_id]
    req.approved = True
    try:
        from core import db
        db.insert_audit_log(actor, "profit_sweep_approved",
                            None, {"amount": req.amount_to_sweep_usdc, "to": req.cold_wallet}, "", "")
    except Exception:
        pass
    return True


async def execute_approved_sweeps(portfolio_state) -> int:
    """승인된 sweep을 실제 송금 — Polymarket withdraw API 사용."""
    from core.logger import log
    executed_count = 0
    for req in _PENDING_SWEEPS:
        if req.approved and not req.executed:
            try:
                # 실제 Polymarket withdrawal — relayer를 통해
                # TODO: py_clob_client withdraw 구현. 현재는 manual log만.
                log.warning(
                    f"[profit_sweeper] APPROVED sweep ${req.amount_to_sweep_usdc:.2f} "
                    f"→ {req.cold_wallet}. **수동 인출 필요** (자동 인출 미구현)"
                )
                # 텔레그램 알림 — 사람이 메타마스크에서 직접 인출하라고
                try:
                    from notifications.telegram import notify
                    notify("CRITICAL", "Sweep 승인됨 — 수동 인출 필요", {
                        "amount": f"${req.amount_to_sweep_usdc:.2f}",
                        "to": req.cold_wallet,
                        "instruction": "Polymarket → Withdraw → 위 주소로 송금",
                    })
                except Exception:
                    pass
                # executed 마크 — 사람이 송금했다고 가정 (정확한 추적은 향후)
                req.executed = True
                executed_count += 1
            except Exception as e:
                log.warning(f"[profit_sweeper] execute error: {e}")
    return executed_count


async def profit_sweep_loop(portfolio_state, interval_sec: int = 3600):
    """매시간 자동 평가."""
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            req = evaluate_sweep_need(portfolio_state)
            if req:
                log.info(f"[profit_sweeper] sweep needed: {req.reason}")
                request_sweep(req)
            await execute_approved_sweeps(portfolio_state)
        except Exception as e:
            log.warning(f"[profit_sweeper] {e}")
            await asyncio.sleep(300)
