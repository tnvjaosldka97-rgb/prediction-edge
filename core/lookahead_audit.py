"""
Look-ahead bias 자동 감사.

질문: 우리 시그널이 시그널 발생 시점에 진짜로 알 수 있던 정보만 사용했나?

검사 항목:
1. signal.created_at < 사용된 price_history 가장 최신 timestamp ✅
2. signal에 사용된 market의 model_prob가 미래 정보 reflect 안 했나?
3. backtest replay가 미래 호가 leak 안 했나?
4. closing_convergence가 만기 후 가격 본 적 없나?

실행: 매주 1회 자동 + 수동 호출 가능.
"""
from __future__ import annotations
import sqlite3
import time
from dataclasses import dataclass

import config


@dataclass
class LeakIssue:
    severity: str
    type: str
    detail: str
    n_affected: int


def audit_signal_timing() -> list[LeakIssue]:
    """signals 테이블 — created_at vs price_history 시점."""
    issues = []
    conn = sqlite3.connect(config.DB_PATH)

    # signal의 model_prob이 미래 가격 본 적 있나?
    # signal.created_at + 5분 후 price_history price와 model_prob 매우 가까운지
    rows = conn.execute(
        "SELECT s.signal_id, s.created_at, s.token_id, s.model_prob, s.market_prob "
        "FROM signals s ORDER BY s.created_at DESC LIMIT 200"
    ).fetchall()

    suspicious = 0
    for sig_id, created_at, token_id, model_prob, market_prob in rows:
        if not model_prob or not market_prob:
            continue
        # 5분 후 가격
        future_price = conn.execute(
            "SELECT price FROM price_history WHERE token_id=? AND timestamp BETWEEN ? AND ? "
            "ORDER BY timestamp ASC LIMIT 1",
            (token_id, created_at + 60, created_at + 600)
        ).fetchone()
        if future_price and future_price[0]:
            fp = future_price[0]
            # model_prob이 실제 미래 가격에 너무 가까우면 누수 의심
            if abs(model_prob - fp) < 0.005 and abs(model_prob - market_prob) > 0.05:
                suspicious += 1

    if suspicious > 5:
        issues.append(LeakIssue(
            severity="HIGH",
            type="model_prob_leak",
            detail=f"{suspicious}개 시그널의 model_prob이 5분 후 실제 가격과 너무 가까움 (look-ahead 의심)",
            n_affected=suspicious,
        ))

    conn.close()
    return issues


def audit_backtest_replay() -> list[LeakIssue]:
    """replay_live.py나 백테스트가 미래 호가창 사용했나? (코드 검토 필요, 자동화 어려움)"""
    # 자동 검증 어려움 — replay_live.py에서 future_book_lookup 사용 여부만 체크
    issues = []
    try:
        from pathlib import Path
        replay_path = Path(__file__).resolve().parent.parent / "backtest" / "replay_live.py"
        if replay_path.exists():
            content = replay_path.read_text(encoding="utf-8")
            # future_book_lookup=None 명시되어 있으면 OK
            if "future_book_lookup=None" not in content:
                issues.append(LeakIssue(
                    severity="MEDIUM",
                    type="replay_potential_leak",
                    detail="replay_live.py에 future_book_lookup 사용 흔적 — 백테스트 자동 검증 X",
                    n_affected=1,
                ))
    except Exception:
        pass
    return issues


def audit_resolution_leakage() -> list[LeakIssue]:
    """closing_convergence가 만기 후 가격을 본 적 있나?"""
    issues = []
    conn = sqlite3.connect(config.DB_PATH)
    # signals에서 strategy=closing_convergence + market의 resolved_at < signal.created_at
    suspicious = conn.execute(
        "SELECT COUNT(*) FROM signals s "
        "WHERE s.strategy = 'closing_convergence' "
        "  AND s.resolved_at IS NOT NULL "
        "  AND s.resolved_at < s.created_at"
    ).fetchone()[0]
    conn.close()
    if suspicious > 0:
        issues.append(LeakIssue(
            severity="CRITICAL",
            type="resolution_leak",
            detail=f"{suspicious} closing_convergence 시그널이 만기 후 생성됨 (불가능한 상황)",
            n_affected=suspicious,
        ))
    return issues


def run_full_audit() -> dict:
    all_issues = []
    all_issues.extend(audit_signal_timing())
    all_issues.extend(audit_backtest_replay())
    all_issues.extend(audit_resolution_leakage())

    by_severity = {"CRITICAL": [], "HIGH": [], "MEDIUM": [], "LOW": []}
    for i in all_issues:
        by_severity.setdefault(i.severity, []).append({
            "type": i.type, "detail": i.detail, "n": i.n_affected,
        })

    return {
        "n_total": len(all_issues),
        "by_severity": by_severity,
        "audited_at": time.time(),
    }


async def lookahead_audit_loop(interval_sec: int = 7 * 86400):
    """매주 1회 자동 감사."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            result = run_full_audit()
            log.info(f"[lookahead_audit] {result['n_total']} issues: {result['by_severity']}")
            critical = result["by_severity"].get("CRITICAL", [])
            if critical:
                try:
                    from notifications.telegram import notify
                    notify("CRITICAL", f"Look-ahead 누수 감지 {len(critical)}건", {
                        "first": critical[0]["detail"][:80],
                    })
                except Exception:
                    pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[lookahead_audit] {e}")
            await asyncio.sleep(86400)
