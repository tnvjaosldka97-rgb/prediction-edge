"""
일일 자동 리포트 — 매일 오전 9시 KST 텔레그램 push.

목적: 사용자가 봇 상태를 정기적으로 인지하면 패닉 X.
"안 보면 무서워서 뭐가 일어나고 있는지 모름" 패턴 방어.

리포트 항목:
1. 어제 PnL + 벤치마크 비교
2. 누적 30일 PnL + IR
3. 현재 포지션 (수, 가치, 카테고리 분포)
4. 활성 전략별 trade 수·승률
5. 가장 큰 winner / loser
6. drawdown 현황 (피크 대비 %)
7. friction 통계 (latency, slippage, rejection)
8. 알파 decay 경고 (전략별 30/7/1d Sharpe)
9. 보호 모드 상태
10. 자동 액션 요약 (캘리브레이션, 자동 비활성화 등)

사람이 매일 봇 상태 한눈에. 패닉 없음. 신뢰 형성.
"""
from __future__ import annotations
import asyncio
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config


def _conn():
    return sqlite3.connect(config.DB_PATH)


def _yesterday_window_ts() -> tuple[float, float]:
    """KST 기준 어제 0시 ~ 24시 timestamp."""
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    return yesterday_start.timestamp(), today_start.timestamp()


def generate_report() -> dict:
    yesterday_start, yesterday_end = _yesterday_window_ts()

    conn = _conn()

    # 1. 어제 PnL
    daily_pnl_row = conn.execute(
        "SELECT MIN(total_value), MAX(total_value), "
        "       (SELECT total_value FROM portfolio_snapshots "
        "        WHERE timestamp BETWEEN ? AND ? "
        "        ORDER BY timestamp ASC LIMIT 1), "
        "       (SELECT total_value FROM portfolio_snapshots "
        "        WHERE timestamp BETWEEN ? AND ? "
        "        ORDER BY timestamp DESC LIMIT 1) "
        "FROM portfolio_snapshots WHERE timestamp BETWEEN ? AND ?",
        (yesterday_start, yesterday_end, yesterday_start, yesterday_end,
         yesterday_start, yesterday_end)
    ).fetchone()

    daily_low, daily_high, daily_open, daily_close = daily_pnl_row or (0, 0, 0, 0)
    daily_pnl_pct = (daily_close - daily_open) / daily_open * 100 if daily_open and daily_open > 0 else 0

    # 2. 30일 IR (벤치마크 모듈 활용)
    try:
        from core.benchmark import compare
        bench = compare(window_days=30)
        ir = bench.information_ratio
        alpha_30d = bench.alpha_vs_buyhold_pct
    except Exception:
        ir = 0
        alpha_30d = 0

    # 3. 현재 포지션 — virtual_trades 미해결 카운트
    n_open = conn.execute(
        "SELECT COUNT(*) FROM virtual_trades WHERE resolved_at IS NULL"
    ).fetchone()[0]

    # 4. 어제 trade
    yesterday_trades = conn.execute(
        "SELECT strategy, COUNT(*), AVG(COALESCE(pnl, 0)) "
        "FROM trades WHERE timestamp BETWEEN ? AND ? AND strategy IS NOT NULL "
        "GROUP BY strategy",
        (yesterday_start, yesterday_end)
    ).fetchall()

    # 5. winner / loser
    biggest_win = conn.execute(
        "SELECT token_id, pnl FROM trades WHERE timestamp BETWEEN ? AND ? "
        "AND pnl > 0 ORDER BY pnl DESC LIMIT 1",
        (yesterday_start, yesterday_end)
    ).fetchone()
    biggest_loss = conn.execute(
        "SELECT token_id, pnl FROM trades WHERE timestamp BETWEEN ? AND ? "
        "AND pnl < 0 ORDER BY pnl ASC LIMIT 1",
        (yesterday_start, yesterday_end)
    ).fetchone()

    # 6. drawdown
    peak_row = conn.execute(
        "SELECT MAX(total_value) FROM portfolio_snapshots WHERE timestamp >= ?",
        (time.time() - 30 * 86400,)
    ).fetchone()
    peak = peak_row[0] or 0
    current_value = daily_close or 0
    dd_from_peak = (peak - current_value) / peak * 100 if peak > 0 else 0

    # 7. friction 통계
    try:
        from core import db as cdb
        recent_traces = cdb.get_friction_traces(since_ts=yesterday_start, limit=1000)
        n_filled = sum(1 for t in recent_traces if t.get("fill_ts"))
        n_rejected = sum(1 for t in recent_traces if t.get("rejection_reason"))
        latencies = [t["submit_to_fill_ms"] for t in recent_traces if t.get("submit_to_fill_ms")]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        slippages = [t["slippage_bps"] for t in recent_traces if t.get("slippage_bps") is not None]
        avg_slip = sum(slippages) / len(slippages) if slippages else 0
    except Exception:
        n_filled = n_rejected = 0
        avg_latency = avg_slip = 0

    # 8. 알파 decay 경고
    try:
        from risk.alpha_decay import evaluate_all_strategies
        decay = evaluate_all_strategies()
        decay_warns = [r for r in decay if r.recommendation in ("scale_down", "disable")]
    except Exception:
        decay_warns = []

    # 9. 보호 모드
    try:
        from risk.protection_mode import status as p_status
        protection = p_status()
    except Exception:
        protection = {"active": False, "remaining_days": 0}

    conn.close()

    return {
        "date": datetime.fromtimestamp(yesterday_start, tz=timezone(timedelta(hours=9))).strftime("%Y-%m-%d"),
        "daily_pnl_pct": daily_pnl_pct,
        "daily_open": daily_open,
        "daily_close": daily_close,
        "daily_low": daily_low,
        "daily_high": daily_high,
        "30d_information_ratio": ir,
        "30d_alpha_pct": alpha_30d,
        "n_open_positions": n_open,
        "yesterday_trades_by_strategy": [
            {"strategy": r[0], "n": r[1], "avg_pnl": r[2]}
            for r in yesterday_trades
        ],
        "biggest_win": biggest_win[1] if biggest_win else 0,
        "biggest_loss": biggest_loss[1] if biggest_loss else 0,
        "drawdown_from_peak_pct": dd_from_peak,
        "current_value": current_value,
        "peak_value_30d": peak,
        "filled_yesterday": n_filled,
        "rejected_yesterday": n_rejected,
        "avg_latency_ms": avg_latency,
        "avg_slippage_bps": avg_slip,
        "alpha_decay_warnings": [
            {"strategy": r.strategy, "rec": r.recommendation, "sharpe_7d": r.sharpe_7d}
            for r in decay_warns
        ],
        "protection_active": protection["active"],
        "protection_remaining_days": protection["remaining_days"],
    }


def format_telegram(report: dict) -> str:
    """텔레그램용 포맷 (Markdown)."""
    pnl_emoji = "📈" if report["daily_pnl_pct"] > 0 else "📉" if report["daily_pnl_pct"] < 0 else "➡️"
    dd = report["drawdown_from_peak_pct"]
    dd_emoji = "🟢" if dd < 5 else "🟡" if dd < 10 else "🔴"

    lines = [
        f"📊 *일일 리포트 {report['date']}*",
        "",
        f"{pnl_emoji} 어제 PnL: *{report['daily_pnl_pct']:+.2f}%*",
        f"   (open ${report['daily_open']:.2f} → close ${report['daily_close']:.2f})",
        "",
        f"📈 30일 IR: *{report['30d_information_ratio']:.2f}* {' (강한 알파)' if report['30d_information_ratio'] > 1 else ' (실 알파)' if report['30d_information_ratio'] > 0.5 else ' (노이즈 의심)'}",
        f"   알파 vs buyhold: {report['30d_alpha_pct']:+.2f}%",
        "",
        f"{dd_emoji} drawdown: {dd:.1f}% from peak ${report['peak_value_30d']:.0f}",
        f"📦 보유 포지션: {report['n_open_positions']}",
        f"⚡ 어제 체결: {report['filled_yesterday']}건 / 거부: {report['rejected_yesterday']}건",
        f"⏱️  평균 latency: {report['avg_latency_ms']:.0f}ms / slip: {report['avg_slippage_bps']:.1f}bps",
        "",
    ]

    if report["yesterday_trades_by_strategy"]:
        lines.append("*전략별 어제 활동:*")
        for s in report["yesterday_trades_by_strategy"][:5]:
            lines.append(f"  • {s['strategy']}: {s['n']}건, 평균 ${s['avg_pnl']:.4f}")
        lines.append("")

    if report["alpha_decay_warnings"]:
        lines.append(f"⚠️ *알파 decay 경고 {len(report['alpha_decay_warnings'])}개:*")
        for w in report["alpha_decay_warnings"][:3]:
            lines.append(f"  • {w['strategy']}: {w['rec']} (7d Sharpe {w['sharpe_7d']:.2f})")
        lines.append("")

    if report["protection_active"]:
        lines.append(f"🛡 보호 모드: 잔여 {report['protection_remaining_days']:.1f}일")

    return "\n".join(lines)


async def send_daily_report():
    report = generate_report()
    msg = format_telegram(report)
    try:
        from notifications.telegram import notify_async
        # notify_async는 짧은 메시지용 — 긴 메시지는 직접 호출
        from notifications.telegram import TelegramNotifier
        n = TelegramNotifier.get()
        if n.enabled:
            import httpx
            url = f"https://api.telegram.org/bot{n.token}/sendMessage"
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.post(url, json={
                        "chat_id": n.chat_id, "text": msg, "parse_mode": "Markdown",
                    })
            except Exception:
                pass
    except Exception:
        pass

    # WebSocket
    try:
        from dashboard.realtime import broadcast_event
        broadcast_event("daily_report", report)
    except Exception:
        pass

    return report


async def daily_report_loop():
    """매일 KST 9시 리포트."""
    from datetime import datetime, timedelta, timezone
    from core.logger import log
    kst = timezone(timedelta(hours=9))
    while True:
        try:
            now = datetime.now(kst)
            target = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_sec = (target - now).total_seconds()
            await asyncio.sleep(wait_sec)
            log.info("[daily_report] generating report")
            await send_daily_report()
        except Exception as e:
            from core.logger import log
            log.warning(f"[daily_report] {e}")
            await asyncio.sleep(3600)
