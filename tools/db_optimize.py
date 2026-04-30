"""
DB 최적화 — 인덱스·VACUUM·ANALYZE.

수동 실행: venv/Scripts/python tools/db_optimize.py
자동 실행: 매주 일요일 main.py 백그라운드.

작업:
1. ANALYZE — 쿼리 옵티마이저 통계 갱신
2. VACUUM — 사용 안 하는 페이지 회수 (DB 크기 줄임)
3. 누락 인덱스 추가
4. 오래된 데이터 archive (price_history > 30일)
"""
from __future__ import annotations
import sqlite3
import time
from pathlib import Path

import config


CRITICAL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_trades_strategy_ts ON trades(strategy, timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_trades_token_ts ON trades(token_id, timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_signals_resolved ON signals(resolved_at) WHERE resolved_at IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_friction_token_ts ON friction_traces(token_id, submit_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_virtual_resolved ON virtual_trades(resolved_at) WHERE resolved_at IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_price_token_ts_desc ON price_history(token_id, timestamp DESC)",
]


def run_optimization() -> dict:
    conn = sqlite3.connect(config.DB_PATH)
    stats = {}

    # 시작 크기
    db_path = Path(config.DB_PATH)
    if db_path.exists():
        stats["size_before_mb"] = db_path.stat().st_size / 1_000_000

    # 1. 누락 인덱스
    indexes_added = 0
    for idx_sql in CRITICAL_INDEXES:
        try:
            conn.execute(idx_sql)
            indexes_added += 1
        except sqlite3.Error as e:
            print(f"index skip: {e}")
    conn.commit()
    stats["indexes_processed"] = indexes_added

    # 2. ANALYZE
    t0 = time.time()
    conn.execute("ANALYZE")
    conn.commit()
    stats["analyze_sec"] = round(time.time() - t0, 2)

    # 3. 오래된 price_history archive (30일 이상)
    cutoff = time.time() - 30 * 86400
    deleted = conn.execute(
        "DELETE FROM price_history WHERE timestamp < ?",
        (cutoff,)
    ).rowcount
    conn.commit()
    stats["price_history_deleted"] = deleted

    # 4. 오래된 friction_traces archive (60일 이상)
    cutoff_friction = time.time() - 60 * 86400
    try:
        deleted_friction = conn.execute(
            "DELETE FROM friction_traces WHERE submit_ts < ?",
            (cutoff_friction,)
        ).rowcount
        conn.commit()
        stats["friction_traces_deleted"] = deleted_friction
    except Exception:
        stats["friction_traces_deleted"] = 0

    conn.close()

    # 5. VACUUM (별도 connection — VACUUM은 transaction 안 됨)
    t0 = time.time()
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("VACUUM")
    conn.close()
    stats["vacuum_sec"] = round(time.time() - t0, 2)

    if db_path.exists():
        stats["size_after_mb"] = db_path.stat().st_size / 1_000_000
        stats["space_saved_mb"] = stats["size_before_mb"] - stats["size_after_mb"]

    return stats


async def db_optimize_loop(interval_sec: int = 7 * 86400):
    """매주 1회 자동 실행."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            log.info("[db_optimize] starting weekly optimization")
            stats = run_optimization()
            log.info(f"[db_optimize] {stats}")
            try:
                from notifications.telegram import notify
                notify("INFO", "DB 최적화 완료", {
                    "size_before_mb": f"{stats.get('size_before_mb', 0):.1f}",
                    "size_after_mb": f"{stats.get('size_after_mb', 0):.1f}",
                    "saved_mb": f"{stats.get('space_saved_mb', 0):.1f}",
                    "deleted_price_rows": stats.get("price_history_deleted", 0),
                })
            except Exception:
                pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[db_optimize] {e}")
            await asyncio.sleep(86400)


if __name__ == "__main__":
    print("Running DB optimization...")
    stats = run_optimization()
    print(f"Results: {stats}")
