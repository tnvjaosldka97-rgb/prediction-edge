"""
메모리 모니터링 + auto-restart 트리거.

장기간 가동 시 메모리 누수 가능성. RSS 추적해서:
- 1GB 초과 → WARN
- 1.5GB 초과 → CRITICAL + 의도적 sys.exit (Railway가 재시작)

각 모듈에서 미세한 누수가 누적되는 패턴 방어.
"""
from __future__ import annotations
import asyncio
import os
import sys
import time
from dataclasses import dataclass


@dataclass
class MemoryStats:
    rss_mb: float
    heap_mb: float
    n_objects: int
    timestamp: float


def get_memory_stats() -> MemoryStats:
    """현재 프로세스 RSS + Python heap."""
    rss_mb = 0.0
    heap_mb = 0.0
    n_obj = 0

    try:
        import resource
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux: KB, macOS: bytes
        rss_mb = rss_kb / 1024 if sys.platform != "darwin" else rss_kb / 1_000_000
    except (ImportError, AttributeError):
        # Windows fallback — psutil 권장
        try:
            import psutil
            p = psutil.Process()
            rss_mb = p.memory_info().rss / 1_000_000
        except ImportError:
            pass

    try:
        import gc
        n_obj = len(gc.get_objects())
        # heap 대략 추정 (모든 객체의 sys.getsizeof 합 — 비싸서 skip)
    except Exception:
        pass

    return MemoryStats(
        rss_mb=rss_mb,
        heap_mb=heap_mb,
        n_objects=n_obj,
        timestamp=time.time(),
    )


_RECENT_STATS = []


async def memory_monitor_loop(
    interval_sec: int = 300,
    warn_threshold_mb: float = 1000,
    critical_threshold_mb: float = 1500,
    auto_restart: bool = True,
):
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            stats = get_memory_stats()
            _RECENT_STATS.append(stats)
            if len(_RECENT_STATS) > 100:
                _RECENT_STATS.pop(0)

            log.debug(f"[memory] RSS={stats.rss_mb:.0f}MB, n_objects={stats.n_objects:,}")

            if stats.rss_mb > critical_threshold_mb:
                log.error(f"[memory] CRITICAL — RSS {stats.rss_mb:.0f}MB > {critical_threshold_mb}MB")
                try:
                    from notifications.telegram import notify
                    notify("CRITICAL", "메모리 임계 초과 — 재시작", {
                        "rss_mb": f"{stats.rss_mb:.0f}",
                        "threshold": critical_threshold_mb,
                        "action": "sys.exit(1) → Railway 자동 재시작",
                    })
                except Exception:
                    pass
                if auto_restart:
                    # Railway가 자동 재시작 (railway.toml restart_policy=always)
                    log.error("[memory] sys.exit(1) — Railway will auto-restart")
                    sys.exit(1)
            elif stats.rss_mb > warn_threshold_mb:
                log.warning(f"[memory] WARN — RSS {stats.rss_mb:.0f}MB > {warn_threshold_mb}MB")
                try:
                    import gc
                    collected = gc.collect()
                    log.info(f"[memory] gc.collect → {collected} objects freed")
                except Exception:
                    pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[memory_monitor] {e}")
            await asyncio.sleep(60)


def get_recent_stats() -> list[dict]:
    return [
        {"ts": s.timestamp, "rss_mb": s.rss_mb, "n_objects": s.n_objects}
        for s in _RECENT_STATS[-20:]
    ]
