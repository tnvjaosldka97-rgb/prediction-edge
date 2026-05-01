"""
Whale Activity → 가격 시차 알파 — 진짜 시장 주도 시그널.

원리:
- on-chain 큰 지갑이 매수 → 1~5분 후 가격이 그 방향으로 따라감 (정보 비대칭)
- 우리는 whale 진입 직후 5초~30초 내 가격이 움직이기 전에 진입
- copy_trade와 다른 점: 우리는 통계적 시차를 활용하여 적극적 front-run

흐름:
1. on-chain watcher가 whale trade 감지
2. trade 시점 + market mid 기록 (whale_lag_observations 테이블)
3. +5s, +60s, +5min 후 mid를 drift_tracker가 샘플
4. 30개 샘플 누적 시 통계 도출:
   - mean lag → 우리가 들어갈 시간 윈도우
   - hit rate → 우리 confidence
5. 다음 whale trade 시 → 통계 기반 우리 시그널 발생

이건 단순 copy_trade보다 정교 — 시차·hit rate 학습 후 진입.
"""
from __future__ import annotations
import asyncio
import sqlite3
import time
import uuid
from collections import defaultdict

import config


def _ensure_table():
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS whale_lag_observations (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet_address  TEXT NOT NULL,
        condition_id    TEXT NOT NULL,
        token_id        TEXT NOT NULL,
        side            TEXT NOT NULL,
        whale_trade_ts  REAL NOT NULL,
        mid_at_trade    REAL,
        mid_after_5s    REAL,
        mid_after_60s   REAL,
        mid_after_300s  REAL,
        winrate_for_wallet REAL,
        sharpe_for_wallet  REAL
    );
    CREATE INDEX IF NOT EXISTS idx_whale_lag_token ON whale_lag_observations(token_id, whale_trade_ts);
    CREATE INDEX IF NOT EXISTS idx_whale_lag_wallet ON whale_lag_observations(wallet_address);
    """)
    conn.commit()
    conn.close()


def record_whale_trade(
    wallet_address: str, condition_id: str, token_id: str,
    side: str, mid_at_trade: float,
    winrate: float = 0.0, sharpe: float = 0.0
) -> int:
    """on-chain watcher가 whale trade 감지 시 호출."""
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.execute(
        "INSERT INTO whale_lag_observations "
        "(wallet_address, condition_id, token_id, side, whale_trade_ts, mid_at_trade, "
        "winrate_for_wallet, sharpe_for_wallet) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (wallet_address, condition_id, token_id, side, time.time(), mid_at_trade,
         winrate, sharpe),
    )
    conn.commit()
    obs_id = cur.lastrowid
    conn.close()
    return obs_id


async def schedule_drift_samples(obs_id: int, token_id: str, store):
    """5s/60s/300s 후 mid 가격 샘플."""
    from core.logger import log

    async def sample(delay: int, col: str):
        await asyncio.sleep(delay)
        try:
            book = store.get_orderbook(token_id) if store else None
            if book and not book.is_stale() and book.bids and book.asks:
                mid = (book.best_bid + book.best_ask) / 2
                conn = sqlite3.connect(config.DB_PATH)
                conn.execute(f"UPDATE whale_lag_observations SET {col} = ? WHERE id = ?",
                              (mid, obs_id))
                conn.commit()
                conn.close()
        except Exception as e:
            log.debug(f"[whale_lag] sample {col}: {e}")

    await asyncio.gather(
        sample(5, "mid_after_5s"),
        sample(60, "mid_after_60s"),
        sample(300, "mid_after_300s"),
        return_exceptions=True,
    )


def compute_lag_stats(min_samples: int = 30) -> dict:
    """누적 데이터에서 시차 통계 — 우리 시그널 기준."""
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT side, mid_at_trade, mid_after_5s, mid_after_60s, mid_after_300s, "
        "       winrate_for_wallet, sharpe_for_wallet "
        "FROM whale_lag_observations "
        "WHERE mid_after_60s IS NOT NULL"
    ).fetchall()
    conn.close()

    if len(rows) < min_samples:
        return {"n": len(rows), "ready": False, "reason": f"need {min_samples}+, have {len(rows)}"}

    # 방향 보정 markout
    by_window = {"5s": [], "60s": [], "300s": []}
    by_wallet_quality = {"high": [], "low": []}
    hit_count = 0
    for side, m0, m5, m60, m300, wr, _ in rows:
        if not m0 or m0 <= 0:
            continue
        for col_name, m in [("5s", m5), ("60s", m60), ("300s", m300)]:
            if m and m > 0:
                signed = (m - m0) / m0 if side == "BUY" else (m0 - m) / m0
                by_window[col_name].append(signed)
        # 60s 기준으로 hit (positive markout) 계산
        if m60 and m60 > 0:
            signed_60 = (m60 - m0) / m0 if side == "BUY" else (m0 - m60) / m0
            if signed_60 > 0.001:    # 0.1% 이상 우리한테 유리
                hit_count += 1
            if (wr or 0) >= 0.65:
                by_wallet_quality["high"].append(signed_60)
            else:
                by_wallet_quality["low"].append(signed_60)

    n_60s = len(by_window["60s"])
    return {
        "n": len(rows),
        "n_60s": n_60s,
        "ready": True,
        "avg_markout_5s_pct": sum(by_window["5s"]) / max(1, len(by_window["5s"])) * 100,
        "avg_markout_60s_pct": sum(by_window["60s"]) / max(1, n_60s) * 100,
        "avg_markout_300s_pct": sum(by_window["300s"]) / max(1, len(by_window["300s"])) * 100,
        "hit_rate_60s": hit_count / max(1, n_60s),
        "high_quality_wallet_avg_60s_pct": (
            sum(by_wallet_quality["high"]) / max(1, len(by_wallet_quality["high"])) * 100
            if by_wallet_quality["high"] else 0
        ),
        "high_quality_n": len(by_wallet_quality["high"]),
    }


async def emit_signal_for_whale_trade(
    wallet_address: str, condition_id: str, token_id: str, side: str,
    market, signal_bus, store, winrate: float = 0.0
):
    """Whale trade 감지 후, 통계 기반으로 우리 시그널 발생.

    조건:
    - hit_rate_60s > 55% (시차 알파 검증됨)
    - high_quality_wallet (winrate >= 0.65) 시 더 강한 confidence
    """
    from core.logger import log
    from core.models import Signal

    stats = compute_lag_stats(min_samples=30)

    # 통계 미숙 시 단순 copy 시그널 (낮은 confidence)
    if not stats.get("ready"):
        confidence = 0.5
        edge_assumption = 0.005
    else:
        hit_rate = stats.get("hit_rate_60s", 0.5)
        if hit_rate < 0.55:
            log.debug(f"[whale_lag] hit_rate {hit_rate:.2f} < 0.55, skip signal")
            return    # 시차 알파 없음
        confidence = min(0.85, 0.5 + (hit_rate - 0.5) * 1.5)
        edge_assumption = abs(stats.get("avg_markout_60s_pct", 0.5)) / 100

    # 고품질 지갑이면 boost
    if winrate >= 0.65 and stats.get("ready"):
        confidence = min(0.95, confidence * 1.15)

    if not market or not market.yes_token:
        return
    yes = market.yes_token
    if yes.price <= 0.05 or yes.price >= 0.95:
        return    # 극단치 제외

    fee_pct = config.TAKER_FEE_RATE * yes.price * (1 - yes.price)
    net_edge = max(0.001, edge_assumption - fee_pct)
    if net_edge < config.MIN_EDGE_AFTER_FEES:
        return

    sig = Signal(
        signal_id=str(uuid.uuid4()),
        strategy="copy_trade",    # 기존 라벨 활용
        condition_id=condition_id,
        token_id=token_id,
        direction=side,
        model_prob=yes.price + (edge_assumption if side == "BUY" else -edge_assumption),
        market_prob=yes.price,
        edge=edge_assumption,
        net_edge=net_edge,
        confidence=confidence,
        urgency="HIGH",    # 시차 윈도우 좁음
        stale_price=yes.price,
        stale_threshold=0.01,
    )
    await signal_bus.put(sig)

    # 관찰 기록 + 시차 샘플
    obs_id = record_whale_trade(
        wallet_address, condition_id, token_id, side,
        mid_at_trade=yes.price, winrate=winrate,
    )
    asyncio.create_task(schedule_drift_samples(obs_id, token_id, store))

    log.info(
        f"[whale_lag] {side} {condition_id[:8]} edge={edge_assumption:.3f} "
        f"conf={confidence:.2f} (n_data={stats.get('n', 0)})"
    )
