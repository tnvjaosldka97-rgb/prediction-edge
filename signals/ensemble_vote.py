"""
Ensemble Voting — 같은 token_id에 여러 전략이 동시 발화 시 가중 합산.

원리:
- closing_convergence + claude_oracle + news_lag 셋이 모두 BUY → 강한 신호
- vs 하나만 BUY → 약한 신호
- 각 전략 weight = 최근 7일 Sharpe (음수면 0)
- 가중 합산 confidence + edge 사용

기존 signal_aggregator는 단순 dedup (가장 높은 confidence만).
이건 voting 추가로 고려.
"""
from __future__ import annotations
import math
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass

import config


@dataclass
class StrategyWeight:
    strategy: str
    weight: float           # 0~1
    recent_sharpe: float
    n_trades: int


def compute_strategy_weights(window_days: float = 7) -> dict[str, StrategyWeight]:
    """전략별 최근 Sharpe → 가중치."""
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT strategy, pnl FROM trades WHERE timestamp >= ? AND pnl IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    by_strategy: dict[str, list[float]] = defaultdict(list)
    for s, p in rows:
        if s and p is not None:
            by_strategy[s].append(p)

    weights = {}
    for strategy, pnls in by_strategy.items():
        if len(pnls) < 5:
            weights[strategy] = StrategyWeight(strategy, 0.5, 0, len(pnls))    # 기본값
            continue
        mean = sum(pnls) / len(pnls)
        var = sum((p - mean) ** 2 for p in pnls) / max(1, len(pnls) - 1)
        std = math.sqrt(var)
        sharpe_simple = mean / std if std > 0 else 0    # daily Sharpe 근사
        # 0~1 범위로 매핑 — Sharpe 0 = 0.5, Sharpe >2 → 1.0, Sharpe <-1 → 0
        weight = max(0.0, min(1.0, 0.5 + sharpe_simple / 4))
        weights[strategy] = StrategyWeight(strategy, weight, sharpe_simple, len(pnls))

    return weights


@dataclass
class EnsembleVote:
    token_id: str
    direction: str          # "BUY" / "SELL"
    weighted_confidence: float
    weighted_edge: float
    contributing_strategies: list[str]
    n_voters: int


def aggregate_votes(signals: list, window_days: float = 7) -> dict[str, EnsembleVote]:
    """동일 token_id + direction 시그널들을 가중 합산.

    Args: signals — [(token_id, direction, confidence, edge, strategy), ...]
    Returns: token_id → EnsembleVote (방향성 합쳐진 결과)
    """
    weights = compute_strategy_weights(window_days)

    # token_id × direction → 시그널 묶음
    bucket: dict[tuple[str, str], list] = defaultdict(list)
    for token_id, direction, confidence, edge, strategy in signals:
        bucket[(token_id, direction)].append((strategy, confidence, edge))

    out = {}
    for (token_id, direction), members in bucket.items():
        if len(members) < 2:
            continue    # 단일 전략 시그널은 그냥 통과
        total_w = 0
        weighted_conf = 0
        weighted_edge = 0
        contributing = []
        for strategy, conf, edge in members:
            w = weights.get(strategy, StrategyWeight(strategy, 0.5, 0, 0)).weight
            total_w += w
            weighted_conf += w * conf
            weighted_edge += w * edge
            contributing.append(strategy)

        if total_w > 0:
            out[token_id] = EnsembleVote(
                token_id=token_id,
                direction=direction,
                weighted_confidence=weighted_conf / total_w,
                weighted_edge=weighted_edge / total_w,
                contributing_strategies=contributing,
                n_voters=len(members),
            )

    return out


def boost_confidence_if_ensemble(token_id: str, base_confidence: float,
                                   active_signals_for_token: list) -> float:
    """단일 시그널의 confidence를 같은 token에 다른 전략도 참여 시 부스트.

    부스트 공식: 1 + 0.1 × (n_voters - 1), 최대 +0.3
    """
    n = len(active_signals_for_token)
    if n <= 1:
        return base_confidence
    boost_factor = 1 + min(0.3, 0.1 * (n - 1))
    return min(0.99, base_confidence * boost_factor)
