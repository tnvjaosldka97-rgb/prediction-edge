"""
Pre-trade Monte Carlo — 매 주문 직전 1000회 시뮬 → expected PnL.

원리:
- 주문 발사 직전 friction.orchestrator로 1000회 가상 체결
- 각 시뮬에서 random latency, partial fill, rejection 적용
- 시뮬 결과 분포 → expected return, p5, p95, win rate
- expected return < threshold → 주문 cancel

각 주문 추가 ~10ms 부담 (1000 × 10μs/sim). 가치 충분.
"""
from __future__ import annotations
import math
import random
from dataclasses import dataclass
from typing import Optional


@dataclass
class MonteCarloResult:
    n_simulations: int
    expected_pnl_usd: float
    p5_pnl_usd: float
    p95_pnl_usd: float
    win_rate: float
    fill_rate: float                # 체결 성공률
    expected_return_pct: float
    accept: bool
    reason: str


def simulate_order(
    side: str,
    size_usd: float,
    limit_price: float,
    expected_resolution_price: float,    # 시그널의 model_prob
    book,
    n_sims: int = 1000,
    accept_threshold_pct: float = 0.005,    # 0.5% expected return 이상이면 accept
) -> MonteCarloResult:
    """
    각 sim마다:
    1. friction.orchestrator로 시뮬 체결 (랜덤 latency·rejection·partial)
    2. 체결가에서 expected_resolution_price까지 PnL 계산
    3. 거부 시 PnL = 0
    """
    try:
        from friction.orchestrator import FrictionOrchestrator
        orch = FrictionOrchestrator()
    except ImportError:
        return MonteCarloResult(0, 0, 0, 0, 0, 0, 0, True, "no_friction_module")

    pnls = []
    n_filled = 0
    submit_ts = 0  # 시뮬에선 절대 시각 무관

    for i in range(n_sims):
        rng = random.Random(i)    # 결정론적 (재현 가능)
        sim = orch.simulate(
            side=side,
            size_usd=size_usd,
            price=limit_price,
            order_type="GTC",
            is_maker=True,
            book_at_submit=book,
            submit_ts=submit_ts,
            future_book_lookup=None,
            market_volatility_5m=0.1,
            rng=rng,
        )
        if not sim.accepted:
            pnls.append(0)
            continue

        n_filled += 1
        # PnL = (resolution - fill_price) × shares - fee, 단 BUY 기준
        if side == "BUY":
            pnl = (expected_resolution_price - sim.avg_fill_price) * sim.filled_size_shares - sim.fee_paid
        else:
            pnl = (sim.avg_fill_price - expected_resolution_price) * sim.filled_size_shares - sim.fee_paid
        pnls.append(pnl)

    if not pnls:
        return MonteCarloResult(0, 0, 0, 0, 0, 0, 0, False, "all_rejected")

    pnls.sort()
    expected = sum(pnls) / len(pnls)
    p5 = pnls[int(len(pnls) * 0.05)]
    p95 = pnls[int(len(pnls) * 0.95)]
    win_rate = sum(1 for p in pnls if p > 0) / len(pnls)
    fill_rate = n_filled / n_sims
    return_pct = expected / size_usd if size_usd > 0 else 0

    accept = return_pct >= accept_threshold_pct
    if not accept:
        reason = f"expected_return {return_pct*100:.2f}% < threshold {accept_threshold_pct*100:.2f}%"
    elif fill_rate < 0.30:
        accept = False
        reason = f"fill_rate {fill_rate*100:.1f}% too low"
    else:
        reason = "accept"

    return MonteCarloResult(
        n_simulations=n_sims,
        expected_pnl_usd=expected,
        p5_pnl_usd=p5,
        p95_pnl_usd=p95,
        win_rate=win_rate,
        fill_rate=fill_rate,
        expected_return_pct=return_pct,
        accept=accept,
        reason=reason,
    )
