"""
Fund lock model — 체인 정착 시간.

Polygon 위에서 USDC 거래가 실제 정착(finality)되기까지의 시간 모델.
P&L 자체는 안 바꾸지만, 다음 주문까지의 minimum gap을 결정 → 처리량 한도.

Polymarket의 경우:
- 트레이드 자체: gasless (CLOB이 매칭, exchange가 서명)
- USDC approve: 첫 거래 시 한 번 필요, ~30초
- Gas spike 시: tx 정착 1~5분 추가
- Reorg risk: Polygon 매우 낮음, 무시 가능
"""
from __future__ import annotations
import random
from dataclasses import dataclass


@dataclass
class FundLockResult:
    settle_delay_sec: float
    is_first_approve: bool
    gas_spike_occurred: bool


class FundLockModel:
    def __init__(
        self,
        base_finality_sec: float = 30.0,
        first_approve_extra_sec: float = 30.0,
        gas_spike_prob: float = 0.05,
        gas_spike_extra_min_sec: float = 60.0,
        gas_spike_extra_max_sec: float = 300.0,
    ):
        self.base = base_finality_sec
        self.first_approve_extra = first_approve_extra_sec
        self.gas_spike_prob = gas_spike_prob
        self.spike_min = gas_spike_extra_min_sec
        self.spike_max = gas_spike_extra_max_sec
        self._approved = False

    def settle(self, rng: random.Random | None = None) -> FundLockResult:
        r = rng or random
        delay = self.base
        is_first = not self._approved
        if is_first:
            delay += self.first_approve_extra
            self._approved = True

        spike = False
        if r.random() < self.gas_spike_prob:
            delay += r.uniform(self.spike_min, self.spike_max)
            spike = True

        return FundLockResult(
            settle_delay_sec=delay,
            is_first_approve=is_first,
            gas_spike_occurred=spike,
        )

    def reset_approve_state(self) -> None:
        """테스트나 새 세션 시작 시 첫 approve 상태 초기화."""
        self._approved = False

    def calibrate(
        self,
        observed_settle_delays: list[float],
        first_approve_observed: bool,
    ) -> None:
        """라이브 정착 시간 분포로 base + spike 갱신."""
        if len(observed_settle_delays) < 10:
            return
        sorted_d = sorted(observed_settle_delays)
        # 중앙값 = base
        median = sorted_d[len(sorted_d) // 2]
        self.base = median
        # 95퍼센타일 - 중앙값 = 추가 spike 평균
        p95 = sorted_d[int(len(sorted_d) * 0.95)]
        spike_extra = max(0.0, p95 - median)
        self.spike_min = spike_extra * 0.5
        self.spike_max = spike_extra * 1.5
        # spike 빈도 — 중앙값 대비 2배 이상인 사례 비율
        spike_count = sum(1 for d in observed_settle_delays if d > median * 2)
        self.gas_spike_prob = spike_count / len(observed_settle_delays)

    def to_dict(self) -> dict:
        return {
            "base_finality_sec": self.base,
            "first_approve_extra_sec": self.first_approve_extra,
            "gas_spike_prob": self.gas_spike_prob,
            "gas_spike_extra_min_sec": self.spike_min,
            "gas_spike_extra_max_sec": self.spike_max,
        }
