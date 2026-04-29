"""
Rejection model — 주문 거부 시뮬레이션.

거부 사유 종류:
- rate_limit       : Polymarket CLOB 50 req/sec 한도 초과
- min_size         : $1 미만 주문
- tick_size        : 0.001 단위 어긋남
- signature_expired: EIP-712 서명 60초 만료 (latency 길어진 경우)
- polygon_rpc_error: Polygon RPC 일시 죽음
- market_inactive  : 결제 직전·결제 중 마켓
- insufficient_balance: 잔고 부족 (race condition)

결정론적 거부 (rate_limit, min_size, tick_size)는 항상 체크.
확률적 거부 (signature, RPC, inactive)는 calibrated rate로 샘플.
"""
from __future__ import annotations
import random
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class TokenBucket:
    """Polymarket CLOB 50 req/sec 시뮬용 토큰 버킷."""
    capacity: float
    refill_per_sec: float
    tokens: float = 0.0
    last_refill: float = 0.0

    def __post_init__(self):
        self.tokens = self.capacity
        self.last_refill = time.time()

    def try_take(self, ts: Optional[float] = None) -> bool:
        ts = ts if ts is not None else time.time()
        elapsed = max(0.0, ts - self.last_refill)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
        self.last_refill = ts
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


@dataclass
class RejectionResult:
    rejected: bool
    reason: Optional[str] = None


class RejectionModel:
    REASONS = (
        "rate_limit",
        "min_size",
        "tick_size",
        "signature_expired",
        "polygon_rpc_error",
        "market_inactive",
        "insufficient_balance",
    )

    def __init__(
        self,
        rate_limit_capacity: float = 50.0,
        rate_limit_per_sec: float = 50.0,
        min_size_usd: float = 1.0,
        tick_size: float = 0.001,
        prob_signature_expired: float = 0.01,
        prob_polygon_rpc_error: float = 0.008,
        prob_market_inactive: float = 0.002,
        prob_insufficient_balance: float = 0.0,
    ):
        self.bucket = TokenBucket(rate_limit_capacity, rate_limit_per_sec)
        self.min_size_usd = min_size_usd
        self.tick_size = tick_size
        self.probs = {
            "signature_expired": prob_signature_expired,
            "polygon_rpc_error": prob_polygon_rpc_error,
            "market_inactive": prob_market_inactive,
            "insufficient_balance": prob_insufficient_balance,
        }

    def check(
        self,
        size_usd: float,
        price: float,
        submit_ts: float,
        latency_ms: float,
        rng: Optional[random.Random] = None,
    ) -> RejectionResult:
        """전체 마찰 파이프라인 중 거부 단계."""
        r = rng or random

        # 1. Rate limit (결정론적)
        if not self.bucket.try_take(submit_ts):
            return RejectionResult(True, "rate_limit")

        # 2. Min size (결정론적)
        if size_usd < self.min_size_usd:
            return RejectionResult(True, "min_size")

        # 3. Tick size (결정론적) — Polymarket은 0.001 단위
        # 가격이 tick의 정수 배인지 확인 (부동소수점 오차 허용)
        scaled = price / self.tick_size
        if abs(scaled - round(scaled)) > 1e-6:
            return RejectionResult(True, "tick_size")

        # 4. Signature 만료 — latency가 60초 넘으면 결정론적 거부
        if latency_ms >= 60_000:
            return RejectionResult(True, "signature_expired")

        # 5. 확률적 거부들
        for reason, p in self.probs.items():
            if p <= 0:
                continue
            if r.random() < p:
                return RejectionResult(True, reason)

        return RejectionResult(False, None)

    def calibrate(self, observed_rejections: dict[str, int], total_orders: int) -> None:
        """라이브 trace의 거부 사유별 빈도로 확률 갱신."""
        if total_orders < 50:
            return
        for reason in ("signature_expired", "polygon_rpc_error", "market_inactive", "insufficient_balance"):
            count = observed_rejections.get(reason, 0)
            self.probs[reason] = count / total_orders

    def to_dict(self) -> dict:
        return {
            "rate_limit_capacity": self.bucket.capacity,
            "rate_limit_per_sec": self.bucket.refill_per_sec,
            "min_size_usd": self.min_size_usd,
            "tick_size": self.tick_size,
            **{f"prob_{k}": v for k, v in self.probs.items()},
        }
