"""
Partial fill model.

주문 타입별 부분 체결 동작:
  - FOK (Fill-or-Kill):   100% 가능하면 체결, 아니면 0
  - IOC (Immediate-or-Cancel): 호가창 깊이만큼만 체결, 나머지 취소
  - GTC (Good-til-Cancel): 즉시 채워질 수 있는 만큼 + 변동성 높으면 취소 확률
"""
from __future__ import annotations
import math
import random
from dataclasses import dataclass


@dataclass
class FillRatioResult:
    ratio: float          # 0.0 ~ 1.0
    canceled_due_to_volatility: bool


class PartialFillModel:
    def __init__(
        self,
        gtc_cancel_steepness: float = 2.0,
        gtc_cancel_midpoint: float = 0.25,
        gtc_partial_when_canceled: tuple[float, float] = (0.0, 0.5),
    ):
        # GTC: vol_5m > midpoint → cancel 확률 sigmoid 상승
        self.gtc_cancel_steepness = gtc_cancel_steepness
        self.gtc_cancel_midpoint = gtc_cancel_midpoint
        self.gtc_partial_when_canceled = gtc_partial_when_canceled

    def compute(
        self,
        order_type: str,             # "FOK" / "IOC" / "GTC"
        size_usd: float,
        depth_at_price_usd: float,
        market_volatility_5m: float = 0.0,
        rng: random.Random | None = None,
    ) -> FillRatioResult:
        r = rng or random

        if order_type == "FOK":
            return FillRatioResult(
                ratio=1.0 if depth_at_price_usd >= size_usd else 0.0,
                canceled_due_to_volatility=False,
            )

        if order_type == "IOC":
            return FillRatioResult(
                ratio=min(1.0, depth_at_price_usd / size_usd) if size_usd > 0 else 0.0,
                canceled_due_to_volatility=False,
            )

        # GTC: 변동성 시그모이드로 취소 확률
        x = self.gtc_cancel_steepness * (market_volatility_5m - self.gtc_cancel_midpoint)
        cancel_p = 1.0 / (1.0 + math.exp(-x))

        if r.random() < cancel_p:
            lo, hi = self.gtc_partial_when_canceled
            return FillRatioResult(
                ratio=r.uniform(lo, hi),
                canceled_due_to_volatility=True,
            )
        # 취소 안 됨 — 호가창 깊이 한도까지
        ratio = min(1.0, depth_at_price_usd / size_usd) if size_usd > 0 else 0.0
        return FillRatioResult(ratio=ratio, canceled_due_to_volatility=False)

    def calibrate(self, observed_ratios: list[float]) -> None:
        """라이브 부분 체결 비율로 cancel midpoint 조정."""
        if len(observed_ratios) < 20:
            return
        # cancel_p ≈ fraction of orders with ratio < 1.0
        cancels = sum(1 for r in observed_ratios if r < 0.95)
        cancel_rate = cancels / len(observed_ratios)
        # sigmoid를 cancel_rate에 맞추는 단순한 휴리스틱
        if cancel_rate > 0 and cancel_rate < 1:
            # logit
            self.gtc_cancel_midpoint = -math.log(1 / cancel_rate - 1) / self.gtc_cancel_steepness

    def to_dict(self) -> dict:
        return {
            "gtc_cancel_steepness": self.gtc_cancel_steepness,
            "gtc_cancel_midpoint": self.gtc_cancel_midpoint,
            "gtc_partial_when_canceled": list(self.gtc_partial_when_canceled),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PartialFillModel":
        return cls(
            gtc_cancel_steepness=d.get("gtc_cancel_steepness", 2.0),
            gtc_cancel_midpoint=d.get("gtc_cancel_midpoint", 0.25),
            gtc_partial_when_canceled=tuple(d.get("gtc_partial_when_canceled", [0.0, 0.5])),
        )
