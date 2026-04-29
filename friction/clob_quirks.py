"""
CLOB quirks — Polymarket 결정론적 규칙.

확률적 마찰이 아니라 "이렇게 하면 무조건 거부됨"인 규칙들:
- tick_size 라운딩 (0.001 단위)
- min_order_usd ($1)
- min_size_shares (5 shares)
- 가격 범위 [0.001, 0.999] (0이나 1 끝값은 매칭 X)
- max_levels_to_walk (호가 10단계 이상 깊이 들어가는 큰 주문은 partial)

orchestrator는 이 모듈을 가장 먼저 호출해서 발사 전 normalize/early-reject.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class PolymarketRules:
    tick_size: float = 0.001
    min_order_usd: float = 1.0
    min_size_shares: float = 5.0
    price_min: float = 0.001
    price_max: float = 0.999
    max_levels_walk: int = 10
    signature_validity_sec: float = 60.0


@dataclass
class QuirkResult:
    accepted: bool
    rejection_reason: Optional[str] = None
    normalized_price: float = 0.0
    normalized_size_usd: float = 0.0


class ClobQuirks:
    def __init__(self, rules: Optional[PolymarketRules] = None):
        self.rules = rules or PolymarketRules()

    def normalize_and_check(
        self,
        price: float,
        size_usd: float,
    ) -> QuirkResult:
        """발사 전 결정론적 검증 + 정규화."""
        rules = self.rules

        # 1. 가격 범위
        if price < rules.price_min or price > rules.price_max:
            return QuirkResult(False, "price_out_of_range")

        # 2. tick 라운딩
        normalized_price = round(price / rules.tick_size) * rules.tick_size
        normalized_price = round(normalized_price, 6)  # float 정밀도

        # 3. min size USD
        if size_usd < rules.min_order_usd:
            return QuirkResult(False, "min_size_usd")

        # 4. min size shares
        shares = size_usd / normalized_price if normalized_price > 0 else 0
        if shares < rules.min_size_shares:
            return QuirkResult(False, "min_size_shares")

        return QuirkResult(
            accepted=True,
            rejection_reason=None,
            normalized_price=normalized_price,
            normalized_size_usd=size_usd,
        )

    def to_dict(self) -> dict:
        return {
            "tick_size": self.rules.tick_size,
            "min_order_usd": self.rules.min_order_usd,
            "min_size_shares": self.rules.min_size_shares,
            "price_min": self.rules.price_min,
            "price_max": self.rules.price_max,
            "max_levels_walk": self.rules.max_levels_walk,
            "signature_validity_sec": self.rules.signature_validity_sec,
        }
