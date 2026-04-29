"""
Slippage model — 호가창 walking.

기존 realistic_engine의 단순 half-spread 모델 대신, 실제 호가창의
가격×수량 레벨을 차례로 소진하는 walking 방식.

체결 가능 사이즈가 부족하면 부분 체결. PartialFillModel과 자연스럽게 결합.
"""
from __future__ import annotations
from dataclasses import dataclass
from core.models import OrderBook


@dataclass
class WalkResult:
    filled_usd: float          # 실제 체결된 USD
    filled_shares: float
    avg_fill_price: float      # 가중평균 체결가
    levels_consumed: int       # 몇 단계 소진했는지
    slippage_bps: float        # (avg_fill - best) / best * 10000


class SlippageModel:
    def walk(
        self,
        side: str,                 # "BUY" or "SELL"
        size_usd: float,
        book: OrderBook,
    ) -> WalkResult:
        if size_usd <= 0:
            return WalkResult(0.0, 0.0, 0.0, 0, 0.0)

        levels = book.asks if side == "BUY" else book.bids
        if not levels:
            return WalkResult(0.0, 0.0, 0.0, 0, 0.0)

        best = levels[0][0]
        remaining_usd = size_usd
        total_shares = 0.0
        total_cost_usd = 0.0
        consumed = 0

        for price, depth_shares in levels:
            if remaining_usd <= 0 or price <= 0:
                break
            level_usd_capacity = price * depth_shares
            take_usd = min(remaining_usd, level_usd_capacity)
            take_shares = take_usd / price
            total_shares += take_shares
            total_cost_usd += take_usd
            remaining_usd -= take_usd
            consumed += 1

        if total_shares == 0:
            return WalkResult(0.0, 0.0, 0.0, 0, 0.0)

        avg_price = total_cost_usd / total_shares
        # BUY는 avg_price ≥ best (위로 walking), SELL은 ≤ best (아래로)
        slippage_bps = (avg_price - best) / best * 10000 if side == "BUY" else (best - avg_price) / best * 10000
        return WalkResult(
            filled_usd=total_cost_usd,
            filled_shares=total_shares,
            avg_fill_price=avg_price,
            levels_consumed=consumed,
            slippage_bps=slippage_bps,
        )

    def calibrate(self, observed_slippage_bps: list[float]) -> None:
        """슬리피지는 호가창에서 결정론적으로 계산되므로 별도 파라미터 없음.
        대신 라이브에서 우리 walking 모델과 실제 fill 차이를 추적해서
        '추가 마찰 fudge factor'를 보정할 수 있음. 일단 stub."""
        pass
