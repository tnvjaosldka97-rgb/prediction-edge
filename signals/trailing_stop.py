"""
Trailing Stop + Position Pyramiding.

원리:
- Winning position이 커지면 추가 진입 (피라미딩) — 강한 momentum 활용
- 가격이 peak 대비 X% 빠지면 자동 부분/전체 exit (트레일링 스탑)
- exit_signal 로직보다 더 적극적

조건:
- 포지션 진입 후 +5% 도달 → trailing 시작
- peak 대비 -3% drop → 50% 청산
- peak 대비 -7% drop → 100% 청산
- 동시에 +10% 도달 시 → 25% 추가 진입 (피라미딩)

호환: existing exit_signal과 별도 시그널 — 트레일링은 빠른 reaction, exit는 carry.
"""
from __future__ import annotations
import asyncio
import time
import uuid
from dataclasses import dataclass

import config


@dataclass
class TrailingState:
    token_id: str
    entry_price: float
    entry_time: float
    peak_price: float           # BUY 기준. SELL이면 trough_price
    pyramid_count: int = 0      # 피라미딩 추가 횟수


class TrailingStopManager:
    def __init__(self, portfolio_state, store, signal_bus):
        self._portfolio = portfolio_state
        self._store = store
        self._bus = signal_bus
        self._states: dict[str, TrailingState] = {}    # token_id → state

    async def start(self):
        from core.logger import log
        from core.models import Signal
        log.info("[trailing_stop] manager started")

        while True:
            try:
                await asyncio.sleep(30)    # 30초 폴링
                if not self._portfolio or not self._store:
                    continue

                for token_id, pos in list(self._portfolio.positions.items()):
                    book = self._store.get_orderbook(token_id)
                    if not book or book.is_stale():
                        continue
                    current = book.mid if (book.bids and book.asks) else pos.current_price
                    if current <= 0:
                        continue

                    state = self._states.get(token_id)
                    if not state:
                        # 새 포지션 발견
                        self._states[token_id] = TrailingState(
                            token_id=token_id,
                            entry_price=pos.avg_entry_price,
                            entry_time=pos.entry_time,
                            peak_price=current,
                        )
                        continue

                    # peak 갱신 (BUY만 처리)
                    if pos.side == "BUY":
                        if current > state.peak_price:
                            state.peak_price = current

                        gain_from_entry = (current - state.entry_price) / state.entry_price
                        drawdown_from_peak = (state.peak_price - current) / state.peak_price

                        # 트레일링 스탑 — peak 대비 drawdown
                        if gain_from_entry > 0.05:    # 5% 이상 수익에서만 트레일링 활성
                            if drawdown_from_peak > 0.07:
                                # 100% 청산 시그널
                                await self._emit_exit(token_id, pos, current,
                                                       size_ratio=1.0, reason="trailing_full")
                                continue
                            elif drawdown_from_peak > 0.03:
                                # 50% 청산
                                await self._emit_exit(token_id, pos, current,
                                                       size_ratio=0.5, reason="trailing_partial")

                        # 피라미딩 — +10% 가속 + 아직 추가 진입 X
                        if gain_from_entry > 0.10 and state.pyramid_count == 0:
                            await self._emit_pyramid(token_id, pos, current)
                            state.pyramid_count += 1

            except Exception as e:
                from core.logger import log
                log.warning(f"[trailing_stop] {e}")
                await asyncio.sleep(30)

    async def _emit_exit(self, token_id, pos, current, size_ratio, reason):
        from core.models import Signal
        from core.logger import log
        sig = Signal(
            signal_id=str(uuid.uuid4()),
            strategy="exit_signal",
            condition_id=pos.condition_id,
            token_id=token_id,
            direction="SELL",
            model_prob=current,
            market_prob=current,
            edge=0.0,
            net_edge=0.0,
            confidence=0.95,
            urgency="HIGH",
            stale_price=current,
            stale_threshold=0.02,
        )
        await self._bus.put(sig)
        log.info(f"[trailing_stop] {reason} {token_id[:8]} @ {current:.4f} (size_ratio={size_ratio})")

    async def _emit_pyramid(self, token_id, pos, current):
        from core.models import Signal
        from core.logger import log
        # 25% 추가 진입
        add_size_usd = pos.size_shares * current * 0.25
        if add_size_usd < config.MIN_ORDER_SIZE_USD:
            return
        sig = Signal(
            signal_id=str(uuid.uuid4()),
            strategy="closing_convergence",    # 기존 strategy 라벨 — 피라미딩은 momentum 활용
            condition_id=pos.condition_id,
            token_id=token_id,
            direction="BUY",
            model_prob=current * 1.05,    # +5% 더 갈 거란 가정
            market_prob=current,
            edge=0.05,
            net_edge=0.04,
            confidence=0.70,
            urgency="MEDIUM",
            stale_price=current,
            stale_threshold=0.02,
        )
        await self._bus.put(sig)
        log.info(f"[trailing_stop] PYRAMID {token_id[:8]} @ {current:.4f} +${add_size_usd:.2f}")
