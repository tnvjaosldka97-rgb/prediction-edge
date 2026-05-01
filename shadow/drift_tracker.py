"""
Adverse-selection tracker.

For every virtual fill, schedule price samples at T+5s / T+60s / T+5min.
A negative drift (price moves against us) quantifies adverse selection —
the "when I buy, the price immediately drops" effect that distinguishes
real edge from information asymmetry.

After 2 weeks of shadow-live, the drift distribution per strategy tells
us whether our signals are genuinely profitable or just chasing fills
into informed counterparty flow.
"""
from __future__ import annotations
import asyncio
from core.logger import log
from shadow.virtual_executor import record_drift_sample

SAMPLE_TIMES_SEC = [5, 60, 300]


async def schedule_drift_samples(
    trade_id: int, token_id: str, store,
):
    """Fire-and-forget background task that samples mid at 5s, 60s, 300s."""
    if trade_id is None or store is None:
        return
    for delay in SAMPLE_TIMES_SEC:
        await asyncio.sleep(delay)
        try:
            book = store.get_orderbook(token_id)
            if book and not book.is_stale():
                mid = book.mid
                if mid and mid > 0:
                    record_drift_sample(trade_id, delay, mid)
        except Exception as e:
            log.debug(f"[shadow-drift] sample error: {e}")
            continue
