"""
Virtual Executor — realistic fill simulation using live orderbook.

Given a signal + order, this:
  1. Reads the CURRENT L2 orderbook from market_store (live WS feed)
  2. Walks asks (for BUY) / bids (for SELL) to fill the requested size
  3. Returns the blended fill price + realistic slippage number
  4. Persists a `virtual_trades` row with full snapshot + metadata
  5. Schedules adverse-selection samplers at T+5s/60s/5min

This is a drop-in replacement for gateway._simulate_fill() when running
in shadow mode. It does NOT touch bankroll or positions — that's still
handled by the main fill consumer loop. It returns a Fill object that
goes through the same pipeline as a live fill.
"""
from __future__ import annotations
import asyncio
import json
import time
import uuid
from typing import Optional

import config
from core.models import Order, Fill, OrderBook
from core.logger import log
from core import db


# ── Realistic fill pricing ────────────────────────────────────────────────────

def walk_orderbook(
    book: OrderBook, side: str, size_usd: float
) -> tuple[float, float, float, int]:
    """
    Walk the orderbook to compute realistic fill for a given USD size.

    Returns:
      (avg_fill_price, slippage_vs_top, depth_consumed, levels_touched)

    - avg_fill_price: size-weighted blended price
    - slippage_vs_top: difference between avg and best price (in $)
    - depth_consumed: total size in shares eaten
    - levels_touched: number of price levels we had to walk

    Conservative assumptions:
      - If book is empty or synthetic (depth=500 sentinel), fall back to
        "would take mid + 2¢" as a degenerate estimate.
      - If size exceeds total visible depth, apply a 5¢ penalty on the
        uncovered portion (represents walking into hidden / moving liq).
    """
    levels = book.asks if side == "BUY" else book.bids
    if not levels:
        # No book → degenerate, use mid + wide penalty
        mid = book.mid_price if book.mid_price else 0.5
        return (mid + 0.02 if side == "BUY" else mid - 0.02), 0.02, 0.0, 0

    # Synthetic book detection (market_store uses 500 as sentinel)
    top_size = levels[0][1]
    if top_size >= 490 and len(levels) <= 1:
        mid = book.mid_price or levels[0][0]
        return (levels[0][0] + 0.01 if side == "BUY" else levels[0][0] - 0.01), 0.01, 0.0, 1

    top_price = levels[0][0]
    remaining_usd = size_usd
    total_shares = 0.0
    total_cost = 0.0
    levels_touched = 0

    for price, size in levels:
        if remaining_usd <= 0:
            break
        # At this price, size shares are available = size * price USD of notional
        level_capacity_usd = size * price
        if remaining_usd <= level_capacity_usd:
            shares = remaining_usd / price
            total_shares += shares
            total_cost += shares * price
            remaining_usd = 0.0
            levels_touched += 1
            break
        else:
            total_shares += size
            total_cost += size * price
            remaining_usd -= level_capacity_usd
            levels_touched += 1

    if remaining_usd > 0:
        # Walked off the visible book — apply hidden-liquidity penalty.
        # Model: worst-level price + 5¢ (direction-adjusted).
        worst_price = levels[-1][0]
        penalty_price = (
            min(0.999, worst_price + 0.05)
            if side == "BUY"
            else max(0.001, worst_price - 0.05)
        )
        extra_shares = remaining_usd / penalty_price
        total_shares += extra_shares
        total_cost += remaining_usd

    avg_fill = total_cost / total_shares if total_shares > 0 else top_price
    slippage_vs_top = abs(avg_fill - top_price)
    return avg_fill, slippage_vs_top, total_shares, levels_touched


# ── DB persistence ────────────────────────────────────────────────────────────

def persist_virtual_trade(
    order: Order,
    fill_price: float,
    slippage: float,
    levels_touched: int,
    book_snapshot: dict,
    mid_at_signal: float,
    ask_at_signal: float,
    bid_at_signal: float,
) -> int:
    """Insert a virtual_trades row and return its rowid."""
    conn = db.get_conn()
    cur = conn.execute(
        """INSERT INTO virtual_trades
           (signal_ts, fill_ts, condition_id, token_id, strategy, side,
            size_usd, mid_at_signal, ask_at_signal, bid_at_signal,
            fill_price, slippage, levels_touched, book_snapshot_json,
            category)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            time.time(),
            time.time(),
            order.condition_id,
            order.token_id,
            order.strategy or "",
            order.side,
            order.size_usd,
            mid_at_signal,
            ask_at_signal,
            bid_at_signal,
            fill_price,
            slippage,
            levels_touched,
            json.dumps(book_snapshot),
            "",  # filled by caller
        ),
    )
    conn.commit()
    return cur.lastrowid


def record_drift_sample(
    trade_id: int, seconds_after: int, mid_price: float
) -> None:
    """Store a post-fill price drift sample."""
    conn = db.get_conn()
    col = f"mid_after_{seconds_after}s"
    try:
        conn.execute(
            f"UPDATE virtual_trades SET {col} = ? WHERE id = ?",
            (mid_price, trade_id),
        )
        conn.commit()
    except Exception as e:
        log.debug(f"[shadow] drift sample write error: {e}")


def mark_resolved(trade_id: int, exit_price: float, realized_pnl: float) -> None:
    conn = db.get_conn()
    try:
        conn.execute(
            """UPDATE virtual_trades
               SET exit_price = ?, realized_pnl = ?, resolved_at = ?
               WHERE id = ?""",
            (exit_price, realized_pnl, time.time(), trade_id),
        )
        conn.commit()
    except Exception as e:
        log.debug(f"[shadow] mark resolved error: {e}")


# ── High-level entry point ────────────────────────────────────────────────────

async def virtual_execute(
    order: Order,
    store,
) -> tuple[Optional[Fill], Optional[int]]:
    """
    Execute an order in shadow mode against the live orderbook.

    Returns (Fill, virtual_trade_id). Fill is compatible with the normal
    fill_bus → consumer pipeline. virtual_trade_id is the DB rowid for
    subsequent drift / mark-to-market updates.

    If no orderbook is available (market not subscribed / synthetic),
    falls back to order.price and flags `no_book=True`.
    """
    book: Optional[OrderBook] = store.get_orderbook(order.token_id) if store else None
    if book and not book.is_stale():
        fill_price, slippage, shares, levels = walk_orderbook(
            book, order.side, order.size_usd
        )
        mid = book.mid_price or order.price
        ask = book.best_ask or order.price
        bid = book.best_bid or order.price
        snap = {
            "asks": list(book.asks[:10]),
            "bids": list(book.bids[:10]),
            "ts": time.time(),
        }
    else:
        # Degenerate — no book available at fill time
        fill_price = order.price
        slippage = 0.0
        shares = order.size_usd / order.price
        levels = 0
        mid = ask = bid = order.price
        snap = {"asks": [], "bids": [], "no_book": True, "ts": time.time()}

    fee = fill_price * (1 - fill_price) * config.TAKER_FEE_RATE * shares

    # Persist the virtual trade
    try:
        trade_id = persist_virtual_trade(
            order=order,
            fill_price=fill_price,
            slippage=slippage,
            levels_touched=levels,
            book_snapshot=snap,
            mid_at_signal=mid,
            ask_at_signal=ask,
            bid_at_signal=bid,
        )
        # Attach category if we can resolve it
        try:
            mkt = store.get_market(order.condition_id) if store else None
            from core.category import effective_category as _eff_cat
            cat = _eff_cat(mkt) if mkt else ""
            conn = db.get_conn()
            conn.execute(
                "UPDATE virtual_trades SET category = ? WHERE id = ?",
                (cat, trade_id),
            )
            conn.commit()
        except Exception:
            pass
    except Exception as e:
        log.warning(f"[shadow] persist virtual_trade failed: {e}")
        trade_id = None

    fill = Fill(
        order_id=f"shadow_{uuid.uuid4().hex[:8]}",
        condition_id=order.condition_id,
        token_id=order.token_id,
        side=order.side,
        fill_price=fill_price,
        fill_size=shares,
        fee_paid=fee,
        timestamp=time.time(),
        strategy=order.strategy or "",
    )
    return fill, trade_id
