"""
Mark-to-market loop for virtual trades.

Runs in the background, every 15 minutes:
  1. Query virtual_trades that are unresolved (resolved_at IS NULL)
  2. For each, check if the underlying market has resolved (Gamma API)
  3. If resolved, compute realized PnL and update the row
  4. If not, update unrealized PnL with the current mid price

After 9 days of running, this produces a fully-populated virtual P&L
ledger with realized outcomes on resolved markets and live mid-mark on
open ones.
"""
from __future__ import annotations
import asyncio
import json
import time
import httpx

from core import db
from core.logger import log

GAMMA_HOST = "https://gamma-api.polymarket.com"
CHECK_INTERVAL_SEC = 15 * 60   # 15 minutes
MAX_BATCH = 50


async def _fetch_market_state(client: httpx.AsyncClient, condition_id: str):
    """Return (is_closed, winning_token_idx, outcome_prices) or None."""
    try:
        resp = await client.get(
            f"{GAMMA_HOST}/markets",
            params={"condition_ids": condition_id},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("data", [])
        if not data:
            return None
        m = data[0]
        closed = m.get("closed", False)
        op_raw = m.get("outcomePrices", "[]")
        try:
            op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
            prices = [float(x) for x in op]
        except Exception:
            prices = []
        if not closed:
            return (False, None, prices)
        winner = next((i for i, p in enumerate(prices) if p >= 0.99), None)
        return (True, winner, prices)
    except Exception as e:
        log.debug(f"[mtm] fetch error for {condition_id[:8]}: {e}")
        return None


def _settle_trade(row, winner_idx: int, token_yes_first: bool = True) -> float:
    """Compute realized PnL for a virtual BUY trade that has resolved."""
    # We don't know which token is YES/NO without extra metadata, so use
    # the stored token_id. We assume winner_idx 0 = first clob token.
    # If our trade's token corresponds to the winner, exit_price = 1.0; else 0.
    # Simpler: if realized (post-resolution) orderbook mid ~ 1.0 → winner.
    # For now, rely on the Gamma outcomePrices order.
    fill_price = row["fill_price"]
    size_usd = row["size_usd"]
    shares = size_usd / fill_price if fill_price > 0 else 0
    # Without a token→outcomeIdx map stored, we have to guess. Store this
    # at insert time for accuracy (TODO). For now, mark as unknown.
    return 0.0   # placeholder — see periodic_mark_loop for the real logic


async def periodic_mark_loop(stop_event: asyncio.Event, store=None):
    """Long-running coroutine to be spawned from main.py."""
    async with httpx.AsyncClient() as client:
        while not stop_event.is_set():
            try:
                conn = db.get_conn()
                rows = conn.execute(
                    """SELECT id, condition_id, token_id, fill_price, size_usd,
                              side FROM virtual_trades
                       WHERE resolved_at IS NULL
                       ORDER BY fill_ts ASC LIMIT ?""",
                    (MAX_BATCH,),
                ).fetchall()
                for row in rows:
                    state = await _fetch_market_state(client, row["condition_id"])
                    if not state:
                        continue
                    closed, winner_idx, prices = state
                    if not closed or winner_idx is None:
                        # Mark unrealized via market_store if available
                        if store is not None:
                            book = store.get_orderbook(row["token_id"])
                            if book and not book.is_stale():
                                mid = book.mid
                                if mid:
                                    shares = row["size_usd"] / row["fill_price"]
                                    unrealized = (mid - row["fill_price"]) * shares
                                    conn.execute(
                                        "UPDATE virtual_trades SET unrealized_pnl = ?, last_mark_ts = ? WHERE id = ?",
                                        (unrealized, time.time(), row["id"]),
                                    )
                                    conn.commit()
                        continue
                    # Market closed — determine exit price.
                    # The winning token_idx corresponds to the token whose
                    # outcomePrices value is 1.0. We need to know whether OUR
                    # token_id is that one. The gamma market returned both
                    # token_ids in clobTokenIds — need to re-fetch for mapping.
                    try:
                        m_resp = await client.get(
                            f"{GAMMA_HOST}/markets",
                            params={"condition_ids": row["condition_id"]},
                            timeout=10,
                        )
                        m_data = m_resp.json()
                        if isinstance(m_data, dict):
                            m_data = m_data.get("data", [])
                        if not m_data:
                            continue
                        mkt = m_data[0]
                        tids_raw = mkt.get("clobTokenIds", "[]")
                        tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                        tids = [str(t) for t in tids]
                        if row["token_id"] not in tids:
                            continue
                        our_idx = tids.index(row["token_id"])
                        exit_price = 1.0 if our_idx == winner_idx else 0.0
                    except Exception as e:
                        log.debug(f"[mtm] resolve map error: {e}")
                        continue

                    shares = row["size_usd"] / row["fill_price"] if row["fill_price"] > 0 else 0
                    fee = row["fill_price"] * (1 - row["fill_price"]) * 0.02 * shares
                    realized = (exit_price - row["fill_price"]) * shares - fee
                    conn.execute(
                        """UPDATE virtual_trades
                           SET exit_price = ?, realized_pnl = ?, resolved_at = ?
                           WHERE id = ?""",
                        (exit_price, realized, time.time(), row["id"]),
                    )
                    conn.commit()
                    log.info(
                        f"[MTM] resolved {row['condition_id'][:8]} "
                        f"fill={row['fill_price']:.3f} exit={exit_price:.0f} "
                        f"pnl=${realized:+.2f}"
                    )
                    await asyncio.sleep(0.1)   # rate limit
            except Exception as e:
                log.warning(f"[mtm] loop error: {e}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL_SEC)
            except asyncio.TimeoutError:
                pass
