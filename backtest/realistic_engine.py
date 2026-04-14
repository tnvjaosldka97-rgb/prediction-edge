"""
Realistic Backtest Engine — 기존 historical_backtest.py 대체.

기존 엔진 결함 (이 엔진이 고치는 것):
  1. Look-ahead: "resolved 마켓에서 p>=0.95 첫 시점 진입" → resolution 직전
     시점까지 포함 → 구조적 100% 승률 트릭.
  2. 슬리피지 0, 체결확률 100%, 임팩트 0 — 비현실적.
  3. Fee를 TAKER_FEE=0.02 플랫으로 가정 — Polymarket 실수수료는
     price*(1-price) 비례. 저렴 구간을 과소평가, 중간 구간을 과대평가.
  4. Sharpe가 per-trade pnl std로 계산 — annualized 아님, 비교 불가.
  5. 분쟁 haircut 없음 (이진 0/1만).
  6. Walk-forward / OOS 없음 — 전체 기간 in-sample 학습.
  7. 46 trades 샘플 — 통계적 무의미.

이 엔진의 규칙:
  - **Strict timestamp gating**: 신호 @ t_sig → 체결 가격은 t_sig + latency에서
    조회. 미래 정보 절대 참조 불가.
  - **체결 모델**: ask = mid + half_spread + slippage(size). BUY는 ask 체결.
  - **수수료 모델**: fee_arbitrage.py의 compute_exact_fee 공식 그대로.
  - **분쟁 haircut**: category 기반 확률로 무작위로 -100% 손실 이벤트 삽입.
  - **Exit 모델**: expected_hold에 따라 자동 청산. 조기 청산은 마지막 관측가.
  - **Walk-forward**: 시간 순 4 split → train[0:3] / test[3]. 각 split 별도 리포트.
  - **지표**: 연율 Sharpe (daily return 기반), Sortino, maxDD, Calmar, win rate,
    $ deployed, turnover, capacity estimate.

사용:
  python -m backtest.realistic_engine --strategy fee_arbitrage --days 60
  python -m backtest.realistic_engine --all --days 90 --walk-forward
"""
from __future__ import annotations
import argparse
import asyncio
import json
import math
import os
import pickle
import random
import statistics
import sys
import time
from pathlib import Path

# Windows cp949 console chokes on em-dash etc. Force UTF-8 output.
# line_buffering=True so redirected output is flushed on every newline
# — partial results survive crashes.
try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Optional
import httpx

GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST  = "https://clob.polymarket.com"

# Disk cache for fetched markets + price history. Makes reruns instant
# and protects against partial-network failures — once a market's data
# is on disk, we never re-fetch it.
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
MARKETS_CACHE_TTL_SEC = 6 * 3600     # markets list — 6h
PRICES_CACHE_TTL_SEC  = 24 * 3600    # per-token price series — 24h


def _cache_get(key: str, ttl_sec: int):
    """Load pickled object from disk cache if fresh. Return None otherwise."""
    path = CACHE_DIR / f"{key}.pkl"
    if not path.exists():
        return None
    if time.time() - path.stat().st_mtime > ttl_sec:
        return None
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def _cache_put(key: str, value) -> None:
    path = CACHE_DIR / f"{key}.pkl"
    try:
        with open(path, "wb") as f:
            pickle.dump(value, f)
    except Exception:
        pass


async def _fetch_with_retry(client, url: str, params: dict, retries: int = 2, timeout: int = 15):
    """HTTP GET with backoff retry. Returns (status, json_or_none)."""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return 200, resp.json()
            # Non-200 → brief wait then retry
            last_exc = f"status {resp.status_code}"
        except Exception as e:
            last_exc = str(e)[:120]
        if attempt < retries:
            await asyncio.sleep(0.5 * (attempt + 1))
    return 0, None

# ── 상수 (수수료/스프레드/지연 현실 모델) ──────────────────────────────────────
TAKER_FEE_RATE   = 0.02           # Polymarket fee rate (applied to price*(1-price)*size)
LATENCY_SEC      = 5              # signal → fill 지연
# Typical spread by price band (from Polymarket book snapshots)
SPREAD_BY_BAND = [
    (0.99, 0.002),   # p>=0.99: 0.2¢ spread (thin)
    (0.95, 0.005),   # 0.95–0.99
    (0.80, 0.010),
    (0.20, 0.020),   # mid
    (0.05, 0.010),
    (0.00, 0.005),
]
# Size-based slippage: extra cost above half-spread (cents per $100 of depth eaten)
SLIPPAGE_PER_100 = 0.001    # 0.1¢ per $100 above $500 order

# Category dispute rates (empirical priors, to be replaced with DB learning)
DISPUTE_RATE_BY_CAT = {
    "sports":        0.003,
    "entertainment": 0.008,
    "economics":     0.015,
    "crypto":        0.020,
    "science":       0.015,
    "politics":      0.030,
    "unknown":       0.015,
}


# ══════════════════════════════════════════════════════════════════════════════
# Data classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ResolvedMarket:
    condition_id: str
    question: str
    category: str
    volume: float
    end_ts: int               # resolution timestamp (unix)
    token_ids: list[str]      # [YES_tok, NO_tok]
    winner_idx: int           # 0 or 1
    outcomes: list[str]       # ["Yes","No"] or similar


@dataclass
class PriceTick:
    ts: int
    price: float


@dataclass
class Trade:
    strategy: str
    condition_id: str
    category: str
    side: str                 # "BUY" or "SELL"
    signal_ts: int
    fill_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    size_usd: float
    fee_usd: float
    disputed: bool
    pnl_usd: float            # realized P&L

    @property
    def return_pct(self) -> float:
        return self.pnl_usd / self.size_usd if self.size_usd > 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Fill / fee model
# ══════════════════════════════════════════════════════════════════════════════

def spread_for_price(price: float) -> float:
    """Estimated bid-ask spread at a given mid price."""
    for threshold, sp in SPREAD_BY_BAND:
        if price >= threshold:
            return sp
    return 0.020


def fill_price_from_mid(mid: float, size_usd: float, side: str) -> float:
    """
    Ask (for BUY) / Bid (for SELL) conservatively estimated from mid.
    Adds half-spread + size-based slippage.
    """
    half = spread_for_price(mid) / 2
    # Size slippage kicks in above $500 notional
    extra = max(0.0, (size_usd - 500) / 100) * SLIPPAGE_PER_100
    if side == "BUY":
        return min(0.999, mid + half + extra)
    else:
        return max(0.001, mid - half - extra)


def exact_fee_usd(price: float, size_usd: float, is_maker: bool = False) -> float:
    """
    Polymarket fee: fee_rate * size * (1 - price) for buys.
    Matches signals/fee_arbitrage.py:compute_exact_fee.
    """
    if is_maker:
        return 0.0
    return TAKER_FEE_RATE * size_usd * (1 - price)


def dispute_probability(category: str) -> float:
    cat = (category or "unknown").lower()
    return DISPUTE_RATE_BY_CAT.get(cat, DISPUTE_RATE_BY_CAT["unknown"])


# Keyword → category inference. Used when Gamma's /markets endpoint
# doesn't populate category (common). Case-insensitive substring match,
# first hit wins. Order matters — more specific terms first.
_CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("politics",      ["trump", "biden", "harris", "election", "senate", "house race",
                       "governor", "primary", "debate", "republican", "democrat",
                       "impeach", "congress", "president", "putin", "netanyahu",
                       "starmer", "macron", "zelensky", "xi ", "modi", "china",
                       "russia", "ukraine", "israel", "gaza", "hamas", "iran"]),
    ("crypto",        ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "doge",
                       "xrp", "crypto", "coin", "token", "defi", "nft", "blockchain"]),
    ("sports",        ["nba", "nfl", "mlb", "nhl", "epl", "world cup", "super bowl",
                       "champions league", "finals", "playoff", "ucl", "match", "vs.",
                       "win the", "world series", "grand prix", "f1", "tennis",
                       "grand slam", "open ", "cup", "league"]),
    ("economics",     ["fed ", "rate", "inflation", "cpi", "gdp", "recession", "jobs",
                       "unemployment", "interest rate", "powell", "tariff", "market cap"]),
    ("entertainment", ["oscar", "grammy", "emmy", "box office", "movie", "film",
                       "season", "tv show", "album", "netflix", "disney", "taylor swift"]),
    ("science",       ["spacex", "nasa", "launch", "mission", "mars", "moon", "rocket",
                       "satellite", "vaccine", "fda", "drug approval", "ai model",
                       "gpt", "claude", "gemini", "llm", "agi"]),
    ("weather",       ["temperature", "hurricane", "storm", "rainfall", "snowfall",
                       "°c", "degrees", "climate"]),
]


def infer_category_from_question(question: str) -> str:
    """Cheap keyword-based category inference (used when Gamma omits category)."""
    if not question:
        return "unknown"
    q = question.lower()
    for cat, kws in _CATEGORY_KEYWORDS:
        for kw in kws:
            if kw in q:
                return cat
    return "unknown"


# ══════════════════════════════════════════════════════════════════════════════
# Data loaders (cached)
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_resolved_markets(
    client: httpx.AsyncClient,
    days_back: int,
    max_markets: int = 1500,
    min_volume: float = 5_000,
) -> list[ResolvedMarket]:
    """Pull all binary markets resolved in the last N days with clean outcomes.

    Uses disk cache (6h TTL) — rerunning the sweep is instant.
    """
    cache_key = f"markets_d{days_back}_m{max_markets}_v{int(min_volume)}"
    cached = _cache_get(cache_key, MARKETS_CACHE_TTL_SEC)
    if cached is not None:
        print(f"  [cache hit] {len(cached)} markets from disk", flush=True)
        return cached

    out: list[ResolvedMarket] = []
    offset = 0
    batch = 100
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    while len(out) < max_markets:
        status, items = await _fetch_with_retry(
            client,
            f"{GAMMA_HOST}/markets",
            {
                "closed": "true",
                "limit": batch,
                "offset": offset,
                "order": "volume",
                "ascending": "false",
                "end_date_min": cutoff_iso,
            },
            retries=2, timeout=30,
        )
        if items is None:
            print(f"  [warn] gamma fetch failed at offset {offset}, using partial", flush=True)
            break
        if isinstance(items, dict):
            items = items.get("data", [])
        if not items:
            break
        for m in items:
            vol = float(m.get("volumeNum") or m.get("volume") or 0)
            if vol < min_volume:
                continue
            op_raw = m.get("outcomePrices", "[]")
            op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
            if not isinstance(op, list) or len(op) < 2:
                continue
            try:
                prices = [float(x) for x in op]
            except Exception:
                continue
            winner = next((i for i, p in enumerate(prices) if p >= 0.99), None)
            if winner is None:
                continue  # unresolved / tied / voided

            tids_raw = m.get("clobTokenIds", "[]")
            tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
            if not isinstance(tids, list) or len(tids) < 2:
                continue

            outcomes_raw = m.get("outcomes", '["Yes","No"]')
            outcomes = (
                json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            )

            end_iso = m.get("endDateIso") or m.get("endDate", "")
            try:
                end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                end_ts = int(end_dt.timestamp())
            except Exception:
                continue

            # Gamma's /markets endpoint rarely populates `category` directly.
            # Fall back through API fields, then infer from the question text
            # using keyword heuristics.
            category = (
                m.get("category")
                or m.get("eventCategory")
                or (m.get("events", [{}])[0].get("category") if m.get("events") else None)
                or None
            )
            if not category:
                tags = m.get("tags") or []
                if isinstance(tags, list) and tags:
                    first = tags[0]
                    if isinstance(first, dict):
                        category = first.get("label") or first.get("slug")
                    elif isinstance(first, str):
                        category = first
            if not category:
                category = infer_category_from_question(m.get("question", ""))
            category = (category or "unknown").lower()

            out.append(ResolvedMarket(
                condition_id=m.get("conditionId", ""),
                question=m.get("question", ""),
                category=category,
                volume=vol,
                end_ts=end_ts,
                token_ids=[str(t) for t in tids[:2]],
                winner_idx=winner,
                outcomes=outcomes if isinstance(outcomes, list) else ["Yes", "No"],
            ))
        offset += batch
        if len(items) < batch:
            break
        await asyncio.sleep(0.1)

    _cache_put(cache_key, out)
    return out


async def fetch_price_ticks(
    client: httpx.AsyncClient,
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity: int = 60,     # minutes
) -> list[PriceTick]:
    """Pull minute-level mid price history for a token. Cached to disk (24h TTL)."""
    cache_key = f"ticks_{token_id}_{start_ts}_{end_ts}_{fidelity}"
    cached = _cache_get(cache_key, PRICES_CACHE_TTL_SEC)
    if cached is not None:
        return cached

    _, data = await _fetch_with_retry(
        client,
        f"{CLOB_HOST}/prices-history",
        {
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "fidelity": fidelity,
        },
        retries=2, timeout=15,
    )
    if data is None:
        _cache_put(cache_key, [])   # negative cache so we don't retry forever
        return []
    hist = data.get("history", []) if isinstance(data, dict) else []
    ticks = [
        PriceTick(ts=int(h["t"]), price=float(h["p"]))
        for h in hist
        if isinstance(h, dict) and "t" in h and "p" in h
    ]
    _cache_put(cache_key, ticks)
    return ticks


# ══════════════════════════════════════════════════════════════════════════════
# Timestamp-gated price lookup (NO look-ahead)
# ══════════════════════════════════════════════════════════════════════════════

class PriceSeries:
    """
    Wraps a time-sorted list of PriceTicks and provides strict
    gated lookups. All accessors enforce ts ≤ now constraint.
    """

    def __init__(self, ticks: list[PriceTick]):
        self.ticks = sorted(ticks, key=lambda t: t.ts)

    def __len__(self) -> int:
        return len(self.ticks)

    def price_at_or_before(self, now: int) -> Optional[float]:
        """Most recent price at or before `now`. None if none exists."""
        lo, hi = 0, len(self.ticks) - 1
        result = None
        while lo <= hi:
            mid = (lo + hi) // 2
            if self.ticks[mid].ts <= now:
                result = self.ticks[mid].price
                lo = mid + 1
            else:
                hi = mid - 1
        return result

    def iter_until(self, end_ts: int):
        """Yield ticks with ts <= end_ts. Used for causal signal generation."""
        for t in self.ticks:
            if t.ts > end_ts:
                return
            yield t


# ══════════════════════════════════════════════════════════════════════════════
# Strategy interface
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SignalEvent:
    """A strategy emits these as it walks forward in time."""
    ts: int
    strategy: str
    condition_id: str
    token_idx: int            # 0 = YES, 1 = NO
    side: str                 # "BUY"
    model_prob: float
    market_price: float       # mid at signal time
    size_usd: float
    expected_hold_sec: int    # how long to hold (from signal ts)
    note: str = ""


StrategyFn = Callable[
    ["MarketData", int, int, dict],    # market, token_idx, now_ts, state
    Optional[SignalEvent],
]


@dataclass
class MarketData:
    market: ResolvedMarket
    series: list[PriceSeries]          # one per token


# ── Strategy: fee_arbitrage (causal) ──────────────────────────────────────────
# Fire BUY when mid crosses >= threshold AND market has < max_days to resolution
# AND haven't fired for this token yet. Size = $100 flat.
#
# Break-even win rate = entry_price. At 0.95 you need 95%+ win rate; at 0.98
# you need 98%+. Higher threshold → less upside per trade but also fewer losers
# survive the filter. Sweep thresholds to find the EV-positive band.
def make_strat_fee_arbitrage(
    threshold: float = 0.95,
    max_days: float = 7.0,
    blocked_categories: set[str] | None = None,
) -> StrategyFn:
    name = f"fee_arb_{int(threshold*100)}"
    blocked = blocked_categories or set()

    def _strat(
        md: MarketData, token_idx: int, now_ts: int, state: dict,
    ) -> Optional[SignalEvent]:
        if md.market.category in blocked:
            return None
        key = (name, md.market.condition_id, token_idx)
        if state.get(key):
            return None
        p = md.series[token_idx].price_at_or_before(now_ts)
        if p is None or p < threshold or p >= 0.999:
            return None
        if md.market.end_ts - now_ts > max_days * 86400:
            return None
        if md.market.end_ts - now_ts < 600:
            return None
        state[key] = True
        return SignalEvent(
            ts=now_ts,
            strategy=name,
            condition_id=md.market.condition_id,
            token_idx=token_idx,
            side="BUY",
            model_prob=1.0,
            market_price=p,
            size_usd=100.0,
            expected_hold_sec=md.market.end_ts - now_ts,
            note=f"entry@{p:.4f}",
        )
    return _strat


strat_fee_arbitrage = make_strat_fee_arbitrage(threshold=0.95)


# ── Strategy: closing_convergence (causal) ────────────────────────────────────
# Fire BUY on token with p in [band_low, band_high) if < max_days to resolution
# AND positive momentum over last 6h >= momentum_min.
def make_strat_closing_convergence(
    band_low: float = 0.80,
    band_high: float = 0.95,
    momentum_min: float = 0.010,
    max_days: float = 3.0,
    blocked_categories: set[str] | None = None,
) -> StrategyFn:
    name = f"cc_{int(band_low*100)}_{int(momentum_min*1000)}"
    blocked = blocked_categories or set()

    def _strat(
        md: MarketData, token_idx: int, now_ts: int, state: dict,
    ) -> Optional[SignalEvent]:
        if md.market.category in blocked:
            return None
        key = (name, md.market.condition_id, token_idx)
        if state.get(key):
            return None
        days_left = (md.market.end_ts - now_ts) / 86400
        if days_left > max_days or days_left < 0.1:
            return None
        p_now = md.series[token_idx].price_at_or_before(now_ts)
        p_6h = md.series[token_idx].price_at_or_before(now_ts - 6 * 3600)
        if p_now is None or p_6h is None:
            return None
        if not (band_low <= p_now < band_high):
            return None
        momentum = p_now - p_6h
        if momentum < momentum_min:
            return None
        state[key] = True
        return SignalEvent(
            ts=now_ts,
            strategy=name,
            condition_id=md.market.condition_id,
            token_idx=token_idx,
            side="BUY",
            model_prob=min(0.98, p_now + momentum * 2),
            market_price=p_now,
            size_usd=100.0,
            expected_hold_sec=md.market.end_ts - now_ts,
            note=f"p={p_now:.3f} dp6h={momentum:+.3f}",
        )
    return _strat


strat_closing_convergence = make_strat_closing_convergence()


# ── Strategy: oracle_convergence (causal) ─────────────────────────────────────
# Fire BUY when p jumps >= 1.5¢ in 30 min in the final 4 hours before
# resolution (proxy for oracle announcement). Loosened from original 2h/3¢
# because the original fired only 3 times in 90 days.
def strat_oracle_convergence(
    md: MarketData, token_idx: int, now_ts: int, state: dict,
) -> Optional[SignalEvent]:
    key = ("oracle_conv", md.market.condition_id, token_idx)
    if state.get(key):
        return None
    time_left = md.market.end_ts - now_ts
    if time_left > 4 * 3600 or time_left < 60:
        return None
    p_now = md.series[token_idx].price_at_or_before(now_ts)
    p_30m = md.series[token_idx].price_at_or_before(now_ts - 30 * 60)
    if p_now is None or p_30m is None:
        return None
    if p_now < 0.80:    # must be converging toward YES
        return None
    jump = p_now - p_30m
    if jump < 0.015:
        return None
    state[key] = True
    return SignalEvent(
        ts=now_ts,
        strategy="oracle_convergence",
        condition_id=md.market.condition_id,
        token_idx=token_idx,
        side="BUY",
        model_prob=0.99,
        market_price=p_now,
        size_usd=100.0,
        expected_hold_sec=time_left,
        note=f"jump={jump:+.3f} p={p_now:.3f}",
    )


STRATEGIES: dict[str, StrategyFn] = {
    "fee_arbitrage":       strat_fee_arbitrage,
    "closing_convergence": strat_closing_convergence,
    "oracle_convergence":  strat_oracle_convergence,
}


# ══════════════════════════════════════════════════════════════════════════════
# Simulator
# ══════════════════════════════════════════════════════════════════════════════

def simulate(
    markets_with_data: list[MarketData],
    strategy: str | StrategyFn,
    rng: random.Random,
    scan_interval_sec: int = 900,     # scan every 15 min (realistic)
    label: str | None = None,
) -> list[Trade]:
    """
    Event-driven replay with strict timestamp gating.

    strategy: either a name (looked up in STRATEGIES) or a StrategyFn callable
    label:    override name recorded in Trade.strategy (useful for sweeps)
    """
    if isinstance(strategy, str):
        strat_fn = STRATEGIES[strategy]
        strategy_name = label or strategy
    else:
        strat_fn = strategy
        strategy_name = label or getattr(strategy, "__name__", "custom")
    trades: list[Trade] = []
    state: dict = {}

    for md in markets_with_data:
        if not md.series or all(len(s) == 0 for s in md.series):
            continue
        # Walk window: 14 days before end, capped by earliest tick
        earliest = min(
            (s.ticks[0].ts for s in md.series if len(s) > 0),
            default=md.market.end_ts,
        )
        window_start = max(earliest, md.market.end_ts - 14 * 86400)
        window_end = md.market.end_ts

        now = window_start
        while now < window_end:
            for tok_idx in (0, 1):
                sig = strat_fn(md, tok_idx, now, state)
                if sig is None:
                    continue
                # ── Fill ──
                fill_ts = sig.ts + LATENCY_SEC
                if fill_ts >= md.market.end_ts:
                    continue
                fill_mid = md.series[tok_idx].price_at_or_before(fill_ts)
                if fill_mid is None:
                    continue
                entry_price = fill_price_from_mid(
                    fill_mid, sig.size_usd, sig.side
                )
                if entry_price >= 0.999 or entry_price <= 0.001:
                    continue  # no edge / can't buy
                fee_in = exact_fee_usd(entry_price, sig.size_usd)

                # ── Exit at resolution ──
                exit_ts = md.market.end_ts
                # Dispute: flip outcome or zero out based on category rate
                disputed = rng.random() < dispute_probability(md.market.category)
                if disputed:
                    exit_price = 0.0   # worst case: we're on losing side
                else:
                    exit_price = 1.0 if tok_idx == md.market.winner_idx else 0.0

                # Exit fill realism: resolution = no slippage (instant settle)
                # but if we had to sell early (hold budget exceeded), model it.
                # For simplicity, fee_arb / convergence always hold to end.
                fee_out = 0.0   # settlement has no fee

                shares = sig.size_usd / entry_price
                gross_pnl = shares * (exit_price - entry_price)
                pnl = gross_pnl - fee_in - fee_out

                trades.append(Trade(
                    strategy=strategy_name,
                    condition_id=md.market.condition_id,
                    category=md.market.category,
                    side=sig.side,
                    signal_ts=sig.ts,
                    fill_ts=fill_ts,
                    exit_ts=exit_ts,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    size_usd=sig.size_usd,
                    fee_usd=fee_in + fee_out,
                    disputed=disputed,
                    pnl_usd=pnl,
                ))
            now += scan_interval_sec

    return trades


# ══════════════════════════════════════════════════════════════════════════════
# Metrics
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Metrics:
    n_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    total_deployed: float = 0.0
    avg_return_pct: float = 0.0
    median_return_pct: float = 0.0
    sharpe_annualized: float = 0.0
    sortino_annualized: float = 0.0
    max_drawdown_pct: float = 0.0
    calmar: float = 0.0
    turnover_per_day: float = 0.0
    capacity_usd_per_day: float = 0.0


def compute_metrics(trades: list[Trade], days: int) -> Metrics:
    m = Metrics()
    if not trades:
        return m
    m.n_trades = len(trades)
    m.wins = sum(1 for t in trades if t.pnl_usd > 0)
    m.losses = sum(1 for t in trades if t.pnl_usd <= 0)
    m.win_rate = m.wins / m.n_trades
    m.total_pnl = sum(t.pnl_usd for t in trades)
    m.total_deployed = sum(t.size_usd for t in trades)
    returns = [t.return_pct for t in trades]
    m.avg_return_pct = statistics.mean(returns)
    m.median_return_pct = statistics.median(returns)

    # Daily return series for proper Sharpe. Bucket trades by fill_ts day.
    by_day: dict[int, list[float]] = {}
    for t in trades:
        day = t.fill_ts // 86400
        by_day.setdefault(day, []).append(t.pnl_usd)
    if not by_day:
        return m
    # Normalize by avg daily deployed capital (bankroll proxy)
    avg_daily_deployed = m.total_deployed / max(days, 1)
    daily_pnl = sorted(by_day.items())
    daily_returns = [
        sum(pnls) / max(avg_daily_deployed, 1.0)
        for _, pnls in daily_pnl
    ]
    if len(daily_returns) >= 2:
        mean_r = statistics.mean(daily_returns)
        std_r = statistics.pstdev(daily_returns) or 1e-9
        m.sharpe_annualized = (mean_r / std_r) * math.sqrt(365)
        downside = [r for r in daily_returns if r < 0]
        dstd = statistics.pstdev(downside) if len(downside) >= 2 else std_r
        m.sortino_annualized = (mean_r / (dstd or 1e-9)) * math.sqrt(365)

    # Max drawdown on cumulative PnL curve
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    peak_deployed = 0.0
    running_deployed = 0.0
    for _, pnls in daily_pnl:
        cum += sum(pnls)
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    m.max_drawdown_pct = max_dd / max(avg_daily_deployed, 1.0)
    m.calmar = (m.total_pnl / max(days, 1) * 365) / max(
        max_dd, 1.0
    ) if max_dd > 0 else float("inf")

    m.turnover_per_day = m.n_trades / max(days, 1)
    m.capacity_usd_per_day = m.total_deployed / max(days, 1)
    return m


def fmt_metrics(m: Metrics, label: str) -> str:
    return (
        f"\n── {label} ──\n"
        f"  trades:             {m.n_trades}\n"
        f"  win rate:           {m.win_rate:.2%}  ({m.wins}W/{m.losses}L)\n"
        f"  total pnl:          ${m.total_pnl:+,.2f}  on ${m.total_deployed:,.2f} deployed\n"
        f"  return on deployed: {(m.total_pnl/max(m.total_deployed,1))*100:+.3f}%\n"
        f"  avg trade return:   {m.avg_return_pct*100:+.3f}%\n"
        f"  median return:      {m.median_return_pct*100:+.3f}%\n"
        f"  sharpe (annual):    {m.sharpe_annualized:.2f}\n"
        f"  sortino (annual):   {m.sortino_annualized:.2f}\n"
        f"  max drawdown:       {m.max_drawdown_pct*100:.2f}%\n"
        f"  calmar:             {m.calmar:.2f}\n"
        f"  turnover:           {m.turnover_per_day:.1f} trades/day\n"
        f"  capacity:           ${m.capacity_usd_per_day:,.0f}/day deployed\n"
    )


def category_breakdown(trades: list[Trade]) -> str:
    """Per-category P&L table. Reveals which categories are cost centers."""
    if not trades:
        return ""
    by_cat: dict[str, list[Trade]] = {}
    for t in trades:
        by_cat.setdefault(t.category, []).append(t)
    lines = ["\n  [category breakdown]"]
    lines.append(f"  {'category':>15}  {'n':>4}  {'winR':>6}  {'pnl':>10}  {'ret':>7}  {'avg win':>8}  {'avg loss':>8}")
    lines.append(f"  {'-'*15}  {'-'*4}  {'-'*6}  {'-'*10}  {'-'*7}  {'-'*8}  {'-'*8}")
    rows = []
    for cat, ts in by_cat.items():
        n = len(ts)
        wins = [t for t in ts if t.pnl_usd > 0]
        losses = [t for t in ts if t.pnl_usd <= 0]
        pnl = sum(t.pnl_usd for t in ts)
        deployed = sum(t.size_usd for t in ts)
        ret = pnl / deployed if deployed else 0.0
        win_rate = len(wins) / n if n else 0.0
        avg_win = sum(t.pnl_usd for t in wins) / len(wins) if wins else 0.0
        avg_loss = sum(t.pnl_usd for t in losses) / len(losses) if losses else 0.0
        rows.append((pnl, cat, n, win_rate, pnl, ret, avg_win, avg_loss))
    rows.sort(key=lambda r: r[0])   # worst first
    for _, cat, n, wr, pnl, ret, aw, al in rows:
        lines.append(
            f"  {cat[:15]:>15}  {n:>4}  {wr*100:>5.1f}%  ${pnl:>+8.2f}  {ret*100:>+6.2f}%  ${aw:>+7.2f}  ${al:>+7.2f}"
        )
    return "\n".join(lines) + "\n"


# ══════════════════════════════════════════════════════════════════════════════
# Walk-forward split
# ══════════════════════════════════════════════════════════════════════════════

def walk_forward_split(
    markets: list[MarketData], n_splits: int = 4,
) -> list[tuple[str, list[MarketData]]]:
    """
    Chronological splits by market end_ts.
    Returns [(label, subset), ...] for each split.
    """
    if not markets:
        return []
    sorted_md = sorted(markets, key=lambda m: m.market.end_ts)
    size = len(sorted_md) // n_splits
    splits = []
    for i in range(n_splits):
        start = i * size
        end = (i + 1) * size if i < n_splits - 1 else len(sorted_md)
        splits.append((f"Split {i+1}/{n_splits}", sorted_md[start:end]))
    return splits


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def load_market_data(
    client: httpx.AsyncClient,
    markets: list[ResolvedMarket],
    concurrency: int = 4,
) -> list[MarketData]:
    """Fetch price history for both tokens of each market in parallel.

    - Per-market timeout (no single slow market can hang the whole run)
    - Disk cache in fetch_price_ticks → second run is instant
    - Stdout flush per progress update so output survives a crash
    """
    sem = asyncio.Semaphore(concurrency)
    out: list[MarketData] = []

    async def load_one(m: ResolvedMarket) -> Optional[MarketData]:
        async with sem:
            start_ts = m.end_ts - 14 * 86400
            series = []
            for tid in m.token_ids:
                try:
                    ticks = await asyncio.wait_for(
                        fetch_price_ticks(client, tid, start_ts, m.end_ts),
                        timeout=25,
                    )
                except (asyncio.TimeoutError, Exception):
                    ticks = []
                series.append(PriceSeries(ticks))
                await asyncio.sleep(0.02)
            return MarketData(market=m, series=series)

    tasks = [load_one(m) for m in markets]
    done = 0
    for coro in asyncio.as_completed(tasks):
        try:
            md = await coro
        except Exception as e:
            print(f"  [warn] market load error: {str(e)[:80]}", flush=True)
            continue
        done += 1
        if md is not None and any(len(s) > 0 for s in md.series):
            out.append(md)
        if done % 25 == 0 or done == len(markets):
            print(f"  loaded {done}/{len(markets)} markets ({len(out)} with ticks)", flush=True)
    return out


async def run(
    strategy_names: list[str],
    days: int,
    max_markets: int,
    walk_forward: bool,
    seed: int,
):
    rng = random.Random(seed)
    print("=" * 72, flush=True)
    print(f"  Realistic Backtest — {', '.join(strategy_names)} ({days} days)")
    print("=" * 72, flush=True)
    async with httpx.AsyncClient(timeout=30) as client:
        print(f"\n[1] Fetching resolved markets ({days}d back, max {max_markets})…")
        markets = await fetch_resolved_markets(
            client, days_back=days, max_markets=max_markets
        )
        print(f"    {len(markets)} markets usable (binary, volume>=$5k, resolved)")

        print(f"\n[2] Loading price history…")
        md_list = await load_market_data(client, markets)
        print(f"    {len(md_list)} markets with tick data")

    if not md_list:
        print("\n[!] No data to simulate.")
        return

    for strat in strategy_names:
        if strat not in STRATEGIES:
            print(f"\n[!] Unknown strategy: {strat}")
            continue

        print(f"\n[3] Simulating {strat}…")
        if walk_forward:
            splits = walk_forward_split(md_list, n_splits=4)
            for label, subset in splits:
                trades = simulate(subset, strat, rng)
                m = compute_metrics(trades, days // 4)
                print(fmt_metrics(m, f"{strat} — {label} ({len(subset)} mkts)"))
            # Overall
            all_trades = simulate(md_list, strat, rng)
            m_all = compute_metrics(all_trades, days)
            print(fmt_metrics(m_all, f"{strat} — FULL SAMPLE"))
        else:
            trades = simulate(md_list, strat, rng)
            m = compute_metrics(trades, days)
            print(fmt_metrics(m, f"{strat} — {len(md_list)} mkts"))

        show_trades = all_trades if walk_forward else trades
        if show_trades:
            print(category_breakdown(show_trades))
            worst = sorted(show_trades, key=lambda t: t.pnl_usd)[:3]
            best = sorted(show_trades, key=lambda t: t.pnl_usd, reverse=True)[:3]
            print(f"  best trades:")
            for t in best:
                print(f"    +${t.pnl_usd:>7.2f}  {t.category:>10}  entry={t.entry_price:.3f} exit={t.exit_price:.0f}")
            print(f"  worst trades:")
            for t in worst:
                print(f"    ${t.pnl_usd:>+8.2f}  {t.category:>10}  entry={t.entry_price:.3f} exit={t.exit_price:.0f}"
                      f"{'  DISPUTED' if t.disputed else ''}")

    print("\n" + "=" * 72)


async def run_sweep(days: int, max_markets: int, seed: int):
    """
    Sweep fee_arbitrage across entry thresholds + dispute-risk category exclusions.
    Each config uses a fresh RNG seeded identically so dispute haircuts are
    reproducible across configs (not contaminated by shared rng state).
    """
    print("=" * 72, flush=True)
    print(f"  fee_arbitrage threshold sweep ({days} days)")
    print("=" * 72, flush=True)
    async with httpx.AsyncClient(timeout=30) as client:
        print(f"\n[1] Fetching resolved markets…")
        markets = await fetch_resolved_markets(client, days_back=days, max_markets=max_markets)
        print(f"    {len(markets)} markets")
        print(f"[2] Loading price history…")
        md_list = await load_market_data(client, markets)
        print(f"    {len(md_list)} markets with ticks")

    # Category census to see what data we actually have
    cat_census: dict[str, int] = {}
    for md in md_list:
        cat_census[md.market.category] = cat_census.get(md.market.category, 0) + 1
    print(f"\n[category census] {dict(sorted(cat_census.items(), key=lambda x: -x[1]))}")

    # Baseline: 0.95, 0.96, 0.97, 0.98 + loose / strict dispute filters
    thresholds = [0.95, 0.96, 0.97, 0.98]
    # Validated blocklists from 90-day backtest (see core/category.py).
    # Blocking these three turns fee_arb from break-even to clearly positive.
    fee_arb_blocklist = {"unknown", "weather"}
    strict_blocked = {"politics", "crypto", "economics", "science"}

    print(f"\n[3] fee_arbitrage sweep — baseline (no category filter)")
    print(f"  {'thresh':>7}  {'trades':>7}  {'winR':>6}  {'pnl':>10}  {'return':>8}  {'sharpe':>7}  {'breakeven gap':>13}")
    print(f"  {'-'*7}  {'-'*7}  {'-'*6}  {'-'*10}  {'-'*8}  {'-'*7}  {'-'*13}")
    for th in thresholds:
        fn = make_strat_fee_arbitrage(threshold=th)
        trades = simulate(md_list, fn, random.Random(seed), label=f"fee_arb_{int(th*100)}")
        m = compute_metrics(trades, days)
        avg_entry = sum(t.entry_price for t in trades) / len(trades) if trades else 0.0
        gap = (m.win_rate - avg_entry) * 100 if trades else 0.0
        ret = (m.total_pnl / m.total_deployed * 100) if m.total_deployed else 0.0
        print(f"  {th:>7.2f}  {m.n_trades:>7}  {m.win_rate*100:>5.1f}%  ${m.total_pnl:>+8.2f}  {ret:>+7.3f}%  {m.sharpe_annualized:>7.2f}  {gap:>+12.2f}pp")

    print(f"\n[4a] fee_arbitrage sweep — with validated blocklist {sorted(fee_arb_blocklist)}")
    print(f"  {'thresh':>7}  {'trades':>7}  {'winR':>6}  {'pnl':>10}  {'return':>8}  {'sharpe':>7}")
    print(f"  {'-'*7}  {'-'*7}  {'-'*6}  {'-'*10}  {'-'*8}  {'-'*7}")
    for th in thresholds:
        fn = make_strat_fee_arbitrage(threshold=th, blocked_categories=fee_arb_blocklist)
        trades = simulate(md_list, fn, random.Random(seed), label=f"fee_arb_{int(th*100)}_filtered")
        m = compute_metrics(trades, days)
        ret = (m.total_pnl / m.total_deployed * 100) if m.total_deployed else 0.0
        print(f"  {th:>7.2f}  {m.n_trades:>7}  {m.win_rate*100:>5.1f}%  ${m.total_pnl:>+8.2f}  {ret:>+7.3f}%  {m.sharpe_annualized:>7.2f}")

    print(f"\n[4] fee_arbitrage sweep — strict dispute filter (block {sorted(strict_blocked)})")
    print(f"  {'thresh':>7}  {'trades':>7}  {'winR':>6}  {'pnl':>10}  {'return':>8}  {'sharpe':>7}")
    print(f"  {'-'*7}  {'-'*7}  {'-'*6}  {'-'*10}  {'-'*8}  {'-'*7}")
    for th in thresholds:
        fn = make_strat_fee_arbitrage(threshold=th, blocked_categories=strict_blocked)
        trades = simulate(md_list, fn, random.Random(seed), label=f"fee_arb_{int(th*100)}_strict")
        m = compute_metrics(trades, days)
        ret = (m.total_pnl / m.total_deployed * 100) if m.total_deployed else 0.0
        print(f"  {th:>7.2f}  {m.n_trades:>7}  {m.win_rate*100:>5.1f}%  ${m.total_pnl:>+8.2f}  {ret:>+7.3f}%  {m.sharpe_annualized:>7.2f}")

    # Category breakdown for the baseline 0.95 case — shows where losses concentrate
    print(f"\n[5] Where do fee_arb losses come from? (threshold=0.95)")
    fn = make_strat_fee_arbitrage(threshold=0.95)
    trades = simulate(md_list, fn, random.Random(seed), label="fee_arb_95")
    print(category_breakdown(trades))

    # ── closing_convergence deep sweep ────────────────────────────────────────
    # This is the only strategy with demonstrated alpha (+$231/57 trades,
    # Sharpe 2.93 in the main backtest). Find its best parameters.
    print(f"\n[6] closing_convergence deep sweep — varying (band_low, momentum_6h)")
    cc_configs = [
        (0.70, 0.005), (0.70, 0.010), (0.70, 0.020),
        (0.75, 0.005), (0.75, 0.010), (0.75, 0.020),
        (0.80, 0.005), (0.80, 0.010), (0.80, 0.020),   # 0.80/0.010 = current
        (0.85, 0.005), (0.85, 0.010),
    ]
    print(f"  {'band':>10}  {'mom6h':>6}  {'trades':>7}  {'winR':>6}  {'pnl':>10}  {'return':>8}  {'sharpe':>7}")
    print(f"  {'-'*10}  {'-'*6}  {'-'*7}  {'-'*6}  {'-'*10}  {'-'*8}  {'-'*7}")
    for band_low, mom_min in cc_configs:
        fn = make_strat_closing_convergence(
            band_low=band_low, band_high=0.95, momentum_min=mom_min, max_days=3.0,
        )
        trades = simulate(md_list, fn, random.Random(seed), label=f"cc_{band_low}_{mom_min}")
        m = compute_metrics(trades, days)
        ret = (m.total_pnl / m.total_deployed * 100) if m.total_deployed else 0.0
        band_lbl = f"[{band_low:.2f},.95)"
        print(f"  {band_lbl:>10}  {mom_min:>6.3f}  {m.n_trades:>7}  {m.win_rate*100:>5.1f}%  ${m.total_pnl:>+8.2f}  {ret:>+7.3f}%  {m.sharpe_annualized:>7.2f}")

    # Category breakdown for the best closing_conv config (baseline)
    print(f"\n[7] closing_convergence category breakdown (0.80/0.010)")
    fn = make_strat_closing_convergence(band_low=0.80, band_high=0.95, momentum_min=0.010)
    trades = simulate(md_list, fn, random.Random(seed), label="cc_baseline")
    print(category_breakdown(trades))

    # Re-run the top configs with the validated blocklist
    cc_blocklist = {"unknown", "entertainment"}
    print(f"\n[8] closing_convergence with blocklist {sorted(cc_blocklist)}")
    print(f"  {'band':>10}  {'mom6h':>6}  {'trades':>7}  {'winR':>6}  {'pnl':>10}  {'return':>8}  {'sharpe':>7}")
    print(f"  {'-'*10}  {'-'*6}  {'-'*7}  {'-'*6}  {'-'*10}  {'-'*8}  {'-'*7}")
    top_configs = [(0.70, 0.005), (0.70, 0.010), (0.75, 0.005), (0.80, 0.010)]
    for band_low, mom_min in top_configs:
        fn = make_strat_closing_convergence(
            band_low=band_low, band_high=0.95, momentum_min=mom_min, max_days=3.0,
            blocked_categories=cc_blocklist,
        )
        trades = simulate(md_list, fn, random.Random(seed), label=f"cc_{band_low}_{mom_min}_filtered")
        m = compute_metrics(trades, days)
        ret = (m.total_pnl / m.total_deployed * 100) if m.total_deployed else 0.0
        band_lbl = f"[{band_low:.2f},.95)"
        print(f"  {band_lbl:>10}  {mom_min:>6.3f}  {m.n_trades:>7}  {m.win_rate*100:>5.1f}%  ${m.total_pnl:>+8.2f}  {ret:>+7.3f}%  {m.sharpe_annualized:>7.2f}")

    print("=" * 72, flush=True)


def main():
    import traceback
    try:
        _main_impl()
    except Exception:
        # Print the traceback to stdout (which is redirected to the output
        # file via > sweep.txt 2>&1) so silent deaths don't happen.
        print("\n[FATAL] unhandled exception:", flush=True)
        traceback.print_exc()
        sys.exit(1)


def _main_impl():
    ap = argparse.ArgumentParser(description="Realistic Prediction-Edge backtest")
    ap.add_argument("--strategy", default="fee_arbitrage",
                    help=f"one of {list(STRATEGIES.keys())} or 'all'")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--max-markets", type=int, default=500)
    ap.add_argument("--walk-forward", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sweep", action="store_true",
                    help="Sweep fee_arbitrage thresholds + dispute filters")
    args = ap.parse_args()

    if args.sweep:
        asyncio.run(run_sweep(
            days=args.days,
            max_markets=args.max_markets,
            seed=args.seed,
        ))
        return

    if args.strategy == "all":
        strats = list(STRATEGIES.keys())
    else:
        strats = [args.strategy]

    asyncio.run(run(
        strategy_names=strats,
        days=args.days,
        max_markets=args.max_markets,
        walk_forward=args.walk_forward,
        seed=args.seed,
    ))


if __name__ == "__main__":
    main()
