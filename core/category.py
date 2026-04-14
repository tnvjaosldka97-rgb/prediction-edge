"""
Keyword-based category inference.

Gamma's /markets endpoint rarely populates the `category` field, so without
inference every market ends up labeled "unknown" — and our backtest showed
that the "unknown" bucket is where losses concentrate (both fee_arbitrage
and closing_convergence bleed there).

This module is the single source of truth for mapping a market question to
a category. Use `infer_category(question)` when `Market.category` is empty.

Categories and their blocklist status (from 90-day backtest 2026-04-14):
  fee_arbitrage:
    winners: crypto, sports, politics, science, entertainment
    LOSERS:  weather (−$38/21 trades), unknown (−$168/13 trades)
  closing_convergence:
    winners: weather, crypto, sports, politics
    LOSERS:  unknown (−$466/14 trades), entertainment (−$67/3 trades)
"""
from __future__ import annotations

# Order matters — more specific keywords first. First hit wins.
_CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("politics", [
        "trump", "biden", "harris", "election", "senate", "house race",
        "governor", "primary", "debate", "republican", "democrat",
        "impeach", "congress", "president", "putin", "netanyahu",
        "starmer", "macron", "zelensky", "xi ", "modi", "china",
        "russia", "ukraine", "israel", "gaza", "hamas", "iran",
        "supreme court", "scotus", "parliament", "cabinet",
    ]),
    ("crypto", [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "doge",
        "xrp", "crypto", "coin", "token", "defi", "nft", "blockchain",
        "binance", "coinbase", "ath", "halving", "etf",
    ]),
    ("sports", [
        "nba", "nfl", "mlb", "nhl", "epl", "world cup", "super bowl",
        "champions league", "finals", "playoff", "ucl", "match", "vs.",
        "win the", "world series", "grand prix", "f1", "tennis",
        "grand slam", "open ", "cup", "league", "mvp", "coach",
    ]),
    ("economics", [
        "fed ", "rate", "inflation", "cpi", "gdp", "recession", "jobs",
        "unemployment", "interest rate", "powell", "tariff", "market cap",
        "s&p 500", "dow jones", "nasdaq",
    ]),
    ("entertainment", [
        "oscar", "grammy", "emmy", "box office", "movie", "film",
        "season", "tv show", "album", "netflix", "disney", "taylor swift",
        "concert", "tour",
    ]),
    ("science", [
        "spacex", "nasa", "launch", "mission", "mars", "moon", "rocket",
        "satellite", "vaccine", "fda", "drug approval", "ai model",
        "gpt", "claude", "gemini", "llm", "agi",
    ]),
    ("weather", [
        "temperature", "hurricane", "storm", "rainfall", "snowfall",
        "°c", "degrees", "climate", "warmest", "coldest",
    ]),
]


def infer_category(question: str) -> str:
    """
    Keyword-based category inference. Returns a category string or
    "unknown" if no keyword matches.

    Cheap (linear scan). Call once per market on ingest, not per signal.
    """
    if not question:
        return "unknown"
    q = question.lower()
    for cat, kws in _CATEGORY_KEYWORDS:
        for kw in kws:
            if kw in q:
                return cat
    return "unknown"


def effective_category(market) -> str:
    """
    Return market.category if non-empty, else infer from question.
    Normalizes to lowercase.
    """
    cat = getattr(market, "category", "") or ""
    if cat:
        return cat.lower()
    question = getattr(market, "question", "") or ""
    return infer_category(question)


# ── Per-strategy category blocklists (from 90-day backtest) ───────────────────
# These are strategies that LOSE money on these categories empirically.
# Re-measure quarterly and update.

BLOCKED_CATEGORIES_FEE_ARB: set[str] = {
    "unknown",    # −$168/13 trades, 84.6% winR (too many bad calls)
    "weather",    # −$38/21 trades, dispute-prone
}

BLOCKED_CATEGORIES_CLOSING_CONV: set[str] = {
    "unknown",       # −$466/14 trades, 57.1% winR — landmine
    "entertainment", # −$67/3 trades, 66.7% winR — too small/noisy
}
