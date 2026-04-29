"""
Kelly Criterion with:
1. Empirical calibration — bet sizes scale with how accurate your model is
2. Annualized return adjustment — prefer short-dated markets
3. Uncertainty shrinkage — shrink toward 0.5 when calibration error is high
4. Phase-in schedule — starts conservative, scales up with trade count

CRITICAL: Kelly with wrong model_prob = ruin. These adjustments prevent that.
"""
from __future__ import annotations
import math
from core import db
import config
from core.logger import log


def _get_sharpe_multiplier() -> float:
    """
    Adjust Kelly fraction based on recent portfolio Sharpe ratio.
    Uses closed-trade returns from DB to compute rolling Sharpe.

    Range: 0.60x (losing streak) → 1.30x (strong edge confirmed)
    Only kicks in after 10+ closed trades — neutral before that.
    """
    import math as _math
    returns = db.get_recent_trade_returns(limit=30)
    if len(returns) < 10:
        return 1.0

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = _math.sqrt(variance) if variance > 0 else 0.01
    sharpe = mean / std

    if sharpe > 1.5:
        return 1.30
    elif sharpe > 1.0:
        return 1.15
    elif sharpe > 0.5:
        return 1.00
    elif sharpe > 0.0:
        return 0.85
    else:
        return 0.60   # negative Sharpe → halve bets until we diagnose


def _get_kelly_fraction(trade_count: int) -> float:
    """Phase-in Kelly fraction based on number of calibrated trades."""
    phases = sorted(config.KELLY_CALIBRATION_PHASES.items())
    fraction = phases[0][1]
    for min_trades, f in phases:
        if trade_count >= min_trades:
            fraction = f
    return fraction


def _shrink_probability(model_prob: float, calibration_error: float) -> float:
    """
    Shrink model probability toward 0.5 proportional to calibration error.

    When calibration_error = 0 (perfect model): no shrinkage
    When calibration_error = 0.5 (random model): full shrinkage to 0.5

    This prevents over-betting when the model is unreliable.
    """
    shrinkage = min(calibration_error * 2, 1.0)  # 0–1
    return model_prob * (1 - shrinkage) + 0.5 * shrinkage


def _phase_in_multiplier() -> float:
    """
    Linearly ramp position size from PHASE_IN_START_MULT → 1.0 over the
    first PHASE_IN_TRADES LIVE trades. Only applies when DRY_RUN is False.

    Intent: catch catastrophic bugs with $5 instead of $500 on day one
    of a DRY→LIVE transition. A flat-10% size for 20 trades is enough to
    prove the live pipeline works without betting the farm.
    """
    if getattr(config, "DRY_RUN", True):
        return 1.0
    n_phase = int(getattr(config, "PHASE_IN_TRADES", 20))
    start = float(getattr(config, "PHASE_IN_START_MULT", 0.10))
    if n_phase <= 0:
        return 1.0
    # Live trades so far — count closed trades from DB
    try:
        from core import db as _db
        conn = _db.get_conn()
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM trades WHERE pnl IS NOT NULL"
        ).fetchone()
        n_done = int(row["n"]) if row else 0
    except Exception:
        n_done = 0
    if n_done >= n_phase:
        return 1.0
    progress = n_done / n_phase
    return start + (1.0 - start) * progress


def _correlation_downsize(
    portfolio, new_condition_id: str, new_category: str,
) -> float:
    """
    Return a multiplier in (0, 1] that shrinks the Kelly size when the new
    position would be correlated with existing ones.

    Pure-Kelly assumes bet independence. On Polymarket a single event (e.g.
    "Trump wins 2026") spawns dozens of correlated sub-markets; stacking
    them under full Kelly is a ruin path. This caps exposure clusters.

    Rules (empirically tuned, conservative):
      - Each existing position in the SAME category adds 0.12 penalty
        (capped at 0.72). E.g. 3 crypto positions already → new one sized
        at 1.0 − 3*0.12 = 0.64x.
      - Each existing position in a condition_id whose first 16 chars match
        (same event cluster) adds 0.25 penalty.
      - Result is clamped to [0.15, 1.0] — never fully zero out (that's
        the strategy cap's job) but never more than full Kelly.
    """
    if not portfolio or not portfolio.positions:
        return 1.0
    cat_count = 0
    cluster_count = 0
    new_prefix = (new_condition_id or "")[:16]
    for p in portfolio.positions.values():
        p_cat = getattr(p, "category", None) or ""
        if p_cat and new_category and p_cat.lower() == new_category.lower():
            cat_count += 1
        p_cid = getattr(p, "condition_id", "") or ""
        if new_prefix and p_cid[:16] == new_prefix and p_cid != new_condition_id:
            cluster_count += 1
    penalty = cat_count * 0.12 + cluster_count * 0.25
    return max(0.15, min(1.0, 1.0 - penalty))


def compute_kelly(
    model_prob: float,
    market_price: float,
    bankroll: float,
    days_to_resolution: float,
    strategy: str,
    fee_cost_per_dollar: float = 0.0,
    is_maker: bool = False,
    portfolio=None,
    condition_id: str = "",
    category: str = "",
) -> float:
    """
    Returns optimal position size in USD.

    Args:
        model_prob: Your estimated probability of YES resolving
        market_price: Current YES token price
        bankroll: Available capital
        days_to_resolution: Days until market resolves
        strategy: Used to look up calibration stats
        fee_cost_per_dollar: Fee as fraction of position size

    Returns:
        Position size in USD (0 if no edge)
    """
    # Step 1: Get calibration stats for this strategy
    cal = db.get_calibration_stats(strategy)
    trade_count = cal["count"]
    calibration_error = cal["calibration_error"]

    # Step 2: Shrink probability toward 0.5 based on calibration error
    adjusted_prob = _shrink_probability(model_prob, calibration_error)

    # Step 3: Compute net odds (after fees)
    # On a YES buy: win (1 - market_price) per dollar, lose market_price per dollar
    # After fees: win (1 - market_price - fee) per dollar
    # Makers pay 0% fee — use 0 fee_cost regardless of input
    effective_fee = 0.0 if is_maker else fee_cost_per_dollar
    win_per_dollar = (1 - market_price) / market_price   # b in Kelly formula
    # H2 fix: fee_cost_per_dollar는 이미 투자 $1당 수수료 (= rate * p * (1-p))
    # win_per_dollar도 투자 $1당 수익이므로 단위 일치 — 나누기 불필요
    win_after_fees = win_per_dollar - effective_fee

    if win_after_fees <= 0:
        return 0.0

    lose_prob = 1 - adjusted_prob

    # Step 4: Full Kelly fraction
    full_kelly = (adjusted_prob * win_after_fees - lose_prob) / win_after_fees

    if full_kelly <= 0:
        log.debug(f"No edge after calibration: raw={model_prob:.3f} adj={adjusted_prob:.3f} market={market_price:.3f}")
        return 0.0

    # Step 5: Time-horizon adjustment
    # Prediction markets: longer horizon = more uncertainty = smaller bet
    # But we don't annualize aggressively — it over-sizes short-term bets
    days = max(1, days_to_resolution)
    # Gentle boost for short-dated markets (max 2x for same-day markets)
    # C3 fix: 4x는 full Kelly 초과 → 기하 성장률 음수 (파산 경로). 2x로 제한.
    time_mult = min(30.0 / days, 2.0)
    adjusted_kelly = full_kelly * time_mult

    # Step 6: Apply phase-in fraction × Sharpe multiplier
    phase_fraction  = _get_kelly_fraction(trade_count)
    sharpe_mult     = _get_sharpe_multiplier()

    # Step 6b: Correlation-aware downsize. Pure Kelly assumes bet
    # independence; prediction markets cluster heavily (same election,
    # same crypto, same sport). Without this, stacking 10 correlated
    # "independent" bets is really one big bet at 10x size → ruin.
    corr_mult = _correlation_downsize(portfolio, condition_id, category)

    # Step 6c: Live phase-in. First ~20 live trades are aggressively
    # downsized so a silent bug can only burn $5 instead of $500.
    phase_in = _phase_in_multiplier()

    # Near-certain token: price > 0.95 → fee ≈ 0%, outcome near-certain → loosen cap
    # e.g. oracle convergence at p=0.97 with 3% remaining → risk is tiny
    near_certain = market_price > 0.95 or model_prob > 0.97
    hard_cap = 0.15 if near_certain else 0.08   # 15% cap vs standard 8%

    final_fraction  = min(
        adjusted_kelly * phase_fraction * sharpe_mult * corr_mult * phase_in,
        hard_cap,
    )

    # Step 7: Hard cap per market
    max_per_market = bankroll * config.MAX_SINGLE_MARKET_PCT

    size = min(bankroll * final_fraction, max_per_market)

    log.debug(
        f"Kelly: model={model_prob:.3f} adj={adjusted_prob:.3f} "
        f"market={market_price:.3f} edge={full_kelly:.4f} "
        f"time_mult={time_mult:.1f}x corr={corr_mult:.2f}x phase_in={phase_in:.2f}x "
        f"phase={phase_fraction} maker={'Y' if is_maker else 'N'} "
        f"near_certain={'Y' if near_certain else 'N'} cap={hard_cap:.0%} "
        f"cal_trades={trade_count} cal_err={calibration_error:.3f} "
        f"size=${size:.2f}"
    )
    return size


def compute_kelly_for_arb(
    gross_profit_pct: float,
    leg_fail_prob: float,
    leg_fail_loss_pct: float,
    bankroll: float,
) -> float:
    """
    Kelly for internal YES+NO arb with explicit leg risk.

    NOT risk-free. Models the probability that the second leg fails
    and we end up with a naked directional position.

    Args:
        gross_profit_pct: (1 - YES_ask - NO_ask) as fraction
        leg_fail_prob: P(second leg doesn't fill at expected price)
        leg_fail_loss_pct: Expected loss if second leg fails
        bankroll: Available capital
    """
    # EV = p_success * gross_profit - p_fail * leg_fail_loss - fees
    # This is not Kelly directly, but a simple EV-based sizing
    ev = (1 - leg_fail_prob) * gross_profit_pct - leg_fail_prob * leg_fail_loss_pct
    if ev <= 0:
        return 0.0

    # Conservative sizing: never more than 2% bankroll on arb due to execution risk
    # Kelly on EV: f = ev / (1 + ev) approximately for small ev
    kelly_raw = ev / max(gross_profit_pct, 0.001)
    fraction = min(kelly_raw * 0.1, 0.02)  # max 2% bankroll
    return bankroll * fraction
