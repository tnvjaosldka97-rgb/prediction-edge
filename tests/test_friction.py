"""마찰 모델 단위 테스트."""
import random
import pytest
from core.models import OrderBook
from friction.orchestrator import FrictionOrchestrator, SimulatedFill
from friction.latency import LatencyModel
from friction.slippage import SlippageModel
from friction.partial_fill import PartialFillModel
from friction.rejection import RejectionModel
from friction.network_blip import NetworkBlipModel
from friction.clob_quirks import ClobQuirks
from friction.fund_lock import FundLockModel


def make_book(token_id="t1") -> OrderBook:
    """가벼운 호가창 — bids/asks 5단계."""
    return OrderBook(
        token_id=token_id,
        bids=[(0.49, 1000.0), (0.48, 2000.0), (0.47, 3000.0), (0.46, 4000.0), (0.45, 5000.0)],
        asks=[(0.51, 1000.0), (0.52, 2000.0), (0.53, 3000.0), (0.54, 4000.0), (0.55, 5000.0)],
    )


# ── LatencyModel ─────────────────────────────────────────────────────────────

def test_latency_within_range():
    m = LatencyModel(mu_ms=200, sigma_ms=100, p_timeout=0.0)
    samples = [m.sample().delay_ms for _ in range(1000)]
    mean = sum(samples) / len(samples)
    # 평균이 mu에 가까워야 (±30%)
    assert 140 < mean < 260, f"mean={mean}"


def test_latency_timeout():
    m = LatencyModel(p_timeout=1.0)  # 항상 timeout
    s = m.sample()
    assert s.timed_out
    assert s.delay_ms == 10_000


def test_latency_calibrate():
    m = LatencyModel(mu_ms=200, sigma_ms=100)
    # 라이브 trace에서 평균 500ms 관측됐다면
    observed = [400.0 + i for i in range(200)]  # mean ~ 499.5
    m.calibrate(observed)
    assert 480 < m.mu_ms < 520


# ── SlippageModel ────────────────────────────────────────────────────────────

def test_slippage_buy_within_one_level():
    m = SlippageModel()
    book = make_book()
    # asks[0] = (0.51, 1000 shares) = 510 USD 깊이
    r = m.walk("BUY", size_usd=100, book=book)
    assert r.filled_shares == pytest.approx(100 / 0.51, rel=1e-3)
    assert r.avg_fill_price == pytest.approx(0.51)
    assert r.levels_consumed == 1
    assert r.slippage_bps == pytest.approx(0.0, abs=1e-6)


def test_slippage_buy_walks_multiple_levels():
    m = SlippageModel()
    book = make_book()
    # 첫 레벨 510 USD + 두번째 레벨 1040 USD = 1550 USD까지 walking 가능
    r = m.walk("BUY", size_usd=800, book=book)
    assert r.levels_consumed >= 2
    # 평균가는 0.51보다 비싸고 0.52보다 가벼움
    assert 0.51 < r.avg_fill_price < 0.52
    assert r.slippage_bps > 0


def test_slippage_sell_walks_bids():
    m = SlippageModel()
    book = make_book()
    r = m.walk("SELL", size_usd=300, book=book)
    assert r.avg_fill_price == pytest.approx(0.49)
    assert r.slippage_bps == pytest.approx(0.0, abs=1e-6)


def test_slippage_empty_book():
    m = SlippageModel()
    book = OrderBook(token_id="t", bids=[], asks=[])
    r = m.walk("BUY", 100, book)
    assert r.filled_shares == 0


# ── PartialFillModel ─────────────────────────────────────────────────────────

def test_partial_fill_fok_full_or_zero():
    m = PartialFillModel()
    # 깊이가 충분 — 100% 체결
    r = m.compute("FOK", size_usd=50, depth_at_price_usd=200, market_volatility_5m=0.5)
    assert r.ratio == 1.0
    # 깊이 부족 — 0% 체결
    r = m.compute("FOK", size_usd=200, depth_at_price_usd=50, market_volatility_5m=0.5)
    assert r.ratio == 0.0


def test_partial_fill_ioc_capped_by_depth():
    m = PartialFillModel()
    r = m.compute("IOC", size_usd=200, depth_at_price_usd=50)
    assert r.ratio == pytest.approx(0.25)


def test_partial_fill_gtc_low_volatility():
    # gtc_cancel_steepness=2, midpoint=0.5, vol=0 → sigmoid(-1) ≈ 27% cancel
    # 즉 ~73% 풀필. 100 표본 기준 60 이상이면 정상 (sampling 분산 고려)
    m = PartialFillModel(gtc_cancel_midpoint=0.5)
    rng = random.Random(42)
    ratios = [m.compute("GTC", 100, 200, market_volatility_5m=0.0, rng=rng).ratio for _ in range(100)]
    full_count = sum(1 for r in ratios if r >= 0.99)
    assert full_count > 60, f"full_count={full_count}"


# ── RejectionModel ───────────────────────────────────────────────────────────

def test_rejection_min_size():
    m = RejectionModel(min_size_usd=1.0, prob_signature_expired=0, prob_polygon_rpc_error=0,
                       prob_market_inactive=0, prob_insufficient_balance=0)
    r = m.check(size_usd=0.5, price=0.5, submit_ts=1000, latency_ms=200)
    assert r.rejected
    assert r.reason == "min_size"


def test_rejection_tick_size():
    m = RejectionModel(tick_size=0.001, prob_signature_expired=0, prob_polygon_rpc_error=0,
                       prob_market_inactive=0, prob_insufficient_balance=0)
    # 0.5005는 0.001 단위에 안 맞음
    r = m.check(size_usd=10, price=0.5005, submit_ts=1000, latency_ms=200)
    assert r.rejected
    assert r.reason == "tick_size"


def test_rejection_signature_expired():
    m = RejectionModel(prob_signature_expired=0, prob_polygon_rpc_error=0,
                       prob_market_inactive=0, prob_insufficient_balance=0)
    # latency 60초 넘으면 sig 만료 결정론
    r = m.check(size_usd=10, price=0.5, submit_ts=1000, latency_ms=70_000)
    assert r.rejected
    assert r.reason == "signature_expired"


def test_rejection_pass_through():
    m = RejectionModel(prob_signature_expired=0, prob_polygon_rpc_error=0,
                       prob_market_inactive=0, prob_insufficient_balance=0)
    r = m.check(size_usd=10, price=0.5, submit_ts=1000, latency_ms=200)
    assert not r.rejected


def test_rejection_rate_limit():
    m = RejectionModel(rate_limit_capacity=2, rate_limit_per_sec=1,
                       prob_signature_expired=0, prob_polygon_rpc_error=0,
                       prob_market_inactive=0, prob_insufficient_balance=0)
    # 같은 시점에 3번 → 첫 2번 통과, 3번째 rate_limit
    ts = 1000.0
    r1 = m.check(size_usd=10, price=0.5, submit_ts=ts, latency_ms=200)
    r2 = m.check(size_usd=10, price=0.5, submit_ts=ts, latency_ms=200)
    r3 = m.check(size_usd=10, price=0.5, submit_ts=ts, latency_ms=200)
    assert not r1.rejected
    assert not r2.rejected
    assert r3.rejected and r3.reason == "rate_limit"


# ── NetworkBlipModel ─────────────────────────────────────────────────────────

def test_blip_generation_roughly_correct_count():
    m = NetworkBlipModel(blips_per_hour=10, mean_duration_sec=20)
    rng = random.Random(0)
    m.generate(0, 3600, rng=rng)
    # 시간당 10번 평균 → 5~20 사이 정상
    assert 3 <= len(m._blips) <= 25


def test_blip_covers_interval():
    m = NetworkBlipModel()
    # 수동으로 blip 주입
    from friction.network_blip import Blip
    m._blips = [Blip(100, 120)]
    assert m.covers_interval(110, 115)
    assert m.covers_interval(50, 105)
    assert not m.covers_interval(0, 50)
    assert not m.covers_interval(150, 200)


# ── ClobQuirks ───────────────────────────────────────────────────────────────

def test_quirks_tick_rounding():
    q = ClobQuirks()
    r = q.normalize_and_check(0.5004, 100)
    assert r.accepted
    assert r.normalized_price == pytest.approx(0.5)


def test_quirks_min_size_usd():
    q = ClobQuirks()
    r = q.normalize_and_check(0.5, 0.5)
    assert not r.accepted
    assert r.rejection_reason == "min_size_usd"


def test_quirks_min_shares():
    q = ClobQuirks()
    # $1 / 0.001 = 1000 shares ✓ 통과해야 함
    r = q.normalize_and_check(0.001, 1.0)
    # 1.0 USD / 0.001 = 1000 shares > 5 ✓
    assert r.accepted

    # 너무 작은 size + 높은 price → shares 부족
    r = q.normalize_and_check(0.99, 1.0)
    # 1 USD / 0.99 = 1.01 shares < 5
    assert not r.accepted
    assert r.rejection_reason == "min_size_shares"


def test_quirks_price_range():
    q = ClobQuirks()
    r = q.normalize_and_check(0.0001, 100)
    assert not r.accepted
    r = q.normalize_and_check(0.9999, 100)
    assert not r.accepted


# ── FundLockModel ────────────────────────────────────────────────────────────

def test_fund_lock_first_approve_extra():
    m = FundLockModel(base_finality_sec=30, first_approve_extra_sec=30, gas_spike_prob=0)
    r1 = m.settle()
    assert r1.is_first_approve
    assert r1.settle_delay_sec == 60
    r2 = m.settle()
    assert not r2.is_first_approve
    assert r2.settle_delay_sec == 30


def test_fund_lock_gas_spike():
    m = FundLockModel(base_finality_sec=30, first_approve_extra_sec=0, gas_spike_prob=1.0,
                      gas_spike_extra_min_sec=60, gas_spike_extra_max_sec=60)
    r = m.settle()
    assert r.gas_spike_occurred
    assert r.settle_delay_sec == 90  # 30 + 60


# ── Orchestrator (통합) ───────────────────────────────────────────────────────

def test_orchestrator_happy_path():
    o = FrictionOrchestrator(
        rejection=RejectionModel(prob_signature_expired=0, prob_polygon_rpc_error=0,
                                  prob_market_inactive=0, prob_insufficient_balance=0),
        network_blip=NetworkBlipModel(blips_per_hour=0),  # 아무 blip 없음
        latency=LatencyModel(p_timeout=0),
    )
    book = make_book()
    rng = random.Random(0)
    fill = o.simulate(
        side="BUY",
        size_usd=100,
        price=0.51,
        order_type="GTC",
        is_maker=False,
        book_at_submit=book,
        submit_ts=1000.0,
        market_volatility_5m=0.1,
        rng=rng,
    )
    assert fill.accepted
    assert fill.rejection_reason is None
    assert fill.filled_size_usd > 0
    assert fill.avg_fill_price >= 0.51
    assert fill.submit_to_fill_ms > 0
    assert fill.settle_delay_sec > 0
    assert fill.fee_paid > 0


def test_orchestrator_rejects_min_size():
    o = FrictionOrchestrator()
    book = make_book()
    fill = o.simulate(
        side="BUY",
        size_usd=0.5,  # min_order_usd=1 미만
        price=0.51,
        order_type="GTC",
        is_maker=False,
        book_at_submit=book,
        submit_ts=1000.0,
    )
    assert not fill.accepted
    assert fill.rejection_reason in ("min_size_usd", "min_size", "min_size_shares")


def test_orchestrator_network_blip_during_latency():
    o = FrictionOrchestrator(
        rejection=RejectionModel(prob_signature_expired=0, prob_polygon_rpc_error=0,
                                  prob_market_inactive=0, prob_insufficient_balance=0),
        latency=LatencyModel(mu_ms=500, sigma_ms=10, p_timeout=0),
    )
    # 강제로 1000~1001초 구간에 blip 주입
    from friction.network_blip import Blip
    o.network_blip._blips = [Blip(1000.0, 1001.0)]
    book = make_book()
    rng = random.Random(42)
    fill = o.simulate(
        side="BUY",
        size_usd=100,
        price=0.51,
        order_type="GTC",
        is_maker=False,
        book_at_submit=book,
        submit_ts=1000.0,
        rng=rng,
    )
    assert not fill.accepted
    assert fill.rejection_reason == "network_blip"
    assert fill.network_blip_during


def test_orchestrator_partial_fill_when_book_thin():
    o = FrictionOrchestrator(
        rejection=RejectionModel(prob_signature_expired=0, prob_polygon_rpc_error=0,
                                  prob_market_inactive=0, prob_insufficient_balance=0),
        network_blip=NetworkBlipModel(blips_per_hour=0),
        latency=LatencyModel(p_timeout=0),
    )
    # 호가 매우 얕음
    thin_book = OrderBook(
        token_id="t",
        bids=[(0.49, 10)],
        asks=[(0.51, 10)],  # 5.1 USD 깊이만
    )
    rng = random.Random(0)
    # IOC 200 USD — 5.1만 채워지고 나머지 cancel
    fill = o.simulate(
        side="BUY",
        size_usd=200,
        price=0.51,
        order_type="IOC",
        is_maker=False,
        book_at_submit=thin_book,
        submit_ts=1000.0,
        rng=rng,
    )
    assert fill.accepted
    assert fill.is_partial
    assert fill.filled_size_usd < 200
