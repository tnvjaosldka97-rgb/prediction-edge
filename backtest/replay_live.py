"""
6일치 라이브 페이퍼 데이터를 friction.orchestrator 통과시켜 재시뮬.

목적: gateway._simulate_fill의 마찰 0% 가정으로 쌓인 +377%의 진위 판정.
- DB의 trades 56건 + signals 5,460건 + price_history 690만행 사용
- 각 trade의 그 시점 호가창을 mid price + 가정 spread/depth로 합성
- friction.orchestrator로 가상 체결
- 마찰 적용 후 entry price·fill ratio·rejection 등 종합
- 마지막 가용 가격으로 mark-to-market → 새 P&L

원본 라이브 결과와 나란히 비교 출력.
"""
from __future__ import annotations
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.models import OrderBook
from friction.orchestrator import FrictionOrchestrator, SimulatedFill
from friction.latency import LatencyModel
from friction.partial_fill import PartialFillModel
from friction.rejection import RejectionModel
from friction.network_blip import NetworkBlipModel
from friction.clob_quirks import ClobQuirks
from friction.fund_lock import FundLockModel
from friction.slippage import SlippageModel


DB_PATH = ROOT / "prediction_edge.db"
ASSUMED_SPREAD_PCT = 0.02     # mid price 기준 ±1% (실제 Polymarket 평균)
ASSUMED_DEPTH_USD_PER_LEVEL = 500.0   # 각 호가 단계별 가용 USD (보수적)
N_LEVELS = 5


@dataclass
class ReplayResult:
    n_orders_attempted: int = 0
    n_filled: int = 0
    n_partial: int = 0
    n_rejected: int = 0
    rejection_breakdown: dict = field(default_factory=lambda: defaultdict(int))
    total_size_requested_usd: float = 0.0
    total_size_filled_usd: float = 0.0
    total_fees_paid_usd: float = 0.0
    total_slippage_bps: float = 0.0
    total_latency_ms: float = 0.0
    cumulative_pnl: float = 0.0     # 가상 P&L (mark-to-market)
    starting_bankroll: float = 0.0
    final_bankroll: float = 0.0
    n_open_positions: int = 0
    open_positions_value: float = 0.0


def get_price_at(conn: sqlite3.Connection, token_id: str, ts: float) -> float | None:
    """price_history에서 ts 시점 또는 직전의 가격."""
    row = conn.execute(
        "SELECT price FROM price_history WHERE token_id=? AND timestamp<=? ORDER BY timestamp DESC LIMIT 1",
        (token_id, ts),
    ).fetchone()
    return row[0] if row else None


def synthesize_book(token_id: str, mid: float) -> OrderBook:
    """mid price에서 가정 spread/depth로 호가창 합성."""
    half = mid * (ASSUMED_SPREAD_PCT / 2)
    bids = []
    asks = []
    for i in range(N_LEVELS):
        ask_price = round(min(0.999, mid + half + i * mid * 0.005), 3)
        bid_price = round(max(0.001, mid - half - i * mid * 0.005), 3)
        depth_shares = ASSUMED_DEPTH_USD_PER_LEVEL / max(0.001, ask_price)
        asks.append((ask_price, depth_shares))
        bids.append((bid_price, depth_shares))
    return OrderBook(token_id=token_id, bids=bids, asks=asks)


def replay() -> ReplayResult:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 시작 bankroll — 첫 snapshot에서
    first_snap = conn.execute(
        "SELECT total_value, bankroll FROM portfolio_snapshots ORDER BY timestamp ASC LIMIT 1"
    ).fetchone()
    starting_bankroll = float(first_snap["bankroll"]) if first_snap else 75.0

    print(f"[INFO] Starting bankroll: ${starting_bankroll:.2f}")

    # 모든 trade 시간순
    trades = conn.execute(
        "SELECT order_id, condition_id, token_id, side, fill_price, size_shares, "
        "       fee_paid, strategy, timestamp FROM trades ORDER BY timestamp ASC"
    ).fetchall()

    print(f"[INFO] Replaying {len(trades)} trades")

    # 마찰 모델 — 보수적 기본값
    orchestrator = FrictionOrchestrator(
        latency=LatencyModel(mu_ms=200, sigma_ms=120, p_timeout=0.005),
        slippage=SlippageModel(),
        partial_fill=PartialFillModel(gtc_cancel_midpoint=0.3),
        rejection=RejectionModel(
            prob_signature_expired=0.005,
            prob_polygon_rpc_error=0.005,
            prob_market_inactive=0.001,
            prob_insufficient_balance=0.0,
        ),
        network_blip=NetworkBlipModel(blips_per_hour=0.5, mean_duration_sec=15),
        clob_quirks=ClobQuirks(),
        fund_lock=FundLockModel(),
    )

    # blip 사전 생성 (전체 6일치)
    if trades:
        t0 = trades[0]["timestamp"]
        t1 = trades[-1]["timestamp"]
        orchestrator.network_blip.generate(t0, t1)
        print(f"[INFO] Generated {len(orchestrator.network_blip._blips)} network blips over {(t1-t0)/3600:.1f}h")

    result = ReplayResult(starting_bankroll=starting_bankroll, final_bankroll=starting_bankroll)
    bankroll = starting_bankroll
    positions: dict[str, dict] = {}  # token_id -> {shares, entry_price}

    skipped_no_price = 0
    for tr in trades:
        token_id = tr["token_id"]
        ts = tr["timestamp"]
        side = tr["side"]
        original_size_usd = tr["fill_price"] * tr["size_shares"]
        original_price = tr["fill_price"]

        # 그 시점 mid 가격
        mid = get_price_at(conn, token_id, ts)
        if mid is None or mid <= 0:
            skipped_no_price += 1
            continue

        # 호가 합성 + 마찰 통과
        book = synthesize_book(token_id, mid)

        # 사이즈가 너무 크면 호가 깊이로 일부만 통과 가능 — original_size_usd 그대로 시도
        result.n_orders_attempted += 1
        result.total_size_requested_usd += original_size_usd

        fill: SimulatedFill = orchestrator.simulate(
            side=side,
            size_usd=original_size_usd,
            price=mid,                    # 시그널 시점의 mid를 우리 limit price로 가정
            order_type="GTC",             # 라이브에서 대부분 maker GTC
            is_maker=True,                # maker 가정 (fee 0)
            book_at_submit=book,
            submit_ts=ts,
            market_volatility_5m=0.1,
        )

        if not fill.accepted:
            result.n_rejected += 1
            result.rejection_breakdown[fill.rejection_reason or "unknown"] += 1
            continue

        result.n_filled += 1
        if fill.is_partial:
            result.n_partial += 1
        result.total_size_filled_usd += fill.filled_size_usd
        result.total_fees_paid_usd += fill.fee_paid
        result.total_slippage_bps += fill.slippage_bps
        result.total_latency_ms += fill.submit_to_fill_ms

        # 가상 portfolio 갱신
        if side == "BUY":
            bankroll -= fill.filled_size_usd + fill.fee_paid
            if token_id in positions:
                pos = positions[token_id]
                total_shares = pos["shares"] + fill.filled_size_shares
                avg_price = (pos["shares"] * pos["entry_price"] + fill.filled_size_shares * fill.avg_fill_price) / total_shares
                pos["shares"] = total_shares
                pos["entry_price"] = avg_price
            else:
                positions[token_id] = {
                    "shares": fill.filled_size_shares,
                    "entry_price": fill.avg_fill_price,
                }
        else:  # SELL
            bankroll += fill.filled_size_usd - fill.fee_paid
            if token_id in positions:
                pos = positions[token_id]
                pos["shares"] -= fill.filled_size_shares
                if pos["shares"] <= 0.001:
                    del positions[token_id]

    # mark-to-market: 마지막 가용 가격으로
    open_value = 0.0
    for token_id, pos in positions.items():
        # token의 마지막 가격
        last = conn.execute(
            "SELECT price FROM price_history WHERE token_id=? ORDER BY timestamp DESC LIMIT 1",
            (token_id,),
        ).fetchone()
        if last:
            open_value += pos["shares"] * last[0]

    result.final_bankroll = bankroll
    result.cumulative_pnl = (bankroll + open_value) - starting_bankroll
    result.n_open_positions = len(positions)
    result.open_positions_value = open_value

    if skipped_no_price:
        print(f"[WARN] Skipped {skipped_no_price} trades with no price_history")

    return result


def print_report(result: ReplayResult) -> None:
    print()
    print("=" * 70)
    print("  마찰 적용 6일치 재시뮬 결과 (friction-applied replay)")
    print("=" * 70)

    accepted = result.n_filled
    total = result.n_orders_attempted
    fill_rate = accepted / total * 100 if total else 0
    avg_slip = result.total_slippage_bps / accepted if accepted else 0
    avg_lat = result.total_latency_ms / accepted if accepted else 0

    print(f"  주문 시도:           {total}")
    print(f"    체결:             {accepted} ({fill_rate:.1f}%)")
    print(f"    부분 체결:        {result.n_partial}")
    print(f"    거부:             {result.n_rejected}")
    if result.rejection_breakdown:
        for reason, count in sorted(result.rejection_breakdown.items(), key=lambda x: -x[1]):
            print(f"      - {reason:25s} {count}")

    print()
    print(f"  요청 사이즈:         ${result.total_size_requested_usd:.2f}")
    print(f"  체결 사이즈:         ${result.total_size_filled_usd:.2f}")
    print(f"  체결률:              {result.total_size_filled_usd/max(1,result.total_size_requested_usd)*100:.1f}%")
    print(f"  총 수수료:           ${result.total_fees_paid_usd:.4f}")
    print(f"  평균 슬리피지:       {avg_slip:.1f} bps")
    print(f"  평균 레이턴시:       {avg_lat:.0f} ms")

    print()
    print(f"  시작 자본:           ${result.starting_bankroll:.2f}")
    print(f"  최종 cash:           ${result.final_bankroll:.2f}")
    print(f"  보유 포지션:         {result.n_open_positions} ({result.open_positions_value:.2f} USD)")
    print(f"  총 포트폴리오:       ${result.final_bankroll + result.open_positions_value:.2f}")
    print(f"  누적 P&L:            ${result.cumulative_pnl:+.2f} ({result.cumulative_pnl/max(0.01,result.starting_bankroll)*100:+.1f}%)")

    # 비교: 라이브 +377% 데이터
    print()
    print("  ── 비교: 라이브 (마찰 0%) ──")
    print(f"  라이브 결과:         $75 → $358 (+377%)")
    sim_total = result.final_bankroll + result.open_positions_value
    sim_pct = (sim_total / result.starting_bankroll - 1) * 100
    print(f"  마찰 적용 시뮬:      ${result.starting_bankroll:.0f} → ${sim_total:.0f} ({sim_pct:+.1f}%)")

    print()
    print("=" * 70)
    if sim_pct > 50:
        print("  판정: ✅ 알파 살아있음. 마찰 후에도 의미 있는 수익")
    elif sim_pct > 0:
        print("  판정: ⚠️ 알파 약함. 마찰이 대부분 먹음. 추가 검증 필요")
    else:
        print("  판정: ❌ 알파 없음 또는 음수. 라이브 전 진단 필수")
    print("=" * 70)


def main() -> int:
    if not DB_PATH.exists():
        print(f"[FAIL] DB 없음: {DB_PATH}")
        return 1

    print("[INFO] Replaying 6 days of live paper trading through friction model...")
    result = replay()
    print_report(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
