"""
Tax tracker — 한국 거주자 가상자산 양도소득세 자동 계산.

2025년 1월부터 가상자산 양도소득 22% 과세 시행 (기본공제 250만원/년).
모든 trade를 FIFO 매칭 + USD→KRW 시점 환율 적용으로 양도손익 계산.

매매:
- 매수: USDC 사용 → 시점 USD→KRW 환산하여 취득가액 기록 (FIFO 큐 푸시)
- 매도: USDC 회수 → FIFO에서 매수 취득가액 매칭 → 양도차익 = 매도가 - 취득가

연간 정산 자료 export → 종소세 신고 시 사용.
"""
from __future__ import annotations
import json
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import config


@dataclass
class TaxLot:
    """FIFO 큐의 단위 — 매수 1건이 1 lot."""
    trade_id: int
    token_id: str
    acquired_at: float
    shares: float
    cost_basis_krw: float       # 취득 시점 KRW 환산
    cost_basis_usdc: float
    fx_rate_at_buy: float       # USDC/KRW
    strategy: str = ""


@dataclass
class TaxableEvent:
    """매도 1건 → 양도손익 발생."""
    sell_trade_id: int
    matched_buy_trade_id: int
    token_id: str
    sold_at: float
    shares: float
    sale_proceeds_krw: float
    cost_basis_krw: float
    capital_gain_krw: float
    holding_period_days: float
    short_term: bool = True       # 한국은 단기/장기 구분 X (전부 양도세 22%)


def _ensure_table():
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS tax_lots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id        INTEGER NOT NULL,
        token_id        TEXT NOT NULL,
        acquired_at     REAL NOT NULL,
        shares          REAL NOT NULL,
        remaining_shares REAL NOT NULL,
        cost_basis_krw  REAL NOT NULL,
        cost_basis_usdc REAL NOT NULL,
        fx_rate_at_buy  REAL NOT NULL,
        strategy        TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_taxlot_token ON tax_lots(token_id, acquired_at);
    CREATE INDEX IF NOT EXISTS idx_taxlot_remaining ON tax_lots(remaining_shares) WHERE remaining_shares > 0;

    CREATE TABLE IF NOT EXISTS taxable_events (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        sell_trade_id       INTEGER NOT NULL,
        matched_buy_trade_id INTEGER,
        token_id            TEXT NOT NULL,
        sold_at             REAL NOT NULL,
        shares              REAL NOT NULL,
        sale_proceeds_krw   REAL NOT NULL,
        cost_basis_krw      REAL NOT NULL,
        capital_gain_krw    REAL NOT NULL,
        holding_period_days REAL,
        fx_rate_at_sell     REAL,
        notes               TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_tax_events_at ON taxable_events(sold_at);
    """)
    conn.commit()
    conn.close()


def get_usd_krw_rate() -> float:
    """USD→KRW 시점 환율. 캐싱 5분."""
    cached = getattr(get_usd_krw_rate, "_cache", None)
    if cached and time.time() - cached[0] < 300:
        return cached[1]

    # 무료 공개 API들 시도
    try:
        import httpx
        # 1) exchangerate.host
        r = httpx.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        if r.status_code == 200:
            rate = r.json().get("rates", {}).get("KRW", 0)
            if rate > 0:
                get_usd_krw_rate._cache = (time.time(), rate)
                return rate
    except Exception:
        pass

    try:
        import httpx
        # 2) Upbit USDT/KRW (USDC와 거의 동일)
        r = httpx.get("https://api.upbit.com/v1/ticker?markets=KRW-USDT", timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data and "trade_price" in data[0]:
                rate = float(data[0]["trade_price"])
                get_usd_krw_rate._cache = (time.time(), rate)
                return rate
    except Exception:
        pass

    # 폴백 — 보수적 기본값
    return 1400.0


def record_buy(trade_id: int, token_id: str, shares: float, fill_price: float,
               strategy: str = "") -> None:
    """매수 trade → tax_lot 추가."""
    _ensure_table()
    cost_usdc = shares * fill_price
    fx_rate = get_usd_krw_rate()
    cost_krw = cost_usdc * fx_rate

    conn = sqlite3.connect(config.DB_PATH)
    conn.execute(
        "INSERT INTO tax_lots (trade_id, token_id, acquired_at, shares, "
        "remaining_shares, cost_basis_krw, cost_basis_usdc, fx_rate_at_buy, strategy) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (trade_id, token_id, time.time(), shares, shares, cost_krw, cost_usdc, fx_rate, strategy),
    )
    conn.commit()
    conn.close()


def record_sell(trade_id: int, token_id: str, shares: float, fill_price: float,
                 strategy: str = "") -> Optional[TaxableEvent]:
    """매도 trade → FIFO 매칭 → taxable_event 생성."""
    _ensure_table()
    sale_usdc = shares * fill_price
    fx_rate = get_usd_krw_rate()
    sale_krw = sale_usdc * fx_rate

    conn = sqlite3.connect(config.DB_PATH)
    # FIFO — 가장 오래된 remaining > 0 lot부터
    rows = conn.execute(
        "SELECT id, trade_id, acquired_at, remaining_shares, cost_basis_krw, cost_basis_usdc "
        "FROM tax_lots WHERE token_id=? AND remaining_shares > 0 ORDER BY acquired_at ASC",
        (token_id,)
    ).fetchall()

    if not rows:
        conn.close()
        return None

    remaining_to_match = shares
    matched_lots = []

    for lot_id, buy_trade_id, acquired_at, lot_remaining, cost_krw, cost_usdc in rows:
        if remaining_to_match <= 0:
            break
        take = min(remaining_to_match, lot_remaining)
        # 비례 배분 — take / lot_remaining 비율로 cost 가져옴
        ratio = take / lot_remaining if lot_remaining > 0 else 0
        cost_taken_krw = cost_krw * ratio
        cost_taken_usdc = cost_usdc * ratio

        new_lot_remaining = lot_remaining - take
        new_lot_cost_krw = cost_krw - cost_taken_krw
        new_lot_cost_usdc = cost_usdc - cost_taken_usdc

        conn.execute(
            "UPDATE tax_lots SET remaining_shares=?, cost_basis_krw=?, cost_basis_usdc=? WHERE id=?",
            (new_lot_remaining, new_lot_cost_krw, new_lot_cost_usdc, lot_id),
        )
        matched_lots.append({
            "buy_trade_id": buy_trade_id,
            "acquired_at": acquired_at,
            "shares": take,
            "cost_krw": cost_taken_krw,
        })
        remaining_to_match -= take

    if not matched_lots:
        conn.close()
        return None

    # taxable_event 1건으로 통합 (가장 오래된 매수와 매칭으로 기록)
    matched_total_krw = sum(m["cost_krw"] for m in matched_lots)
    matched_total_shares = sum(m["shares"] for m in matched_lots)

    # 매도 비례 배분 (실제 매도 사이즈 vs 매칭된 사이즈)
    if matched_total_shares < shares:
        # 보유 부족 — 매칭된 만큼만 과세 이벤트
        sale_proceeds_for_matched = sale_krw * (matched_total_shares / shares)
    else:
        sale_proceeds_for_matched = sale_krw

    capital_gain = sale_proceeds_for_matched - matched_total_krw
    oldest_buy = matched_lots[0]
    holding_days = (time.time() - oldest_buy["acquired_at"]) / 86400

    conn.execute(
        "INSERT INTO taxable_events (sell_trade_id, matched_buy_trade_id, token_id, "
        "sold_at, shares, sale_proceeds_krw, cost_basis_krw, capital_gain_krw, "
        "holding_period_days, fx_rate_at_sell, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (trade_id, oldest_buy["buy_trade_id"], token_id, time.time(),
         matched_total_shares, sale_proceeds_for_matched, matched_total_krw,
         capital_gain, holding_days, fx_rate,
         json.dumps({"matched_lots": len(matched_lots), "strategy": strategy})),
    )
    conn.commit()
    conn.close()

    return TaxableEvent(
        sell_trade_id=trade_id,
        matched_buy_trade_id=oldest_buy["buy_trade_id"],
        token_id=token_id,
        sold_at=time.time(),
        shares=matched_total_shares,
        sale_proceeds_krw=sale_proceeds_for_matched,
        cost_basis_krw=matched_total_krw,
        capital_gain_krw=capital_gain,
        holding_period_days=holding_days,
    )


def annual_summary(year: int) -> dict:
    """연간 양도세 신고 자료."""
    _ensure_table()
    start = datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp()

    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT capital_gain_krw, sale_proceeds_krw, cost_basis_krw "
        "FROM taxable_events WHERE sold_at >= ? AND sold_at < ?",
        (start, end)
    ).fetchall()
    conn.close()

    total_gains = sum(r[0] for r in rows if r[0] > 0)
    total_losses = sum(abs(r[0]) for r in rows if r[0] < 0)
    net = total_gains - total_losses
    n = len(rows)
    total_proceeds = sum(r[1] for r in rows)
    total_cost = sum(r[2] for r in rows)

    # 한국 가상자산 양도세 — 250만원 기본 공제 후 22%
    BASIC_DEDUCTION_KRW = 2_500_000
    TAX_RATE = 0.22
    taxable_amount = max(0, net - BASIC_DEDUCTION_KRW)
    estimated_tax = taxable_amount * TAX_RATE

    return {
        "year": year,
        "n_taxable_events": n,
        "total_proceeds_krw": total_proceeds,
        "total_cost_krw": total_cost,
        "total_gains_krw": total_gains,
        "total_losses_krw": total_losses,
        "net_capital_gain_krw": net,
        "basic_deduction_krw": BASIC_DEDUCTION_KRW,
        "taxable_amount_krw": taxable_amount,
        "estimated_tax_krw": estimated_tax,
        "tax_rate": TAX_RATE,
    }


def export_csv(year: int, output_path: str = "tax_export.csv") -> int:
    """연간 자료 CSV export — 세무사·국세청 신고용."""
    _ensure_table()
    start = datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp()

    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT sold_at, token_id, shares, sale_proceeds_krw, cost_basis_krw, "
        "       capital_gain_krw, holding_period_days, fx_rate_at_sell "
        "FROM taxable_events WHERE sold_at >= ? AND sold_at < ? ORDER BY sold_at",
        (start, end)
    ).fetchall()
    conn.close()

    import csv
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "거래일시", "토큰ID(앞 8자)", "수량", "매도가(KRW)", "취득가(KRW)",
            "양도손익(KRW)", "보유기간(일)", "환율"
        ])
        for r in rows:
            dt = datetime.fromtimestamp(r[0], tz=timezone.utc)
            writer.writerow([
                dt.strftime("%Y-%m-%d %H:%M:%S"),
                r[1][:8],
                f"{r[2]:.4f}",
                f"{r[3]:.0f}",
                f"{r[4]:.0f}",
                f"{r[5]:.0f}",
                f"{r[6]:.1f}" if r[6] else "",
                f"{r[7]:.2f}" if r[7] else "",
            ])
    return len(rows)
