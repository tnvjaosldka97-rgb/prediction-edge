"""
Friction modeling — 실거래에서 발생하는 모든 마찰을 단일 모듈로.

Backtest(realistic_engine), live DRY_RUN(gateway._simulate_fill),
shadow virtual_executor가 모두 동일한 friction.orchestrator를 호출하면
세 경로의 결과가 일치한다. 라이브 trace로 캘리브레이션하면
시뮬→실거래 갭이 줄어든다.

7개 마찰 레이어:
  1. latency       — submit→fill 지연 (log-normal)
  2. slippage      — 호가 walking
  3. partial_fill  — 주문타입별 부분 체결
  4. rejection     — rate limit / sig 만료 / RPC 에러 / min size 등
  5. network_blip  — Polygon RPC 정전 (Poisson)
  6. clob_quirks   — tick size, min size 결정론적 규칙
  7. fund_lock     — USDC approve, gas 컨펌 시간

진입점: friction.orchestrator.FrictionOrchestrator.simulate_fill()
"""
from friction.orchestrator import FrictionOrchestrator, SimulatedFill

__all__ = ["FrictionOrchestrator", "SimulatedFill"]
