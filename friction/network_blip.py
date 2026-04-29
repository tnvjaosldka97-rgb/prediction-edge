"""
Network blip model — Polygon RPC가 가끔 죽는 사건.

Poisson 프로세스로 모델링:
- 평균 시간당 N번 발생 (lambda)
- 한 번 발생하면 평균 D초 지속 (지수분포)
- 그 시간 동안 모든 RPC 호출 실패

시뮬: 시간 구간 [t0, t1]에 발생할 blip들 미리 생성.
체결 시점이 blip 안에 들어오면 → 주문 실패 (rejection_reason="network_blip").
"""
from __future__ import annotations
import math
import random
from dataclasses import dataclass, field


@dataclass
class Blip:
    start_ts: float
    end_ts: float


class NetworkBlipModel:
    def __init__(
        self,
        blips_per_hour: float = 1.5,
        mean_duration_sec: float = 20.0,
    ):
        self.lam_per_sec = blips_per_hour / 3600.0
        self.mean_duration_sec = mean_duration_sec
        self._blips: list[Blip] = []

    def generate(self, t0: float, t1: float, rng: random.Random | None = None) -> None:
        """[t0, t1] 구간에 발생할 blip 사전 생성."""
        r = rng or random
        self._blips = []
        if self.lam_per_sec <= 0 or t1 <= t0:
            return
        t = t0
        while True:
            # 다음 발생까지 시간 — 지수분포
            interval = -math.log(max(1e-12, r.random())) / self.lam_per_sec
            t += interval
            if t >= t1:
                break
            duration = -math.log(max(1e-12, r.random())) * self.mean_duration_sec
            self._blips.append(Blip(t, t + duration))

    def is_down_at(self, ts: float) -> bool:
        """ts가 어떤 blip 구간에 속하는지."""
        for b in self._blips:
            if b.start_ts <= ts <= b.end_ts:
                return True
        return False

    def covers_interval(self, t_start: float, t_end: float) -> bool:
        """[t_start, t_end]가 blip 구간과 겹치는지."""
        for b in self._blips:
            if not (t_end < b.start_ts or t_start > b.end_ts):
                return True
        return False

    def calibrate(self, observed_blips: list[tuple[float, float]], total_window_sec: float) -> None:
        """라이브 trace의 (start_ts, end_ts) 쌍들로 파라미터 갱신."""
        if total_window_sec <= 0:
            return
        n = len(observed_blips)
        if n == 0:
            self.lam_per_sec = 0.0
            return
        self.lam_per_sec = n / total_window_sec
        durations = [e - s for s, e in observed_blips if e > s]
        if durations:
            self.mean_duration_sec = sum(durations) / len(durations)

    def to_dict(self) -> dict:
        return {
            "blips_per_hour": self.lam_per_sec * 3600,
            "mean_duration_sec": self.mean_duration_sec,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "NetworkBlipModel":
        return cls(
            blips_per_hour=d.get("blips_per_hour", 1.5),
            mean_duration_sec=d.get("mean_duration_sec", 20.0),
        )
