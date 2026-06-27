"""探针 4：价值风格基准指数覆盖率（spec §12 #4）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import PASSED, WARNING, Fetcher, Probe, ProbeResult


def missing_rate(bars: list[dict], calendar: list[str]) -> float:
    """基准序列相对交易日历的缺失比例。"""
    if not calendar:
        return 1.0
    have = {b["date"] for b in bars}
    missing = sum(1 for d in calendar if d not in have)
    return missing / len(calendar)


# spec §6.1 降级映射
DOWNGRADE = {
    "csi300_value": "csi300",
    "sp500_value": "sp500",
    "hsi_composite": "hsi",
}


class ValueBenchmarkProbe(Probe):
    name = "value_benchmark_coverage"
    description = "价值风格基准指数 10 年日线序列完整性"

    def __init__(self, index_code: str = "csi300_value", threshold: float = 0.05, years: int = 10):
        self.index_code = index_code
        self.threshold = threshold
        self.years = years

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        bars = fetcher.fetch("benchmark_daily", index_code=self.index_code, years=self.years)
        calendar = fetcher.fetch("trading_calendar", market="a_share", years=self.years)

        if not calendar:
            return ProbeResult(
                status=WARNING,
                summary=f"基准 {self.index_code}：缺交易日历，无法评估缺失率，默认降级为宽基",
                stats={"index_code": self.index_code, "total_bars": len(bars), "expected_bars": 0, "missing_rate": 1.0},
            )

        rate = missing_rate(bars, calendar)
        stats = {
            "index_code": self.index_code,
            "total_bars": len(bars),
            "expected_bars": len(calendar),
            "missing_rate": rate,
        }
        if rate <= self.threshold:
            return ProbeResult(
                status=PASSED,
                summary=f"基准 {self.index_code} 缺失率 {rate:.1%} ≤ {self.threshold:.0%}，可用",
                stats=stats,
            )
        fallback = DOWNGRADE.get(self.index_code, "宽基")
        return ProbeResult(
            status=WARNING,
            summary=f"基准 {self.index_code} 缺失率 {rate:.1%} > {self.threshold:.0%}，降级为 {fallback}",
            stats=stats,
        )
