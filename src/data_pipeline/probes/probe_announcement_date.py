"""探针 1：A 股历史财报披露日期覆盖率（spec §12 #1）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import FAILED, PASSED, WARNING, Fetcher, Probe, ProbeResult


def coverage_rate(rows: list[dict]) -> float:
    """announcement_date 非空率。"""
    if not rows:
        return 0.0
    non_empty = sum(1 for r in rows if r.get("announcement_date"))
    return non_empty / len(rows)


class AnnouncementDateProbe(Probe):
    name = "announcement_date_coverage"
    description = "A 股历史财报披露日期(announcement_date)非空率"

    def __init__(self, sample_size: int = 300, threshold: float = 0.80):
        self.sample_size = sample_size
        self.threshold = threshold

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        rows = fetcher.fetch(
            "a_share_announcement_dates",
            years=10,
            sample_size=self.sample_size,
        )
        if not rows:
            return ProbeResult(
                status=FAILED,
                summary="未取到任何财报披露日期样本，无法评估 PIT 可用性",
                stats={"sample_size": 0, "coverage_rate": 0.0},
            )
        rate = coverage_rate(rows)
        if rate >= self.threshold:
            return ProbeResult(
                status=PASSED,
                summary=f"A 股财报披露日期覆盖率 {rate:.1%} ≥ {self.threshold:.0%}，PIT 可用",
                stats={"sample_size": len(rows), "coverage_rate": rate},
            )
        return ProbeResult(
            status=WARNING,
            summary=(
                f"A 股财报披露日期覆盖率 {rate:.1%} < {self.threshold:.0%}，"
                f"需降级为'报告期+固定滞后'近似（年报+4月/中报+2月/季报+1月）"
            ),
            stats={"sample_size": len(rows), "coverage_rate": rate},
        )
