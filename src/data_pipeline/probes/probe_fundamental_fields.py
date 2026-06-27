"""探针 5：财务数据字段完整性（spec §12 #5）。"""
from __future__ import annotations

from collections import defaultdict

from src.data_pipeline.probes.base import PASSED, WARNING, Fetcher, Probe, ProbeResult

DEFAULT_FIELDS = ["roe", "fcf", "revenue", "net_profit", "market_cap"]


def field_missing_rates(rows: list[dict], fields: list[str]) -> dict[tuple[str, str], float]:
    """按 (market, field) 统计缺失率。"""
    counts: dict[tuple[str, str], list[int, int]] = defaultdict(lambda: [0, 0])  # [total, missing]
    for r in rows:
        key = (r["market"], r["field"])
        counts[key][0] += 1
        if r.get("value") is None:
            counts[key][1] += 1
    return {k: (m / t if t else 1.0) for k, (t, m) in counts.items()}


class FundamentalFieldsProbe(Probe):
    name = "fundamental_field_completeness"
    description = "三地财务关键字段缺失率"

    def __init__(self, threshold: float = 0.15, fields: list[str] | None = None, per_market_n: int = 20):
        self.threshold = threshold
        self.fields = fields or DEFAULT_FIELDS
        self.per_market_n = per_market_n

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        rows = fetcher.fetch(
            "fundamental_sample",
            markets=["a_share", "us", "hk"],
            n_stocks=self.per_market_n,
            years=5,
        )
        if not rows:
            return ProbeResult(
                status=WARNING,
                summary="未取到财务样本",
                stats={"overall_pass": False, "per_market_field": {}},
            )
        rates = field_missing_rates(rows, self.fields)
        over = {k: v for k, v in rates.items() if v > self.threshold}
        overall_pass = not over
        if overall_pass:
            return ProbeResult(
                status=PASSED,
                summary=f"三地财务字段缺失率均 ≤ {self.threshold:.0%}",
                stats={"overall_pass": True, "per_market_field": rates},
            )
        over_desc = ", ".join(f"{m}:{f}={v:.0%}" for (m, f), v in over.items())
        return ProbeResult(
            status=WARNING,
            summary=(
                f"部分字段缺失率超阈值：{over_desc}；"
                f"超阈值股该期从因子体系移除并记 data_quality_log"
            ),
            stats={"overall_pass": False, "per_market_field": rates},
        )
