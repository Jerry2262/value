"""探针 3：美股退市列表可行性（spec §12 #3）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import PASSED, WARNING, Fetcher, Probe, ProbeResult


class UsDelistingProbe(Probe):
    name = "us_delisting_feasibility"
    description = "美股退市列表替代源可用性"

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        info = fetcher.fetch("us_delisted_source_available")
        available = bool(info.get("available"))
        source = info.get("source", "")
        count = int(info.get("sample_count", 0))

        if available and count > 0:
            return ProbeResult(
                status=PASSED,
                summary=f"美股退市源可用：{source}（样本 {count} 条）",
                stats={"available": True, "source": source, "sample_count": count},
            )
        return ProbeResult(
            status=WARNING,
            summary=(
                f"美股退市列表无可行免费源（available={available}, source={source!r}）；"
                f"回测仅对 A 股做退市修正，美股标注'未修正幸存者偏差，收益可能被高估'"
            ),
            stats={"available": available, "source": source, "sample_count": count},
        )
