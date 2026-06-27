"""探针 2：港股退市列表完整性（spec §12 #2）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import FAILED, PASSED, WARNING, Fetcher, Probe, ProbeResult


class HkDelistingProbe(Probe):
    name = "hk_delisting_completeness"
    description = "港股退市列表对已知重大退市案例的覆盖率"

    def __init__(self, min_samples: int = 5, hit_threshold: int = 4):
        self.min_samples = min_samples
        self.hit_threshold = hit_threshold

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        delisted = fetcher.fetch("hk_delisted_list")
        known = fetcher.fetch("hk_known_delisting_samples")

        if not delisted:
            return ProbeResult(
                status=FAILED,
                summary="港股退市列表为空，无法做幸存者偏差修正",
                stats={"delisted_total": 0, "sample_count": len(known), "hit_count": 0},
            )
        if not known:
            return ProbeResult(
                status=FAILED,
                summary="缺少已知退市案例用于校验，无法评估覆盖率",
                stats={"delisted_total": len(delisted), "sample_count": 0, "hit_count": 0},
            )

        delisted_codes = {d["code"] for d in delisted}
        hits = sum(1 for s in known if s["code"] in delisted_codes)
        if hits >= self.hit_threshold:
            return ProbeResult(
                status=PASSED,
                summary=f"港股退市列表命中已知案例 {hits}/{len(known)}，覆盖率达标",
                stats={"delisted_total": len(delisted), "sample_count": len(known), "hit_count": hits},
            )
        return ProbeResult(
            status=WARNING,
            summary=(
                f"港股退市列表仅命中 {hits}/{len(known)}，覆盖率偏低，"
                f"回测需标注'港股退市数据覆盖不足'并人工补录"
            ),
            stats={"delisted_total": len(delisted), "sample_count": len(known), "hit_count": hits},
        )
