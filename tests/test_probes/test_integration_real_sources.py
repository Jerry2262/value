import pytest

from src.data_pipeline.probes.integration_fetcher import RealFetcher
from src.data_pipeline.probes.runner import init_quality_db, run_all_probes


@pytest.mark.integration
def test_real_probes_run_without_hard_failure(quality_db):
    """spec §12 硬门槛：五项探针对真实源运行，不应出现 FAILED（FAILED 表示取数完全失败）。

    WARNING 是可接受的（触发降级路径），FAILED 不可接受。
    需联网运行：pytest tests/test_probes/test_integration_real_sources.py -m integration -v
    """
    init_quality_db(quality_db)
    results = run_all_probes(RealFetcher(), quality_db, run_at="2026-06-27T22:00:00")
    assert len(results) == 5
    failed = [r for r in results if r.status == "failed"]
    assert not failed, f"探针出现 FAILED：{[r.summary for r in failed]}"
