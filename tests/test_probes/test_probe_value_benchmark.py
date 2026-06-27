import pandas as pd

from src.data_pipeline.probes.base import PASSED, WARNING
from src.data_pipeline.probes.probe_value_benchmark import ValueBenchmarkProbe, missing_rate


class FakeFetcher:
    def __init__(self, bars, calendar):
        self._data = {
            "benchmark_daily": bars,
            "trading_calendar": calendar,
        }

    def fetch(self, kind, **kwargs):
        return self._data[kind]


def _bars(dates):
    return [{"date": d, "close": 100.0} for d in dates]


def test_missing_rate_basic():
    cal = [f"2023-01-{i:02d}" for i in range(2, 21)]  # 19 天
    bars = _bars(cal[:18])  # 缺 1
    assert abs(missing_rate(bars, cal) - 1 / 19) < 1e-9


def test_probe_passes_within_threshold():
    cal = [f"2023-01-{i:02d}" for i in range(2, 22)]  # 20 天
    bars = _bars(cal[:19])  # 缺 1 → 5%
    r = ValueBenchmarkProbe(index_code="000300value", threshold=0.05).run(FakeFetcher(bars, cal))
    assert r.status == PASSED
    assert r.stats["missing_rate"] == 0.05


def test_probe_warns_above_threshold():
    cal = [f"2023-01-{i:02d}" for i in range(2, 22)]  # 20 天
    bars = _bars(cal[:16])  # 缺 4 → 20%
    r = ValueBenchmarkProbe(index_code="000300value", threshold=0.05).run(FakeFetcher(bars, cal))
    assert r.status == WARNING
    assert r.stats["missing_rate"] == 0.20
    assert "降级" in r.summary


def test_probe_warns_when_no_calendar():
    r = ValueBenchmarkProbe(index_code="000300value", threshold=0.05).run(FakeFetcher(_bars(["2023-01-02"]), []))
    assert r.status == WARNING


def test_probe_uses_downgrade_map_for_known_key():
    # 用真实 DOWNGRADE 映射的键 csi300_value，断言映射值 csi300 出现在 summary
    cal = [f"2023-01-{i:02d}" for i in range(2, 22)]  # 20 天
    bars = _bars(cal[:16])  # 缺 4 → 20% → WARNING
    r = ValueBenchmarkProbe(index_code="csi300_value", threshold=0.05).run(FakeFetcher(bars, cal))
    assert r.status == WARNING
    assert "csi300" in r.summary  # DOWNGRADE["csi300_value"] == "csi300"
