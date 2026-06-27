from src.data_pipeline.probes.base import FAILED, PASSED, WARNING
from src.data_pipeline.probes.probe_announcement_date import (
    AnnouncementDateProbe,
    coverage_rate,
)


def _rows(with_date, without_date):
    rows = [{"code": f"C{i}", "report_period": "2023-12-31", "announcement_date": "2024-03-30"} for i in range(with_date)]
    rows += [{"code": f"X{i}", "report_period": "2023-12-31", "announcement_date": None} for i in range(without_date)]
    return rows


def test_coverage_rate_basic():
    assert coverage_rate(_rows(80, 20)) == 0.80
    assert coverage_rate([]) == 0.0


class FakeFetcher:
    def __init__(self, rows):
        self.rows = rows

    def fetch(self, kind, **kwargs):
        assert kind == "a_share_announcement_dates"
        return self.rows


def test_probe_passes_at_threshold():
    probe = AnnouncementDateProbe(sample_size=100, threshold=0.80)
    r = probe.run(FakeFetcher(_rows(85, 15)))
    assert r.status == PASSED
    assert abs(r.stats["coverage_rate"] - 0.85) < 1e-9
    assert r.stats["sample_size"] == 100


def test_probe_warns_below_threshold():
    probe = AnnouncementDateProbe(sample_size=100, threshold=0.80)
    r = probe.run(FakeFetcher(_rows(70, 30)))
    assert r.status == WARNING
    assert r.stats["coverage_rate"] == 0.70
    assert "固定滞后" in r.summary


def test_probe_fails_on_empty():
    probe = AnnouncementDateProbe(sample_size=100, threshold=0.80)
    r = probe.run(FakeFetcher([]))
    assert r.status == FAILED
    assert r.stats["sample_size"] == 0
