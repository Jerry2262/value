from src.data_pipeline.probes.base import FAILED, PASSED, WARNING
from src.data_pipeline.probes.probe_hk_delisting import HkDelistingProbe


class FakeFetcher:
    def __init__(self, delisted_codes, known_samples):
        self._data = {
            "hk_delisted_list": [{"code": c, "delist_date": "2022-01-01", "reason": "privatization"} for c in delisted_codes],
            "hk_known_delisting_samples": [{"code": c} for c in known_samples],
        }

    def fetch(self, kind, **kwargs):
        return self._data[kind]


def test_probe_passes_when_4_of_5_hit():
    fetcher = FakeFetcher(delisted_codes={"A", "B", "C", "D"}, known_samples=["A", "B", "C", "D", "E"])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == PASSED
    assert r.stats["hit_count"] == 4
    assert r.stats["sample_count"] == 5


def test_probe_warns_when_below_threshold():
    fetcher = FakeFetcher(delisted_codes={"A", "B"}, known_samples=["A", "B", "C", "D", "E"])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == WARNING
    assert r.stats["hit_count"] == 2
    assert "覆盖率偏低" in r.summary


def test_probe_fails_when_delisted_list_empty():
    fetcher = FakeFetcher(delisted_codes=set(), known_samples=["A"])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == FAILED
    assert r.stats["delisted_total"] == 0


def test_probe_fails_when_no_known_samples():
    fetcher = FakeFetcher(delisted_codes={"A"}, known_samples=[])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == FAILED
    assert r.stats["sample_count"] == 0
