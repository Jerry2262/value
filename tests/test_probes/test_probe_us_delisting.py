from src.data_pipeline.probes.base import PASSED, WARNING
from src.data_pipeline.probes.probe_us_delisting import UsDelistingProbe


class FakeFetcher:
    def __init__(self, payload):
        self.payload = payload

    def fetch(self, kind, **kwargs):
        assert kind == "us_delisted_source_available"
        return self.payload


def test_probe_passes_when_source_available_with_samples():
    fetcher = FakeFetcher({"available": True, "source": "nasdaq_delistings", "sample_count": 1200})
    r = UsDelistingProbe().run(fetcher)
    assert r.status == PASSED
    assert r.stats["available"] is True
    assert r.stats["source"] == "nasdaq_delistings"


def test_probe_warns_when_no_source():
    fetcher = FakeFetcher({"available": False, "source": "", "sample_count": 0})
    r = UsDelistingProbe().run(fetcher)
    assert r.status == WARNING
    assert "未修正幸存者偏差" in r.summary
    assert r.stats["available"] is False


def test_probe_warns_when_source_but_no_samples():
    fetcher = FakeFetcher({"available": True, "source": "x", "sample_count": 0})
    r = UsDelistingProbe().run(fetcher)
    assert r.status == WARNING
    assert r.stats["sample_count"] == 0
