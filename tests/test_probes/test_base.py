from src.data_pipeline.probes.base import (
    FAILED,
    PASSED,
    WARNING,
    Fetcher,
    Probe,
    ProbeResult,
)


class FakeFetcher:
    def __init__(self, data):
        self.data = data

    def fetch(self, kind, **kwargs):
        return self.data[kind]


class GoodProbe(Probe):
    name = "good"

    def _compute(self, fetcher):
        return ProbeResult(PASSED, "ok", {"n": fetcher.fetch("n")})


class BadProbe(Probe):
    name = "bad"

    def _compute(self, fetcher):
        raise ValueError("boom")


def test_probe_result_passed_property():
    r = ProbeResult(PASSED, "ok")
    assert r.passed is True
    r2 = ProbeResult(FAILED, "no")
    assert r2.passed is False


def test_good_probe_returns_passed():
    p = GoodProbe()
    r = p.run(FakeFetcher({"n": 5}))
    assert r.status == PASSED
    assert r.stats == {"n": 5}


def test_bad_probe_caught_as_warning():
    p = BadProbe()
    r = p.run(FakeFetcher({}))
    assert r.status == WARNING
    assert "boom" in r.summary
    assert r.passed is False


def test_fetcher_protocol_runtime_checkable():
    assert isinstance(FakeFetcher({}), Fetcher)
