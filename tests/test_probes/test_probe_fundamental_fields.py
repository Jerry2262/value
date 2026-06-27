from src.data_pipeline.probes.base import PASSED, WARNING
from src.data_pipeline.probes.probe_fundamental_fields import (
    FundamentalFieldsProbe,
    field_missing_rates,
)

FIELDS = ["roe", "fcf", "revenue", "net_profit", "market_cap"]


def _rows(market, present, missing):
    rows = []
    for f in FIELDS:
        for _ in range(present):
            rows.append({"market": market, "code": "C", "field": f, "value": 1.0})
        for _ in range(missing):
            rows.append({"market": market, "code": "C", "field": f, "value": None})
    return rows


def test_field_missing_rates():
    rates = field_missing_rates(_rows("a_share", present=17, missing=3), FIELDS)
    # 每个字段 17 present + 3 missing → 15% 缺失
    for f in FIELDS:
        assert abs(rates[("a_share", f)] - 0.15) < 1e-9


class FakeFetcher:
    def __init__(self, rows):
        self.rows = rows

    def fetch(self, kind, **kwargs):
        assert kind == "fundamental_sample"
        return self.rows


def test_probe_passes_all_under_threshold():
    rows = _rows("a_share", 17, 3) + _rows("us", 18, 2) + _rows("hk", 19, 1)
    r = FundamentalFieldsProbe(threshold=0.15).run(FakeFetcher(rows))
    assert r.status == PASSED
    assert r.stats["overall_pass"] is True


def test_probe_warns_when_one_field_over():
    rows = _rows("a_share", 17, 3) + _rows("us", 10, 10)  # us 50% 缺失
    r = FundamentalFieldsProbe(threshold=0.15).run(FakeFetcher(rows))
    assert r.status == WARNING
    assert r.stats["overall_pass"] is False
    assert ("us", "roe") in r.stats["per_market_field"]


def test_probe_warns_on_empty():
    r = FundamentalFieldsProbe(threshold=0.15).run(FakeFetcher([]))
    assert r.status == WARNING
    assert r.stats["overall_pass"] is False
