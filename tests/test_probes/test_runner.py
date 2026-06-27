import json
import sqlite3

from src.data_pipeline.probes.base import PASSED, WARNING, ProbeResult
from src.data_pipeline.probes.runner import (
    init_quality_db,
    run_all_probes,
    list_recent_results,
)
from src.storage import sqlite as sq


class StubProbe:
    def __init__(self, name, result):
        self.name = name
        self._result = result

    def run(self, fetcher):
        return self._result


def _probes():
    return [
        StubProbe("p1", ProbeResult(PASSED, "ok1", {"a": 1})),
        StubProbe("p2", ProbeResult(WARNING, "warn2", {"b": 2})),
    ]


class FakeFetcher:
    def fetch(self, kind, **kwargs):
        return []


def test_init_quality_db_creates_table(quality_db):
    init_quality_db(quality_db)
    conn = sq.get_connection(quality_db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}
    conn.close()
    assert "probe_results" in tables


def test_run_all_probes_persists(quality_db):
    init_quality_db(quality_db)
    results = run_all_probes(FakeFetcher(), quality_db, probes=_probes(), run_at="2026-06-27T22:00:00")
    assert len(results) == 2
    rows = list_recent_results(quality_db, run_at="2026-06-27T22:00:00")
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert names == {"p1", "p2"}
    stats = json.loads(rows[0]["stats_json"])
    assert stats in ({"a": 1}, {"b": 2})


def test_run_all_probes_writes_one_row_per_probe(quality_db):
    init_quality_db(quality_db)
    run_all_probes(FakeFetcher(), quality_db, probes=_probes(), run_at="t1")
    run_all_probes(FakeFetcher(), quality_db, probes=_probes(), run_at="t2")
    conn = sq.get_connection(quality_db)
    n = conn.execute("SELECT COUNT(*) FROM probe_results;").fetchone()[0]
    conn.close()
    assert n == 4  # 两次 × 2 探针
