"""探针批量运行器 + data_quality.db 持久化。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from src.data_pipeline.probes.base import Fetcher, Probe, ProbeResult
from src.data_pipeline.probes.probe_announcement_date import AnnouncementDateProbe
from src.data_pipeline.probes.probe_fundamental_fields import FundamentalFieldsProbe
from src.data_pipeline.probes.probe_hk_delisting import HkDelistingProbe
from src.data_pipeline.probes.probe_us_delisting import UsDelistingProbe
from src.data_pipeline.probes.probe_value_benchmark import ValueBenchmarkProbe
from src.storage import sqlite as sq

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_quality_db(db_path: Path) -> None:
    """建 probe_results 表（幂等）。"""
    script = SCHEMA_PATH.read_text(encoding="utf-8")
    sq.execute_script(db_path, script)


def default_probes() -> list[Probe]:
    """spec §12 五项探针的默认实例。"""
    return [
        AnnouncementDateProbe(),
        HkDelistingProbe(),
        UsDelistingProbe(),
        ValueBenchmarkProbe(),
        FundamentalFieldsProbe(),
    ]


def run_all_probes(
    fetcher: Fetcher,
    db_path: Path,
    *,
    probes: list[Probe] | None = None,
    run_at: str,
) -> list[ProbeResult]:
    """运行全部探针并写一行结果到 data_quality.db。返回结果列表。"""
    probes = probes if probes is not None else default_probes()
    init_quality_db(db_path)
    results = [p.run(fetcher) for p in probes]

    def _insert(conn):
        for p, r in zip(probes, results):
            conn.execute(
                "INSERT INTO probe_results(name, status, summary, stats_json, run_at) "
                "VALUES (?, ?, ?, ?, ?);",
                (p.name, r.status, r.summary, json.dumps(r.stats, ensure_ascii=False), run_at),
            )

    sq.run_write(db_path, _insert)
    return results


def list_recent_results(db_path: Path, *, run_at: str | None = None) -> list[dict]:
    """读取探针结果。run_at 指定时只返回该批次。"""
    conn = sq.get_connection(db_path)
    try:
        if run_at:
            cur = conn.execute(
                "SELECT name, status, summary, stats_json, run_at FROM probe_results "
                "WHERE run_at = ? ORDER BY name;",
                (run_at,),
            )
        else:
            cur = conn.execute(
                "SELECT name, status, summary, stats_json, run_at FROM probe_results "
                "ORDER BY run_at DESC, name;"
            )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
