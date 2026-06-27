import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.store import (
    incremental_merge,
    read_parquet,
    write_parquet_partition,
)


def _quote(rows):
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS)


def test_write_partition_creates_dated_file(isolated_data_dir):
    df = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                  "open": 10, "high": 11, "low": 9, "close": 10,
                  "volume": 100, "adj_factor": 1.0}])
    path = write_parquet_partition(df, kind="market", partition_date="2026-06-27", market="a_share")
    assert path.exists()
    assert "raw/market/2026-06-27/a_share.parquet" in str(path)


def test_read_partition_roundtrip(isolated_data_dir):
    df = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                  "open": 10, "high": 11, "low": 9, "close": 10,
                  "volume": 100, "adj_factor": 1.0}])
    write_parquet_partition(df, "market", "2026-06-27", "a_share")
    out = read_parquet("market", "a_share")
    assert len(out) == 1
    assert out["code"].iloc[0] == "C"


def test_read_as_of_filters_future_partitions(isolated_data_dir):
    """as_of 只读 ≤ 该日期的分区（PIT 约束）。"""
    df1 = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                   "open": 10, "high": 11, "low": 9, "close": 10,
                   "volume": 100, "adj_factor": 1.0}])
    df2 = _quote([{"date": "2026-06-26", "code": "C", "market": "a_share",
                   "open": 11, "high": 12, "low": 10, "close": 11,
                   "volume": 110, "adj_factor": 1.0}])
    write_parquet_partition(df1, "market", "2026-06-25", "a_share")
    write_parquet_partition(df2, "market", "2026-06-26", "a_share")
    # as_of=2026-06-25 → 只看到 df1
    out = read_parquet("market", "a_share", as_of="2026-06-25")
    assert len(out) == 1
    assert out["date"].iloc[0] == "2026-06-25"


def test_incremental_merge_dedups(isolated_data_dir):
    """增量更新：新数据覆盖同主键旧行，新增行追加。"""
    old = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                   "open": 10, "high": 11, "low": 9, "close": 10,
                   "volume": 100, "adj_factor": 1.0}])
    write_parquet_partition(old, "market", "2026-06-25", "a_share")
    existing = read_parquet("market", "a_share")
    # 新数据：修正 06-25 + 新增 06-26
    new = _quote([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10.5,  # 修正
         "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-26", "code": "C", "market": "a_share",
         "open": 11, "high": 12, "low": 10, "close": 11,
         "volume": 110, "adj_factor": 1.0},
    ])
    merged = incremental_merge(new, existing, keys=["code", "date"])
    assert len(merged) == 2
    # 06-25 被新值覆盖
    row_25 = merged[merged["date"] == "2026-06-25"].iloc[0]
    assert row_25["close"] == 10.5


def test_read_missing_returns_empty(isolated_data_dir):
    out = read_parquet("market", "us")
    assert len(out) == 0
