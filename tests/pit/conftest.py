import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import (
    DELISTING_COLUMNS,
    FUNDAMENTAL_COLUMNS,
    QUOTE_COLUMNS,
)
from src.data_pipeline import store


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """把 VALUE_DATA_DIR 指向临时目录，刷新 config。"""
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)


@pytest.fixture
def multi_partition_quote(isolated_data_dir):
    """两个拉取分区的行情：分区 2026-06-25 含 06-23/06-24；分区 2026-06-27 含 06-26 修正版 06-24 + 06-26。

    用于验证：(1) 行级 date<=as_of 过滤；(2) 跨分区去重——06-24 在两个分区都有，
    取最新分区（2026-06-27）的版本。
    """
    part1 = pd.DataFrame([
        {"date": "2026-06-23", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-24", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10.0, "volume": 100, "adj_factor": 1.0},
    ], columns=QUOTE_COLUMNS)
    part2 = pd.DataFrame([
        # 06-24 修正：close 10.0 → 10.5（应覆盖 part1 的 06-24）
        {"date": "2026-06-24", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-26", "code": "C", "market": "a_share",
         "open": 11, "high": 12, "low": 10, "close": 11, "volume": 110, "adj_factor": 1.0},
    ], columns=QUOTE_COLUMNS)
    store.write_parquet_partition(part1, "market", "2026-06-25", "a_share")
    store.write_parquet_partition(part2, "market", "2026-06-27", "a_share")


@pytest.fixture
def multi_partition_fundamental(isolated_data_dir):
    """两个披露分区的财报：分区 2024-04-30（年报 2023 披露）含 2023 年报；
    分区 2024-08-31（中报 2024 披露）含 2024 中报 + 2023 年报修正版。

    用于验证：(1) 行级 announcement_date_approx<=as_of 过滤；
    (2) 跨分区去重——2023 年报在两分区都有，取较新披露版本。
    """
    part1 = pd.DataFrame([
        {"code": "C", "market": "a_share", "report_period": "2023-12-31",
         "announcement_date_approx": "2024-04-30",
         "revenue": 1.5e11, "net_profit": 7e10, "roe": 30.0,
         "debt_ratio": 20.0, "fcf": 5e10, "total_market_cap": 1e12},
    ], columns=FUNDAMENTAL_COLUMNS)
    part2 = pd.DataFrame([
        # 2023 年报修正：roe 30.0 → 31.0（应覆盖 part1）
        {"code": "C", "market": "a_share", "report_period": "2023-12-31",
         "announcement_date_approx": "2024-04-30",
         "revenue": 1.5e11, "net_profit": 7e10, "roe": 31.0,
         "debt_ratio": 20.0, "fcf": 5e10, "total_market_cap": 1e12},
        {"code": "C", "market": "a_share", "report_period": "2024-06-30",
         "announcement_date_approx": "2024-08-31",
         "revenue": 8e10, "net_profit": 4e10, "roe": 32.0,
         "debt_ratio": 21.0, "fcf": 3e10, "total_market_cap": 1.1e12},
    ], columns=FUNDAMENTAL_COLUMNS)
    store.write_parquet_partition(part1, "fundamental", "2024-04-30", "a_share")
    store.write_parquet_partition(part2, "fundamental", "2024-08-31", "a_share")


@pytest.fixture
def a_share_delisting(isolated_data_dir):
    """A 股退市列表：C1 2020 退市，C2 2025 退市，C3 在市。"""
    df = pd.DataFrame([
        {"code": "C1", "market": "a_share", "delist_date": "2020-01-01", "reason": "强制退市"},
        {"code": "C2", "market": "a_share", "delist_date": "2025-06-30", "reason": "吸收合并"},
    ], columns=DELISTING_COLUMNS)
    store.write_parquet_partition(df, "delisting", "2026-06-27", "a_share")
