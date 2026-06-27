import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.fetchers.macro import BenchmarkFetcher, FXFetcher
from src.data_pipeline.fetchers.quote import AShareQuoteFetcher, USQuoteFetcher


@pytest.mark.integration
def test_real_a_share_quote(isolated_data_dir):
    """真实拉取茅台一日行情（联网）。"""
    df = AShareQuoteFetcher().fetch_daily("600519", "2026-06-20", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert (df["code"] == "600519").all()
    assert len(df) > 0


@pytest.mark.integration
def test_real_us_quote(isolated_data_dir):
    df = USQuoteFetcher().fetch_daily("AAPL", "2026-06-20", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert len(df) > 0


@pytest.mark.integration
def test_real_fx(isolated_data_dir):
    df = FXFetcher().fetch("USD/CNY", "2026-06-20", "2026-06-26")
    assert len(df) > 0
    assert (df["base"] == "USD").all()


@pytest.mark.integration
def test_real_benchmark_a_share(isolated_data_dir):
    """A 股基准降级为沪深300宽基（探针实测约束）。"""
    df = BenchmarkFetcher().fetch("a_share", "2026-06-20", "2026-06-26")
    assert len(df) > 0
    assert (df["market"] == "a_share").all()
