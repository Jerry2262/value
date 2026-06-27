import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import BENCHMARK_COLUMNS, FX_COLUMNS
from src.data_pipeline.fetchers.macro import BenchmarkFetcher, FXFetcher


def test_fx_fetcher_normalizes(mocker):
    raw = pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "收盘": [7.20, 7.18],
    })
    mocker.patch("akshare.currency_boc_sina", return_value=raw)
    df = FXFetcher().fetch("USD/CNY", "2026-06-25", "2026-06-26")
    assert list(df.columns) == FX_COLUMNS
    assert (df["base"] == "USD").all()
    assert (df["quote"] == "CNY").all()
    assert df["rate"].iloc[0] == 7.20


def test_fx_fetcher_pair_parse():
    """pair 字符串解析为 base/quote。"""
    from src.data_pipeline.fetchers.macro import _parse_pair
    assert _parse_pair("USD/CNY") == ("USD", "CNY")
    assert _parse_pair("HKD/CNY") == ("HKD", "CNY")


def test_benchmark_a_share_degrades_to_csi300(mocker):
    """A 股基准降级为沪深300宽基（探针实测：沪深300价值缺失率68%）。"""
    raw = pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "收盘": [4800.0, 4820.0],
    })
    mock_hist = mocker.patch("akshare.index_zh_a_hist", return_value=raw)
    df = BenchmarkFetcher().fetch("a_share", "2026-06-25", "2026-06-26")
    assert list(df.columns) == BENCHMARK_COLUMNS
    assert (df["market"] == "a_share").all()
    # 确认调用了宽基 000300（而非价值指数）
    args, kwargs = mock_hist.call_args
    assert kwargs.get("symbol") == "000300"


def test_benchmark_us_uses_yfinance(mocker):
    raw = pd.DataFrame({"Close": [5000.0, 5050.0]},
                       index=pd.to_datetime(["2026-06-24", "2026-06-25"]))
    mocker.patch("yfinance.download", return_value=raw)
    df = BenchmarkFetcher().fetch("us", "2026-06-24", "2026-06-25")
    assert list(df.columns) == BENCHMARK_COLUMNS
    assert (df["market"] == "us").all()
    assert df["close"].iloc[0] == 5000.0


def test_benchmark_hk(mocker):
    raw = pd.DataFrame({"Close": [18000.0, 18100.0]},
                       index=pd.to_datetime(["2026-06-25", "2026-06-26"]))
    mocker.patch("yfinance.download", return_value=raw)
    df = BenchmarkFetcher().fetch("hk", "2026-06-25", "2026-06-26")
    assert (df["market"] == "hk").all()
    assert len(df) == 2
