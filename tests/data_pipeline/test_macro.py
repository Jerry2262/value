import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import BENCHMARK_COLUMNS, FX_COLUMNS, FetcherError
from src.data_pipeline.fetchers.macro import BenchmarkFetcher, FXFetcher


def test_fx_fetcher_normalizes(mocker):
    # 实测 currency_boc_sina 返回列：央行中间价（交易日）/ 中行折算价（逐日），无「收盘」列。
    # row2 央行中间价缺失（周末/假日）→ 走 中行折算价 fillna 回退路径。
    raw = pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "央行中间价": [718.0, None],
        "中行折算价": [718.0, 716.0],
    })
    mock_boc = mocker.patch("akshare.currency_boc_sina", return_value=raw)
    df = FXFetcher().fetch("USD/CNY", "2026-06-25", "2026-06-26")
    assert list(df.columns) == FX_COLUMNS
    assert (df["base"] == "USD").all()
    assert (df["quote"] == "CNY").all()
    assert df["rate"].iloc[0] == 718.0
    # row2 中间价缺 → 折算价回退填 716.0
    assert df["rate"].iloc[1] == 716.0
    # 确认传给 akshare 的是中文货币名（实测：USDCNY 形式已不可用）
    args, kwargs = mock_boc.call_args
    assert kwargs.get("symbol") == "美元"


def test_fx_fetcher_raises_on_all_nan_rate(mocker):
    """两列皆存在但某行中间价/折算价同为 NaN → FetcherError（行级 NaN 守卫）。

    回归锁死：列存在性检查通过，但 fillna 后仍残留 NaN rate 行 → 显式失败，
    不让 NaN 静默混入 FX 序列（修复「loud failure 仅在列缺失时触发」的缺口）。
    """
    mocker.patch("src.data_pipeline.fetchers.base.time.sleep")  # 跳过重试退避
    raw = pd.DataFrame({
        "日期": ["2026-06-25"],
        "央行中间价": [None],
        "中行折算价": [None],
    })
    mocker.patch("akshare.currency_boc_sina", return_value=raw)
    with pytest.raises(FetcherError):
        FXFetcher().fetch("USD/CNY", "2026-06-25", "2026-06-25")


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
