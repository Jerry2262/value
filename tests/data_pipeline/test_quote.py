import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.fetchers.quote import (
    AShareQuoteFetcher,
    HKQuoteFetcher,
    USQuoteFetcher,
)


def test_a_share_fetcher_normalizes_columns(mocker, sample_a_share_quote_raw):
    """akshare 返回中文列名 → 标准化列，并补 code/market/adj_factor。"""
    mocker.patch(
        "akshare.stock_zh_a_hist",
        return_value=sample_a_share_quote_raw,
    )
    df = AShareQuoteFetcher().fetch_daily("600519", "2026-06-25", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert len(df) == 2
    assert (df["code"] == "600519").all()
    assert (df["market"] == "a_share").all()
    assert (df["adj_factor"] == 1.0).all()  # akshare 默认不复权，因子=1；后复权由 cleaner 处理
    assert df["close"].iloc[0] == 10.2


def test_a_share_fetcher_retry_then_fail(mocker):
    """连续失败 → 重试耗尽抛 FetcherError。"""
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    mocker.patch(
        "akshare.stock_zh_a_hist",
        side_effect=RuntimeError("network"),
    )
    from src.data_pipeline.fetchers.base import FetcherError
    with pytest.raises(FetcherError):
        AShareQuoteFetcher().fetch_daily("600519", "2026-06-25", "2026-06-26")


def test_us_fetcher_normalizes_yfinance(mocker, sample_us_quote_raw):
    mocker.patch("yfinance.download", return_value=sample_us_quote_raw)
    df = USQuoteFetcher().fetch_daily("AAPL", "2026-06-24", "2026-06-25")
    assert list(df.columns) == QUOTE_COLUMNS
    assert (df["code"] == "AAPL").all()
    assert (df["market"] == "us").all()
    assert df["close"].iloc[0] == 101.5


def test_hk_fetcher_uses_akshare_primary(mocker, sample_a_share_quote_raw):
    mocker.patch("akshare.stock_hk_hist", return_value=sample_a_share_quote_raw)
    df = HKQuoteFetcher().fetch_daily("00700", "2026-06-25", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert (df["market"] == "hk").all()
    assert (df["code"] == "00700").all()


def test_hk_fetcher_fallback_to_yfinance(mocker, sample_us_quote_raw):
    """akshare 失败 → 降级 yfinance 备源（spec §3.6 港股主备互补）。"""
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    mocker.patch("akshare.stock_hk_hist", side_effect=RuntimeError("akshare down"))
    mocker.patch("yfinance.download", return_value=sample_us_quote_raw)
    df = HKQuoteFetcher().fetch_daily("00700", "2026-06-24", "2026-06-25")
    assert len(df) == 2
    assert (df["market"] == "hk").all()


def test_normalize_rejects_unrecognized_columns(mocker):
    """数据源返回非空但无 OHLC 列 → FetcherError（不静默存 NaN，spec §3.6 STALE）。

    回归锁死：yfinance 风格 DatetimeIndex + 无 Close 列 → reset_index 得 date，
    但 reindex 后 close 全 NaN。若无 NaN guard，下游会静默存全 NaN 行（绕过 STALE）。
    """
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    # yfinance 风格：DatetimeIndex + 无任何 OHLC 列（reindex 前可识别 date，close 缺失）
    garbage = pd.DataFrame(
        {"foo": [1, 2], "bar": [3, 4]},
        index=pd.to_datetime(["2026-06-24", "2026-06-25"]),
    )
    mocker.patch("yfinance.download", return_value=garbage)
    from src.data_pipeline.fetchers.base import FetcherError
    with pytest.raises(FetcherError):
        USQuoteFetcher().fetch_daily("AAPL", "2026-06-24", "2026-06-25")
