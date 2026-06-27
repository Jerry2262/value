import pandas as pd
import pytest

from src.data_pipeline.cleaners import clean_fundamental, clean_quote, flag_quote_anomalies
from src.data_pipeline.fetchers.base import QUOTE_COLUMNS


def _quote_df(rows):
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS)


def test_clean_quote_applies_adj_factor():
    """后复权：OHLC 乘 adj_factor，volume 不变（spec §3.3）。"""
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": "10.0", "high": "10.5", "low": "9.9", "close": "10.2",
         "volume": "100000", "adj_factor": "2.0"},
    ])
    out = clean_quote(df)
    assert out["close"].iloc[0] == 20.4  # 10.2 * 2.0
    assert out["open"].iloc[0] == 20.0
    assert out["volume"].iloc[0] == 100000  # volume 不复权
    assert out["adj_factor"].iloc[0] == 2.0


def test_clean_quote_dtypes_numeric():
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": "10", "high": "11", "low": "9", "close": "10",
         "volume": "100", "adj_factor": "1"},
    ])
    out = clean_quote(df)
    assert pd.api.types.is_numeric_dtype(out["close"])
    assert pd.api.types.is_numeric_dtype(out["volume"])


def test_flag_anomalies_low_gt_high():
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 9, "low": 11, "close": 10,  # low>high
         "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-26", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10,
         "volume": 100, "adj_factor": 1.0},
    ])
    anomalies = flag_quote_anomalies(df)
    assert len(anomalies) == 1
    assert anomalies["date"].iloc[0] == "2026-06-25"


def test_flag_anomalies_close_zero():
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 0,  # close<=0
         "volume": 100, "adj_factor": 1.0},
    ])
    anomalies = flag_quote_anomalies(df)
    assert len(anomalies) == 1


def test_flag_anomalies_huge_swing():
    """单日涨跌 >50% → 异常（spec §3.6）。"""
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 20, "low": 9, "close": 18,  # 18/10=1.8 涨80%
         "volume": 100, "adj_factor": 1.0},
    ])
    anomalies = flag_quote_anomalies(df)
    assert len(anomalies) == 1


def test_clean_fundamental_numeric_coercion():
    from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS
    df = pd.DataFrame([{
        "code": "C", "market": "a_share", "report_period": "2023-12-31",
        "announcement_date_approx": "2024-04-30",
        "revenue": "1.5e11", "net_profit": "7e10", "roe": "30.0",
        "debt_ratio": "20.0", "fcf": "5e10", "total_market_cap": "1e12",
    }], columns=FUNDAMENTAL_COLUMNS)
    out = clean_fundamental(df)
    assert out["revenue"].iloc[0] == 1.5e11
    assert pd.api.types.is_numeric_dtype(out["roe"])
