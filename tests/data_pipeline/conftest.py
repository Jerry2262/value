import pandas as pd
import pytest


@pytest.fixture
def sample_a_share_quote_raw():
    """模拟 akshare index/个股接口返回的原始 DataFrame（中文列名）。

    后续 cleaner 测试和 fetcher mock 测试复用。
    """
    return pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "开盘": [10.0, 10.5],
        "收盘": [10.2, 10.4],
        "最高": [10.5, 10.6],
        "最低": [9.9, 10.1],
        "成交量": [100000, 120000],
    })


@pytest.fixture
def sample_us_quote_raw():
    """模拟 yfinance 返回的原始 DataFrame（英文列名、DatetimeIndex）。"""
    idx = pd.to_datetime(["2026-06-24", "2026-06-25"])
    return pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "Close": [101.5, 100.8],
            "High": [102.0, 101.5],
            "Low": [99.5, 100.0],
            "Volume": [1000000, 1100000],
        },
        index=idx,
    )


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """把 VALUE_DATA_DIR 指向临时目录，刷新 config 模块常量。"""
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)
