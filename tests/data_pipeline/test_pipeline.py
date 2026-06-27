import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.pipeline import run_daily_pipeline


def test_run_daily_pipeline_quote_clean_store(mocker, isolated_data_dir):
    """一日链路：fetch 行情 → clean（后复权）→ store → 可读回。"""
    raw = pd.DataFrame({
        "日期": ["2026-06-26"], "开盘": [10.0], "收盘": [10.2],
        "最高": [10.5], "最低": [9.9], "成交量": [100000],
    })
    mocker.patch("akshare.stock_zh_a_hist", return_value=raw)
    # Task 9 FXFetcher 实测列：currency_boc_sina 返回「央行中间价/中行折算价」而非「收盘」
    fx_raw = pd.DataFrame({"日期": ["2026-06-26"], "央行中间价": [7.18], "中行折算价": [7.18]})
    mocker.patch("akshare.currency_boc_sina", return_value=fx_raw)

    result = run_daily_pipeline(
        run_date="2026-06-27",
        codes={"a_share": ["600519"]},
        fx_pairs=["USD/CNY"],
    )
    assert result.status["quote"]["a_share"] == "ok"
    # FX 路径须成功（旧 mock 用「收盘」列会落入「未找到可用汇率列」分支 → 静默 STALE）
    assert result.status["fx"]["USD/CNY"] == "ok"
    # 行情已清洗存储，可读回
    from src.data_pipeline.store import read_parquet
    df = read_parquet("market", "a_share", as_of="2026-06-27")
    assert len(df) == 1
    assert df["close"].iloc[0] == 10.2  # adj_factor=1 → 后复权=原值


def test_run_daily_pipeline_marks_stale_on_failure(mocker, isolated_data_dir):
    """某市场 fetcher 重试耗尽 → 标记 STALE，不阻塞其他市场（spec §3.6）。"""
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    mocker.patch("akshare.stock_zh_a_hist", side_effect=RuntimeError("net"))
    us_raw = pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2026-06-26"]))
    mocker.patch("yfinance.download", return_value=us_raw)

    result = run_daily_pipeline(
        run_date="2026-06-27",
        codes={"a_share": ["600519"], "us": ["AAPL"]},
        fx_pairs=[],
    )
    assert result.status["quote"]["a_share"] == "STALE"
    assert result.status["quote"]["us"] == "ok"
    assert "a_share" in result.stale_markets


def test_pipeline_result_dataclass():
    from src.data_pipeline.pipeline import PipelineResult
    r = PipelineResult(status={"quote": {}}, anomalies=pd.DataFrame(), stale_markets=[])
    assert r.stale_markets == []
