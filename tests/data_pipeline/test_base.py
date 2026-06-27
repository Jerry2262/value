import time

import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import (
    DELISTING_COLUMNS,
    FETCHER_MARKET_STALE,
    FX_COLUMNS,
    FUNDAMENTAL_COLUMNS,
    FetcherError,
    QUOTE_COLUMNS,
    BENCHMARK_COLUMNS,
    retry_with_backoff,
)


def test_standard_columns_are_canonical():
    """标准化列名常量是下游契约，名字不可漂移。"""
    assert QUOTE_COLUMNS == ["date", "code", "market", "open", "high", "low", "close", "volume", "adj_factor"]
    assert FUNDAMENTAL_COLUMNS == [
        "code", "market", "report_period", "announcement_date_approx",
        "revenue", "net_profit", "roe", "debt_ratio", "fcf", "total_market_cap",
    ]
    assert FX_COLUMNS == ["date", "base", "quote", "rate"]
    assert BENCHMARK_COLUMNS == ["date", "code", "market", "close"]
    assert DELISTING_COLUMNS == ["code", "market", "delist_date", "reason"]


def test_retry_succeeds_on_first_try(mocker):
    calls = {"n": 0}

    @retry_with_backoff(retries=3, delays=(0, 0, 0))
    def ok():
        calls["n"] += 1
        return "done"

    assert ok() == "done"
    assert calls["n"] == 1


def test_retry_succeeds_after_transient_failures(mocker):
    """前两次抛异常、第三次成功 → 返回成功值，调用 3 次。"""
    mocker.patch("src.data_pipeline.fetchers.base.time.sleep")  # 跳过真实退避
    calls = {"n": 0}

    @retry_with_backoff(retries=3, delays=(0, 0, 0))
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_retry_raises_after_exhausting(mocker):
    mocker.patch("src.data_pipeline.fetchers.base.time.sleep")
    calls = {"n": 0}

    @retry_with_backoff(retries=3, delays=(0, 0, 0))
    def always_fail():
        calls["n"] += 1
        raise RuntimeError("boom")

    with pytest.raises(FetcherError) as exc_info:
        always_fail()
    assert calls["n"] == 3
    assert "boom" in str(exc_info.value)


def test_fetcher_error_wraps_cause():
    cause = ValueError("network down")
    err = FetcherError("拉取 A 股行情失败", cause=cause)
    assert err.__cause__ is cause
    assert "A 股行情" in str(err)


def test_stale_constant_value():
    assert FETCHER_MARKET_STALE == "STALE"
