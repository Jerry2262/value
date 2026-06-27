"""Pipeline 编排：fetch → clean → store 一日全链路（spec §3.6 STALE 不阻塞）。"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.data_pipeline.cleaners import clean_quote, flag_quote_anomalies
from src.data_pipeline.fetchers.base import FETCHER_MARKET_STALE, FetcherError
from src.data_pipeline.fetchers.macro import FXFetcher
from src.data_pipeline.fetchers.quote import AShareQuoteFetcher, HKQuoteFetcher, USQuoteFetcher
from src.data_pipeline.store import write_parquet_partition

_QUOTE_FETCHERS = {
    "a_share": AShareQuoteFetcher,
    "us": USQuoteFetcher,
    "hk": HKQuoteFetcher,
}


@dataclass
class PipelineResult:
    status: dict = field(default_factory=dict)
    anomalies: pd.DataFrame = field(default_factory=pd.DataFrame)
    stale_markets: list = field(default_factory=list)


def run_daily_pipeline(
    run_date: str,
    codes: dict[str, list[str]],
    fx_pairs: list[str],
) -> PipelineResult:
    """执行一日数据链路：行情 + 汇率 → 清洗 → 存储。

    某市场失败标记 STALE，不阻塞其他市场（spec §3.6）。
    """
    result = PipelineResult(status={"quote": {}, "fx": {}})
    all_anomalies = []

    # 行情
    for market, market_codes in codes.items():
        if market not in _QUOTE_FETCHERS:
            continue
        fetcher = _QUOTE_FETCHERS[market]()
        frames = []
        for code in market_codes:
            try:
                raw = fetcher.fetch_daily(code, start=run_date, end=run_date)
                frames.append(raw)
            except FetcherError:
                result.status["quote"][market] = FETCHER_MARKET_STALE
                result.stale_markets.append(market)
                break
        else:
            if frames:
                combined = pd.concat(frames, ignore_index=True)
                cleaned = clean_quote(combined)
                write_parquet_partition(cleaned, "market", run_date, market)
                anomalies = flag_quote_anomalies(cleaned)
                if not anomalies.empty:
                    all_anomalies.append(anomalies)
                result.status["quote"][market] = "ok"
            else:
                result.status["quote"][market] = "empty"

    # 汇率
    fx_frames = []
    for pair in fx_pairs:
        try:
            raw = FXFetcher().fetch(pair, start=run_date, end=run_date)
            fx_frames.append(raw)
            result.status["fx"][pair] = "ok"
        except FetcherError:
            result.status["fx"][pair] = FETCHER_MARKET_STALE
    if fx_frames:
        fx_df = pd.concat(fx_frames, ignore_index=True)
        write_parquet_partition(fx_df, "macro", run_date, "fx")

    if all_anomalies:
        result.anomalies = pd.concat(all_anomalies, ignore_index=True)
    return result
