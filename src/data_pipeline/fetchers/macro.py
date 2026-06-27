"""汇率 + 基准指数 Fetcher。

探针实测约束：沪深300价值指数缺失率 68.3% → 降级宽基。
"""
from __future__ import annotations

import pandas as pd

from src.data_pipeline.fetchers.base import (
    BENCHMARK_COLUMNS,
    FX_COLUMNS,
    FetcherError,
    retry_with_backoff,
)

# 降级映射（spec §6.1 + 探针实测）：市场 → (代码, 源)
BENCHMARK_MAP = {
    "a_share": ("000300", "akshare"),   # 沪深300宽基（价值指数降级）
    "us": ("^GSPC", "yfinance"),        # 标普500宽基
    "hk": ("^HSI", "yfinance"),         # 恒生指数（恒生综合降级）
}


def _parse_pair(pair: str) -> tuple[str, str]:
    base, quote = pair.split("/")
    return base, quote


class FXFetcher:
    """汇率日线（akshare currency_boc_sina）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, pair: str, start: str, end: str) -> pd.DataFrame:
        import akshare as ak
        base, quote = _parse_pair(pair)
        try:
            raw = ak.currency_boc_sina(
                symbol=pair.replace("/", ""),  # akshare 用 USDCNY 形式
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare 汇率失败 {pair}") from exc
        df = raw.copy()
        if "日期" in df.columns:
            df = df.rename(columns={"日期": "date", "收盘": "rate"})
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["base"] = base
        df["quote"] = quote
        return df[FX_COLUMNS]


class BenchmarkFetcher:
    """基准指数日线（自动降级为宽基）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, market: str, start: str, end: str) -> pd.DataFrame:
        if market not in BENCHMARK_MAP:
            raise FetcherError(f"未知市场 {market}")
        symbol, source = BENCHMARK_MAP[market]
        if source == "akshare":
            import akshare as ak
            try:
                raw = ak.index_zh_a_hist(
                    symbol=symbol, period="daily",
                    start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                )
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"akshare 基准失败 {symbol}") from exc
            df = raw.rename(columns={"日期": "date", "收盘": "close"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["code"] = symbol
            df["market"] = market
            return df[BENCHMARK_COLUMNS]
        else:  # yfinance
            import yfinance as yf
            try:
                raw = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=False)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"yfinance 基准失败 {symbol}") from exc
            if raw.empty:
                raise RuntimeError(f"yfinance 基准返回空 {symbol}")
            df = raw.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            # brief 修正：yfinance DatetimeIndex 名称可为 "Date" 或 None，
            # reset_index 后对应列名为 "Date" 或 "index"（与 Task 2 quote.py 同源问题）。
            # 原 brief 仅 rename "Date" → "index" 列漏改，导致 df["date"] KeyError。
            rename_map = {"Close": "close"}
            if "Date" in df.columns:
                rename_map["Date"] = "date"
            elif "index" in df.columns:
                rename_map["index"] = "date"
            df = df.rename(columns=rename_map)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["code"] = symbol
            df["market"] = market
            return df[BENCHMARK_COLUMNS]
