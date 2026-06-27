"""三地日线行情 Fetcher（akshare/yfinance 懒加载）。

列契约：QUOTE_COLUMNS（date/code/market/open/high/low/close/volume/adj_factor）。
adj_factor 默认 1.0（不复权快照）；后复权变换在 cleaners 中应用。
"""
from __future__ import annotations

import time

import pandas as pd

from src.data_pipeline.fetchers.base import FetcherError, QUOTE_COLUMNS, retry_with_backoff


def _normalize_quote(df: pd.DataFrame, code: str, market: str) -> pd.DataFrame:
    """把原始行情统一为 QUOTE_COLUMNS 列。"""
    df = df.copy()
    # 统一 date 列为字符串 YYYY-MM-DD
    if "日期" in df.columns:
        df = df.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
        })
    elif "Date" in df.columns:
        df = df.rename(columns={
            "Date": "date", "Open": "open", "Close": "close",
            "High": "high", "Low": "low", "Volume": "volume",
        })
    else:
        # yfinance 用 DatetimeIndex（name 可为 None 或 "Date"）→ reset 成 date 列
        if not isinstance(df.index, pd.RangeIndex):
            df = df.reset_index()
            if "Date" in df.columns:
                df = df.rename(columns={"Date": "date"})
            elif "index" in df.columns:
                df = df.rename(columns={"index": "date"})
        # brief 修正：DatetimeIndex 分支下 OHLCV 列仍为大写，
        # 需统一为小写（原实现漏掉此步导致 df[QUOTE_COLUMNS] KeyError）
        df = df.rename(columns={
            "Open": "open", "Close": "close", "High": "high",
            "Low": "low", "Volume": "volume",
        })

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["code"] = code
    df["market"] = market
    if "adj_factor" not in df.columns:
        df["adj_factor"] = 1.0
    return df[QUOTE_COLUMNS]


class AShareQuoteFetcher:
    """A 股日线行情（akshare stock_zh_a_hist）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch_daily(self, code: str, start: str, end: str) -> pd.DataFrame:
        import akshare as ak
        try:
            raw = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                adjust="",  # 不复权快照；后复权在 cleaner 处理
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare stock_zh_a_hist 失败 {code}") from exc
        return _normalize_quote(raw, code, "a_share")


class USQuoteFetcher:
    """美股日线行情（yfinance download）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch_daily(self, code: str, start: str, end: str) -> pd.DataFrame:
        import yfinance as yf
        try:
            raw = yf.download(code, start=start, end=end, progress=False, auto_adjust=False)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yfinance 失败 {code}") from exc
        if raw.empty:
            raise RuntimeError(f"yfinance 返回空 {code}")
        return _normalize_quote(raw, code, "us")


class HKQuoteFetcher:
    """港股日线行情：akshare 主源，失败降级 yfinance 备源（spec §3.6）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch_daily(self, code: str, start: str, end: str) -> pd.DataFrame:
        # 主源 akshare
        try:
            import akshare as ak
            raw = ak.stock_hk_hist(
                symbol=code, period="daily",
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                adjust="",
            )
            return _normalize_quote(raw, code, "hk")
        except Exception:  # noqa: BLE001
            pass  # 降级备源
        # 备源 yfinance（港股代码加 .HK 后缀）
        try:
            import yfinance as yf
            raw = yf.download(f"{code}.HK", start=start, end=end, progress=False, auto_adjust=False)
            if raw.empty:
                raise RuntimeError("yfinance 港股返回空")
            return _normalize_quote(raw, code, "hk")
        except Exception as exc:  # noqa: BLE001
            raise FetcherError(f"港股 {code} 主备源均失败") from exc
