"""三地日线行情 Fetcher（akshare/yfinance 懒加载）。

列契约：QUOTE_COLUMNS（date/code/market/open/high/low/close/volume/adj_factor）。
adj_factor 默认 1.0（不复权快照）；后复权变换在 cleaners 中应用。
"""
from __future__ import annotations

import logging
import time

import pandas as pd

from src.data_pipeline.fetchers.base import FetcherError, QUOTE_COLUMNS, retry_with_backoff

logger = logging.getLogger(__name__)


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
    # Brief bug fix (Task 8): 数据源偶尔只返回部分 OHLCV 列（如 yfinance 仅 Close），
    # 直接 df[QUOTE_COLUMNS] 会 KeyError → 经 retry_with_backoff 包装成 FetcherError
    # → 市场被误标 STALE。缺失列补 NaN；下游 clean_quote / flag_quote_anomalies 均以
    # `if col in df.columns` + pd.to_numeric(errors="coerce") 守卫，可安全吞 NaN。
    out = df.reindex(columns=QUOTE_COLUMNS)
    # NaN guard (Task 8 review fix): 数据源返回非空但 OHLC 列不可识别时，reindex 会
    # 产出 date/code/market/adj_factor 有值、open/high/low/close/volume 全 NaN 的行 →
    # 下游静默存脏数据，绕过 spec §3.6 STALE 机制。改为响亮失败：抛 FetcherError，
    # 经 retry_with_backoff 包装后 pipeline 将该市场标记 STALE（不静默存 NaN）。
    if not out.empty and out["close"].isna().all():
        raise FetcherError(
            f"{market} {code}: 归一化后 close 全为 NaN（数据源列不可识别）"
        )
    return out


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
        except Exception as exc:  # noqa: BLE001
            # 记录主源失败原因（含 _normalize_quote 的 NaN-guard FetcherError），
            # 避免数据质量失败与「源宕机」无法区分。
            logger.warning("港股 %s akshare 主源失败，降级 yfinance: %s", code, exc)
        # 备源 yfinance（港股代码加 .HK 后缀）
        try:
            import yfinance as yf
            raw = yf.download(f"{code}.HK", start=start, end=end, progress=False, auto_adjust=False)
            if raw.empty:
                raise RuntimeError("yfinance 港股返回空")
            return _normalize_quote(raw, code, "hk")
        except Exception as exc:  # noqa: BLE001
            raise FetcherError(f"港股 {code} 主备源均失败") from exc
