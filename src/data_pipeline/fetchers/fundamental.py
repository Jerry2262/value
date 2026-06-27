"""三地财务 Fetcher。

探针实测约束：
- A 股 akshare 无「公告日」列 → announcement_date_approx = 报告期 + 固定滞后（spec §3.4 降级）
- 财务字段（roe/营收/净利润/fcf）禁用 stock_zh_a_spot_em（无这些列），用 stock_financial_abstract
"""
from __future__ import annotations

import calendar
import time  # 必须 top-level import：retry 测试 patch src.data_pipeline.fetchers.fundamental.time.sleep
from datetime import date

import pandas as pd

from src.data_pipeline.fetchers.base import (
    FUNDAMENTAL_COLUMNS,
    retry_with_backoff,
)

# 报告期 → 公告日近似滞后（月）
_LAG_MONTHS = {"annual": 4, "interim": 2, "q1": 1, "q3": 1}
# 报告期月份 → 报告类型
_PERIOD_MONTH_TO_TYPE = {3: "q1", 6: "interim", 9: "q3", 12: "annual"}


def _add_months(d: date, months: int) -> date:
    """加月（月末语义：原日为该月月末 → 结果取目标月月末）。

    brief 原实现 ``min(d.day, last_day)`` 仅向下钳位，导致
    06-30 +2月 → 08-30（应为 08-31）、09-30 +1月 → 10-30（应为 10-31）。
    此处在原日为月末时直接取目标月月末，保持月末语义。
    例：06-30 +2月 → 08-31；01-31 +1月 → 02-28/29；12-31 +4月 → 04-30。
    """
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    src_last_day = calendar.monthrange(d.year, d.month)[1]
    if d.day == src_last_day:
        return date(y, m, last_day)
    return date(y, m, min(d.day, last_day))


def approx_announcement_date(report_period: str, report_type: str) -> str | None:
    """报告期 + 固定滞后 → 近似公告日（YYYY-MM-DD）。

    spec §3.4 降级：年报+4月、中报+2月、季报+1月。
    report_period 须为标准月末（03-31/06-30/09-30/12-31），否则返回 None。
    """
    try:
        rp = date.fromisoformat(report_period)
    except (ValueError, TypeError):
        return None
    if rp.month not in _PERIOD_MONTH_TO_TYPE:
        return None
    lag = _LAG_MONTHS[report_type]
    approx = _add_months(rp, lag)
    return approx.isoformat()


def _classify_period(report_period: str) -> str | None:
    """根据报告期月份推断报告类型。"""
    try:
        rp = date.fromisoformat(report_period)
    except (ValueError, TypeError):
        return None
    return _PERIOD_MONTH_TO_TYPE.get(rp.month)


def _normalize_a_share(raw: pd.DataFrame, code: str) -> pd.DataFrame:
    df = raw.copy()
    rename = {
        "股票代码": "code", "报告期": "report_period",
        "营业收入": "revenue", "净利润": "net_profit",
        "净资产收益率": "roe", "资产负债率": "debt_ratio",
        "经营现金流": "fcf",  # 近似：经营现金流作为 FCF 代理（探针阶段 FCF 字段常缺）
        "总市值": "total_market_cap",
    }
    df = df.rename(columns=rename)
    df["market"] = "a_share"
    # 公告日降级
    df["announcement_date_approx"] = df.apply(
        lambda r: (
            approx_announcement_date(r["report_period"], _classify_period(r["report_period"]))
            if pd.notna(r.get("report_period")) else None
        ),
        axis=1,
    )
    # 缺失字段补 None
    for col in FUNDAMENTAL_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df["code"] = code
    return df[FUNDAMENTAL_COLUMNS]


class AShareFundamentalFetcher:
    """A 股财务（akshare stock_financial_abstract，批量接口）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, code: str) -> pd.DataFrame:
        import akshare as ak
        try:
            raw = ak.stock_financial_abstract(symbol=code)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare stock_financial_abstract 失败 {code}") from exc
        if raw is None or raw.empty:
            return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)
        return _normalize_a_share(raw, code)


class USFundamentalFetcher:
    """美股财务（yfinance Ticker.financials/info）。v1 用基础字段。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, code: str) -> pd.DataFrame:
        import yfinance as yf
        try:
            tkr = yf.Ticker(code)
            info = tkr.info or {}
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yfinance 财务失败 {code}") from exc
        row = {
            "code": code, "market": "us",
            "report_period": None, "announcement_date_approx": None,
            "revenue": info.get("totalRevenue"),
            "net_profit": info.get("netIncomeToCommon"),
            "roe": info.get("returnOnEquity"),
            "debt_ratio": None,
            "fcf": info.get("freeCashflow"),
            "total_market_cap": info.get("marketCap"),
        }
        return pd.DataFrame([row])[FUNDAMENTAL_COLUMNS]


class HKFundamentalFetcher:
    """港股财务（akshare 主，字段不全时部分为 None）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, code: str) -> pd.DataFrame:
        import akshare as ak
        try:
            raw = ak.stock_financial_hk_report_em(symbol=code, symbol_type="资产负债表")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare 港股财务失败 {code}") from exc
        if raw is None or raw.empty:
            return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)
        df = raw.copy()
        df["code"] = code
        df["market"] = "hk"
        df["announcement_date_approx"] = None  # 港股 PIT 标注不修正（spec §3.5）
        for col in FUNDAMENTAL_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[FUNDAMENTAL_COLUMNS]
