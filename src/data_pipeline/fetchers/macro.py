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


# akshare currency_boc_sina 接受中文货币名（实测：USDCNY 形式已不可用，抛 KeyError）。
# 映射来源：akshare currency_boc_sina docstring 所列全部可接受 symbol。
_BOC_SYMBOL_MAP = {
    "USD": "美元", "GBP": "英镑", "EUR": "欧元", "MOP": "澳门元",
    "THB": "泰国铢", "PHP": "菲律宾比索", "HKD": "港币", "CHF": "瑞士法郎",
    "SGD": "新加坡元", "SEK": "瑞典克朗", "DKK": "丹麦克朗", "NOK": "挪威克朗",
    "JPY": "日元", "CAD": "加拿大元", "AUD": "澳大利亚元", "NZD": "新西兰元",
    "KRW": "韩国元",
}


class FXFetcher:
    """汇率日线（akshare currency_boc_sina）。

    实测字段映射修正（Task 9 集成测试）：currency_boc_sina 不返回 ``收盘`` 列，
    实际返回 ``央行中间价``（PBoC 中间价，交易日有值、周末 NaN）与 ``中行折算价``
    （BoC 折算价，逐日有值，交易日与中间价一致）。``rate`` 取 ``央行中间价`` 并以
    ``中行折算价`` 填充周末/假日缺口，得到无缺口日线序列（spec §3：汇率用 T 日收盘价，
    此处为最接近的可用参考价）。注意 BoC 牌价按「每 100 外币单位」报价，``rate`` 保留
    原始刻度；下游回测汇率变动公式为比值（刻度无关），未来若按单位折算需 /100。
    """

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, pair: str, start: str, end: str) -> pd.DataFrame:
        import akshare as ak
        base, quote = _parse_pair(pair)
        boc_symbol = _BOC_SYMBOL_MAP.get(base)
        if boc_symbol is None:
            raise FetcherError(f"currency_boc_sina 不支持 {base}（无中文货币名映射）")
        try:
            raw = ak.currency_boc_sina(
                symbol=boc_symbol,
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare 汇率失败 {pair}") from exc
        df = raw.copy()
        if "日期" in df.columns:
            df = df.rename(columns={"日期": "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        # 实测列：央行中间价（交易日）/ 中行折算价（逐日）。优先中间价，缺口用折算价填。
        if "央行中间价" in df.columns and "中行折算价" in df.columns:
            df["rate"] = pd.to_numeric(df["央行中间价"], errors="coerce").fillna(
                pd.to_numeric(df["中行折算价"], errors="coerce")
            )
        elif "中行折算价" in df.columns:
            df["rate"] = pd.to_numeric(df["中行折算价"], errors="coerce")
        elif "央行中间价" in df.columns:
            df["rate"] = pd.to_numeric(df["央行中间价"], errors="coerce")
        else:
            raise FetcherError(
                f"currency_boc_sina {pair}: 未找到可用汇率列"
                f"（columns={list(df.columns)}）"
            )
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
