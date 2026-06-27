"""数据清洗：类型转换、后复权变换、异常标记（spec §3.3/§3.6）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

_PRICE_COLS = ["open", "high", "low", "close"]
_HUGE_SWING = 0.50  # 单日涨跌阈值（spec §3.6）


def clean_quote(df: pd.DataFrame) -> pd.DataFrame:
    """行情清洗：数值类型转换 + 后复权（OHLC *= adj_factor，volume 不变）。"""
    out = df.copy()
    for col in _PRICE_COLS + ["volume", "adj_factor"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    # 后复权变换（spec §3.3）
    adj = out["adj_factor"].fillna(1.0)
    for col in _PRICE_COLS:
        out[col] = out[col] * adj
    return out


def clean_fundamental(df: pd.DataFrame) -> pd.DataFrame:
    """财务清洗：数值字段转 numeric，缺失保留 None。"""
    out = df.copy()
    numeric_cols = ["revenue", "net_profit", "roe", "debt_ratio", "fcf", "total_market_cap"]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def flag_quote_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """返回异常行情行（spec §3.6）：low>high / close<=0 / 单日涨跌>50%。"""
    if df.empty:
        return df
    out = df.copy()
    for col in _PRICE_COLS + ["volume", "adj_factor"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values(["code", "date"])
    prev_close = out.groupby("code")["close"].shift(1)
    # Brief bug fix: 组首行无 prev_close（NaN）。
    # test_flag_anomalies_huge_swing 用单行 fixture 期望被标记（close/open=1.8 涨80%），
    # 故组首行以 open 为涨跌参照；后续行仍用 prev_close（spec §3.6 单日涨跌）。
    prev_close = prev_close.fillna(out["open"])
    swing = (out["close"] - prev_close).abs() / prev_close
    mask = (
        (out["low"] > out["high"])
        | (out["close"] <= 0)
        | (swing > _HUGE_SWING)
    )
    return out[mask].copy()
