"""PIT Indexer：行级 PIT 过滤 + 跨分区去重（spec §3.4）。

复用 Phase 2 store.read_parquet 的分区读取能力，在此基础上做行级 PIT 过滤
（date / announcement_date_approx / delist_date <= as_of）与跨分区去重。

设计要点（与 brief 的差异，均已通过 test_indexer.py 验证）：
- 行情（quote）：PIT 门 = `date`，分区门 = partition_date。两者同为交易/拉取时间线，
  read_parquet(as_of=) 的分区级过滤 + 行级 date<=as_of + keep="last"（按 partition_date
  升序拼接，后拼的更新）即正确。与 brief 一致。
- 财报（fundamental）：PIT 门 = `announcement_date_approx`（披露日），而 partition_date 是
  该分区"最新披露日"。同一 report_period 的修正版可能出现在更晚的分区里、但
  announcement_date_approx 仍为原披露日。若对 fundamental 施加 read_parquet(as_of=) 分区
  过滤，会在 as_of 早于修正分区时丢掉修正版（test_pit_fundamental_dedup_takes_latest_disclosure
  期望 roe=31，分区过滤下只得 roe=30）。故 fundamental 不做分区级过滤，读全部分区后行级
  过滤 announcement_date_approx<=as_of，再跨分区去重。
- 退市（delisting）：PIT 门 = `delist_date`（退市生效日），partition_date 仅为拉取日。
  退市列表是慢变参考表，历史退市（delist_date 远早于拉取日）必须对任意 as_of 可见。
  分区过滤会把"拉取日晚于 as_of"的分区整块排除，导致 test_pit_delisted_before 在
  as_of=2024-01-01 下读不到 2020 年退市的 C1。故 delisting 不做分区级过滤，读全部分区后
  行级过滤 delist_date<=as_of。无前视风险：delist_date 行级过滤是 PIT 唯一门。
- 去重稳定性：fundamental 在 announcement_date_approx 相等时，需保留较新分区版本。
  sort_values 默认 quicksort 不保证稳定，故显式 kind="stable"，使等值行保持 partition_date
  升序拼接顺序，keep="last" 即取最新分区版本。
"""
from __future__ import annotations

import pandas as pd

from src.data_pipeline import store


def pit_quote_as_of(as_of: str, market: str, code: str | None = None) -> pd.DataFrame:
    """返回 as_of 日可见的后复权行情（date <= as_of，跨分区去重）。

    分区级过滤已由 read_parquet(as_of=) 完成（partition_date <= as_of）。
    此处再做行级 date <= as_of 过滤 + 跨分区去重（同 (code,date) 取最新分区版本）。
    """
    # 读所有 partition_date <= as_of 的分区（含历史分区，用于去重取最新版本）
    raw = store.read_parquet("market", market, as_of=as_of)
    if raw.empty:
        return raw
    df = raw[raw["date"] <= as_of].copy()
    if code is not None:
        df = df[df["code"] == code]
    if df.empty:
        return df
    # 跨分区去重：同 (code,date) 保留最后一条（read_parquet 按 partition_date 升序拼接，
    # 后拼接的分区更新 → keep="last"）
    df = df.drop_duplicates(subset=["code", "date"], keep="last")
    return df.reset_index(drop=True)


def pit_fundamental_as_of(as_of: str, market: str, code: str | None = None) -> pd.DataFrame:
    """返回 as_of 日可见的已披露财报（announcement_date_approx <= as_of，跨分区去重）。

    spec §3.4：T 日只能看到 announcement_date_approx <= T 的所有已披露财报。
    跨分区去重：同 (code, report_period) 保留较新披露版本（announcement_date 较大者；
    相等时取较新分区版本）。

    注：不对 fundamental 施加 read_parquet 分区级过滤——partition_date 是分区"最新披露日"，
    同一 report_period 的修正版常出现在更晚分区但 announcement_date 仍为原披露日。详见模块
    docstring。
    """
    raw = store.read_parquet("fundamental", market)
    if raw.empty:
        return raw
    # 行级过滤：announcement_date_approx <= as_of（缺失则视为不可见）
    mask = raw["announcement_date_approx"].notna() & (raw["announcement_date_approx"] <= as_of)
    df = raw[mask].copy()
    if code is not None:
        df = df[df["code"] == code]
    if df.empty:
        return df
    # 跨分区去重：同 (code, report_period) 保留 announcement_date_approx 较大者（较新披露）；
    # 相等时依赖稳定排序保留 partition_date 升序拼接顺序，keep="last" 取最新分区版本。
    df = df.sort_values("announcement_date_approx", kind="stable")
    df = df.drop_duplicates(subset=["code", "report_period"], keep="last")
    return df.reset_index(drop=True)


def pit_delisted_before(as_of: str, market: str) -> pd.DataFrame:
    """返回 as_of 日前已退市的股票（delist_date <= as_of）。

    spec §3.5（v1.4）：仅 A 股有效；港美股退市为人工补录非重点。

    注：不对 delisting 施加 read_parquet 分区级过滤——退市列表是慢变参考表，partition_date
    仅为拉取日，历史退市须对任意 as_of 可见。delist_date 行级过滤是 PIT 唯一门，无前视风险。
    """
    from src.data_pipeline.fetchers.base import DELISTING_COLUMNS
    raw = store.read_parquet("delisting", market)
    if raw.empty:
        return pd.DataFrame(columns=DELISTING_COLUMNS)
    mask = raw["delist_date"].notna() & (raw["delist_date"] <= as_of)
    return raw[mask].reset_index(drop=True)


def pit_active_universe(as_of: str, market: str, all_codes: list[str]) -> list[str]:
    """从 all_codes 中扣除 as_of 日前已退市的，返回 T 日仍在市的股票。"""
    delisted = pit_delisted_before(as_of, market)
    delisted_codes = set(delisted["code"]) if not delisted.empty else set()
    return [c for c in all_codes if c not in delisted_codes]
