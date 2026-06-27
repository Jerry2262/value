"""Parquet 日期分区存储 + 增量更新 + PIT 读接口（spec §3.2/§3.4）。"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src import config


def _raw_dir() -> Path:
    return config.DATA_DIR / "raw"


def write_parquet_partition(
    df: pd.DataFrame, kind: str, partition_date: str, market: str
) -> Path:
    """写不可变 Parquet 分区：data/raw/{kind}/{partition_date}/{market}.parquet。

    partition_date 为拉取日期（行情）或财报披露日（财务）。
    若分区已存在，覆盖写（同日重拉以最新为准）。
    """
    path = _raw_dir() / kind / partition_date / f"{market}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def read_parquet(
    kind: str, market: str, as_of: str | None = None
) -> pd.DataFrame:
    """读取某 kind/market 的全部分区并拼接。

    as_of 给定时，只读 partition_date ≤ as_of 的分区（PIT 约束，spec §3.4）。
    无数据 → 空 DataFrame。
    """
    base = _raw_dir() / kind
    if not base.exists():
        return pd.DataFrame()
    frames = []
    for part_dir in sorted(base.iterdir()):
        if not part_dir.is_dir():
            continue
        if as_of is not None and part_dir.name > as_of:
            continue
        f = part_dir / f"{market}.parquet"
        if f.exists():
            frames.append(pd.read_parquet(f))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def incremental_merge(
    new_df: pd.DataFrame, existing_df: pd.DataFrame, keys: list[str]
) -> pd.DataFrame:
    """按 keys 去重合并：new_df 覆盖 existing_df 同主键行，新行追加。"""
    if existing_df.empty:
        return new_df.copy()
    if new_df.empty:
        return existing_df.copy()
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    # 保留每个 key 组合的最后一条（new 在后，覆盖 old）
    combined = combined.drop_duplicates(subset=keys, keep="last")
    return combined.reset_index(drop=True)
