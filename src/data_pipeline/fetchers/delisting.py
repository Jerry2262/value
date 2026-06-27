"""退市列表 Fetcher。

spec §3.5（v1.4 降级）：A 股用 akshare 尽力修正；港美股人工补录 CSV，非重点。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data_pipeline.fetchers.base import DELISTING_COLUMNS, retry_with_backoff


class AShareDelistingFetcher:
    """A 股退市列表（akshare）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self) -> pd.DataFrame:
        import akshare as ak
        try:
            # akshare 退市股票列表接口（字段名以实际为准，cleaner 容错）
            raw = ak.stock_info_sh_name_code(symbol="退市")
        except Exception:
            try:
                raw = ak.stock_info_a_code_name()  # 退市接口变更时的兜底
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("akshare 退市列表失败") from exc
        df = raw.copy()
        # 容错列名映射
        col_map = {
            "公司代码": "code", "code": "code", "证券代码": "code",
            "退市日期": "delist_date", "delist_date": "delist_date",
            "退市原因": "reason", "reason": "reason",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["market"] = "a_share"
        for col in DELISTING_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[DELISTING_COLUMNS]


def load_manual_delisting(market: str, csv_path: Path) -> pd.DataFrame:
    """读取港美股人工补录退市 CSV（spec §3.5：非重点，人工维护）。

    CSV 列：code,delist_date,reason。文件不存在 → 空 DataFrame（降级为标注）。
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return pd.DataFrame(columns=DELISTING_COLUMNS)
    df = pd.read_csv(csv_path, dtype={"code": str})
    df["market"] = market
    for col in DELISTING_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[DELISTING_COLUMNS]
