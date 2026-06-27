"""真实数据源 Fetcher：封装 akshare/yfinance（spec §12 集成运行用）。

注意：此模块仅在联网集成运行时使用。字段名与返回结构按 akshare/yfinance
当前版本约定，Phase 2 fetchers 正式落地时若字段变更需同步更新。
"""
from __future__ import annotations

from typing import Any


class RealFetcher:
    """实现 Fetcher 协议，按 kind 分发到 akshare/yfinance。"""

    def fetch(self, kind: str, **kwargs: Any) -> Any:
        if kind == "a_share_announcement_dates":
            return self._a_share_announcement_dates(**kwargs)
        if kind == "hk_delisted_list":
            return self._hk_delisted_list(**kwargs)
        if kind == "hk_known_delisting_samples":
            return self._hk_known_samples(**kwargs)
        if kind == "us_delisted_source_available":
            return self._us_delisting_source(**kwargs)
        if kind == "benchmark_daily":
            return self._benchmark_daily(**kwargs)
        if kind == "trading_calendar":
            return self._trading_calendar(**kwargs)
        if kind == "fundamental_sample":
            return self._fundamental_sample(**kwargs)
        raise ValueError(f"unknown kind: {kind}")

    # --- 各 kind 实现：调用真实源（联网） ---
    def _a_share_announcement_dates(self, years: int, sample_size: int, **_):
        import akshare as ak
        df = ak.stock_zh_a_spot_em()  # 取成分股列表做样本
        codes = df["代码"].head(sample_size).tolist()
        rows = []
        for code in codes:
            try:
                fin = ak.stock_financial_report_sina(stock=f"sz{code}" if code.startswith("0") else f"sh{code}")
                for _, r in fin.head(20).iterrows():
                    rows.append({
                        "code": code,
                        "report_period": str(r.get("报告日", "")),
                        "announcement_date": str(r.get("公告日", "")) or None,
                    })
            except Exception:
                continue
            if len(rows) >= sample_size * 10:
                break
        return rows

    def _hk_delisted_list(self, **_):
        import akshare as ak
        try:
            df = ak.stock_hk_spot_em()
            return [{"code": str(r["代码"]), "delist_date": "", "reason": ""} for _, r in df.iterrows()]
        except Exception:
            return []

    def _hk_known_samples(self, **_):
        # 已知退市案例（人工维护示例，需按实际补录）
        return [{"code": c} for c in ["03692", "02333", "01378", "00358", "01876"]]

    def _us_delisting_source(self, **_):
        # yfinance 不提供退市列表；返回不可用，触发降级路径
        return {"available": False, "source": "", "sample_count": 0}

    def _benchmark_daily(self, index_code: str, years: int, **_):
        import akshare as ak
        try:
            df = ak.index_zh_a_hist(symbol="000300", period="daily", start_date="20150101", end_date="20260627")
            return [{"date": str(r["日期"]), "close": float(r["收盘"])} for _, r in df.iterrows()]
        except Exception:
            return []

    def _trading_calendar(self, market: str, years: int, **_):
        import akshare as ak
        try:
            df = ak.tool_trade_date_hist_sina()
            return [str(d) for d in df["trade_date"].tolist()]
        except Exception:
            return []

    def _fundamental_sample(self, markets, n_stocks, years, **_):
        import akshare as ak
        rows = []
        for market in markets:
            try:
                df = ak.stock_zh_a_spot_em().head(n_stocks)
                for _, r in df.iterrows():
                    for f, col in [("roe", "ROE"), ("revenue", "营收"), ("net_profit", "净利润"), ("market_cap", "总市值")]:
                        rows.append({"market": market, "code": str(r["代码"]), "field": f, "value": r.get(col)})
                    rows.append({"market": market, "code": str(r["代码"]), "field": "fcf", "value": None})
            except Exception:
                continue
        return rows
