"""Fetcher 基础设施：重试装饰器、标准化列名契约、异常类。"""
from __future__ import annotations

import functools
import time
from typing import Callable, TypeVar

T = TypeVar("T")

# 下游 cleaners/store 依赖的标准化列名（不可漂移）
QUOTE_COLUMNS = [
    "date", "code", "market", "open", "high", "low", "close", "volume", "adj_factor",
]
FUNDAMENTAL_COLUMNS = [
    "code", "market", "report_period", "announcement_date_approx",
    "revenue", "net_profit", "roe", "debt_ratio", "fcf", "total_market_cap",
]
FX_COLUMNS = ["date", "base", "quote", "rate"]
BENCHMARK_COLUMNS = ["date", "code", "market", "close"]
DELISTING_COLUMNS = ["code", "market", "delist_date", "reason"]

# 连续失败标记值（spec §3.6：连续3天失败标记 STALE）
FETCHER_MARKET_STALE = "STALE"


class FetcherError(Exception):
    """Fetcher 拉取失败（重试耗尽后抛出）。"""

    def __init__(self, message: str, *, cause: Exception | None = None):
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause


def retry_with_backoff(retries: int = 3, delays: tuple = (1, 3, 9)):
    """重试装饰器：失败按 delays 退避重试，耗尽后抛 FetcherError 包装原异常。

    delays 长度应 >= retries-1；不足时末次退避为 0。
    """
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            last_exc: Exception | None = None
            for attempt in range(retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < retries - 1:
                        delay = delays[attempt] if attempt < len(delays) else 0
                        time.sleep(delay)
            raise FetcherError(
                f"{fn.__name__} 重试 {retries} 次仍失败: {last_exc}",
                cause=last_exc,
            )
        return wrapper
    return decorator
