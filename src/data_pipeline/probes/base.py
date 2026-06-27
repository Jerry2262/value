"""探针基类、结果与可注入 Fetcher 协议。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

PASSED = "passed"
FAILED = "failed"
WARNING = "warning"


@dataclass
class ProbeResult:
    status: str                       # "passed" | "failed" | "warning"
    summary: str                      # 人类可读的一句话结论
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == PASSED


@runtime_checkable
class Fetcher(Protocol):
    """探针依赖的数据获取接口。

    各市场具体实现（akshare/yfinance 封装）在 Phase 2 落地；探针阶段用
    mock 实现此协议进行单元测试，集成运行用真实封装。
    """

    def fetch(self, kind: str, **kwargs: Any) -> Any: ...


class Probe:
    """探针基类。子类实现 _compute(fetcher) -> ProbeResult。"""

    name: str = "base"
    description: str = ""

    def run(self, fetcher: Fetcher) -> ProbeResult:
        try:
            return self._compute(fetcher)
        except Exception as exc:  # noqa: BLE001
            return ProbeResult(
                status=WARNING,
                summary=f"探针 {self.name} 执行异常: {exc!r}",
                stats={"exception": repr(exc)},
            )

    def _compute(self, fetcher: Fetcher) -> ProbeResult:  # pragma: no cover
        raise NotImplementedError
