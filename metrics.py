"""
Amazon 产品采集系统 v2 - 滑动窗口指标采集器
实时跟踪请求延迟、成功率、被封率、带宽使用等关键指标
供自适应并发控制器 (adaptive.py) 消费
"""
import asyncio
import time
from collections import deque
from dataclasses import dataclass

import config


@dataclass
class RequestRecord:
    """单次请求的指标记录"""
    timestamp: float          # 完成时间戳（monotonic）
    latency_s: float          # 请求耗时（秒）
    success: bool             # 是否成功
    blocked: bool             # 是否被封（403/503/验证码）
    resp_bytes: int           # 响应体大小（字节）


class MetricsCollector:
    """
    滑动窗口指标采集器

    保留最近 window_seconds 秒内的请求记录，实时计算：
    - 请求延迟 p50 / p95
    - 成功率 (success / total)
    - 被封率 (blocked / total)
    - 带宽使用率 (bytes/s vs 上限)
    - 当前在飞请求数
    """

    def __init__(self, window_seconds: float = 30.0):
        self._window = window_seconds
        self._records: deque[RequestRecord] = deque()
        self._lock = asyncio.Lock()
        self._inflight = 0

    def record(self, latency_s: float, success: bool, blocked: bool, resp_bytes: int = 0):
        """记录一次请求完成（同步，可在协程外调用）"""
        rec = RequestRecord(
            timestamp=time.monotonic(),
            latency_s=latency_s,
            success=success,
            blocked=blocked,
            resp_bytes=resp_bytes,
        )
        # deque 操作在 CPython 中是线程安全的，直接 append 即可
        self._records.append(rec)
        self._prune_sync()

    def request_start(self):
        """标记一个请求开始（在飞 +1）"""
        self._inflight += 1

    def request_end(self):
        """标记一个请求结束（在飞 -1）"""
        self._inflight = max(0, self._inflight - 1)

    @property
    def inflight(self) -> int:
        return self._inflight

    def _prune_sync(self):
        """清理过期记录（无锁，依赖 CPython deque 原子性）"""
        cutoff = time.monotonic() - self._window
        while self._records and self._records[0].timestamp < cutoff:
            self._records.popleft()

    def snapshot(self) -> dict:
        """
        获取当前窗口内的汇总指标

        返回:
            {
                "total": int,
                "success_rate": float,     # 0.0 ~ 1.0
                "block_rate": float,       # 0.0 ~ 1.0
                "latency_p50": float,      # 秒
                "latency_p95": float,      # 秒
                "bandwidth_bps": float,    # bytes/s
                "bandwidth_pct": float,    # 带宽使用率 0.0 ~ 1.0
                "inflight": int,
                "window_seconds": float,
            }
        """
        self._prune_sync()
        records = list(self._records)

        total = len(records)
        if total == 0:
            return {
                "total": 0,
                "success_rate": 1.0,
                "block_rate": 0.0,
                "latency_p50": 0.0,
                "latency_p95": 0.0,
                "bandwidth_bps": 0.0,
                "bandwidth_pct": 0.0,
                "inflight": self._inflight,
                "window_seconds": self._window,
            }

        successes = sum(1 for r in records if r.success)
        blocks = sum(1 for r in records if r.blocked)
        success_rate = successes / total
        block_rate = blocks / total

        latencies = sorted(r.latency_s for r in records)
        p50 = self._percentile(latencies, 0.50)
        p95 = self._percentile(latencies, 0.95)

        total_bytes = sum(r.resp_bytes for r in records)
        time_span = records[-1].timestamp - records[0].timestamp if total > 1 else self._window
        time_span = max(time_span, 1.0)
        bandwidth_bps = total_bytes / time_span

        bandwidth_limit = config.PROXY_BANDWIDTH_MBPS * 1_000_000 / 8  # Mbps → Bytes/s
        bandwidth_pct = (bandwidth_bps / bandwidth_limit) if bandwidth_limit > 0 else 0.0

        return {
            "total": total,
            "success_rate": success_rate,
            "block_rate": block_rate,
            "latency_p50": p50,
            "latency_p95": p95,
            "bandwidth_bps": bandwidth_bps,
            "bandwidth_pct": bandwidth_pct,
            "inflight": self._inflight,
            "window_seconds": self._window,
        }

    @staticmethod
    def _percentile(sorted_data: list, pct: float) -> float:
        """线性插值百分位数计算"""
        if not sorted_data:
            return 0.0
        n = len(sorted_data)
        if n == 1:
            return sorted_data[0]
        # 线性插值：index = pct * (n-1)
        idx_f = pct * (n - 1)
        lo = int(idx_f)
        hi = min(lo + 1, n - 1)
        frac = idx_f - lo
        return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])

    def format_summary(self) -> str:
        """格式化输出，用于日志"""
        s = self.snapshot()
        bw_display = s["bandwidth_bps"] / 1024  # KB/s
        return (
            f"指标 | 在飞:{s['inflight']} | "
            f"成功率:{s['success_rate']:.0%} | "
            f"封锁率:{s['block_rate']:.0%} | "
            f"p50:{s['latency_p50']:.2f}s p95:{s['latency_p95']:.2f}s | "
            f"带宽:{bw_display:.0f}KB/s ({s['bandwidth_pct']:.0%})"
        )
