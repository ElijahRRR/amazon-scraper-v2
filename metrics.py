"""
Amazon 产品采集系统 v2 - 滑动窗口指标采集器
实时跟踪请求延迟、成功率、被封率、带宽使用等关键指标
供自适应并发控制器 (adaptive.py) 消费
"""
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import config


@dataclass
class RequestRecord:
    """单次请求的指标记录"""
    timestamp: float          # 完成时间戳
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
        self._lock = threading.Lock()  # 线程安全（asyncio + 可能的多线程日志）

        # 在飞请求计数（原子操作）
        self._inflight = 0

    def record(self, latency_s: float, success: bool, blocked: bool, resp_bytes: int = 0):
        """记录一次请求完成"""
        rec = RequestRecord(
            timestamp=time.time(),
            latency_s=latency_s,
            success=success,
            blocked=blocked,
            resp_bytes=resp_bytes,
        )
        with self._lock:
            self._records.append(rec)
            self._prune()

    def request_start(self):
        """标记一个请求开始（在飞 +1）"""
        self._inflight += 1

    def request_end(self):
        """标记一个请求结束（在飞 -1）"""
        self._inflight = max(0, self._inflight - 1)

    @property
    def inflight(self) -> int:
        """当前在飞请求数"""
        return self._inflight

    def _prune(self):
        """清理过期记录（在 lock 内调用）"""
        cutoff = time.time() - self._window
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
        with self._lock:
            self._prune()
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

        # 成功率 & 被封率
        successes = sum(1 for r in records if r.success)
        blocks = sum(1 for r in records if r.blocked)
        success_rate = successes / total
        block_rate = blocks / total

        # 延迟分位数
        latencies = sorted(r.latency_s for r in records)
        p50 = self._percentile(latencies, 0.50)
        p95 = self._percentile(latencies, 0.95)

        # 带宽：窗口内总字节 / 实际时间跨度
        total_bytes = sum(r.resp_bytes for r in records)
        time_span = records[-1].timestamp - records[0].timestamp if total > 1 else self._window
        time_span = max(time_span, 1.0)  # 避免除零
        bandwidth_bps = total_bytes / time_span

        # 带宽使用率（对比配置上限）
        bandwidth_limit = getattr(config, "PROXY_BANDWIDTH_MBPS", 0) * 1_000_000 / 8  # Mbps → Bytes/s
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
        """计算分位数（已排序数据）"""
        if not sorted_data:
            return 0.0
        idx = int(len(sorted_data) * pct)
        idx = min(idx, len(sorted_data) - 1)
        return sorted_data[idx]

    def format_summary(self) -> str:
        """格式化输出，用于日志"""
        s = self.snapshot()
        bw_display = s["bandwidth_bps"] / 1024  # KB/s
        return (
            f"📊 指标 | 在飞:{s['inflight']} | "
            f"成功率:{s['success_rate']:.0%} | "
            f"封锁率:{s['block_rate']:.0%} | "
            f"p50:{s['latency_p50']:.2f}s p95:{s['latency_p95']:.2f}s | "
            f"带宽:{bw_display:.0f}KB/s ({s['bandwidth_pct']:.0%})"
        )
