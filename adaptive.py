"""
Amazon 产品采集系统 v2 - AIMD 自适应并发控制器
类 TCP 拥塞控制：根据实时指标动态调整并发数

算法：
  - Additive Increase: 一切顺利 → 并发 +1
  - Multiplicative Decrease: 出问题 → 并发 ÷ 2（TPS）或 × 0.75（隧道）
  - 带宽感知: 带宽饱和时停止增长
  - 冷却机制: 被封后 30s（TPS）或 60s（隧道）内不加速

双模式差异：
  - TPS: 被封影响全局（所有 worker 暂停），需要激进减半
  - 隧道: 被封仅影响 1/8 通道，响应更温和
"""
import asyncio
import random
import time
import logging
from typing import Optional

import config
from metrics import MetricsCollector

logger = logging.getLogger(__name__)


class AdaptiveController:
    """
    自适应并发控制器

    核心接口：
    - current_concurrency: 当前允许的最大并发数
    - acquire() / release(): 获取/释放并发槽位
    - record_result(): 记录请求结果
    - start(): 启动后台评估协程
    - stop(): 停止控制器
    """

    def __init__(
        self,
        initial: int = None,
        min_c: int = None,
        max_c: int = None,
        metrics: MetricsCollector = None,
    ):
        self._concurrency = initial or config.INITIAL_CONCURRENCY
        self._min = min_c or config.MIN_CONCURRENCY
        self._max = max_c or config.MAX_CONCURRENCY

        # 确保初始值在合法范围
        self._concurrency = max(self._min, min(self._max, self._concurrency))

        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._adjust_lock = asyncio.Lock()  # 保护并发调整的原子性

        self.metrics = metrics or MetricsCollector()

        self._cooldown_until: float = 0.0

        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._adjust_interval = config.ADJUST_INTERVAL_S
        self._target_latency = config.TARGET_LATENCY_S
        self._max_latency = config.MAX_LATENCY_S
        self._target_success = config.TARGET_SUCCESS_RATE
        self._min_success = config.MIN_SUCCESS_RATE
        self._block_threshold = config.BLOCK_RATE_THRESHOLD
        self._bw_soft_cap = config.BANDWIDTH_SOFT_CAP

        # 代理模式感知的参数
        self._proxy_mode = config.PROXY_MODE
        if self._proxy_mode == "tunnel":
            # 隧道模式：封锁影响范围小（1/8），响应更温和
            self._cooldown_duration = 60     # 匹配 IP 轮换周期
            self._block_decrease_factor = 0.75  # ×0.75（减 25%）
        else:
            # TPS 模式：封锁影响全局，激进减半
            self._cooldown_duration = config.COOLDOWN_AFTER_BLOCK_S
            self._block_decrease_factor = 0.5   # ÷2（减半）

        # 恢复抖动系数（由 Worker 从 Server 同步设置，防止多 Worker 同步振荡）
        self._recovery_jitter = 0.5

    @property
    def current_concurrency(self) -> int:
        return self._concurrency

    async def acquire(self):
        """获取一个并发槽位（阻塞直到有空位）"""
        await self._semaphore.acquire()
        self.metrics.request_start()

    def release(self):
        """释放一个并发槽位"""
        self.metrics.request_end()
        self._semaphore.release()

    def record_result(self, latency_s: float, success: bool, blocked: bool, resp_bytes: int = 0):
        """记录一次请求结果"""
        self.metrics.record(latency_s, success, blocked, resp_bytes)

    async def start(self):
        """启动后台评估协程"""
        self._running = True
        self._task = asyncio.create_task(self._adjust_loop())
        logger.info(f"自适应控制器启动 | 初始并发={self._concurrency} | 范围=[{self._min}, {self._max}]")

    async def stop(self):
        """停止控制器"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _adjust_loop(self):
        """后台循环：每 ADJUST_INTERVAL_S 秒评估一次"""
        while self._running:
            try:
                await asyncio.sleep(self._adjust_interval)
                if not self._running:
                    break
                await self._evaluate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"自适应控制器异常: {e}")

    async def _evaluate(self):
        """
        核心评估逻辑 — AIMD + 带宽感知

        优先级：
        1. 被封率高 → 紧急减半 + 冷却
        2. 成功率低 OR 延迟高 → 减半
        3. 带宽饱和 → 不动
        4. 冷却期 → 不动
        5. 一切正常 → +1
        6. 其他 → 不动
        """
        snap = self.metrics.snapshot()

        if snap["total"] < 5:
            logger.debug(f"样本不足 ({snap['total']}), 跳过调整")
            return

        async with self._adjust_lock:
            old_c = self._concurrency
            now = time.monotonic()
            in_cooldown = now < self._cooldown_until
            reason = ""

            if snap["block_rate"] > self._block_threshold:
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                self._cooldown_until = now + self._cooldown_duration
                action = f"×{self._block_decrease_factor}" if self._block_decrease_factor != 0.5 else "÷2"
                reason = f"封锁率 {snap['block_rate']:.0%} > {self._block_threshold:.0%} -> {action}+冷却{self._cooldown_duration}s"

            elif snap["success_rate"] < self._min_success or snap["latency_p50"] > self._max_latency:
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                action = f"×{self._block_decrease_factor}" if self._block_decrease_factor != 0.5 else "÷2"
                reason = f"成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s -> {action}"

            elif snap["bandwidth_pct"] > self._bw_soft_cap:
                new_c = self._concurrency
                reason = f"带宽 {snap['bandwidth_pct']:.0%} > {self._bw_soft_cap:.0%} -> 维持"

            elif in_cooldown:
                new_c = self._concurrency
                remaining = int(self._cooldown_until - now)
                reason = f"冷却中 (剩余 {remaining}s) -> 维持"

            elif (snap["success_rate"] >= self._target_success
                  and snap["latency_p50"] < self._target_latency):
                # 随机抖动恢复：不同 Worker 以不同概率加速，防止同步振荡
                # jitter=0.9 → 93% 概率 +1，jitter=0.1 → 37% 概率 +1
                if random.random() < (0.3 + 0.7 * self._recovery_jitter):
                    new_c = min(self._max, self._concurrency + 1)
                    reason = f"成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s -> +1"
                else:
                    new_c = self._concurrency
                    reason = f"成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s -> 维持(抖动跳过)"

            else:
                new_c = self._concurrency
                reason = f"稳态 | 成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s"

            if new_c != old_c:
                await self._resize_semaphore(old_c, new_c)
                self._concurrency = new_c
                logger.info(f"并发调整 {old_c} -> {new_c} | {reason}")
            else:
                logger.debug(f"{reason} | 并发={self._concurrency}")

        logger.info(self.metrics.format_summary())

    async def _resize_semaphore(self, old_value: int, new_value: int):
        """
        安全地调整信号量大小

        增加：多次 release 来增加可用 permit
        减少：重建一个更小的 Semaphore

        注意：减少时不强制中断在飞的请求，而是等自然归还后
        新请求使用新的更小信号量，实现渐进式缩容。
        在飞的请求持有的 permit 归还给旧 semaphore，不影响新的。
        """
        diff = new_value - old_value
        if diff > 0:
            for _ in range(diff):
                self._semaphore.release()
        elif diff < 0:
            # 重建信号量，初始值 = 新目标值
            # 当前已在飞的请求会继续使用旧 semaphore 的 release 路径
            # 这里直接替换，新 acquire 全部走新 semaphore
            # 为平滑过渡：新 semaphore 初始值 = max(new_value, 0)，不能为负
            self._semaphore = asyncio.Semaphore(max(new_value, 1))


class TokenBucket:
    """
    全局令牌桶限流器

    控制系统级请求发起速率（QPS），与 Semaphore（并发连接数）互补：
    - Semaphore 控制同时在飞的请求数
    - TokenBucket 控制新请求的产生速率
    """

    def __init__(self, rate: float = None, burst: int = None):
        self._rate = rate or config.TOKEN_BUCKET_RATE
        self._burst = burst or max(1, int(self._rate))
        self._tokens = float(self._burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """获取一个令牌，不够时等待"""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate

            await asyncio.sleep(wait)

    def _refill(self):
        """补充令牌"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)

    @property
    def rate(self) -> float:
        return self._rate

    @rate.setter
    def rate(self, value: float):
        """动态调整速率"""
        self._rate = max(0.1, value)
        self._burst = max(1, int(self._rate))
