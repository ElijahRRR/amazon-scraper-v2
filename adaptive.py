"""
Amazon 产品采集系统 v2 - AIMD 自适应并发控制器
类 TCP 拥塞控制：根据实时指标动态调整并发数

算法：
  - Additive Increase: 一切顺利 → 并发 +2（TPS）或 +1（隧道）
  - Multiplicative Decrease: 出问题 → 并发 × 0.7（TPS，减 30%）或 × 0.75（隧道，减 25%）
  - 带宽感知: 带宽饱和时停止增长
  - 冷却机制: 减速后进入冷却期，防止连续多次减速形成雪崩

双模式差异：
  - TPS: 被封时 session 轮换即可，温和减速保留管道容量（高并发 + 令牌桶 QPS 上限）
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
        self._drain_task: Optional[asyncio.Task] = None  # 缩容时后台排空任务
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
            # TPS 模式：封锁时 session 轮换即可，温和减速保留管道容量
            self._cooldown_duration = 15         # 轮换本身 7-15s，冷却不必再加 30s
            self._block_decrease_factor = 0.7    # ×0.7（减 30%），避免管道饿死

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
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
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
        1. 冷却期 → 不动（最高优先级，防止减速雪崩）
        2. 被封率高 → 减速 + 冷却
        3. 成功率低 → 减速 + 冷却
        4. 延迟高 → TPS 模式减速；隧道模式仅维持（高延迟 = 带宽共享，不是故障）
        5. 带宽饱和 → 不动
        6. 一切正常 → +2
        7. 其他 → 不动

        隧道模式延迟策略：
        高并发共享带宽管道时，per-request 延迟 = C × pageSize / bandwidth，
        延迟升高是并发增加的自然结果，不代表代理过载或被封。
        只在超时导致 success_rate 下降时才减速（由条件 3 覆盖）。
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

            # 冷却期：最高优先级，防止减速雪崩（28→19→13→9 这种连续×0.7）
            if in_cooldown:
                new_c = self._concurrency
                remaining = int(self._cooldown_until - now)
                reason = f"冷却中 (剩余 {remaining}s) -> 维持"

            elif snap["block_rate"] > self._block_threshold:
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                self._cooldown_until = now + self._cooldown_duration
                action = f"×{self._block_decrease_factor}" if self._block_decrease_factor != 0.5 else "÷2"
                reason = f"封锁率 {snap['block_rate']:.0%} > {self._block_threshold:.0%} -> {action}+冷却{self._cooldown_duration}s"

            elif snap["success_rate"] < self._min_success:
                # 成功率不达标 → 减速（两种模式都适用）
                new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                soft_cooldown = max(15, self._cooldown_duration // 2)
                self._cooldown_until = now + soft_cooldown
                action = f"×{self._block_decrease_factor}" if self._block_decrease_factor != 0.5 else "÷2"
                reason = f"成功率={snap['success_rate']:.0%} < {self._min_success:.0%} -> {action}+冷却{soft_cooldown}s"

            elif snap["latency_p50"] > self._max_latency:
                if self._proxy_mode == "tunnel":
                    # 隧道模式：高延迟 = 带宽共享导致的正常现象，不减速
                    # 如果延迟过高导致超时，success_rate 会自然下降，由上一条件捕获
                    new_c = self._concurrency
                    reason = f"隧道带宽延迟 p50={snap['latency_p50']:.2f}s > {self._max_latency:.0f}s -> 维持(非故障)"
                else:
                    # TPS 模式：高延迟说明代理拥塞 → 减速
                    new_c = max(self._min, int(self._concurrency * self._block_decrease_factor))
                    soft_cooldown = max(15, self._cooldown_duration // 2)
                    self._cooldown_until = now + soft_cooldown
                    action = f"×{self._block_decrease_factor}" if self._block_decrease_factor != 0.5 else "÷2"
                    reason = f"延迟 p50={snap['latency_p50']:.2f}s > {self._max_latency:.0f}s -> {action}+冷却{soft_cooldown}s"

            elif snap["bandwidth_pct"] > self._bw_soft_cap:
                new_c = self._concurrency
                reason = f"带宽 {snap['bandwidth_pct']:.0%} > {self._bw_soft_cap:.0%} -> 维持"

            elif (snap["success_rate"] >= self._target_success
                  and snap["latency_p50"] < self._target_latency):
                # 随机抖动恢复：不同 Worker 以不同概率加速，防止同步振荡
                if random.random() < (0.3 + 0.7 * self._recovery_jitter):
                    increment = 2
                    new_c = min(self._max, self._concurrency + increment)
                    reason = f"成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s -> +{increment}"
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
        减少：后台排空多余的 permit，渐进式缩容

        排空策略：不重建信号量，而是从现有信号量中 acquire 多余的 permit。
        这样旧的在飞请求 release 时归还给同一个信号量，不会造成 permit 膨胀。
        排空是异步的 — 每获取一个 permit 都等待一个在飞请求完成后归还。
        """
        # 先取消之前可能正在进行的排空任务
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
            try:
                await self._drain_task
            except (asyncio.CancelledError, Exception):
                pass
            self._drain_task = None

        diff = new_value - old_value
        if diff > 0:
            for _ in range(diff):
                self._semaphore.release()
        elif diff < 0:
            # 后台排空：从信号量中 acquire 多余的 permit，渐进式缩容
            self._drain_task = asyncio.create_task(self._drain_permits(abs(diff)))

    async def _drain_permits(self, count: int):
        """后台排空 permit：每获取一个 permit 就从流通中移除一个槽位"""
        drained = 0
        try:
            for _ in range(count):
                await self._semaphore.acquire()
                drained += 1
        except asyncio.CancelledError:
            # 被取消时，归还已排空的 permit（可能因为又要扩容了）
            for _ in range(drained):
                self._semaphore.release()


class TokenBucket:
    """
    令牌桶限流器

    控制请求发起速率（QPS），与 Semaphore（并发连接数）互补：
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


class ChannelRateLimiter:
    """
    Per-channel 令牌桶管理器（DPS 隧道模式专用）

    每个 channel 独立限流，替代全局 TokenBucket。
    DPS 独享 IP，各 channel 的 Session/Cookie 独立，无需全局限速。
    每 channel 默认 3 QPS（可配置），总 QPS = channels × per_channel_qps。
    """

    def __init__(self, channels: int = None, per_channel_rate: float = None):
        self._channels = channels or config.TUNNEL_CHANNELS
        self._per_channel_rate = per_channel_rate or getattr(
            config, "PER_CHANNEL_QPS", 3.0
        )
        self._buckets: dict[int, TokenBucket] = {}
        for ch_id in range(1, self._channels + 1):
            self._buckets[ch_id] = TokenBucket(
                rate=self._per_channel_rate,
                burst=max(2, int(self._per_channel_rate)),
            )
        logger.info(
            f"Per-channel 限流器初始化: {self._channels} channels × "
            f"{self._per_channel_rate} QPS = {self._channels * self._per_channel_rate:.1f} 总 QPS"
        )

    async def acquire(self, channel_id: int = None):
        """获取指定 channel 的令牌，channel_id 为 None 时不限流"""
        if channel_id is None or channel_id not in self._buckets:
            return
        await self._buckets[channel_id].acquire()

    def resize(self, channels: int):
        """运行时调整 channel 数量"""
        for ch_id in range(1, channels + 1):
            if ch_id not in self._buckets:
                self._buckets[ch_id] = TokenBucket(
                    rate=self._per_channel_rate,
                    burst=max(2, int(self._per_channel_rate)),
                )
        to_remove = [k for k in self._buckets if k > channels]
        for k in to_remove:
            del self._buckets[k]
        self._channels = channels

    @property
    def per_channel_rate(self) -> float:
        return self._per_channel_rate

    @per_channel_rate.setter
    def per_channel_rate(self, value: float):
        """动态调整每 channel 的 QPS"""
        self._per_channel_rate = max(0.5, value)
        for bucket in self._buckets.values():
            bucket.rate = self._per_channel_rate
