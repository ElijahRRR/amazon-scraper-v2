"""
Amazon 产品采集系统 v2 - AIMD 自适应并发控制器
类 TCP 拥塞控制：根据实时指标动态调整并发数

算法：
  - Additive Increase: 一切顺利 → 并发 +1
  - Multiplicative Decrease: 出问题 → 并发 ÷ 2
  - 带宽感知: 带宽饱和时停止增长
  - 冷却机制: 被封后 30s 内不加速
"""
import asyncio
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
    - acquire() / release(): 获取/释放并发槽位（替代固定 Semaphore）
    - record_result(): 记录请求结果（喂给 MetricsCollector）
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
        self._concurrency = initial or getattr(config, "INITIAL_CONCURRENCY", 5)
        self._min = min_c or getattr(config, "MIN_CONCURRENCY", 2)
        self._max = max_c or getattr(config, "MAX_CONCURRENCY", 50)
        
        # 确保初始值在合法范围
        self._concurrency = max(self._min, min(self._max, self._concurrency))
        
        # 动态信号量：用 asyncio.Semaphore 实现，但定期重建以调整大小
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._sem_value = self._concurrency  # 跟踪信号量初始值
        
        # 指标采集器
        self.metrics = metrics or MetricsCollector()
        
        # 冷却状态
        self._cooldown_until: float = 0.0
        
        # 运行控制
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        # 调节参数（从 config 读取）
        self._adjust_interval = getattr(config, "ADJUST_INTERVAL_S", 10)
        self._target_latency = getattr(config, "TARGET_LATENCY_S", 2.0)
        self._max_latency = getattr(config, "MAX_LATENCY_S", 4.0)
        self._target_success = getattr(config, "TARGET_SUCCESS_RATE", 0.95)
        self._min_success = getattr(config, "MIN_SUCCESS_RATE", 0.85)
        self._block_threshold = getattr(config, "BLOCK_RATE_THRESHOLD", 0.05)
        self._bw_soft_cap = getattr(config, "BANDWIDTH_SOFT_CAP", 0.80)
        self._cooldown_duration = getattr(config, "COOLDOWN_AFTER_BLOCK_S", 30)
    
    @property
    def current_concurrency(self) -> int:
        """当前目标并发数"""
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
        logger.info(f"🎛️  自适应控制器启动 | 初始并发={self._concurrency} | 范围=[{self._min}, {self._max}]")
    
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
                self._evaluate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"自适应控制器异常: {e}")
    
    def _evaluate(self):
        """
        核心评估逻辑 — AIMD + 带宽感知
        
        优先级：
        1. 被封率高 → 紧急减半 + 冷却
        2. 成功率低 OR 延迟高 → 减半
        3. 带宽饱和 → 不动
        4. 一切正常 → +1
        5. 其他 → 不动
        """
        snap = self.metrics.snapshot()
        
        # 样本不足，不调整（至少需要 5 个样本点）
        if snap["total"] < 5:
            logger.debug(f"🎛️  样本不足 ({snap['total']}), 跳过调整")
            return
        
        old_c = self._concurrency
        now = time.time()
        in_cooldown = now < self._cooldown_until
        reason = ""
        
        # ① 被封率过高 → 紧急减半 + 冷却
        if snap["block_rate"] > self._block_threshold:
            self._concurrency = max(self._min, self._concurrency // 2)
            self._cooldown_until = now + self._cooldown_duration
            reason = f"🚨 封锁率 {snap['block_rate']:.0%} > {self._block_threshold:.0%} → 减半+冷却{self._cooldown_duration}s"
        
        # ② 成功率低 OR 延迟过高 → 减半
        elif snap["success_rate"] < self._min_success or snap["latency_p50"] > self._max_latency:
            self._concurrency = max(self._min, self._concurrency // 2)
            reason = (
                f"⚠️ 成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s → 减半"
            )
        
        # ③ 带宽饱和 → 不增
        elif snap["bandwidth_pct"] > self._bw_soft_cap:
            reason = f"📶 带宽 {snap['bandwidth_pct']:.0%} > {self._bw_soft_cap:.0%} → 维持"
        
        # ④ 冷却期 → 不增
        elif in_cooldown:
            remaining = int(self._cooldown_until - now)
            reason = f"❄️ 冷却中 (剩余 {remaining}s) → 维持"
        
        # ⑤ 一切正常 → +1
        elif (snap["success_rate"] >= self._target_success 
              and snap["latency_p50"] < self._target_latency):
            self._concurrency = min(self._max, self._concurrency + 1)
            reason = f"✅ 成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s → +1"
        
        # ⑥ 中间地带 → 不动
        else:
            reason = f"➖ 稳态 | 成功率={snap['success_rate']:.0%} p50={snap['latency_p50']:.2f}s"
        
        # 调整信号量（如果并发数变化了）
        if self._concurrency != old_c:
            self._adjust_semaphore(old_c, self._concurrency)
            logger.info(f"🎛️  并发调整 {old_c} → {self._concurrency} | {reason}")
        else:
            logger.debug(f"🎛️  {reason} | 并发={self._concurrency}")
        
        # 打印指标摘要
        logger.info(self.metrics.format_summary())
    
    def _adjust_semaphore(self, old_value: int, new_value: int):
        """
        动态调整信号量大小
        
        增加 → 释放额外的 permit
        减少 → 设置新的更小信号量（已持有的 permit 会自然归还旧的，
              但新请求会用新信号量。这里用渐进方式：减少时不强制
              踢掉在飞的请求，而是在释放时对比决定。）
        
        简化实现：直接替换信号量。增加时多 release 差值；减少时
        新建信号量（在飞的请求 release 旧的不会有问题，因为
        acquire/release 在 worker 协程内配对使用）。
        """
        diff = new_value - old_value
        if diff > 0:
            # 扩容：给当前信号量多 release 几个 permit
            for _ in range(diff):
                self._semaphore.release()
        elif diff < 0:
            # 缩容：创建新的更小信号量
            # 当前在飞的请求会继续使用旧的 release（不影响）
            # 后续新的 acquire 使用新的信号量
            # 
            # 但为了简化，我们用单一信号量 + 手动 acquire 来减少 permit
            # 即：acquire 掉多余的 permit，这些 permit 不会被 release 回来
            # 注意：这是非阻塞的尝试，如果 acquire 不到说明都在飞，
            # 等它们落地时自然就少了
            for _ in range(-diff):
                # 非阻塞 acquire：如果拿到了 permit，就不还了（相当于减少总量）
                # 拿不到说明都在用，等自然减少
                if self._semaphore._value > 0:
                    self._semaphore._value -= 1
