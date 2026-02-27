"""
Amazon 产品采集系统 v2 - Worker 采集引擎（流水线 + 自适应并发）

架构：
  task_feeder  → [task_queue] → worker_pool (N个独立协程) → [result_queue] → batch_submitter
  
  adaptive_controller 实时调整 N 的大小

连接中央服务器 API 拉取任务、推送结果
启动方式：python worker.py --server http://x.x.x.x:8899
"""
import asyncio
import argparse
import logging
import time
import uuid
import signal
import sys
from typing import Optional, Dict, List

import httpx

import config
from proxy import get_proxy_manager
from session import AmazonSession, SessionPool
from parser import AmazonParser
from metrics import MetricsCollector
from adaptive import AdaptiveController, TokenBucket

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class Worker:
    """流水线异步采集 Worker"""

    def __init__(self, server_url: str, worker_id: str = None, concurrency: int = None,
                 zip_code: str = None, fast_mode: bool = False):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self.fast_mode = fast_mode  # 快速模式: AOD 优先获取价格

        # 代理模式
        self._proxy_mode = config.PROXY_MODE

        # 组件
        self.proxy_manager = get_proxy_manager()
        self.parser = AmazonParser()
        self._session: Optional[AmazonSession] = None       # TPS 模式
        self._session_pool: Optional[SessionPool] = None    # 隧道模式

        # 速率控制
        self._rate_limiter = TokenBucket()

        # 自适应并发控制
        self._metrics = MetricsCollector()
        max_c = concurrency or config.MAX_CONCURRENCY
        self._controller = AdaptiveController(
            initial=config.INITIAL_CONCURRENCY,
            min_c=config.MIN_CONCURRENCY,
            max_c=max_c,
            metrics=self._metrics,
        )

        # 任务队列（流水线核心）
        self._task_queue: asyncio.Queue = None
        self._queue_size = getattr(config, "TASK_QUEUE_SIZE", 100)
        self._prefetch_threshold = getattr(config, "TASK_PREFETCH_THRESHOLD", 0.5)

        # 统计
        self._stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "start_time": None,
        }

        # 运行控制
        self._running = False

        # 批量提交队列
        self._result_queue: asyncio.Queue = None
        self._batch_submitter_task: Optional[asyncio.Task] = None
        self._batch_size = 10
        self._batch_interval = 2.0  # 秒

        # 实例级运行参数（不污染全局 config）
        self._max_retries = config.MAX_RETRIES

        # Session 轮换控制
        self._success_since_rotate = 0
        self._rotate_every = config.SESSION_ROTATE_EVERY
        self._rotate_lock = asyncio.Lock()
        self._last_rotate_time = 0.0  # 轮换防抖（monotonic）
        self._session_ready = asyncio.Event()  # Session 就绪信号

        # Worker 协程管理
        self._worker_tasks: List[asyncio.Task] = []

        # 截图队列（有界，防止内存无限增长）
        self._screenshot_queue: asyncio.Queue = None
        self._screenshot_concurrency = 3   # 并发截图协程数
        self._browser = None               # 持久化 Playwright 浏览器实例
        self._playwright = None            # Playwright 上下文管理器
        self._browser_lock = asyncio.Lock()  # 浏览器初始化/关闭锁

        # 设置同步
        self._settings_version = 0

        # 全局并发协调
        self._global_block_epoch = 0   # 已处理的全局封锁 epoch
        self._recovery_jitter = 0.5    # Server 分配的恢复抖动系数

    async def start(self):
        """启动 Worker（流水线架构）"""
        logger.info(f"🚀 Worker [{self.worker_id}] 启动（流水线模式）")
        logger.info(f"   服务器: {self.server_url}")
        logger.info(f"   初始并发: {self._controller.current_concurrency}")
        logger.info(f"   并发范围: [{config.MIN_CONCURRENCY}, {self._controller._max}]")
        logger.info(f"   邮编: {self.zip_code}")
        logger.info(f"   快速模式: {'开启 (AOD优先)' if self.fast_mode else '关闭'}")
        logger.info(f"   代理模式: {self._proxy_mode.upper()}"
                     + (f" ({config.TUNNEL_CHANNELS} 通道)" if self._proxy_mode == "tunnel" else ""))

        self._running = True
        self._stats["start_time"] = time.time()

        # 初始化队列
        self._task_queue = asyncio.Queue(maxsize=self._queue_size)
        self._result_queue = asyncio.Queue()
        self._screenshot_queue = asyncio.Queue(maxsize=200)  # 有界队列，防止内存无限增长

        # 启动前先从 Server 拉取设置（代理地址、邮编等），远程 Worker 无需本地配置
        await self._pull_initial_settings()

        # 初始化 session（此时 proxy_api_url 已从 Server 同步）
        await self._init_session()

        # 启动自适应控制器
        await self._controller.start()

        # 启动核心协程（含截图后台 worker）
        try:
            coroutines = [
                self._task_feeder(),         # 1. 持续从 Server 拉任务
                self._worker_pool(),         # 2. 工人池：自适应并发
                self._batch_submitter(),     # 3. 批量回传结果
                self._screenshot_workers(),   # 4. 截图渲染（多协程并发）
                self._settings_sync(),       # 5. 定期同步服务端设置
            ]
            # 隧道模式：添加 IP 轮换监控协程
            if self._proxy_mode == "tunnel":
                coroutines.append(self._ip_rotation_watcher())
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            pass

        await self._cleanup()
        logger.info(f"🛑 Worker [{self.worker_id}] 已停止")
        self._print_stats()

    async def stop(self):
        """停止 Worker"""
        self._running = False
        # 向任务队列放入 None 哨兵，唤醒所有等待的 worker
        for _ in range(self._controller._max):
            try:
                self._task_queue.put_nowait(None)
            except (asyncio.QueueFull, AttributeError):
                break

    # ═══════════════════════════════════════════════
    # 流水线三大组件
    # ═══════════════════════════════════════════════

    async def _task_feeder(self):
        """
        任务补给协程：持续从 Server 拉任务，保持队列不空

        当队列低于阈值时，主动拉取新任务填充。
        如果拉到高优先级任务（priority > 0），立即清空当前队列，
        让 Worker 秒级切换到新批次（旧任务靠超时回收）。
        """
        logger.info("📡 任务补给协程启动")
        empty_streak = 0  # 连续空响应计数

        while self._running:
            try:
                queue_size = self._task_queue.qsize()
                threshold = int(self._queue_size * self._prefetch_threshold)

                if queue_size < threshold:
                    # 拉取量 = 当前并发数的 2 倍（预取），但不超过队列剩余空间
                    fetch_count = min(
                        self._controller.current_concurrency * 2,
                        self._queue_size - queue_size,
                    )
                    fetch_count = max(fetch_count, 5)  # 至少拉 5 个

                    tasks = await self._pull_tasks(count=fetch_count)

                    if tasks:
                        empty_streak = 0

                        # 检测是否有高优先级任务（优先采集）
                        has_priority = any(t.get("priority", 0) > 0 for t in tasks)
                        if has_priority and not self._task_queue.empty():
                            # 收集被清空的旧任务 ID，通知 Server 立即归还
                            dropped_ids = []
                            while not self._task_queue.empty():
                                try:
                                    old_task = self._task_queue.get_nowait()
                                    if old_task and isinstance(old_task, dict):
                                        dropped_ids.append(old_task["id"])
                                except asyncio.QueueEmpty:
                                    break
                            logger.info(f"🚀 检测到优先采集任务，已清空队列中 {len(dropped_ids)} 个旧任务")
                            # 异步通知 Server 归还旧任务（不阻塞补给流程）
                            if dropped_ids:
                                asyncio.create_task(self._release_tasks(dropped_ids))

                        for task in tasks:
                            await self._task_queue.put(task)
                        logger.debug(f"📡 补给 {len(tasks)} 个任务 (队列: {self._task_queue.qsize()})")
                    else:
                        empty_streak += 1
                        # 指数退避：连续空响应时逐渐增加等待
                        wait = min(5 * (2 ** min(empty_streak - 1, 3)), 30)
                        logger.info(f"📭 暂无任务，等待 {wait} 秒... (队列剩余: {queue_size})")
                        await asyncio.sleep(wait)
                else:
                    # 队列充足，短暂休息
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 任务补给异常: {e}")
                await asyncio.sleep(3)

        logger.info("📡 任务补给协程退出")

    async def _worker_pool(self):
        """
        工人池协程：管理动态数量的 worker 协程
        
        每个 worker 独立循环：acquire → 取任务 → 处理 → release → 循环
        """
        logger.info("⚙️ 工人池启动")
        
        # 启动初始 worker 协程，错开启动时间
        initial = self._controller.current_concurrency
        for i in range(initial):
            task = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(task)

        # 监控循环：根据并发变化动态增减 worker
        last_target = initial
        while self._running:
            await asyncio.sleep(2)  # 每 2 秒检查一次
            
            target = self._controller.current_concurrency
            current = len([t for t in self._worker_tasks if not t.done()])
            
            if target > current:
                # 需要更多 worker
                for i in range(target - current):
                    idx = len(self._worker_tasks)
                    task = asyncio.create_task(self._worker_loop(idx))
                    self._worker_tasks.append(task)
                if target != last_target:
                    logger.info(f"⚙️ Worker 扩容: {current} → {target}")
            
            last_target = target
            
            # 清理已完成的 task 引用
            self._worker_tasks = [t for t in self._worker_tasks if not t.done()]

        # 等待所有 worker 完成
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        
        logger.info("⚙️ 工人池退出")

    async def _worker_loop(self, worker_idx: int):
        """
        单个 worker 协程：持续取任务处理
        
        错开启动 → acquire 并发槽 → 取任务 → 处理 → release → 循环
        """
        # 错开启动，分散请求
        initial_c = self._controller.current_concurrency
        if initial_c > 0:
            stagger = worker_idx * (1.0 / initial_c)
            stagger = min(stagger, 2.0)  # 最多错开 2 秒
            if stagger > 0:
                await asyncio.sleep(stagger)

        while self._running:
            try:
                # 1. 获取并发槽位（自适应控制器管控）
                await self._controller.acquire()
                
                try:
                    # 2. 从队列取任务（最多等 5 秒）
                    try:
                        task = await asyncio.wait_for(
                            self._task_queue.get(), timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        # 队列暂时为空，释放槽位后继续等
                        continue
                    
                    # 3. 哨兵值 → 退出
                    if task is None:
                        break
                    
                    # 4. 处理任务（带计时）
                    start_time = time.time()
                    success, blocked, resp_bytes = await self._process_task(task)
                    elapsed = time.time() - start_time
                    
                    # 5. 记录指标
                    self._controller.record_result(
                        latency_s=elapsed,
                        success=success,
                        blocked=blocked,
                        resp_bytes=resp_bytes,
                    )
                finally:
                    # 6. 释放并发槽位（保证 release）
                    self._controller.release()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker-{worker_idx} 异常: {e}")
                await asyncio.sleep(1)

    # ═══════════════════════════════════════════════
    # 核心处理逻辑（保持不变）
    # ═══════════════════════════════════════════════

    async def _pull_initial_settings(self):
        """启动时从 Server 拉取一次设置，确保所有运行参数与 Server 一致。
        远程 Worker 无需本地 .env 或环境变量，所有配置由 Server 统一下发。"""
        logger.info("⚙️ 从服务器拉取初始设置...")
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(f"{self.server_url}/api/settings")
                if resp.status_code != 200:
                    logger.warning(f"⚠️ 拉取初始设置失败: HTTP {resp.status_code}")
                    return
                s = resp.json()

            changes = []

            # 代理模式（最关键：决定 Worker 的运行方式）
            new_mode = s.get("proxy_mode")
            if new_mode and new_mode in ("tps", "tunnel") and new_mode != self._proxy_mode:
                self._proxy_mode = new_mode
                config.PROXY_MODE = new_mode  # noqa
                changes.append(f"proxy_mode={new_mode}")

            # 隧道配置（远程 Worker 本地凭证为空，必须从 Server 获取）
            for cfg_key, cfg_attr in [
                ("tunnel_host", "TUNNEL_HOST"),
                ("tunnel_user", "TUNNEL_USER"),
                ("tunnel_pass", "TUNNEL_PASS"),
            ]:
                val = s.get(cfg_key)
                if val and val != getattr(config, cfg_attr, ""):
                    setattr(config, cfg_attr, val)
                    changes.append(cfg_key)
            tunnel_port = s.get("tunnel_port")
            if tunnel_port and tunnel_port != config.TUNNEL_PORT:
                config.TUNNEL_PORT = tunnel_port
                changes.append(f"tunnel_port={tunnel_port}")
            tunnel_channels = s.get("tunnel_channels")
            if tunnel_channels and tunnel_channels != config.TUNNEL_CHANNELS:
                config.TUNNEL_CHANNELS = tunnel_channels
                changes.append(f"tunnel_channels={tunnel_channels}")
            tunnel_rotate = s.get("tunnel_rotate_interval")
            if tunnel_rotate and tunnel_rotate != config.TUNNEL_ROTATE_INTERVAL:
                config.TUNNEL_ROTATE_INTERVAL = tunnel_rotate
                changes.append(f"tunnel_rotate={tunnel_rotate}")

            # 代理 API 地址（远程 Worker 本地凭证为空，必须从 Server 获取）
            new_proxy_url = s.get("proxy_api_url")
            if new_proxy_url and new_proxy_url != config.PROXY_API_URL_AUTH:
                config.PROXY_API_URL_AUTH = new_proxy_url  # noqa
                changes.append("proxy_api_url")

            # 邮编（命令行未指定时，用 Server 端设置覆盖）
            new_zip = s.get("zip_code")
            if new_zip and self.zip_code == config.DEFAULT_ZIP_CODE and new_zip != self.zip_code:
                self.zip_code = new_zip
                changes.append(f"zip_code={new_zip}")

            # 令牌桶 QPS
            new_rate = s.get("token_bucket_rate")
            if new_rate and new_rate != self._rate_limiter.rate:
                self._rate_limiter.rate = new_rate
                changes.append(f"QPS={new_rate}")

            # 并发控制：min / max / initial（顺序：先设范围，再设初始值）
            new_min = s.get("min_concurrency")
            if new_min and new_min != self._controller._min:
                self._controller._min = new_min
                changes.append(f"min_c={new_min}")

            new_max = s.get("max_concurrency")
            if new_max and new_max != self._controller._max:
                self._controller._max = new_max
                changes.append(f"max_c={new_max}")

            new_initial = s.get("initial_concurrency")
            if new_initial and new_initial != self._controller._concurrency:
                # 确保在合法范围内
                clamped = max(self._controller._min, min(self._controller._max, new_initial))
                self._controller._concurrency = clamped
                # 启动前重建信号量，使其与新并发值匹配
                self._controller._semaphore = asyncio.Semaphore(clamped)
                changes.append(f"initial_c={clamped}")

            # 最大重试
            new_retries = s.get("max_retries")
            if new_retries and new_retries != self._max_retries:
                self._max_retries = new_retries
                changes.append(f"retries={new_retries}")

            # Session 轮换
            new_rotate = s.get("session_rotate_every")
            if new_rotate and new_rotate != self._rotate_every:
                self._rotate_every = new_rotate
                changes.append(f"rotate={new_rotate}")

            # 截图并发
            new_sc = s.get("screenshot_concurrency")
            if new_sc and new_sc != self._screenshot_concurrency:
                self._screenshot_concurrency = new_sc
                changes.append(f"screenshot_c={new_sc}")

            # AIMD 调控参数
            for attr, key in [
                ("_adjust_interval", "adjust_interval"),
                ("_target_latency", "target_latency"),
                ("_max_latency", "max_latency"),
                ("_target_success", "target_success_rate"),
                ("_min_success", "min_success_rate"),
                ("_block_threshold", "block_rate_threshold"),
                ("_cooldown_duration", "cooldown_after_block"),
            ]:
                val = s.get(key)
                if val is not None and val != getattr(self._controller, attr, None):
                    setattr(self._controller, attr, val)
                    changes.append(f"{key}={val}")

            self._settings_version = s.get("_version", 0)

            if changes:
                logger.info(f"⚙️ 初始设置已同步: {', '.join(changes)}")
            else:
                logger.info("⚙️ 初始设置已确认（与本地一致）")

        except Exception as e:
            logger.warning(f"⚠️ 拉取初始设置异常（将使用本地配置）: {e}")

    async def _init_session(self):
        """初始化 Amazon session（失败时重试，确保 _session_ready 最终被 set）"""
        if self._proxy_mode == "tunnel":
            await self._init_session_tunnel()
        else:
            await self._init_session_tps()

    async def _init_session_tps(self):
        """TPS 模式：初始化单个全局 Session"""
        logger.info("🔧 初始化 Amazon session (TPS)...")
        self._session_ready.clear()
        for attempt in range(3):
            self._session = AmazonSession(self.proxy_manager, self.zip_code)
            success = await self._session.initialize()
            self._success_since_rotate = 0
            if success:
                self._session_ready.set()
                return
            # 初始化失败，等待后重试
            logger.warning(f"⚠️ Session 初始化失败 (尝试 {attempt+1}/3)")
            if self._session:
                await self._session.close()
            self._session = None
            if attempt < 2:
                await asyncio.sleep(5)
        # 3 次全部失败，仍然 set event 让 worker 走正常的重试/失败流程
        logger.error("❌ Session 初始化 3 次全部失败，Worker 将在处理任务时继续重试")
        self._session_ready.set()

    async def _init_session_tunnel(self):
        """隧道模式：初始化 SessionPool，预热前几个通道"""
        logger.info(f"🔧 初始化 SessionPool (隧道, {config.TUNNEL_CHANNELS} 通道)...")
        self._session_pool = SessionPool(self.proxy_manager, self.zip_code)
        # 预热前 2 个通道（至少 2 个才能跑满 5Mbps 总带宽）
        warmup_count = min(2, config.TUNNEL_CHANNELS)
        warmup_ok = 0
        for ch_id in range(1, warmup_count + 1):
            session = await self._session_pool.get_session(ch_id)
            if session and session.is_ready():
                warmup_ok += 1
        if warmup_ok > 0:
            logger.info(f"✅ SessionPool 预热完成: {warmup_ok}/{warmup_count} 通道就绪")
        else:
            logger.error("❌ SessionPool 预热失败: 无可用通道")
        # 隧道模式不依赖 _session_ready（每次请求独立获取通道 session）
        self._session_ready.set()

    async def _rotate_session(self, reason: str = "主动轮换"):
        """
        轮换 session（仅 TPS 模式）。
        隧道模式下由 proxy_manager.report_blocked(channel) + SessionPool 处理。
        """
        if self._proxy_mode == "tunnel":
            return  # 隧道模式不使用全局 session 轮换
        async with self._rotate_lock:
            # 防抖：5秒内不重复轮换
            now = time.monotonic()
            if now - self._last_rotate_time < 5:
                logger.debug(f"🔄 跳过轮换（距上次不足5秒）")
                return
            logger.info(f"🔄 Session {reason}...")
            # 通知所有 worker：session 不可用，请等待
            self._session_ready.clear()
            if self._session:
                await self._session.close()
                self._session = None
            await self.proxy_manager.report_blocked()
            await asyncio.sleep(1)

            # 轮换重试（最多 3 次）
            for attempt in range(3):
                self._session = AmazonSession(self.proxy_manager, self.zip_code)
                success = await self._session.initialize()
                self._success_since_rotate = 0
                self._last_rotate_time = time.monotonic()
                if success:
                    self._session_ready.set()
                    logger.info("🔄 Session 轮换成功")
                    return
                # 失败，清理后重试
                logger.warning(f"⚠️ Session 轮换初始化失败 (尝试 {attempt+1}/3)")
                if self._session:
                    await self._session.close()
                self._session = None
                if attempt < 2:
                    await asyncio.sleep(3)

            # 全部失败，set event 让 worker 走正常失败流程
            logger.error("❌ Session 轮换 3 次全部失败")
            self._session_ready.set()

    async def _pull_tasks(self, count: int = None) -> List[Dict]:
        """从服务器拉取任务"""
        try:
            url = f"{self.server_url}/api/tasks/pull"
            params = {
                "worker_id": self.worker_id,
                "count": count or self._controller.current_concurrency,
            }
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json().get("tasks", [])
            logger.warning(f"拉取任务失败: HTTP {resp.status_code}")
            return []
        except Exception as e:
            logger.error(f"拉取任务异常: {e}")
            return []

    async def _release_tasks(self, task_ids: List[int]):
        """通知 Server 归还未处理的任务（优先采集切换时调用）"""
        try:
            url = f"{self.server_url}/api/tasks/release"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json={"task_ids": task_ids})
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"已归还 {data.get('released', 0)} 个旧任务到 pending")
            else:
                logger.warning(f"归还任务失败: HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"归还任务异常: {e}")

    async def _settings_sync(self):
        """定期与 Server 同步：上报 metrics + 拉取 settings + 接收配额"""
        logger.info("⚙️ 设置同步协程启动（每 30 秒）")
        while self._running:
            try:
                await asyncio.sleep(30)
                if not self._running:
                    break

                # 收集本地 metrics 快照
                snap = self._metrics.snapshot()
                payload = {
                    "worker_id": self.worker_id,
                    "metrics": {
                        "total": snap["total"],
                        "success_rate": snap["success_rate"],
                        "block_rate": snap["block_rate"],
                        "latency_p50": snap["latency_p50"],
                        "latency_p95": snap["latency_p95"],
                        "inflight": snap["inflight"],
                        "bandwidth_bps": snap["bandwidth_bps"],
                        "current_concurrency": self._controller.current_concurrency,
                    },
                }

                # 优先使用新的综合同步端点
                s = None
                async with httpx.AsyncClient(timeout=5) as client:
                    try:
                        resp = await client.post(
                            f"{self.server_url}/api/worker/sync",
                            json=payload,
                        )
                        if resp.status_code == 200:
                            s = resp.json()
                    except Exception:
                        pass

                    # 降级：旧版 Server 没有 /api/worker/sync
                    if s is None:
                        resp = await client.get(f"{self.server_url}/api/settings")
                        if resp.status_code == 200:
                            s = resp.json()

                if s is None:
                    continue

                # === 现有 settings 同步 ===
                ver = s.get("_version", 0)
                changes = []

                if ver > self._settings_version:
                    self._settings_version = ver

                    # 令牌桶 QPS（仅在无配额时使用全局值）
                    if "_quota" not in s:
                        new_rate = s.get("token_bucket_rate")
                        if new_rate and new_rate != self._rate_limiter.rate:
                            self._rate_limiter.rate = new_rate
                            changes.append(f"QPS={new_rate}")

                    # 并发范围（仅在无配额时使用全局值）
                    if "_quota" not in s:
                        new_max = s.get("max_concurrency")
                        if new_max and new_max != self._controller._max:
                            self._controller._max = new_max
                            changes.append(f"max_c={new_max}")

                    new_min = s.get("min_concurrency")
                    if new_min and new_min != self._controller._min:
                        self._controller._min = new_min
                        changes.append(f"min_c={new_min}")

                    # AIMD 调控参数
                    for attr, key in [
                        ("_adjust_interval", "adjust_interval"),
                        ("_target_latency", "target_latency"),
                        ("_max_latency", "max_latency"),
                        ("_target_success", "target_success_rate"),
                        ("_min_success", "min_success_rate"),
                        ("_block_threshold", "block_rate_threshold"),
                        ("_cooldown_duration", "cooldown_after_block"),
                    ]:
                        val = s.get(key)
                        if val is not None and val != getattr(self._controller, attr, None):
                            setattr(self._controller, attr, val)
                            changes.append(f"{key}={val}")

                    # Session 轮换
                    new_rotate = s.get("session_rotate_every")
                    if new_rotate and new_rotate != self._rotate_every:
                        self._rotate_every = new_rotate
                        changes.append(f"rotate={new_rotate}")

                    # 最大重试
                    new_retries = s.get("max_retries")
                    if new_retries and new_retries != self._max_retries:
                        self._max_retries = new_retries
                        changes.append(f"retries={new_retries}")

                    # 截图并发数
                    new_sc = s.get("screenshot_concurrency")
                    if new_sc and new_sc != self._screenshot_concurrency:
                        self._screenshot_concurrency = new_sc
                        changes.append(f"screenshot_c={new_sc}")

                    # 代理 API 地址
                    new_proxy_url = s.get("proxy_api_url")
                    if new_proxy_url and new_proxy_url != config.PROXY_API_URL_AUTH:
                        config.PROXY_API_URL_AUTH = new_proxy_url  # noqa
                        changes.append(f"proxy_url=***{new_proxy_url[-20:]}")

                    # 代理模式（热切换：TPS ↔ 隧道）
                    new_mode = s.get("proxy_mode")
                    if new_mode and new_mode in ("tps", "tunnel") and new_mode != self._proxy_mode:
                        self._proxy_mode = new_mode
                        config.PROXY_MODE = new_mode  # noqa
                        changes.append(f"proxy_mode={new_mode}")

                    # 隧道配置
                    for cfg_key, cfg_attr in [
                        ("tunnel_host", "TUNNEL_HOST"),
                        ("tunnel_user", "TUNNEL_USER"),
                        ("tunnel_pass", "TUNNEL_PASS"),
                    ]:
                        val = s.get(cfg_key)
                        if val and val != getattr(config, cfg_attr, ""):
                            setattr(config, cfg_attr, val)
                            changes.append(cfg_key)
                    tunnel_port = s.get("tunnel_port")
                    if tunnel_port and tunnel_port != config.TUNNEL_PORT:
                        config.TUNNEL_PORT = tunnel_port
                        changes.append(f"tunnel_port={tunnel_port}")
                    tunnel_channels = s.get("tunnel_channels")
                    if tunnel_channels and tunnel_channels != config.TUNNEL_CHANNELS:
                        config.TUNNEL_CHANNELS = tunnel_channels
                        changes.append(f"tunnel_channels={tunnel_channels}")

                    if changes:
                        logger.info(f"⚙️ 设置已同步 (v{ver}): {', '.join(changes)}")

                # === 配额执行（每次都执行，不受 version 限制）===
                quota = s.get("_quota")
                if quota:
                    new_max_c = quota.get("concurrency")
                    if new_max_c and new_max_c != self._controller._max:
                        old_max = self._controller._max
                        self._controller._max = new_max_c
                        # 当前并发超出配额 → 强制缩容
                        if self._controller._concurrency > new_max_c:
                            await self._controller._resize_semaphore(
                                self._controller._concurrency, new_max_c
                            )
                            self._controller._concurrency = new_max_c
                        logger.info(f"📊 配额: max_c {old_max}->{new_max_c}")

                    new_qps = quota.get("qps")
                    if new_qps and abs(new_qps - self._rate_limiter.rate) > 0.1:
                        old_qps = self._rate_limiter.rate
                        self._rate_limiter.rate = new_qps
                        logger.info(f"📊 配额: QPS {old_qps:.1f}->{new_qps:.1f}")

                # === 全局封锁处理 ===
                block_info = s.get("_global_block", {})
                if block_info.get("active"):
                    epoch = block_info.get("epoch", 0)
                    if epoch > self._global_block_epoch:
                        self._global_block_epoch = epoch
                        # 立即并发减半
                        new_c = max(
                            self._controller._min,
                            self._controller._concurrency // 2,
                        )
                        if new_c < self._controller._concurrency:
                            await self._controller._resize_semaphore(
                                self._controller._concurrency, new_c
                            )
                            self._controller._concurrency = new_c
                            # 设置本地冷却
                            remaining = block_info.get("remaining_s", 30)
                            self._controller._cooldown_until = time.monotonic() + remaining
                        logger.warning(
                            f"⚠️ 全局封锁 epoch={epoch}, "
                            f"并发 -> {new_c}, 冷却 {block_info.get('remaining_s')}s"
                        )

                # === 恢复抖动系数 ===
                jitter = s.get("_recovery_jitter")
                if jitter is not None:
                    self._recovery_jitter = jitter
                    self._controller._recovery_jitter = jitter

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"⚙️ 设置同步异常: {e}")

    async def _process_task(self, task: Dict) -> tuple:
        """
        处理单个采集任务

        返回: (success: bool, blocked: bool, resp_bytes: int)

        双模式分支：
        - TPS: 所有 worker 共享全局 self._session，被封时触发全局 _rotate_session
        - 隧道: 每次请求从 proxy_manager 分配通道，从 session_pool 取对应 session，
                被封时仅标记该通道，下次循环自动切到其他通道
        """
        asin = task["asin"]
        task_id = task["id"]
        zip_code = task.get("zip_code", self.zip_code)
        max_retries = self._max_retries
        resp_bytes = 0
        last_error_type = "network"
        last_error_detail = ""
        is_tunnel = (self._proxy_mode == "tunnel")

        attempt = 0
        while attempt < max_retries:
            try:
                # 全局令牌桶限流（替代 per-worker sleep，确保系统级 QPS 不超标）
                await self._rate_limiter.acquire()

                # === Session 获取（按模式分支）===
                session = None
                channel = None

                if is_tunnel:
                    # 隧道模式：从 proxy_manager 分配可用通道
                    channel = self.proxy_manager.get_available_channel()
                    if channel is None:
                        # 全部通道被封 → 等待 IP 轮换
                        logger.warning(f"ASIN {asin} 全部通道被封，等待 IP 轮换...")
                        await self.proxy_manager.wait_for_rotation()
                        attempt += 1
                        continue
                    session = await self._session_pool.get_session(channel)
                    if session is None or not session.is_ready():
                        attempt += 1
                        logger.warning(f"ASIN {asin} [ch{channel}] session 未就绪 (尝试 {attempt}/{max_retries})")
                        await asyncio.sleep(2)
                        continue
                else:
                    # TPS 模式：等待全局 session 就绪
                    if not self._session_ready.is_set():
                        logger.debug(f"ASIN {asin} 等待 session 就绪...")
                        try:
                            await asyncio.wait_for(self._session_ready.wait(), timeout=30)
                        except asyncio.TimeoutError:
                            logger.warning(f"ASIN {asin} 等待 session 超时 30s")
                            attempt += 1
                            continue
                    if self._session is None or not self._session.is_ready():
                        attempt += 1
                        logger.warning(f"ASIN {asin} session 仍未就绪 (尝试 {attempt}/{max_retries})")
                        await asyncio.sleep(2)
                        continue
                    session = self._session

                ch_tag = f" [ch{channel}]" if is_tunnel else ""

                # 快速模式: 先用 AOD 获取价格数据
                if self.fast_mode and attempt == 0:
                    aod_result = await self._try_aod_fast(asin, zip_code, task, session)
                    if aod_result is not None:
                        await self._submit_result(task_id, aod_result, success=True)
                        self._stats["success"] += 1
                        self._stats["total"] += 1
                        title_short = aod_result["title"][:40] if aod_result.get("title") else "AOD"
                        logger.info(f"AOD {asin}{ch_tag} | {title_short}... | {aod_result['buybox_price']}")
                        if not is_tunnel:
                            self._success_since_rotate += 1
                            if self._success_since_rotate >= self._rotate_every:
                                await self._rotate_session(reason=f"主动轮换 (已完成 {self._success_since_rotate} 次)")
                        return (True, False, resp_bytes)

                # 发起请求
                resp = await session.fetch_product_page(asin)

                # 请求失败（超时/网络异常）→ 不换 IP，等待后重试
                if resp is None:
                    attempt += 1
                    logger.warning(f"ASIN {asin}{ch_tag} 请求超时 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 记录响应大小
                resp_bytes = len(resp.content) if hasattr(resp, 'content') else 0

                # 真正被封（403/503/验证码）
                if session.is_blocked(resp):
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = f"HTTP {resp.status_code}"
                    if is_tunnel:
                        logger.warning(f"ASIN {asin} [ch{channel}] 被封 HTTP {resp.status_code} (尝试 {attempt}/{max_retries})")
                        await self.proxy_manager.report_blocked(channel)
                        continue  # 继续循环 → 下次分配到其他通道
                    else:
                        logger.warning(f"ASIN {asin} 被封 HTTP {resp.status_code} (尝试 {attempt}/{max_retries})")
                        await self._rotate_session(reason="被封锁")
                        return (False, True, resp_bytes)  # 标记被封，让控制器知道

                # 404 处理
                if session.is_404(resp):
                    logger.info(f"ASIN {asin}{ch_tag} 商品不存在 (404)")
                    result_data = self.parser._default_result(asin, zip_code)
                    result_data["title"] = "[商品不存在]"
                    result_data["batch_name"] = task.get("batch_name", "")
                    await self._submit_result(task_id, result_data, success=True)
                    self._stats["success"] += 1
                    self._stats["total"] += 1
                    return (True, False, resp_bytes)

                # 解析页面
                result_data = self.parser.parse_product(resp.text, asin, zip_code)
                result_data["batch_name"] = task.get("batch_name", "")

                # 检查是否是拦截或空页面
                title = result_data.get("title", "")
                if title == "[验证码拦截]":
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "captcha"
                    last_error_detail = "validateCaptcha / Robot Check"
                    logger.warning(f"ASIN {asin}{ch_tag} {title} (尝试 {attempt}/{max_retries})")
                    if is_tunnel:
                        await self.proxy_manager.report_blocked(channel)
                    else:
                        await self._rotate_session(reason="页面拦截")
                    continue

                if title == "[API封锁]":
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = "api-services-support@amazon.com"
                    logger.warning(f"ASIN {asin}{ch_tag} {title} (尝试 {attempt}/{max_retries})")
                    if is_tunnel:
                        await self.proxy_manager.report_blocked(channel)
                    else:
                        await self._rotate_session(reason="页面拦截")
                    continue

                if title in ["[页面为空]", "[HTML解析失败]"]:
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = title
                    logger.warning(f"ASIN {asin}{ch_tag} {title} (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 标题为空视为软拦截，重试
                if not title or title == "N/A":
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = "标题为空"
                    logger.warning(f"ASIN {asin}{ch_tag} 标题为空 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 邮编/货币校验：检测是否采集到了非美国地区的数据
                price = result_data.get("current_price", "")
                if price and price not in ["N/A", "不可售", "See price in cart"]:
                    # 价格应包含 $ 符号；出现 CNY/¥/€/£ 说明邮编没生效
                    if any(c in price for c in ["¥", "€", "£", "CNY"]) or "$" not in price:
                        attempt += 1
                        last_error_type = "parse_error"
                        last_error_detail = f"非美国价格: {price}"
                        logger.warning(f"ASIN {asin}{ch_tag} 非美国价格 '{price}' (尝试 {attempt}/{max_retries})")
                        if is_tunnel:
                            await self.proxy_manager.report_blocked(channel)
                        else:
                            await self._rotate_session(reason="非美国区域数据")
                        continue

                # 成功
                await self._submit_result(task_id, result_data, success=True)
                self._stats["success"] += 1
                self._stats["total"] += 1

                title_short = result_data["title"][:40] if result_data["title"] else "N/A"
                logger.info(f"OK {asin}{ch_tag} | {title_short}... | {result_data['current_price']}")

                # 截图存证：放入截图队列（无限队列，不会丢失）
                if task.get("needs_screenshot"):
                    await self._screenshot_queue.put({
                        "task_id": task_id,
                        "asin": asin,
                        "batch_name": task.get("batch_name", ""),
                        "html": resp.text,
                    })

                # 主动轮换：每 N 次成功请求更换 session 防止被检测（仅 TPS 模式）
                if not is_tunnel:
                    self._success_since_rotate += 1
                    if self._success_since_rotate >= self._rotate_every:
                        await self._rotate_session(reason=f"主动轮换 (已完成 {self._success_since_rotate} 次)")

                return (True, False, resp_bytes)

            except Exception as e:
                attempt += 1
                err_name = type(e).__name__
                if "timeout" in err_name.lower() or "Timeout" in str(e):
                    last_error_type = "timeout"
                elif "connect" in err_name.lower() or "ConnectionError" in err_name:
                    last_error_type = "network"
                else:
                    last_error_type = "network"
                last_error_detail = f"{err_name}: {str(e)[:200]}"
                logger.error(f"ASIN {asin} 异常 (尝试 {attempt}/{max_retries}): {e}")
                await asyncio.sleep(2)

        # 所有重试用完，标记失败
        logger.error(f"ASIN {asin} 采集失败 (已重试 {max_retries} 次) [{last_error_type}]")
        await self._submit_result(task_id, None, success=False,
                                  error_type=last_error_type, error_detail=last_error_detail)
        self._stats["failed"] += 1
        self._stats["total"] += 1
        return (False, False, resp_bytes)

    async def _try_aod_fast(self, asin: str, zip_code: str, task: Dict,
                            session: AmazonSession = None) -> Optional[Dict]:
        """
        AOD 快速路径: 用 AOD AJAX 端点获取价格数据
        成功返回 result_data，失败返回 None（会 fallback 到产品页）

        Args:
            session: 指定使用的 session（隧道模式下传入通道 session）
        """
        session = session or self._session
        try:
            resp = await session.fetch_aod_page(asin)
            if resp is None:
                return None
            if session.is_blocked(resp):
                return None
            if resp.status_code != 200:
                return None

            aod_data = self.parser.parse_aod_response(resp.text, asin)

            # AOD 必须至少有价格才算成功
            if not aod_data.get("offers") or aod_data["buybox_price"] == "N/A":
                return None

            # 构建完整结果（AOD 只有价格数据，其他字段留默认）
            result_data = self.parser._default_result(asin, zip_code)
            result_data["title"] = f"[AOD] {asin}"  # AOD 不包含标题
            result_data["buybox_price"] = aod_data["buybox_price"]
            result_data["current_price"] = aod_data["buybox_price"]
            result_data["buybox_shipping"] = aod_data["buybox_shipping"]
            result_data["is_fba"] = aod_data["is_fba"]
            result_data["batch_name"] = task.get("batch_name", "")
            return result_data

        except Exception as e:
            logger.debug(f"AOD 快速路径失败 {asin}: {e}")
            return None

    # ═══════════════════════════════════════════════
    # 结果提交（保持不变）
    # ═══════════════════════════════════════════════

    async def _submit_result(self, task_id: int, result_data: Optional[Dict], success: bool,
                             error_type: str = None, error_detail: str = None):
        """将结果放入批量提交队列"""
        payload = {
            "task_id": task_id,
            "worker_id": self.worker_id,
            "success": success,
            "result": result_data,
        }
        if error_type:
            payload["error_type"] = error_type
            payload["error_detail"] = (error_detail or "")[:500]
        await self._result_queue.put(payload)

    async def _batch_submitter(self):
        """后台协程：每攒够 batch_size 个或每 batch_interval 秒批量提交"""
        batch: List[Dict] = []
        while self._running or not self._result_queue.empty():
            try:
                # 等待第一条数据到来（最多等 batch_interval 秒）
                try:
                    item = await asyncio.wait_for(
                        self._result_queue.get(), timeout=self._batch_interval
                    )
                    batch.append(item)
                except asyncio.TimeoutError:
                    # 超时且无数据 → 继续等
                    if batch:
                        await self._submit_batch(batch)
                        batch = []
                    continue

                # 拿到第一条后，在剩余窗口内继续攒数据
                deadline = asyncio.get_event_loop().time() + self._batch_interval
                while len(batch) < self._batch_size:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(
                            self._result_queue.get(), timeout=remaining
                        )
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break  # 窗口到期

                # 提交攒到的批次
                if batch:
                    await self._submit_batch(batch)
                    batch = []

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"批量提交协程异常: {e}")
                await asyncio.sleep(1)

        # 退出前刷新剩余
        if batch:
            await self._submit_batch(batch)

    async def _flush_results(self):
        """刷新队列中所有剩余结果"""
        batch: List[Dict] = []
        while not self._result_queue.empty():
            batch.append(self._result_queue.get_nowait())
        if batch:
            await self._submit_batch(batch)

    async def _submit_batch(self, batch: List[Dict], retry: int = 3):
        """批量 POST 提交结果到服务器（含重试）"""
        url = f"{self.server_url}/api/tasks/result/batch"
        for attempt in range(retry):
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.post(url, json={"results": batch})
                if resp.status_code == 200:
                    logger.debug(f"批量提交 {len(batch)} 条结果成功")
                    return
                logger.warning(f"批量提交失败 HTTP {resp.status_code} (尝试 {attempt+1}/{retry})")
            except Exception as e:
                logger.error(f"批量提交异常 (尝试 {attempt+1}/{retry}): {e}")
            if attempt < retry - 1:
                await asyncio.sleep(2 ** attempt)
        # 全部重试失败，回退逐条提交
        logger.error("批量提交多次失败，回退逐条提交")
        await self._submit_batch_fallback(batch)

    async def _submit_batch_fallback(self, batch: List[Dict]):
        """逐条提交 fallback（批量接口不可用时）"""
        url = f"{self.server_url}/api/tasks/result"
        async with httpx.AsyncClient(timeout=10) as client:
            for payload in batch:
                try:
                    resp = await client.post(url, json=payload)
                    if resp.status_code != 200:
                        logger.warning(f"逐条提交失败: task_id={payload.get('task_id')} HTTP {resp.status_code}")
                except Exception as e:
                    logger.error(f"逐条提交异常: task_id={payload.get('task_id')} {e}")

    # ═══════════════════════════════════════════════
    # 截图渲染管道
    # ═══════════════════════════════════════════════

    async def _screenshot_workers(self):
        """
        截图协程池：动态管理多个并发截图协程，共享同一个 Playwright 浏览器实例。
        Playwright 原生支持多 page 并发（每个 page 独立渲染管线），无线程安全问题。
        """
        n = self._screenshot_concurrency
        logger.info(f"📸 截图协程池启动（{n} 并发）")
        tasks: List[asyncio.Task] = []
        for i in range(n):
            tasks.append(asyncio.create_task(self._screenshot_loop(i)))

        # 监控循环：动态增减截图协程
        while self._running or not self._screenshot_queue.empty():
            await asyncio.sleep(3)
            target = self._screenshot_concurrency
            active = [t for t in tasks if not t.done()]
            current = len(active)
            if target > current:
                for i in range(target - current):
                    idx = len(tasks)
                    tasks.append(asyncio.create_task(self._screenshot_loop(idx)))
                logger.info(f"📸 截图协程扩容: {current} → {target}")
            tasks = [t for t in tasks if not t.done()]

        # 等待所有截图协程完成
        remaining = [t for t in tasks if not t.done()]
        if remaining:
            await asyncio.gather(*remaining, return_exceptions=True)
        logger.info("📸 截图协程池退出")

    async def _screenshot_loop(self, idx: int):
        """单个截图协程：从队列取任务，渲染并上传"""
        while self._running or not self._screenshot_queue.empty():
            try:
                try:
                    item = await asyncio.wait_for(
                        self._screenshot_queue.get(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    continue

                asin = item["asin"]
                batch_name = item["batch_name"]
                html_content = item["html"]

                try:
                    png_bytes = await self._render_screenshot(html_content, asin)
                    if png_bytes:
                        await self._upload_screenshot(batch_name, asin, png_bytes)
                        logger.info(f"📸 截图完成: {asin} ({len(png_bytes)} bytes) [worker-{idx}]")
                    else:
                        logger.warning(f"📸 截图渲染失败: {asin}")
                except Exception as e:
                    logger.error(f"📸 截图异常 {asin}: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"📸 截图协程 #{idx} 异常: {e}")
                await asyncio.sleep(1)

    async def _render_screenshot(self, html_content: str, asin: str) -> Optional[bytes]:
        """
        用 Playwright 渲染 Amazon 网页截图

        优化点：
        1. setContent() 直接注入 HTML，省去 URL 导航和主文档拦截开销
        2. 屏蔽 JS/字体/媒体/追踪，只保留 CSS 和图片保证页面外观
        3. 更可靠的裁剪逻辑：扫描多个锚点元素取最大 bottom
        4. 浏览器持久化复用，page 级错误不杀浏览器（防止级联崩溃）
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("📸 playwright 未安装，跳过截图渲染")
            return None

        page = None
        try:
            # 懒初始化：首次调用时启动浏览器（加锁防止并发重复初始化）
            if self._browser is None:
                async with self._browser_lock:
                    if self._browser is None:  # double-check
                        self._playwright = await async_playwright().__aenter__()
                        self._browser = await self._playwright.chromium.launch(
                            headless=True,
                            args=["--disable-gpu", "--disable-dev-shm-usage",
                                  "--no-sandbox", "--disable-extensions"]
                        )
                        logger.info("📸 Playwright 浏览器已启动（持久化复用）")

            page = await self._browser.new_page(viewport={"width": 1280, "height": 900})

            # 屏蔽无关资源：只保留 CSS 和图片，保证页面外观
            async def block_resources(route):
                rt = route.request.resource_type
                url = route.request.url
                if rt in ("stylesheet", "image"):
                    await route.continue_()
                elif rt in ("script", "font", "media", "websocket",
                            "manifest", "other"):
                    await route.abort()
                elif any(x in url for x in ("analytics", "tracking", "beacon",
                                            "ads", "doubleclick", "facebook")):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", block_resources)

            # 注入 <base> 标签，使 protocol-relative URL (//...) 和相对路径都能正确解析
            # setContent 在 about:blank 上下文中运行，没有 base 则 //cdn... 会变成 about://cdn...
            base_tag = '<base href="https://www.amazon.com/">'
            lower_head = html_content[:2000].lower()
            if "<base " not in lower_head:
                # 找到 <head> 或 <head ...> 的结束位置
                head_pos = lower_head.find("<head")
                if head_pos != -1:
                    close_pos = html_content.index(">", head_pos) + 1
                    html_content = html_content[:close_pos] + base_tag + html_content[close_pos:]
                else:
                    html_content = base_tag + html_content

            # setContent 直接注入 HTML（比 goto + route 拦截快 ~500ms）
            try:
                await page.set_content(
                    html_content,
                    wait_until="domcontentloaded",
                    timeout=15000,
                )
            except Exception:
                pass  # 超时不影响截图

            # 等待网络空闲（CSS/图片加载完毕），超时 2s 兼顾速度与质量
            try:
                await page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass  # 超时仍继续截图，大部分资源应已加载

            # 计算裁剪高度：扫描多个锚点元素，取最大 bottom 值
            clip_height = await page.evaluate("""() => {
                // 防御 document.body 为 null（setContent 异常时可能发生）
                if (!document.body) return 1200;
                const anchors = [
                    '#buybox', '#rightCol', '#buyBoxAccordion',
                    '#add-to-cart-button', '#buy-now-button',
                    '#submitOrderButtonId', '#averageCustomerReviews',
                    '#productOverview_feature_div', '#centerCol',
                    '#apex_desktop_newAccordionRow'
                ];
                let maxBottom = 0;
                for (const sel of anchors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const rect = el.getBoundingClientRect();
                        if (rect.bottom > maxBottom) maxBottom = rect.bottom;
                    }
                }
                // 找到了锚点元素 → 底部加 100px 边距
                if (maxBottom > 0) return Math.ceil(maxBottom + 100);
                // 兜底：取页面实际高度，但不超过 3000px
                return Math.min(document.body.scrollHeight || 1200, 3000);
            }""")
            clip_height = max(800, min(clip_height, 3000))

            # 截图前检查页面是否有可见内容（防止空白截图）
            has_content = await page.evaluate("""() => {
                if (!document.body) return false;
                // 检查是否有可见的文本或图片
                const text = document.body.innerText || '';
                if (text.trim().length > 50) return true;
                const imgs = document.querySelectorAll('img[src]');
                if (imgs.length > 0) return true;
                return false;
            }""")

            screenshot = await page.screenshot(
                type="png",
                clip={"x": 0, "y": 0, "width": 1280, "height": clip_height}
            )

            # 空白检测：PNG < 10KB 且页面无可见内容 → 判定为空白截图
            if len(screenshot) < 10240 and not has_content:
                logger.warning(f"📸 空白截图已丢弃: {asin} ({len(screenshot)} bytes, 无可见内容)")
                return None

            return screenshot
        except Exception as e:
            err_msg = str(e)
            # 只有浏览器进程级崩溃才重置浏览器；page 级错误不连坐
            if "browser has been closed" in err_msg or "Target closed" in err_msg:
                logger.error(f"📸 浏览器进程崩溃，将重新启动: {asin}")
                await self._close_browser()
            else:
                logger.warning(f"📸 页面渲染失败 {asin}: {e}")
            return None
        finally:
            # 无论成功失败都安全关闭 page（不影响浏览器和其他 page）
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    async def _close_browser(self):
        """安全关闭 Playwright 浏览器（加锁防止并发关闭冲突）"""
        async with self._browser_lock:
            try:
                if self._browser:
                    await self._browser.close()
            except Exception:
                pass
            try:
                if self._playwright:
                    await self._playwright.__aexit__(None, None, None)
            except Exception:
                pass
            self._browser = None
            self._playwright = None

    async def _upload_screenshot(self, batch_name: str, asin: str, png_bytes: bytes):
        """将截图 POST 到 Server"""
        try:
            url = f"{self.server_url}/api/tasks/screenshot"
            files = {"file": (f"{asin}.png", png_bytes, "image/png")}
            data = {"batch_name": batch_name, "asin": asin}
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, files=files, data=data)
            if resp.status_code != 200:
                logger.warning(f"截图上传失败 {asin}: HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"截图上传异常 {asin}: {e}")

    # ═══════════════════════════════════════════════
    # 隧道模式 IP 轮换监控
    # ═══════════════════════════════════════════════

    async def _ip_rotation_watcher(self):
        """
        IP 轮换监控协程（仅隧道模式）。

        每秒检查是否到达 IP 轮换时间点（60s 周期），
        轮换后重建所有通道的 Session（关闭旧连接，新建走新 IP 的连接）。
        """
        logger.info(f"🔄 IP 轮换监控启动 (周期: {config.TUNNEL_ROTATE_INTERVAL}s)")
        while self._running:
            try:
                await asyncio.sleep(1)
                if not self._running:
                    break

                rotated = await self.proxy_manager.handle_ip_rotation()
                if rotated:
                    logger.info("🔄 IP 轮换触发，重建所有通道 Session...")
                    if self._session_pool:
                        await self._session_pool.rebuild_all()
                    logger.info(f"🔄 IP 轮换完成，{self._session_pool.ready_count}/{config.TUNNEL_CHANNELS} 通道就绪"
                                f" | 下次轮换: {self.proxy_manager.time_to_next_rotation():.0f}s")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"🔄 IP 轮换监控异常: {e}")
                await asyncio.sleep(5)

        logger.info("🔄 IP 轮换监控退出")

    # ═══════════════════════════════════════════════
    # 生命周期
    # ═══════════════════════════════════════════════

    async def _cleanup(self):
        """清理资源"""
        # 停止自适应控制器
        await self._controller.stop()
        # 刷新批量提交队列中的剩余结果
        if self._result_queue:
            await self._flush_results()
        if self._batch_submitter_task:
            self._batch_submitter_task.cancel()
            try:
                await self._batch_submitter_task
            except asyncio.CancelledError:
                pass
        # 关闭 Session（TPS 模式）
        if self._session:
            await self._session.close()
        # 关闭 SessionPool（隧道模式）
        if self._session_pool:
            await self._session_pool.close_all()
        # 关闭持久化浏览器
        await self._close_browser()

    def _print_stats(self):
        """打印统计信息"""
        elapsed = time.time() - self._stats["start_time"] if self._stats["start_time"] else 0
        total = self._stats["total"]
        success = self._stats["success"]
        rate = success / total * 100 if total > 0 else 0
        speed = total / elapsed * 60 if elapsed > 0 else 0

        logger.info("=" * 60)
        logger.info(f"📊 Worker [{self.worker_id}] 统计")
        logger.info(f"   总采集: {total}")
        logger.info(f"   成功: {success} ({rate:.1f}%)")
        logger.info(f"   失败: {self._stats['failed']}")
        logger.info(f"   被封: {self._stats['blocked']}")
        logger.info(f"   速度: {speed:.1f} 条/分钟")
        logger.info(f"   耗时: {elapsed:.0f} 秒")
        logger.info(f"   最终并发: {self._controller.current_concurrency}")
        # 最终指标快照
        logger.info(self._metrics.format_summary())
        logger.info("=" * 60)


def main():
    """Worker 入口"""
    arg_parser = argparse.ArgumentParser(description="Amazon Scraper Worker (Pipeline + Adaptive)")
    arg_parser.add_argument("--server", required=True, help="中央服务器地址 (如 http://192.168.1.100:8899)")
    arg_parser.add_argument("--worker-id", default=None, help="Worker ID（默认自动生成）")
    arg_parser.add_argument("--concurrency", type=int, default=None,
                            help=f"最大并发数上限（默认 {config.MAX_CONCURRENCY}，自适应控制器自动探索最优值）")
    arg_parser.add_argument("--zip-code", default=None, help=f"邮编（默认 {config.DEFAULT_ZIP_CODE}）")
    arg_parser.add_argument("--fast", action="store_true", help="快速模式: AOD 优先获取价格数据")

    args = arg_parser.parse_args()

    worker = Worker(
        server_url=args.server,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        zip_code=args.zip_code,
        fast_mode=args.fast,
    )

    # 优雅退出
    loop = asyncio.new_event_loop()

    def signal_handler(sig, frame):
        logger.info("⏹️ 收到停止信号，正在退出...")
        loop.create_task(worker.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        loop.run_until_complete(worker.start())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
