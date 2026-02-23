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
import random
import time
import uuid
import signal
import sys
from typing import Optional, Dict, List

from curl_cffi import requests as curl_requests

import config
from proxy import ProxyManager, get_proxy_manager
from session import AmazonSession
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

        # 组件
        self.proxy_manager = get_proxy_manager()
        self.parser = AmazonParser()
        self._session: Optional[AmazonSession] = None

        # 速率控制
        self._interval = config.REQUEST_INTERVAL
        self._jitter = config.REQUEST_JITTER
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

        # Session 轮换控制
        self._success_since_rotate = 0
        self._rotate_every = config.SESSION_ROTATE_EVERY
        self._rotate_lock = asyncio.Lock()
        self._last_rotate_time = 0.0  # 轮换防抖
        self._session_ready = asyncio.Event()  # Session 就绪信号

        # Worker 协程管理
        self._worker_tasks: List[asyncio.Task] = []

        # 截图队列（非阻塞异步管道）
        self._screenshot_queue: asyncio.Queue = None
        self._browser = None           # 持久化 Playwright 浏览器实例
        self._playwright = None        # Playwright 上下文管理器

        # 设置同步
        self._settings_version = 0

    async def start(self):
        """启动 Worker（流水线架构）"""
        logger.info(f"🚀 Worker [{self.worker_id}] 启动（流水线模式）")
        logger.info(f"   服务器: {self.server_url}")
        logger.info(f"   初始并发: {self._controller.current_concurrency}")
        logger.info(f"   并发范围: [{config.MIN_CONCURRENCY}, {self._controller._max}]")
        logger.info(f"   邮编: {self.zip_code}")
        logger.info(f"   快速模式: {'开启 (AOD优先)' if self.fast_mode else '关闭'}")

        self._running = True
        self._stats["start_time"] = time.time()

        # 初始化队列
        self._task_queue = asyncio.Queue(maxsize=self._queue_size)
        self._result_queue = asyncio.Queue()
        self._screenshot_queue = asyncio.Queue(maxsize=500)

        # 初始化 session
        await self._init_session()

        # 启动自适应控制器
        await self._controller.start()

        # 启动核心协程（含截图后台 worker）
        try:
            await asyncio.gather(
                self._task_feeder(),         # 1. 持续从 Server 拉任务
                self._worker_pool(),         # 2. 工人池：自适应并发
                self._batch_submitter(),     # 3. 批量回传结果
                self._screenshot_worker(),   # 4. 截图渲染后台协程
                self._settings_sync(),       # 5. 定期同步服务端设置
            )
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
                            # 清空当前队列中的旧任务（靠 5 分钟超时机制自动回收为 pending）
                            dropped = 0
                            while not self._task_queue.empty():
                                try:
                                    self._task_queue.get_nowait()
                                    dropped += 1
                                except asyncio.QueueEmpty:
                                    break
                            logger.info(f"🚀 检测到优先采集任务，已清空队列中 {dropped} 个旧任务")

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

    async def _init_session(self):
        """初始化 Amazon session（失败时重试，确保 _session_ready 最终被 set）"""
        logger.info("🔧 初始化 Amazon session...")
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

    async def _rotate_session(self, reason: str = "主动轮换"):
        """轮换 session：关闭旧的，刷新代理，创建新的（带防抖 + 就绪信号 + 失败重试）"""
        async with self._rotate_lock:
            # 防抖：5秒内不重复轮换
            now = time.time()
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
                self._last_rotate_time = time.time()
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
            resp = curl_requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("tasks", [])
            else:
                logger.warning(f"拉取任务失败: HTTP {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"拉取任务异常: {e}")
            return []

    async def _settings_sync(self):
        """定期从服务端同步设置，热更新运行参数"""
        logger.info("⚙️ 设置同步协程启动（每 30 秒检查一次）")
        while self._running:
            try:
                await asyncio.sleep(30)
                if not self._running:
                    break

                resp = curl_requests.get(
                    f"{self.server_url}/api/settings", timeout=5
                )
                if resp.status_code != 200:
                    continue

                s = resp.json()
                ver = s.get("_version", 0)
                if ver <= self._settings_version:
                    continue  # 没有变化

                self._settings_version = ver
                changes = []

                # 令牌桶 QPS
                new_rate = s.get("token_bucket_rate")
                if new_rate and new_rate != self._rate_limiter.rate:
                    self._rate_limiter.rate = new_rate
                    changes.append(f"QPS={new_rate}")

                # 并发范围
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
                if new_retries and new_retries != config.MAX_RETRIES:
                    config.MAX_RETRIES = new_retries
                    changes.append(f"retries={new_retries}")

                if changes:
                    logger.info(f"⚙️ 设置已同步 (v{ver}): {', '.join(changes)}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"⚙️ 设置同步异常: {e}")

    async def _process_task(self, task: Dict) -> tuple:
        """
        处理单个采集任务
        
        返回: (success: bool, blocked: bool, resp_bytes: int)
        """
        asin = task["asin"]
        task_id = task["id"]
        zip_code = task.get("zip_code", self.zip_code)
        max_retries = config.MAX_RETRIES
        resp_bytes = 0
        last_error_type = "network"
        last_error_detail = ""

        attempt = 0
        while attempt < max_retries:
            try:
                # 全局令牌桶限流（替代 per-worker sleep，确保系统级 QPS 不超标）
                await self._rate_limiter.acquire()

                # 等待 session 就绪（轮换期间统一等待信号，不各自初始化）
                if not self._session_ready.is_set():
                    logger.debug(f"ASIN {asin} 等待 session 就绪...")
                    try:
                        await asyncio.wait_for(self._session_ready.wait(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.warning(f"ASIN {asin} 等待 session 超时 30s")
                        attempt += 1
                        continue

                if self._session is None or self._session._session is None:
                    attempt += 1
                    logger.warning(f"ASIN {asin} session 仍未就绪 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 快速模式: 先用 AOD 获取价格数据
                if self.fast_mode and attempt == 0:
                    aod_result = await self._try_aod_fast(asin, zip_code, task)
                    if aod_result is not None:
                        await self._submit_result(task_id, aod_result, success=True)
                        self._stats["success"] += 1
                        self._stats["total"] += 1
                        self._success_since_rotate += 1
                        title_short = aod_result["title"][:40] if aod_result.get("title") else "AOD"
                        logger.info(f"AOD {asin} | {title_short}... | {aod_result['buybox_price']}")
                        if self._success_since_rotate >= self._rotate_every:
                            await self._rotate_session(reason=f"主动轮换 (已完成 {self._success_since_rotate} 次)")
                        return (True, False, resp_bytes)

                # 发起请求
                resp = await self._session.fetch_product_page(asin)

                # 请求失败（超时/网络异常）→ 不换 IP，等待后重试
                if resp is None:
                    attempt += 1
                    logger.warning(f"ASIN {asin} 请求超时 (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 记录响应大小
                resp_bytes = len(resp.content) if hasattr(resp, 'content') else 0

                # 真正被封（403/503/验证码）→ 换 IP + 换 session
                if self._session.is_blocked(resp):
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = f"HTTP {resp.status_code}"
                    logger.warning(f"ASIN {asin} 被封 HTTP {resp.status_code} (尝试 {attempt}/{max_retries})")
                    await self._rotate_session(reason="被封锁")
                    return (False, True, resp_bytes)  # 标记被封，让控制器知道

                # 404 处理
                if self._session.is_404(resp):
                    logger.info(f"ASIN {asin} 商品不存在 (404)")
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
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await self._rotate_session(reason="页面拦截")
                    continue

                if title == "[API封锁]":
                    attempt += 1
                    self._stats["blocked"] += 1
                    last_error_type = "blocked"
                    last_error_detail = "api-services-support@amazon.com"
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await self._rotate_session(reason="页面拦截")
                    continue

                if title in ["[页面为空]", "[HTML解析失败]"]:
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = title
                    logger.warning(f"ASIN {asin} {title} (尝试 {attempt}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 标题为空视为软拦截，重试
                if not title or title == "N/A":
                    attempt += 1
                    last_error_type = "parse_error"
                    last_error_detail = "标题为空"
                    logger.warning(f"ASIN {asin} 标题为空 (尝试 {attempt}/{max_retries})")
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
                        logger.warning(f"ASIN {asin} 检测到非美国价格 '{price}'，邮编可能未生效 (尝试 {attempt}/{max_retries})")
                        await self._rotate_session(reason="非美国区域数据")
                        continue

                # 成功
                await self._submit_result(task_id, result_data, success=True)
                self._stats["success"] += 1
                self._stats["total"] += 1
                self._success_since_rotate += 1

                title_short = result_data["title"][:40] if result_data["title"] else "N/A"
                logger.info(f"OK {asin} | {title_short}... | {result_data['current_price']}")

                # 截图存证：非阻塞放入截图队列
                if task.get("needs_screenshot"):
                    try:
                        self._screenshot_queue.put_nowait({
                            "task_id": task_id,
                            "asin": asin,
                            "batch_name": task.get("batch_name", ""),
                            "html": resp.text,
                        })
                    except asyncio.QueueFull:
                        logger.warning(f"📸 截图队列已满，跳过 ASIN {asin}")

                # 主动轮换：每 N 次成功请求更换 session 防止被检测
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

    async def _try_aod_fast(self, asin: str, zip_code: str, task: Dict) -> Optional[Dict]:
        """
        AOD 快速路径: 用 AOD AJAX 端点获取价格数据
        成功返回 result_data，失败返回 None（会 fallback 到产品页）
        """
        try:
            resp = await self._session.fetch_aod_page(asin)
            if resp is None:
                return None
            if self._session.is_blocked(resp):
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

    async def _submit_batch(self, batch: List[Dict]):
        """批量 POST 提交结果到服务器"""
        try:
            url = f"{self.server_url}/api/tasks/result/batch"
            resp = curl_requests.post(
                url,
                json={"results": batch},
                timeout=15,
            )
            if resp.status_code == 200:
                logger.debug(f"批量提交 {len(batch)} 条结果成功")
            else:
                logger.warning(f"批量提交失败 HTTP {resp.status_code}，回退逐条提交")
                await self._submit_batch_fallback(batch)
        except Exception as e:
            logger.error(f"批量提交异常: {e}，回退逐条提交")
            await self._submit_batch_fallback(batch)

    async def _submit_batch_fallback(self, batch: List[Dict]):
        """逐条提交 fallback（批量接口不可用时）"""
        url = f"{self.server_url}/api/tasks/result"
        for payload in batch:
            try:
                resp = curl_requests.post(url, json=payload, timeout=10)
                if resp.status_code != 200:
                    logger.warning(f"逐条提交失败: task_id={payload.get('task_id')} HTTP {resp.status_code}")
            except Exception as e:
                logger.error(f"逐条提交异常: task_id={payload.get('task_id')} {e}")

    # ═══════════════════════════════════════════════
    # 截图渲染管道
    # ═══════════════════════════════════════════════

    async def _screenshot_worker(self):
        """
        后台截图协程：从截图队列取任务，用 Playwright 渲染 PNG，POST 给 Server。
        串行处理（每次 1 个），避免 Chrome 占用过多内存。
        """
        logger.info("📸 截图后台协程启动")

        while self._running or not self._screenshot_queue.empty():
            try:
                # 等待截图任务（最多等 5 秒）
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
                    # 渲染 Amazon 网页截图
                    png_bytes = await self._render_screenshot(html_content, asin)
                    if png_bytes:
                        # POST 截图到 Server
                        await self._upload_screenshot(batch_name, asin, png_bytes)
                        logger.info(f"📸 截图完成: {asin} ({len(png_bytes)} bytes)")
                    else:
                        logger.warning(f"📸 截图渲染失败: {asin}")
                except Exception as e:
                    logger.error(f"📸 截图异常 {asin}: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"📸 截图协程异常: {e}")
                await asyncio.sleep(1)

        logger.info("📸 截图后台协程退出")

    async def _render_screenshot(self, html_content: str, asin: str) -> Optional[bytes]:
        """
        用 Playwright 渲染 Amazon 网页截图

        优化点（相比旧版 goto + route 拦截）：
        1. setContent() 直接注入 HTML，省去 URL 导航和主文档拦截开销
        2. 屏蔽 JS/字体/媒体/追踪，只保留 CSS 和图片保证页面外观
        3. 更可靠的裁剪逻辑：扫描多个锚点元素取最大 bottom
        4. 浏览器持久化复用
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("📸 playwright 未安装，跳过截图渲染")
            return None

        try:
            # 懒初始化：首次调用时启动浏览器
            if self._browser is None:
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

            # setContent 直接注入 HTML（比 goto + route 拦截快 ~500ms）
            try:
                await page.set_content(html_content, wait_until="domcontentloaded", timeout=10000)
            except Exception:
                pass  # 超时不影响截图

            # 等待 CSS 和关键图片加载
            await page.wait_for_timeout(1000)

            # 计算裁剪高度：扫描多个锚点元素，取最大 bottom 值
            clip_height = await page.evaluate("""() => {
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
                return Math.min(document.body.scrollHeight, 3000);
            }""")
            clip_height = max(800, min(clip_height, 3000))

            screenshot = await page.screenshot(
                type="png",
                clip={"x": 0, "y": 0, "width": 1280, "height": clip_height}
            )
            await page.close()
            return screenshot
        except Exception as e:
            logger.error(f"📸 Playwright 渲染异常 {asin}: {e}")
            # 浏览器可能崩溃，重置实例
            await self._close_browser()
            return None

    async def _close_browser(self):
        """安全关闭 Playwright 浏览器"""
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
            from curl_cffi import CurlMime
            url = f"{self.server_url}/api/tasks/screenshot"
            mp = CurlMime()
            mp.addpart(name="batch_name", data=batch_name)
            mp.addpart(name="asin", data=asin)
            mp.addpart(
                name="file",
                filename=f"{asin}.png",
                content_type="image/png",
                data=png_bytes,
            )
            resp = curl_requests.post(url, multipart=mp, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"📸 截图上传失败 {asin}: HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"📸 截图上传异常 {asin}: {e}")

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
        if self._session:
            await self._session.close()
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
