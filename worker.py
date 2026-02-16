"""
Amazon 产品采集系统 v2 - Worker 采集引擎
连接中央服务器 API 拉取任务、推送结果
每个 worker 维护独立 session
严格 5次/s 限速（200ms ± 50ms 随机抖动）
被封检测 → 换 IP + 换 session 重试，最多 3 次
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

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class Worker:
    """异步采集 Worker"""

    def __init__(self, server_url: str, worker_id: str = None, concurrency: int = None,
                 zip_code: str = None, fast_mode: bool = False):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.concurrency = concurrency or config.DEFAULT_CONCURRENCY
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self.fast_mode = fast_mode  # 快速模式: AOD 优先获取价格

        # 组件
        self.proxy_manager = get_proxy_manager()
        self.parser = AmazonParser()
        self._session: Optional[AmazonSession] = None

        # 速率控制
        self._interval = config.REQUEST_INTERVAL
        self._jitter = config.REQUEST_JITTER
        self._semaphore = asyncio.Semaphore(self.concurrency)

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
        self._result_queue: asyncio.Queue = None  # 在 start() 中初始化
        self._batch_submitter_task: Optional[asyncio.Task] = None
        self._batch_size = 10
        self._batch_interval = 2.0  # 秒

        # Session 轮换控制
        self._success_since_rotate = 0
        self._rotate_every = config.SESSION_ROTATE_EVERY
        self._rotate_lock = asyncio.Lock()

    async def start(self):
        """启动 Worker"""
        logger.info(f"🚀 Worker [{self.worker_id}] 启动")
        logger.info(f"   服务器: {self.server_url}")
        logger.info(f"   并发数: {self.concurrency}")
        logger.info(f"   邮编: {self.zip_code}")
        logger.info(f"   快速模式: {'开启 (AOD优先)' if self.fast_mode else '关闭'}")

        self._running = True
        self._stats["start_time"] = time.time()

        # 初始化批量提交队列和后台任务
        self._result_queue = asyncio.Queue()
        self._batch_submitter_task = asyncio.create_task(self._batch_submitter())

        # 初始化 session
        await self._init_session()

        # 主循环：持续拉取和处理任务
        while self._running:
            try:
                tasks = await self._pull_tasks()
                if not tasks:
                    logger.info("📭 暂无任务，等待 5 秒...")
                    await asyncio.sleep(5)
                    continue

                logger.info(f"📋 拉取到 {len(tasks)} 个任务")

                # 并发处理任务
                sem_tasks = [self._process_with_semaphore(task) for task in tasks]
                await asyncio.gather(*sem_tasks, return_exceptions=True)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"❌ 主循环异常: {e}")
                await asyncio.sleep(3)

        await self._cleanup()
        logger.info(f"🛑 Worker [{self.worker_id}] 已停止")
        self._print_stats()

    async def stop(self):
        """停止 Worker"""
        self._running = False

    async def _init_session(self):
        """初始化 Amazon session"""
        logger.info("🔧 初始化 Amazon session...")
        self._session = AmazonSession(self.proxy_manager, self.zip_code)
        success = await self._session.initialize()
        if not success:
            logger.warning("⚠️ Session 初始化失败，将在首次请求时重试")
        self._success_since_rotate = 0

    async def _rotate_session(self, reason: str = "主动轮换"):
        """轮换 session：关闭旧的，刷新代理，创建新的"""
        async with self._rotate_lock:
            logger.info(f"🔄 Session {reason}...")
            if self._session:
                await self._session.close()
            await self.proxy_manager.report_blocked()
            await asyncio.sleep(1)
            self._session = AmazonSession(self.proxy_manager, self.zip_code)
            success = await self._session.initialize()
            self._success_since_rotate = 0
            if success:
                logger.info("🔄 Session 轮换成功")
            else:
                logger.warning("⚠️ Session 轮换后初始化失败")

    async def _pull_tasks(self) -> List[Dict]:
        """从服务器拉取任务"""
        try:
            url = f"{self.server_url}/api/tasks/pull"
            params = {
                "worker_id": self.worker_id,
                "count": self.concurrency,
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

    async def _process_with_semaphore(self, task: Dict):
        """带信号量的任务处理（控制并发）"""
        async with self._semaphore:
            await self._process_task(task)

    async def _process_task(self, task: Dict):
        """
        处理单个采集任务
        区分超时和真正的封锁：
        - 超时/网络错误 → 等待后直接重试（不换 IP）
        - 验证码/403/503 → 换 IP + 换 session → 重试
        """
        asin = task["asin"]
        task_id = task["id"]
        zip_code = task.get("zip_code", self.zip_code)
        max_retries = config.MAX_RETRIES

        for attempt in range(max_retries):
            try:
                # 速率控制：200ms ± 50ms 随机抖动
                delay = self._interval + random.uniform(-self._jitter, self._jitter)
                await asyncio.sleep(delay)

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
                        return

                # 发起请求
                resp = await self._session.fetch_product_page(asin)

                # 请求失败（超时/网络异常）→ 不换 IP，等待后重试
                if resp is None:
                    logger.warning(f"ASIN {asin} 请求超时 (尝试 {attempt+1}/{max_retries})")
                    await asyncio.sleep(2)
                    continue

                # 真正被封（403/503/验证码）→ 换 IP + 换 session
                if self._session.is_blocked(resp):
                    self._stats["blocked"] += 1
                    logger.warning(f"ASIN {asin} 被封 HTTP {resp.status_code} (尝试 {attempt+1}/{max_retries})")
                    await self._rotate_session(reason="被封锁")
                    continue

                # 404 处理
                if self._session.is_404(resp):
                    logger.info(f"ASIN {asin} 商品不存在 (404)")
                    result_data = self.parser._default_result(asin, zip_code)
                    result_data["title"] = "[商品不存在]"
                    result_data["batch_name"] = task.get("batch_name", "")
                    await self._submit_result(task_id, result_data, success=True)
                    self._stats["success"] += 1
                    self._stats["total"] += 1
                    return

                # 解析页面
                result_data = self.parser.parse_product(resp.text, asin, zip_code)
                result_data["batch_name"] = task.get("batch_name", "")

                # 检查是否是拦截页面
                if result_data["title"] in ["[验证码拦截]", "[API封锁]"]:
                    self._stats["blocked"] += 1
                    logger.warning(f"ASIN {asin} {result_data['title']} (尝试 {attempt+1}/{max_retries})")
                    await self._rotate_session(reason="页面拦截")
                    continue

                # 标题为空视为软拦截
                if not result_data["title"] or result_data["title"] == "N/A":
                    logger.warning(f"ASIN {asin} 标题为空 (尝试 {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                        continue

                # 成功
                await self._submit_result(task_id, result_data, success=True)
                self._stats["success"] += 1
                self._stats["total"] += 1
                self._success_since_rotate += 1

                title_short = result_data["title"][:40] if result_data["title"] else "N/A"
                logger.info(f"OK {asin} | {title_short}... | {result_data['current_price']}")

                # 主动轮换：每 N 次成功请求更换 session 防止被检测
                if self._success_since_rotate >= self._rotate_every:
                    await self._rotate_session(reason=f"主动轮换 (已完成 {self._success_since_rotate} 次)")

                return

            except Exception as e:
                logger.error(f"ASIN {asin} 异常 (尝试 {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    continue

        # 所有重试用完，标记失败
        logger.error(f"ASIN {asin} 采集失败 (已重试 {max_retries} 次)")
        await self._submit_result(task_id, None, success=False)
        self._stats["failed"] += 1
        self._stats["total"] += 1

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

    async def _submit_result(self, task_id: int, result_data: Optional[Dict], success: bool):
        """将结果放入批量提交队列"""
        payload = {
            "task_id": task_id,
            "worker_id": self.worker_id,
            "success": success,
            "result": result_data,
        }
        await self._result_queue.put(payload)

    async def _batch_submitter(self):
        """后台协程：每攒够 batch_size 个或每 batch_interval 秒批量提交"""
        batch: List[Dict] = []
        while self._running or not self._result_queue.empty():
            try:
                # 等待队列中的数据，最多等 batch_interval 秒
                try:
                    item = await asyncio.wait_for(
                        self._result_queue.get(), timeout=self._batch_interval
                    )
                    batch.append(item)
                except asyncio.TimeoutError:
                    pass

                # 快速排空队列中已有的数据
                while not self._result_queue.empty() and len(batch) < self._batch_size:
                    batch.append(self._result_queue.get_nowait())

                # 达到批量大小或超时且有数据 → 提交
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

    async def _cleanup(self):
        """清理资源"""
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

    def _print_stats(self):
        """打印统计信息"""
        elapsed = time.time() - self._stats["start_time"] if self._stats["start_time"] else 0
        total = self._stats["total"]
        success = self._stats["success"]
        rate = success / total * 100 if total > 0 else 0
        speed = total / elapsed * 60 if elapsed > 0 else 0
        
        logger.info("=" * 50)
        logger.info(f"📊 Worker [{self.worker_id}] 统计")
        logger.info(f"   总采集: {total}")
        logger.info(f"   成功: {success} ({rate:.1f}%)")
        logger.info(f"   失败: {self._stats['failed']}")
        logger.info(f"   被封: {self._stats['blocked']}")
        logger.info(f"   速度: {speed:.1f} 条/分钟")
        logger.info(f"   耗时: {elapsed:.0f} 秒")
        logger.info("=" * 50)


def main():
    """Worker 入口"""
    arg_parser = argparse.ArgumentParser(description="Amazon Scraper Worker")
    arg_parser.add_argument("--server", required=True, help="中央服务器地址 (如 http://192.168.1.100:8899)")
    arg_parser.add_argument("--worker-id", default=None, help="Worker ID（默认自动生成）")
    arg_parser.add_argument("--concurrency", type=int, default=None, help=f"并发数（默认 {config.DEFAULT_CONCURRENCY}）")
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
