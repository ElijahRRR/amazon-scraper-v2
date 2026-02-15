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
                 zip_code: str = None):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.concurrency = concurrency or config.DEFAULT_CONCURRENCY
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE

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

    async def start(self):
        """启动 Worker"""
        logger.info(f"🚀 Worker [{self.worker_id}] 启动")
        logger.info(f"   服务器: {self.server_url}")
        logger.info(f"   并发数: {self.concurrency}")
        logger.info(f"   邮编: {self.zip_code}")

        self._running = True
        self._stats["start_time"] = time.time()

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
                    await self.proxy_manager.report_blocked()
                    await self._session.close()
                    await self._init_session()
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
                    await self.proxy_manager.report_blocked()
                    await self._session.close()
                    await self._init_session()
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

                title_short = result_data["title"][:40] if result_data["title"] else "N/A"
                logger.info(f"OK {asin} | {title_short}... | {result_data['current_price']}")
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

    async def _submit_result(self, task_id: int, result_data: Optional[Dict], success: bool):
        """提交采集结果到服务器"""
        try:
            url = f"{self.server_url}/api/tasks/result"
            payload = {
                "task_id": task_id,
                "worker_id": self.worker_id,
                "success": success,
                "result": result_data,
            }
            resp = curl_requests.post(
                url,
                json=payload,
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning(f"提交结果失败: HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"提交结果异常: {e}")

    async def _cleanup(self):
        """清理资源"""
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
    
    args = arg_parser.parse_args()

    worker = Worker(
        server_url=args.server,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        zip_code=args.zip_code,
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
