"""
采集性能基准测试脚本
自动化测试不同配置组合，记录速度和成功率
"""
import asyncio
import time
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from database import Database
from proxy import ProxyManager, get_proxy_manager
from session import AmazonSession, SessionPool
from parser import AmazonParser

import logging
logging.basicConfig(
    level=logging.WARNING,  # 只显示警告以上，减少噪音
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("benchmark")
logger.setLevel(logging.INFO)


async def run_benchmark(
    asins: list,
    concurrency: int,
    interval: float,
    jitter: float,
    session_count: int,
    label: str,
):
    """
    运行单次基准测试
    返回: {label, concurrency, interval, session_count, total, success, failed, blocked, elapsed, speed, success_rate}
    """
    import random

    proxy_mgr = get_proxy_manager()
    parser = AmazonParser()

    # 创建 session 池
    sessions = []
    for i in range(session_count):
        s = AmazonSession(proxy_mgr, config.DEFAULT_ZIP_CODE)
        ok = await s.initialize()
        if ok:
            sessions.append(s)
            logger.info(f"  Session {i+1}/{session_count} 初始化成功")
        else:
            logger.warning(f"  Session {i+1}/{session_count} 初始化失败")
        if i < session_count - 1:
            await asyncio.sleep(0.5)

    if not sessions:
        logger.error("  无可用 session，跳过测试")
        return None

    # 统计
    stats = {"success": 0, "failed": 0, "blocked": 0, "timeout": 0}
    sem = asyncio.Semaphore(concurrency)
    session_idx = [0]  # 用列表实现轮换索引

    async def process_one(asin):
        async with sem:
            delay = interval + random.uniform(-jitter, jitter)
            if delay > 0:
                await asyncio.sleep(delay)

            # 轮换 session
            s = sessions[session_idx[0] % len(sessions)]
            session_idx[0] += 1

            for attempt in range(2):  # 最多重试 1 次
                resp = await s.fetch_product_page(asin)

                if resp is None:
                    stats["timeout"] += 1
                    await asyncio.sleep(1)
                    continue

                if s.is_blocked(resp):
                    stats["blocked"] += 1
                    await asyncio.sleep(2)
                    continue

                if s.is_404(resp):
                    stats["success"] += 1
                    return

                result = parser.parse_product(resp.text, asin)
                if result["title"] in ["[验证码拦截]", "[API封锁]"]:
                    stats["blocked"] += 1
                    await asyncio.sleep(2)
                    continue

                stats["success"] += 1
                return

            stats["failed"] += 1

    # 执行
    logger.info(f"\n{'='*60}")
    logger.info(f"  测试: {label}")
    logger.info(f"  并发={concurrency}, 间隔={interval}s, session数={session_count}, ASIN数={len(asins)}")
    logger.info(f"{'='*60}")

    start = time.time()
    tasks = [process_one(a) for a in asins]
    await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.time() - start

    # 清理 session
    for s in sessions:
        await s.close()

    total = stats["success"] + stats["failed"]
    speed = total / elapsed * 60 if elapsed > 0 else 0
    success_rate = stats["success"] / total * 100 if total > 0 else 0

    result = {
        "label": label,
        "concurrency": concurrency,
        "interval": interval,
        "session_count": session_count,
        "total": total,
        "success": stats["success"],
        "failed": stats["failed"],
        "blocked": stats["blocked"],
        "timeout": stats["timeout"],
        "elapsed": round(elapsed, 1),
        "speed": round(speed, 1),
        "success_rate": round(success_rate, 1),
    }

    logger.info(f"  结果: {stats['success']}/{total} 成功 ({success_rate:.1f}%)")
    logger.info(f"  速度: {speed:.1f} 条/分钟")
    logger.info(f"  耗时: {elapsed:.1f} 秒")
    logger.info(f"  超时: {stats['timeout']}, 封锁: {stats['blocked']}")

    return result


async def main():
    # 加载测试 ASIN
    batches = {}
    for i in range(1, 6):
        fn = f"/tmp/test_batch_{i}.txt"
        with open(fn) as f:
            batches[i] = [line.strip() for line in f if line.strip()]

    results = []

    # ============================================================
    # 测试 1: 基准线 — concurrency=5, interval=0.2, 1 session
    # ============================================================
    r = await run_benchmark(
        asins=batches[1],
        concurrency=5,
        interval=0.2,
        jitter=0.05,
        session_count=1,
        label="A: 基准 (c=5, i=0.2, s=1)",
    )
    if r:
        results.append(r)

    # ============================================================
    # 测试 2: 提高并发到 10
    # ============================================================
    r = await run_benchmark(
        asins=batches[2],
        concurrency=10,
        interval=0.1,
        jitter=0.03,
        session_count=1,
        label="B: 高并发 (c=10, i=0.1, s=1)",
    )
    if r:
        results.append(r)

    # ============================================================
    # 测试 3: 高并发 + 多 session
    # ============================================================
    r = await run_benchmark(
        asins=batches[3],
        concurrency=10,
        interval=0.1,
        jitter=0.03,
        session_count=3,
        label="C: 高并发+多session (c=10, i=0.1, s=3)",
    )
    if r:
        results.append(r)

    # ============================================================
    # 测试 4: 极限并发
    # ============================================================
    r = await run_benchmark(
        asins=batches[4],
        concurrency=20,
        interval=0.05,
        jitter=0.02,
        session_count=3,
        label="D: 极限并发 (c=20, i=0.05, s=3)",
    )
    if r:
        results.append(r)

    # ============================================================
    # 测试 5: 平衡配置
    # ============================================================
    r = await run_benchmark(
        asins=batches[5],
        concurrency=15,
        interval=0.05,
        jitter=0.02,
        session_count=2,
        label="E: 平衡配置 (c=15, i=0.05, s=2)",
    )
    if r:
        results.append(r)

    # ============================================================
    # 汇总
    # ============================================================
    logger.info(f"\n{'='*80}")
    logger.info("  性能测试汇总")
    logger.info(f"{'='*80}")
    logger.info(f"{'配置':<40} {'速度':>8} {'成功率':>8} {'耗时':>8} {'封锁':>6}")
    logger.info("-" * 80)
    for r in results:
        logger.info(
            f"{r['label']:<40} {r['speed']:>6.1f}/m {r['success_rate']:>6.1f}% {r['elapsed']:>6.1f}s {r['blocked']:>5}"
        )
    logger.info(f"{'='*80}")

    # 保存结果
    with open("/Users/nextderboy/Projects/amazon-scraper-v2/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("结果已保存到 benchmark_results.json")


if __name__ == "__main__":
    asyncio.run(main())
