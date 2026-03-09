"""
独立截图进程：与采集 Worker 完全隔离的 asyncio 事件循环。

通信协议（基于文件系统）：
  screenshot_cache/html/{batch_name}/{asin}.html  — 采集 Worker 写入的 HTML
  screenshot_cache/html/{batch_name}/_scraping_done — 采集完成标记
  screenshot_cache/png/{batch_name}/{asin}.png     — 渲染后的截图
  screenshot_cache/html/{batch_name}/_uploaded      — 上传完成标记（通知主 Worker）

启动方式：由 worker.py 作为子进程启动，传入 server_url 参数。
"""

import asyncio
import json
import logging
import os
import re
import shutil
import sys
import time
from typing import Optional

import httpx

logger = logging.getLogger("screenshot_worker")


class ScreenshotWorker:
    def __init__(self, server_url: str, base_dir: str = None,
                 browsers_count: int = 2, pages_per_browser: int = 6):
        self.server_url = server_url
        self.base_dir = base_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "screenshot_cache"
        )
        self.html_dir = os.path.join(self.base_dir, "html")
        self.png_dir = os.path.join(self.base_dir, "png")
        self._browsers_count = browsers_count
        self._pages_per_browser = pages_per_browser
        self._concurrency = browsers_count * pages_per_browser
        self._browser_slots = []
        self._browser_lock = asyncio.Lock()
        self._browser_counter = 0
        self._running = True

    async def start(self):
        """主循环：扫描 HTML 目录，渲染截图，上传完成后写标记"""
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.png_dir, exist_ok=True)
        logger.info(f"📸 截图独立进程启动（并发: {self._concurrency}, 监控: {self.html_dir}）")

        try:
            while self._running:
                batches = self._scan_pending_batches()
                if not batches:
                    await asyncio.sleep(1)
                    continue

                for batch_name, html_files in batches.items():
                    await self._process_batch(batch_name, html_files)
        except KeyboardInterrupt:
            pass
        finally:
            await self._close_browsers()
            logger.info("📸 截图独立进程退出")

    def _scan_pending_batches(self) -> dict:
        """扫描有未处理 HTML 的批次"""
        batches = {}
        if not os.path.isdir(self.html_dir):
            return batches

        for batch_name in os.listdir(self.html_dir):
            batch_dir = os.path.join(self.html_dir, batch_name)
            if not os.path.isdir(batch_dir):
                continue
            # 已上传完成的批次跳过
            if os.path.exists(os.path.join(batch_dir, "_uploaded")):
                continue

            html_files = [f for f in os.listdir(batch_dir)
                          if f.endswith(".html") and not f.startswith("_")]
            # 找出尚未渲染的
            png_batch_dir = os.path.join(self.png_dir, batch_name)
            pending = []
            for f in html_files:
                asin = f[:-5]  # remove .html
                png_path = os.path.join(png_batch_dir, f"{asin}.png")
                if not os.path.exists(png_path):
                    pending.append(f)

            if pending:
                batches[batch_name] = pending
            elif os.path.exists(os.path.join(batch_dir, "_scraping_done")):
                # 所有 HTML 都已渲染，触发上传
                asyncio.ensure_future(self._upload_and_mark(batch_name))

        return batches

    async def _process_batch(self, batch_name: str, html_files: list):
        """并发渲染一个批次的截图"""
        logger.info(f"📸 开始渲染批次: {batch_name} ({len(html_files)} 张待处理)")
        sem = asyncio.Semaphore(self._concurrency)

        async def render_one(filename):
            asin = filename[:-5]
            async with sem:
                await self._render_and_save(batch_name, asin)

        tasks = [asyncio.create_task(render_one(f)) for f in html_files]
        await asyncio.gather(*tasks, return_exceptions=True)

        # 检查是否采集已完成 + 全部渲染完毕
        batch_html_dir = os.path.join(self.html_dir, batch_name)
        scraping_done = os.path.exists(os.path.join(batch_html_dir, "_scraping_done"))
        if scraping_done:
            # 再检查一次是否全部渲染完
            all_html = [f for f in os.listdir(batch_html_dir)
                        if f.endswith(".html") and not f.startswith("_")]
            png_batch_dir = os.path.join(self.png_dir, batch_name)
            all_done = all(
                os.path.exists(os.path.join(png_batch_dir, f"{f[:-5]}.png"))
                for f in all_html
            )
            if all_done:
                await self._upload_and_mark(batch_name)

    async def _render_and_save(self, batch_name: str, asin: str):
        """读取 HTML，渲染截图，保存 PNG"""
        html_path = os.path.join(self.html_dir, batch_name, f"{asin}.html")
        try:
            with open(html_path, "r", encoding="utf-8", errors="replace") as f:
                html_content = f.read()
        except FileNotFoundError:
            return

        png_bytes = await self._render_screenshot(html_content, asin)
        if png_bytes:
            png_dir = os.path.join(self.png_dir, batch_name)
            os.makedirs(png_dir, exist_ok=True)
            png_path = os.path.join(png_dir, f"{asin}.png")
            with open(png_path, "wb") as f:
                f.write(png_bytes)
            logger.info(f"📸 截图完成: {asin} ({len(png_bytes)} bytes)")
        else:
            # 渲染失败也标记为完成（写空 PNG 占位，避免无限重试）
            png_dir = os.path.join(self.png_dir, batch_name)
            os.makedirs(png_dir, exist_ok=True)
            png_path = os.path.join(png_dir, f"{asin}.png")
            with open(png_path, "wb") as f:
                f.write(b"")
            logger.warning(f"📸 截图渲染失败: {asin}")

    async def _upload_and_mark(self, batch_name: str):
        """上传批次所有截图到 Server，写 _uploaded 标记"""
        png_dir = os.path.join(self.png_dir, batch_name)
        if not os.path.isdir(png_dir):
            return

        files = [f for f in os.listdir(png_dir) if f.endswith(".png")]
        valid_files = []
        for f in files:
            fpath = os.path.join(png_dir, f)
            if os.path.getsize(fpath) > 0:
                valid_files.append(f)

        if valid_files:
            logger.info(f"📸 开始上传截图: {batch_name} ({len(valid_files)} 张)")
            uploaded = 0
            failed = 0
            async with httpx.AsyncClient(timeout=30) as client:
                for fname in valid_files:
                    asin = fname[:-4]
                    fpath = os.path.join(png_dir, fname)
                    try:
                        with open(fpath, "rb") as f:
                            png_bytes = f.read()
                        resp = await client.post(
                            f"{self.server_url}/api/tasks/screenshot",
                            files={"file": (fname, png_bytes, "image/png")},
                            data={"batch_name": batch_name, "asin": asin},
                        )
                        if resp.status_code == 200:
                            uploaded += 1
                        else:
                            failed += 1
                            logger.warning(f"截图上传失败 {asin}: HTTP {resp.status_code}")
                    except Exception as e:
                        failed += 1
                        logger.error(f"截图上传异常 {asin}: {e}")
            logger.info(f"📸 上传完成: {batch_name} (成功 {uploaded}, 失败 {failed})")

        # 写 _uploaded 标记，通知主 Worker
        uploaded_marker = os.path.join(self.html_dir, batch_name, "_uploaded")
        with open(uploaded_marker, "w") as f:
            f.write(str(time.time()))
        logger.info(f"📸 批次完成标记已写入: {batch_name}")

        # 清理 HTML 和 PNG 暂存
        html_batch_dir = os.path.join(self.html_dir, batch_name)
        shutil.rmtree(html_batch_dir, ignore_errors=True)
        shutil.rmtree(png_dir, ignore_errors=True)

    async def _render_screenshot(self, html_content: str, asin: str) -> Optional[bytes]:
        """Playwright 渲染截图"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("📸 playwright 未安装，跳过截图渲染")
            return None

        page = None
        try:
            # 懒初始化浏览器池
            if not self._browser_slots:
                async with self._browser_lock:
                    if not self._browser_slots:
                        for i in range(self._browsers_count):
                            pw = await async_playwright().__aenter__()
                            browser = await pw.chromium.launch(
                                headless=True,
                                args=["--disable-gpu", "--disable-dev-shm-usage",
                                      "--no-sandbox", "--disable-extensions"]
                            )
                            self._browser_slots.append({"playwright": pw, "browser": browser})
                        logger.info(f"📸 浏览器池启动（{self._browsers_count} 实例）")

            idx = self._browser_counter % len(self._browser_slots)
            self._browser_counter += 1
            browser = self._browser_slots[idx]["browser"]
            page = await browser.new_page(viewport={"width": 1280, "height": 1300})

            # 屏蔽无关资源
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

            # 注入 <base> 标签
            base_tag = '<base href="https://www.amazon.com/">'
            lower_head = html_content[:2000].lower()
            if "<base " not in lower_head:
                head_pos = lower_head.find("<head")
                if head_pos != -1:
                    close_pos = html_content.index(">", head_pos) + 1
                    html_content = html_content[:close_pos] + base_tag + html_content[close_pos:]
                else:
                    html_content = base_tag + html_content

            try:
                await page.set_content(
                    html_content,
                    wait_until="domcontentloaded",
                    timeout=5000,
                )
            except Exception:
                pass

            # 检查页面可见内容
            has_content = await page.evaluate("""() => {
                if (!document.body) return false;
                const text = document.body.innerText || '';
                if (text.trim().length > 50) return true;
                const imgs = document.querySelectorAll('img[src]');
                if (imgs.length > 0) return true;
                return false;
            }""")

            screenshot = await page.screenshot(
                type="png",
                clip={"x": 0, "y": 0, "width": 1280, "height": 1300}
            )

            if len(screenshot) < 10240 and not has_content:
                logger.warning(f"📸 空白截图已丢弃: {asin} ({len(screenshot)} bytes)")
                return None

            return screenshot
        except Exception as e:
            err_msg = str(e)
            if "browser has been closed" in err_msg or "Target closed" in err_msg:
                logger.error(f"📸 浏览器崩溃，将重启: {asin}")
                await self._close_browsers()
            else:
                logger.warning(f"📸 渲染失败 {asin}: {e}")
            return None
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    async def _close_browsers(self):
        """关闭所有浏览器"""
        async with self._browser_lock:
            for slot in self._browser_slots:
                try:
                    await slot["browser"].close()
                except Exception:
                    pass
                try:
                    await slot["playwright"].stop()
                except Exception:
                    pass
            self._browser_slots.clear()


def main():
    """入口：python screenshot_worker.py <server_url> [browsers_count] [pages_per_browser]"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [SCREENSHOT] %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) < 2:
        print("Usage: python screenshot_worker.py <server_url> [browsers_count] [pages_per_browser]")
        sys.exit(1)

    server_url = sys.argv[1].rstrip("/")
    browsers_count = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    pages_per_browser = int(sys.argv[3]) if len(sys.argv) > 3 else 6

    worker = ScreenshotWorker(
        server_url=server_url,
        browsers_count=browsers_count,
        pages_per_browser=pages_per_browser,
    )
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
