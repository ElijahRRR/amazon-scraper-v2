"""
Amazon 产品采集系统 v2 - Session 管理模块
使用 curl_cffi 模拟浏览器 TLS 指纹
正确实现邮编设置（POST 到 address-change.html）
Cookie jar 管理
"""
import asyncio
import random
import re
import logging
import time
from typing import Optional, Dict, Any

from curl_cffi.requests import AsyncSession, Response

import config
from proxy import ProxyManager

logger = logging.getLogger(__name__)


class AmazonSession:
    """
    Amazon 会话管理器
    每个实例维护独立的 cookie jar 和 session
    """

    AMAZON_BASE = "https://www.amazon.com"
    ZIP_CHANGE_URL = "https://www.amazon.com/gp/delivery/ajax/address-change.html"

    def __init__(self, proxy_manager: ProxyManager, zip_code: str = None):
        self.proxy_manager = proxy_manager
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self._session: Optional[AsyncSession] = None
        self._initialized = False
        self._request_count = 0
        self._last_url: Optional[str] = None
        # 随机选择 User-Agent
        self._user_agent = random.choice(config.USER_AGENTS)
        # 根据 UA 选择平台
        if "Windows" in self._user_agent:
            self._platform = '"Windows"'
        elif "Macintosh" in self._user_agent:
            self._platform = '"macOS"'
        else:
            self._platform = '"Linux"'

    async def initialize(self) -> bool:
        """
        初始化 session：
        1. 创建 curl_cffi 会话
        2. 访问 Amazon 首页获取 cookies（带重试）
        3. POST 设置邮编
        """
        for init_attempt in range(3):
            try:
                proxy = await self.proxy_manager.get_proxy()

                # 创建会话（impersonate Chrome, HTTP/2 多路复用）
                self._session = AsyncSession(
                    impersonate=config.IMPERSONATE_BROWSER,
                    timeout=config.REQUEST_TIMEOUT,
                    proxy=proxy,
                    max_clients=config.DEFAULT_CONCURRENCY,
                    http_version=2,
                )

                # 1. 访问首页获取初始 cookies
                headers = self._build_headers()
                resp = await self._session.get(
                    self.AMAZON_BASE,
                    headers=headers,
                )

                # 接受所有 2xx 响应（200/202 等都有效）
                if resp.status_code >= 300:
                    logger.warning(f"首页返回 {resp.status_code}，重试 ({init_attempt+1}/3)")
                    await self._session.close()
                    self._session = None
                    await asyncio.sleep(3)
                    continue

                # 2. 设置邮编
                success = await self._set_zip_code()
                if success:
                    self._initialized = True
                    logger.info(f"✅ Session 初始化成功 (邮编: {self.zip_code})")
                else:
                    self._initialized = True
                    logger.warning(f"⚠️ 邮编设置失败，但 session 仍可使用")

                return True

            except Exception as e:
                logger.error(f"❌ Session 初始化失败 (尝试 {init_attempt+1}/3): {e}")
                if self._session:
                    await self._session.close()
                    self._session = None
                if init_attempt < 2:
                    await asyncio.sleep(3)
                    continue

        logger.error("❌ Session 初始化失败，已重试 3 次")
        return False

    async def _set_zip_code(self) -> bool:
        """
        通过 POST 请求设置配送邮编
        这是正确的邮编设置方式（而非伪造 cookie）
        """
        try:
            # 从首页 cookie 中提取 csrf token
            cookies = self._session.cookies
            session_id = None
            for cookie in cookies.jar:
                if cookie.name == "session-id":
                    session_id = cookie.value
                    break

            # 构建邮编设置请求
            headers = self._build_headers()
            headers.update({
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.amazon.com/",
                "Origin": "https://www.amazon.com",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "anti-csrftoken-a2z": cookies.get("csm-hit", ""),
            })

            data = {
                "locationType": "LOCATION_INPUT",
                "zipCode": self.zip_code,
                "storeContext": "generic",
                "deviceType": "web",
                "pageType": "Gateway",
                "actionSource": "glow",
            }

            resp = await self._session.post(
                self.ZIP_CHANGE_URL,
                headers=headers,
                data=data,
            )

            if resp.status_code == 200:
                try:
                    result = resp.json()
                    # Amazon 返回 {"isValidAddress": 1} 表示成功
                    if result.get("isValidAddress") == 1:
                        logger.info(f"📍 邮编设置成功: {self.zip_code}")
                        return True
                    else:
                        logger.warning(f"📍 邮编设置响应: {result}")
                        return False
                except Exception:
                    # 即使解析失败，200 状态码也算部分成功
                    logger.info(f"📍 邮编设置请求已发送 (200)")
                    return True
            else:
                logger.warning(f"📍 邮编设置返回 {resp.status_code}")
                return False

        except Exception as e:
            logger.error(f"📍 邮编设置异常: {e}")
            return False

    def _build_headers(self, referer: str = None) -> Dict[str, str]:
        """
        构建反指纹请求头
        按照真实浏览器的请求头顺序排列
        """
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "User-Agent": self._user_agent,
            "Upgrade-Insecure-Requests": "1",
            "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": self._platform,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        
        # Referer 链：第一次请求不带 referer，后续带上一个请求的 URL
        if referer:
            headers["Referer"] = referer
            headers["Sec-Fetch-Site"] = "same-origin"
        elif self._last_url:
            headers["Referer"] = self._last_url
            headers["Sec-Fetch-Site"] = "same-origin"

        return headers

    async def fetch_product_page(self, asin: str) -> Optional[Response]:
        """
        采集 Amazon 商品页面
        返回: Response 对象 或 None
        """
        if not self._initialized:
            await self.initialize()

        url = f"{self.AMAZON_BASE}/dp/{asin}"
        referer = self._last_url or f"{self.AMAZON_BASE}/"
        headers = self._build_headers(referer=referer)

        try:
            resp = await self._session.get(
                url,
                headers=headers,
            )
            
            self._last_url = url
            self._request_count += 1
            
            return resp
        except Exception as e:
            logger.error(f"❌ 请求失败 ASIN={asin}: {e}")
            return None

    def is_blocked(self, response: Response) -> bool:
        """
        检测是否被 Amazon 封锁
        注意：response 为 None（超时）时由调用方单独处理，这里只检查实际响应
        """
        if response is None:
            return False

        # HTTP 状态码检测
        if response.status_code in (403, 503):
            return True

        # 验证码检测
        text = response.text
        if "captcha" in response.url.lower():
            return True
        if "validateCaptcha" in text or "Robot Check" in text:
            return True
        if "api-services-support@amazon.com" in text:
            return True

        return False

    def is_404(self, response: Response) -> bool:
        """检测商品是否不存在"""
        return response.status_code == 404

    async def close(self):
        """关闭会话"""
        if self._session:
            await self._session.close()
            self._session = None
            self._initialized = False

    @property
    def stats(self) -> Dict:
        """获取会话统计"""
        return {
            "initialized": self._initialized,
            "zip_code": self.zip_code,
            "request_count": self._request_count,
            "user_agent": self._user_agent[:50] + "...",
        }


class SessionPool:
    """
    Session 池
    管理多个 AmazonSession 实例，支持轮换
    """

    def __init__(self, proxy_manager: ProxyManager, pool_size: int = 3, zip_code: str = None):
        self.proxy_manager = proxy_manager
        self.pool_size = pool_size
        self.zip_code = zip_code or config.DEFAULT_ZIP_CODE
        self._sessions: list = []
        self._index = 0

    async def initialize(self):
        """初始化所有 session"""
        for i in range(self.pool_size):
            session = AmazonSession(self.proxy_manager, self.zip_code)
            success = await session.initialize()
            if success:
                self._sessions.append(session)
                logger.info(f"✅ Session {i+1}/{self.pool_size} 初始化成功")
            else:
                logger.warning(f"⚠️ Session {i+1}/{self.pool_size} 初始化失败")
            
            # 各 session 初始化之间加入延迟
            if i < self.pool_size - 1:
                await asyncio.sleep(1.0)

    def get_session(self) -> Optional[AmazonSession]:
        """获取下一个 session（轮换）"""
        if not self._sessions:
            return None
        session = self._sessions[self._index % len(self._sessions)]
        self._index += 1
        return session

    async def replace_session(self, old_session: AmazonSession) -> Optional[AmazonSession]:
        """
        替换被封锁的 session
        关闭旧的，创建新的
        """
        try:
            idx = self._sessions.index(old_session)
        except ValueError:
            idx = -1

        await old_session.close()

        # 创建新 session
        new_session = AmazonSession(self.proxy_manager, self.zip_code)
        success = await new_session.initialize()
        
        if success:
            if idx >= 0:
                self._sessions[idx] = new_session
            else:
                self._sessions.append(new_session)
            logger.info("🔄 Session 替换成功")
            return new_session
        else:
            if idx >= 0:
                self._sessions.pop(idx)
            logger.error("❌ Session 替换失败")
            return None

    async def close_all(self):
        """关闭所有 session"""
        for session in self._sessions:
            await session.close()
        self._sessions.clear()
