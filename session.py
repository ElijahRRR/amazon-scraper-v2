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
        self._init_lock = asyncio.Lock()
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

        带锁保护：多个协程同时调用时，只有第一个执行初始化，其余等待并复用结果
        """
        async with self._init_lock:
            # 已初始化 → 直接返回（被其他协程抢先完成了）
            if self._initialized:
                return True

            for init_attempt in range(3):
                try:
                    proxy = await self.proxy_manager.get_proxy()

                    # 创建会话（impersonate Chrome, HTTP/2 多路复用）
                    self._session = AsyncSession(
                        impersonate=config.IMPERSONATE_BROWSER,
                        timeout=config.REQUEST_TIMEOUT,
                        proxy=proxy,
                        max_clients=config.MAX_CLIENTS,
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

                    # 2. 设置邮编（带重试）
                    zip_ok = False
                    for zip_attempt in range(3):
                        if await self._set_zip_code():
                            zip_ok = True
                            break
                        logger.warning(f"📍 邮编设置失败 (尝试 {zip_attempt+1}/3)")
                        await asyncio.sleep(1)

                    if not zip_ok:
                        # 邮编设置 3 次全失败 → 放弃该 session，换代理重试
                        logger.warning(f"⚠️ 邮编设置 3 次全失败，放弃当前代理 (初始化 {init_attempt+1}/3)")
                        await self._session.close()
                        self._session = None
                        # 强制刷新代理（换一个出口 IP）
                        await self.proxy_manager.report_blocked()
                        await asyncio.sleep(2)
                        continue

                    # 3. 验证邮编是否生效（重新访问首页检查 location widget）
                    verified = await self._verify_zip_code()
                    if not verified:
                        logger.warning(f"⚠️ 邮编验证失败（页面未反映 {self.zip_code}），放弃当前代理 (初始化 {init_attempt+1}/3)")
                        await self._session.close()
                        self._session = None
                        await self.proxy_manager.report_blocked()
                        await asyncio.sleep(2)
                        continue

                    self._initialized = True
                    logger.info(f"✅ Session 初始化成功 (邮编: {self.zip_code})")
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
            if self._session is None:
                return False
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

    async def _verify_zip_code(self) -> bool:
        """
        验证邮编是否实际生效
        重新访问首页，检查 location widget 是否显示了正确的邮编
        防止代理 IP 导致 Amazon 忽略邮编设置
        """
        try:
            headers = self._build_headers(referer="https://www.amazon.com/")
            resp = await self._session.get(
                self.AMAZON_BASE,
                headers=headers,
            )
            if resp.status_code != 200:
                return False

            text = resp.text
            # 检查 location widget 中的邮编（glow-ingress-line2 显示当前配送地址）
            import re
            zip_match = re.search(r'id="glow-ingress-line2"[^>]*>\s*([^<]+)', text)
            if zip_match:
                location_text = zip_match.group(1).strip()
                if self.zip_code in location_text:
                    logger.info(f"📍 邮编验证通过: {location_text}")
                    return True
                else:
                    logger.warning(f"📍 邮编验证不匹配: 期望 {self.zip_code}, 页面显示 '{location_text}'")
                    return False

            # 备选：检查是否有非美国货币标识（CNY/¥/€/£）
            # 如果出现这些标识说明 session 没被定位到美国
            non_us_indicators = ['CNY', '¥', '€', '£', 'JP¥']
            for indicator in non_us_indicators:
                if indicator in text[:50000]:  # 只检查前半部分避免误匹配
                    logger.warning(f"📍 邮编验证失败: 页面包含非美国货币标识 '{indicator}'")
                    return False

            # 如果 widget 不存在但也没有非美国标识，放行
            logger.info(f"📍 邮编验证: 未找到 location widget，但无异常货币标识")
            return True

        except Exception as e:
            logger.error(f"📍 邮编验证异常: {e}")
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
            "sec-ch-ua": '"Chromium";v="133", "Google Chrome";v="133", "Not_A Brand";v="24"',
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

    async def fetch_aod_page(self, asin: str) -> Optional[Response]:
        """
        采集 AOD (All Offers Display) AJAX 页面
        响应体比产品页小 5-10 倍，包含卖家价格、运费、FBA状态
        返回: Response 对象 或 None
        """
        if not self._initialized:
            await self.initialize()

        if self._session is None:
            logger.warning(f"⚠️ Session 未就绪，跳过 AOD ASIN={asin}")
            return None

        url = f"{self.AMAZON_BASE}/gp/aod/ajax?asin={asin}&pc=dp&isonlyrenderofferlist=true"
        referer = f"{self.AMAZON_BASE}/dp/{asin}"
        headers = self._build_headers(referer=referer)
        # AOD 是 AJAX 请求，需要额外的头
        headers.update({
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Accept": "text/html,*/*",
        })

        try:
            resp = await self._session.get(
                url,
                headers=headers,
            )

            self._last_url = referer  # referer 保持为产品页
            self._request_count += 1

            return resp
        except Exception as e:
            logger.error(f"AOD 请求失败 ASIN={asin}: {e}")
            return None

    async def fetch_product_page(self, asin: str) -> Optional[Response]:
        """
        采集 Amazon 商品页面
        返回: Response 对象 或 None
        """
        if not self._initialized:
            await self.initialize()

        if self._session is None:
            logger.warning(f"⚠️ Session 未就绪，跳过 ASIN={asin}")
            return None

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

            # 检测空响应或过短响应（正常产品页至少 50KB）
            if resp.status_code == 200 and len(resp.content) < 1000:
                logger.warning(f"⚠️ ASIN={asin} 响应体过短 ({len(resp.content)} bytes)，视为空页面")
                return None

            return resp
        except Exception as e:
            logger.error(f"❌ 请求失败 ASIN={asin}: {e}")
            return None

    def is_blocked(self, response: Response) -> bool:
        """
        检测是否被 Amazon 封锁
        注意：response 为 None（超时）时由调用方单独处理，这里只检查实际响应
        注意：404 不算封锁（Amazon 标准 404 页面包含 api-services-support 注释）
        """
        if response is None:
            return False

        # 404 是正常的商品不存在，不是封锁
        if response.status_code == 404:
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

        # api-services-support 检测：只在短页面（非正常产品页）中检查
        # 正常产品页 > 50KB，被封的错误页面通常 < 10KB
        if "api-services-support@amazon.com" in text and len(text) < 20000:
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


