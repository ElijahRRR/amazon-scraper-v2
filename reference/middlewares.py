import base64
import requests
import logging
import random
import sys
import os
from scrapy.exceptions import IgnoreRequest

# 确保 config 目录在 Python 路径中
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

try:
    from config.secrets import KDL_API_URL
except ImportError:
    # 如果导入失败，使用空字符串作为默认值
    KDL_API_URL = ""
    logging.warning("⚠️ 无法导入 config.secrets，请确保 KDL_API_URL 已配置")


# 1. 随机 User-Agent 中间件
class RandomUserAgentMiddleware:
    def __init__(self):
        self.ua_list = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

    def process_request(self, request, spider):
        ua = random.choice(self.ua_list)
        request.headers['User-Agent'] = ua


# 2. 邮编定位与会话保持中间件
class ZipCodeCookieMiddleware:
    def process_request(self, request, spider):
        request.meta['dont_merge_cookies'] = True

        # 1. 模拟更底层、更真实的亚马逊定位令牌
        # 关键在于 'address-state' 和 'ab-main' 这种内部校验字段
        session_id = f"{random.randint(100, 999)}-{random.randint(1000000, 9999999)}-{random.randint(1000000, 9999999)}"

        cookies = {
            'i18n-prefs': 'USD',
            'lc-main': 'en_US',
            'session-id': session_id,
            'session-id-time': '2082787201l',
            'ubid-main': f"{random.randint(100, 999)}-{random.randint(1000000, 9999999)}-{random.randint(1000000, 9999999)}",
            'sp-cdn': '"L5Z9:US"',
            # 2. 模拟点击"Deliver to"按钮后生成的持久化字段
            'skin': 'noskin',
            'p13n-sc-ua': '{"zipCode":"10001","locationType":"ZIP_CODE"}',
            # 3. 这是一个极关键的隐藏字段，亚马逊用它来记录用户的手动地理位置选择
            'address-state': 'NY',
            'international_shipping_checkout_p13n_zip_code': '10001'
        }

        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
        request.headers['Cookie'] = cookie_str

        # 4. 增加一个特定的 Header，告诉亚马逊我们接受美国英语的内容
        request.headers['Accept-Language'] = 'en-US,en;q=0.9'


# 3. 动态代理中间件 (适配快代理 TPS 隧道模式)
class DynamicProxyMiddleware:
    def __init__(self):
        self.logger = logging.getLogger('Proxy')
        # 你的快代理 TPS 提取 API 链接
        self.api_url = KDL_API_URL
        self.current_proxy = None
        self.proxy_auth = None
        self.last_fetch_time = 0
        self.request_count = 0  # 请求计数，用于控制代理刷新频率
        
        # 检测代理是否已配置
        self.proxy_enabled = bool(self.api_url) and "YOUR_ID" not in self.api_url
        if not self.proxy_enabled:
            self.logger.warning("⚠️ [代理未配置] KDL_API_URL 为空或为示例值，将以无代理模式运行")

    def get_proxy(self, force=False):
        """获取代理，force=True 时强制刷新"""
        import time
        
        # 非强制刷新时，设置 API 提取间隔，建议 8-10 秒，防止请求过频被封 API
        if not force and time.time() - self.last_fetch_time < 8:
            return self.current_proxy

        try:
            self.logger.info(">>> 正在请求快代理 API 获取新 IP...")
            resp = requests.get(self.api_url, timeout=10)
            if resp.status_code == 200:
                proxy_info = resp.text.strip()
                # 简单验证返回格式是否包含冒号且不是错误 JSON
                if ":" in proxy_info and "{" not in proxy_info:
                    parts = proxy_info.split(':')
                    # 情况 A: 包含用户名密码认证 (host:port:user:pwd)
                    if len(parts) == 4:
                        host, port, user, pwd = parts
                        self.current_proxy = f"http://{host}:{port}"
                        auth_str = f"{user}:{pwd}"
                        auth_b64 = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
                        self.proxy_auth = f"Basic {auth_b64}"
                    # 情况 B: 仅包含 IP:Port (已绑定白名单模式)
                    else:
                        self.current_proxy = f"http://{proxy_info}"
                        self.proxy_auth = None

                    self.last_fetch_time = time.time()
                    self.logger.info(f">>>> 成功更新代理: {self.current_proxy}")
                    return self.current_proxy
                else:
                    self.logger.warning(f"API 返回数据异常: {proxy_info}")
        except Exception as e:
            self.logger.error(f"获取代理失败: {e}")
        return None

    def process_request(self, request, spider):
        # 如果代理未配置，直接跳过
        if not self.proxy_enabled:
            return
        
        # 确保请求发起前已有可用代理
        if not self.current_proxy:
            self.get_proxy()

        if self.current_proxy:
            request.meta['proxy'] = self.current_proxy
            # 如果存在认证信息，注入 Proxy-Authorization 头部
            if self.proxy_auth:
                request.headers['Proxy-Authorization'] = self.proxy_auth

            # 注入额外的现代浏览器指纹头部，降低亚马逊 TLS 拦截率
            request.headers['Connection'] = 'keep-alive'
            request.headers['sec-ch-ua'] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
            request.headers['sec-ch-ua-mobile'] = '?0'
            request.headers['sec-ch-ua-platform'] = '"Windows"'

    def process_response(self, request, response, spider):
        # 无代理模式下直接返回响应
        if not self.proxy_enabled:
            return response
        
        # 亚马逊反爬处理：检查 URL 和页面内容
        is_blocked = False
        
        # URL 检测
        if "captcha" in response.url.lower():
            is_blocked = True
        
        # 状态码检测
        if response.status in [403, 503]:
            is_blocked = True
        
        # 页面内容检测 (更智能的拦截识别)
        if not is_blocked and response.status == 200:
            # 只检查前 5000 字符，提高效率
            page_start = response.text[:5000] if len(response.text) > 5000 else response.text
            if "validateCaptcha" in page_start or "Robot Check" in page_start:
                is_blocked = True

        if is_blocked:
            self.logger.warning(f"遭遇拦截 (状态码: {response.status})，正在强制换 IP 重试...")
            # 强制立即刷新代理
            self.get_proxy(force=True)
            # 将请求重新放回调度队列，不计入去重
            # 注意：不使用 time.sleep()，让 Scrapy 异步处理
            return request.replace(dont_filter=True)

        return response

    def process_exception(self, request, exception, spider):
        # 无代理模式下不进行代理切换
        if not self.proxy_enabled:
            self.logger.error(f"网络异常 ({exception})，请求重试...")
            return request.replace(dont_filter=True)
        
        # 捕捉超时、连接被重置等底层网络异常
        self.logger.error(f"网络异常 ({exception})，尝试更换代理重试...")
        self.get_proxy(force=True)
        return request.replace(dont_filter=True)