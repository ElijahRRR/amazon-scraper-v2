"""
Amazon 产品采集系统 v2 - 代理管理模块
快代理 TPS 隧道代理集成
TPS 隧道特点：固定入口地址，IP 轮换在服务端透明进行
"""
import asyncio
import time
import logging
from typing import Optional, Dict

import config

logger = logging.getLogger(__name__)


class ProxyManager:
    """TPS 隧道代理管理器"""

    def __init__(self):
        # 当前可用代理
        self._current_proxy: Optional[str] = None
        self._proxy_expire_at: float = 0
        # 代理刷新间隔（秒）
        self._refresh_interval = config.PROXY_REFRESH_INTERVAL
        # 上次获取时间（用于限速，快代理 API 有调用频率限制）
        self._last_fetch_time: float = 0
        self._fetch_lock = asyncio.Lock()
        # 统计
        self._total_fetched = 0
        self._total_errors = 0
        self._total_blocked = 0

    async def get_proxy(self) -> Optional[str]:
        """
        获取当前可用代理
        如果代理过期或不存在，自动获取新代理
        """
        now = time.time()
        if self._current_proxy and now < self._proxy_expire_at:
            return self._current_proxy
        return await self.refresh_proxy()

    async def refresh_proxy(self) -> Optional[str]:
        """
        从快代理 API 获取新的隧道代理
        线程安全，避免多个协程同时请求
        """
        async with self._fetch_lock:
            # 双重检查
            now = time.time()
            if self._current_proxy and now < self._proxy_expire_at:
                return self._current_proxy

            # 避免过于频繁请求 API（至少间隔 1 秒）
            elapsed = now - self._last_fetch_time
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)

            try:
                proxy = await self._fetch_proxy_from_api()
                if proxy:
                    self._current_proxy = proxy
                    self._proxy_expire_at = time.time() + self._refresh_interval
                    self._total_fetched += 1
                    logger.info(f"获取代理: {proxy}")
                    return proxy
                else:
                    logger.warning("代理 API 返回空结果")
                    self._total_errors += 1
                    return self._current_proxy  # 返回旧代理（如果有）
            except Exception as e:
                logger.error(f"获取代理失败: {e}")
                self._total_errors += 1
                return self._current_proxy

    async def _fetch_proxy_from_api(self) -> Optional[str]:
        """调用快代理 TPS API 获取隧道代理"""
        self._last_fetch_time = time.time()

        from curl_cffi import requests as curl_requests

        try:
            resp = curl_requests.get(
                config.PROXY_API_URL_AUTH,
                timeout=10
            )
            data = resp.json()

            if data.get("code") == 0:
                proxy_list = data.get("data", {}).get("proxy_list", [])
                if proxy_list:
                    proxy_str = proxy_list[0]  # 格式: ip:port:user:pwd
                    parts = proxy_str.split(":")
                    if len(parts) == 4:
                        ip, port, user, pwd = parts
                        proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
                    elif len(parts) == 2:
                        ip, port = parts
                        proxy_url = f"http://{ip}:{port}"
                    else:
                        proxy_url = f"http://{proxy_str}"
                    return proxy_url
            else:
                logger.error(f"代理 API 返回错误: {data}")
                return None
        except Exception as e:
            logger.error(f"代理 API 请求异常: {e}")
            return None

    async def report_blocked(self):
        """
        报告代理被封锁
        TPS 隧道代理：强制过期当前代理，触发 API 重新获取
        TPS 服务端会自动分配新的出口 IP
        """
        self._total_blocked += 1
        logger.warning(f"代理被封（第 {self._total_blocked} 次），触发刷新")

        # 强制过期，下次 get_proxy() 会重新获取
        self._proxy_expire_at = 0
        self._current_proxy = None

        return await self.refresh_proxy()

    def get_stats(self) -> Dict:
        """获取代理统计信息"""
        return {
            "current_proxy": self._current_proxy,
            "proxy_valid": self._current_proxy is not None and time.time() < self._proxy_expire_at,
            "expire_in": max(0, int(self._proxy_expire_at - time.time())),
            "total_fetched": self._total_fetched,
            "total_errors": self._total_errors,
            "total_blocked": self._total_blocked,
        }


# 全局代理管理器实例
_proxy_manager: Optional[ProxyManager] = None


def get_proxy_manager() -> ProxyManager:
    """获取全局代理管理器实例"""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
