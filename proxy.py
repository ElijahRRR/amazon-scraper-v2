"""
Amazon 产品采集系统 v2 - 代理管理模块
快代理 TPS 隧道代理集成
TPS 隧道特点：固定入口地址，IP 轮换在服务端透明进行
"""
import asyncio
import time
import logging
from typing import Optional, Dict

import httpx

import config

logger = logging.getLogger(__name__)


class ProxyManager:
    """TPS 隧道代理管理器"""

    def __init__(self):
        self._current_proxy: Optional[str] = None
        self._proxy_expire_at: float = 0
        self._refresh_interval = config.PROXY_REFRESH_INTERVAL
        self._last_fetch_time: float = 0
        self._fetch_lock = asyncio.Lock()
        self._total_fetched = 0
        self._total_errors = 0
        self._total_blocked = 0

    async def get_proxy(self) -> Optional[str]:
        """获取当前可用代理，过期则自动刷新"""
        now = time.monotonic()
        if self._current_proxy and now < self._proxy_expire_at:
            return self._current_proxy
        return await self.refresh_proxy()

    async def refresh_proxy(self) -> Optional[str]:
        """从快代理 API 获取新的隧道代理（线程安全）"""
        async with self._fetch_lock:
            now = time.monotonic()
            if self._current_proxy and now < self._proxy_expire_at:
                return self._current_proxy

            elapsed = now - self._last_fetch_time
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)

            for attempt in range(3):
                try:
                    proxy = await self._fetch_proxy_from_api()
                    if proxy:
                        self._current_proxy = proxy
                        self._proxy_expire_at = time.monotonic() + self._refresh_interval
                        self._total_fetched += 1
                        logger.info(f"获取代理: {proxy}")
                        return proxy
                    logger.warning(f"代理 API 返回空结果 (尝试 {attempt+1}/3)")
                except Exception as e:
                    logger.error(f"获取代理失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

            self._total_errors += 1
            return self._current_proxy  # 返回旧代理（如果有）

    async def _fetch_proxy_from_api(self) -> Optional[str]:
        """调用快代理 TPS API 获取隧道代理（纯异步）"""
        self._last_fetch_time = time.monotonic()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(config.PROXY_API_URL_AUTH)
            data = resp.json()

        if data.get("code") == 0:
            proxy_list = data.get("data", {}).get("proxy_list", [])
            if proxy_list:
                proxy_str = proxy_list[0]  # 格式: ip:port:user:pwd 或 ip:port
                parts = proxy_str.split(":")
                if len(parts) == 4:
                    ip, port, user, pwd = parts
                    return f"http://{user}:{pwd}@{ip}:{port}"
                elif len(parts) == 2:
                    ip, port = parts
                    return f"http://{ip}:{port}"
                else:
                    return f"http://{proxy_str}"
        else:
            logger.error(f"代理 API 返回错误: {data}")
        return None

    async def report_blocked(self):
        """报告代理被封锁，强制过期触发重新获取"""
        self._total_blocked += 1
        logger.warning(f"代理被封（第 {self._total_blocked} 次），触发刷新")
        self._proxy_expire_at = 0
        self._current_proxy = None
        return await self.refresh_proxy()

    def get_stats(self) -> Dict:
        """获取代理统计信息"""
        now = time.monotonic()
        return {
            "current_proxy": self._current_proxy,
            "proxy_valid": self._current_proxy is not None and now < self._proxy_expire_at,
            "expire_in": max(0, int(self._proxy_expire_at - now)),
            "total_fetched": self._total_fetched,
            "total_errors": self._total_errors,
            "total_blocked": self._total_blocked,
        }


_proxy_manager: Optional[ProxyManager] = None
_proxy_manager_lock = asyncio.Lock()


async def get_proxy_manager_async() -> ProxyManager:
    """获取全局代理管理器实例（异步安全单例）"""
    global _proxy_manager
    if _proxy_manager is None:
        async with _proxy_manager_lock:
            if _proxy_manager is None:
                _proxy_manager = ProxyManager()
    return _proxy_manager


def get_proxy_manager() -> ProxyManager:
    """获取全局代理管理器实例（同步调用，适用于初始化阶段）"""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
