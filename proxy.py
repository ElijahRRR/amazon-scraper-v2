"""
Amazon 产品采集系统 v2 - 代理管理模块

支持两种模式（共用同一个快代理 API）：
- TPS 模式：每次请求自动换 IP（API 获取 1 个代理，缓存复用）
- 隧道模式：定时换 IP，多通道并行（API 获取 N 个代理，轮询分发）

两种模式的区别仅在于代理行为（每次换 IP vs 定时换 IP），
API 地址和凭证完全相同（都走 PROXY_API_URL_AUTH）。
"""
import asyncio
import re
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

import httpx

import config

logger = logging.getLogger(__name__)


# ==================== 通道状态（隧道模式专用）====================

@dataclass
class ChannelState:
    """单个隧道通道的运行时状态"""
    channel_id: int                     # 通道编号 1-N
    proxy_url: str = ""                 # 该通道的代理 URL（从 API 获取）
    blocked: bool = False               # 是否被封
    blocked_at: float = 0               # 封锁时间戳（monotonic）
    request_count: int = 0              # 当前周期内请求计数
    last_request_at: float = 0          # 上次请求时间

    def reset_for_rotation(self):
        """IP 轮换时重置通道状态（proxy_url 由外部刷新）"""
        self.blocked = False
        self.blocked_at = 0
        self.request_count = 0


# ==================== 代理管理器 ====================

class ProxyManager:
    """
    统一代理管理器，通过 config.PROXY_MODE 区分行为：
    - "tps": 原有 TPS 逻辑（单代理、被封换 IP）
    - "tunnel": 多通道隧道（API 获取 N 个代理、轮询分发、被封换通道）
    """

    def __init__(self):
        self.mode = config.PROXY_MODE

        # --- TPS 模式状态 ---
        self._current_proxy: Optional[str] = None
        self._proxy_expire_at: float = 0
        self._refresh_interval = config.PROXY_REFRESH_INTERVAL
        self._last_fetch_time: float = 0
        self._fetch_lock = asyncio.Lock()

        # --- 隧道模式状态 ---
        self._channels: Dict[int, ChannelState] = {}
        self._round_robin_index = 0         # 轮询计数器
        self._rotation_at: float = 0        # 下次 IP 轮换时间点
        self._all_blocked_event = asyncio.Event()
        self._all_blocked_event.set()        # 初始不阻塞
        self._tunnel_init_lock = asyncio.Lock()

        if self.mode == "tunnel":
            # 先创建空的通道状态，proxy_url 由 init_tunnel_channels() 填充
            for i in range(1, config.TUNNEL_CHANNELS + 1):
                self._channels[i] = ChannelState(channel_id=i)
            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            logger.info(f"隧道模式初始化：{config.TUNNEL_CHANNELS} 通道，"
                        f"{config.TUNNEL_ROTATE_INTERVAL}s 轮换周期")

        # --- 公共统计 ---
        self._total_fetched = 0
        self._total_errors = 0
        self._total_blocked = 0

    # ==================== 公共接口 ====================

    async def get_proxy(self, channel: int = None) -> Tuple[Optional[str], Optional[int]]:
        """
        获取代理。

        返回: (proxy_url, channel_id)
        - TPS 模式: channel_id 固定为 None
        - 隧道模式: channel_id 为分配的通道编号
        """
        if self.mode == "tps":
            proxy = await self._tps_get_proxy()
            return proxy, None
        else:
            return await self._tunnel_get_proxy(channel)

    async def report_blocked(self, channel: int = None):
        """
        报告代理被封锁。

        - TPS 模式: 强制刷新代理
        - 隧道模式: 标记指定通道为被封
        """
        self._total_blocked += 1
        if self.mode == "tps":
            return await self._tps_report_blocked()
        else:
            return await self._tunnel_report_blocked(channel)

    async def wait_for_rotation(self):
        """等待 IP 轮换（仅隧道模式，全部通道被封时调用）"""
        if self.mode != "tunnel":
            return
        remaining = max(0, self._rotation_at - time.monotonic())
        if remaining > 0:
            logger.info(f"⏳ 全部通道被封，等待 IP 轮换（{remaining:.0f}s）...")
            await asyncio.sleep(remaining)

    def get_available_channel(self) -> Optional[int]:
        """获取一个可用通道（轮询分发），返回 None 表示全部被封"""
        if self.mode != "tunnel":
            return None
        available = [ch for ch in self._channels.values()
                     if not ch.blocked and ch.proxy_url]
        if not available:
            return None
        # round-robin
        self._round_robin_index = (self._round_robin_index + 1) % len(available)
        return available[self._round_robin_index].channel_id

    def all_channels_blocked(self) -> bool:
        """是否全部通道都被封（仅隧道模式）"""
        if self.mode != "tunnel":
            return False
        return all(ch.blocked for ch in self._channels.values())

    def get_channel_proxy_url(self, channel_id: int) -> Optional[str]:
        """获取指定通道的代理 URL（从 API 缓存中取）"""
        ch = self._channels.get(channel_id)
        if ch and ch.proxy_url:
            return ch.proxy_url
        return None

    async def init_tunnel_channels(self):
        """
        隧道模式启动初始化：调用 API 获取 N 个代理，填充到各通道。
        由 Worker 在 _init_session_tunnel() 中调用。
        """
        async with self._tunnel_init_lock:
            num = config.TUNNEL_CHANNELS
            logger.info(f"🔧 从 API 获取 {num} 个隧道代理...")
            proxies = await self._fetch_proxies_from_api(num)
            if not proxies:
                logger.error("❌ 获取隧道代理失败：API 返回空")
                return 0

            # 将获取到的代理分配到各通道
            assigned = 0
            for i, proxy_url in enumerate(proxies):
                ch_id = i + 1
                if ch_id in self._channels:
                    self._channels[ch_id].proxy_url = proxy_url
                    self._channels[ch_id].reset_for_rotation()
                    assigned += 1

            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            self._total_fetched += assigned
            logger.info(f"✅ 隧道代理就绪：{assigned}/{num} 通道已分配")
            return assigned

    async def refresh_tunnel_channels(self):
        """
        IP 轮换后重新获取代理，替换所有通道的 proxy_url。
        返回成功分配的通道数。
        """
        return await self.init_tunnel_channels()

    async def handle_ip_rotation(self):
        """
        处理 IP 轮换：检查是否到达轮换时间点。
        由 worker 的 _ip_rotation_watcher() 协程调用。
        返回 True 表示需要轮换（调用者需执行 refresh + session rebuild）。
        """
        now = time.monotonic()
        if now < self._rotation_at:
            return False  # 还没到轮换时间

        logger.info("🔄 IP 轮换时间到达，准备刷新代理...")
        # 重置通道状态（proxy_url 稍后由 refresh_tunnel_channels 更新）
        for ch in self._channels.values():
            ch.reset_for_rotation()
        self._all_blocked_event.set()  # 解除全封锁等待
        return True

    def time_to_next_rotation(self) -> float:
        """距离下次 IP 轮换的秒数"""
        return max(0, self._rotation_at - time.monotonic())

    def get_stats(self) -> Dict:
        """获取代理统计信息"""
        now = time.monotonic()
        stats = {
            "mode": self.mode,
            "total_fetched": self._total_fetched,
            "total_errors": self._total_errors,
            "total_blocked": self._total_blocked,
        }
        if self.mode == "tps":
            stats.update({
                "current_proxy": self._current_proxy,
                "proxy_valid": self._current_proxy is not None and now < self._proxy_expire_at,
                "expire_in": max(0, int(self._proxy_expire_at - now)),
            })
        else:
            stats.update({
                "channels": {
                    ch.channel_id: {
                        "blocked": ch.blocked,
                        "proxy": ch.proxy_url[:30] + "..." if ch.proxy_url else "",
                        "request_count": ch.request_count,
                    }
                    for ch in self._channels.values()
                },
                "next_rotation_in": int(self.time_to_next_rotation()),
                "blocked_channels": sum(1 for ch in self._channels.values() if ch.blocked),
            })
        return stats

    # ==================== API 调用（两种模式共用）====================

    def _make_api_url(self, num: int = 1) -> str:
        """构造 API URL，修改 num 参数为指定值"""
        url = config.PROXY_API_URL_AUTH
        # 替换 num=N 参数
        if "num=" in url:
            url = re.sub(r'num=\d+', f'num={num}', url)
        else:
            url += f"&num={num}"
        return url

    async def _fetch_proxies_from_api(self, num: int = 1) -> List[str]:
        """
        调用快代理 API 获取 N 个代理。
        返回: 代理 URL 列表，如 ["http://user:pwd@host:port", ...]
        """
        self._last_fetch_time = time.monotonic()
        api_url = self._make_api_url(num)

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(api_url)
                data = resp.json()
        except Exception as e:
            logger.error(f"代理 API 请求异常: {e}")
            self._total_errors += 1
            return []

        if data.get("code") != 0:
            logger.error(f"代理 API 返回错误: {data}")
            self._total_errors += 1
            return []

        proxy_list = data.get("data", {}).get("proxy_list", [])
        results = []
        for proxy_str in proxy_list:
            parts = proxy_str.split(":")
            if len(parts) == 4:
                ip, port, user, pwd = parts
                results.append(f"http://{user}:{pwd}@{ip}:{port}")
            elif len(parts) == 2:
                ip, port = parts
                results.append(f"http://{ip}:{port}")
            else:
                results.append(f"http://{proxy_str}")

        return results

    # ==================== TPS 模式内部实现 ====================

    async def _tps_get_proxy(self) -> Optional[str]:
        """TPS: 获取当前可用代理，过期则自动刷新"""
        now = time.monotonic()
        if self._current_proxy and now < self._proxy_expire_at:
            return self._current_proxy
        return await self._tps_refresh_proxy()

    async def _tps_refresh_proxy(self) -> Optional[str]:
        """TPS: 从快代理 API 获取新的隧道代理（线程安全）"""
        async with self._fetch_lock:
            now = time.monotonic()
            if self._current_proxy and now < self._proxy_expire_at:
                return self._current_proxy

            elapsed = now - self._last_fetch_time
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)

            for attempt in range(3):
                try:
                    proxies = await self._fetch_proxies_from_api(num=1)
                    if proxies:
                        self._current_proxy = proxies[0]
                        self._proxy_expire_at = time.monotonic() + self._refresh_interval
                        self._total_fetched += 1
                        logger.info(f"获取代理: {self._current_proxy}")
                        return self._current_proxy
                    logger.warning(f"代理 API 返回空结果 (尝试 {attempt+1}/3)")
                except Exception as e:
                    logger.error(f"获取代理失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

            self._total_errors += 1
            return self._current_proxy

    async def _tps_report_blocked(self):
        """TPS: 报告代理被封锁，强制过期触发重新获取"""
        logger.warning(f"代理被封（第 {self._total_blocked} 次），触发刷新")
        self._proxy_expire_at = 0
        self._current_proxy = None
        return await self._tps_refresh_proxy()

    # ==================== 隧道模式内部实现 ====================

    async def _tunnel_get_proxy(self, channel: int = None) -> Tuple[Optional[str], Optional[int]]:
        """隧道: 获取指定通道（或自动分配通道）的代理 URL"""
        if channel is None:
            channel = self.get_available_channel()
        if channel is None:
            # 全部通道被封
            return None, None

        ch_state = self._channels[channel]
        ch_state.request_count += 1
        ch_state.last_request_at = time.monotonic()
        return ch_state.proxy_url, channel

    async def _tunnel_report_blocked(self, channel: int):
        """隧道: 标记通道被封"""
        if channel is None or channel not in self._channels:
            return
        ch_state = self._channels[channel]
        ch_state.blocked = True
        ch_state.blocked_at = time.monotonic()
        blocked_count = sum(1 for ch in self._channels.values() if ch.blocked)
        logger.warning(f"🚫 通道 {channel} 被封（已封 {blocked_count}/{len(self._channels)}）")

        # 检查是否全部通道被封
        if self.all_channels_blocked():
            self._all_blocked_event.clear()
            logger.error("❌ 全部通道被封！等待 IP 轮换...")


# ==================== 全局单例 ====================

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
