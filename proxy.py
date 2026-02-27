"""
Amazon 产品采集系统 v2 - 代理管理模块

支持两种模式：
- TPS 模式：快代理 TPS 隧道，每次请求自动换 IP
- 隧道模式：快代理隧道代理，8 通道，每 60 秒轮换 IP
"""
import asyncio
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
    blocked: bool = False               # 是否被封
    blocked_at: float = 0               # 封锁时间戳（monotonic）
    request_count: int = 0              # 当前周期内请求计数
    last_request_at: float = 0          # 上次请求时间（用于每通道限速）
    manual_change_count: int = 0        # 当前周期内手动换 IP 次数（上限 2）

    def reset_for_rotation(self):
        """IP 轮换时重置通道状态"""
        self.blocked = False
        self.blocked_at = 0
        self.request_count = 0
        self.manual_change_count = 0


# ==================== 代理管理器 ====================

class ProxyManager:
    """
    统一代理管理器，通过 config.PROXY_MODE 区分行为：
    - "tps": 原有 TPS 逻辑（单代理、被封换 IP）
    - "tunnel": 多通道隧道（轮询分发、被封换通道）
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
        self._change_ip_lock = asyncio.Lock()

        if self.mode == "tunnel":
            for i in range(1, config.TUNNEL_CHANNELS + 1):
                self._channels[i] = ChannelState(channel_id=i)
            self._rotation_at = time.monotonic() + config.TUNNEL_ROTATE_INTERVAL
            logger.info(f"🔧 隧道模式初始化：{config.TUNNEL_CHANNELS} 通道，"
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
        - 隧道模式: 标记指定通道为被封，尝试手动换 IP
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
        available = [ch for ch in self._channels.values() if not ch.blocked]
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

    def get_channel_proxy_url(self, channel_id: int) -> str:
        """构造指定通道的代理 URL"""
        return (f"http://{config.TUNNEL_USER}:{config.TUNNEL_PASS}:{channel_id}"
                f"@{config.TUNNEL_HOST}:{config.TUNNEL_PORT}")

    async def handle_ip_rotation(self):
        """
        处理 IP 轮换：重置所有通道状态，更新下次轮换时间。
        由 worker 的 _ip_rotation_watcher() 协程调用。
        """
        now = time.monotonic()
        if now < self._rotation_at:
            return False  # 还没到轮换时间

        logger.info("🔄 IP 轮换：重置所有通道状态")
        for ch in self._channels.values():
            ch.reset_for_rotation()
        self._rotation_at = now + config.TUNNEL_ROTATE_INTERVAL
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
                        "request_count": ch.request_count,
                        "manual_changes": ch.manual_change_count,
                    }
                    for ch in self._channels.values()
                },
                "next_rotation_in": int(self.time_to_next_rotation()),
                "blocked_channels": sum(1 for ch in self._channels.values() if ch.blocked),
            })
        return stats

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
            return self._current_proxy

    async def _tps_report_blocked(self):
        """TPS: 报告代理被封锁，强制过期触发重新获取"""
        logger.warning(f"代理被封（第 {self._total_blocked} 次），触发刷新")
        self._proxy_expire_at = 0
        self._current_proxy = None
        return await self._tps_refresh_proxy()

    async def _fetch_proxy_from_api(self) -> Optional[str]:
        """调用快代理 TPS API 获取隧道代理"""
        self._last_fetch_time = time.monotonic()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(config.PROXY_API_URL_AUTH)
            data = resp.json()

        if data.get("code") == 0:
            proxy_list = data.get("data", {}).get("proxy_list", [])
            if proxy_list:
                proxy_str = proxy_list[0]
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
        proxy_url = self.get_channel_proxy_url(channel)
        return proxy_url, channel

    async def _tunnel_report_blocked(self, channel: int):
        """隧道: 标记通道被封，尝试手动换 IP"""
        if channel is None or channel not in self._channels:
            return
        ch_state = self._channels[channel]
        ch_state.blocked = True
        ch_state.blocked_at = time.monotonic()
        blocked_count = sum(1 for ch in self._channels.values() if ch.blocked)
        logger.warning(f"🚫 通道 {channel} 被封（已封 {blocked_count}/{len(self._channels)}）")

        # 尝试手动换 IP
        if ch_state.manual_change_count < config.TUNNEL_MAX_MANUAL_CHANGE:
            await self._tunnel_change_ip(channel)

        # 检查是否全部通道被封
        if self.all_channels_blocked():
            self._all_blocked_event.clear()
            logger.error("❌ 全部通道被封！等待 IP 轮换...")

    async def _tunnel_change_ip(self, channel: int):
        """调用快代理 ChangeTpsIp API 手动换 IP"""
        async with self._change_ip_lock:
            ch_state = self._channels[channel]
            if ch_state.manual_change_count >= config.TUNNEL_MAX_MANUAL_CHANGE:
                return False

            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(config.TUNNEL_CHANGE_IP_URL)
                    data = resp.json()

                if data.get("code") == 0:
                    ch_state.manual_change_count += 1
                    ch_state.blocked = False
                    ch_state.blocked_at = 0
                    logger.info(f"✅ 通道 {channel} 手动换 IP 成功"
                                f"（本周期第 {ch_state.manual_change_count} 次）")
                    # 解除全封锁状态
                    if not self.all_channels_blocked():
                        self._all_blocked_event.set()
                    return True
                else:
                    logger.warning(f"⚠️ 通道 {channel} 手动换 IP 失败: {data}")
                    return False
            except Exception as e:
                logger.error(f"❌ 通道 {channel} 手动换 IP 异常: {e}")
                return False


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
