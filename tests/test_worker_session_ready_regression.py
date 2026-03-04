import unittest
from unittest.mock import patch

import worker as worker_module
import proxy as proxy_module


class _BrokenSession:
    """模拟一个 _session 为 None 但已初始化的 session（is_ready 返回 False）"""
    def is_ready(self) -> bool:
        return False

    async def close(self):
        pass


class _FailingAmazonSession:
    def __init__(self, *args, **kwargs):
        self._session = None
        self._initialized = False

    async def initialize(self):
        return False

    async def close(self):
        self._session = None


class _BlockedResponse:
    def __init__(self):
        self.status_code = 503
        self.text = "blocked"
        self.content = b"x" * 2048
        self.url = "https://www.amazon.com/dp/B000BLOCKED"


class _BlockedSession:
    def is_ready(self) -> bool:
        return True

    async def fetch_product_page(self, asin, max_recv_speed=0):
        return _BlockedResponse()

    def is_blocked(self, response) -> bool:
        return True

    def is_captcha(self, response) -> bool:
        return False

    def is_404(self, response) -> bool:
        return False


class WorkerSessionReadyRegressionTest(unittest.IsolatedAsyncioTestCase):
    async def test_process_task_stops_when_ready_but_session_is_invalid(self):
        """session is_ready() 返回 False 时，任务应在重试耗尽后标记失败"""
        worker = worker_module.Worker(server_url="http://127.0.0.1:8899")
        worker._max_retries = 2  # 直接设置实例变量，不再通过 config
        worker._session = _BrokenSession()
        worker._session_ready.set()

        submit_calls = []

        async def fake_submit(task_id, data, success, error_type=None, error_detail=None):
            submit_calls.append((task_id, success))

        async def fast_sleep(_delay):
            return None

        worker._submit_result = fake_submit
        task = {"asin": "B000TEST01", "id": 1}

        with patch("worker.asyncio.sleep", new=fast_sleep):
            result = await worker._process_task(task)

        self.assertEqual(result, (False, False, 0))
        self.assertEqual(worker._stats["failed"], 1)
        self.assertEqual(worker._stats["total"], 1)
        self.assertEqual(submit_calls, [(1, False)])

    async def test_init_session_sets_ready_even_on_failure(self):
        """初始化全部失败时，_session_ready 仍应被 set（防止 worker 死锁）"""
        worker = worker_module.Worker(server_url="http://127.0.0.1:8899")
        worker._session_ready.set()

        with patch("worker.AmazonSession", _FailingAmazonSession):
            await worker._init_session()

        # 即使 3 次全部失败，event 仍应 set，否则 worker 会永久阻塞
        self.assertTrue(worker._session_ready.is_set())
        self.assertIsNone(worker._session)

    async def test_process_task_reads_instance_max_retries(self):
        """_process_task 应读取 self._max_retries，而非全局 config.MAX_RETRIES"""
        worker = worker_module.Worker(server_url="http://127.0.0.1:8899")
        worker._max_retries = 1  # 只允许 1 次尝试
        worker._session = _BrokenSession()
        worker._session_ready.set()

        submit_calls = []

        async def fake_submit(task_id, data, success, error_type=None, error_detail=None):
            submit_calls.append((task_id, success))

        async def fast_sleep(_delay):
            return None

        worker._submit_result = fake_submit
        task = {"asin": "B000TEST02", "id": 2}

        with patch("worker.asyncio.sleep", new=fast_sleep):
            result = await worker._process_task(task)

        self.assertEqual(result, (False, False, 0))
        # 只提交了一次失败，证明 _max_retries=1 生效
        self.assertEqual(len(submit_calls), 1)
        self.assertFalse(submit_calls[0][1])

    async def test_tps_blocked_path_submits_failure_result(self):
        """TPS 模式被封后应立即提交失败结果，避免任务卡在 processing。"""
        worker = worker_module.Worker(server_url="http://127.0.0.1:8899")
        worker._proxy_mode = "tps"
        worker._session = _BlockedSession()
        worker._session_ready.set()

        submit_calls = []

        async def fake_submit(task_id, data, success, error_type=None, error_detail=None):
            submit_calls.append({
                "task_id": task_id,
                "success": success,
                "error_type": error_type,
                "error_detail": error_detail,
            })

        async def fake_rotate(*_args, **_kwargs):
            return None

        worker._submit_result = fake_submit
        worker._rotate_session = fake_rotate
        task = {"asin": "B000BLOCKED", "id": 99, "batch_name": "batch-x", "zip_code": "10001"}

        result = await worker._process_task(task)

        self.assertEqual(result[0], False)
        self.assertEqual(result[1], True)
        self.assertEqual(len(submit_calls), 1)
        self.assertEqual(submit_calls[0]["task_id"], 99)
        self.assertFalse(submit_calls[0]["success"])
        self.assertEqual(submit_calls[0]["error_type"], "blocked")
        self.assertIn("HTTP 503", submit_calls[0]["error_detail"])

    async def test_pull_initial_settings_syncs_mode_profile_and_tunnel_channels(self):
        """初始设置同步时，proxy_mode 与 tunnel_channels 应同步到运行时组件。"""
        old_mode = worker_module.config.PROXY_MODE
        old_channels = worker_module.config.TUNNEL_CHANNELS
        old_rotate = worker_module.config.TUNNEL_ROTATE_INTERVAL
        old_manager = proxy_module._proxy_manager

        worker_module.config.PROXY_MODE = "tps"
        worker_module.config.TUNNEL_CHANNELS = 8
        worker_module.config.TUNNEL_ROTATE_INTERVAL = 60
        proxy_module._proxy_manager = None
        worker = worker_module.Worker(server_url="http://127.0.0.1:8899")

        payload = {
            "proxy_mode": "tunnel",
            "tunnel_channels": 16,
            "tunnel_rotate_interval": 90,
        }

        class _Resp:
            status_code = 200
            def json(self):
                return payload

        class _Client:
            def __init__(self, *args, **kwargs):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc, tb):
                return False
            async def get(self, url):
                return _Resp()

        try:
            with patch("worker.httpx.AsyncClient", _Client):
                await worker._pull_initial_settings()

            self.assertEqual(worker._proxy_mode, "tunnel")
            self.assertEqual(worker._controller._proxy_mode, "tunnel")
            self.assertEqual(worker_module.config.TUNNEL_CHANNELS, 16)
            self.assertEqual(worker_module.config.TUNNEL_ROTATE_INTERVAL, 90)
            self.assertEqual(len(worker.proxy_manager._channels), 16)
        finally:
            worker_module.config.PROXY_MODE = old_mode
            worker_module.config.TUNNEL_CHANNELS = old_channels
            worker_module.config.TUNNEL_ROTATE_INTERVAL = old_rotate
            proxy_module._proxy_manager = old_manager
