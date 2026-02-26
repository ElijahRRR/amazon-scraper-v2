import unittest
from unittest.mock import patch

import worker as worker_module


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
