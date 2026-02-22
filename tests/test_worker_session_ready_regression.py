import unittest
from unittest.mock import patch

import worker as worker_module


class _BrokenSession:
    _session = None


class _FailingAmazonSession:
    def __init__(self, *args, **kwargs):
        self._session = None

    async def initialize(self):
        return False

    async def close(self):
        self._session = None


class WorkerSessionReadyRegressionTest(unittest.IsolatedAsyncioTestCase):
    async def test_process_task_stops_when_ready_but_session_is_invalid(self):
        worker = worker_module.Worker(server_url="http://127.0.0.1:8899")
        worker._interval = 0
        worker._jitter = 0
        worker._session = _BrokenSession()
        worker._session_ready.set()

        submit_calls = []

        async def fake_submit(task_id, data, success):
            submit_calls.append((task_id, success))

        async def fast_sleep(_delay):
            return None

        worker._submit_result = fake_submit
        task = {"asin": "B000TEST", "id": 1}

        with patch.object(worker_module.config, "MAX_RETRIES", 2):
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
