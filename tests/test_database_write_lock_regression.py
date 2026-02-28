import asyncio
import os
import tempfile
import unittest

from database import Database


class DatabaseWriteLockRegressionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.db_path = tempfile.mkstemp(prefix="db_lock_reg_", suffix=".sqlite3")
        os.close(fd)
        os.unlink(self.db_path)
        self.db = Database(self.db_path)
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    async def test_pull_tasks_and_retry_all_failed_do_not_conflict(self):
        asins = [f"B{i:09d}" for i in range(1, 121)]
        await self.db.create_tasks("batch_lock", asins, "10001", False)

        # 造一些 failed 任务，触发 retry_all_failed 的写路径
        for task_id in range(1, 30):
            await self.db.mark_task_failed(task_id, "w0", "network", "x")

        errors = []

        async def puller():
            for _ in range(200):
                try:
                    await self.db.pull_tasks("w1", 10)
                except Exception as e:
                    errors.append(("pull", repr(e)))

        async def retryer():
            for _ in range(200):
                try:
                    await self.db.retry_all_failed("batch_lock")
                except Exception as e:
                    errors.append(("retry", repr(e)))

        await asyncio.gather(puller(), retryer())

        self.assertEqual(errors, [])

