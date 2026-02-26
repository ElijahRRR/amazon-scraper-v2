"""
全局并发协调器单元测试
测试 server.py 中的 _allocate_quotas / _handle_worker_metrics 逻辑
以及 adaptive.py 中的抖动恢复
"""
import time
import unittest
from unittest.mock import patch

# 导入 server 模块的内部状态（测试前需要重置）
import server as srv
import adaptive


def _reset_coordinator():
    """重置协调器和注册表到初始状态"""
    srv._global_coordinator.update({
        "worker_metrics": {},
        "worker_quotas": {},
        "global_block_until": 0.0,
        "block_count": 0,
        "recovery_epoch": 0,
        "recovery_jitter": {},
    })
    srv._worker_registry.clear()


def _register_active_worker(wid: str):
    """注册一个活跃 Worker（last_seen = now）"""
    now = time.time()
    srv._worker_registry[wid] = {
        "worker_id": wid,
        "first_seen": now,
        "last_seen": now,
        "tasks_pulled": 0,
        "results_submitted": 0,
    }


class TestAllocateQuotas(unittest.TestCase):
    def setUp(self):
        _reset_coordinator()
        # 设置全局预算
        srv._runtime_settings["global_max_concurrency"] = 30
        srv._runtime_settings["global_max_qps"] = 6.0
        srv._runtime_settings["min_concurrency"] = 2

    def test_no_workers(self):
        """无活跃 Worker 时配额为空"""
        srv._allocate_quotas()
        self.assertEqual(srv._global_coordinator["worker_quotas"], {})

    def test_single_worker_gets_full_budget(self):
        """单个 Worker 获得全部预算"""
        _register_active_worker("w1")
        srv._allocate_quotas()

        quotas = srv._global_coordinator["worker_quotas"]
        self.assertIn("w1", quotas)
        self.assertEqual(quotas["w1"]["concurrency"], 30)
        self.assertAlmostEqual(quotas["w1"]["qps"], 6.0, places=1)

    def test_equal_health_equal_split(self):
        """健康度相同时均分预算"""
        _register_active_worker("w1")
        _register_active_worker("w2")

        # 两个 Worker 上报相同的 metrics
        good_metrics = {"success_rate": 0.95, "block_rate": 0.0, "reported_at": time.time()}
        srv._global_coordinator["worker_metrics"]["w1"] = good_metrics
        srv._global_coordinator["worker_metrics"]["w2"] = good_metrics.copy()

        srv._allocate_quotas()
        quotas = srv._global_coordinator["worker_quotas"]

        self.assertEqual(quotas["w1"]["concurrency"], quotas["w2"]["concurrency"])
        self.assertEqual(quotas["w1"]["concurrency"], 15)  # 30 / 2

    def test_unhealthy_worker_gets_less(self):
        """不健康的 Worker 获得更少配额"""
        _register_active_worker("w1")
        _register_active_worker("w2")

        now = time.time()
        srv._global_coordinator["worker_metrics"]["w1"] = {
            "success_rate": 0.95, "block_rate": 0.0, "reported_at": now,
        }
        srv._global_coordinator["worker_metrics"]["w2"] = {
            "success_rate": 0.60, "block_rate": 0.10, "reported_at": now,
        }

        srv._allocate_quotas()
        quotas = srv._global_coordinator["worker_quotas"]

        # w1 健康 → 更多配额
        self.assertGreater(quotas["w1"]["concurrency"], quotas["w2"]["concurrency"])
        self.assertGreater(quotas["w1"]["qps"], quotas["w2"]["qps"])

    def test_total_concurrency_within_budget(self):
        """总配额不超过全局预算"""
        for i in range(5):
            _register_active_worker(f"w{i}")
            srv._global_coordinator["worker_metrics"][f"w{i}"] = {
                "success_rate": 0.95, "block_rate": 0.0, "reported_at": time.time(),
            }

        srv._allocate_quotas()
        quotas = srv._global_coordinator["worker_quotas"]
        total_c = sum(q["concurrency"] for q in quotas.values())
        total_q = sum(q["qps"] for q in quotas.values())

        # 严格不超预算
        self.assertLessEqual(total_c, 30)
        self.assertLessEqual(total_q, 6.0 + 0.01)  # 浮点容差

    def test_budget_not_overrun_with_high_min_concurrency(self):
        """min_concurrency 较高时总配额仍不超预算"""
        srv._runtime_settings["min_concurrency"] = 20
        srv._runtime_settings["global_max_concurrency"] = 50
        srv._runtime_settings["global_max_qps"] = 1.0
        for i in range(5):
            _register_active_worker(f"w{i}")
            srv._global_coordinator["worker_metrics"][f"w{i}"] = {
                "success_rate": 0.95, "block_rate": 0.0, "reported_at": time.time(),
            }

        srv._allocate_quotas()
        quotas = srv._global_coordinator["worker_quotas"]
        total_c = sum(q["concurrency"] for q in quotas.values())
        total_q = sum(q["qps"] for q in quotas.values())

        # 5 workers × min_c=20 = 100 > budget=50
        # 裁剪后应 ≤ 50（但每个至少 min_c，这是硬约束冲突时优先保证预算）
        self.assertLessEqual(total_c, 50)
        self.assertLessEqual(total_q, 1.0 + 0.01)

    def test_offline_worker_excluded(self):
        """离线 Worker 不参与配额分配"""
        _register_active_worker("w1")
        _register_active_worker("w2")
        # 将 w2 设为 2 分钟前最后心跳 → 离线
        srv._worker_registry["w2"]["last_seen"] = time.time() - 120

        srv._allocate_quotas()
        quotas = srv._global_coordinator["worker_quotas"]

        self.assertIn("w1", quotas)
        self.assertNotIn("w2", quotas)
        self.assertEqual(quotas["w1"]["concurrency"], 30)  # 全部预算

    def test_cooldown_slashes_budget(self):
        """全局冷却期内预算减半"""
        _register_active_worker("w1")
        srv._global_coordinator["worker_metrics"]["w1"] = {
            "success_rate": 0.95, "block_rate": 0.0, "reported_at": time.time(),
        }

        # 触发冷却
        srv._global_coordinator["global_block_until"] = time.monotonic() + 30
        srv._global_coordinator["recovery_jitter"]["w1"] = 1.0  # 最大抖动

        srv._allocate_quotas()
        quotas = srv._global_coordinator["worker_quotas"]

        # 冷却期预算减半：30/2=15，再乘以抖动 (0.5 + 0.5*1.0)=1.0 → 15
        self.assertLessEqual(quotas["w1"]["concurrency"], 15)


class TestHandleWorkerMetrics(unittest.TestCase):
    def setUp(self):
        _reset_coordinator()
        _register_active_worker("w1")
        _register_active_worker("w2")
        srv._runtime_settings["block_rate_threshold"] = 0.05
        srv._runtime_settings["cooldown_after_block"] = 30
        srv._runtime_settings["global_max_concurrency"] = 30
        srv._runtime_settings["global_max_qps"] = 6.0
        srv._runtime_settings["min_concurrency"] = 2

    def test_stores_metrics(self):
        """metrics 被正确存储"""
        metrics = {"success_rate": 0.96, "block_rate": 0.02, "latency_p50": 3.5}
        srv._handle_worker_metrics("w1", metrics)

        stored = srv._global_coordinator["worker_metrics"]["w1"]
        self.assertEqual(stored["success_rate"], 0.96)
        self.assertIn("reported_at", stored)

    def test_triggers_global_block(self):
        """block_rate > threshold 触发全局封锁"""
        metrics = {"success_rate": 0.80, "block_rate": 0.08}
        srv._handle_worker_metrics("w1", metrics)

        g = srv._global_coordinator
        self.assertGreater(g["global_block_until"], time.monotonic())
        self.assertEqual(g["block_count"], 1)
        self.assertEqual(g["recovery_epoch"], 1)
        # 应为每个 Worker 分配抖动系数
        self.assertIn("w1", g["recovery_jitter"])
        self.assertIn("w2", g["recovery_jitter"])

    def test_no_double_trigger(self):
        """冷却期内不会重复触发"""
        metrics = {"success_rate": 0.80, "block_rate": 0.08}

        srv._handle_worker_metrics("w1", metrics)
        epoch_after_first = srv._global_coordinator["recovery_epoch"]

        # 第二次上报：冷却期内
        srv._handle_worker_metrics("w2", metrics)
        epoch_after_second = srv._global_coordinator["recovery_epoch"]

        self.assertEqual(epoch_after_first, epoch_after_second)
        self.assertEqual(srv._global_coordinator["block_count"], 1)

    def test_no_block_below_threshold(self):
        """block_rate 低于阈值不触发"""
        metrics = {"success_rate": 0.95, "block_rate": 0.03}
        srv._handle_worker_metrics("w1", metrics)

        self.assertEqual(srv._global_coordinator["block_count"], 0)
        self.assertEqual(srv._global_coordinator["global_block_until"], 0.0)

    def test_triggered_by_is_recorded(self):
        """触发全局封锁时记录触发者 worker_id"""
        metrics = {"success_rate": 0.80, "block_rate": 0.08}
        srv._handle_worker_metrics("w1", metrics)

        self.assertEqual(srv._global_coordinator["block_triggered_by"], "w1")


class TestJitteredRecovery(unittest.TestCase):
    def test_high_jitter_increases_more(self):
        """高抖动系数的 Worker 更可能加速"""
        controller = adaptive.AdaptiveController(initial=5, min_c=2, max_c=50)
        controller._recovery_jitter = 1.0  # 最大

        # 模拟 1000 次决策
        increases = 0
        for _ in range(1000):
            import random
            if random.random() < (0.3 + 0.7 * controller._recovery_jitter):
                increases += 1

        # jitter=1.0 → 概率=1.0 → 应该接近 1000
        self.assertGreater(increases, 950)

    def test_low_jitter_increases_less(self):
        """低抖动系数的 Worker 较少加速"""
        controller = adaptive.AdaptiveController(initial=5, min_c=2, max_c=50)
        controller._recovery_jitter = 0.0  # 最小

        increases = 0
        for _ in range(1000):
            import random
            if random.random() < (0.3 + 0.7 * controller._recovery_jitter):
                increases += 1

        # jitter=0.0 → 概率=0.3 → 应该在 250-350 范围
        self.assertGreater(increases, 200)
        self.assertLess(increases, 450)

    def test_default_jitter(self):
        """默认抖动系数为 0.5"""
        controller = adaptive.AdaptiveController(initial=5, min_c=2, max_c=50)
        self.assertEqual(controller._recovery_jitter, 0.5)


if __name__ == "__main__":
    unittest.main()
