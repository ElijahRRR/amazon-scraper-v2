"""
Amazon 产品采集系统 v2 - 数据库操作模块
使用 aiosqlite 实现异步 SQLite 数据库操作
包含 tasks 和 results 两张表的完整 CRUD
"""
import os
import time
import aiosqlite
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

import logging

import config
from models import Task, Result, RESULT_FIELDS

logger = logging.getLogger(__name__)


class Database:
    """异步 SQLite 数据库管理器"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        self._db: Optional[aiosqlite.Connection] = None
        # 写操作序列化锁：防止 pull_tasks 和 submit_batch 并发时事务冲突
        # (aiosqlite 单线程执行 SQL，但 async 协程会交错 DML 语句导致隐式事务冲突)
        self._write_lock = asyncio.Lock()

    async def connect(self):
        """建立数据库连接"""
        # 确保目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        # 启用 WAL 模式提升并发性能
        await self._db.execute("PRAGMA journal_mode=WAL")
        # 返回字典行
        self._db.row_factory = aiosqlite.Row
        await self.init_tables()

    async def close(self):
        """关闭数据库连接"""
        if self._db:
            await self._db.close()
            self._db = None

    async def init_tables(self):
        """初始化数据库表"""
        await self._db.executescript("""
            -- 采集任务表
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_name TEXT NOT NULL,
                asin TEXT NOT NULL,
                zip_code TEXT DEFAULT '10001',
                status TEXT DEFAULT 'pending',
                priority INTEGER DEFAULT 0,
                needs_screenshot BOOLEAN DEFAULT 0,
                worker_id TEXT,
                retry_count INTEGER DEFAULT 0,
                error_type TEXT,
                error_detail TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(batch_name, asin)
            );

            -- 采集结果表
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_name TEXT NOT NULL,
                asin TEXT NOT NULL,
                zip_code TEXT,
                crawl_time TEXT,
                site TEXT DEFAULT 'US',
                product_url TEXT,
                title TEXT,
                brand TEXT,
                product_type TEXT,
                manufacturer TEXT,
                model_number TEXT,
                part_number TEXT,
                country_of_origin TEXT,
                is_customized TEXT,
                best_sellers_rank TEXT,
                original_price TEXT,
                current_price TEXT,
                buybox_price TEXT,
                buybox_shipping TEXT,
                is_fba TEXT,
                stock_count TEXT,
                stock_status TEXT,
                delivery_date TEXT,
                delivery_time TEXT,
                image_urls TEXT,
                bullet_points TEXT,
                long_description TEXT,
                upc_list TEXT,
                ean_list TEXT,
                parent_asin TEXT,
                variation_asins TEXT,
                root_category_id TEXT,
                category_ids TEXT,
                category_tree TEXT,
                first_available_date TEXT,
                package_dimensions TEXT,
                package_weight TEXT,
                item_dimensions TEXT,
                item_weight TEXT,
                screenshot_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 索引
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_batch ON tasks(batch_name);
            CREATE INDEX IF NOT EXISTS idx_tasks_batch_asin ON tasks(batch_name, asin);
            CREATE INDEX IF NOT EXISTS idx_results_batch ON results(batch_name);
            CREATE INDEX IF NOT EXISTS idx_results_asin ON results(asin);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_results_batch_asin ON results(batch_name, asin);
        """)

        # 迁移：为已存在的表添加新列（CREATE TABLE IF NOT EXISTS 不会修改已有表）
        migrations = [
            ("tasks", "priority", "INTEGER DEFAULT 0"),
            ("tasks", "needs_screenshot", "BOOLEAN DEFAULT 0"),
            ("tasks", "error_type", "TEXT"),
            ("tasks", "error_detail", "TEXT"),
            ("results", "screenshot_path", "TEXT"),
        ]
        for table, column, col_type in migrations:
            try:
                await self._db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            except Exception as e:
                # 列已存在时 SQLite 会报 "duplicate column name"，属正常情况
                if "duplicate column name" not in str(e).lower():
                    logger.warning(f"Migration 异常 ({table}.{column}): {e}")

        await self._db.commit()

    # ==================== 任务操作 ====================

    async def create_tasks(self, batch_name: str, asins: List[str], zip_code: str = "10001",
                           needs_screenshot: bool = False) -> int:
        """
        批量创建采集任务（使用 executemany 高效插入）
        返回: 实际插入的任务数（跳过已存在的）
        """
        # 预处理：去空、去重
        clean_asins = []
        seen = set()
        screenshot_val = 1 if needs_screenshot else 0
        for asin in asins:
            asin = asin.strip()
            if asin and asin not in seen:
                clean_asins.append((batch_name, asin, zip_code, screenshot_val))
                seen.add(asin)

        if not clean_asins:
            return 0

        async with self._write_lock:
            before = self._db.total_changes
            await self._db.executemany(
                "INSERT OR IGNORE INTO tasks (batch_name, asin, zip_code, needs_screenshot) VALUES (?, ?, ?, ?)",
                clean_asins
            )
            await self._db.commit()
            inserted = self._db.total_changes - before
        return inserted

    async def pull_tasks(self, worker_id: str, count: int = 10) -> List[Dict]:
        """
        Worker 拉取待处理任务
        原子操作：在写锁 + BEGIN IMMEDIATE 中完成 SELECT + UPDATE，防止并发重复分发

        修复：使用 _write_lock 序列化，防止与 submit_batch / reset_timeout_tasks 并发时
        出现 "cannot start a transaction within a transaction" 错误
        """
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        tasks = []

        async with self._write_lock:
            # 先回退超时任务（在同一把锁内，避免与外部 reset_timeout_tasks 冲突）
            await self._reset_timeout_tasks_unlocked()

            # BEGIN IMMEDIATE 保证写锁，防止两个 Worker 同时拉到相同任务
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                async with self._db.execute(
                    """SELECT id, batch_name, asin, zip_code, retry_count, priority, needs_screenshot
                       FROM tasks
                       WHERE status = 'pending'
                       ORDER BY priority DESC, id ASC
                       LIMIT ?""",
                    (count,)
                ) as cursor:
                    rows = await cursor.fetchall()

                if not rows:
                    await self._db.execute("COMMIT")
                    return tasks

                ids = []
                for row in rows:
                    task = {
                        "id": row["id"],
                        "batch_name": row["batch_name"],
                        "asin": row["asin"],
                        "zip_code": row["zip_code"],
                        "retry_count": row["retry_count"],
                        "priority": row["priority"],
                        "needs_screenshot": row["needs_screenshot"],
                    }
                    tasks.append(task)
                    ids.append(row["id"])

                # 批量更新状态为 processing
                placeholders = ",".join(["?"] * len(ids))
                await self._db.execute(
                    f"""UPDATE tasks
                        SET status = 'processing', worker_id = ?, updated_at = ?
                        WHERE id IN ({placeholders})""",
                    [worker_id, now] + ids
                )
                await self._db.execute("COMMIT")
            except Exception:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:
                    pass  # ROLLBACK 本身失败时忽略（连接可能已断）
                raise

        return tasks

    async def update_task_status(self, task_id: int, status: str, worker_id: str = None):
        """更新单个任务状态"""
        async with self._write_lock:
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            await self._db.execute(
                "UPDATE tasks SET status = ?, worker_id = ?, updated_at = ? WHERE id = ?",
                (status, worker_id, now, task_id)
            )
            await self._db.commit()

    async def mark_task_done(self, task_id: int, worker_id: str = None):
        """标记任务完成"""
        await self.update_task_status(task_id, "done", worker_id)

    async def mark_task_failed(self, task_id: int, worker_id: str = None,
                               error_type: str = None, error_detail: str = None):
        """标记任务失败，增加重试次数，记录错误类型"""
        async with self._write_lock:
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            await self._db.execute(
                """UPDATE tasks
                   SET status = 'failed', worker_id = ?, retry_count = retry_count + 1,
                       error_type = ?, error_detail = ?, updated_at = ?
                   WHERE id = ?""",
                (worker_id, error_type, error_detail, now, task_id)
            )
            await self._db.commit()

    async def retry_failed_task(self, task_id: int):
        """将失败任务重新设为 pending"""
        async with self._write_lock:
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            await self._db.execute(
                "UPDATE tasks SET status = 'pending', worker_id = NULL, updated_at = ? WHERE id = ? AND status = 'failed'",
                (now, task_id)
            )
            await self._db.commit()

    async def _reset_timeout_tasks_unlocked(self):
        """
        内部方法：回退超时任务（调用方已持有 _write_lock）
        不单独 commit，由调用方统一管理事务
        """
        timeout = config.TASK_TIMEOUT_MINUTES
        cutoff = (datetime.utcnow() - timedelta(minutes=timeout)).strftime('%Y-%m-%d %H:%M:%S')
        result = await self._db.execute(
            """UPDATE tasks
               SET status = 'pending', worker_id = NULL, updated_at = CURRENT_TIMESTAMP
               WHERE status = 'processing' AND updated_at < ?""",
            (cutoff,)
        )
        await self._db.commit()
        return result.rowcount

    async def reset_timeout_tasks(self):
        """
        断点续传：将 processing 超过 5 分钟的任务回退为 pending
        防止 worker 崩溃后任务永远卡在 processing

        外部调用时通过 _write_lock 序列化，避免与 pull_tasks / submit_batch 冲突
        """
        async with self._write_lock:
            return await self._reset_timeout_tasks_unlocked()

    async def retry_all_failed(self, batch_name: str = None):
        """将所有失败任务重新设为 pending（可按批次筛选）"""
        async with self._write_lock:
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            if batch_name:
                await self._db.execute(
                    """UPDATE tasks SET status = 'pending', worker_id = NULL, updated_at = ?
                       WHERE status = 'failed' AND batch_name = ? AND retry_count < ?""",
                    (now, batch_name, config.MAX_RETRIES)
                )
            else:
                await self._db.execute(
                    """UPDATE tasks SET status = 'pending', worker_id = NULL, updated_at = ?
                       WHERE status = 'failed' AND retry_count < ?""",
                    (now, config.MAX_RETRIES)
                )
            await self._db.commit()

    async def prioritize_batch(self, batch_name: str, priority: int = 10):
        """将批次中所有 pending 任务的优先级设为指定值"""
        async with self._write_lock:
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            result = await self._db.execute(
                """UPDATE tasks SET priority = ?, updated_at = ?
                   WHERE batch_name = ? AND status = 'pending'""",
                (priority, now, batch_name)
            )
            await self._db.commit()
        return result.rowcount

    async def release_tasks(self, task_ids: List[int]):
        """
        将指定任务从 processing 释放回 pending
        用于优先采集时清空队列后立即归还旧任务，避免等 5 分钟超时
        """
        if not task_ids:
            return 0
        async with self._write_lock:
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            placeholders = ",".join(["?"] * len(task_ids))
            result = await self._db.execute(
                f"""UPDATE tasks SET status = 'pending', worker_id = NULL, updated_at = ?
                   WHERE id IN ({placeholders}) AND status = 'processing'""",
                [now] + list(task_ids)
            )
            await self._db.commit()
        return result.rowcount

    async def update_screenshot_path(self, batch_name: str, asin: str, path: str):
        """更新结果记录的截图路径"""
        async with self._write_lock:
            await self._db.execute(
                "UPDATE results SET screenshot_path = ? WHERE batch_name = ? AND asin = ?",
                (path, batch_name, asin)
            )
            await self._db.commit()

    # ==================== 结果操作 ====================

    async def save_result(self, result_data: Dict[str, Any]) -> int:
        """
        保存采集结果
        使用 INSERT OR REPLACE 避免重复（基于 batch_name + asin 唯一索引）
        """
        # 构建字段列表
        fields = ["batch_name", "asin"] + [f for f in RESULT_FIELDS if f != "asin"]
        values = [result_data.get(f, "") for f in fields]
        placeholders = ",".join(["?"] * len(fields))
        field_names = ",".join(fields)

        async with self._write_lock:
            await self._db.execute(
                f"INSERT OR REPLACE INTO results ({field_names}) VALUES ({placeholders})",
                values
            )
            await self._db.commit()
        return self._db.total_changes

    async def get_results(self, batch_name: str = None, page: int = 1, per_page: int = 50,
                          search: str = None) -> tuple:
        """
        获取采集结果（分页）
        返回: (results_list, total_count)
        """
        conditions = []
        params = []

        if batch_name:
            conditions.append("batch_name = ?")
            params.append(batch_name)
        if search:
            conditions.append("(asin LIKE ? OR title LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # 获取总数
        async with self._db.execute(
            f"SELECT COUNT(*) as cnt FROM results {where}", params
        ) as cursor:
            row = await cursor.fetchone()
            total = row["cnt"]

        # 获取分页数据
        offset = (page - 1) * per_page
        async with self._db.execute(
            f"SELECT * FROM results {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [per_page, offset]
        ) as cursor:
            rows = await cursor.fetchall()
            results = [dict(row) for row in rows]

        return results, total

    async def get_result_by_asin(self, batch_name: str, asin: str) -> Optional[Dict]:
        """获取单个 ASIN 的采集结果"""
        async with self._db.execute(
            "SELECT * FROM results WHERE batch_name = ? AND asin = ? ORDER BY id DESC LIMIT 1",
            (batch_name, asin)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_all_results(self, batch_name: str) -> List[Dict]:
        """获取某批次的全部结果（用于导出）"""
        async with self._db.execute(
            "SELECT * FROM results WHERE batch_name = ? ORDER BY id ASC",
            (batch_name,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # ==================== 统计与进度 ====================

    async def get_progress(self, batch_name: str = None) -> Dict:
        """
        获取采集进度统计
        返回: {total, pending, processing, done, failed, success_rate}
        """
        if batch_name:
            condition = "WHERE batch_name = ?"
            params = (batch_name,)
        else:
            condition = ""
            params = ()

        async with self._db.execute(
            f"""SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM tasks {condition}""",
            params
        ) as cursor:
            row = await cursor.fetchone()

        total = row["total"] or 0
        done = row["done"] or 0
        failed = row["failed"] or 0

        completed = done + failed
        return {
            "total": total,
            "pending": row["pending"] or 0,
            "processing": row["processing"] or 0,
            "done": done,
            "failed": failed,
            "success_rate": round(done / completed * 100, 1) if completed > 0 else 0,
            "completion_rate": round(done / total * 100, 1) if total > 0 else 0,
        }

    async def get_batch_list(self) -> List[Dict]:
        """获取所有批次列表及其进度"""
        async with self._db.execute(
            """SELECT
                batch_name,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                MIN(created_at) as created_at
            FROM tasks
            GROUP BY batch_name
            ORDER BY MIN(created_at) DESC"""
        ) as cursor:
            rows = await cursor.fetchall()
            batches = []
            for row in rows:
                total = row["total"]
                done = row["done"] or 0
                failed = row["failed"] or 0
                completed = done + failed
                batches.append({
                    "batch_name": row["batch_name"],
                    "total": total,
                    "done": done,
                    "failed": failed,
                    "pending": row["pending"] or 0,
                    "processing": row["processing"] or 0,
                    "created_at": row["created_at"],
                    "progress": round(done / total * 100, 1) if total > 0 else 0,
                    "success_rate": round(done / completed * 100, 1) if completed > 0 else 0,
                })
            return batches

    async def get_error_summary(self, batch_name: str = None) -> Dict[str, int]:
        """获取失败任务的错误类型统计"""
        if batch_name:
            condition = "WHERE status = 'failed' AND batch_name = ?"
            params = (batch_name,)
        else:
            condition = "WHERE status = 'failed'"
            params = ()

        async with self._db.execute(
            f"""SELECT COALESCE(error_type, 'unknown') as etype, COUNT(*) as cnt
                FROM tasks {condition}
                GROUP BY error_type
                ORDER BY cnt DESC""",
            params
        ) as cursor:
            rows = await cursor.fetchall()
            return {row["etype"]: row["cnt"] for row in rows}

    async def get_failed_tasks(self, batch_name: str, limit: int = 50) -> List[Dict]:
        """获取批次中失败任务的详情（含错误类型）"""
        async with self._db.execute(
            """SELECT asin, error_type, error_detail, retry_count, updated_at
               FROM tasks
               WHERE status = 'failed' AND batch_name = ?
               ORDER BY updated_at DESC
               LIMIT ?""",
            (batch_name, limit)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def batch_submit_results(self, results_list: List[Dict], result_fields: List[str]) -> int:
        """
        批量提交采集结果（写锁保护，防止与 pull_tasks 事务冲突）

        Args:
            results_list: [{"task_id": int, "worker_id": str, "success": bool, "result": dict, ...}]
            result_fields: 结果表的字段列表

        Returns:
            成功处理的条目数
        """
        if not results_list:
            return 0

        async with self._write_lock:
            try:
                for item in results_list:
                    task_id = item.get("task_id")
                    worker_id = item.get("worker_id", "unknown")
                    success = item.get("success", False)
                    result_data = item.get("result")
                    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                    if success and result_data:
                        # 保存结果
                        fields = ["batch_name", "asin"] + [f for f in result_fields if f != "asin"]
                        values = [result_data.get(f, "") for f in fields]
                        placeholders = ",".join(["?"] * len(fields))
                        field_names = ",".join(fields)
                        await self._db.execute(
                            f"INSERT OR REPLACE INTO results ({field_names}) VALUES ({placeholders})",
                            values
                        )
                        # 标记任务完成
                        await self._db.execute(
                            "UPDATE tasks SET status = 'done', worker_id = ?, updated_at = ? WHERE id = ?",
                            (worker_id, now, task_id)
                        )
                    else:
                        # 标记任务失败（含错误分类）
                        error_type = item.get("error_type")
                        error_detail = item.get("error_detail")
                        await self._db.execute(
                            """UPDATE tasks
                               SET status = 'failed', worker_id = ?, retry_count = retry_count + 1,
                                   error_type = ?, error_detail = ?, updated_at = ?
                               WHERE id = ?""",
                            (worker_id, error_type, error_detail, now, task_id)
                        )

                # 整批统一 commit（在锁内完成，不会被其他操作打断）
                await self._db.commit()
            except Exception:
                try:
                    await self._db.rollback()
                except Exception:
                    pass
                raise

        return len(results_list)

    async def delete_batch(self, batch_name: str):
        """删除整个批次（任务 + 结果）"""
        async with self._write_lock:
            await self._db.execute("DELETE FROM tasks WHERE batch_name = ?", (batch_name,))
            await self._db.execute("DELETE FROM results WHERE batch_name = ?", (batch_name,))
            await self._db.commit()

    async def clear_all(self) -> Dict[str, int]:
        """清空所有数据（tasks + results 表）"""
        async with self._write_lock:
            async with self._db.execute("SELECT COUNT(*) as cnt FROM tasks") as cursor:
                tasks_count = (await cursor.fetchone())["cnt"]
            async with self._db.execute("SELECT COUNT(*) as cnt FROM results") as cursor:
                results_count = (await cursor.fetchone())["cnt"]
            await self._db.execute("DELETE FROM tasks")
            await self._db.execute("DELETE FROM results")
            await self._db.commit()
        return {"tasks_deleted": tasks_count, "results_deleted": results_count}


# ==================== 便捷函数 ====================

_db_instance: Optional[Database] = None
_db_init_lock: Optional[asyncio.Lock] = None


def _get_init_lock() -> asyncio.Lock:
    """延迟创建 Lock（必须在 event loop 存在后调用）"""
    global _db_init_lock
    if _db_init_lock is None:
        _db_init_lock = asyncio.Lock()
    return _db_init_lock


async def get_db() -> Database:
    """获取全局数据库实例（异步安全单例）"""
    global _db_instance
    if _db_instance is None:
        async with _get_init_lock():
            if _db_instance is None:
                _db_instance = Database()
                await _db_instance.connect()
    return _db_instance


async def close_db():
    """关闭全局数据库连接"""
    global _db_instance
    if _db_instance:
        await _db_instance.close()
        _db_instance = None
