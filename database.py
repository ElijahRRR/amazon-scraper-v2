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

import config
from models import Task, Result, RESULT_FIELDS


class Database:
    """异步 SQLite 数据库管理器"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        self._db: Optional[aiosqlite.Connection] = None

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
                worker_id TEXT,
                retry_count INTEGER DEFAULT 0,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 索引
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_batch ON tasks(batch_name);
            CREATE INDEX IF NOT EXISTS idx_tasks_batch_asin ON tasks(batch_name, asin);
            CREATE INDEX IF NOT EXISTS idx_results_batch ON results(batch_name);
            CREATE INDEX IF NOT EXISTS idx_results_asin ON results(asin);
        """)
        await self._db.commit()

    # ==================== 任务操作 ====================

    async def create_tasks(self, batch_name: str, asins: List[str], zip_code: str = "10001") -> int:
        """
        批量创建采集任务
        返回: 实际插入的任务数（跳过已存在的）
        """
        inserted = 0
        for asin in asins:
            asin = asin.strip()
            if not asin:
                continue
            try:
                await self._db.execute(
                    "INSERT OR IGNORE INTO tasks (batch_name, asin, zip_code) VALUES (?, ?, ?)",
                    (batch_name, asin, zip_code)
                )
                inserted += 1
            except Exception:
                pass
        await self._db.commit()
        return inserted

    async def pull_tasks(self, worker_id: str, count: int = 10) -> List[Dict]:
        """
        Worker 拉取待处理任务
        原子操作：SELECT + UPDATE 在同一事务中
        """
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        tasks = []

        # 先回退超时任务
        await self.reset_timeout_tasks()

        async with self._db.execute(
            """SELECT id, batch_name, asin, zip_code, retry_count
               FROM tasks
               WHERE status = 'pending'
               ORDER BY id ASC
               LIMIT ?""",
            (count,)
        ) as cursor:
            rows = await cursor.fetchall()

        if not rows:
            return tasks

        ids = []
        for row in rows:
            task = {
                "id": row["id"],
                "batch_name": row["batch_name"],
                "asin": row["asin"],
                "zip_code": row["zip_code"],
                "retry_count": row["retry_count"],
            }
            tasks.append(task)
            ids.append(row["id"])

        # 批量更新状态为 processing
        if ids:
            placeholders = ",".join(["?"] * len(ids))
            await self._db.execute(
                f"""UPDATE tasks
                    SET status = 'processing', worker_id = ?, updated_at = ?
                    WHERE id IN ({placeholders})""",
                [worker_id, now] + ids
            )
            await self._db.commit()

        return tasks

    async def update_task_status(self, task_id: int, status: str, worker_id: str = None):
        """更新单个任务状态"""
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        await self._db.execute(
            "UPDATE tasks SET status = ?, worker_id = ?, updated_at = ? WHERE id = ?",
            (status, worker_id, now, task_id)
        )
        await self._db.commit()

    async def mark_task_done(self, task_id: int, worker_id: str = None):
        """标记任务完成"""
        await self.update_task_status(task_id, "done", worker_id)

    async def mark_task_failed(self, task_id: int, worker_id: str = None):
        """标记任务失败，增加重试次数"""
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        await self._db.execute(
            """UPDATE tasks
               SET status = 'failed', worker_id = ?, retry_count = retry_count + 1, updated_at = ?
               WHERE id = ?""",
            (worker_id, now, task_id)
        )
        await self._db.commit()

    async def retry_failed_task(self, task_id: int):
        """将失败任务重新设为 pending"""
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        await self._db.execute(
            "UPDATE tasks SET status = 'pending', worker_id = NULL, updated_at = ? WHERE id = ? AND status = 'failed'",
            (now, task_id)
        )
        await self._db.commit()

    async def reset_timeout_tasks(self):
        """
        断点续传：将 processing 超过 5 分钟的任务回退为 pending
        防止 worker 崩溃后任务永远卡在 processing
        """
        timeout = config.TASK_TIMEOUT_MINUTES
        cutoff = (datetime.utcnow() - timedelta(minutes=timeout)).strftime('%Y-%m-%d %H:%M:%S')
        result = await self._db.execute(
            """UPDATE tasks
               SET status = 'pending', worker_id = NULL, updated_at = CURRENT_TIMESTAMP
               WHERE status = 'processing' AND updated_at < ?""",
            (cutoff,)
        )
        if result.rowcount > 0:
            await self._db.commit()
        return result.rowcount

    async def retry_all_failed(self, batch_name: str = None):
        """将所有失败任务重新设为 pending（可按批次筛选）"""
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

    # ==================== 结果操作 ====================

    async def save_result(self, result_data: Dict[str, Any]) -> int:
        """
        保存采集结果
        使用 INSERT OR REPLACE 避免重复
        """
        # 构建字段列表
        fields = ["batch_name", "asin"] + [f for f in RESULT_FIELDS if f != "asin"]
        values = [result_data.get(f, "") for f in fields]
        placeholders = ",".join(["?"] * len(fields))
        field_names = ",".join(fields)

        await self._db.execute(
            f"INSERT INTO results ({field_names}) VALUES ({placeholders})",
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

        return {
            "total": total,
            "pending": row["pending"] or 0,
            "processing": row["processing"] or 0,
            "done": done,
            "failed": failed,
            "success_rate": round(done / total * 100, 1) if total > 0 else 0,
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
                batches.append({
                    "batch_name": row["batch_name"],
                    "total": total,
                    "done": done,
                    "failed": row["failed"] or 0,
                    "pending": row["pending"] or 0,
                    "processing": row["processing"] or 0,
                    "created_at": row["created_at"],
                    "progress": round(done / total * 100, 1) if total > 0 else 0,
                })
            return batches

    async def delete_batch(self, batch_name: str):
        """删除整个批次（任务 + 结果）"""
        await self._db.execute("DELETE FROM tasks WHERE batch_name = ?", (batch_name,))
        await self._db.execute("DELETE FROM results WHERE batch_name = ?", (batch_name,))
        await self._db.commit()


# ==================== 便捷函数 ====================

_db_instance: Optional[Database] = None


async def get_db() -> Database:
    """获取全局数据库实例（单例）"""
    global _db_instance
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
