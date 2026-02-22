# Project Progress

## Current Status
- Total: 14 features
- Passing: 14 / 14 (100%)
- Status: COMPLETE

## Session Log
### Session 1 - 2026-02-22 三大功能升级

**Phase 3: 批次筛选结果 (Features #1-3)**
- results.html 全面改造为 AJAX 动态筛选
- 新增 4 个统计卡片，随批次切换动态更新
- 导出按钮始终可见，动态链接
- server.py results_page 传递 progress 数据

**Phase 2: 优先级调度 (Features #4-7)**
- database.py: tasks 表新增 priority 列，pull_tasks ORDER BY priority DESC
- server.py: 新增 POST /api/batches/{name}/prioritize
- worker.py: _task_feeder 检测高优先级任务，清空旧队列
- tasks.html: 新增闪电图标优先采集按钮

**Phase 1: 截图存证 (Features #8-14)**
- database.py: tasks 表新增 needs_screenshot，results 表新增 screenshot_path
- database.py: create_tasks 接收 needs_screenshot，新增 prioritize_batch/update_screenshot_path
- server.py: upload 接口接收 needs_screenshot，新增截图上传 API
- worker.py: 新增 _screenshot_queue + _screenshot_worker (Playwright 渲染)
- worker.py: _process_task 成功后 put_nowait 到截图队列（非阻塞）
- tasks.html: 上传表单新增截图开关
- results.html: 新增截图列 + Modal 预览/下载

### Session 2 - 2026-02-22 收尾修复

**数据库迁移:**
- database.py init_tables() 新增 ALTER TABLE ADD COLUMN 迁移逻辑
- 现有数据库自动添加 priority / needs_screenshot / screenshot_path 列
- ~~不再需要手动删除 scraper.db 重建~~

**依赖更新:**
- requirements-worker.txt 新增 playwright>=1.40.0

**部署提醒:**
- Worker 截图功能需要 `pip install playwright && playwright install chromium`
