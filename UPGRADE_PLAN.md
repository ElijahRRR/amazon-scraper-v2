# Amazon Scraper V2 - 功能升级执行计划 (修订版)

本文档详述了为 `amazon-scraper-v2` 新增三大核心功能的具体执行方案。
实施顺序：阶段三 → 阶段二 → 阶段一（按复杂度递增）。

---

## 阶段三：Server 端支持按项目（批次）筛选结果（纯前端改造）

**目标**：Results 页面告别大杂烩，实现精准的批次隔离视图。

### 后端
- 现有的 `/api/results?batch_name=xxx` 和 `/api/progress/{batch_name}` 已支持，无需改动。

### 前端页面改造 (`templates/results.html`)
- **AJAX 动态筛选**：将当前的 `<form>` 整页提交改为 JS `fetch` 动态刷新，切换批次无需刷新页面。
- **统计卡片**：在页面顶部新增 4 个统计卡片（总任务数、成功数、失败数、采集进度），随批次切换动态更新。
- **动态导出按钮**：导出 Excel/CSV 按钮始终显示，根据当前选中批次动态拼接 URL。
- **分页保持**：AJAX 分页，保持当前筛选条件。

---

## 阶段二：支持按批次优先采集 (Priority Queuing)

**目标**：允许用户强行插队某个批次，让 Worker 秒级切换到新批次任务。

### 1. 数据库改造 (`database.py`)
- 在 `tasks` 表中增加一列 `priority` (INTEGER DEFAULT 0)。
- 修改 `pull_tasks` SQL：`ORDER BY priority DESC, id ASC`。

### 2. Server 接口 (`server.py`)
- 新增 API `POST /api/batches/{batch_name}/prioritize`，将该批次下所有 `status='pending'` 的记录的 `priority` 设为 `10`。

### 3. Worker 端秒级切换 (`worker.py`)
- `_task_feeder` 拉取到高优先级任务时（`priority > 0`），立即清空当前 `_task_queue`。
- 被清空的旧任务直接丢弃，依靠后端 5 分钟超时机制（`reset_timeout_tasks`）自动回收为 `pending`。
- 无需新增退回 API。

### 4. 前端 (`templates/tasks.html`)
- 在批次操作按钮区新增"优先采集"按钮，点击后调用 prioritize API。

---

## 阶段一：按需网页截图存证 (Playwright 渲染)

**目标**：在不阻塞主采集流水线的前提下，将抓取到的 HTML 用 Playwright 渲染为 PNG 截图存证。

### 技术方案
- **渲染引擎**：Playwright + Chromium (headless)，替代原计划的 imgkit/wkhtmltopdf（后者无法渲染现代页面）。
- **运行位置**：Worker 端。HTML 已在内存中，直接渲染，避免双重传输。
- **渲染策略**：`page.route()` 拦截主文档请求，注入保存的 HTML，外部 CSS/图片正常从 CDN 加载。
- **并发控制**：每 Worker 串行 1 个渲染任务，避免 Chrome 占用过多内存。

### 1. 数据库改造 (`database.py`)
- `tasks` 表增加 `needs_screenshot` (BOOLEAN DEFAULT 0)。
- `results` 表增加 `screenshot_path` (TEXT)。

### 2. Server 改造 (`server.py`)
- `upload_asin_file` 接口接收 `needs_screenshot` 参数，写入 tasks。
- 新增 `POST /api/tasks/screenshot`：接收 Worker 传回的 PNG 截图（multipart），保存到 `static/screenshots/{batch_name}/`，更新 results 表。
- `pull_tasks` 返回数据增加 `needs_screenshot` 字段。

### 3. Worker 改造 (`worker.py`)
- 新增 `_screenshot_queue = asyncio.Queue(maxsize=50)`。
- 新增 `_screenshot_worker()` 后台常驻协程：
  - 从队列取出 `(task_id, html_content, asin, batch_name)`。
  - 用 Playwright 渲染为 PNG。
  - POST 图片给 Server。
- `_process_task` 中：如果 `task.get('needs_screenshot')` 为真，成功获取 HTML 后 `put_nowait()` 到截图队列（非阻塞）。

### 4. 前端改造
- `tasks.html`：上传表单新增"开启网页截图取证"开关。
- `results.html`：结果表格新增"截图"列，有截图时显示"查看"按钮（Modal 预览 + 下载）。

### 5. 环境依赖
```
# requirements-worker.txt 新增
playwright>=1.40.0

# 系统级安装
playwright install chromium
```

---

## 实施顺序

| 顺序 | 阶段 | 改动范围 | 复杂度 |
|------|------|----------|--------|
| 1 | 阶段三：批次筛选 | 纯前端 | 低 |
| 2 | 阶段二：优先调度 | DB + Server + Worker + 前端 | 中 |
| 3 | 阶段一：截图存证 | DB + Server + Worker + 前端 + 新依赖 | 高 |
