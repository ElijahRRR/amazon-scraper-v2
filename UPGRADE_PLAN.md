# Amazon Scraper V2 - 功能升级执行计划

本文档详述了为 `amazon-scraper-v2` 新增三大核心功能（异步网页截图、任务优先级调度、按批次查看结果）的具体执行方案。请按照以下步骤和逻辑指导 Claude Code 完成代码改造。

---

## 阶段一：按需网页截图存证 (HTML-to-Image Snapshot)

**目标**：在不阻塞主采集流水线的前提下，将抓取到的 HTML 保存并渲染为图片存证。

### 1. 数据库与数据结构改造
- **`database.py`**:
  - 修改 `create_tables` 方法，在 `tasks` 表和 `results` 表中各增加一列 `needs_screenshot` (BOOLEAN DEFAULT 0)。
  - 在 `results` 表中增加一列 `screenshot_path` (TEXT)。
- **`server.py`**:
  - 修改 `upload_asin_file` 接口，接收前端表单传来的 `needs_screenshot` (Form(False)) 参数，并在批量写入 `tasks` 时存入该字段。
  - 新增 API `/api/tasks/screenshot` (POST)，用于接收 Worker 传回的截图图片文件，保存在 Server 的 `static/screenshots/` 目录下，并更新 `results` 表的 `screenshot_path`。

### 2. 前端改造
- **`templates/tasks.html`**:
  - 在 `#uploadForm` 中新增一个 Switch 开关（Checkbox）：`开启网页截图取证 (可能增加带宽消耗)`。
- **`templates/results.html`**:
  - 在结果表格的最后增加一列“截图”。如果 `screenshot_path` 有值，则显示一个“🖼️ 查看”的按钮，点击可在 Modal (弹窗) 中查看原图。

### 3. Worker 端异步渲染引擎 (核心难点)
- **环境依赖**:
  - 需要在 `requirements-worker.txt` 中增加 `imgkit`。
  - *系统要求*：运行 Worker 的机器需要安装 `wkhtmltopdf` 工具。
- **`worker.py`**:
  - 初始化时，增加一个专用的低优先级异步队列 `self._screenshot_queue = asyncio.Queue()`。
  - 新增一个后台常驻协程 `_screenshot_worker(self)`：
    - 持续从 `_screenshot_queue` 取出 `(task_id, html_content, asin)`。
    - 调用 `imgkit` 将 `html_content` 转换为图片二进制流。
    - 将图片通过 POST 请求发送给 Server 新增的 `/api/tasks/screenshot` 接口。
  - **在 `_process_task` 中的修改**：
    - 如果当前 `task.get('needs_screenshot')` 为真，在成功获取 `resp.text` 后，**非阻塞地**将其扔进 `_screenshot_queue`。不要等待渲染完成，直接返回结果进入正常提交流程。

---

## 阶段二：支持按批次优先采集 (Priority Queuing)

**目标**：允许用户强行插队某个批次，让 Worker 秒级切换到新批次任务。

### 1. 数据库改造
- **`database.py`**:
  - 在 `tasks` 表中增加一列 `priority` (INTEGER DEFAULT 0)。
  - 修改 `pull_tasks` SQL 语句：
    ```sql
    ORDER BY priority DESC, id ASC
    ```

### 2. 前端与 Server 接口
- **`server.py`**:
  - 新增 API `/api/batches/{batch_name}/prioritize` (POST)，将该批次下所有 `status='pending'` 的记录的 `priority` 更新为 `1`（或时间戳）。
- **`templates/tasks.html`**:
  - 在批次列表的操作按钮区，新增一个红色的“🚀 优先采集”按钮。点击后调用上述 API。

### 3. Worker 端的“秒级切换”逻辑
- **`worker.py`**:
  - 目前 Worker 有一个 `self._task_queue`（流水线缓冲队列，可能积压了几十个旧任务）。
  - 在 `_task_feeder` 协程拉取到新任务后，检查新任务的 `priority` 字段。
  - **关键判断**：如果发现刚拉到的任务 `priority > 0`，且这是本次新发现的插队任务：
    1. 立即清空当前的 `self._task_queue`。
    2. 将被清空弹出的任务通过 API (`/api/tasks/return`) 退回给 Server，将它们的状态重新标为 `pending`。
    3. 将新的高优先级任务装入队列。
  - *或者更简单的退回策略*：依靠现有的超时回收机制，被丢弃在内存里的旧任务，5 分钟后 Server 会自动把它们变回 `pending`，这样无需额外写 API，直接 `self._task_queue.empty()` 即可。

---

## 阶段三：Server 端支持按项目（批次）筛选结果

**目标**：Results 页面告别大杂烩，实现精准的批次隔离视图。

### 1. 后端接口适配
- **`server.py`**:
  - 现有的 `/api/results` 和 `/api/progress` 接口已经支持 `?batch_name=xxx` 传参，确保它们逻辑健壮。

### 2. 前端页面改造
- **`templates/results.html`**:
  - **增加选择器**：在页面顶部工具栏，新增一个 `<select id="batchFilter" class="form-select">`。
  - **动态加载选项**：页面加载时，请求 `/api/batches` 填充下拉菜单，首项为“全部批次”。
  - **联动刷新逻辑**：
    - 当用户切换下拉菜单时，触发 JS 函数重新获取数据。
    - 更新顶部的 4 个统计卡片（总任务数、成功数、失败数、采集进度），使这些数据**仅反映当前选中的批次**。
    - 更新下方的数据表格，仅展示选中批次的数据。
  - **导出按钮改造**：
    - 原本的“导出 Excel”按钮变为动态链接。如果下拉框选了 `batch_A`，点击导出的就是 `/api/export/batch_A`。

---

## 提示 Claude 的关键提示词 (Prompts)

当您准备让 Claude 开始编码时，可以分批发送以下指令：

**第一步指令（打基础）：**
> "我们需要修改 `database.py` 和 `server.py`。请在 tasks 表中新增 `needs_screenshot` (BOOL) 和 `priority` (INT) 字段，在 results 表新增 `needs_screenshot` 和 `screenshot_path` (TEXT) 字段。然后修改 upload 接口支持接收 `needs_screenshot` 参数。"

**第二步指令（实现截图流水线）：**
> "修改 `worker.py`，新增基于 `imgkit` 的异步截图队列。主采集协程拿到 HTML 后立刻放入队列，由后台专门的协程渲染成图片并 POST 给 Server。这绝对不能阻塞主采集的 `Semaphore`。"

**第三步指令（实现优先切换）：**
> "在 `worker.py` 的 `_task_feeder` 中，如果拉取到的任务带有 `priority=1`，请直接清空当前的 `task_queue`，让高优任务立刻开始执行。被清空的任务直接丢弃，依靠后端的 5 分钟超时机制自动回收。"

**第四步指令（UI 改造）：**
> "修改 `tasks.html` 和 `results.html`。实现上传时的复选框、批次列表的'优先采集'按钮，以及结果页的下拉筛选和动态统计面板。结果列表要新增一列显示截图的查看按钮。"