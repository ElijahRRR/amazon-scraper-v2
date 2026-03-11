# Amazon Product Scraper v2

高性能 Amazon 商品数据采集系统。采用分布式架构，支持多 Worker 并行采集，通过 TLS 指纹模拟和隧道代理实现稳定的反反爬。

## 系统架构

```
┌─────────────────────────────────────────────────────┐
│                   Web UI (浏览器)                      │
│         仪表盘 / 任务管理 / 结果浏览 / 设置             │
└───────────────────────┬─────────────────────────────┘
                        │ HTTP
┌───────────────────────▼─────────────────────────────┐
│              Server (FastAPI :8899)                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │ 任务分发  │  │ 结果入库  │  │ 全局并发协调器    │   │
│  └────┬─────┘  └────▲─────┘  │ 预算分配+封锁传播 │   │
│       │             │        └────────┬─────────┘   │
│  ┌────▼─────────────┴────┐            │              │
│  │   SQLite (WAL+5s锁等待)│            │              │
│  │  tasks + results 表   │            │              │
│  └───────────────────────┘            │              │
└───────────────────────┬───────────────┘──────────────┘
                        │ REST API (含配额分配)
         ┌──────────────┼──────────────┐
         │              │              │
┌────────▼───┐  ┌───────▼────┐  ┌─────▼──────┐
│  Worker 1  │  │  Worker 2  │  │  Worker N  │
│ curl_cffi  │  │ curl_cffi  │  │ curl_cffi  │
│ 截图:开启   │  │ 截图:关闭   │  │ 截图:关闭   │
│     │ HTML │  └──────┬─────┘  └──────┬─────┘
│  ┌──▼────┐ │         │               │
│  │截图子 │ │         └───────┼───────┘
│  │进程   │ │                 │
│  │Playw. │ │        ┌────────▼────────┐
│  └───────┘ │        │   快代理 TPS     │
└────────────┘        │   隧道代理       │
                      │  (自动换 IP)     │
                      └────────┬────────┘
                               │
                      ┌────────▼────────┐
                      │   Amazon.com    │
                      └─────────────────┘
```

### 数据流

1. 用户通过 Web UI 上传 ASIN 文件 → Server 创建采集任务入库
2. Worker 从 Server 拉取待处理任务（`/api/tasks/pull`），优先任务独占队列（数据库层面隔离最高优先级）
3. Worker 通过隧道代理向 Amazon 发起请求，解析 HTML 提取数据
4. Worker 将采集结果批量推送回 Server（`/api/tasks/result/batch`）
5. 若任务需要截图且 Worker 开启截图功能：HTML 写入磁盘 → 截图子进程渲染 → **逐张立即上传** Server → 删除 HTML
6. 截图批次全部上传完成前，Worker 暂停拉取新任务（门控机制）
7. 用户在 Web UI 查看进度、浏览结果、导出 Excel/CSV/数据库文件

## 技术栈

| 组件 | 技术 | 用途 |
|------|------|------|
| HTTP 客户端 | `curl_cffi` (AsyncSession) | TLS/JA3 指纹模拟，HTTP/2 多路复用 |
| 浏览器指纹 | `impersonate="chrome131"` | 模拟 Chrome 131 的完整 TLS 握手特征 |
| 内部 HTTP | `httpx` (AsyncClient) | Worker↔Server 通信（拉取任务、提交结果） |
| HTML 解析 | `selectolax` (Lexbor) / `lxml` | selectolax 比 lxml 快 2-5x，lxml 作 fallback |
| Web 框架 | FastAPI + Uvicorn | 异步 REST API 和 Web UI |
| 数据库 | SQLite (WAL 模式) + `aiosqlite` | 异步读写，WAL 支持并发，busy_timeout=5s 防锁冲突 |
| 代理 | 快代理 TPS 隧道代理 | 固定入口，服务端自动换 IP |
| 压缩 | Brotli (`br`) | 比 gzip 小 15-25%，减少传输量 |
| 模板 | Jinja2 | Web UI 页面渲染 |
| 导出 | openpyxl / csv / SQLite | Excel (.xlsx)、CSV、数据库文件导出 |
| 截图存证 | Playwright (Chromium) | 可选功能，渲染商品页截图用于数据抽查 |

## 反反爬策略

### TLS 指纹模拟

普通 Python HTTP 库（requests/httpx/aiohttp）的 TLS 握手特征与浏览器不同，Amazon 通过 JA3 指纹检测即可识别。`curl_cffi` 通过 `impersonate` 参数完整模拟 Chrome 的 TLS 握手过程（密码套件、扩展顺序、ALPN 等），使请求在网络层面与真实浏览器无法区分。

### HTTP/2 多路复用

启用 `http_version=2`，多个请求共享同一条 TCP+TLS 连接，减少连接建立开销，同时降低被代理端检测到多连接的风险。

### Session 主动轮换

每完成 1000 次成功请求，主动关闭当前 Session 并创建新的。更换 User-Agent、重新获取代理 IP、重新初始化 Cookie，防止 Amazon 通过行为指纹（请求频率、Cookie 老化等）识别。

### 请求头仿真

- 15 个真实 Chrome 131 User-Agent 轮换
- 完整的 `sec-ch-ua` / `sec-ch-ua-platform` 客户端提示头（与 `impersonate=chrome131` 保持版本一致）
- 正确的 Referer 链（首页 → 产品页）
- 标准的 `Sec-Fetch-*` 头组合

### 邮编设置

通过 POST 请求 `/gp/delivery/ajax/address-change.html` 设置配送邮编，而非伪造 Cookie。这确保 Amazon 返回正确的价格和配送信息。

## 核心模块

### `config.py` — 配置中心

所有可调参数集中管理。**代理凭证通过环境变量注入**，不硬编码在代码中。支持两种方式：

```bash
# 方式 1：环境变量
export KDL_SECRET_ID=your_secret_id
export KDL_SIGNATURE=your_signature

# 方式 2：.env 文件（自动加载，无需手动 source）
cp .env.example .env
# 编辑 .env 填入凭证
```

**基础参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MAX_CLIENTS` | 32 | HTTP/2 连接池大小（建议为并发数的 2 倍） |
| `REQUEST_TIMEOUT` | 15s | 单次请求超时（短超时更快释放并发槽位） |
| `MAX_RETRIES` | 3 | 最大重试次数 |
| `SESSION_ROTATE_EVERY` | 1000 | 每 N 次成功请求轮换 Session |
| `TOKEN_BUCKET_RATE` | 5.0 | 全局 QPS 上限（令牌桶速率，每秒请求数） |
| `IMPERSONATE_BROWSER` | chrome131 | TLS 指纹模拟目标 |
| `PROXY_REFRESH_INTERVAL` | 30s | 代理刷新间隔 |

**自适应并发控制参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `INITIAL_CONCURRENCY` | 8 | 冷启动并发数（保守开始，自动攀升） |
| `MIN_CONCURRENCY` | 4 | 并发下限 |
| `MAX_CONCURRENCY` | 16 | TPS 模式并发上限 |
| `TUNNEL_MAX_CONCURRENCY` | 20 | DPS 模式并发上限 |
| `ADJUST_INTERVAL_S` | 10s | 评估间隔 |
| `TARGET_LATENCY_S` | 6.0s | 目标 p50 延迟（低于此加速） |
| `MAX_LATENCY_S` | 10.0s | 延迟上限（超过则减速） |
| `TARGET_SUCCESS_RATE` | 95% | 成功率目标（高于此加速） |
| `MIN_SUCCESS_RATE` | 85% | 成功率下限（低于此减速） |
| `BLOCK_RATE_THRESHOLD` | 5% | 封锁率阈值（超过则紧急减半+冷却） |
| `COOLDOWN_AFTER_BLOCK_S` | 30s | 被封后冷却时间 |

**全局并发协调参数（多 Worker 场景）：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `GLOBAL_MAX_CONCURRENCY` | 50 | 所有 Worker 总并发上限（Server 按健康度分配） |
| `GLOBAL_MAX_QPS` | 4.5 | 所有 Worker 总 QPS 上限（Server 按健康度分配） |

### `session.py` — 会话管理

- `AmazonSession`：单个采集会话，维护独立的 Cookie jar 和代理连接
  - `initialize()`：访问首页获取 Cookie → POST 设置邮编，带 `asyncio.Lock` 防并发初始化 + 3 次重试
  - `is_ready()`：检查 session 是否已初始化并可用（Worker 在处理任务前调用）
  - `fetch_product_page(asin)`：采集商品详情页
  - `is_blocked(resp)`：检测 403/503/验证码拦截（含页面体积阈值，避免正常产品页误判）

### `worker.py` — 采集引擎（流水线 + 自适应并发）

分布式 Worker，可在多台机器上运行，采用流水线架构：

```
task_feeder → [task_queue] → worker_pool (N个协程) → [result_queue] → batch_submitter
                  ↑               │ (needs_screenshot && enable_screenshot)
       adaptive_controller        ↓
         实时调整 N          HTML→磁盘 → 截图子进程(独立PID) → 逐张上传Server
                                         ↓ _uploaded标记
                              gate_monitor → 开门放行
```

- **全异步架构**：Worker↔Server 通信（拉取任务、提交结果、同步设置）全部使用 `httpx.AsyncClient`，不阻塞 event loop
- **流水线架构**：任务补给、采集、结果提交三大协程并行，互不阻塞
- **自适应并发**：冷启动 8 并发，AIMD 算法根据延迟/成功率/封锁率自动探索最优并发数
- **实例级参数**：`max_retries` 等运行时可调参数存储在 Worker 实例上，热同步不污染全局 config
- **任务预取**：队列低于 50% 时触发补给，保持 Worker 始终有任务可做
- **优先任务独占**：数据库 `pull_tasks` 先查询 `MAX(priority)`，仅返回最高优先级任务，不混入普通任务
- **封锁处理**：检测到 403/503/验证码 → 自动轮换 Session（换 IP + 换 Cookie）
- **主动轮换**：每 1000 次成功请求主动更换 Session
- **截图开关**：`--no-screenshot` 参数可禁用截图功能，仅采集数据不启动截图子进程
- **启动设置全量同步**：Worker 启动时自动从 Server 拉取所有运行参数（并发控制、QPS、AIMD 参数、代理地址等），远程 Worker 无需本地 `.env` 或环境变量
- **全局协调同步**：每 30 秒通过 `POST /api/worker/sync` 上报 metrics 快照 + 接收 Server 分配的并发/QPS 配额 + 检测全局封锁事件，无需重启即可热更新所有参数
- **批量提交**：结果先入队列，攒满 10 条或每 2 秒批量 POST，失败自动 3 次重试后逐条回退
- **优雅退出**：SIGINT/SIGTERM 信号处理，退出前刷新队列

### `screenshot_worker.py` — 截图独立子进程

截图功能采用**进程级隔离**——采集 Worker 和截图渲染运行在两个独立的 Python 进程中，各自拥有独立的 asyncio 事件循环，彻底避免 CPU/事件循环争抢：

```
┌─────────────────────────┐    ┌──────────────────────────┐
│  Worker 主进程 (采集)     │    │  截图子进程 (独立 PID)     │
│  asyncio 事件循环 #1     │    │  asyncio 事件循环 #2      │
│                         │    │                          │
│  curl_cffi HTTP 请求     │    │  Playwright/Chromium 渲染  │
│  HTML 解析              │    │  渲染完立即上传 Server     │
│  ↓                      │    │  ↑                        │
│  写 HTML 到磁盘 ─────────┼──→─│  读 HTML 从磁盘            │
│                         │    │  ↓                        │
│  检查 _uploaded 标记 ←───┼──←─│  写 _uploaded 标记         │
└─────────────────────────┘    └──────────────────────────┘
```

**截图流程（逐张即时上传）：**

```
扫描 HTML 目录 → 发现待处理文件
      ↓
并发渲染（默认 3 并发 = 1 浏览器 × 3 页面）
      ↓
等待主图加载（最多 7 秒，自动检测 Amazon 主图选择器）
      ↓
截图（1280×1300px 固定尺寸）
      ↓
立即上传到 Server（失败重试 3 次）
      ↓
上传成功 → 删除 HTML 文件
上传失败 → 保留 HTML，下轮自动重试
渲染失败 → 删除 HTML（避免无限重试）
      ↓
检查批次完成（_scraping_done 存在 + 无剩余 HTML）
      ↓
写 _uploaded 标记 → 主 Worker 门控放行
```

**关键特性：**

- **进程隔离**：采集与截图各自独立的 asyncio 事件循环 + GC，互不干扰
- **逐张即时上传**：每张截图渲染完立即上传 Server，不等待批次完成，实时可查看
- **按需启动**：仅当任务标记 `needs_screenshot=True` 且 Worker 开启截图功能时才启动截图子进程
- **主图等待**：截图前等待 Amazon 主图（`#landingImage` 等）加载完成，最多 7 秒
- **内存回收**：每渲染 200 张自动重启 Chromium 浏览器实例，防止内存泄漏导致卡死
- **上传重试**：上传失败保留 HTML 文件，下一轮扫描自动重试；渲染失败则删除 HTML 避免无限重试
- **文件系统通信**：采集完成的 HTML 写入 `screenshot_cache/html/{batch}/`，截图子进程监控该目录自动渲染
- **批次门控**：截图全部上传完成后写入 `_uploaded` 标记 → 主 Worker 检测到标记后才拉取下一批次任务
- **固定截图尺寸**：1280×1300px viewport
- **`setContent()` 渲染**：直接将 HTML 注入 Page，无需网络导航
- **`<base>` 标签注入**：修复协议相对 URL 在 `about:blank` 上下文中无法加载的问题
- **空白截图检测**：PNG < 10KB 且页面无可见内容时自动丢弃
- **资源拦截**：屏蔽 JS、字体、追踪脚本，只加载 CSS 和图片
- **级联崩溃防护**：页面级错误仅关闭当前 Page；浏览器进程崩溃才触发实例重建
- 截图保存路径：Server 端 `static/screenshots/{batch_name}/{asin}.png`
- 截图为可选功能，需安装 `requirements-screenshot.txt` 中的依赖

**截图设置（Web UI 可调）：**

| 设置 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| 截图浏览器数 | 1 | 1-10 | Chromium 实例数量 |
| 单浏览器并发数 | 3 | 1-10 | 每个浏览器的最大并发 Page 数 |

> 总并发 = 浏览器数 × 单浏览器并发数。建议保持总并发 ≤ 6，避免 CPU/内存过载。

### `adaptive.py` — AIMD 自适应并发控制器

类 TCP 拥塞控制算法，根据实时指标动态调整并发数：

- **Additive Increase**：一切正常（成功率 ≥ 95%，p50 < 6s）→ 并发 +1
- **Multiplicative Decrease**：出问题 → 并发 ÷ 2
- **五级决策链**：封锁率超标 → 成功率/延迟 → 带宽饱和 → 冷却期 → 正常加速
- **安全 Semaphore 调整**：通过重建 Semaphore 实现缩容（不操作私有 `_value`），并发调整加 `asyncio.Lock` 保护原子性
- **冷却机制**：被封后 30 秒内不加速，防止抖动
- **抖动恢复**：每个 Worker 从 Server 获得不同的恢复抖动系数（0.0~1.0），加速分支以概率 `0.3 + 0.7 × jitter` 决定是否 +1，防止多 Worker 同步振荡

### `metrics.py` — 滑动窗口指标采集器

实时跟踪请求延迟、成功率、被封率、带宽使用等关键指标：

- 30 秒滑动窗口，自动清理过期记录，使用 `time.monotonic()` 避免系统时钟跳变影响
- 支持 p50/p95 延迟分位数计算（线性插值，精确计算）
- 带宽使用率跟踪（对比代理上限）
- 在飞请求数计数

### `parser.py` — HTML 解析器

解析 Amazon 商品页面，提取 37 个字段：

- 基础信息：标题、品牌、型号、制造商、原产国
- 价格数据：原价、现价、BuyBox 价格、运费、FBA 状态
- 库存配送：库存数量、库存状态、配送日期、配送时长
- 分类编码：BSR 排名、类目树、UPC/EAN、父体 ASIN、变体列表
- 规格参数：包装/商品尺寸重量、上架时间
- 内容：五点描述、长描述、图片 URL

**三层数据提取策略（稳定性优先）：**

```
JSON-LD（结构化数据）→ CSS/XPath（页面元素）→ 正则/JSON 文本
```

1. **JSON-LD 优先提取**：从 `<script type="application/ld+json">` 中提取 Schema.org Product 对象。这是 Google SEO 标准，Amazon 不会轻易改变结构，字段稳定性远高于 CSS class。
2. **CSS/XPath 补充**：对于 JSON-LD 缺失或不够精确的字段（如 BuyBox 价格、FBA 状态、配送日期），使用 CSS 选择器 / XPath 从页面 DOM 提取。
3. **正则兜底**：UPC/EAN、父体 ASIN、变体列表等通过正则从页面 JS 变量中提取。

**价格提取修复：**
- 限定在主商品容器内提取价格，排除推荐商品区域的价格干扰
- 无价格 + 无 BuyBox 时库存状态返回 "N/A" 而非默认 "In Stock"

**长描述 SEO 过滤：**
- 使用 `re.finditer` 逐段匹配，自动过滤包含 SEO 关键词（"sponsored", "related", "customers also" 等）的段落

| 字段 | 优先数据源 | 说明 |
|------|-----------|------|
| title, brand | JSON-LD | 最稳定，Amazon 改版不影响 |
| current_price | CSS → JSON-LD | CSS 能区分 BuyBox/原价，JSON-LD 做兜底 |
| stock_status | CSS → JSON-LD | CSS 有更细粒度文本，JSON-LD 只有 InStock/OutOfStock |
| image_urls | CSS → JSON-LD | CSS 从 JS 变量提取 hiRes 大图，JSON-LD 兜底 |
| ean_list | 合并 | CSS 正则 + JSON-LD gtin13 合并去重 |

解析引擎优先使用 selectolax（基于 Lexbor C 引擎），比 lxml 快 2-5 倍。

### `proxy.py` — 代理管理

快代理 TPS 隧道代理集成：

- 固定隧道入口地址，IP 轮换在快代理服务端透明进行
- 代理按需获取 + 定时过期（默认 30 秒有效期），使用 `time.monotonic()` 计时
- 全异步 `httpx.AsyncClient` 请求 API，不阻塞 event loop
- 被封时强制刷新，触发服务端分配新出口 IP，最多重试 3 次（指数退避）
- 线程安全（`asyncio.Lock`），避免多协程同时请求 API

### `database.py` — 数据库

SQLite WAL 模式 + `busy_timeout=5000ms`，两张核心表：

- `tasks`：采集任务队列（pending → processing → done/failed），含 `error_type` / `error_detail` 错误分类
- `results`：采集结果存储（37 个商品字段，`batch_name + asin` 唯一索引自动去重）

**优先级任务隔离：**

`pull_tasks` 方法先查询 `SELECT MAX(priority) FROM tasks WHERE status='pending'`，然后仅返回该优先级的任务。确保优先批次采集期间不会混入普通任务。

**并发写入保护：**

`PRAGMA busy_timeout=5000` 确保多 Worker 并发写入时，锁冲突会等待最多 5 秒而非立即失败，避免数据丢失。

**错误分类（error_type）：**

| error_type | 触发条件 | 处理建议 |
|------------|----------|----------|
| `blocked` | HTTP 403/503、API 封锁页面 | 降低并发 / 检查代理质量 |
| `captcha` | 验证码拦截（Robot Check） | 降低 QPS / 更换代理通道 |
| `timeout` | 请求超时 | 检查网络 / 增大超时时间 |
| `parse_error` | 页面为空、解析失败、标题为空、非美国价格 | 检查邮编设置 / 页面结构变化 |
| `network` | 连接异常、DNS 解析失败等 | 检查代理连通性 |

关键机制：
- 任务拉取为原子操作（`BEGIN IMMEDIATE` 写锁 + SELECT + UPDATE 同一事务，防并发重复分发）
- 批量创建任务使用 `executemany` 高效插入
- 结果保存使用 `INSERT OR REPLACE`，基于唯一索引自动覆盖旧数据
- 超时回退：processing 超过 5 分钟自动回退为 pending（防 Worker 崩溃丢任务）
- 失败重试：支持按批次重试所有失败任务
- 全局单例通过 `asyncio.Lock` 保证并发安全初始化

### `server.py` — API 服务

FastAPI 应用（`lifespan` 上下文管理生命周期），提供 REST API 和 Web UI：

**安全机制：**

- 上传文件类型白名单（仅允许 `.xlsx`/`.xls`/`.csv`/`.txt`）
- 批次名白名单净化（只允许字母、数字、`_`、`-`，防路径穿越）
- 截图路径使用 `os.path.realpath()` 越界检测，防止目录遍历攻击
- Worker 注册表自动清理：超过 10 分钟无心跳的 Worker 定时移除，防止无限增长

**全局并发协调（多 Worker 场景）：**

Server 端维护全局并发协调器，解决多 Worker 并发失控问题：

```
Worker A ──POST metrics──→ Server ──分配配额──→ Worker A (max_c=15, qps=2.0)
Worker B ──POST metrics──→ Server ──分配配额──→ Worker B (max_c=10, qps=1.5)
Worker C ──POST metrics──→ Server ──分配配额──→ Worker C (max_c=5,  qps=1.0)
                            ↑
                    全局预算 = 50 并发 / 4.5 QPS
                    按健康度加权分配
```

- **全局预算分配**：`GLOBAL_MAX_CONCURRENCY` 和 `GLOBAL_MAX_QPS` 定义总上限，Server 按 Worker 的成功率和封锁率加权分配配额
- **封锁传播**：任一 Worker 上报封锁率 > 5% → 触发全局冷却，所有 Worker 在下次 sync 时同步减半并发
- **抖动恢复**：每个 Worker 获得不同的恢复抖动系数（0.0~1.0），恢复时以不同概率加速，避免同步振荡
- **向后兼容**：单 Worker 模式获得 100% 预算；旧版 Server 无 `/api/worker/sync` 时 Worker 自动降级为 `GET /api/settings`

可通过 `GET /api/coordinator` 查看实时协调状态（总预算、已分配、每个 Worker 的配额和 metrics）。

**运行时设置热同步：**

通过 Web UI 的「设置」页面可实时修改以下参数，Worker 每 30 秒自动同步更新（无需重启）：

- 基础：邮编、最大重试、代理地址、Session 轮换频率
- 截图：浏览器实例数、单浏览器并发数
- 速率与并发：全局总并发上限、全局总 QPS 上限、单 Worker QPS（令牌桶速率）、初始/最小/最大并发
- AIMD 调控：评估间隔、目标延迟、延迟上限、目标成功率、成功率下限、封锁率阈值、冷却时间

设置使用版本号增量同步——Server 每次更新 `_settings_version` 递增，Worker 对比版本号跳过无变化的轮询。

**API 端点：**

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/upload` | 上传 ASIN 文件（Excel/CSV/TXT，白名单类型校验） |
| GET | `/api/tasks/pull` | Worker 拉取待处理任务（优先级隔离） |
| POST | `/api/tasks/result` | Worker 提交单条结果 |
| POST | `/api/tasks/result/batch` | Worker 批量提交结果（单次 commit，减少磁盘 IO） |
| POST | `/api/tasks/screenshot` | Worker 上传截图（multipart/form-data，逐张上传） |
| POST | `/api/tasks/release` | Worker 归还未处理任务（优先采集切换时防丢失） |
| GET | `/api/progress` | 获取总体进度 |
| GET | `/api/progress/{batch}` | 获取批次进度 |
| GET | `/api/batches` | 获取批次列表 |
| GET | `/api/results` | 分页查询结果 |
| GET | `/api/export/{batch}` | 导出数据（≤5000条Excel，>5000条自动CSV） |
| GET | `/api/export/{batch}/db` | 导出批次数据为独立 SQLite 文件（零 CPU 开销） |
| GET | `/api/export/{batch}/screenshots` | 批量下载截图（ZIP 压缩包） |
| GET | `/api/workers` | 查看在线 Worker |
| DELETE | `/api/workers` | 清理所有离线 Worker |
| DELETE | `/api/workers/{id}` | 移除指定 Worker |
| GET/PUT | `/api/settings` | 读取/修改运行时设置（带版本号增量同步） |
| POST | `/api/worker/sync` | Worker 综合同步（上报 metrics + 接收配额 + 检测全局封锁） |
| GET | `/api/coordinator` | 查看全局并发协调状态（预算分配、封锁事件、Worker 配额） |
| POST | `/api/batches/{batch}/retry` | 重试失败任务 |
| POST | `/api/batches/{batch}/prioritize` | 将批次设为优先采集 |
| GET | `/api/batches/{batch}/errors` | 查看失败任务错误分类统计与详情 |
| DELETE | `/api/batches/{batch}` | 删除批次（含数据库记录 + 截图文件） |
| DELETE | `/api/database` | 清空所有数据（任务 + 结果 + 截图文件） |
| GET | `/api/worker/download` | 下载 Worker 安装包（ZIP，含启动脚本） |
| GET | `/api/tool/export-local` | 下载本地导出工具 export_local.py |

**Web UI 页面：**

| 路径 | 页面 |
|------|------|
| `/` | 仪表盘（总览进度、活跃 Worker） |
| `/tasks` | 任务管理（上传 ASIN、查看批次、错误分类详情、优先采集、截图下载） |
| `/results` | 结果浏览（分页、搜索、导出 Excel/CSV/数据库） |
| `/settings` | 设置（邮编、速率、并发、截图浏览器数/并发数、AIMD 参数，保存后 Worker 自动同步）+ 下载工具 |
| `/workers` | Worker 监控（在线状态、统计、下载 Worker 包、清理离线） |

## 快速开始

### 方式一：Docker 部署（推荐）

无需安装 Python，只需 Docker 环境即可运行。

**第一步：配置凭证**

```bash
cd /path/to/amazon-scraper-v2
cp .env.example .env
# 编辑 .env，填入快代理 TPS 凭证
```

`.env` 文件内容：

```env
KDL_SECRET_ID=your_secret_id_here
KDL_SIGNATURE=your_signature_here
```

**第二步：一键启动 Server + Worker**

```bash
docker compose up -d
```

**单独部署 Worker 到其他机器：**

```bash
# 先构建 Worker 镜像
docker build -f Dockerfile.worker -t scraper-worker .

# 在 Worker 机器上运行（只需指向 Server 地址，配置自动同步）
docker run -d scraper-worker --server http://<服务器IP>:8899

# 禁用截图功能（仅采集数据）
docker run -d scraper-worker --server http://<服务器IP>:8899 --no-screenshot
```

**推送到 Docker Hub 后，Worker 机器无需项目文件：**

```bash
# 构建并推送（在开发机上执行一次）
docker build -f Dockerfile.worker -t yourname/scraper-worker .
docker push yourname/scraper-worker

# 任意 Worker 机器只需一行命令（配置自动从 Server 拉取）
docker run -d yourname/scraper-worker --server http://<服务器IP>:8899
```

> **关于多 Worker 并发**：`docker compose up -d --scale worker=3` 可以在一台机器上启动多个 Worker。系统内置**全局并发协调器**——Server 维护总并发/QPS 预算（`GLOBAL_MAX_CONCURRENCY=50`, `GLOBAL_MAX_QPS=4.5`），按每个 Worker 的健康度（成功率、封锁率）动态分配配额。当任一 Worker 触发 Amazon 封锁时，Server 会向所有 Worker 发出全局冷却信号，避免雪崩。恢复阶段使用随机抖动防止同步振荡。快代理单通道可稳定承载的并发约为 14，**单代理通道下跑 1 个 Worker 是最优配置**。多 Worker 并行需要配合多条代理通道。通过 `GET /api/coordinator` 可实时查看全局配额分配状态。

Docker 文件说明：

| 文件 | 用途 |
|------|------|
| `Dockerfile.server` | Server 镜像（Web UI + API + 数据库，含全部依赖） |
| `Dockerfile.worker` | Worker 镜像（精简版，不含 FastAPI/数据库） |
| `docker-compose.yml` | 一键编排 Server + Worker（凭证通过环境变量注入） |
| `.env.example` | 环境变量模板，复制为 `.env` 并填入真实凭证 |
| `requirements-worker.txt` | Worker 精简依赖清单 |

### 方式二：原生 Python 部署

#### 环境要求

- Python 3.10+
- pip

#### 安装

```bash
cd /path/to/amazon-scraper-v2
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
# .venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

#### 配置代理凭证

```bash
# 推荐：通过环境变量注入（不修改代码）
export KDL_SECRET_ID=your_secret_id
export KDL_SIGNATURE=your_signature
```

或复制 `.env.example` 为 `.env` 并填入凭证。系统启动时自动加载 `.env` 文件（优先使用 `python-dotenv`，未安装时自动解析）。

#### 启动服务

```bash
# 启动中央服务器
python server.py
# 服务器运行在 http://0.0.0.0:8899
```

#### 启动 Worker

```bash
# 本机启动 Worker（连接本机服务器，默认开启截图功能）
python worker.py --server http://127.0.0.1:8899

# 指定参数
python worker.py --server http://192.168.1.100:8899 \
  --concurrency 10 \
  --zip-code 10001 \
  --worker-id my-worker-1

# 禁用截图功能（仅采集数据，不启动截图子进程）
python worker.py --server http://127.0.0.1:8899 --no-screenshot
```

Worker 只需以下文件即可独立运行（不需要完整项目）：

```
worker.py, screenshot_worker.py, config.py, models.py, proxy.py, session.py, parser.py, metrics.py, adaptive.py
requirements-worker.txt（精简依赖，pip install -r requirements-worker.txt）
requirements-screenshot.txt（可选，截图功能需要 playwright）
```

#### 方式三：Web UI 一键下载 Worker

最简单的方式——在 Server 的 Web UI 上下载 Worker 安装包：

1. 打开浏览器访问 Server 地址（如 `http://192.168.1.100:8899`）
2. 进入「设置」或「Worker 监控」页面，点击「下载 Worker 安装包」
3. 解压 ZIP 文件，双击 `start.sh`（macOS/Linux）或 `start.bat`（Windows）
4. 启动脚本会自动创建虚拟环境、安装依赖、连接到 Server

> 安装包内含 8 个 Python 文件 + 依赖清单 + 启动脚本，Server 地址已自动注入。首次启动会自动创建虚拟环境、安装依赖并下载 Playwright Chromium 用于截图功能。**无需配置任何环境变量**——Worker 启动时会自动从 Server 拉取全部运行参数（代理地址、并发控制、QPS 限速、AIMD 调控参数等），完全由 Server 统一管理。

#### 方式四：本地导出工具

大批量数据（>5000 条）在服务器上生成 Excel 会占满 CPU，推荐使用本地导出工具：

1. 在「采集结果」页面点击「数据库」按钮，下载批次的 `.db` 文件
2. 在「设置」页面点击「下载 export_local.py」，获取本地导出工具
3. 运行导出工具：

```bash
# 方式 1：双击运行（弹出文件选择器，选择 .db 文件，自动生成 Excel）
python export_local.py

# 方式 2：命令行
python export_local.py batch_xxx.db              # 生成 Excel（同目录）
python export_local.py batch_xxx.db -f csv       # 生成 CSV
python export_local.py batch_xxx.db -o report.xlsx  # 指定输出路径
```

> 工具完全独立运行，不依赖项目其他文件。首次运行自动安装 `openpyxl` 依赖（使用清华镜像加速）。仅需 Python 3.8+ 环境。

Worker 命令行参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--server` | (必填) | 中央服务器地址 |
| `--worker-id` | 自动生成 | Worker 唯一标识 |
| `--concurrency` | 16 | 最大并发上限（自适应控制器自动探索最优值） |
| `--zip-code` | 10001 | 配送邮编 |
| `--no-screenshot` | 默认开启 | 禁用截图功能（仅采集数据，不启动截图子进程） |

### 使用流程

1. 打开浏览器访问 `http://localhost:8899`
2. 进入「任务管理」页面，上传包含 ASIN 的 Excel/CSV/TXT 文件（可勾选「需要截图」）
3. 启动 Worker（Docker 或 Python，可在多台机器上部署）
4. 在「仪表盘」查看实时采集进度和速度
5. 在「结果浏览」页面查看、搜索采集数据
6. 点击「导出」下载数据：
   - **≤5000 条**：可选 Excel、CSV、数据库文件三种格式
   - **>5000 条**：可选 CSV、数据库文件（Excel 按钮自动隐藏，避免服务器 CPU 过载）
   - **大批量推荐**：下载 `.db` 数据库文件，使用本地导出工具生成 Excel（见下方说明）
7. 截图批次可在任务管理页面下载 ZIP 压缩包

### ASIN 文件格式

支持三种格式（均自动验证格式 `^B[0-9A-Z]{9}$`）：

- **Excel (.xlsx / .xls)**：系统自动扫描所有单元格，提取合法 ASIN
- **CSV (.csv)**：扫描所有字段
- **纯文本 (.txt)**：每行一个 ASIN

### 云端部署注意事项

| 项目 | 说明 |
|------|------|
| 代理白名单 | 快代理 TPS 需要绑定服务器 IP，部署后在快代理后台添加云服务器 IP |
| 凭证安全 | 使用环境变量或 Docker secrets 注入凭证，不要将 `.env` 提交到版本控制 |
| 数据库 | 单机 1-2 个 Worker 用 SQLite 即可；大规模多机 Worker 建议换 PostgreSQL |
| 安全 | API 无认证，公网部署建议加防火墙或 API Key |
| 数据持久化 | Docker 部署时 `data/` 目录已挂载到宿主机，原生部署注意备份 `data/scraper.db` |

## 性能指标

### 纯采集模式（1500 ASINs 全量测试）

| 指标 | 数值 |
|------|------|
| 采集速度 | ~700 条/分钟 |
| 成功率 | 99.9% (1498/1500) |
| 并发数 | 峰值 ~36 |
| 封锁率 | 0% |
| p50 延迟 | 3.2s |
| p95 延迟 | 5.8s |
| 带宽 | ~11 MB/s |
| 总耗时 | ~2 分钟 |

### 采集 + 截图模式（1500 ASINs 全量测试）

| 指标 | 数值 |
|------|------|
| 采集速度 | ~167 条/分钟 |
| 采集耗时 | ~9 分钟 |
| 截图并发 | 3（1 浏览器 × 3 页面） |
| 截图尺寸 | 1280×1300px |

> 截图模式下采集速度降低是因为门控机制——截图批次上传完成前 Worker 暂停拉取新任务，确保截图数据完整。采集本身的速度不受截图影响（进程隔离）。

### 调优历程

| 阶段 | 配置 | 速度 | 成功率 |
|------|------|------|--------|
| 初始基准 | c=5, interval=0.2s, timeout=30s | ~30/min | 70% |
| 第一轮优化 | c=10, interval=0.1s, session 轮换 | 47/min | 96.6% |
| 第二轮优化 | +HTTP/2, +Chrome131, +Brotli | 80/min | 100% |
| 参数调优 | c=14, timeout=15s, 令牌桶 QPS=4.5 | 116/min | 99.8% |
| 当前版本 | AIMD 自适应, 峰值 c=36 | **700/min** | **99.9%** |

## 文件结构

```
amazon-scraper-v2/
├── config.py              # 配置中心（凭证通过环境变量注入）
├── server.py              # FastAPI 服务端（API + Web UI）
├── worker.py              # 采集 Worker（流水线 + 自适应并发，可多机部署）
├── screenshot_worker.py   # 截图独立子进程（逐张即时上传，内存自动回收）
├── adaptive.py            # AIMD 自适应并发控制器（类 TCP 拥塞控制）
├── metrics.py             # 滑动窗口指标采集器（延迟/成功率/带宽实时追踪）
├── session.py             # Amazon 会话管理（TLS 指纹、Cookie、邮编）
├── parser.py              # HTML 解析器（selectolax/lxml，37 个字段）
├── proxy.py               # 代理管理（快代理 TPS 隧道，全异步 httpx）
├── database.py            # 数据库操作（aiosqlite，WAL + busy_timeout）
├── models.py              # 数据模型（Task, Result dataclass）
├── export_local.py        # 本地导出工具（独立运行，从 .db 生成 Excel/CSV）
├── requirements.txt       # Server 完整依赖（含 httpx）
├── requirements-worker.txt # Worker 精简依赖（含 httpx）
├── requirements-screenshot.txt # 截图功能依赖（可选，playwright）
├── Dockerfile.server      # Server Docker 镜像（HEALTHCHECK + 层缓存优化）
├── Dockerfile.worker      # Worker Docker 镜像（精简版，HEALTHCHECK）
├── docker-compose.yml     # Docker 编排（凭证通过环境变量，含资源限制）
├── .env.example           # 环境变量模板（复制为 .env 并填入凭证）
├── templates/             # Web UI 模板（Jinja2）
│   ├── base.html
│   ├── dashboard.html
│   ├── tasks.html
│   ├── results.html
│   ├── settings.html
│   └── workers.html
├── screenshot_cache/      # 截图临时文件（HTML 中转 + 标记文件）
├── tests/                 # 单元测试
├── data/                  # 数据目录（SQLite 数据库文件）
└── static/                # 前端静态资源
    ├── css/style.css
    └── screenshots/       # 截图存证（按批次/ASIN 存储 PNG）
```
