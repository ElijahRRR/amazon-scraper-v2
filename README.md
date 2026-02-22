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
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ 任务分发  │  │ 结果入库  │  │ 数据导出  │           │
│  └────┬─────┘  └────▲─────┘  └──────────┘           │
│       │             │                                │
│  ┌────▼─────────────┴────┐                           │
│  │     SQLite (WAL)      │                           │
│  │  tasks + results 表   │                           │
│  └───────────────────────┘                           │
└───────────────────────┬─────────────────────────────┘
                        │ REST API
         ┌──────────────┼──────────────┐
         │              │              │
┌────────▼───┐  ┌───────▼────┐  ┌─────▼──────┐
│  Worker 1  │  │  Worker 2  │  │  Worker N  │
│ curl_cffi  │  │ curl_cffi  │  │ curl_cffi  │
│ Chrome131  │  │ Chrome131  │  │ Chrome131  │
└──────┬─────┘  └──────┬─────┘  └──────┬─────┘
       │               │               │
       └───────────────┼───────────────┘
                       │
              ┌────────▼────────┐
              │   快代理 TPS     │
              │   隧道代理       │
              │  (自动换 IP)     │
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │   Amazon.com    │
              └─────────────────┘
```

### 数据流

1. 用户通过 Web UI 上传 ASIN 文件 → Server 创建采集任务入库
2. Worker 从 Server 拉取待处理任务（`/api/tasks/pull`）
3. Worker 通过隧道代理向 Amazon 发起请求，解析 HTML 提取数据
4. Worker 将采集结果批量推送回 Server（`/api/tasks/result/batch`）
5. 用户在 Web UI 查看进度、浏览结果、导出 Excel/CSV

## 技术栈

| 组件 | 技术 | 用途 |
|------|------|------|
| HTTP 客户端 | `curl_cffi` (AsyncSession) | TLS/JA3 指纹模拟，HTTP/2 多路复用 |
| 浏览器指纹 | `impersonate="chrome131"` | 模拟 Chrome 131 的完整 TLS 握手特征 |
| HTML 解析 | `selectolax` (Lexbor) / `lxml` | selectolax 比 lxml 快 2-5x，lxml 作 fallback |
| Web 框架 | FastAPI + Uvicorn | 异步 REST API 和 Web UI |
| 数据库 | SQLite (WAL 模式) + `aiosqlite` | 异步读写，WAL 支持并发 |
| 代理 | 快代理 TPS 隧道代理 | 固定入口，服务端自动换 IP |
| 压缩 | Brotli (`br`) | 比 gzip 小 15-25%，减少传输量 |
| 模板 | Jinja2 | Web UI 页面渲染 |
| 导出 | openpyxl / csv | Excel (.xlsx) 和 CSV 导出 |

## 反反爬策略

### TLS 指纹模拟

普通 Python HTTP 库（requests/httpx/aiohttp）的 TLS 握手特征与浏览器不同，Amazon 通过 JA3 指纹检测即可识别。`curl_cffi` 通过 `impersonate` 参数完整模拟 Chrome 的 TLS 握手过程（密码套件、扩展顺序、ALPN 等），使请求在网络层面与真实浏览器无法区分。

### HTTP/2 多路复用

启用 `http_version=2`，多个请求共享同一条 TCP+TLS 连接，减少连接建立开销，同时降低被代理端检测到多连接的风险。

### Session 主动轮换

每完成 100 次成功请求，主动关闭当前 Session 并创建新的。更换 User-Agent、重新获取代理 IP、重新初始化 Cookie，防止 Amazon 通过行为指纹（请求频率、Cookie 老化等）识别。

### 请求头仿真

- 15 个真实 Chrome 131 User-Agent 轮换
- 完整的 `sec-ch-ua` / `sec-ch-ua-platform` 客户端提示头
- 正确的 Referer 链（首页 → 产品页）
- 标准的 `Sec-Fetch-*` 头组合

### 邮编设置

通过 POST 请求 `/gp/delivery/ajax/address-change.html` 设置配送邮编，而非伪造 Cookie。这确保 Amazon 返回正确的价格和配送信息。

## 核心模块

### `config.py` — 配置中心

所有可调参数集中管理：

**基础参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MAX_CLIENTS` | 28 | HTTP/2 连接池大小（建议为并发数的 2 倍） |
| `REQUEST_INTERVAL` | 0.05s | 请求间隔（微间隔让代理更从容） |
| `REQUEST_JITTER` | 0.02s | 间隔随机抖动 |
| `REQUEST_TIMEOUT` | 15s | 单次请求超时（短超时更快释放并发槽位） |
| `MAX_RETRIES` | 3 | 最大重试次数 |
| `SESSION_ROTATE_EVERY` | 100 | 每 N 次成功请求轮换 Session |
| `IMPERSONATE_BROWSER` | chrome131 | TLS 指纹模拟目标 |
| `PROXY_REFRESH_INTERVAL` | 30s | 代理刷新间隔 |

**自适应并发控制参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `INITIAL_CONCURRENCY` | 5 | 冷启动并发数（保守开始，自动攀升） |
| `MIN_CONCURRENCY` | 2 | 并发下限 |
| `MAX_CONCURRENCY` | 50 | 并发上限 |
| `ADJUST_INTERVAL_S` | 10s | 评估间隔 |
| `TARGET_LATENCY_S` | 5.0s | 目标 p50 延迟（低于此加速） |
| `MAX_LATENCY_S` | 8.0s | 延迟上限（超过则减速） |
| `TARGET_SUCCESS_RATE` | 95% | 成功率目标（高于此加速） |
| `MIN_SUCCESS_RATE` | 85% | 成功率下限（低于此减速） |
| `BLOCK_RATE_THRESHOLD` | 5% | 封锁率阈值（超过则紧急减半+冷却） |
| `COOLDOWN_AFTER_BLOCK_S` | 30s | 被封后冷却时间 |

### `session.py` — 会话管理

- `AmazonSession`：单个采集会话，维护独立的 Cookie jar 和代理连接
  - `initialize()`：访问首页获取 Cookie → POST 设置邮编，带 `asyncio.Lock` 防并发初始化 + 3 次重试
  - `fetch_product_page(asin)`：采集商品详情页
  - `fetch_aod_page(asin)`：采集 AOD (All Offers Display) AJAX 页面，响应体仅为产品页的 10-20%
  - `is_blocked(resp)`：检测 403/503/验证码拦截（含页面体积阈值，避免正常产品页误判）

### `worker.py` — 采集引擎（流水线 + 自适应并发）

分布式 Worker，可在多台机器上运行，采用流水线架构：

```
task_feeder → [task_queue] → worker_pool (N个协程) → [result_queue] → batch_submitter
                                ↑
                     adaptive_controller 实时调整 N
```

- **流水线架构**：任务补给、采集、结果提交三大协程并行，互不阻塞
- **自适应并发**：冷启动 5 并发，AIMD 算法根据延迟/成功率/封锁率自动探索最优并发数
- **任务预取**：队列低于 50% 时触发补给，保持 Worker 始终有任务可做
- **动态工人池**：根据控制器决策动态增加 Worker 协程数量
- **封锁处理**：检测到 403/503/验证码 → 自动轮换 Session（换 IP + 换 Cookie）
- **主动轮换**：每 100 次成功请求主动更换 Session
- **快速模式** (`--fast`)：优先用 AOD AJAX 获取价格，失败再 fallback 到完整产品页
- **批量提交**：结果先入队列，攒满 10 条或每 2 秒批量 POST 提交
- **优雅退出**：SIGINT/SIGTERM 信号处理，退出前刷新队列

### `adaptive.py` — AIMD 自适应并发控制器

类 TCP 拥塞控制算法，根据实时指标动态调整并发数：

- **Additive Increase**：一切正常（成功率 ≥ 95%，p50 < 5s）→ 并发 +1
- **Multiplicative Decrease**：出问题 → 并发 ÷ 2
- **五级决策链**：封锁率超标 → 成功率/延迟 → 带宽饱和 → 冷却期 → 正常加速
- **动态信号量**：实时调整 Semaphore 大小，无需重建
- **冷却机制**：被封后 30 秒内不加速，防止抖动

### `metrics.py` — 滑动窗口指标采集器

实时跟踪请求延迟、成功率、被封率、带宽使用等关键指标：

- 30 秒滑动窗口，自动清理过期记录
- 支持 p50/p95 延迟分位数计算
- 带宽使用率跟踪（对比代理上限）
- 在飞请求数计数
- 线程安全（`threading.Lock`）

### `parser.py` — HTML 解析器

解析 Amazon 商品页面，提取 37 个字段：

- 基础信息：标题、品牌、型号、制造商、原产国
- 价格数据：原价、现价、BuyBox 价格、运费、FBA 状态
- 库存配送：库存数量、库存状态、配送日期、配送时长
- 分类编码：BSR 排名、类目树、UPC/EAN、父体 ASIN、变体列表
- 规格参数：包装/商品尺寸重量、上架时间
- 内容：五点描述、长描述、图片 URL

解析引擎优先使用 selectolax（基于 Lexbor C 引擎），比 lxml 快 2-5 倍。

### `proxy.py` — 代理管理

快代理 TPS 隧道代理集成：

- 固定隧道入口地址，IP 轮换在快代理服务端透明进行
- 代理按需获取 + 定时过期（默认 30 秒有效期）
- 被封时强制刷新，触发服务端分配新出口 IP
- 线程安全（`asyncio.Lock`），避免多协程同时请求 API

### `database.py` — 数据库

SQLite WAL 模式，两张核心表：

- `tasks`：采集任务队列（pending → processing → done/failed）
- `results`：采集结果存储（37 个商品字段，`batch_name + asin` 唯一索引自动去重）

关键机制：
- 任务拉取为原子操作（`BEGIN IMMEDIATE` 写锁 + SELECT + UPDATE 同一事务，防并发重复分发）
- 批量创建任务使用 `executemany` 高效插入
- 结果保存使用 `INSERT OR REPLACE`，基于唯一索引自动覆盖旧数据
- 超时回退：processing 超过 5 分钟自动回退为 pending（防 Worker 崩溃丢任务）
- 失败重试：支持按批次重试所有失败任务

### `server.py` — API 服务

FastAPI 应用（`lifespan` 上下文管理生命周期），提供 REST API 和 Web UI：

**API 端点：**

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/upload` | 上传 ASIN 文件（Excel/CSV/TXT） |
| GET | `/api/tasks/pull` | Worker 拉取待处理任务 |
| POST | `/api/tasks/result` | Worker 提交单条结果 |
| POST | `/api/tasks/result/batch` | Worker 批量提交结果（单次 commit，减少磁盘 IO） |
| GET | `/api/progress` | 获取总体进度 |
| GET | `/api/progress/{batch}` | 获取批次进度 |
| GET | `/api/batches` | 获取批次列表 |
| GET | `/api/results` | 分页查询结果 |
| GET | `/api/export/{batch}` | 导出数据（Excel/CSV） |
| GET | `/api/workers` | 查看在线 Worker |
| GET/PUT | `/api/settings` | 读取/修改运行时设置 |
| POST | `/api/batches/{batch}/retry` | 重试失败任务 |
| DELETE | `/api/batches/{batch}` | 删除批次 |

**Web UI 页面：**

| 路径 | 页面 |
|------|------|
| `/` | 仪表盘（总览进度、活跃 Worker） |
| `/tasks` | 任务管理（上传 ASIN、查看批次） |
| `/results` | 结果浏览（分页、搜索、导出） |
| `/settings` | 设置（邮编、并发数、代理等） |
| `/workers` | Worker 监控（在线状态、统计） |

## 快速开始

### 方式一：Docker 部署（推荐）

无需安装 Python，只需 Docker 环境即可运行。

**一键启动 Server + Worker：**

```bash
cd /path/to/amazon-scraper-v2
docker compose up -d
```

**单独部署 Worker 到其他机器：**

```bash
# 先构建 Worker 镜像
docker build -f Dockerfile.worker -t scraper-worker .

# 在 Worker 机器上运行（指向 Server 地址）
docker run -d scraper-worker --server http://<服务器IP>:8899
```

**推送到 Docker Hub 后，Worker 机器无需项目文件：**

```bash
# 构建并推送（在开发机上执行一次）
docker build -f Dockerfile.worker -t yourname/scraper-worker .
docker push yourname/scraper-worker

# 任意 Worker 机器只需一行命令
docker run -d yourname/scraper-worker --server http://<服务器IP>:8899
```

> **关于多 Worker 并发**：`docker compose up -d --scale worker=3` 可以在一台机器上启动多个 Worker。每个 Worker 的**冷启动并发为 5**，默认会自适应探索到 `--concurrency` 上限（默认 50）；但快代理单通道可稳定承载的并发通常约为 14。**单代理通道下跑 1 个 Worker 是最优配置**。多 Worker 并行需要配合多条代理通道，否则会因代理限流导致失败率上升。

Docker 文件说明：

| 文件 | 用途 |
|------|------|
| `Dockerfile.server` | Server 镜像（Web UI + API + 数据库，含全部依赖） |
| `Dockerfile.worker` | Worker 镜像（精简版，仅 4 个依赖包，不含 FastAPI/数据库） |
| `docker-compose.yml` | 一键编排 Server + Worker |
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

#### 配置代理

编辑 `config.py`，将 `PROXY_API_URL` 和 `PROXY_API_URL_AUTH` 替换为你的快代理 TPS API 地址（包含 secret_id 和 signature）。

#### 启动服务

```bash
# 启动中央服务器
python server.py
# 服务器运行在 http://0.0.0.0:8899
```

#### 启动 Worker

```bash
# 本机启动 Worker（连接本机服务器）
python worker.py --server http://127.0.0.1:8899

# 指定参数
python worker.py --server http://192.168.1.100:8899 \
  --concurrency 10 \
  --zip-code 10001 \
  --worker-id my-worker-1

# 快速模式（AOD 优先获取价格，响应更小更快）
python worker.py --server http://127.0.0.1:8899 --fast
```

Worker 只需以下文件即可独立运行（不需要完整项目）：

```
worker.py, config.py, models.py, proxy.py, session.py, parser.py, metrics.py, adaptive.py
requirements-worker.txt（精简依赖，pip install -r requirements-worker.txt）
```

Worker 命令行参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--server` | (必填) | 中央服务器地址 |
| `--worker-id` | 自动生成 | Worker 唯一标识 |
| `--concurrency` | 50 | 最大并发上限（自适应控制器自动探索最优值） |
| `--zip-code` | 10001 | 配送邮编 |
| `--fast` | 关闭 | AOD 快速模式 |

### 使用流程

1. 打开浏览器访问 `http://localhost:8899`
2. 进入「任务管理」页面，上传包含 ASIN 的 Excel/CSV/TXT 文件
3. 启动 Worker（Docker 或 Python，可在多台机器上部署）
4. 在「仪表盘」查看实时采集进度和速度
5. 在「结果浏览」页面查看、搜索采集数据
6. 点击「导出」下载 Excel 或 CSV 文件

### ASIN 文件格式

支持三种格式：

- **Excel (.xlsx)**：系统自动扫描所有单元格，提取符合 `^B[0-9A-Z]{9}$` 格式的 ASIN
- **CSV (.csv)**：同上
- **纯文本 (.txt)**：每行一个 ASIN（同样使用正则验证格式）

### 云端部署注意事项

| 项目 | 说明 |
|------|------|
| 代理白名单 | 快代理 TPS 需要绑定服务器 IP，部署后在快代理后台添加云服务器 IP |
| 数据库 | 单机 1-2 个 Worker 用 SQLite 即可；大规模多机 Worker 建议换 PostgreSQL |
| 安全 | API 无认证，公网部署建议加防火墙或 API Key |
| 数据持久化 | Docker 部署时 `data/` 目录已挂载到宿主机，原生部署注意备份 `data/scraper.db` |

## 性能指标

在快代理 TPS 隧道代理 + 单 Worker 配置下，经过 11 组参数系统调优后的测试数据（500 ASIN 大规模确认测试）：

| 指标 | 数值 |
|------|------|
| 采集速度 | ~116 条/分钟 |
| 成功率 | 99.8% (499/500) |
| 并发数 | 14 |
| 请求超时 | 15 秒 |
| Session 轮换 | 每 100 次成功请求 |
| 封锁率 | 0% |

### 调优历程

| 阶段 | 配置 | 速度 | 成功率 |
|------|------|------|--------|
| 初始基准 | c=5, interval=0.2s, timeout=30s | ~30/min | 70% |
| 第一轮优化 | c=10, interval=0.1s, session 轮换 | 47/min | 96.6% |
| 第二轮优化 | +HTTP/2, +Chrome131, +Brotli | 80/min | 100% |
| 参数调优 | c=14, interval=0.05s, timeout=15s | **116/min** | **99.8%** |

> 注：实际速度受代理质量和网络环境影响。隧道代理的并发连接上限约为 14，超过此值会出现连接崩溃。`MAX_CLIENTS` 应设为并发数的 2 倍以确保 HTTP/2 连接池充足。

## 文件结构

```
amazon-scraper-v2/
├── config.py              # 配置中心（代理、并发、UA、请求头、自适应参数等）
├── server.py              # FastAPI 服务端（API + Web UI）
├── worker.py              # 采集 Worker（流水线 + 自适应并发，可多机部署）
├── adaptive.py            # AIMD 自适应并发控制器（类 TCP 拥塞控制）
├── metrics.py             # 滑动窗口指标采集器（延迟/成功率/带宽实时追踪）
├── session.py             # Amazon 会话管理（TLS 指纹、Cookie、邮编）
├── parser.py              # HTML 解析器（selectolax/lxml，37 个字段）
├── proxy.py               # 代理管理（快代理 TPS 隧道）
├── database.py            # 数据库操作（aiosqlite，任务队列+结果存储）
├── models.py              # 数据模型（Task, Result dataclass）
├── requirements.txt       # Server 完整依赖
├── requirements-worker.txt # Worker 精简依赖（4 个包）
├── Dockerfile.server      # Server Docker 镜像
├── Dockerfile.worker      # Worker Docker 镜像（精简版）
├── docker-compose.yml     # Docker 编排（Server + Worker）
├── .dockerignore          # Docker 构建排除规则
├── templates/             # Web UI 模板（Jinja2）
│   ├── base.html
│   ├── dashboard.html
│   ├── tasks.html
│   ├── results.html
│   ├── settings.html
│   └── workers.html
├── static/                # 前端静态资源（CSS/JS）
└── data/                  # 数据目录（SQLite 数据库文件）
```
