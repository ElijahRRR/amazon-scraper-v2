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

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_CONCURRENCY` | 14 | 每个 Worker 并发数 |
| `MAX_CLIENTS` | 28 | HTTP/2 连接池大小（建议为并发数的 2 倍） |
| `REQUEST_INTERVAL` | 0.05s | 请求间隔（微间隔让代理更从容） |
| `REQUEST_JITTER` | 0.02s | 间隔随机抖动 |
| `REQUEST_TIMEOUT` | 15s | 单次请求超时（短超时更快释放并发槽位） |
| `MAX_RETRIES` | 3 | 最大重试次数 |
| `SESSION_ROTATE_EVERY` | 100 | 每 N 次成功请求轮换 Session |
| `IMPERSONATE_BROWSER` | chrome131 | TLS 指纹模拟目标 |
| `PROXY_REFRESH_INTERVAL` | 30s | 代理刷新间隔 |

### `session.py` — 会话管理

- `AmazonSession`：单个采集会话，维护独立的 Cookie jar 和代理连接
  - `initialize()`：访问首页获取 Cookie → POST 设置邮编，带 3 次重试
  - `fetch_product_page(asin)`：采集商品详情页
  - `fetch_aod_page(asin)`：采集 AOD (All Offers Display) AJAX 页面，响应体仅为产品页的 10-20%
  - `is_blocked(resp)`：检测 403/503/验证码拦截
- `SessionPool`：Session 池，管理多个会话实例的轮换和替换

### `worker.py` — 采集引擎

分布式 Worker，可在多台机器上运行：

- 从 Server 拉取任务 → 并发采集 → 批量推送结果
- **速率控制**：Semaphore 控制并发 + 间隔抖动
- **封锁处理**：检测到 403/503/验证码 → 自动轮换 Session（换 IP + 换 Cookie）
- **主动轮换**：每 100 次成功请求主动更换 Session
- **快速模式** (`--fast`)：优先用 AOD AJAX 获取价格，失败再 fallback 到完整产品页
- **批量提交**：结果先入队列，攒满 10 条或每 2 秒批量 POST 提交
- **优雅退出**：SIGINT/SIGTERM 信号处理，退出前刷新队列

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
- 代理定时刷新（默认 30 秒）
- 被封时强制刷新，触发服务端分配新出口 IP
- 线程安全（asyncio.Lock），避免多协程同时请求 API

### `database.py` — 数据库

SQLite WAL 模式，两张核心表：

- `tasks`：采集任务队列（pending → processing → done/failed）
- `results`：采集结果存储（37 个商品字段）

关键机制：
- 任务拉取为原子操作（SELECT + UPDATE 同一事务）
- 超时回退：processing 超过 5 分钟自动回退为 pending（防 Worker 崩溃丢任务）
- 失败重试：支持按批次重试所有失败任务

### `server.py` — API 服务

FastAPI 应用，提供 REST API 和 Web UI：

**API 端点：**

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/upload` | 上传 ASIN 文件（Excel/CSV/TXT） |
| GET | `/api/tasks/pull` | Worker 拉取待处理任务 |
| POST | `/api/tasks/result` | Worker 提交单条结果 |
| POST | `/api/tasks/result/batch` | Worker 批量提交结果 |
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

### 环境要求

- Python 3.10+
- pip

### 安装

```bash
cd /path/to/amazon-scraper-v2
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
# .venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 配置代理

编辑 `config.py`，将 `PROXY_API_URL` 和 `PROXY_API_URL_AUTH` 替换为你的快代理 TPS API 地址（包含 secret_id 和 signature）。

### 启动服务

```bash
# 启动中央服务器
python server.py
# 服务器运行在 http://0.0.0.0:8899
```

### 启动 Worker

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

Worker 命令行参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--server` | (必填) | 中央服务器地址 |
| `--worker-id` | 自动生成 | Worker 唯一标识 |
| `--concurrency` | 14 | 并发数 |
| `--zip-code` | 10001 | 配送邮编 |
| `--fast` | 关闭 | AOD 快速模式 |

### 使用流程

1. 打开浏览器访问 `http://localhost:8899`
2. 进入「任务管理」页面，上传包含 ASIN 的 Excel/CSV/TXT 文件
3. 启动 Worker（可在多台机器上启动多个）
4. 在「仪表盘」查看实时采集进度
5. 在「结果浏览」页面查看、搜索采集数据
6. 点击「导出」下载 Excel 或 CSV 文件

### ASIN 文件格式

支持三种格式：

- **Excel (.xlsx)**：系统自动扫描所有单元格，提取 10 位 B 开头的字符串
- **CSV (.csv)**：同上
- **纯文本 (.txt)**：每行一个 ASIN

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
├── config.py         # 配置中心（代理、并发、UA、请求头等）
├── server.py         # FastAPI 服务端（API + Web UI）
├── worker.py         # 采集 Worker（分布式，可多机部署）
├── session.py        # Amazon 会话管理（TLS 指纹、Cookie、邮编）
├── parser.py         # HTML 解析器（selectolax/lxml，37 个字段）
├── proxy.py          # 代理管理（快代理 TPS 隧道）
├── database.py       # 数据库操作（aiosqlite，任务队列+结果存储）
├── models.py         # 数据模型（Task, Result dataclass）
├── requirements.txt  # Python 依赖
├── templates/        # Web UI 模板（Jinja2）
│   ├── base.html
│   ├── dashboard.html
│   ├── tasks.html
│   ├── results.html
│   ├── settings.html
│   └── workers.html
├── static/           # 前端静态资源（CSS/JS）
└── data/             # 数据目录（SQLite 数据库文件）
```
