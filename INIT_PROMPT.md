# Amazon 产品采集系统 v2 — 开发指令

## 项目概述
重构亚马逊产品采集工具。要求**稳定**、**支持多机并行**、**带 Web UI**。

## 核心架构

### 1. 中央服务器（Web UI + API + 数据库）
- **框架**: FastAPI + Jinja2 模板（或简单前端）
- **数据库**: SQLite（单机）/ 可选 PostgreSQL（多机共享）
- **端口**: 8899
- **功能**:
  - 上传 ASIN 列表（Excel/CSV）
  - 任务分配 API（worker 拉取未完成的 ASIN）
  - 实时进度显示（总数/已完成/失败/成功率）
  - 结果下载（Excel/CSV）
  - 邮编配置
  - 代理状态监控

### 2. Worker 采集端（可在多台电脑运行）
- **HTTP 客户端**: httpx（async）或 curl_cffi（更好的反指纹）
- **并发**: 每台机器 5 并发（匹配代理 5次/s 限制）
- **连接中央服务器**: 通过 HTTP API 拉取任务、推送结果
- **启动方式**: `python worker.py --server http://192.168.x.x:8899`

## 技术要求

### 邮编定位（必须正确实现！）
现有代码通过伪造 cookie 失败了。正确方式：
1. 先访问 amazon.com 获取有效 session cookies
2. POST 到 `https://www.amazon.com/gp/delivery/ajax/address-change.html` 设置邮编
3. 用返回的 cookies 发起后续采集请求
4. 每个 worker 启动时建立一次 session，之后复用

**默认邮编: 10001**（可在 Web UI 修改）

### 代理配置（快代理 TPS）
- **限制**: 5次/s 并发，5Mbps 带宽
- **API (ip:port 格式)**:
  ```
  https://tps.kdlapi.com/api/gettps/?secret_id=onka1jdd3e9gh86l3eie&signature=evvgu6n8vhjfok0t1qqy6epcna9qw8m5&num=1&format=json&sep=1
  ```
- **API (带认证格式 server:port:user:pwd)**:
  ```
  https://tps.kdlapi.com/api/gettps/?secret_id=onka1jdd3e9gh86l3eie&signature=evvgu6n8vhjfok0t1qqy6epcna9qw8m5&num=1&format=json&sep=1&generateType=1
  ```
- 代理 IP 有时效性，需要定期刷新（建议每 30-60 秒换一个）
- 被 Amazon 封锁（403/503/captcha）时立即换 IP

### 反爬策略
1. **随机 User-Agent** — 维护 20+ 个现代浏览器 UA
2. **请求间隔** — 每个 worker 控制 200ms 间隔（5次/s）
3. **重试机制** — 被封锁时换 IP 重试，最多 3 次
4. **Session 管理** — 每个 worker 维护独立的 cookie session
5. **TLS 指纹** — 使用 curl_cffi 模拟真实浏览器 TLS 指纹

### 数据库表设计

```sql
-- 采集任务表
CREATE TABLE tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_name TEXT NOT NULL,        -- 批次名称
    asin TEXT NOT NULL,
    zip_code TEXT DEFAULT '10001',
    status TEXT DEFAULT 'pending',   -- pending/processing/done/failed
    worker_id TEXT,                  -- 哪个 worker 在处理
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batch_name, asin)
);

-- 采集结果表（保存所有字段）
CREATE TABLE results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_name TEXT NOT NULL,
    asin TEXT NOT NULL,
    zip_code TEXT,
    crawl_time TIMESTAMP,
    site TEXT,
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
```

### 采集字段（与原版完全一致）
参考文件: `/Users/nextderboy/代码项目文件/采集模块mac/amazon_spider/items.py`

包含 62 个字段：
- 采集元数据: crawl_time, site, zip_code, product_url, asin
- 商品基础: title, brand, product_type, manufacturer, model_number, part_number, country_of_origin, is_customized, best_sellers_rank
- 价格与交易: original_price, current_price, buybox_price, buybox_shipping, is_fba
- 库存与配送: stock_count, stock_status, delivery_date, delivery_time
- 内容描述: image_urls, bullet_points, long_description
- 编码与分类: upc_list, ean_list, parent_asin, variation_asins, root_category_id, category_ids, category_tree
- 规格参数: first_available_date, package_dimensions, package_weight, item_dimensions, item_weight

### 解析器
参考原版 spiders 目录中的解析逻辑：
```
/Users/nextderboy/代码项目文件/采集模块mac/amazon_spider/spiders/
```
复用已验证的 XPath/CSS 选择器。

### Web UI 功能
1. **首页仪表盘** — 当前任务进度、成功率、速度统计
2. **任务管理** — 上传 ASIN 文件、查看历史批次
3. **结果查看** — 分页浏览、搜索、筛选
4. **数据导出** — 下载 Excel/CSV
5. **设置页** — 邮编配置、代理配置、并发数设置
6. **Worker 监控** — 在线 worker 列表、各 worker 速度

### 断点续传
- 每个 ASIN 在数据库中有状态（pending/processing/done/failed）
- Worker 重启后从 pending/failed 状态继续
- processing 超过 5 分钟自动回退为 pending（防止 worker 崩溃后任务丢失）

## 项目结构
```
amazon-scraper-v2/
├── server.py          # 中央服务器（FastAPI）
├── worker.py          # 采集 worker
├── parser.py          # Amazon 页面解析器
├── proxy.py           # 代理管理
├── session.py         # Amazon session + 邮编管理
├── database.py        # 数据库操作
├── config.py          # 配置文件
├── models.py          # 数据模型
├── requirements.txt   # 依赖
├── templates/         # Web UI 模板
│   ├── base.html
│   ├── dashboard.html
│   ├── tasks.html
│   ├── results.html
│   └── settings.html
├── static/            # 静态资源
│   ├── css/
│   └── js/
└── README.md
```

## 开发方法论
使用增量式开发：
1. 创建 feature_list.json
2. 每完成一个功能，测试通过后标记 passes:true
3. git commit
4. 更新 claude-progress.txt

## 优先级
1. 数据库 + 基础模型 (highest)
2. Amazon 页面解析器（复用原版逻辑）
3. 代理管理
4. Session + 邮编管理
5. Worker 采集引擎
6. 中央服务器 API
7. Web UI
8. 断点续传 + 进度显示
9. 数据导出
10. Worker 监控

## 注意事项
- 代码注释用中文
- 不要过度设计，保持简洁
- 优先保证稳定性，其次才是速度
- 每个 worker 严格遵守 5次/s 的限制
- 所有网络请求必须有超时（15秒）和重试（3次）
