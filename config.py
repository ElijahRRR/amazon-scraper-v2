"""
Amazon 产品采集系统 v2 - 配置文件
所有可调参数集中管理
凭证通过环境变量注入，避免硬编码
"""
import os

# 自动加载 .env 文件（如果存在）
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    # python-dotenv 未安装时，手动加载 .env
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.isfile(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _key, _, _val = _line.partition("=")
                    os.environ.setdefault(_key.strip(), _val.strip())

# ============================================================
# 基础路径
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "scraper.db")
EXPORT_DIR = os.path.join(BASE_DIR, "data", "exports")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

# ============================================================
# 服务器配置
# ============================================================
SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("SERVER_PORT", 8899))

# ============================================================
# 采集配置
# ============================================================
# 解析引擎: "selectolax" (默认, Lexbor C 引擎最快) | "lxml" | "scrapling" (Parsel 风格 CSS/XPath)
PARSER_ENGINE = os.environ.get("PARSER_ENGINE", "selectolax")

DEFAULT_ZIP_CODE = os.environ.get("DEFAULT_ZIP_CODE", "10001")
MAX_CLIENTS = 32                 # HTTP/1.1 连接池（每 session 最大 TCP 连接数，应 ≥ max_concurrency）
REQUEST_TIMEOUT = 15             # 请求超时（秒）
MAX_RETRIES = 3                  # 最大重试次数
TASK_TIMEOUT_MINUTES = 1.5       # 任务处理超时（分钟），超时回退为 pending（90秒）
SESSION_ROTATE_EVERY = 1000      # 每 N 次成功请求主动轮换 session

# 令牌桶限流
TOKEN_BUCKET_RATE = 5.0          # TPS 模式全局 QPS 上限
PER_CHANNEL_QPS = 5.0            # DPS 隧道模式每 channel QPS（总 QPS = channels × 此值）

# ============================================================
# 自适应并发控制（流水线模式）
# ============================================================
INITIAL_CONCURRENCY = 8          # 冷启动并发（避免启动封锁风暴，10s 内爬升到甜区）
MIN_CONCURRENCY = 4              # 并发下限
MAX_CONCURRENCY = 16             # TPS 模式并发上限（实测：超过 16 延迟升高但带宽不增）
TUNNEL_MAX_CONCURRENCY = 20      # DPS 隧道模式总并发上限（实测 12-16 最优，留余量给 AIMD 探索）
TUNNEL_INITIAL_CONCURRENCY = 8   # DPS 隧道模式冷启动并发（AIMD 从 8→15 探索最优点）

# Per-channel 并发控制（DPS 模式下每个 channel 的 AIMD 范围）
# 带宽约束：总带宽 5Mbps，压缩后每页 ~46KB(wire)
# 填满 5Mbps 需 ~13.6 req/s，p50=6s → 需 ~82 并发 → 每通道 ~10
PER_CHANNEL_INITIAL_CONCURRENCY = 3  # 每通道初始并发（3×8=24 起步）
PER_CHANNEL_MIN_CONCURRENCY = 1      # 每通道最小并发
PER_CHANNEL_MAX_CONCURRENCY = 4      # 每通道最大并发（4×8=32 上限，代理甜区 20-30）

# 代理硬约束
PROXY_BANDWIDTH_MBPS = 15        # 代理带宽上限（Mbps），用于 AIMD 带宽感知

# 自适应调节参数
ADJUST_INTERVAL_S = 10           # 评估间隔（秒）
TARGET_LATENCY_S = 8.0           # p50 目标（甜区 4-6s，允许到 8s）
MAX_LATENCY_S = 15.0             # p50 上限（超过说明代理拥塞严重）
TARGET_SUCCESS_RATE = 0.95       # 成功率目标
MIN_SUCCESS_RATE = 0.85          # 成功率下限
BLOCK_RATE_THRESHOLD = 0.05      # 封锁率阈值
BANDWIDTH_SOFT_CAP = 0.80        # 带宽软上限
# 全局并发协调（多 Worker 场景）
GLOBAL_MAX_CONCURRENCY = 48      # 匹配 TUNNEL_MAX_CONCURRENCY（单 Worker 场景取更高值）
GLOBAL_MAX_QPS = 40.0            # DPS: 8 channels × 5 QPS（TPS 模式由 Server 下发配额覆盖）

# 任务预取
TASK_QUEUE_SIZE = 100            # 任务缓冲队列大小
TASK_PREFETCH_THRESHOLD = 0.5    # 队列低于 50% 时触发预取

# ============================================================
# 代理配置
# ============================================================

# 代理模式: "tps" = 每次请求换IP, "tunnel" = 多通道定时换IP
PROXY_MODE = os.environ.get("PROXY_MODE", "tps")

# --- TPS 模式配置（快代理 TPS）---
# 凭证通过环境变量注入：KDL_SECRET_ID / KDL_SIGNATURE
_KDL_SECRET_ID = os.environ.get("KDL_SECRET_ID", "")
_KDL_SIGNATURE = os.environ.get("KDL_SIGNATURE", "")

PROXY_API_URL = (
    f"https://tps.kdlapi.com/api/gettps/"
    f"?secret_id={_KDL_SECRET_ID}"
    f"&signature={_KDL_SIGNATURE}"
    f"&num=1&format=json&sep=1"
)
PROXY_API_URL_AUTH = (
    f"https://tps.kdlapi.com/api/gettps/"
    f"?secret_id={_KDL_SECRET_ID}"
    f"&signature={_KDL_SIGNATURE}"
    f"&num=1&format=json&sep=1&generateType=1"
)
PROXY_REFRESH_INTERVAL = 30      # 代理刷新间隔（秒）

# --- 隧道模式配置（快代理定时换 IP）---
# 隧道模式与 TPS 共用同一个 API (PROXY_API_URL_AUTH)，只是代理行为不同：
# - TPS: 每次请求自动换 IP
# - 隧道: 定时换 IP，多通道并行
TUNNEL_CHANNELS = int(os.environ.get("TUNNEL_CHANNELS", "8"))  # 同时获取的代理通道数
TUNNEL_ROTATE_INTERVAL = 60        # IP 轮换周期（秒），到期后重新获取代理
# 固定隧道代理地址（帐密模式）：设置后跳过 API 调用，直接使用该地址
# 格式: http://user:pwd@host:port
TUNNEL_PROXY_URL = os.environ.get("TUNNEL_PROXY_URL", "")

# 隧道 ChangeTpsIp API（手动换 IP）
TUNNEL_CHANGE_IP_URL = (
    f"https://tps.kdlapi.com/api/changetpsip/"
    f"?secret_id={_KDL_SECRET_ID}"
    f"&signature={_KDL_SIGNATURE}"
)

# ============================================================
# 反爬策略
# ============================================================
# 浏览器指纹配置：每个 profile 包含 impersonate (TLS 指纹)、sec-ch-ua、UA 列表
# AmazonSession 创建时随机选取一个 profile，确保 UA / sec-ch-ua / TLS 三者一致
BROWSER_PROFILES = [
    {
        "impersonate": "chrome120",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome123",
        "sec_ch_ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome124",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome131",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.85 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.108 Safari/537.36",
        ],
    },
    {
        "impersonate": "chrome136",
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        ],
    },
]

# 向后兼容：保留 USER_AGENTS 扁平列表（用于不使用 BROWSER_PROFILES 的场景）
USER_AGENTS = []
for _p in BROWSER_PROFILES:
    USER_AGENTS.extend(_p["user_agents"])

# 默认请求头（与 chrome131 匹配）
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# ============================================================
# 导出配置
# ============================================================
HEADER_MAP = {
    "crawl_time": "商品采集时间",
    "zip_code": "配送邮编",
    "product_url": "产品链接",
    "asin": "ASIN (商品ID)",
    "title": "商品标题",
    "original_price": "商品原价",
    "current_price": "当前价格",
    "buybox_price": "BuyBox 价格",
    "buybox_shipping": "BuyBox 运费",
    "is_fba": "是否 FBA 发货",
    "stock_count": "库存数量",
    "stock_status": "库存状态",
    "delivery_date": "配送到达时间",
    "delivery_time": "配送时长",
    "brand": "品牌",
    "model_number": "产品型号",
    "country_of_origin": "原产国",
    "is_customized": "是否为定制产品",
    "best_sellers_rank": "畅销排名",
    "upc_list": "UPC 列表",
    "ean_list": "EAN 列表",
    "package_dimensions": "包装尺寸",
    "package_weight": "包装重量",
    "item_dimensions": "商品本体尺寸",
    "item_weight": "商品本体重量",
    "parent_asin": "父体 ASIN",
    "variation_asins": "变体 ASIN 列表",
    "root_category_id": "根类目 ID",
    "category_ids": "类目 ID 链",
    "category_tree": "类目路径树",
    "bullet_points": "五点描述",
    "image_urls": "商品图片链接",
    "site": "站点",
    "manufacturer": "制造商",
    "part_number": "部件编号",
    "first_available_date": "上架时间",
    "long_description": "长描述",
    "product_type": "商品类型",
}

EXPORT_COLUMN_ORDER = [
    "商品采集时间", "配送邮编", "产品链接", "ASIN (商品ID)", "商品标题",
    "商品原价", "当前价格", "BuyBox 价格", "BuyBox 运费", "总价", "是否 FBA 发货",
    "库存数量", "库存状态", "配送到达时间", "配送时长", "品牌", "产品型号",
    "原产国", "是否为定制产品", "畅销排名", "UPC 列表", "EAN 列表",
    "包装尺寸", "包装重量", "商品本体尺寸", "商品本体重量", "父体 ASIN",
    "变体 ASIN 列表", "根类目 ID", "类目路径树", "五点描述", "商品图片链接",
    "站点", "制造商", "部件编号", "上架时间", "长描述", "商品类型", "类目 ID 链",
]

# ============================================================
# 启动校验
# ============================================================
assert MIN_CONCURRENCY <= INITIAL_CONCURRENCY <= MAX_CONCURRENCY, (
    f"TPS 并发参数不合法: MIN={MIN_CONCURRENCY} INITIAL={INITIAL_CONCURRENCY} MAX={MAX_CONCURRENCY}"
)
assert MIN_CONCURRENCY <= TUNNEL_INITIAL_CONCURRENCY <= TUNNEL_MAX_CONCURRENCY, (
    f"Tunnel 并发参数不合法: MIN={MIN_CONCURRENCY} INITIAL={TUNNEL_INITIAL_CONCURRENCY} MAX={TUNNEL_MAX_CONCURRENCY}"
)
assert TARGET_LATENCY_S < MAX_LATENCY_S, (
    f"TARGET_LATENCY_S({TARGET_LATENCY_S}) 必须小于 MAX_LATENCY_S({MAX_LATENCY_S})"
)
