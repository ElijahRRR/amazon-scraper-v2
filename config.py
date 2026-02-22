"""
Amazon 产品采集系统 v2 - 配置文件
所有可调参数集中管理
"""
import os

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
SERVER_PORT = 8899

# ============================================================
# 采集配置
# ============================================================
DEFAULT_ZIP_CODE = "10001"
DEFAULT_CONCURRENCY = 14         # 仅用于 Web UI 设置页展示，实际并发由 AdaptiveController 管理
MAX_CLIENTS = 28                 # HTTP/2 连接池 max_clients（多路复用）
REQUEST_INTERVAL = 0.05          # 请求间隔（秒）— 仅作为令牌桶不可用时的 fallback
REQUEST_JITTER = 0.02            # 间隔随机抖动范围（秒）
REQUEST_TIMEOUT = 15             # 请求超时（秒）— 短超时更快释放并发槽位
MAX_RETRIES = 3                  # 最大重试次数
TASK_TIMEOUT_MINUTES = 5         # 任务处理超时（分钟），超时回退为 pending
SESSION_ROTATE_EVERY = 1000      # 每 N 次成功请求主动轮换 session（被封时仍会被动轮换）

# 全局令牌桶限流（与 Semaphore 并发控制互补）
TOKEN_BUCKET_RATE = 4.5          # 目标 QPS（每秒发起请求数），留 10% buffer

# ============================================================
# 自适应并发控制（流水线模式）
# ============================================================
INITIAL_CONCURRENCY = 8          # 冷启动并发数（保守开始，自动攀升）
MIN_CONCURRENCY = 2              # 并发下限（再差也保持 2 个在飞）
MAX_CONCURRENCY = 50             # 并发上限（受代理套餐约束）

# 代理硬约束
PROXY_BANDWIDTH_MBPS = 0          # 0=不限带宽（禁用带宽感知）

# 自适应调节参数
ADJUST_INTERVAL_S = 10           # 评估间隔（秒）
TARGET_LATENCY_S = 6.0           # 目标 p50 延迟（低于此才加速）
MAX_LATENCY_S = 10.0             # 延迟上限（超过则减速）
TARGET_SUCCESS_RATE = 0.95       # 成功率目标（高于此才加速）
MIN_SUCCESS_RATE = 0.85          # 成功率下限（低于此则减速）
BLOCK_RATE_THRESHOLD = 0.05      # 封锁率阈值（超 5% 紧急减速）
BANDWIDTH_SOFT_CAP = 0.80        # 带宽软上限（超 80% 停止加速）
COOLDOWN_AFTER_BLOCK_S = 30      # 被封后冷却时间（秒）

# 任务预取
TASK_QUEUE_SIZE = 100            # 任务缓冲队列大小
TASK_PREFETCH_THRESHOLD = 0.5    # 队列低于 50% 时触发预取

# ============================================================
# 代理配置（快代理 TPS）
# ============================================================
# ip:port 格式（需绑定白名单）
PROXY_API_URL = (
    "https://tps.kdlapi.com/api/gettps/"
    "?secret_id=onka1jdd3e9gh86l3eie"
    "&signature=evvgu6n8vhjfok0t1qqy6epcna9qw8m5"
    "&num=1&format=json&sep=1"
)
# server:port:user:pwd 格式
PROXY_API_URL_AUTH = (
    "https://tps.kdlapi.com/api/gettps/"
    "?secret_id=onka1jdd3e9gh86l3eie"
    "&signature=evvgu6n8vhjfok0t1qqy6epcna9qw8m5"
    "&num=1&format=json&sep=1&generateType=1"
)
PROXY_REFRESH_INTERVAL = 30      # 代理刷新间隔（秒）

# ============================================================
# 反爬策略
# ============================================================
# curl_cffi impersonate 目标浏览器
IMPERSONATE_BROWSER = "chrome131"

# User-Agent 列表（必须与 impersonate 的浏览器版本匹配）
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.53 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.98 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.53 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.127 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.98 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.98 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.142 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.127 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.127 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.53 Safari/537.36",
]

# 默认请求头
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": '"Chromium";v="133", "Google Chrome";v="133", "Not_A Brand";v="24"',
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
# Excel 中文表头映射
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

# 导出列顺序
EXPORT_COLUMN_ORDER = [
    "商品采集时间", "配送邮编", "产品链接", "ASIN (商品ID)", "商品标题",
    "商品原价", "当前价格", "BuyBox 价格", "BuyBox 运费", "是否 FBA 发货",
    "库存数量", "库存状态", "配送到达时间", "配送时长", "品牌", "产品型号",
    "原产国", "是否为定制产品", "畅销排名", "UPC 列表", "EAN 列表",
    "包装尺寸", "包装重量", "商品本体尺寸", "商品本体重量", "父体 ASIN",
    "变体 ASIN 列表", "根类目 ID", "类目路径树", "五点描述", "商品图片链接",
    "站点", "制造商", "部件编号", "上架时间", "长描述", "商品类型", "类目 ID 链",
]
