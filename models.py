"""
Amazon 产品采集系统 v2 - 数据模型
使用 dataclass 定义任务和采集结果的结构
"""
from dataclasses import dataclass, field, fields, asdict
from typing import Optional
from datetime import datetime


@dataclass
class Task:
    """采集任务"""
    id: Optional[int] = None
    batch_name: str = ""
    asin: str = ""
    zip_code: str = "10001"
    status: str = "pending"            # pending / processing / done / failed
    worker_id: Optional[str] = None
    retry_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Result:
    """采集结果 — 字段与原版 items.py 完全一致"""
    id: Optional[int] = None
    batch_name: str = ""
    asin: str = ""

    # 采集元数据
    crawl_time: str = ""
    site: str = "US"
    zip_code: str = ""
    product_url: str = ""

    # 商品基础
    title: str = ""
    brand: str = ""
    product_type: str = ""
    manufacturer: str = ""
    model_number: str = ""
    part_number: str = ""
    country_of_origin: str = ""
    is_customized: str = ""
    best_sellers_rank: str = ""

    # 价格与交易
    original_price: str = ""
    current_price: str = ""
    buybox_price: str = ""
    buybox_shipping: str = ""
    is_fba: str = ""

    # 库存与配送
    stock_count: str = ""
    stock_status: str = ""
    delivery_date: str = ""
    delivery_time: str = ""

    # 内容描述
    image_urls: str = ""
    bullet_points: str = ""
    long_description: str = ""

    # 编码与分类
    upc_list: str = ""
    ean_list: str = ""
    parent_asin: str = ""
    variation_asins: str = ""
    root_category_id: str = ""
    category_ids: str = ""
    category_tree: str = ""

    # 规格参数
    first_available_date: str = ""
    package_dimensions: str = ""
    package_weight: str = ""
    item_dimensions: str = ""
    item_weight: str = ""

    # 变动追踪字段
    price_change: str = ""
    stock_qty_change: str = ""
    stock_status_change: str = ""
    other_change: str = ""
    prev_current_price: str = ""
    prev_buybox_price: str = ""
    prev_stock_count: str = ""
    prev_stock_status: str = ""
    is_new: str = ""
    updated_at: str = ""
    # 内部字段（不导出到 Excel/CSV）
    content_hash: str = ""
    last_change_at: str = ""
    change_seq: str = ""

    created_at: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Result":
        """从字典构造 Result，忽略不存在的字段"""
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


# 所有采集字段名（不含 id / batch_name / created_at 等管理字段）
_EXCLUDED = {"id", "batch_name", "created_at"}
RESULT_FIELDS = [f.name for f in fields(Result) if f.name not in _EXCLUDED]

# 内部字段（不导出到 Excel/CSV，但存储在数据库中）
_INTERNAL_FIELDS = {"content_hash", "last_change_at", "change_seq"}

# 导出可选字段（= RESULT_FIELDS - _INTERNAL_FIELDS + total_price 虚拟字段）
EXPORTABLE_FIELDS = [f for f in RESULT_FIELDS if f not in _INTERNAL_FIELDS] + ["total_price"]
