"""
Amazon 产品采集系统 v2 - 数据模型
使用 dataclass 定义任务和采集结果的结构
"""
from dataclasses import dataclass, field, asdict
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
RESULT_FIELDS = [
    "asin", "crawl_time", "site", "zip_code", "product_url",
    "title", "brand", "product_type", "manufacturer", "model_number",
    "part_number", "country_of_origin", "is_customized", "best_sellers_rank",
    "original_price", "current_price", "buybox_price", "buybox_shipping", "is_fba",
    "stock_count", "stock_status", "delivery_date", "delivery_time",
    "image_urls", "bullet_points", "long_description",
    "upc_list", "ean_list", "parent_asin", "variation_asins",
    "root_category_id", "category_ids", "category_tree",
    "first_available_date", "package_dimensions", "package_weight",
    "item_dimensions", "item_weight",
]
