import scrapy


class AmazonProductItem(scrapy.Item):
    # --- 1. 采集元数据 ---
    crawl_time = scrapy.Field()
    site = scrapy.Field()
    zip_code = scrapy.Field()  # [新增] 配送地邮编
    product_url = scrapy.Field() # [新增] 产品链接
    asin = scrapy.Field()

    # --- 2. 商品基础 ---
    title = scrapy.Field()
    brand = scrapy.Field()
    product_type = scrapy.Field()
    manufacturer = scrapy.Field()
    model_number = scrapy.Field()
    part_number = scrapy.Field()
    country_of_origin = scrapy.Field()
    is_customized = scrapy.Field()
    best_sellers_rank = scrapy.Field()

    # --- 3. 价格与交易 ---
    original_price = scrapy.Field()
    current_price = scrapy.Field()
    buybox_price = scrapy.Field()
    buybox_shipping = scrapy.Field()
    is_fba = scrapy.Field()

    # --- 4. 库存与配送 ---
    stock_count = scrapy.Field()
    stock_status = scrapy.Field()
    delivery_date = scrapy.Field()
    delivery_time = scrapy.Field()

    # --- 5. 内容描述 ---
    image_urls = scrapy.Field()
    bullet_points = scrapy.Field()
    long_description = scrapy.Field()

    # --- 6. 编码与分类 ---
    upc_list = scrapy.Field()
    ean_list = scrapy.Field()
    parent_asin = scrapy.Field()
    variation_asins = scrapy.Field()
    root_category_id = scrapy.Field()
    category_ids = scrapy.Field()
    category_tree = scrapy.Field()

    # --- 7. 规格参数 ---
    first_available_date = scrapy.Field()
    package_dimensions = scrapy.Field()
    package_weight = scrapy.Field()
    item_dimensions = scrapy.Field()
    item_weight = scrapy.Field()

    # --- ★★★ [新增] 调试字段 ★★★ ---
    source_html = scrapy.Field()  # 必须添加这个，否则会报错 KeyError