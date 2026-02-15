import scrapy
import re
import datetime
import dateparser
import json
import os
from scrapy_redis.spiders import RedisSpider
from amazon_spider.items import AmazonProductItem


class AmazonDistSpider(RedisSpider):
    name = "amazon_dist"
    redis_key = 'amazon:requests'

    # 垃圾文字黑名单
    BULLET_BLACKLIST = [
        "go to your orders", "start the return", "free shipping option", "drop off",
        "leave!", "return this item", "money back", "customer service",
        "full refund", "eligible for return"
    ]

    def parse(self, response, **kwargs):
        asin = response.url.split('/dp/')[-1].split('/')[0].split('?')[0]

        # 0. 404 商品不存在处理
        if response.status == 404:
            self.logger.warning(f"❌ [商品不存在] ASIN: {asin}")
            item = AmazonProductItem()
            item['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            item['site'] = 'US'
            item['asin'] = asin
            item['product_url'] = f"https://www.amazon.com/dp/{asin}"
            item['title'] = '[商品不存在]'
            # 其他字段填充 N/A
            for field in ['brand', 'current_price', 'buybox_price', 'original_price', 'buybox_shipping',
                          'is_fba', 'delivery_date', 'delivery_time', 'is_customized', 'model_number',
                          'part_number', 'country_of_origin', 'best_sellers_rank', 'manufacturer',
                          'product_type', 'first_available_date', 'item_dimensions', 'item_weight',
                          'package_dimensions', 'package_weight', 'stock_status', 'bullet_points',
                          'long_description', 'image_urls', 'upc_list', 'ean_list', 'parent_asin',
                          'variation_asins', 'category_ids', 'root_category_id', 'category_tree']:
                item[field] = 'N/A'
            item['stock_count'] = 0
            item['source_html'] = ''
            yield item
            return

        # 1. 反爬虫检测 - URL 检测
        if "captcha" in response.url.lower():
            self.logger.warning(f"❌ [验证码拦截] URL检测到验证码，请求重试: {asin}")
            yield response.request.replace(dont_filter=True)
            return

        # 2. 反爬虫检测 - 页面内容检测
        page_text = response.text
        if "validateCaptcha" in page_text or "Robot Check" in page_text:
            self.logger.warning(f"❌ [验证码拦截] 内容检测到验证码，请求重试: {asin}")
            yield response.request.replace(dont_filter=True)
            return

        # 3. 全页预扫描
        page_details = self._parse_all_details_once(response)

        item = AmazonProductItem()
        item['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item['site'] = 'US'
        item['asin'] = asin
        item['product_url'] = f"https://www.amazon.com/dp/{asin}"

        # 4. 标题抓取
        meta_title = response.xpath('//meta[@name="title"]/@content').get()
        visible_title = self._get_text_from_list(response, ['//span[@id="productTitle"]/text()', '//h1/span/text()'])
        item['title'] = visible_title if visible_title else (meta_title if meta_title else "N/A")

        # 标题为空判定为软拦截
        if not item['title'] or item['title'] == 'N/A':
            self.logger.warning(f"⚠️ [数据缺失] 未获取到标题 (疑似软拦截)，正在安排自动重试: {asin}")
            yield response.request.replace(dont_filter=True)
            return

        # 4.1 配送地/邮编检查
        zip_code, country = self._extract_delivery_info(response)
        
        # 判定：如果是 China，或者非美国地区且无邮编 -> 视为邮编错误
        if country and "china" in country.lower():
            self.logger.warning(f"❌ [邮编错误] 配送地为 China，重新加入采集队列 ASIN: {asin}")
            yield response.request.replace(dont_filter=True)
            return

        # 保存邮编 (即使没提取到具体的 5 位数，也可以存 N/A，但如果是 China 则上面已经拦截了)
        item['zip_code'] = zip_code if zip_code else "N/A"

        # 5. 检测商品可售状态
        is_unavailable = self._check_unavailable(response)
        is_see_price_in_cart = self._check_see_price_in_cart(response)

        # 6. 品牌抓取 (只使用 Brand，禁止使用 Manufacturer)
        brand_from_details = page_details.get('brand')
        item['brand'] = brand_from_details if brand_from_details else self._get_clean_brand(response)

        # 7. 价格抓取 - 根据可售状态处理
        if is_unavailable:
            item['current_price'] = '不可售'
            item['buybox_price'] = 'N/A'
            item['original_price'] = 'N/A'
            item['buybox_shipping'] = 'N/A'
            item['is_fba'] = 'N/A'
            item['stock_status'] = 'Currently unavailable'
            item['stock_count'] = 0
            item['delivery_date'] = 'N/A'
            item['delivery_time'] = 'N/A'
        elif is_see_price_in_cart:
            item['current_price'] = 'See price in cart'
            item['buybox_price'] = 'N/A'
            orig = response.xpath('//span[@data-a-strike="true"]//span[@class="a-offscreen"]/text()').get()
            item['original_price'] = orig.strip() if orig else "N/A"
            item['buybox_shipping'] = self._get_buybox_shipping(response, None)
            item['is_fba'] = self._detect_fulfillment_strict(response)
            stock_text = self._get_text_from_list(response, ['//div[@id="availability"]/span/text()'])
            item['stock_status'] = stock_text.strip() if stock_text else "In Stock"
            item['stock_count'] = self._parse_stock_count(item['stock_status'], response)
            date_txt, days_int = self._calculate_delivery_metrics(response)
            item['delivery_date'] = date_txt
            item['delivery_time'] = days_int
        else:
            item['current_price'] = self._get_precise_price(response)
            bb_price = self._get_buybox_price(response)
            item['buybox_price'] = bb_price if bb_price else item['current_price']
            orig = response.xpath('//span[@data-a-strike="true"]//span[@class="a-offscreen"]/text()').get()
            item['original_price'] = orig.strip() if orig else "N/A"
            item['buybox_shipping'] = self._get_buybox_shipping(response, item['current_price'])
            item['is_fba'] = self._detect_fulfillment_strict(response)
            stock_text = self._get_text_from_list(response, ['//div[@id="availability"]/span/text()'])
            item['stock_status'] = stock_text.strip() if stock_text else "In Stock"
            item['stock_count'] = self._parse_stock_count(item['stock_status'], response)
            date_txt, days_int = self._calculate_delivery_metrics(response)
            item['delivery_date'] = date_txt
            item['delivery_time'] = days_int

        # 8. 是否定制
        item['is_customized'] = self._detect_customization(response)

        # 9. 详情参数映射
        item['model_number'] = page_details.get('model_number', 'N/A')
        item['part_number'] = page_details.get('part_number', 'N/A')
        item['country_of_origin'] = page_details.get('country_of_origin', 'N/A')
        item['best_sellers_rank'] = page_details.get('best_sellers_rank', 'N/A')
        item['manufacturer'] = page_details.get('manufacturer', 'N/A')
        item['product_type'] = page_details.get('product_type', 'N/A')
        item['first_available_date'] = page_details.get('date_first_available', 'N/A')
        item['item_dimensions'] = page_details.get('item_dimensions', 'N/A')
        item['item_weight'] = page_details.get('item_weight', 'N/A')
        item['package_dimensions'] = page_details.get('package_dimensions', 'N/A')
        item['package_weight'] = page_details.get('package_weight', 'N/A')

        # 10. 五点描述 (精准定位)
        bullets = response.xpath('//div[@id="feature-bullets"]//ul/li//span[@class="a-list-item"]//text()').getall()
        if not bullets:
            bullets = response.xpath(
                '//div[contains(@class,"a-expander-content")]//ul/li//span[@class="a-list-item"]//text()').getall()

        clean_bullets = []
        for b in bullets:
            txt = b.strip()
            if not txt or len(txt) < 2: continue

            is_spam = False
            for spam in self.BULLET_BLACKLIST:
                if spam in txt.lower(): is_spam = True; break

            if not is_spam: clean_bullets.append(txt)

        item['bullet_points'] = "\n".join(clean_bullets)

        # 11. 长描述 (图文混排)
        item['long_description'] = self._get_mixed_description(response)

        # 12. 图片链接 (换行分隔)
        img_urls = []
        try:
            img_script = response.xpath('//script[contains(text(), "colorImages")]/text()').get()
            if img_script:
                urls = re.findall(r'"hiRes":"(https://[^"]+)"', img_script) or re.findall(r'"large":"(https://[^"]+)"',
                                                                                          img_script)
                img_urls = list(set(urls))
            else:
                img_urls = response.xpath('//div[@id="imgTagWrapperId"]/img/@src').getall()
        except Exception:
            pass
        item['image_urls'] = "\n".join(img_urls)

        # 13. UPC/EAN/类目
        upc_set = set(re.findall(r'"upc":"(\d+)"', response.text))
        if 'upc' in page_details: upc_set.add(page_details['upc'])
        upc_set.update(re.findall(r'UPC\s*[:#]?\s*(\d{12})', response.text))
        item['upc_list'] = ",".join(list(upc_set))
        item['ean_list'] = ",".join(list(set(re.findall(r'"gtin13":"(\d+)"', response.text))))

        parent = re.search(r'"parentAsin":"(\w+)"', response.text)
        item['parent_asin'] = parent.group(1) if parent else item['asin']
        vars = re.findall(r'"asin":"(\w+)"', response.text)
        item['variation_asins'] = ",".join(list(set(vars) - {item['asin'], item['parent_asin']}))

        cids = re.findall(r'node=(\d+)',
                          "".join(response.xpath('//div[@id="wayfinding-breadcrumbs_feature_div"]//a/@href').getall()))
        item['category_ids'] = ",".join(cids)
        item['root_category_id'] = cids[0] if cids else "N/A"
        item['category_tree'] = " > ".join(
            response.xpath('//div[@id="wayfinding-breadcrumbs_feature_div"]//li//a/text()').getall()).strip()

        if self.settings.getbool('SAVE_HTML_SOURCE', False):
            item['source_html'] = response.text
        else:
            item['source_html'] = ''

        # 日志输出 - 新格式
        self.logger.info(f"✅ {item['zip_code']} | ASIN: {item['asin']} | Title: {item['title'][:30]}... | Price: {item['current_price']} | Delivery: {item['delivery_date']} | FBA: {item['is_fba']}")
        yield item

    # ================= 工具方法 (Helper Methods) =================

    def _extract_delivery_info(self, response):
        """
        提取配送信息：邮编和国家/地区
        返回: (zip_code, country)
        """
        # 1. 尝试从 line1 提取邮编 (例如: "Delivering to Nashville 37217")
        line1 = response.xpath('//span[@id="glow-ingress-line1"]/text()').get()
        zip_code = None
        if line1:
            line1 = line1.strip()
            # 提取 5 位数字
            match = re.search(r'(\d{5})', line1)
            if match:
                zip_code = match.group(1)

        # 2. 尝试从 line2 提取国家 (例如: "China" 或 "Update location")
        line2 = response.xpath('//span[@id="glow-ingress-line2"]/text()').get()
        country = None
        if line2:
            country = line2.strip()
            
        # 特殊情况：如果 line1 包含 "Deliver to"，line2 通常是国家
        if line1 and "Deliver to" in line1:
            # 此时 line2 就是国家
            pass
        elif line1 and "Delivering to" in line1:
            # 此时通常是美国地址，line2 是 "Update location"
            if not country or "Update" in country:
                country = "United States"

        return zip_code, country

    def _check_unavailable(self, response):
        """检测商品是否不可售"""
        availability_text = response.xpath('//div[@id="availability"]//text()').getall()
        full_text = " ".join(availability_text).lower()
        return "currently unavailable" in full_text

    def _check_see_price_in_cart(self, response):
        """检测是否需要加入购物车才能看价格"""
        # 检查 "See price in cart" 链接
        see_price_link = response.xpath('//a[contains(text(), "See price in cart")]').get()
        if see_price_link:
            return True
        # 检查弹窗标识
        map_help = response.xpath('//*[@id="a-popover-map_help_pop_"]').get()
        if map_help:
            return True
        # 检查表格中的提示
        price_table = response.xpath('//table[@class="a-lineitem"]//text()').getall()
        if any("see price in cart" in t.lower() for t in price_table):
            return True
        return False

    def _get_buybox_shipping(self, response, current_price):
        """
        获取 BuyBox 运费
        优先级:
        1. Prime 会员免费配送
        2. 满额免邮判断
        3. 普通运费
        """
        delivery_block_text = " ".join(response.xpath('//div[@id="deliveryBlockMessage"]//text()').getall())

        # 1. 检测 Prime 会员免费配送
        if "prime members get free delivery" in delivery_block_text.lower():
            return "FREE"

        # 2. 检测满额免邮
        free_over_match = re.search(r'free delivery.*?on orders over \$(\d+(?:\.\d+)?)', delivery_block_text, re.IGNORECASE)
        if free_over_match:
            threshold = float(free_over_match.group(1))
            # 解析当前价格进行比较
            if current_price and current_price not in ['N/A', 'See price in cart', '不可售']:
                price_match = re.search(r'\$?([\d,]+\.?\d*)', current_price)
                if price_match:
                    price_value = float(price_match.group(1).replace(',', ''))
                    if price_value >= threshold:
                        return "FREE"
            return "N/A"  # 不满足条件

        # 3. 提取 data-csa-c-delivery-price 属性
        delivery_price = response.xpath('//span[@data-csa-c-delivery-price]/@data-csa-c-delivery-price').get()
        if delivery_price:
            return delivery_price.strip()

        # 4. 兜底
        shipping_text = self._get_text_from_list(response, [
            '//div[@id="mir-layout-loaded-comparison-row-2"]//span/text()'
        ])
        return shipping_text if shipping_text else "FREE"



    def _parse_stock_count(self, stock_status, response=None):
        """解析库存数量 (增强版: 支持下拉菜单检测)"""
        if not stock_status:
            stock_status = "In Stock" # 默认防空
            
        stock_lower = stock_status.lower()
        
        # 1. "Only X left"
        if "only" in stock_lower:
            count = re.search(r'(\d+)', stock_status)
            return int(count.group(1)) if count else 999
        # 2. 缺货
        elif "out of stock" in stock_lower or "unavailable" in stock_lower:
            return 0
        
        # 3. 尝试下拉菜单 (当没有 Only X left 提示时)
        if response:
            try:
                dq = self._get_dropdown_quantity(response)
                if dq: return dq
            except Exception:
                pass
                
        return 999

    def _get_dropdown_quantity(self, response):
        """解析 Quantity 下拉菜单的最大值"""
        # 优先找 name="quantity" 的 select
        options = response.xpath('//select[@name="quantity"]//option/@value').getall()
        if not options:
            options = response.xpath('//select[@id="quantity"]//option/@value').getall()
            
        if options:
            values = []
            for v in options:
                v = v.strip()
                if v.isdigit():
                    values.append(int(v))
            if values:
                return max(values)
        return None

    def _parse_all_details_once(self, response):
        """
        全页扫描逻辑，提取 Product Information 表格内容。
        """
        d = {}
        # 扫描表格行
        for row in response.xpath('//th/../..//tr'):
            k = row.xpath('normalize-space(./th)').get()
            v = row.xpath('normalize-space(./td)').get()
            if k and v: self._map(d, k, v)
        # 扫描列表项
        for s in response.xpath('//li/span/span[contains(@class, "a-text-bold")]'):
            k = s.xpath('normalize-space(./text())').get()
            v = s.xpath('normalize-space(./following-sibling::span)').get()
            if k and v: self._map(d, k.replace(':', ''), v)

        gl = re.search(r'"gl_product_group_type":"([^"]+)"', response.text)
        if gl: d['product_type'] = gl.group(1)
        return d

    def _map(self, d, k, v):
        """字段名映射 - 注意：brand 和 manufacturer 是独立的"""
        k = k.lower()
        if 'model number' in k:
            d['model_number'] = v
        elif 'part number' in k:
            d['part_number'] = v
        elif 'country of origin' in k:
            d['country_of_origin'] = v
        elif 'best sellers rank' in k:
            d['best_sellers_rank'] = v
        elif 'manufacturer' in k:
            d['manufacturer'] = v  # 仅存储为 manufacturer，不作为 brand
        elif 'brand' in k and 'processor' not in k and 'compatible' not in k:
            d['brand'] = v  # Brand 单独存储
        elif 'date first available' in k:
            d['date_first_available'] = v
        elif 'upc' in k:
            d['upc'] = v
        elif 'weight' in k and 'item' in k:
            d['item_weight'] = v
        elif 'weight' in k and 'package' in k:
            _, w = self._sdw(v);
            d['package_weight'] = w if w != "N/A" else "N/A"
        elif 'dimensions' in k:
            dim, _ = self._sdw(v)
            if 'package' in k:
                d['package_dimensions'] = dim
            else:
                d['item_dimensions'] = dim

    def _detect_fulfillment_strict(self, response):
        """FBA 严格判定"""
        # 1. 优先匹配结构化标签
        buybox = response.xpath('//div[@id="tabular-buybox"]//tr')
        for row in buybox:
            label = row.xpath(
                './/span[contains(text(), "Ships from")]/text() | .//span[contains(text(), "Shipper")]/text()').get()
            if label:
                value = row.xpath('.//span[contains(@class, "a-color-base")]/text()').get() or ""
                if value and "amazon" not in value.lower(): return "FBM"
                if value and "amazon" in value.lower(): return "FBA"

        # 2. 文本扫码兜底
        blob = " ".join(
            response.xpath('//div[@id="rightCol"]//text() | //div[@id="tabular-buybox"]//text()').getall()).lower()
        match = re.search(r'(ships from|shipper / seller)\s*[:\s]*([a-z0-9\s]+)', blob)
        if match:
            seller_name = match.group(2).strip()
            if "amazon" not in seller_name: return "FBM"

        if "fulfilled by amazon" in blob or "prime" in blob or "amazon.com" in blob: return "FBA"
        return "FBA"

    def _calculate_delivery_metrics(self, response):
        """取最早配送日期"""
        texts = response.xpath(
            '//*[@data-csa-c-delivery-time]//text() | //div[contains(@class,"delivery-message")]//text()').getall()
        full_text = " ".join(texts)

        today = datetime.datetime.now()
        min_days = 999
        best_date_str = "N/A"

        matches = re.finditer(
            r'(Tomorrow|Today|Jan(?:uary)?\.?\s+\d+|Feb(?:ruary)?\.?\s+\d+|Mar(?:ch)?\.?\s+\d+|Apr(?:il)?\.?\s+\d+|May\.?\s+\d+|Jun(?:e)?\.?\s+\d+|Jul(?:y)?\.?\s+\d+|Aug(?:ust)?\.?\s+\d+|Sep(?:tember)?\.?\s+\d+|Oct(?:ober)?\.?\s+\d+|Nov(?:ember)?\.?\s+\d+|Dec(?:ember)?\.?\s+\d+)',
            full_text, re.IGNORECASE)

        found_any = False
        for m in matches:
            raw = m.group(1)
            days = 999
            if "today" in raw.lower():
                days = 0
            elif "tomorrow" in raw.lower():
                days = 1
            else:
                try:
                    dt = dateparser.parse(raw, settings={'PREFER_DATES_FROM': 'future'})
                    if dt:
                        if dt.month < today.month and today.month == 12:
                            dt = dt.replace(year=today.year + 1)
                        elif dt.year < today.year:
                            dt = dt.replace(year=today.year)
                        days = (dt.date() - today.date()).days
                except Exception:
                    continue

            if days >= 0 and days < min_days:
                min_days = days
                best_date_str = raw
                found_any = True

        if found_any: return best_date_str, str(min_days)
        return "N/A", "N/A"

    def _detect_customization(self, response):
        """
        判断是否为定制产品 (Customized / Personalization)
        逻辑：多维度检测，只要满足任意一条即判定为 Yes
        """

        # 1. [文本证据] 检查按钮文字 (忽略大小写)
        if response.xpath(
                '//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "customize now")]'):
            return "Yes"

        # 2. [提示语证据] 检查特定的提示文本
        texts = " ".join(
            response.xpath('//div[@id="rightCol"]//text() | //div[@id="tabular-buybox"]//text()').getall()).lower()
        if "needs to be customized" in texts or "customization required" in texts:
            return "Yes"

        return "No"

    def _get_mixed_description(self, response):
        """长描述图文混排提取"""
        nodes = response.xpath('//div[contains(@class,"aplus")]//*[self::p or self::img]')
        if not nodes:
            nodes = response.xpath('//*[@id="productDescription"]//*[self::p or self::img]')

        content_parts = []
        for node in nodes:
            src = node.xpath('./@src | ./@data-src').get()
            if src:
                if "pixel" not in src and "transparent" not in src:
                    content_parts.append(f"\n[Image: {src.strip()}]\n")
            else:
                text = "".join(node.xpath('.//text()').getall()).strip()
                if text and len(text) > 5:
                    content_parts.append(text)

        if not content_parts:
            json_desc = re.search(r'"description"\s*:\s*"([^"]+)"', response.text)
            if json_desc:
                try:
                    decoded = json_desc.group(1).encode('utf-8').decode('unicode_escape')
                    clean = re.sub(r'<[^>]+>', '\n', decoded)
                    return clean.strip()[:4000]
                except Exception:
                    pass

        return "\n".join(content_parts)[:10000]

    def _get_precise_price(self, r):
        p = r.xpath('//span[@class="a-offscreen"]/text()').get()
        if p and "$" in p: return p.strip()
        w, f = r.xpath('//span[@class="a-price-whole"]/text()').get(), r.xpath(
            '//span[@class="a-price-fraction"]/text()').get()
        return f"${w.replace('.', '')}.{f.strip()}" if w and f else "N/A"

    def _get_buybox_price(self, r):
        candidates = [
            '//*[@id="tabular-buybox"]//span[@class="a-offscreen"]/text()',
            '//*[@id="buybox"]//span[@class="a-offscreen"]/text()',
            '//*[@id="newBuyBoxPrice"]//text()',
            '//*[@id="priceToPay"]//span[@class="a-offscreen"]/text()',
            '//*[@id="price_inside_buybox"]/text()',
        ]
        for xp in candidates:
            t = r.xpath(xp).get()
            if t and re.search(r"\d", t): return t.strip()
        return None

    def _sdw(self, s):
        if not s: return "N/A", "N/A"
        p = s.split(';')
        return p[0].strip(), p[1].strip() if len(p) > 1 else "N/A"

    def _get_clean_brand(self, r):
        return re.sub(r'Visit the | Store|Brand: ', '', r.xpath('//a[@id="bylineInfo"]/text()').get() or "",
                      flags=re.IGNORECASE).strip() or "N/A"

    def _get_text_from_list(self, r, xps):
        for x in xps:
            v = r.xpath(x).get()
            if v: return v.strip()
        return None