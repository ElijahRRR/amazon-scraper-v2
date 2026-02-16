"""
Amazon 产品采集系统 v2 - 页面解析模块
参考原版 amazon_dist.py 的解析逻辑
优先使用 selectolax（Lexbor C 引擎，比 lxml 快 2-5x）
lxml 作为 fallback
健壮的错误处理（字段缺失不崩溃）
"""
import re
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

# 优先 selectolax，fallback 到 lxml
_USE_SELECTOLAX = False
try:
    from selectolax.parser import HTMLParser as SlxParser
    _USE_SELECTOLAX = True
except ImportError:
    SlxParser = None

try:
    from lxml import html as lxml_html
    from lxml import etree
except ImportError:
    lxml_html = None
    etree = None

logger = logging.getLogger(__name__)

if _USE_SELECTOLAX:
    logger.info("解析引擎: selectolax (Lexbor)")
else:
    logger.info("解析引擎: lxml (fallback)")


class AmazonParser:
    """Amazon 商品页面解析器"""

    # 垃圾文字黑名单（五点描述过滤用）
    BULLET_BLACKLIST = [
        "go to your orders", "start the return", "free shipping option", "drop off",
        "leave!", "return this item", "money back", "customer service",
        "full refund", "eligible for return",
    ]

    def parse_product(self, html_text: str, asin: str, zip_code: str = "10001") -> Dict[str, Any]:
        """
        解析 Amazon 商品页面
        返回包含所有字段的字典
        即使某些字段提取失败也不会崩溃
        """
        result = self._default_result(asin, zip_code)

        if not html_text:
            result["title"] = "[页面为空]"
            return result

        if _USE_SELECTOLAX:
            return self._parse_with_selectolax(html_text, asin, zip_code, result)
        else:
            return self._parse_with_lxml(html_text, asin, zip_code, result)

    # ==================== selectolax 解析路径 ====================

    def _parse_with_selectolax(self, html_text: str, asin: str, zip_code: str, result: Dict) -> Dict:
        """使用 selectolax 解析"""
        try:
            tree = SlxParser(html_text)
        except Exception as e:
            logger.error(f"HTML 解析失败: {e}")
            result["title"] = "[HTML解析失败]"
            return result

        # 检测反爬拦截
        block_status = self._check_block(html_text, None)
        if block_status:
            result["title"] = block_status
            return result

        # 全页预扫描 — 提取 Product Information 表格
        page_details = self._slx_parse_all_details(tree, html_text)

        # 逐字段提取
        result["title"] = self._slx_parse_title(tree)
        result["zip_code"] = self._slx_parse_zip_code(tree) or zip_code

        # 商品可售状态检测
        is_unavailable = self._slx_check_unavailable(tree)
        is_see_price_in_cart = self._slx_check_see_price_in_cart(tree)

        # 品牌
        result["brand"] = page_details.get("brand") or self._slx_parse_brand(tree)

        # 价格 & 库存 & 配送
        if is_unavailable:
            result["current_price"] = "不可售"
            result["buybox_price"] = "N/A"
            result["original_price"] = "N/A"
            result["buybox_shipping"] = "N/A"
            result["is_fba"] = "N/A"
            result["stock_status"] = "Currently unavailable"
            result["stock_count"] = "0"
            result["delivery_date"] = "N/A"
            result["delivery_time"] = "N/A"
        elif is_see_price_in_cart:
            result["current_price"] = "See price in cart"
            result["buybox_price"] = "N/A"
            result["original_price"] = self._slx_parse_original_price(tree)
            result["buybox_shipping"] = self._slx_parse_buybox_shipping(tree, None)
            result["is_fba"] = self._slx_parse_fulfillment(tree, html_text)
            avail_node = tree.css_first('div#availability span')
            stock_text = avail_node.text(strip=True) if avail_node else ""
            result["stock_status"] = stock_text if stock_text else "In Stock"
            result["stock_count"] = str(self._slx_parse_stock_count(result["stock_status"], tree))
            d_date, d_time = self._slx_parse_delivery(tree)
            result["delivery_date"] = d_date
            result["delivery_time"] = d_time
        else:
            result["current_price"] = self._slx_parse_current_price(tree)
            bb = self._slx_parse_buybox_price(tree)
            result["buybox_price"] = bb if bb else result["current_price"]
            result["original_price"] = self._slx_parse_original_price(tree)
            result["buybox_shipping"] = self._slx_parse_buybox_shipping(tree, result["current_price"])
            result["is_fba"] = self._slx_parse_fulfillment(tree, html_text)
            avail_node = tree.css_first('div#availability span')
            stock_text = avail_node.text(strip=True) if avail_node else ""
            result["stock_status"] = stock_text if stock_text else "In Stock"
            result["stock_count"] = str(self._slx_parse_stock_count(result["stock_status"], tree))
            d_date, d_time = self._slx_parse_delivery(tree)
            result["delivery_date"] = d_date
            result["delivery_time"] = d_time

        # 是否定制
        result["is_customized"] = self._slx_parse_customization(tree)

        # 详情参数
        result["model_number"] = page_details.get("model_number", "N/A")
        result["part_number"] = page_details.get("part_number", "N/A")
        result["country_of_origin"] = page_details.get("country_of_origin", "N/A")
        result["best_sellers_rank"] = page_details.get("best_sellers_rank", "N/A")
        result["manufacturer"] = page_details.get("manufacturer", "N/A")
        result["product_type"] = page_details.get("product_type", "N/A")
        result["first_available_date"] = page_details.get("date_first_available", "N/A")
        result["item_dimensions"] = page_details.get("item_dimensions", "N/A")
        result["item_weight"] = page_details.get("item_weight", "N/A")
        result["package_dimensions"] = page_details.get("package_dimensions", "N/A")
        result["package_weight"] = page_details.get("package_weight", "N/A")

        # 五点描述
        result["bullet_points"] = self._slx_parse_bullet_points(tree)

        # 长描述
        result["long_description"] = self._slx_parse_long_description(tree, html_text)

        # 图片
        result["image_urls"] = self._slx_parse_images(tree, html_text)

        # UPC / EAN
        result["upc_list"] = self._slx_parse_upc(tree, html_text, page_details)
        result["ean_list"] = self._parse_ean(html_text)

        # 父体 ASIN / 变体
        result["parent_asin"] = self._parse_parent_asin(html_text, asin)
        result["variation_asins"] = self._parse_variation_asins(html_text, asin, result["parent_asin"])

        # 类目
        result["root_category_id"], result["category_ids"], result["category_tree"] = \
            self._slx_parse_categories(tree)

        return result

    # ---- selectolax 辅助 ----

    def _slx_text(self, tree, selectors: List[str]) -> Optional[str]:
        """从多个 CSS 选择器中尝试获取文本"""
        for sel in selectors:
            try:
                node = tree.css_first(sel)
                if node:
                    text = node.text(strip=True)
                    if text:
                        return text
            except Exception:
                continue
        return None

    def _slx_all_text(self, tree, selector: str) -> List[str]:
        """获取 CSS 选择器匹配的所有节点文本"""
        try:
            nodes = tree.css(selector)
            return [n.text(strip=True) for n in nodes if n.text(strip=True)]
        except Exception:
            return []

    def _slx_parse_title(self, tree) -> str:
        try:
            meta = tree.css_first('meta[name="title"]')
            visible = self._slx_text(tree, [
                'span#productTitle',
                'h1 > span',
            ])
            if visible:
                return visible
            if meta:
                content = meta.attributes.get('content', '')
                return content.strip() if content else "N/A"
            return "N/A"
        except Exception:
            return "N/A"

    def _slx_parse_zip_code(self, tree) -> Optional[str]:
        try:
            node = tree.css_first('span#glow-ingress-line1')
            if node:
                text = node.text(strip=True)
                match = re.search(r'(\d{5})', text)
                if match:
                    return match.group(1)
        except Exception:
            pass
        return None

    def _slx_parse_brand(self, tree) -> str:
        try:
            node = tree.css_first('a#bylineInfo')
            if node:
                brand_text = node.text(strip=True)
                brand = re.sub(r'Visit the | Store|Brand: ', '', brand_text, flags=re.IGNORECASE).strip()
                return brand if brand else "N/A"
        except Exception:
            pass
        return "N/A"

    def _slx_parse_current_price(self, tree) -> str:
        try:
            # 方法1: a-offscreen
            node = tree.css_first('span.a-offscreen')
            if node:
                p = node.text(strip=True)
                if p and "$" in p:
                    return p
            # 方法2: 拆分整数+小数
            whole_node = tree.css_first('span.a-price-whole')
            frac_node = tree.css_first('span.a-price-fraction')
            if whole_node and frac_node:
                whole = whole_node.text(strip=True)
                frac = frac_node.text(strip=True)
                if whole and frac:
                    return f"${whole.replace('.', '')}.{frac}"
        except Exception:
            pass
        return "N/A"

    def _slx_parse_buybox_price(self, tree) -> Optional[str]:
        selectors = [
            '#tabular-buybox span.a-offscreen',
            '#buybox span.a-offscreen',
            '#newBuyBoxPrice',
            '#priceToPay span.a-offscreen',
            '#price_inside_buybox',
        ]
        for sel in selectors:
            try:
                nodes = tree.css(sel)
                for node in nodes:
                    text = node.text(strip=True)
                    if text and re.search(r'\d', text):
                        return text
            except Exception:
                continue
        return None

    def _slx_parse_original_price(self, tree) -> str:
        try:
            node = tree.css_first('span[data-a-strike="true"] span.a-offscreen')
            if node:
                return node.text(strip=True)
        except Exception:
            pass
        return "N/A"

    def _slx_parse_buybox_shipping(self, tree, current_price: Optional[str]) -> str:
        try:
            delivery_nodes = tree.css('div#deliveryBlockMessage *')
            delivery_block = " ".join(n.text(strip=True) for n in delivery_nodes if n.text(strip=True))

            # Prime 免费配送
            if "prime members get free delivery" in delivery_block.lower():
                return "FREE"

            # 满额免邮
            free_over = re.search(r'free delivery.*?on orders over \$(\d+(?:\.\d+)?)', delivery_block, re.IGNORECASE)
            if free_over:
                threshold = float(free_over.group(1))
                if current_price and current_price not in ['N/A', 'See price in cart', '不可售']:
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', current_price)
                    if price_match:
                        price_val = float(price_match.group(1).replace(',', ''))
                        if price_val >= threshold:
                            return "FREE"
                return "N/A"

            # data-csa-c-delivery-price 属性
            dp_node = tree.css_first('span[data-csa-c-delivery-price]')
            if dp_node:
                dp = dp_node.attributes.get('data-csa-c-delivery-price', '')
                if dp:
                    return dp.strip()

            # 兜底
            shipping = self._slx_text(tree, [
                'div#mir-layout-loaded-comparison-row-2 span',
            ])
            return shipping if shipping else "FREE"
        except Exception:
            return "N/A"

    def _slx_check_unavailable(self, tree) -> bool:
        try:
            nodes = tree.css('div#availability *')
            full = " ".join(n.text(strip=True) for n in nodes if n.text(strip=True)).lower()
            return "currently unavailable" in full
        except Exception:
            return False

    def _slx_check_see_price_in_cart(self, tree) -> bool:
        try:
            # 检查链接文本
            for node in tree.css('a'):
                text = node.text(strip=True)
                if text and "see price in cart" in text.lower():
                    return True
            # 检查 popover
            if tree.css_first('#a-popover-map_help_pop_'):
                return True
            # 检查表格
            for node in tree.css('table.a-lineitem *'):
                text = node.text(strip=True)
                if text and "see price in cart" in text.lower():
                    return True
        except Exception:
            pass
        return False

    def _slx_parse_fulfillment(self, tree, html_text: str) -> str:
        try:
            # 1. 结构化标签
            rows = tree.css('div#tabular-buybox tr')
            for row in rows:
                # 检查是否包含 "Ships from"
                row_text = row.text(strip=True).lower()
                if "ships from" in row_text or "shipper" in row_text:
                    # 获取值节点
                    value_nodes = row.css('span.a-color-base')
                    for vn in value_nodes:
                        value = vn.text(strip=True)
                        if value:
                            if "amazon" not in value.lower():
                                return "FBM"
                            else:
                                return "FBA"

            # 2. 文本兜底
            blob_parts = []
            for sel in ['div#rightCol *', 'div#tabular-buybox *']:
                for n in tree.css(sel):
                    t = n.text(strip=True)
                    if t:
                        blob_parts.append(t)
            blob = " ".join(blob_parts).lower()

            match = re.search(r'(ships from|shipper / seller)\s*[:\s]*([a-z0-9\s]+)', blob)
            if match and "amazon" not in match.group(2).strip():
                return "FBM"
            if "fulfilled by amazon" in blob or "prime" in blob or "amazon.com" in blob:
                return "FBA"
        except Exception:
            pass
        return "FBA"

    def _slx_parse_stock_count(self, stock_status: str, tree) -> int:
        try:
            stock_lower = stock_status.lower() if stock_status else ""
            if "only" in stock_lower:
                count = re.search(r'(\d+)', stock_status)
                return int(count.group(1)) if count else 999
            if "out of stock" in stock_lower or "unavailable" in stock_lower:
                return 0
            # 下拉菜单
            options = tree.css('select[name="quantity"] option')
            if not options:
                options = tree.css('select#quantity option')
            if options:
                values = []
                for opt in options:
                    val = opt.attributes.get('value', '')
                    if val and val.strip().isdigit():
                        values.append(int(val.strip()))
                if values:
                    return max(values)
        except Exception:
            pass
        return 999

    def _slx_parse_delivery(self, tree) -> Tuple[str, str]:
        try:
            texts = []
            for n in tree.css('[data-csa-c-delivery-time] *, div.delivery-message *'):
                t = n.text(strip=True)
                if t:
                    texts.append(t)
            full_text = " ".join(texts)
            today = datetime.now()
            min_days = 999
            best_date_str = "N/A"

            pattern = (
                r'(Tomorrow|Today|'
                r'Jan(?:uary)?\.?\s+\d+|Feb(?:ruary)?\.?\s+\d+|Mar(?:ch)?\.?\s+\d+|'
                r'Apr(?:il)?\.?\s+\d+|May\.?\s+\d+|Jun(?:e)?\.?\s+\d+|'
                r'Jul(?:y)?\.?\s+\d+|Aug(?:ust)?\.?\s+\d+|Sep(?:tember)?\.?\s+\d+|'
                r'Oct(?:ober)?\.?\s+\d+|Nov(?:ember)?\.?\s+\d+|Dec(?:ember)?\.?\s+\d+)'
            )

            for m in re.finditer(pattern, full_text, re.IGNORECASE):
                raw = m.group(1)
                days = 999
                if "today" in raw.lower():
                    days = 0
                elif "tomorrow" in raw.lower():
                    days = 1
                else:
                    try:
                        import dateparser
                        dt = dateparser.parse(raw, settings={'PREFER_DATES_FROM': 'future'})
                        if dt:
                            if dt.month < today.month and today.month == 12:
                                dt = dt.replace(year=today.year + 1)
                            elif dt.year < today.year:
                                dt = dt.replace(year=today.year)
                            days = (dt.date() - today.date()).days
                    except Exception:
                        continue

                if 0 <= days < min_days:
                    min_days = days
                    best_date_str = raw

            if min_days < 999:
                return best_date_str, str(min_days)
        except Exception:
            pass
        return "N/A", "N/A"

    def _slx_parse_customization(self, tree) -> str:
        try:
            # 遍历所有文本节点查找 "customize now"
            for node in tree.css('*'):
                text = node.text(strip=True)
                if text and "customize now" in text.lower():
                    return "Yes"
            # 查找右列和 buybox 中的定制文本
            for sel in ['div#rightCol *', 'div#tabular-buybox *']:
                for n in tree.css(sel):
                    text = n.text(strip=True)
                    if text:
                        t = text.lower()
                        if "needs to be customized" in t or "customization required" in t:
                            return "Yes"
        except Exception:
            pass
        return "No"

    def _slx_parse_bullet_points(self, tree) -> str:
        try:
            bullets = []
            nodes = tree.css('div#feature-bullets ul > li span.a-list-item')
            if not nodes:
                nodes = tree.css('div.a-expander-content ul > li span.a-list-item')
            for n in nodes:
                txt = n.text(strip=True)
                if txt:
                    bullets.append(txt)

            clean = []
            for b in bullets:
                if len(b) < 2:
                    continue
                if any(spam in b.lower() for spam in self.BULLET_BLACKLIST):
                    continue
                clean.append(b)
            return "\n".join(clean)
        except Exception:
            return ""

    def _slx_parse_long_description(self, tree, html_text: str) -> str:
        try:
            parts = []
            # A+ 内容
            nodes = tree.css('div.aplus p, div.aplus img')
            if not nodes:
                nodes = tree.css('#productDescription p, #productDescription img')

            for node in nodes:
                tag = node.tag
                if tag == 'img':
                    src = node.attributes.get('src') or node.attributes.get('data-src', '')
                    if src and "pixel" not in src and "transparent" not in src:
                        parts.append(f"\n[Image: {src.strip()}]\n")
                else:
                    text = node.text(strip=True)
                    if text and len(text) > 5:
                        parts.append(text)

            if not parts:
                json_desc = re.search(r'"description"\s*:\s*"([^"]+)"', html_text)
                if json_desc:
                    try:
                        decoded = json_desc.group(1).encode('utf-8').decode('unicode_escape')
                        clean = re.sub(r'<[^>]+>', '\n', decoded)
                        return clean.strip()[:4000]
                    except Exception:
                        pass

            return "\n".join(parts)[:10000]
        except Exception:
            return ""

    def _slx_parse_images(self, tree, html_text: str) -> str:
        try:
            img_urls = []
            scripts = tree.css('script')
            for script in scripts:
                text = script.text(strip=True)
                if text and "colorImages" in text:
                    urls = re.findall(r'"hiRes":"(https://[^"]+)"', text) or \
                           re.findall(r'"large":"(https://[^"]+)"', text)
                    img_urls = list(set(urls))
                    break
            if not img_urls:
                img_node = tree.css_first('div#imgTagWrapperId img')
                if img_node:
                    src = img_node.attributes.get('src', '')
                    if src:
                        img_urls = [src]
            return "\n".join(img_urls)
        except Exception:
            return ""

    def _slx_parse_upc(self, tree, html_text: str, page_details: Dict) -> str:
        try:
            upc_set = set(re.findall(r'"upc":"(\d+)"', html_text))
            if 'upc' in page_details:
                upc_set.add(page_details['upc'])
            upc_set.update(re.findall(r'UPC\s*[:#]?\s*(\d{12})', html_text))
            return ",".join(list(upc_set))
        except Exception:
            return ""

    def _slx_parse_categories(self, tree) -> Tuple[str, str, str]:
        try:
            breadcrumb = tree.css('div#wayfinding-breadcrumbs_feature_div a')
            cids = []
            names = []
            for a in breadcrumb:
                href = a.attributes.get('href', '')
                node_match = re.search(r'node=(\d+)', href)
                if node_match:
                    cids.append(node_match.group(1))
                name = a.text(strip=True)
                if name:
                    names.append(name)

            category_ids = ",".join(cids)
            root_id = cids[0] if cids else "N/A"
            category_tree = " > ".join(names)

            return root_id, category_ids, category_tree
        except Exception:
            return "N/A", "", ""

    def _slx_parse_all_details(self, tree, html_text: str) -> Dict[str, str]:
        """selectolax 版全页扫描：提取 Product Information 表格内容"""
        d = {}
        try:
            # 扫描表格行
            for row in tree.css('tr'):
                try:
                    th = row.css_first('th')
                    td = row.css_first('td')
                    if th and td:
                        k = th.text(strip=True)
                        v = td.text(strip=True)
                        if k and v:
                            self._map_detail(d, k, v)
                except Exception:
                    continue

            # 扫描列表项
            for s in tree.css('li > span > span.a-text-bold'):
                try:
                    k = s.text(strip=True)
                    # 获取下一个 sibling span
                    parent = s.parent
                    if parent:
                        spans = parent.css('span')
                        for i, sp in enumerate(spans):
                            if sp.text(strip=True) == k and i + 1 < len(spans):
                                v = spans[i + 1].text(strip=True)
                                if k and v:
                                    self._map_detail(d, k.replace(':', ''), v)
                                break
                except Exception:
                    continue

            # product_type 从 JSON 中提取
            gl = re.search(r'"gl_product_group_type":"([^"]+)"', html_text)
            if gl:
                d['product_type'] = gl.group(1)
        except Exception as e:
            logger.warning(f"详情解析异常: {e}")

        return d

    # ==================== lxml 解析路径 (fallback) ====================

    def _parse_with_lxml(self, html_text: str, asin: str, zip_code: str, result: Dict) -> Dict:
        """使用 lxml 解析（fallback）"""
        try:
            tree = lxml_html.fromstring(html_text)
        except Exception as e:
            logger.error(f"HTML 解析失败: {e}")
            result["title"] = "[HTML解析失败]"
            return result

        # 检测反爬拦截
        block_status = self._check_block(html_text, tree)
        if block_status:
            result["title"] = block_status
            return result

        # 全页预扫描 — 提取 Product Information 表格
        page_details = self._parse_all_details(tree, html_text)

        # 逐字段提取（每个都有 try-except 保护）
        result["title"] = self._parse_title(tree)
        result["zip_code"] = self._parse_zip_code(tree) or zip_code

        # 商品可售状态检测
        is_unavailable = self._check_unavailable(tree)
        is_see_price_in_cart = self._check_see_price_in_cart(tree)

        # 品牌
        result["brand"] = page_details.get("brand") or self._parse_brand(tree)

        # 价格 & 库存 & 配送
        if is_unavailable:
            result["current_price"] = "不可售"
            result["buybox_price"] = "N/A"
            result["original_price"] = "N/A"
            result["buybox_shipping"] = "N/A"
            result["is_fba"] = "N/A"
            result["stock_status"] = "Currently unavailable"
            result["stock_count"] = "0"
            result["delivery_date"] = "N/A"
            result["delivery_time"] = "N/A"
        elif is_see_price_in_cart:
            result["current_price"] = "See price in cart"
            result["buybox_price"] = "N/A"
            result["original_price"] = self._parse_original_price(tree)
            result["buybox_shipping"] = self._parse_buybox_shipping(tree, None)
            result["is_fba"] = self._parse_fulfillment(tree, html_text)
            stock_text = self._get_text(tree, ['//div[@id="availability"]/span/text()'])
            result["stock_status"] = stock_text.strip() if stock_text else "In Stock"
            result["stock_count"] = str(self._parse_stock_count(result["stock_status"], tree))
            d_date, d_time = self._parse_delivery(tree)
            result["delivery_date"] = d_date
            result["delivery_time"] = d_time
        else:
            result["current_price"] = self._parse_current_price(tree)
            bb = self._parse_buybox_price(tree)
            result["buybox_price"] = bb if bb else result["current_price"]
            result["original_price"] = self._parse_original_price(tree)
            result["buybox_shipping"] = self._parse_buybox_shipping(tree, result["current_price"])
            result["is_fba"] = self._parse_fulfillment(tree, html_text)
            stock_text = self._get_text(tree, ['//div[@id="availability"]/span/text()'])
            result["stock_status"] = stock_text.strip() if stock_text else "In Stock"
            result["stock_count"] = str(self._parse_stock_count(result["stock_status"], tree))
            d_date, d_time = self._parse_delivery(tree)
            result["delivery_date"] = d_date
            result["delivery_time"] = d_time

        # 是否定制
        result["is_customized"] = self._parse_customization(tree)

        # 详情参数
        result["model_number"] = page_details.get("model_number", "N/A")
        result["part_number"] = page_details.get("part_number", "N/A")
        result["country_of_origin"] = page_details.get("country_of_origin", "N/A")
        result["best_sellers_rank"] = page_details.get("best_sellers_rank", "N/A")
        result["manufacturer"] = page_details.get("manufacturer", "N/A")
        result["product_type"] = page_details.get("product_type", "N/A")
        result["first_available_date"] = page_details.get("date_first_available", "N/A")
        result["item_dimensions"] = page_details.get("item_dimensions", "N/A")
        result["item_weight"] = page_details.get("item_weight", "N/A")
        result["package_dimensions"] = page_details.get("package_dimensions", "N/A")
        result["package_weight"] = page_details.get("package_weight", "N/A")

        # 五点描述
        result["bullet_points"] = self._parse_bullet_points(tree)

        # 长描述
        result["long_description"] = self._parse_long_description(tree, html_text)

        # 图片
        result["image_urls"] = self._parse_images(tree, html_text)

        # UPC / EAN
        result["upc_list"] = self._parse_upc(tree, html_text, page_details)
        result["ean_list"] = self._parse_ean(html_text)

        # 父体 ASIN / 变体
        result["parent_asin"] = self._parse_parent_asin(html_text, asin)
        result["variation_asins"] = self._parse_variation_asins(html_text, asin, result["parent_asin"])

        # 类目
        result["root_category_id"], result["category_ids"], result["category_tree"] = \
            self._parse_categories(tree)

        return result

    # ==================== 通用辅助方法 ====================

    def _default_result(self, asin: str, zip_code: str) -> Dict[str, Any]:
        """创建默认结果字典"""
        return {
            "asin": asin,
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "site": "US",
            "zip_code": zip_code,
            "product_url": f"https://www.amazon.com/dp/{asin}",
            "title": "N/A",
            "brand": "N/A",
            "product_type": "N/A",
            "manufacturer": "N/A",
            "model_number": "N/A",
            "part_number": "N/A",
            "country_of_origin": "N/A",
            "is_customized": "No",
            "best_sellers_rank": "N/A",
            "original_price": "N/A",
            "current_price": "N/A",
            "buybox_price": "N/A",
            "buybox_shipping": "N/A",
            "is_fba": "N/A",
            "stock_count": "0",
            "stock_status": "N/A",
            "delivery_date": "N/A",
            "delivery_time": "N/A",
            "image_urls": "",
            "bullet_points": "",
            "long_description": "",
            "upc_list": "",
            "ean_list": "",
            "parent_asin": asin,
            "variation_asins": "",
            "root_category_id": "N/A",
            "category_ids": "",
            "category_tree": "",
            "first_available_date": "N/A",
            "package_dimensions": "N/A",
            "package_weight": "N/A",
            "item_dimensions": "N/A",
            "item_weight": "N/A",
        }

    def _check_block(self, html_text: str, tree) -> Optional[str]:
        """检测反爬拦截，返回拦截类型或 None"""
        if "validateCaptcha" in html_text or "Robot Check" in html_text:
            return "[验证码拦截]"
        if "api-services-support@amazon.com" in html_text:
            return "[API封锁]"
        return None

    # ==================== 纯文本/正则方法（两种引擎共用）====================

    def _parse_ean(self, html_text: str) -> str:
        try:
            eans = set(re.findall(r'"gtin13":"(\d+)"', html_text))
            return ",".join(list(eans))
        except Exception:
            return ""

    def _parse_parent_asin(self, html_text: str, asin: str) -> str:
        try:
            parent = re.search(r'"parentAsin":"(\w+)"', html_text)
            return parent.group(1) if parent else asin
        except Exception:
            return asin

    def _parse_variation_asins(self, html_text: str, asin: str, parent_asin: str) -> str:
        try:
            all_asins = set(re.findall(r'"asin":"(\w+)"', html_text))
            variations = all_asins - {asin, parent_asin}
            return ",".join(list(variations))
        except Exception:
            return ""

    def _map_detail(self, d: Dict, k: str, v: str):
        """字段名映射"""
        k_lower = k.lower()
        if 'model number' in k_lower:
            d['model_number'] = v
        elif 'part number' in k_lower:
            d['part_number'] = v
        elif 'country of origin' in k_lower:
            d['country_of_origin'] = v
        elif 'best sellers rank' in k_lower:
            d['best_sellers_rank'] = v
        elif 'manufacturer' in k_lower:
            d['manufacturer'] = v
        elif 'brand' in k_lower and 'processor' not in k_lower and 'compatible' not in k_lower:
            d['brand'] = v
        elif 'date first available' in k_lower:
            d['date_first_available'] = v
        elif 'upc' in k_lower:
            d['upc'] = v
        elif 'weight' in k_lower and 'item' in k_lower:
            d['item_weight'] = v
        elif 'weight' in k_lower and 'package' in k_lower:
            _, w = self._split_dim_weight(v)
            d['package_weight'] = w if w != "N/A" else "N/A"
        elif 'dimensions' in k_lower:
            dim, _ = self._split_dim_weight(v)
            if 'package' in k_lower:
                d['package_dimensions'] = dim
            else:
                d['item_dimensions'] = dim

    def _split_dim_weight(self, s: str) -> Tuple[str, str]:
        """拆分尺寸和重量（用分号分隔）"""
        if not s:
            return "N/A", "N/A"
        parts = s.split(';')
        return parts[0].strip(), (parts[1].strip() if len(parts) > 1 else "N/A")

    # ==================== lxml 字段解析方法 (fallback) ====================

    def _get_text(self, tree, xpaths: List[str]) -> Optional[str]:
        """从多个 XPath 中尝试获取文本"""
        for xp in xpaths:
            try:
                result = tree.xpath(xp)
                if result:
                    text = result[0].strip() if isinstance(result[0], str) else ""
                    if text:
                        return text
            except Exception:
                continue
        return None

    def _get_all_text(self, tree, xpath: str) -> List[str]:
        """获取 XPath 匹配的所有文本"""
        try:
            return tree.xpath(xpath)
        except Exception:
            return []

    def _parse_title(self, tree) -> str:
        try:
            meta = tree.xpath('//meta[@name="title"]/@content')
            visible = self._get_text(tree, [
                '//span[@id="productTitle"]/text()',
                '//h1/span/text()',
            ])
            return visible if visible else (meta[0].strip() if meta else "N/A")
        except Exception:
            return "N/A"

    def _parse_zip_code(self, tree) -> Optional[str]:
        try:
            line1 = self._get_text(tree, ['//span[@id="glow-ingress-line1"]/text()'])
            if line1:
                match = re.search(r'(\d{5})', line1)
                if match:
                    return match.group(1)
        except Exception:
            pass
        return None

    def _parse_brand(self, tree) -> str:
        try:
            brand_text = self._get_text(tree, ['//a[@id="bylineInfo"]/text()'])
            if brand_text:
                brand = re.sub(r'Visit the | Store|Brand: ', '', brand_text, flags=re.IGNORECASE).strip()
                return brand if brand else "N/A"
        except Exception:
            pass
        return "N/A"

    def _parse_current_price(self, tree) -> str:
        try:
            p = self._get_text(tree, ['//span[@class="a-offscreen"]/text()'])
            if p and "$" in p:
                return p.strip()
            whole = self._get_text(tree, ['//span[@class="a-price-whole"]/text()'])
            frac = self._get_text(tree, ['//span[@class="a-price-fraction"]/text()'])
            if whole and frac:
                return f"${whole.replace('.', '')}.{frac.strip()}"
        except Exception:
            pass
        return "N/A"

    def _parse_buybox_price(self, tree) -> Optional[str]:
        xpaths = [
            '//*[@id="tabular-buybox"]//span[@class="a-offscreen"]/text()',
            '//*[@id="buybox"]//span[@class="a-offscreen"]/text()',
            '//*[@id="newBuyBoxPrice"]//text()',
            '//*[@id="priceToPay"]//span[@class="a-offscreen"]/text()',
            '//*[@id="price_inside_buybox"]/text()',
        ]
        for xp in xpaths:
            try:
                vals = tree.xpath(xp)
                for v in vals:
                    if isinstance(v, str) and re.search(r'\d', v):
                        return v.strip()
            except Exception:
                continue
        return None

    def _parse_original_price(self, tree) -> str:
        try:
            orig = tree.xpath('//span[@data-a-strike="true"]//span[@class="a-offscreen"]/text()')
            if orig:
                return orig[0].strip()
        except Exception:
            pass
        return "N/A"

    def _parse_buybox_shipping(self, tree, current_price: Optional[str]) -> str:
        try:
            delivery_texts = self._get_all_text(tree, '//div[@id="deliveryBlockMessage"]//text()')
            delivery_block = " ".join(delivery_texts)

            if "prime members get free delivery" in delivery_block.lower():
                return "FREE"

            free_over = re.search(r'free delivery.*?on orders over \$(\d+(?:\.\d+)?)', delivery_block, re.IGNORECASE)
            if free_over:
                threshold = float(free_over.group(1))
                if current_price and current_price not in ['N/A', 'See price in cart', '不可售']:
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', current_price)
                    if price_match:
                        price_val = float(price_match.group(1).replace(',', ''))
                        if price_val >= threshold:
                            return "FREE"
                return "N/A"

            dp = tree.xpath('//span[@data-csa-c-delivery-price]/@data-csa-c-delivery-price')
            if dp:
                return dp[0].strip()

            shipping = self._get_text(tree, [
                '//div[@id="mir-layout-loaded-comparison-row-2"]//span/text()'
            ])
            return shipping if shipping else "FREE"
        except Exception:
            return "N/A"

    def _check_unavailable(self, tree) -> bool:
        try:
            texts = self._get_all_text(tree, '//div[@id="availability"]//text()')
            full = " ".join(texts).lower()
            return "currently unavailable" in full
        except Exception:
            return False

    def _check_see_price_in_cart(self, tree) -> bool:
        try:
            if tree.xpath('//a[contains(text(), "See price in cart")]'):
                return True
            if tree.xpath('//*[@id="a-popover-map_help_pop_"]'):
                return True
            texts = self._get_all_text(tree, '//table[@class="a-lineitem"]//text()')
            return any("see price in cart" in t.lower() for t in texts)
        except Exception:
            return False

    def _parse_fulfillment(self, tree, html_text: str) -> str:
        try:
            rows = tree.xpath('//div[@id="tabular-buybox"]//tr')
            for row in rows:
                label = row.xpath('.//span[contains(text(), "Ships from")]/text() | .//span[contains(text(), "Shipper")]/text()')
                if label:
                    value_nodes = row.xpath('.//span[contains(@class, "a-color-base")]/text()')
                    value = value_nodes[0] if value_nodes else ""
                    if value and "amazon" not in value.lower():
                        return "FBM"
                    if value and "amazon" in value.lower():
                        return "FBA"

            blob = " ".join(self._get_all_text(tree, '//div[@id="rightCol"]//text() | //div[@id="tabular-buybox"]//text()')).lower()
            match = re.search(r'(ships from|shipper / seller)\s*[:\s]*([a-z0-9\s]+)', blob)
            if match and "amazon" not in match.group(2).strip():
                return "FBM"
            if "fulfilled by amazon" in blob or "prime" in blob or "amazon.com" in blob:
                return "FBA"
        except Exception:
            pass
        return "FBA"

    def _parse_stock_count(self, stock_status: str, tree) -> int:
        try:
            stock_lower = stock_status.lower() if stock_status else ""
            if "only" in stock_lower:
                count = re.search(r'(\d+)', stock_status)
                return int(count.group(1)) if count else 999
            if "out of stock" in stock_lower or "unavailable" in stock_lower:
                return 0
            options = tree.xpath('//select[@name="quantity"]//option/@value')
            if not options:
                options = tree.xpath('//select[@id="quantity"]//option/@value')
            if options:
                values = [int(v) for v in options if v.strip().isdigit()]
                if values:
                    return max(values)
        except Exception:
            pass
        return 999

    def _parse_delivery(self, tree) -> Tuple[str, str]:
        try:
            texts = self._get_all_text(tree,
                '//*[@data-csa-c-delivery-time]//text() | //div[contains(@class,"delivery-message")]//text()')
            full_text = " ".join(texts)
            today = datetime.now()
            min_days = 999
            best_date_str = "N/A"

            pattern = (
                r'(Tomorrow|Today|'
                r'Jan(?:uary)?\.?\s+\d+|Feb(?:ruary)?\.?\s+\d+|Mar(?:ch)?\.?\s+\d+|'
                r'Apr(?:il)?\.?\s+\d+|May\.?\s+\d+|Jun(?:e)?\.?\s+\d+|'
                r'Jul(?:y)?\.?\s+\d+|Aug(?:ust)?\.?\s+\d+|Sep(?:tember)?\.?\s+\d+|'
                r'Oct(?:ober)?\.?\s+\d+|Nov(?:ember)?\.?\s+\d+|Dec(?:ember)?\.?\s+\d+)'
            )

            for m in re.finditer(pattern, full_text, re.IGNORECASE):
                raw = m.group(1)
                days = 999
                if "today" in raw.lower():
                    days = 0
                elif "tomorrow" in raw.lower():
                    days = 1
                else:
                    try:
                        import dateparser
                        dt = dateparser.parse(raw, settings={'PREFER_DATES_FROM': 'future'})
                        if dt:
                            if dt.month < today.month and today.month == 12:
                                dt = dt.replace(year=today.year + 1)
                            elif dt.year < today.year:
                                dt = dt.replace(year=today.year)
                            days = (dt.date() - today.date()).days
                    except Exception:
                        continue

                if 0 <= days < min_days:
                    min_days = days
                    best_date_str = raw

            if min_days < 999:
                return best_date_str, str(min_days)
        except Exception:
            pass
        return "N/A", "N/A"

    def _parse_customization(self, tree) -> str:
        try:
            if tree.xpath('//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "customize now")]'):
                return "Yes"
            texts = " ".join(self._get_all_text(tree, '//div[@id="rightCol"]//text() | //div[@id="tabular-buybox"]//text()')).lower()
            if "needs to be customized" in texts or "customization required" in texts:
                return "Yes"
        except Exception:
            pass
        return "No"

    def _parse_bullet_points(self, tree) -> str:
        try:
            bullets = self._get_all_text(tree, '//div[@id="feature-bullets"]//ul/li//span[@class="a-list-item"]//text()')
            if not bullets:
                bullets = self._get_all_text(tree, '//div[contains(@class,"a-expander-content")]//ul/li//span[@class="a-list-item"]//text()')

            clean = []
            for b in bullets:
                txt = b.strip()
                if not txt or len(txt) < 2:
                    continue
                if any(spam in txt.lower() for spam in self.BULLET_BLACKLIST):
                    continue
                clean.append(txt)
            return "\n".join(clean)
        except Exception:
            return ""

    def _parse_long_description(self, tree, html_text: str) -> str:
        try:
            nodes = tree.xpath('//div[contains(@class,"aplus")]//*[self::p or self::img]')
            if not nodes:
                nodes = tree.xpath('//*[@id="productDescription"]//*[self::p or self::img]')

            parts = []
            for node in nodes:
                src = node.xpath('./@src | ./@data-src')
                if src:
                    s = src[0]
                    if "pixel" not in s and "transparent" not in s:
                        parts.append(f"\n[Image: {s.strip()}]\n")
                else:
                    text = "".join(node.xpath('.//text()')).strip()
                    if text and len(text) > 5:
                        parts.append(text)

            if not parts:
                json_desc = re.search(r'"description"\s*:\s*"([^"]+)"', html_text)
                if json_desc:
                    try:
                        decoded = json_desc.group(1).encode('utf-8').decode('unicode_escape')
                        clean = re.sub(r'<[^>]+>', '\n', decoded)
                        return clean.strip()[:4000]
                    except Exception:
                        pass

            return "\n".join(parts)[:10000]
        except Exception:
            return ""

    def _parse_images(self, tree, html_text: str) -> str:
        try:
            img_urls = []
            scripts = self._get_all_text(tree, '//script[contains(text(), "colorImages")]/text()')
            if scripts:
                script = scripts[0]
                urls = re.findall(r'"hiRes":"(https://[^"]+)"', script) or \
                       re.findall(r'"large":"(https://[^"]+)"', script)
                img_urls = list(set(urls))
            else:
                img_urls = tree.xpath('//div[@id="imgTagWrapperId"]/img/@src')
            return "\n".join(img_urls)
        except Exception:
            return ""

    def _parse_upc(self, tree, html_text: str, page_details: Dict) -> str:
        try:
            upc_set = set(re.findall(r'"upc":"(\d+)"', html_text))
            if 'upc' in page_details:
                upc_set.add(page_details['upc'])
            upc_set.update(re.findall(r'UPC\s*[:#]?\s*(\d{12})', html_text))
            return ",".join(list(upc_set))
        except Exception:
            return ""

    def _parse_categories(self, tree) -> Tuple[str, str, str]:
        try:
            hrefs = tree.xpath('//div[@id="wayfinding-breadcrumbs_feature_div"]//a/@href')
            cids = re.findall(r'node=(\d+)', " ".join(hrefs))
            category_ids = ",".join(cids)
            root_id = cids[0] if cids else "N/A"

            names = tree.xpath('//div[@id="wayfinding-breadcrumbs_feature_div"]//li//a/text()')
            category_tree = " > ".join([n.strip() for n in names if n.strip()])

            return root_id, category_ids, category_tree
        except Exception:
            return "N/A", "", ""

    def _parse_all_details(self, tree, html_text: str) -> Dict[str, str]:
        """全页扫描：提取 Product Information 表格内容"""
        d = {}
        try:
            for row in tree.xpath('//th/../..//tr'):
                try:
                    k_nodes = row.xpath('normalize-space(./th)')
                    v_nodes = row.xpath('normalize-space(./td)')
                    if k_nodes and v_nodes:
                        self._map_detail(d, str(k_nodes), str(v_nodes))
                except Exception:
                    continue

            for s in tree.xpath('//li/span/span[contains(@class, "a-text-bold")]'):
                try:
                    k = s.xpath('normalize-space(./text())')
                    v = s.xpath('normalize-space(./following-sibling::span)')
                    if k and v:
                        self._map_detail(d, str(k).replace(':', ''), str(v))
                except Exception:
                    continue

            gl = re.search(r'"gl_product_group_type":"([^"]+)"', html_text)
            if gl:
                d['product_type'] = gl.group(1)
        except Exception as e:
            logger.warning(f"详情解析异常: {e}")

        return d

    # ==================== AOD 解析方法 ====================

    def parse_aod_response(self, html_text: str, asin: str) -> Dict[str, Any]:
        """
        解析 AOD (All Offers Display) AJAX 响应
        提取卖家价格、运费、FBA状态
        """
        result = {
            "asin": asin,
            "offers": [],
            "buybox_price": "N/A",
            "buybox_shipping": "N/A",
            "is_fba": "N/A",
        }

        if not html_text:
            return result

        if _USE_SELECTOLAX:
            return self._parse_aod_selectolax(html_text, asin, result)
        else:
            return self._parse_aod_lxml(html_text, asin, result)

    def _parse_aod_selectolax(self, html_text: str, asin: str, result: Dict) -> Dict:
        try:
            tree = SlxParser(html_text)

            offers = []
            # AOD 卖家列表
            pinned = tree.css_first('#aod-pinned-offer')
            offer_nodes = tree.css('#aod-offer')

            all_offer_nodes = []
            if pinned:
                all_offer_nodes.append(pinned)
            all_offer_nodes.extend(offer_nodes)

            for i, offer_node in enumerate(all_offer_nodes):
                offer = {"seller": "N/A", "price": "N/A", "shipping": "N/A", "is_fba": "N/A"}

                # 价格
                price_node = offer_node.css_first('span.a-offscreen')
                if price_node:
                    offer["price"] = price_node.text(strip=True)

                # 运费
                shipping_node = offer_node.css_first('#aod-offer-shippingMessage span, #mir-layout-DELIVERY_BLOCK span')
                if shipping_node:
                    ship_text = shipping_node.text(strip=True)
                    if "free" in ship_text.lower():
                        offer["shipping"] = "FREE"
                    else:
                        price_match = re.search(r'\$[\d,.]+', ship_text)
                        offer["shipping"] = price_match.group() if price_match else ship_text

                # 卖家名
                seller_node = offer_node.css_first('#aod-offer-soldBy a, #aod-offer-soldBy span.a-size-small')
                if seller_node:
                    offer["seller"] = seller_node.text(strip=True)

                # FBA 判定
                fulfilled_node = offer_node.css_first('#aod-offer-shipsFrom span.a-size-small, #aod-offer-shipsFrom span.a-color-base')
                if fulfilled_node:
                    ff_text = fulfilled_node.text(strip=True).lower()
                    offer["is_fba"] = "FBA" if "amazon" in ff_text else "FBM"

                offers.append(offer)

            result["offers"] = offers

            # BuyBox (第一个 offer 通常是 BuyBox winner)
            if offers:
                result["buybox_price"] = offers[0]["price"]
                result["buybox_shipping"] = offers[0]["shipping"]
                result["is_fba"] = offers[0]["is_fba"]

        except Exception as e:
            logger.warning(f"AOD 解析异常: {e}")

        return result

    def _parse_aod_lxml(self, html_text: str, asin: str, result: Dict) -> Dict:
        try:
            tree = lxml_html.fromstring(html_text)

            offers = []
            pinned = tree.xpath('//*[@id="aod-pinned-offer"]')
            offer_nodes = tree.xpath('//*[@id="aod-offer"]')

            all_offer_nodes = pinned + offer_nodes

            for offer_node in all_offer_nodes:
                offer = {"seller": "N/A", "price": "N/A", "shipping": "N/A", "is_fba": "N/A"}

                price = offer_node.xpath('.//span[@class="a-offscreen"]/text()')
                if price:
                    offer["price"] = price[0].strip()

                shipping = offer_node.xpath('.//*[contains(@id,"shippingMessage")]//span/text() | .//*[contains(@id,"DELIVERY_BLOCK")]//span/text()')
                if shipping:
                    ship_text = " ".join(s.strip() for s in shipping)
                    if "free" in ship_text.lower():
                        offer["shipping"] = "FREE"
                    else:
                        price_match = re.search(r'\$[\d,.]+', ship_text)
                        offer["shipping"] = price_match.group() if price_match else ship_text

                seller = offer_node.xpath('.//*[contains(@id,"soldBy")]//a/text() | .//*[contains(@id,"soldBy")]//span[@class="a-size-small"]/text()')
                if seller:
                    offer["seller"] = seller[0].strip()

                fulfilled = offer_node.xpath('.//*[contains(@id,"shipsFrom")]//span[@class="a-size-small"]/text() | .//*[contains(@id,"shipsFrom")]//span[contains(@class,"a-color-base")]/text()')
                if fulfilled:
                    ff_text = fulfilled[0].strip().lower()
                    offer["is_fba"] = "FBA" if "amazon" in ff_text else "FBM"

                offers.append(offer)

            result["offers"] = offers
            if offers:
                result["buybox_price"] = offers[0]["price"]
                result["buybox_shipping"] = offers[0]["shipping"]
                result["is_fba"] = offers[0]["is_fba"]

        except Exception as e:
            logger.warning(f"AOD 解析异常: {e}")

        return result


# 全局解析器实例
parser = AmazonParser()
