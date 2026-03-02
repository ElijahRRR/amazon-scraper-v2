"""
Amazon 产品采集系统 v2 - Scrapling 解析引擎
使用 Scrapling 的 Adaptor 类提供 Parsel 风格 .css() / .xpath() 语法
继承 AmazonParser 的共享逻辑（JSON-LD、正则、字段映射等）
"""
import re
import logging
from typing import Optional, List, Dict, Any, Tuple

from scrapling.parser import Adaptor

from parser import AmazonParser

logger = logging.getLogger(__name__)
logger.info("解析引擎: scrapling (Adaptor)")


class ScraplingParser(AmazonParser):
    """Scrapling 引擎的 Amazon 解析器"""

    def parse_product(self, html_text: str, asin: str, zip_code: str = "10001") -> Dict[str, Any]:
        result = self._default_result(asin, zip_code)

        if not html_text:
            result["title"] = "[页面为空]"
            return result

        jsonld = self._extract_jsonld(html_text)
        return self._parse_with_scrapling(html_text, asin, zip_code, result, jsonld)

    def _parse_with_scrapling(self, html_text: str, asin: str, zip_code: str,
                               result: Dict, jsonld: Dict) -> Dict:
        try:
            page = Adaptor(html_text, auto_match=False)
        except Exception as e:
            logger.error(f"HTML 解析失败: {e}")
            result["title"] = "[HTML解析失败]"
            return result

        block_status = self._check_block(html_text, None)
        if block_status:
            result["title"] = block_status
            return result

        page_details = self._scr_parse_all_details(page, html_text)

        result["title"] = jsonld.get("title") or self._scr_parse_title(page)
        result["zip_code"] = self._scr_parse_zip_code(page) or zip_code

        is_unavailable = self._scr_check_unavailable(page)
        is_see_price_in_cart = self._scr_check_see_price_in_cart(page)

        result["brand"] = jsonld.get("brand") or page_details.get("brand") or self._scr_parse_brand(page)

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
            result["original_price"] = self._scr_parse_original_price(page)
            result["buybox_shipping"] = self._scr_parse_buybox_shipping(page, None)
            result["is_fba"] = self._scr_parse_fulfillment(page, html_text)
            stock_text = page.css('div#availability span::text').get('').strip()
            result["stock_status"] = stock_text if stock_text else "In Stock"
            result["stock_count"] = str(self._scr_parse_stock_count(result["stock_status"], page))
            d_date, d_time = self._scr_parse_delivery(page)
            result["delivery_date"] = d_date
            result["delivery_time"] = d_time
        else:
            css_price = self._scr_parse_current_price(page)
            result["current_price"] = css_price if css_price != "N/A" else jsonld.get("current_price", "N/A")
            bb = self._scr_parse_buybox_price(page)
            result["buybox_price"] = bb if bb else result["current_price"]
            result["original_price"] = self._scr_parse_original_price(page)
            result["buybox_shipping"] = self._scr_parse_buybox_shipping(page, result["current_price"])
            result["is_fba"] = self._scr_parse_fulfillment(page, html_text)
            stock_text = page.css('div#availability span::text').get('').strip()
            result["stock_status"] = stock_text if stock_text else jsonld.get("stock_status", "In Stock")
            result["stock_count"] = str(self._scr_parse_stock_count(result["stock_status"], page))
            d_date, d_time = self._scr_parse_delivery(page)
            result["delivery_date"] = d_date
            result["delivery_time"] = d_time

        result["is_customized"] = self._scr_parse_customization(page)

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

        result["bullet_points"] = self._scr_parse_bullet_points(page)

        css_desc = self._scr_parse_long_description(page, html_text)
        result["long_description"] = css_desc if css_desc else jsonld.get("_jsonld_description", "")

        css_imgs = self._scr_parse_images(page, html_text)
        result["image_urls"] = css_imgs if css_imgs else jsonld.get("image_urls", "")

        result["upc_list"] = self._parse_upc_regex(html_text, page_details)
        css_ean = self._parse_ean(html_text)
        jsonld_ean = jsonld.get("ean_list", "")
        if css_ean and jsonld_ean and jsonld_ean not in css_ean:
            result["ean_list"] = f"{css_ean},{jsonld_ean}"
        else:
            result["ean_list"] = css_ean or jsonld_ean

        result["parent_asin"] = self._parse_parent_asin(html_text, asin)
        result["variation_asins"] = self._parse_variation_asins(html_text, asin, result["parent_asin"])

        result["root_category_id"], result["category_ids"], result["category_tree"] = \
            self._scr_parse_categories(page)

        return result

    # ==================== Scrapling CSS 字段解析 ====================

    def _scr_css_first_text(self, page, selectors: List[str]) -> Optional[str]:
        for sel in selectors:
            try:
                text = page.css(f'{sel}::text').get('')
                if text and text.strip():
                    return text.strip()
            except Exception:
                continue
        return None

    def _scr_parse_title(self, page) -> str:
        try:
            visible = self._scr_css_first_text(page, ['span#productTitle', 'h1 > span'])
            if visible:
                return visible
            meta = page.css('meta[name="title"]::attr(content)').get('')
            return meta.strip() if meta.strip() else "N/A"
        except Exception:
            return "N/A"

    def _scr_parse_zip_code(self, page) -> Optional[str]:
        try:
            text = page.css('span#glow-ingress-line1::text').get('')
            if text:
                match = re.search(r'(\d{5})', text)
                if match:
                    return match.group(1)
        except Exception:
            pass
        return None

    def _scr_parse_brand(self, page) -> str:
        try:
            brand_text = page.css('a#bylineInfo::text').get('')
            if brand_text:
                brand = re.sub(r'Visit the | Store|Brand: ', '', brand_text, flags=re.IGNORECASE).strip()
                return brand if brand else "N/A"
        except Exception:
            pass
        return "N/A"

    def _scr_parse_current_price(self, page) -> str:
        try:
            price_selectors = [
                '#corePrice_feature_div span.a-offscreen',
                '#corePriceDisplay_desktop_feature_div span.a-offscreen',
                '#price span.a-offscreen',
                '#priceblock_ourprice',
                '#priceblock_dealprice',
                '#priceToPay span.a-offscreen',
                '#apex_offerDisplay_desktop span.a-offscreen',
                'div.a-section span.a-price span.a-offscreen',
            ]
            for sel in price_selectors:
                text = page.css(f'{sel}::text').get('')
                if text and "$" in text:
                    return text.strip()

            for container in ['#corePrice_feature_div', '#corePriceDisplay_desktop_feature_div',
                              '#price', '#apex_offerDisplay_desktop', '']:
                prefix = f'{container} ' if container else ''
                whole = page.css(f'{prefix}span.a-price-whole::text').get('')
                frac = page.css(f'{prefix}span.a-price-fraction::text').get('')
                if whole and frac:
                    return f"${whole.strip().replace('.', '')}.{frac.strip()}"
        except Exception:
            pass
        return "N/A"

    def _scr_parse_buybox_price(self, page) -> Optional[str]:
        selectors = [
            '#tabular-buybox span.a-offscreen',
            '#buybox span.a-offscreen',
            '#newBuyBoxPrice',
            '#priceToPay span.a-offscreen',
            '#price_inside_buybox',
            '#corePrice_feature_div span.a-offscreen',
            '#corePriceDisplay_desktop_feature_div span.a-offscreen',
            '#apex_offerDisplay_desktop span.a-offscreen',
        ]
        for sel in selectors:
            try:
                texts = page.css(f'{sel}::text').getall()
                for text in texts:
                    if text and re.search(r'\d', text):
                        return text.strip()
            except Exception:
                continue
        return None

    def _scr_parse_original_price(self, page) -> str:
        try:
            text = page.css('span[data-a-strike="true"] span.a-offscreen::text').get('')
            if text:
                return text.strip()
        except Exception:
            pass
        return "N/A"

    def _scr_parse_buybox_shipping(self, page, current_price: Optional[str]) -> str:
        try:
            delivery_texts = page.css('div#deliveryBlockMessage ::text').getall()
            delivery_block = " ".join(t.strip() for t in delivery_texts if t.strip())

            if "prime members get free delivery" in delivery_block.lower():
                return "FREE"

            free_over = re.search(r'free delivery.*?on orders over \$(\d+(?:\.\d+)?)',
                                  delivery_block, re.IGNORECASE)
            if free_over:
                threshold = float(free_over.group(1))
                if current_price and current_price not in ['N/A', 'See price in cart', '不可售']:
                    price_match = re.search(r'\$?([\d,]+\.?\d*)', current_price)
                    if price_match:
                        price_val = float(price_match.group(1).replace(',', ''))
                        if price_val >= threshold:
                            return "FREE"
                return "N/A"

            dp = page.css('span[data-csa-c-delivery-price]::attr(data-csa-c-delivery-price)').get('')
            if dp:
                return dp.strip()

            shipping = page.css('div#mir-layout-loaded-comparison-row-2 span::text').get('')
            return shipping.strip() if shipping.strip() else "FREE"
        except Exception:
            return "N/A"

    def _scr_check_unavailable(self, page) -> bool:
        try:
            texts = page.css('div#availability ::text').getall()
            full = " ".join(t.strip() for t in texts).lower()
            return "currently unavailable" in full
        except Exception:
            return False

    def _scr_check_see_price_in_cart(self, page) -> bool:
        try:
            for text in page.css('a::text').getall():
                if text and "see price in cart" in text.lower():
                    return True
            if page.css('#a-popover-map_help_pop_'):
                return True
            for text in page.css('table.a-lineitem ::text').getall():
                if text and "see price in cart" in text.lower():
                    return True
        except Exception:
            pass
        return False

    def _scr_parse_fulfillment(self, page, html_text: str) -> str:
        try:
            for row in page.css('div#tabular-buybox tr'):
                row_text = " ".join(row.css('::text').getall()).lower()
                if "ships from" in row_text or "shipper" in row_text:
                    values = row.css('span.a-color-base::text').getall()
                    for v in values:
                        v = v.strip()
                        if v:
                            return "FBM" if "amazon" not in v.lower() else "FBA"

            blob_parts = []
            for sel in ['div#rightCol ::text', 'div#tabular-buybox ::text']:
                blob_parts.extend(page.css(sel).getall())
            blob = " ".join(t.strip() for t in blob_parts).lower()

            match = re.search(r'(ships from|shipper / seller)\s*[:\s]*([a-z0-9\s]+)', blob)
            if match and "amazon" not in match.group(2).strip():
                return "FBM"
            if "fulfilled by amazon" in blob or "prime" in blob or "amazon.com" in blob:
                return "FBA"
        except Exception:
            pass
        return "FBA"

    def _scr_parse_stock_count(self, stock_status: str, page) -> int:
        try:
            stock_lower = stock_status.lower() if stock_status else ""
            if "only" in stock_lower:
                count = re.search(r'(\d+)', stock_status)
                return int(count.group(1)) if count else 999
            if "out of stock" in stock_lower or "unavailable" in stock_lower:
                return 0
            options = page.css('select[name="quantity"] option::attr(value)').getall()
            if not options:
                options = page.css('select#quantity option::attr(value)').getall()
            if options:
                values = [int(v) for v in options if v.strip().isdigit()]
                if values:
                    return max(values)
        except Exception:
            pass
        return 999

    def _scr_parse_delivery(self, page) -> Tuple[str, str]:
        try:
            texts = page.css('[data-csa-c-delivery-time] ::text, div.delivery-message ::text').getall()
            full_text = " ".join(t.strip() for t in texts)
            from datetime import datetime
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

    def _scr_parse_customization(self, page) -> str:
        try:
            all_text = " ".join(page.css('::text').getall()).lower()
            if "customize now" in all_text:
                return "Yes"
            right_text = " ".join(page.css('div#rightCol ::text, div#tabular-buybox ::text').getall()).lower()
            if "needs to be customized" in right_text or "customization required" in right_text:
                return "Yes"
        except Exception:
            pass
        return "No"

    def _scr_parse_bullet_points(self, page) -> str:
        try:
            bullets = page.css('div#feature-bullets ul > li span.a-list-item::text').getall()
            if not bullets:
                bullets = page.css('div.a-expander-content ul > li span.a-list-item::text').getall()

            clean = []
            for b in bullets:
                txt = b.strip()
                if len(txt) < 2:
                    continue
                if any(spam in txt.lower() for spam in self.BULLET_BLACKLIST):
                    continue
                clean.append(txt)
            return "\n".join(clean)
        except Exception:
            return ""

    def _scr_parse_long_description(self, page, html_text: str) -> str:
        try:
            parts = []
            nodes = page.css('div.aplus p, div.aplus img')
            if not nodes:
                nodes = page.css('#productDescription p, #productDescription img')

            for node in nodes:
                src = node.css('::attr(src)').get('') or node.css('::attr(data-src)').get('')
                tag = node.xpath('name()').get('')
                if tag == 'img' and src and "pixel" not in src and "transparent" not in src:
                    parts.append(f"\n[Image: {src.strip()}]\n")
                else:
                    text = " ".join(node.css('::text').getall()).strip()
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

    def _scr_parse_images(self, page, html_text: str) -> str:
        try:
            img_urls = []
            for script_text in page.css('script::text').getall():
                if script_text and "colorImages" in script_text:
                    urls = re.findall(r'"hiRes":"(https://[^"]+)"', script_text) or \
                           re.findall(r'"large":"(https://[^"]+)"', script_text)
                    img_urls = list(set(urls))
                    break

            if not img_urls:
                src = page.css('div#imgTagWrapperId img::attr(src)').get('')
                if src:
                    img_urls = [src]

            return "\n".join(img_urls)
        except Exception:
            return ""

    def _scr_parse_categories(self, page) -> Tuple[str, str, str]:
        try:
            cids = []
            names = []
            for a in page.css('div#wayfinding-breadcrumbs_feature_div a'):
                href = a.css('::attr(href)').get('')
                node_match = re.search(r'node=(\d+)', href)
                if node_match:
                    cids.append(node_match.group(1))
                name = a.css('::text').get('').strip()
                if name:
                    names.append(name)

            category_ids = ",".join(cids)
            root_id = cids[0] if cids else "N/A"
            category_tree = " > ".join(names)
            return root_id, category_ids, category_tree
        except Exception:
            return "N/A", "", ""

    def _scr_parse_all_details(self, page, html_text: str) -> Dict[str, str]:
        d = {}
        try:
            for row in page.css('tr'):
                try:
                    k = row.css('th::text').get('')
                    v = row.css('td::text').get('')
                    if k and v:
                        self._map_detail(d, k.strip(), v.strip())
                except Exception:
                    continue

            for bold_span in page.css('li > span > span.a-text-bold'):
                try:
                    k = bold_span.css('::text').get('')
                    parent = bold_span.xpath('..')
                    if parent:
                        spans = parent.css('span::text').getall()
                        for i, text in enumerate(spans):
                            if text.strip() == k.strip() and i + 1 < len(spans):
                                v = spans[i + 1].strip()
                                if k and v:
                                    self._map_detail(d, k.strip().replace(':', ''), v)
                                break
                except Exception:
                    continue

            gl = re.search(r'"gl_product_group_type":"([^"]+)"', html_text)
            if gl:
                d['product_type'] = gl.group(1)
        except Exception as e:
            logger.warning(f"详情解析异常: {e}")

        return d

    def _parse_upc_regex(self, html_text: str, page_details: Dict) -> str:
        try:
            upc_set = set(re.findall(r'"upc":"(\d+)"', html_text))
            if 'upc' in page_details:
                upc_set.add(page_details['upc'])
            upc_set.update(re.findall(r'UPC\s*[:#]?\s*(\d{12})', html_text))
            return ",".join(list(upc_set))
        except Exception:
            return ""

    # ==================== AOD 解析 ====================

    def parse_aod_response(self, html_text: str, asin: str) -> Dict[str, Any]:
        result = {
            "asin": asin,
            "offers": [],
            "buybox_price": "N/A",
            "buybox_shipping": "N/A",
            "is_fba": "N/A",
        }

        if not html_text:
            return result

        try:
            page = Adaptor(html_text, auto_match=False)

            offers = []
            pinned = page.css('#aod-pinned-offer')
            offer_nodes = page.css('#aod-offer')

            all_offer_nodes = list(pinned) + list(offer_nodes)

            for offer_node in all_offer_nodes:
                offer = {"seller": "N/A", "price": "N/A", "shipping": "N/A", "is_fba": "N/A"}

                price_text = offer_node.css('span.a-offscreen::text').get('')
                if price_text:
                    offer["price"] = price_text.strip()

                ship_text = offer_node.css(
                    '#aod-offer-shippingMessage span::text, '
                    '#mir-layout-DELIVERY_BLOCK span::text'
                ).get('')
                if ship_text:
                    if "free" in ship_text.lower():
                        offer["shipping"] = "FREE"
                    else:
                        price_match = re.search(r'\$[\d,.]+', ship_text)
                        offer["shipping"] = price_match.group() if price_match else ship_text.strip()

                seller_text = offer_node.css(
                    '#aod-offer-soldBy a::text, '
                    '#aod-offer-soldBy span.a-size-small::text'
                ).get('')
                if seller_text:
                    offer["seller"] = seller_text.strip()

                ff_text = offer_node.css(
                    '#aod-offer-shipsFrom span.a-size-small::text, '
                    '#aod-offer-shipsFrom span.a-color-base::text'
                ).get('')
                if ff_text:
                    offer["is_fba"] = "FBA" if "amazon" in ff_text.lower() else "FBM"

                offers.append(offer)

            result["offers"] = offers
            if offers:
                result["buybox_price"] = offers[0]["price"]
                result["buybox_shipping"] = offers[0]["shipping"]
                result["is_fba"] = offers[0]["is_fba"]

        except Exception as e:
            logger.warning(f"AOD 解析异常: {e}")

        return result
