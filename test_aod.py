#!/usr/bin/env python3
"""
AOD (All Offers Display) 快速路径测试脚本 v2
分步测试，找出 AOD 失败的确切原因
"""
import asyncio
import time
import sys
import re
import json
import random

sys.path.insert(0, '.')
import config
from proxy import ProxyManager
from session import AmazonSession
from parser import AmazonParser

# 用一个确定存在 BuyBox 的 ASIN
TEST_ASIN = "B09V3KXJPB"  # Echo Dot 5th Gen


async def test_aod():
    print("=" * 60)
    print("AOD 端点诊断测试")
    print("=" * 60)

    # 从服务器拉取配置
    import httpx
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get("http://localhost:8899/api/settings")
            s = resp.json()
            if s.get("proxy_api_url"):
                config.PROXY_API_URL_AUTH = s["proxy_api_url"]
            if s.get("request_timeout"):
                config.REQUEST_TIMEOUT = s["request_timeout"]
            print(f"✓ 服务器配置已同步")
    except Exception as e:
        print(f"⚠ 服务器配置拉取失败: {e}")

    # 初始化代理
    proxy_manager = ProxyManager()
    proxy_manager.switch_mode("tunnel")
    count = await proxy_manager.init_tunnel()
    proxy_url = proxy_manager.get_tunnel_proxy_url()

    if not proxy_url:
        print("⚠ 隧道代理初始化失败，使用直连（无代理）测试")
        proxy_url = None

    # 创建 session
    session = AmazonSession(proxy_manager, zip_code="10001", proxy_url=proxy_url)
    print(f"\n--- 初始化 Session ---")
    ok = await session.initialize()
    print(f"Session: {'✓ 就绪' if ok else '✗ 失败'}")
    if not ok:
        return

    parser = AmazonParser()
    asin = TEST_ASIN

    # ============================================================
    # 测试 1: 直接请求 AOD（当前方式）
    # ============================================================
    print(f"\n{'='*60}")
    print(f"测试 1: 直接请求 AOD（不先访问产品页）")
    print(f"{'='*60}")

    aod_url = f"https://www.amazon.com/gp/aod/ajax?asin={asin}&pc=dp&isonlyrenderofferlist=true"
    referer = f"https://www.amazon.com/dp/{asin}"
    headers = session._build_headers(referer=referer)
    headers.update({
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Accept": "text/html,*/*",
    })

    t1 = time.time()
    try:
        resp = await session._session.get(aod_url, headers=headers)
        print(f"  状态: {resp.status_code} | 大小: {len(resp.content):,} bytes | 耗时: {time.time()-t1:.2f}s")
        if resp.status_code != 200:
            print(f"  内容: {resp.text[:300]}")
    except Exception as e:
        print(f"  ✗ 异常: {e}")
        resp = None

    # ============================================================
    # 测试 2: 先访问产品页，再请求 AOD
    # ============================================================
    print(f"\n{'='*60}")
    print(f"测试 2: 先访问产品页 → 再请求 AOD")
    print(f"{'='*60}")

    # 2a: 访问完整产品页
    dp_url = f"https://www.amazon.com/dp/{asin}"
    dp_headers = session._build_headers(referer="https://www.amazon.com/")
    t2a = time.time()
    try:
        dp_resp = await session._session.get(dp_url, headers=dp_headers)
        print(f"  产品页: {dp_resp.status_code} | {len(dp_resp.content):,} bytes | {time.time()-t2a:.2f}s")
        if dp_resp.status_code == 200:
            title_match = re.search(r'<title[^>]*>([^<]+)', dp_resp.text)
            print(f"  标题: {title_match.group(1)[:60] if title_match else 'N/A'}")
    except Exception as e:
        print(f"  ✗ 产品页异常: {e}")
        dp_resp = None

    await asyncio.sleep(1)

    # 2b: 用产品页设置好的 cookies 请求 AOD
    aod_headers = session._build_headers(referer=dp_url)
    aod_headers.update({
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Accept": "text/html,*/*",
    })

    t2b = time.time()
    try:
        aod_resp2 = await session._session.get(aod_url, headers=aod_headers)
        print(f"  AOD: {aod_resp2.status_code} | {len(aod_resp2.content):,} bytes | {time.time()-t2b:.2f}s")

        if aod_resp2.status_code == 200:
            aod_data = parser.parse_aod_response(aod_resp2.text, asin)
            print(f"  Offers: {len(aod_data.get('offers', []))} | BuyBox: {aod_data.get('buybox_price','N/A')}")
            # 检查关键元素
            text = aod_resp2.text
            for name, pat in [('pinned-offer', 'aod-pinned-offer'), ('aod-offer', 'id="aod-offer"'),
                              ('a-offscreen', 'a-offscreen'), ('price-whole', 'a-price-whole')]:
                c = text.count(pat)
                print(f"    [{name}]: {'✓' if c>0 else '✗'} ({c}x)")
            # 价格
            prices = re.findall(r'\$[\d,.]+', text[:100000])
            print(f"    发现价格: {prices[:10] if prices else '无'}")
            if not aod_data.get('offers'):
                print(f"\n  响应前 2000 字:\n{text[:2000]}")
        else:
            print(f"  内容: {aod_resp2.text[:500]}")
    except Exception as e:
        print(f"  ✗ AOD 异常: {e}")

    # ============================================================
    # 测试 3: 尝试不同的 AOD URL 格式
    # ============================================================
    print(f"\n{'='*60}")
    print(f"测试 3: 尝试不同的 AOD URL 格式")
    print(f"{'='*60}")

    aod_urls = [
        (f"https://www.amazon.com/gp/aod/ajax?asin={asin}&pc=dp", "标准 AOD"),
        (f"https://www.amazon.com/gp/aod/ajax/ref=aod_f_new?asin={asin}&pc=dp&m=&qid=&smid=&sr=", "带 ref 参数"),
        (f"https://www.amazon.com/gp/product/ajax/ref=dp_aod_ALL_mbc?asin={asin}&pc=dp", "旧版 all-offers"),
        (f"https://www.amazon.com/gp/aod/ajax?asin={asin}&pc=dp&isonlyrenderofferlist=true&pageno=1", "带翻页"),
    ]

    for url, desc in aod_urls:
        h = session._build_headers(referer=dp_url)
        h.update({
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Accept": "text/html,*/*",
        })
        t = time.time()
        try:
            r = await session._session.get(url, headers=h)
            status_icon = "✓" if r.status_code == 200 else "✗"
            size = len(r.content)
            has_price = "$" in r.text[:50000] if r.status_code == 200 else False
            has_offer = "aod-offer" in r.text if r.status_code == 200 else False
            print(f"  {status_icon} [{desc}] HTTP {r.status_code} | {size:,}B | {time.time()-t:.2f}s | 价格:{has_price} 报价:{has_offer}")
        except Exception as e:
            print(f"  ✗ [{desc}] 异常: {e}")
        await asyncio.sleep(0.5)

    # ============================================================
    # 测试 4: 尝试 /dp/ 轻量级 Offers 区域
    # ============================================================
    print(f"\n{'='*60}")
    print(f"测试 4: 产品页 Offers 数据提取（Offer Listing）")
    print(f"{'='*60}")

    # 尝试 offers-listing 端点
    offers_url = f"https://www.amazon.com/gp/product/ajax/ref=dp_aod_NEW_mbc?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&experienceId=aodAjaxMain"
    h = session._build_headers(referer=dp_url)
    h.update({
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Accept": "text/html,*/*",
    })
    t = time.time()
    try:
        r = await session._session.get(offers_url, headers=h)
        size = len(r.content)
        has_price = bool(re.findall(r'\$[\d,.]+', r.text[:50000])) if r.status_code == 200 else False
        print(f"  HTTP {r.status_code} | {size:,}B | {time.time()-t:.2f}s | 含价格: {has_price}")
        if r.status_code == 200 and size > 100:
            prices = re.findall(r'\$[\d,.]+', r.text[:50000])
            print(f"  价格: {prices[:10] if prices else '无'}")
            # 检查关键元素
            for name, pat in [('pinned-offer', 'aod-pinned-offer'), ('aod-offer', 'id="aod-offer"'),
                              ('a-offscreen', 'a-offscreen'), ('offer-price', 'offer-price')]:
                c = r.text.count(pat)
                if c > 0:
                    print(f"    [{name}]: ✓ ({c}x)")
    except Exception as e:
        print(f"  ✗ 异常: {e}")

    await session.close()
    print(f"\n{'='*60}")
    print("测试完成")


if __name__ == "__main__":
    asyncio.run(test_aod())
