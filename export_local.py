#!/usr/bin/env python3
"""
Amazon Scraper v2 — 本地 Excel/CSV 生成工具
从服务器下载的 .db 文件生成 Excel 或 CSV 导出文件。
完全独立运行，无需项目其他文件。

用法:
  双击运行    → 弹出文件选择器，选择 .db 文件，自动生成 Excel
  命令行      → python export_local.py data.db [-f csv] [-o output.xlsx]
"""

import sqlite3
import csv
import os
import sys
import argparse

# ── 首次运行自动安装 openpyxl ──
try:
    import openpyxl
except ImportError:
    import subprocess
    print("首次运行，正在安装依赖 openpyxl（使用清华镜像加速）...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "-q",
        "-i", "https://pypi.tuna.tsinghua.edu.cn/simple/",
        "openpyxl"
    ])
    import openpyxl

# ── 内嵌配置（与 config.py / models.py 保持一致） ──

RESULT_FIELDS = [
    "asin", "crawl_time", "site", "zip_code", "product_url",
    "title", "brand", "product_type", "manufacturer", "model_number",
    "part_number", "country_of_origin", "is_customized", "best_sellers_rank",
    "original_price", "current_price", "buybox_price", "buybox_shipping",
    "is_fba", "stock_count", "stock_status", "delivery_date", "delivery_time",
    "image_urls", "bullet_points", "long_description",
    "upc_list", "ean_list", "parent_asin", "variation_asins",
    "root_category_id", "category_ids", "category_tree",
    "first_available_date", "package_dimensions", "package_weight",
    "item_dimensions", "item_weight",
]

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


# ── 导出逻辑（与 server.py 保持一致） ──

def _parse_price(s: str):
    """解析 '$12.99' 格式的价格字符串为浮点数，失败返回 None。"""
    if not s or s == "N/A":
        return None
    s = s.strip().replace(",", "")
    if s.startswith("$"):
        s = s[1:]
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _get_export_headers():
    """获取导出表头"""
    field_keys = list(RESULT_FIELDS)
    headers = [HEADER_MAP.get(f, f) for f in field_keys]

    shipping_header = "BuyBox 运费"
    if shipping_header in headers:
        insert_idx = headers.index(shipping_header) + 1
        headers.insert(insert_idx, "总价")

    return headers, field_keys


def _prepare_single_row(row_data: dict, field_keys: list, headers: list):
    """处理单行数据，返回导出用的列表"""
    row = [str(row_data.get(f, "")) for f in field_keys]

    shipping_header = "BuyBox 运费"
    if shipping_header in headers:
        insert_idx = headers.index(shipping_header) + 1
        price = _parse_price(str(row_data.get("buybox_price", "")))
        shipping_str = str(row_data.get("buybox_shipping", ""))
        if price is None:
            total = "N/A"
        elif shipping_str.upper() == "FREE":
            total = f"${price:.2f}"
        else:
            shipping = _parse_price(shipping_str)
            total = f"${price:.2f}" if shipping is None else f"${price + shipping:.2f}"
        row.insert(insert_idx, total)

    return row


def get_row_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT COUNT(*) FROM results")
    count = cur.fetchone()[0]
    conn.close()
    return count


def export_excel(db_path: str, output_path: str):
    """从 .db 文件生成 Excel"""
    total = get_row_count(db_path)
    if total == 0:
        print("  数据库中无数据")
        return

    headers, field_keys = _get_export_headers()
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(title="采集结果")
    ws.append(headers)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM results ORDER BY id ASC")

    for i, row in enumerate(cur, 1):
        row_data = dict(row)
        ws.append(_prepare_single_row(row_data, field_keys, headers))
        if i % 1000 == 0:
            print(f"\r  处理中: {i}/{total} ({i * 100 // total}%)", end="", flush=True)

    print(f"\r  处理中: {total}/{total} (100%)  ", flush=True)
    print("  正在保存 Excel 文件...")
    wb.save(output_path)
    wb.close()
    conn.close()
    print(f"  完成: {output_path} ({total} 条数据)")


def export_csv(db_path: str, output_path: str):
    """从 .db 文件生成 CSV"""
    total = get_row_count(db_path)
    if total == 0:
        print("  数据库中无数据")
        return

    headers, field_keys = _get_export_headers()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM results ORDER BY id ASC")

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i, row in enumerate(cur, 1):
            row_data = dict(row)
            writer.writerow(_prepare_single_row(row_data, field_keys, headers))
            if i % 1000 == 0:
                print(f"\r  处理中: {i}/{total} ({i * 100 // total}%)", end="", flush=True)

    conn.close()
    print(f"\r  完成: {output_path} ({total} 条数据)          ")


def select_file_gui() -> str:
    """弹出文件选择对话框，返回选择的文件路径"""
    import tkinter as tk
    from tkinter import filedialog
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="选择数据库文件",
        filetypes=[("SQLite 数据库", "*.db"), ("所有文件", "*.*")],
    )
    root.destroy()
    return file_path


def main():
    # 无命令行参数 → GUI 模式
    if len(sys.argv) == 1:
        print("Amazon Scraper v2 — 本地导出工具")
        print("=" * 40)
        db_path = select_file_gui()
        if not db_path:
            print("未选择文件，已退出。")
            input("按回车键关闭...")
            return

        base = os.path.splitext(db_path)[0]
        output_path = base + ".xlsx"
        fmt = "excel"

        print(f"  输入: {db_path}")
        print(f"  输出: {output_path}")
        print(f"  格式: Excel")
        print()

        export_excel(db_path, output_path)
        input("\n按回车键关闭...")
        return

    # 命令行模式
    parser = argparse.ArgumentParser(description="从 .db 文件生成 Excel/CSV 导出文件")
    parser.add_argument("db_file", help="SQLite 数据库文件路径")
    parser.add_argument("-f", "--format", choices=["excel", "csv"], default="excel",
                        help="输出格式 (默认: excel)")
    parser.add_argument("-o", "--output", help="输出文件路径 (默认: 与 .db 同目录同名)")
    args = parser.parse_args()

    if not os.path.isfile(args.db_file):
        print(f"错误: 文件不存在 — {args.db_file}")
        sys.exit(1)

    ext = ".csv" if args.format == "csv" else ".xlsx"
    output_path = args.output or (os.path.splitext(args.db_file)[0] + ext)

    print(f"Amazon Scraper v2 — 本地导出工具")
    print(f"  输入: {args.db_file}")
    print(f"  输出: {output_path}")
    print(f"  格式: {args.format.upper()}")
    print()

    if args.format == "csv":
        export_csv(args.db_file, output_path)
    else:
        export_excel(args.db_file, output_path)


if __name__ == "__main__":
    main()
