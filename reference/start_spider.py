import sys
import redis
import pandas as pd
import os
import time
import subprocess
from scrapy.cmdline import execute

# --- Redis 配置 ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_KEY = 'amazon:requests'

# --- [新增] 指定保存目录 ---
SAVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "产品采集数据")


def _ensure_redis_running():
    """检测 Redis 是否运行，未运行则尝试自动启动 (支持 macOS 和 Windows)"""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()
        return True  # Redis 已运行
    except redis.ConnectionError:
        pass
    
    # Redis 未运行，尝试自动启动
    print("⏳ Redis 未运行，正在尝试自动启动...")
    
    if sys.platform == "darwin":
        # macOS: 使用 brew services
        try:
            result = subprocess.run(
                ["brew", "services", "start", "redis"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                for i in range(5):
                    time.sleep(1)
                    try:
                        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
                        r.ping()
                        print("✅ Redis 已自动启动")
                        return True
                    except redis.ConnectionError:
                        continue
            print("❌ Redis 自动启动失败，请手动运行: brew services start redis")
            return False
        except FileNotFoundError:
            print("❌ 未找到 brew 命令，请先安装 Homebrew 并安装 Redis")
            print("💡 安装命令: brew install redis")
            return False
        except Exception as e:
            print(f"❌ 启动 Redis 时出错: {e}")
            return False
    
    elif sys.platform == "win32":
        # Windows: 尝试启动 Redis 服务或直接运行 redis-server
        try:
            # 方法1: 尝试启动 Windows 服务
            result = subprocess.run(
                ["net", "start", "redis"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0 or "已经启动" in result.stdout or "already been started" in result.stdout.lower():
                time.sleep(2)
                try:
                    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
                    r.ping()
                    print("✅ Redis 服务已启动")
                    return True
                except redis.ConnectionError:
                    pass
        except Exception:
            pass
        
        # 方法2: 尝试直接运行 redis-server.exe
        try:
            # 常见的 Redis 安装路径
            redis_paths = [
                r"C:\Program Files\Redis\redis-server.exe",
                r"C:\Redis\redis-server.exe",
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "Redis-x64-3.0.504", "redis-server.exe"),
            ]
            for redis_path in redis_paths:
                if os.path.exists(redis_path):
                    subprocess.Popen([redis_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    for i in range(5):
                        time.sleep(1)
                        try:
                            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
                            r.ping()
                            print("✅ Redis 已自动启动")
                            return True
                        except redis.ConnectionError:
                            continue
        except Exception as e:
            print(f"❌ 启动 Redis 时出错: {e}")
        
        print("❌ Redis 自动启动失败")
        print("💡 请手动启动 Redis，或下载安装: https://github.com/tporadowski/redis/releases")
        return False
    
    else:
        print("❌ Redis 未运行，请手动启动 Redis")
        return False


def _prompt_file_path_fallback():
    try:
        raw = input("请输入包含 ASIN 的文件路径: ").strip()
        # 兼容用户带引号的输入
        if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
            raw = raw[1:-1]
        # macOS 拖拽文件时，空格会被转义为 '\ '，需还原
        raw = raw.replace('\\ ', ' ')
        return raw
    except EOFError:
        return ""


def _macos_file_dialog_fallback():
    if sys.platform != "darwin":
        return ""

    # 使用 osascript 弹出 macOS 原生文件选择窗口
    # choose file 是 Standard Additions 的命令，不需要 tell application
    script = 'POSIX path of (choose file with prompt "请选择包含 ASIN 的文件")'
    try:
        import subprocess

        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def get_user_selected_file():
    if sys.platform == "darwin":
        file_path = _macos_file_dialog_fallback()
        if file_path:
            return file_path
        print("⚠️ macOS 文件选择窗口未能打开或被取消。")
        print("💡 如未弹窗，请在 系统设置 > 隐私与安全性 > 自动化 中允许 终端/VS Code 控制 System Events。")
        return _prompt_file_path_fallback()

    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        print("⏳ 正在打开文件选择窗口...")
        file_path = filedialog.askopenfilename(
            title="请选择包含 ASIN 的文件",
            filetypes=[
                ("数据文件", "*.xlsx *.xls *.txt *.csv"),  # 新增 CSV 支持
                ("Excel 文件", "*.xlsx *.xls"),
                ("CSV 文件", "*.csv"),
                ("文本文件", "*.txt"),
                ("所有文件", "*.*")
            ]
        )
        root.destroy()
        return file_path
    except Exception as e:
        print(f"⚠️ 无法打开文件选择窗口 ({type(e).__name__})，改用命令行输入路径。")
        return _prompt_file_path_fallback()


def load_asins_to_redis(filename):
    if not filename:
        return False

    filename = os.path.expanduser(filename)
    if not os.path.exists(filename):
        print(f"❌ 文件不存在: {filename}")
        return False
    
    asins = []
    print(f"📖 读取文件: {filename}")

    try:
        if filename.endswith(('.xlsx', '.xls')):
            print(f"⏳ 正在读取 Excel (可能包含多个 Sheet，请稍候)...")
            # sheet_name=None 读取所有工作表，返回字典 {sheet_name: df}
            dfs = pd.read_excel(filename, sheet_name=None, dtype=str)
            asins = []
            
            print(f"📊 Excel 文件包含 {len(dfs)} 个工作表:")
            for sheet_name, df in dfs.items():
                if df.empty:
                    print(f"   ⚪ Sheet '{sheet_name}': 空表 (跳过)")
                    continue
                
                # 智能查找 ASIN 列
                col_name = next((col for col in df.columns if 'asin' in col.lower()), None)
                if not col_name:
                    # 没找到带 'asin' 的列，默认使用第一列
                    col_name = df.columns[0]
                
                sheet_asins = df[col_name].dropna().astype(str).str.strip().tolist()
                count = len(sheet_asins)
                print(f"   🟢 Sheet '{sheet_name}': 找到 {count} 行数据")
                asins.extend(sheet_asins)
                
        elif filename.endswith('.csv'):
            # 新增 CSV 文件支持
            df = pd.read_csv(filename, dtype=str)
            col_name = next((col for col in df.columns if 'asin' in col.lower()), df.columns[0])
            asins = df[col_name].dropna().astype(str).str.strip().tolist()
            print(f"   🟢 CSV 文件: 找到 {len(asins)} 行数据")
        elif filename.endswith('.txt'):
            with open(filename, 'r', encoding='utf-8') as f:
                asins = [line.strip() for line in f if line.strip()]
            print(f"   🟢 TXT 文件: 找到 {len(asins)} 行数据")
    except Exception as e:
        print(f"❌ 读取失败: {e}")
        return False

    if not asins:
        print("⚠️ 未找到有效 ASIN")
        return False

    # 过滤有效 ASIN (长度大于 5)
    valid_asins = [asin for asin in asins if len(asin) > 5]
    
    if not valid_asins:
        print("⚠️ 未找到有效 ASIN (长度需大于5)")
        return False

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
    except redis.ConnectionError:
        print("❌ Redis 连接失败")
        if sys.platform == "darwin":
            print("💡 macOS 可用: brew install redis && brew services start redis")
        return False

    # 清空旧数据
    r.delete(REDIS_KEY)
    r.delete('amazon_dist:dupefilter')

    print(f"🚀 准备注入 {len(valid_asins)} 个任务到 Redis (分批进行)...")

    # 分批注入，防止 Redis 管道缓冲区溢出 (50万数据一次性注入会卡死)
    chunk_size = 2000
    total = len(valid_asins)
    
    for i in range(0, total, chunk_size):
        chunk = valid_asins[i : i + chunk_size]
        pipe = r.pipeline()
        for asin in chunk:
            pipe.lpush(REDIS_KEY, f"https://www.amazon.com/dp/{asin}")
        pipe.execute()
        
        # 打印进度
        current_count = min(i + chunk_size, total)
        if current_count % 10000 == 0 or current_count == total:
            print(f"   👉 进度: {current_count} / {total}")

    print(f"✅ 批量注入完成")
    return True


def main():
    print("=" * 50)
    print("🤖 亚马逊采集启动器 v3.3 (高性能版)")
    print("=" * 50)

    # 0. 检测并启动 Redis
    if not _ensure_redis_running():
        return

    # 1. 确保保存目录存在
    if not os.path.exists(SAVE_DIR):
        print(f"📂 正在创建保存目录: {SAVE_DIR}")
        os.makedirs(SAVE_DIR)

    # 2. 选择文件
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        print(f"📂 [自动运行] 使用命令行指定文件: {input_file}")
    else:
        input_file = get_user_selected_file()
    
    if not input_file:
        return

    # 3. 注入并启动
    if load_asins_to_redis(input_file):
        print("-" * 50)
        print("⚡ 爬虫正在启动...")
        print(f"💾 数据将保存至: {SAVE_DIR}")
        print("-" * 50)
        time.sleep(1)
        execute(["scrapy", "crawl", "amazon_dist", "-s", "LOG_LEVEL=INFO"])


if __name__ == "__main__":
    main()
