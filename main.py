import os
import csv
import time
import requests
from aligo import Aligo
import shutil
import sys

# ======================= 配置区 =========================
GITHUB_WORKSPACE = "/tmp"
PDF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}
ALIYUN_REFRESH_TOKEN = os.getenv("ALIYUN_REFRESH_TOKEN")
ALIYUN_FOLDER_ID = os.getenv("ALIYUN_FOLDER_ID")
TARGET_YEAR = os.getenv("TARGET_YEAR")

if not TARGET_YEAR:
    print("❌ 未指定 TARGET_YEAR 环境变量，退出。")
    sys.exit(1)
TARGET_YEAR = int(TARGET_YEAR)
print(f"📆 本次任务目标年份：{TARGET_YEAR}")

# ======================= 股票列表读取 ======================
def load_stocks_from_csv(filename="stock_list.csv"):
    stocks = []
    with open(filename, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row["code"].strip()
            name = row["name"].strip()
            if "ST" not in name and "*ST" not in name:
                stocks.append((code, name))
    return stocks

# ================== 东方财富年报链接获取 ===================
def get_annual_report_pdf_eastmoney(code, name, year):
    """
    通过东方财富公告接口获取年报 PDF 直链。
    返回 (url, title) 或 None
    """
    url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://data.eastmoney.com/",
    }
    params = {
        "sr": -1,
        "page_size": 10,
        "page_index": 1,
        "ann_type": "A",
        "client_source": "web",
        "stock_list": code,
        "f_node": "0",
        "s_node": "0",
        "begin_time": f"{year}-01-01",
        "end_time": f"{year}-12-31",
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        data = resp.json()
        if not data.get("data") or not data["data"].get("list"):
            print(f"    📭 {year} 年无公告数据")
            return None

        for item in data["data"]["list"]:
            title = item.get("title", "")
            if "年度报告" not in title:
                continue
            if any(kw in title for kw in ["摘要", "英文", "修订", "已取消", "更正"]):
                continue
            art_code = item.get("art_code", "")
            if not art_code:
                continue
            pdf_url = f"https://np-anotice-stock.eastmoney.com/api/security/ann/file?type=pdf&art_code={art_code}"
            print(f"    🔗 找到年报：{title}")
            return pdf_url
        print(f"    ℹ️ 未找到符合条件的年报")
    except Exception as e:
        print(f"    ⚠️ 请求异常：{e}")
    return None

# ======================= PDF 下载 ==========================
def download_pdf(pdf_url, save_path):
    try:
        r = requests.get(pdf_url, headers=PDF_HEADERS, timeout=60)
        if r.status_code == 200 and len(r.content) > 10 * 1024:   # 最少10KB
            with open(save_path, "wb") as f:
                f.write(r.content)
            return True
        else:
            print(f"    ❌ 下载失败，状态码：{r.status_code}，大小：{len(r.content)}")
    except Exception as e:
        print(f"    ❌ 下载异常：{e}")
    return False

# ======================= 主流程 ============================
def main():
    # 1. 连接阿里云盘
    print("🔗 正在连接阿里云盘...")
    ali = Aligo(refresh_token=ALIYUN_REFRESH_TOKEN)
    user = ali.get_user()
    print(f"✅ 登录成功，欢迎：{user.nick_name}")

    target_folder = ali.get_folder_by_path("上市公司年报")
    if target_folder is None:
        target_folder = ali.create_folder("上市公司年报", parent_file_id="root")
        print("📁 已创建云盘根目录文件夹：上市公司年报")
    else:
        print("📁 目标文件夹已存在：上市公司年报")

    # 2. 读取股票列表
    stocks = load_stocks_from_csv("stock_list.csv")
    total = len(stocks)
    print(f"📊 共加载 {total} 只正常股票，开始处理 {TARGET_YEAR} 年年报...")

    success_count = 0
    start_time = time.time()

    # 3. 遍历每一只股票
    for idx, (code, name) in enumerate(stocks, 1):
        print(f"\n[{idx}/{total}] 处理 {code} {name}")
        # 创建本股票临时目录
        local_dir = os.path.join(GITHUB_WORKSPACE, f"{TARGET_YEAR}_{code}")
        os.makedirs(local_dir, exist_ok=True)

        # 获取 PDF 链接
        pdf_url = get_annual_report_pdf_eastmoney(code, name, TARGET_YEAR)
        if not pdf_url:
            # 无年报，跳过
            shutil.rmtree(local_dir, ignore_errors=True)
            continue

        # 下载 PDF
        pdf_filename = f"{code}_{name}_{TARGET_YEAR}.pdf"
        pdf_path = os.path.join(local_dir, pdf_filename)
        print(f"  📥 正在下载 {pdf_filename} ...")
        if not download_pdf(pdf_url, pdf_path):
            shutil.rmtree(local_dir, ignore_errors=True)
            continue

        print(f"  ✅ 下载成功")

        # 上传到云盘（在“上市公司年报/{code}/”下）
        stock_folder = ali.get_folder_by_path(f"上市公司年报/{code}")
        if stock_folder is None:
            stock_folder = ali.create_folder(code, parent_file_id=target_folder.file_id)

        print(f"  ☁️ 上传到阿里云盘...")
        try:
            ali.upload_file(pdf_path, parent_file_id=stock_folder.file_id, name=pdf_filename)
            print(f"  🎉 上传完成")
            success_count += 1
        except Exception as e:
            print(f"  ⚠️ 上传失败：{e}")

        # 清理本地暂存
        shutil.rmtree(local_dir, ignore_errors=True)
        time.sleep(0.3)   # 礼貌等待

    elapsed = time.time() - start_time
    print(f"\n🏁 {TARGET_YEAR} 年任务完成！成功 {success_count}/{total} 只，总耗时 {elapsed/60:.1f} 分钟")

if __name__ == "__main__":
    main()
