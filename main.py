import os
import csv
import time
import re
import requests
from bs4 import BeautifulSoup
from aligo import Aligo
import shutil
import sys

# ======================= 配置 =======================
GITHUB_WORKSPACE = "/tmp"
PDF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "http://vip.stock.finance.sina.com.cn/",
}
ALIYUN_REFRESH_TOKEN = os.getenv("ALIYUN_REFRESH_TOKEN")
ALIYUN_FOLDER_ID = os.getenv("ALIYUN_FOLDER_ID")
TARGET_YEAR = os.getenv("TARGET_YEAR")

if not TARGET_YEAR:
    print("❌ 未指定 TARGET_YEAR 环境变量，退出。")
    sys.exit(1)
TARGET_YEAR = int(TARGET_YEAR)
print(f"📆 本次任务目标年份：{TARGET_YEAR}")

# ======================= 股票列表 =======================
def load_stocks_from_csv(filename="stock_list.csv"):
    stocks = []
    with open(filename, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row["code"].strip()
            name = row["name"].strip()
            # 额外过滤 ST（保证万无一失）
            if "ST" not in name and "*ST" not in name:
                stocks.append((code, name))
    return stocks

# ================== 新浪年报链接获取（支持分页） ==================
def get_annual_report_pdf_sina(code, name, year, max_pages=2):
    """
    从新浪财经公告页面获取指定股票、年份的第一个年度报告 PDF 链接。
    max_pages：最多搜索的页数（默认2，年报通常在前两页）。
    """
    base_url = f"http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/{code}.phtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    # 过滤关键词（包括标题中可能出现的干扰项）
    exclude_kw = ["摘要", "英文", "修订", "已取消", "更正", "补充"]
    # 匹配“年度报告”，允许前后空格
    year_report_pattern = re.compile(r"年度\s*报告")

    for page in range(1, max_pages + 1):
        params = {"year": year, "type": "yearly", "page": page}
        try:
            resp = requests.get(base_url, params=params, headers=headers, timeout=20)
            resp.encoding = "gbk"
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", id="con02_table")
            if not table:
                continue

            rows = table.find_all("tr")[1:]  # 跳过表头
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue
                title_td = cols[1]
                title = title_td.get_text(strip=True)

                # 必须包含“年度报告”
                if not year_report_pattern.search(title):
                    continue
                # 排除含干扰词
                if any(kw in title for kw in exclude_kw):
                    continue

                # 获取附件单元格中的链接
                attach_td = cols[2]
                a_tag = attach_td.find("a")
                if a_tag and a_tag.get("href"):
                    href = a_tag["href"]
                    # 处理相对链接
                    if href.startswith("//"):
                        pdf_url = "http:" + href
                    elif href.startswith("/"):
                        pdf_url = "http://vip.stock.finance.sina.com.cn" + href
                    else:
                        pdf_url = href
                    # 确保是PDF文件
                    if ".pdf" in pdf_url:
                        print(f"    🔗 找到年报（第{page}页）：{title}")
                        return pdf_url
        except Exception as e:
            print(f"    ⚠️ 新浪请求异常（第{page}页） [{code}-{year}]: {e}")
            continue

    return None

# ======================= PDF 下载 =======================
def download_pdf(pdf_url, save_path):
    try:
        r = requests.get(pdf_url, headers=PDF_HEADERS, timeout=60)
        if r.status_code == 200 and len(r.content) > 10 * 1024:
            with open(save_path, "wb") as f:
                f.write(r.content)
            return True
        else:
            print(f"    ❌ 下载失败，状态码：{r.status_code}，大小：{len(r.content)}")
    except Exception as e:
        print(f"    ❌ 下载异常：{e}")
    return False

# ======================= 主流程 =======================
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

    # 3. 遍历每只股票
    for idx, (code, name) in enumerate(stocks, 1):
        print(f"\n[{idx}/{total}] 处理 {code} {name}")
        local_dir = os.path.join(GITHUB_WORKSPACE, f"{TARGET_YEAR}_{code}")
        os.makedirs(local_dir, exist_ok=True)

        # 获取 PDF 链接
        pdf_url = get_annual_report_pdf_sina(code, name, TARGET_YEAR, max_pages=3)
        if not pdf_url:
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

        # 上传到云盘
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

        # 清理本地临时文件
        shutil.rmtree(local_dir, ignore_errors=True)
        time.sleep(0.3)   # 礼貌等待

    elapsed = time.time() - start_time
    print(f"\n🏁 {TARGET_YEAR} 年任务完成！成功 {success_count}/{total} 只，总耗时 {elapsed/60:.1f} 分钟")

if __name__ == "__main__":
    main()
