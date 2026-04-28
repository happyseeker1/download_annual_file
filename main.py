import os
import csv
import time
import requests
from aligo import Aligo
import shutil

GITHUB_WORKSPACE = "/tmp"
PDF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "http://vip.stock.finance.sina.com.cn/",
}
ALIYUN_REFRESH_TOKEN = os.getenv("ALIYUN_REFRESH_TOKEN")
ALIYUN_FOLDER_ID = os.getenv("ALIYUN_FOLDER_ID")

def load_download_list(filename="download_list.csv"):
    tasks = []
    with open(filename, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tasks.append((row["code"], row["name"], int(row["year"]), row["url"]))
    return tasks

def download_pdf(pdf_url, save_path):
    try:
        r = requests.get(pdf_url, headers=PDF_HEADERS, timeout=60)
        if r.status_code == 200 and len(r.content) > 10*1024:
            with open(save_path, "wb") as f:
                f.write(r.content)
            return True
    except Exception as e:
        print(f"    ❌ 下载异常：{e}")
    return False

def main():
    print("🔗 连接阿里云盘...")
    ali = Aligo(refresh_token=ALIYUN_REFRESH_TOKEN)
    user = ali.get_user()
    print(f"✅ 登录成功，欢迎：{user.nick_name}")

    target_folder = ali.get_folder_by_path("上市公司年报")
    if target_folder is None:
        target_folder = ali.create_folder("上市公司年报", parent_file_id="root")
        print("📁 已在根目录创建：上市公司年报")
    else:
        print("📁 目标文件夹已存在：上市公司年报")

    tasks = load_download_list()
    total = len(tasks)
    print(f"📊 共 {total} 条年报链接待处理")

    success = 0
    for idx, (code, name, year, url) in enumerate(tasks, 1):
        print(f"[{idx}/{total}] {code} {name} {year}")
        local_dir = os.path.join(GITHUB_WORKSPACE, f"{code}_{year}")
        os.makedirs(local_dir, exist_ok=True)
        fname = f"{code}_{name}_{year}.pdf"
        fpath = os.path.join(local_dir, fname)

        if download_pdf(url, fpath):
            stock_folder = ali.get_folder_by_path(f"上市公司年报/{code}")
            if stock_folder is None:
                stock_folder = ali.create_folder(code, parent_file_id=target_folder.file_id)
            try:
                ali.upload_file(fpath, parent_file_id=stock_folder.file_id, name=fname)
                print(f"  ☁️ 上传成功")
                success += 1
            except Exception as e:
                print(f"  ⚠️ 上传失败：{e}")
        else:
            print(f"  ❌ 下载失败")

        shutil.rmtree(local_dir, ignore_errors=True)
        time.sleep(0.1)

    print(f"\n🏁 全部完成！成功 {success}/{total}")

if __name__ == "__main__":
    main()
