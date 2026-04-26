import requests
import json
import time
import os
import re
import akshare as ak
import csv
from bs4 import BeautifulSoup
from datetime import datetime
from aligo import Aligo

# ================= 配置区域 (从环境变量读取) =================
ALIYUN_REFRESH_TOKEN = os.getenv("ALIYUN_REFRESH_TOKEN")
ALIYUN_FOLDER_ID = os.getenv("ALIYUN_FOLDER_ID")
GITHUB_WORKSPACE = "/tmp"  # GitHub Actions 的临时运行空间

# 选择需要查询的年份范围
START_YEAR = 2006
END_YEAR = 2026

# 巨潮资讯网API
CNINFO_API = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
# PDF下载的基础URL
PDF_BASE_URL = "http://static.cninfo.com.cn/"

# ================= 模块一: 获取需要处理的股票代码 =================
def get_stock_list():
    print("正在从本地 stock_list.csv 读取股票列表...")
    stocks = []
    try:
        with open('stock_list.csv', 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row['code'].strip()
                name = row['name'].strip()
                if 'ST' not in name and '*ST' not in name:  # 二次过滤（安全）
                    stocks.append((code, name))
        print(f"成功读取 {len(stocks)} 只正常股票。")
    except FileNotFoundError:
        print("错误：stock_list.csv 文件不存在，请确保文件已提交到仓库！")
    return stocks

# ================= 模块二: 爬取年报PDF链接并过滤异常 =================
def get_annual_report_urls_sina(stock_code, stock_name, year):
    """
    从新浪财经获取指定股票和年份的年报 PDF 直链。
    返回 PDF URL 或 None。
    """
    # 新浪财经公告列表页面
    url = f"http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/{stock_code}.phtml"
    params = {
        "year": year,
        "type": "yearly"   # 只显示年度报告
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.encoding = 'gbk'  # 新浪页面通常用 GBK
        soup = BeautifulSoup(resp.text, 'html.parser')

        # 查找所有公告行，筛选“年度报告”且不含“摘要”等关键词的
        table = soup.find('table', id='con02_table')
        if not table:
            return None

        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 3:
                continue
            date_td, title_td, pdf_td = cells[0], cells[1], cells[2]
            title = title_td.get_text(strip=True)
            # 过滤掉摘要、英文版、修订版、已取消等
            if '年度报告' not in title or any(k in title for k in ['摘要', '英文', '修订', '已取消']):
                continue

            # PDF 链接可能在 a 标签中
            pdf_a = pdf_td.find('a')
            if pdf_a and pdf_a.get('href'):
                # 新浪的 PDF 链接可能是绝对路径，也可能相对
                pdf_url = pdf_a['href']
                if pdf_url.startswith('//'):
                    pdf_url = 'http:' + pdf_url
                elif pdf_url.startswith('/'):
                    pdf_url = 'http://vip.stock.finance.sina.com.cn' + pdf_url
                # 检查是否是真正的 PDF 文件
                if pdf_url.endswith('.pdf') or 'fileformat=pdf' in pdf_url:
                    return pdf_url

        # 如果没找到，尝试第二页（部分年份可能在后面）
        resp2 = requests.get(url, params={**params, 'p': '2'}, headers=headers, timeout=15)
        resp2.encoding = 'gbk'
        soup2 = BeautifulSoup(resp2.text, 'html.parser')
        table2 = soup2.find('table', id='con02_table')
        if table2:
            for row in table2.find_all('tr'):
                # ... 同样的解析逻辑，此处略，可封装
                pass  # 可根据需要补充

    except Exception as e:
        print(f"    新浪财经获取年报链接异常 [{stock_code}-{year}]: {e}")

    return None

# ================= 模块三: 记录下载状态，避免重复请求 =================
def load_downloaded_records(record_file):
    """加载已下载的记录"""
    if os.path.exists(record_file):
        with open(record_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def save_downloaded_record(record_file, record):
    """追加一条下载记录"""
    with open(record_file, 'a') as f:
        f.write(record + '\n')

# ================= 模块四: 按股票代码暂存并上传云盘 =================
def main():
    # 初始化阿里云盘客户端
    print("正在连接阿里云盘...")
    ali = Aligo(refresh_token=ALIYUN_REFRESH_TOKEN)
    user_info = ali.get_user()
    print(f"成功登录，欢迎: {user_info.nick_name}")
    
    # 在云盘上检查/创建目标文件夹
    target_folder = ali.get_folder_by_path(path='上市公司年报')
    if target_folder is None:
        target_folder = ali.create_folder('上市公司年报', parent_file_id='root')
        print(f"已在云盘创建根目录文件夹: 上市公司年报")
    else:
        print(f"目标文件夹已存在: 上市公司年报")
    
    # 本地记录文件，用于去重
    record_file = os.path.join(GITHUB_WORKSPACE, "downloaded_records.txt")
    downloaded = load_downloaded_records(record_file)
    
    # 获取正常股票列表
    stocks = get_stock_list()
    
    # 遍历每个股票和每年
    for stock_code, stock_name in stocks:
        print(f"处理股票: {stock_code} {stock_name}")
        
        # 为每个股票代码创建一个本地暂存文件夹
        local_stock_dir = os.path.join(GITHUB_WORKSPACE, stock_code)
        os.makedirs(local_stock_dir, exist_ok=True)
        
        for year in range(START_YEAR, END_YEAR + 1):
            record_key = f"{stock_code}_{year}"
            
            # 去重检查
            if record_key in downloaded:
                print(f"  {year}年年报已下载，跳过。")
                continue
                
            # 获取年报链接
            pdf_url = get_annual_report_urls_sina(stock_code,stock_name, year)
            if not pdf_url:
                continue
                
            # 下载PDF到本地暂存
            pdf_filename = f"{stock_code}_{stock_name}_{year}.pdf"
            pdf_path = os.path.join(local_stock_dir, pdf_filename)
            print(f"  正在下载 {year}年年报...")
            
            try:
                response = requests.get(pdf_url, stream=True, timeout=60)
                if response.status_code == 200:
                    with open(pdf_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"  下载成功: {pdf_filename}")
                    
                    # 记录到已下载集合
                    save_downloaded_record(record_file, record_key)
                    downloaded.add(record_key)
                    
                    # 礼貌延迟，避免对服务器造成压力
                    time.sleep(1)
                else:
                    print(f"  下载失败，状态码: {response.status_code}")
            except Exception as e:
                print(f"  下载出错: {e}")
                continue
        
        # 将当前股票的所有年报打包上传到云盘对应文件夹
        if os.listdir(local_stock_dir):
            # 在云盘目标文件夹下，按股票代码创建子文件夹
            stock_cloud_folder = ali.get_folder_by_path(path=f'上市公司年报/{stock_code}')
            if stock_cloud_folder is None:
                stock_cloud_folder = ali.create_folder(stock_code, parent_file_id=target_folder.file_id)
            
            # 上传文件夹内的所有PDF
            for file_name in os.listdir(local_stock_dir):
                file_path = os.path.join(local_stock_dir, file_name)
                print(f"  上传 {file_name} 到阿里云盘...")
                ali.upload_file(file_path, parent_file_id=stock_cloud_folder.file_id, name=file_name)
            
            # 上传完成后，删除本地暂存，释放GitHub Actions空间
            import shutil
            shutil.rmtree(local_stock_dir)
    
    # 清理记录文件，避免下次启动时重复读取（可选）
    # os.remove(record_file)
    print("所有任务完成！")

if __name__ == "__main__":
    main()
