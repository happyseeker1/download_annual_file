import requests
import json
import time
import os
import re
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
    """
    获取当前A股所有正常上市股票代码，已排除ST、*ST等异常股票。
    数据源来自东方财富的实时行情API，免费稳定。
    """
    print("正在获取A股股票列表...")
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "10000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f14",  # 只获取代码和名称
        "_": str(int(time.time() * 1000))
    }
    resp = requests.get(url, params=params, timeout=10)
    data = resp.json()
    
    stocks = []
    for item in data.get('data', {}).get('diff', []):
        code = item.get('f12', '')
        name = item.get('f14', '')
        # 过滤掉ST和*ST股票
        if 'ST' not in name and '*ST' not in name and 'st' not in name:
            stocks.append((code, name))
    print(f"成功获取 {len(stocks)} 只正常股票。")
    return stocks

# ================= 模块二: 爬取年报PDF链接并过滤异常 =================
def get_annual_report_urls(stock_code, year):
    """
    从巨潮资讯网爬取指定股票和年份的年报PDF直链，已内置异常过滤。
    返回PDF URL或None。
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
    }
    
    # 构造查询参数
    data = {
        "pageNum": 1,
        "pageSize": 30,
        "column": "szse",
        "tabName": "fulltext",
        "plate": "sz;sh",
        "stock": f"{stock_code}, {name}",
        "searchkey": "",
        "secid": "",
        "category": "category_ndbg_szsh",  # 年报类别
        "trade": "",
        "seDate": f"{year}-01-01~{year}-12-31",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true"
    }
    
    try:
        resp = requests.post(CNINFO_API, data=data, headers=headers, timeout=15)
        announcements = resp.json().get('announcements', [])
        
        # 过滤规则：排除摘要、英文版、更正版、已取消公告
        exclude_keywords = ['摘要', '英文', '更正', '修订', '已取消', '公告']
        for ann in announcements:
            title = ann.get('announcementTitle', '')
            if not any(keyword in title for keyword in exclude_keywords):
                adjunct_url = ann.get('adjunctUrl', '')
                if adjunct_url and adjunct_url.endswith('.pdf'):
                    return PDF_BASE_URL + adjunct_url
    except Exception as e:
        print(f"获取年报链接失败 [{stock_code}-{year}]: {e}")
    
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
            pdf_url = get_annual_report_urls(stock_code, year)
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
