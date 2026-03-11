from flask import Flask, render_template, request, send_file
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
from datetime import datetime, timedelta
import concurrent.futures

app = Flask(__name__)
OUTPUT_DIR = 'outputs'

# --- HÀM 1: TỰ ĐỘNG ĐỌC NGÀY TỪ URL ---
def extract_date_from_url(url):
    # Tìm định dạng YYYY-MM-DD trong chuỗi URL
    match = re.search(r'\d{4}-\d{2}-\d{2}', url)
    if match:
        return match.group(0)
    return None

# --- HÀM 2: CÀO VÀ ĐỊNH DẠNG TABLE ---
def scrape_and_format(url, parser_type):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200: return None

        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, parser_type)
        tables = soup.find_all('table')
        
        day_dfs = []
        for table in tables:
            # Lấy header (th) để làm cột
            headers_list = [th.text.strip() for th in table.find_all('th')]
            
            # Lấy dữ liệu (td)
            rows = []
            for tr in table.find_all('tr')[1:]: # Bỏ dòng header đầu tiên
                cells = [td.text.strip() for td in tr.find_all('td')]
                if cells: rows.append(cells)
            
            if rows:
                # Tạo DF với cột nếu có header, không thì để tự động
                df = pd.DataFrame(rows, columns=headers_list if len(headers_list) == len(rows[0]) else None)
                
                # YÊU CẦU: Dùng iloc loại bỏ dòng cuối chứa 'http'
                if not df.empty:
                    last_row_str = str(df.iloc[-1].values).lower()
                    if 'http' in last_row_str or 'giavang.org' in last_row_str:
                        df = df.iloc[:-1]
                
                day_dfs.append(df)
        
        return pd.concat(day_dfs, ignore_index=True) if day_dfs else None
    except:
        return None

# --- HÀM 3: ROUTER CHÍNH ---
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        raw_url = request.form.get('url')
        parser_engine = request.form.get('format')
        
        # 1. Tự bắt ngày tháng
        detected_date = extract_date_from_url(raw_url)
        
        # 2. Tạo danh sách URL (Nếu có ngày thì cào 3 ngày liên tiếp cho thầy thấy đa luồng)
        urls_to_crawl = [raw_url]
        if detected_date:
            base_url_part = raw_url.split(detected_date)[0]
            start_dt = datetime.strptime(detected_date, '%Y-%m-%d')
            for i in range(1, 3): # Thêm 2 ngày kế tiếp để test thread
                next_date = (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
                urls_to_crawl.append(f"{base_url_part}{next_date}.html")

        # 3. Đa luồng xử lý
        final_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(scrape_and_format, u, parser_engine) for u in urls_to_crawl]
            for f in concurrent.futures.as_completed(futures):
                res = f.result()
                if res is not None: final_results.append(res)

        if not final_results:
            return "<h3>Không tìm thấy bảng dữ liệu hợp lệ!</h3>"

        # 4. Gộp toàn bộ DF, reset index
        big_df = pd.concat(final_results, ignore_index=True)
        
        # 5. Lưu và gửi file
        if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
        file_path = os.path.join(OUTPUT_DIR, "crawl_result.csv")
        big_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        return send_file(file_path, as_attachment=True)

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)