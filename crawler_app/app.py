import re
from datetime import timedelta, datetime
import requests
from bs4 import BeautifulSoup # pandas tốt hơn
import pandas as pd
from io import StringIO
from threading import Thread
from flask import Flask, request, render_template, make_response
import os
from concurrent.futures import ThreadPoolExecutor

def crawl(url, parser, results):
    try:
        response = requests.get(url)
        dfs = pd.read_html(StringIO(response.text), flavor=f'{parser}')
        for df in dfs:
            # Xóa https ở cuối table (nếu có)
            last_row = str(df.iloc[-1,0])
            if ("http" in last_row or "https" in last_row):
                df = df.drop(df.index[-1])

            #Xóa cột Unnamed: 4 (nếu có)
            if ('Unnamed: 4' in df.columns):
                df = df.drop(columns=['Unnamed: 4'])

            # kiểm cột khu vực của PNJ (nếu có)
            df = df[df['Khu vực'] != 'Giá vàng nữ trang']

            # format lại tất cả cột thời gian cập nhật (nếu có)
            time_column = None
            for i in range(df.shape[1]):
                if ('Thời gian' in df.columns[i]):
                    time_column = df.columns[i]

            print(time_column)
            if (time_column != None):
                try:
                    for i in range(df.shape[0]):
                        og_time = datetime.strptime(df.loc[i,time_column], '%H:%M:%S %d/%m/%Y').date()
                        format_time = og_time.strftime('%d-%m-%Y')
                        df.loc[i,time_column] = format_time
                except Exception as e:
                    print(e)

            # Thêm cột ngày xuất file
            df["Ngày xuất"] = datetime.now().strftime('%d-%m-%Y')
            print(df.columns)

            # Gộp cột tgian & tgian cập nhật
            df = df.rename(columns={'Thời gian cập nhật': 'Thời gian'})

            results.append(df)

        return True
    except ValueError:
        print(f"Không có data: {url}")
        return False

def auto_date(url):
    curr_date = re.search(r'\d{4}-\d{2}-\d{2}', url)
    if(curr_date != None):
        date_format = datetime.strptime(curr_date.group(), '%Y-%m-%d').date()
        next_day = date_format + timedelta(days=1)
        url = url.replace(curr_date.group(), str(next_day))
        return url
    else:
        return None

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def index():
    if(request.method == 'POST'):
        # "https://giavang.org/trong-nuoc/sjc/lich-su/2009-07-14.html"
        url = request.form['url']
        # get trả về None
        days_amount = int(request.form.get('days_amount') or 1) 
        parser = request.form['format']
        print(url, days_amount, parser)

        results = []

        # threads = []
        # for i in range(days_amount):
        #     t = Thread(target=crawl, args=(url, parser, results))
        #     threads.append(t)
        #     t.start()
        #     url = auto_date(url)
        #     # if(url == None): break

        # for t in threads:
        #     t.join()

        # Improve Threads
        urls = []
        for i in range(days_amount):
            urls.append(url)
            url = auto_date(url)

        with ThreadPoolExecutor (max_workers=20) as executor:
            for u in urls:
                executor.submit(crawl, u, parser, results)

        if (results):
            final_df = pd.concat(results, ignore_index=True)
            if ('Thời gian' in final_df.columns):
                final_df['Thời gian'] = pd.to_datetime(final_df['Thời gian'], format='%d-%m-%Y')
                final_df = final_df.sort_values(by='Thời gian', ascending=True)
                final_df['Thời gian'] = final_df['Thời gian'].dt.strftime('%d-%m-%Y')
            csv_string = final_df.to_csv(index=False)

            response = make_response(csv_string)
            response.headers['Content-Type'] = 'text/csv'
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            
            return response
        else:
            return render_template('index.html', error='Không tìm thấy data để crawl!')
    return render_template('index.html')

if __name__ == '__main__':
    # deploy tạo port
    port = int(os.environ.get('PORT', 5000)) # default port
    app.run(host='0.0.0.0', port=port, debug=False)