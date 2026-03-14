import re
from datetime import timedelta, datetime
import requests
from bs4 import BeautifulSoup # pandas tốt hơn
import pandas as pd
from io import StringIO
from threading import Thread
from flask import Flask, request, render_template, make_response

def crawl(url, parser, results):
    try:
        response = requests.get(url)
        df = pd.read_html(StringIO(response.text), flavor=f'{parser}')[0]
        last_row = str(df.iloc[-1,0])
        if ("http" in last_row or "https" in last_row):
            df = df.drop(df.index[-1])
        results.append(df)
    except ValueError:
        print(f"Không có data: {url}")

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

        threads = []
        for i in range(days_amount):
            t = Thread(target=crawl, args=(url, parser, results))
            threads.append(t)
            t.start()
            url = auto_date(url)
            if(url == None): break

        for t in threads:
            t.join()

        if (results):
            final_df = pd.concat(results, ignore_index=True)
            csv_string = final_df.to_csv(index=False)

            response = make_response(csv_string)
            response.headers['Content-Type'] = 'text/csv'
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            
            return response
        else:
            return render_template('index.html', error='Không tìm thấy data để crawl!')
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)