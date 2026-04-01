import os
import sqlite3
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# 設定
DB_PATH = 'finviz_history.db'
FINVIZ_URL = 'https://finviz.com/groups.ashx?g=industry&v=140&o=-perf1w'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS industries (
            date TEXT,
            name TEXT,
            perf_1w REAL,
            perf_1m REAL,
            perf_3m REAL,
            perf_6m REAL,
            perf_1y REAL,
            perf_ytd REAL,
            PRIMARY KEY (date, name)
        )
    ''')
    conn.commit()
    return conn

def parse_percent(val):
    if not val or val == '-':
        return 0.0
    return float(val.replace('%', '').replace('+', ''))

def scrape_finviz():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # ユーザーエージェントを設定してボット判定を回避
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        print(f"Navigating to {FINVIZ_URL}...")
        try:
            # wait_until="domcontentloaded" に変更し、タイムアウトを延長
            page.goto(FINVIZ_URL, wait_until="domcontentloaded", timeout=60000)
            # 追加の待機
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception as e:
            print(f"Initial load warning: {e}")
            # とりあえず続行
        
        # テーブルが表示されるまで待機
        try:
            page.wait_for_selector("table.styled-table-new", timeout=20000)
        except Exception as e:
            print(f"Selector timeout: {e}")
            # スクリーンショットを撮ってデバッグ
            page.screenshot(path="debug_finviz.png")
            return None
        
        content = page.content()
        browser.close()
        return content

def process_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.select_one('table.styled-table-new')
    if not table:
        print("Error: Table not found")
        return []

    rows = table.find_all('tr', class_='styled-row')
    data = []
    today = datetime.now().strftime('%Y-%m-%d')
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 10:
            continue
            
        # インデックスはブラウザでの確認に基づく
        # 0:No, 1:Name, 2:Perf1W, 3:Perf1M, 4:Perf3M, 5:Perf6M, 6:Perf1Y, 7:PerfYTD
        name = cols[1].text.strip()
        perf_1w = parse_percent(cols[2].text.strip())
        perf_1m = parse_percent(cols[3].text.strip())
        perf_3m = parse_percent(cols[4].text.strip())
        perf_6m = parse_percent(cols[5].text.strip())
        perf_1y = parse_percent(cols[6].text.strip())
        perf_ytd = parse_percent(cols[7].text.strip())
        
        data.append((today, name, perf_1w, perf_1m, perf_3m, perf_6m, perf_1y, perf_ytd))
    
    return data

def save_to_db(data):
    conn = init_db()
    cursor = conn.cursor()
    # 同じ日のデータがあれば上書き（リトライ対応）
    cursor.executemany('''
        INSERT OR REPLACE INTO industries (date, name, perf_1w, perf_1m, perf_3m, perf_6m, perf_1y, perf_ytd)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()
    conn.close()
    print(f"Saved {len(data)} rows to {DB_PATH}")

def generate_report():
    conn = sqlite3.connect(DB_PATH)
    # 最新のデータを取得
    df_latest = pd.read_sql_query("SELECT * FROM industries WHERE date = (SELECT MAX(date) FROM industries) ORDER BY perf_1w DESC LIMIT 10", conn)
    
    print("\n### 最近の勢いがあるセクター (Top 10 - 1 Week Performance)")
    print(df_latest[['name', 'perf_1w', 'perf_1m']].to_markdown(index=False))
    
    # 歴史的な比較（もしデータがあれば）
    dates = pd.read_sql_query("SELECT DISTINCT date FROM industries ORDER BY date DESC LIMIT 2", conn)
    if len(dates) >= 2:
        curr_date = dates.iloc[0]['date']
        prev_date = dates.iloc[1]['date']
        print(f"\n### 前回 ({prev_date}) からの変化 - 勢いが増しているセクター")
        
        query = f"""
        SELECT 
            c.name, 
            c.perf_1w as current_1w, 
            p.perf_1w as previous_1w,
            (c.perf_1w - p.perf_1w) as change_1w
        FROM industries c
        JOIN industries p ON c.name = p.name
        WHERE c.date = '{curr_date}' AND p.date = '{prev_date}'
        ORDER BY change_1w DESC
        LIMIT 5
        """
        df_trends = pd.read_sql_query(query, conn)
        if not df_trends.empty:
            print(df_trends.to_markdown(index=False))
        else:
            print("比較可能なデータがありません。")
            
    conn.close()
        
    conn.close()

if __name__ == "__main__":
    html = scrape_finviz()
    data = process_data(html)
    if data:
        save_to_db(data)
        generate_report()
    else:
        print("No data collected.")
