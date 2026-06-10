import sqlite3
import os
from datetime import datetime

# 获取当前脚本所在目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'gov_notices.db')

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS notices (
            url TEXT PRIMARY KEY,
            title TEXT,
            province TEXT,
            publish_date TEXT,
            found_time TEXT
        )
    ''')
    conn.commit()
    conn.close()

def is_notice_exists(url):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM notices WHERE url = ?', (url,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def save_notice(url, title, province, publish_date):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('''
        INSERT OR IGNORE INTO notices (url, title, province, publish_date, found_time)
        VALUES (?, ?, ?, ?, ?)
    ''', (url, title, province, publish_date, now))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Gov Monitor 数据库初始化完成。")
