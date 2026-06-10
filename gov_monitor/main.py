import time
from db import init_db, is_notice_exists, save_notice
from crawler import fetch_all
from notifier import notify_all

def main():
    print("=== 开始执行招录公告云端监控任务 ===")
    
    # 1. 确保数据库初始化
    init_db()
    
    # 2. 抓取所有目标省份的最新公告
    notices = fetch_all()
    
    # 3. 对比数据库，寻找新公告
    new_notices = []
    for notice in notices:
        url = notice['url']
        if not is_notice_exists(url):
            new_notices.append(notice)
            
    print(f"对比完成，共发现 {len(new_notices)} 条全新公告！")
    
    # 4. 推送并保存新公告
    for notice in new_notices:
        notify_all(
            title=notice['title'],
            province=notice['province'],
            publish_date=notice['publish_date'],
            url=notice['url']
        )
        save_notice(
            url=notice['url'],
            title=notice['title'],
            province=notice['province'],
            publish_date=notice['publish_date']
        )
        time.sleep(1) # 防止频繁调用 Webhook 被封
        
    print("=== 监控任务执行完毕 ===")

if __name__ == '__main__':
    main()
