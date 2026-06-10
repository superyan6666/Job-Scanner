import time
from db import init_db, is_notice_exists, save_notice
from crawler import fetch_all, fetch_article_content
from notifier import notify_all
from hermes import filter_sichuan_gdp, evaluate_notice

def main():
    print("=== 开始执行招录公告云端监控任务 ===")
    
    # 1. 确保数据库初始化
    init_db()
    
    # 2. 抓取所有目标省份的最新公告
    notices = fetch_all()
    
    # 3. 对比数据库，寻找新公告并进行鉴别
    new_notices = []
    for notice in notices:
        url = notice['url']
        if not is_notice_exists(url):
            title = notice['title']
            province = notice['province']
            
            # 提取正文
            content = fetch_article_content(url)
            
            # 鉴别层 1: 四川地市 GDP 过滤
            if not filter_sichuan_gdp(province, title, content):
                save_notice(url, title, province, notice['publish_date'])
                continue
                
            # 鉴别层 2: HERMES AI 简历匹配
            if not evaluate_notice(title, content):
                save_notice(url, title, province, notice['publish_date'])
                continue
                
            new_notices.append(notice)
            
    print(f"对比与 AI 鉴别完成，共发现 {len(new_notices)} 条符合您的优质公告！")
    
    # 4. 推送并保存新公告
    notified_count = 0
    for notice in new_notices:
        # 为了防止初次运行时海量公告轰炸，最多只推送前 3 条作为演示
        if notified_count < 3:
            notify_all(
                title=notice['title'],
                province=notice['province'],
                publish_date=notice['publish_date'],
                url=notice['url']
            )
            notified_count += 1
            time.sleep(1) # 防止频繁调用 Webhook 被封
            
        save_notice(
            url=notice['url'],
            title=notice['title'],
            province=notice['province'],
            publish_date=notice['publish_date']
        )
        
    print("=== 监控任务执行完毕 ===")

if __name__ == '__main__':
    main()
