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
            
            # 提取正文及附件
            content_data = fetch_article_content(url)
            content = content_data["text"]
            attachments = content_data["attachments"]
            
            # 鉴别层 1: 四川地市过滤（只看标题）
            if not filter_sichuan_gdp(province, title, content):
                save_notice(url, title, province, notice['publish_date'])
                continue
                
            # 鉴别层 1.5: 附件表格深度解析 (Phase 2)
            extracted_rows = ""
            if attachments:
                from attachment_parser import parse_attachments
                print(f"[{title}] 发现 {len(attachments)} 个附件，正在下载并精细化寻岗...")
                extracted_rows = parse_attachments(attachments)
                if extracted_rows:
                    print(f"[{title}] 成功提取到匹配您专业的岗位行数据！")
                    
            # 鉴别层 2: HERMES AI 简历匹配
            full_content_for_llm = content
            if extracted_rows:
                full_content_for_llm = content + "\n\n" + extracted_rows
                
            if not evaluate_notice(title, full_content_for_llm):
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
