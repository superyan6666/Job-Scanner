import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# 抓取的栏目（这里以事业编招考信息为例，大部分编外也在这里或统称为招考信息）
# 中公通常的路径
PATHS = [
    "/html/shiyedanwei/zhaokaoxinxi/",
    "/html/gongwuyuan/zhaokaoxinxi/"
]

def fetch_notices_for_province(province_code, province_name):
    notices = []
    for path in PATHS:
        url = f"http://{province_code}.offcn.com{path}"
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            if res.status_code != 200:
                continue
                
            soup = BeautifulSoup(res.content, 'html.parser')
            # 简化抓取：全局真空吸尘器模式，遍历所有的 a 标签
            for a_tag in soup.find_all('a'):
                title = a_tag.text.strip()
                link = a_tag.get('href')
                
                if not link or not title:
                    continue
                    
                # 必须包含公考相关的后缀或目录
                if '.html' not in link and '/gonggao/' not in link and '/zhaokao/' not in link:
                    continue
                    
                # 必须是有效的长标题（过滤掉导航栏短词）
                if len(title) < 15:
                    continue
                    
                # 关键词过滤
                keywords = ['招聘', '招考', '录用', '遴选', '选调', '招录', '事业编', '公务员', '公告']
                if not any(k in title for k in keywords):
                    continue
                    
                # 排除一些干扰项
                exclude_words = ['课程', '培训', '辅导', '笔试题', '面试题', '教材', '真题']
                if any(k in title for k in exclude_words):
                    continue
                    
                if not link.startswith('http'):
                    if link.startswith('//'):
                        link = "http:" + link
                    elif link.startswith('/'):
                        link = f"http://{province_code}.offcn.com" + link
                    else:
                        continue # 相对路径比较复杂，跳过
                        
                # 尝试提取文本里的日期（如果有），没有就算了
                date_match = re.search(r'\d{4}-\d{2}-\d{2}|\d{2}-\d{2}', title)
                publish_date = date_match.group() if date_match else "近期发布"
                
                notices.append({
                    "title": title,
                    "url": link,
                    "province": province_name,
                    "publish_date": publish_date
                })
        except Exception as e:
            print(f"[{province_name}] 抓取失败: {e}")
            
    # 去重处理（同一个页面可能有相同的文章推荐）
    unique_notices = {}
    for n in notices:
        unique_notices[n['url']] = n
        
    return list(unique_notices.values())

def fetch_article_content(url):
    """
    抓取正文，同时提取所有的 xls/xlsx/pdf 附件下载链接。
    返回格式： {"text": "正文内容...", "attachments": [{"name": "附件名", "url": "下载链接"}]}
    """
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            return {"text": "", "attachments": []}
            
        # 注意编码问题
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.content, 'html.parser')
        
        content_div = soup.find('div', class_='zg_main') or \
                      soup.find('div', class_='offcn_content') or \
                      soup.find('div', class_='zgo_notice') or \
                      soup.find('div', class_='zoffcn_zw') or \
                      soup.find('div', class_='art_content')
                      
        result = {"text": "", "attachments": []}
        
        if content_div:
            result["text"] = content_div.get_text(separator='\n', strip=True)
            
            # 提取附件链接
            for a_tag in content_div.find_all('a', href=True):
                href = a_tag['href']
                if href.lower().endswith(('.xls', '.xlsx', '.pdf')):
                    full_url = href
                    if not href.startswith('http'):
                        if href.startswith('//'):
                            full_url = 'http:' + href
                        elif href.startswith('/'):
                            # 简单的域名拼接（中公网的省份站点）
                            base = url.split('/')[0] + "//" + url.split('/')[2]
                            full_url = base + href
                        else:
                            base_path = url.rsplit('/', 1)[0]
                            full_url = base_path + '/' + href
                            
                    name = a_tag.text.strip() or "未命名附件"
                    result["attachments"].append({"name": name, "url": full_url})
                    
        return result
    except Exception as e:
        print(f"提取正文失败 {url}: {e}")
        return {"text": "", "attachments": []}

def fetch_all():
    # 现在根据您的要求，仅保留四川和广东
    targets = {
        'sc': '四川',
        'gd': '广东'
    }
    
    all_notices = []
    for code, name in targets.items():
        print(f"正在抓取 {name} ({code}) ...")
        notices = fetch_notices_for_province(code, name)
        all_notices.extend(notices)
        print(f"抓取完成 {name}: 发现 {len(notices)} 条近期公告")
        
    return all_notices

if __name__ == "__main__":
    res = fetch_all()
    for r in res[:5]:
        print(r)
