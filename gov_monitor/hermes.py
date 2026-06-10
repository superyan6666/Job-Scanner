import os
import requests
import json

# 配置环境变量 SILICONFLOW_API_KEY
SILICONFLOW_API_KEY = os.environ.get('SILICONFLOW_API_KEY', '')
# 默认采用智谱 GLM-4 或 DeepSeek-V2.5，硅基流动接口目前支持 deepseek-ai/DeepSeek-V2.5
SILICONFLOW_MODEL = os.environ.get('SILICONFLOW_MODEL', 'deepseek-ai/DeepSeek-V2.5')

# 四川省人均 GDP 排名前 50% 城市（硬编码白名单）
SICHUAN_TOP_CITIES = ['成都', '攀枝花', '德阳', '绵阳', '宜宾', '乐山', '眉山', '泸州', '自贡', '遂宁', '内江']

def filter_sichuan_gdp(province, title, content):
    """
    如果是四川省的公告，检查标题和前段文本是否属于 Top GDP 城市。
    如果都不是，则直接拦截。
    """
    if province != '四川':
        return True # 不是四川，放行
        
    text_to_check = title + (content[:500] if content else "")
    
    # 只要包含任何一个白名单城市，或者说是全省联考/省属单位，就放行
    if '省属' in text_to_check or '四川省' in title:
        # 有些省级的也会带“四川省”，先大体放过，避免误杀省级好单位
        pass
        
    for city in SICHUAN_TOP_CITIES:
        if city in text_to_check:
            return True
            
    print(f"❌ [地区拦截] {title} - 四川非核心城市，直接过滤。")
    return False

def evaluate_notice(title, content):
    """
    调用 SiliconFlow API 判断用户是否有资格报考。
    """
    if not content:
        # 没有正文（可能是特殊页面或死链），为安全起见先放行
        return True
        
    if not SILICONFLOW_API_KEY:
        print("⚠️ 未配置 SILICONFLOW_API_KEY，HERMES AI 大脑暂不工作，全部放行。")
        return True
        
    prompt = f"""你是一个名为 HERMES 的高级招聘助理系统。
用户的基本画像是：【普通一本学历】（非双一流），【非应届】（往届生，有工作经验），专业为【工商管理类-会计学】。

下面是一篇公务员、事业单位或国企的招录公告正文。请你仔细阅读，如果公告明确出现了**极其严格的限制条件**导致该用户绝对无法报考，请直接输出 "REJECT"，并在下一行给出简短理由。
如果用户有资格报考，或者网页正文中没有明确将该用户卡死的条件（比如未限制必须理工科，未限制应届等），请输出 "ACCEPT"。

卡死条件示例：
1. "招聘对象：2026年全日制普通高校应届毕业生" (用户是非应届生，REJECT)
2. "学历要求：仅限硕士研究生及以上" (用户是本科学历，REJECT)
3. "专业要求：仅限计算机、医药、机械等相关专业" (用户是会计学，REJECT)

注意：详细岗位要求大多在附件中，此处你仅基于网页整体招聘要求进行初步硬性过滤。

公告标题：{title}
公告正文：
{content[:2000]}
"""

    headers = {
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": SILICONFLOW_MODEL,
        "messages": [
            {"role": "system", "content": "You are Hermes, a strict and intelligent job filtering AI."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 150
    }
    
    try:
        response = requests.post("https://api.siliconflow.cn/v1/chat/completions", headers=headers, json=data, timeout=15)
        if response.status_code == 200:
            result = response.json().get('choices', [{}])[0].get('message', {}).get('content', '').strip()
            if result.upper().startswith("REJECT"):
                print(f"🤖 [HERMES AI 拦截] {title}\n理由: {result}")
                return False
            return True
        else:
            print(f"⚠️ HERMES API 请求失败: {response.text}")
            return True
    except Exception as e:
        print(f"⚠️ HERMES 调用异常: {e}")
        return True
