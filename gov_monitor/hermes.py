import os
import requests
import json

def load_hermes_env():
    env_path = os.path.expanduser('~/.hermes/.env')
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip().startswith('SILICONFLOW_API_KEY='):
                    return line.split('=', 1)[1].strip()
    return ''

# 配置环境变量 SILICONFLOW_API_KEY
SILICONFLOW_API_KEY = os.environ.get('SILICONFLOW_API_KEY') or load_hermes_env()
# 默认采用智谱 GLM-4 或 DeepSeek-V2.5，硅基流动接口目前支持 deepseek-ai/DeepSeek-V2.5
SILICONFLOW_MODEL = os.environ.get('SILICONFLOW_MODEL', 'deepseek-ai/DeepSeek-V2.5')

# 四川省黑名单城市（分数虚高，待遇一般）
SICHUAN_BLACKLIST_CITIES = ['德阳', '资阳', '内江', '广元', '巴中', '绵阳', '眉山']

def filter_sichuan_gdp(province, title, content):
    """
    如果是四川省的公告，检查是否命中了黑名单城市，或者三州的非州府所在地。
    命中则拦截，否则放行。
    """
    if province != '四川':
        return True # 不是四川，放行
        
    text_to_check = title + (content[:500] if content else "")
    
    # 1. 拦截明确指定的黑名单城市
    for city in SICHUAN_BLACKLIST_CITIES:
        if city in text_to_check:
            print(f"❌ [地区拦截] {title} - 命中黑名单城市: {city} (分数虚高或待遇一般)")
            return False
            
    # 2. 拦截甘孜、阿坝、凉山的非州府所在地
    # 只有明确提到州府（康定、马尔康、西昌）或州直属（州直、州属、州本级）才放行
    prefectures = {
        '甘孜': ['康定', '州直', '州属', '州本级'],
        '阿坝': ['马尔康', '州直', '州属', '州本级'],
        '凉山': ['西昌', '州直', '州属', '州本级']
    }
    
    for pref, capitals in prefectures.items():
        if pref in text_to_check:
            is_capital_or_direct = any(cap in text_to_check for cap in capitals)
            if not is_capital_or_direct:
                print(f"❌ [地区拦截] {title} - 命中 {pref} 偏远地区（非州府所在地）")
                return False
                
    return True

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
