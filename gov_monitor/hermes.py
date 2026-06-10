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

# 四川省黑名单城市及区县（深度避坑版，结合2026最新真实反馈）
SICHUAN_BLACKLIST_KEYWORDS = [
    # 城市级黑名单（分数虚高、待遇一般或整体拉胯）
    '德阳', '资阳', '内江', '广元', '巴中', '绵阳', '眉山', 
    
    # 巴中所有区县
    '巴州区', '通江县', '南江县', '平昌县',
    
    # 甘孜非州府（除康定外）
    '石渠', '色达', '德格', '白玉', '理塘', '巴塘', '稻城', '乡城', 
    '得荣', '雅江', '道孚', '炉霍', '甘孜县', '新龙',
    
    # 阿坝非州府（除马尔康外）
    '壤塘', '阿坝县', '若尔盖', '红原', '松潘', '九寨沟', '金川', 
    '小金', '黑水', '茂县', '汶川', '理县',
    
    # 凉山非州府（除西昌外）
    '昭觉', '美姑', '布拖', '金阳', '雷波', '普格', '喜德', '越西', 
    '甘洛', '冕宁', '盐源', '木里', '德昌', '会理', '会东', '宁南',
    
    # 其他零散区县避坑
    '安岳', '乐至',  # 资阳市
    '青神', '丹棱',  # 眉山市
    '江油', '三台', '北川', '平武', '盐亭',  # 绵阳市
    '资中', '隆昌', '威远',  # 内江市
    '罗江', '中江',  # 德阳市
    '苍溪', '旺苍', '剑阁', '青川',  # 广元市
    '万源',  # 达州市交通闭塞县
    
    # 高危岗位特征
    '乡镇', '基层服务'
]

def filter_sichuan_gdp(province, title, content):
    """
    如果是四川省的公告，检查是否命中了黑名单城市，或者三州的非州府所在地。
    命中则拦截，否则放行。
    """
    if province != '四川':
        return True # 不是四川，放行
        
    text_to_check = title + (content[:500] if content else "")
    
    # 1. 拦截明确指定的黑名单城市、区县和高危关键词（如乡镇）
    for keyword in SICHUAN_BLACKLIST_KEYWORDS:
        if keyword in text_to_check:
            print(f"❌ [地区拦截] {title} - 命中高危黑名单: {keyword}")
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
        
    prompt = f"""你是一个名为 HERMES 的高级招聘助理系统，负责极其严格的岗位筛查。
用户的基本画像是：【普通一本学历】（非双一流），【非应届】（往届生，有工作经验），专业为【工商管理类-会计学】。

下面是一篇公务员、事业单位或国企的招录公告正文。请你仔细阅读，如果公告明确出现了**极其严格的限制条件**或**避坑红线**导致该用户无法报考或不建议报考，请直接输出 "REJECT"，并在下一行给出简短理由。
如果用户有资格报考，且没有触碰避坑红线，请输出 "ACCEPT"。

【绝对拦截红线 - 只要出现即 REJECT】：
1. "招聘对象：仅限2026年全日制应届毕业生" (用户是非应届生，REJECT)
2. "学历要求：仅限硕士研究生及以上" (用户是本科学历，REJECT)
3. "专业要求：仅限计算机、医药、机械等" (用户是会计学，REJECT)
4. "岗位性质：乡镇岗位 / 偏远基层" (待遇差/发展受限，REJECT)
5. "最低服务年限超过5年" 或 "在本单位服务满5年方可调动" (长期绑定，REJECT)
6. "三不限岗位" (不限专业、不限学历、不限户籍，这种岗位分数卷到天上，性价比极低，REJECT)

注意：详细岗位要求大多在附件中，此处你仅基于网页整体招聘要求和通告说明进行初步硬性过滤。

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
