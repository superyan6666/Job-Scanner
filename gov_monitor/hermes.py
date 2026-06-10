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

# 动态加载 JSON 规则文件
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
try:
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
        SICHUAN_BLACKLIST_KEYWORDS = config_data.get("SICHUAN_BLACKLIST_KEYWORDS", [])
        PREFECTURES_RULES = config_data.get("PREFECTURES_RULES", {})
except Exception as e:
    print(f"加载 config.json 失败，降级为空规则: {e}")
    SICHUAN_BLACKLIST_KEYWORDS = []
    PREFECTURES_RULES = {}

def filter_sichuan_gdp(province, title, content):
    """
    如果是四川省的公告，仅对【标题】执行最严苛的区县黑名单过滤。
    这是为了防止“全省/全市联合招聘”因为正文里带了差县的名字而被整篇误杀。
    """
    if province != '四川':
        return True # 不是四川，放行
        
    # 1. 拦截明确指定的黑名单城市、区县和高危关键词（如乡镇）
    for keyword in SICHUAN_BLACKLIST_KEYWORDS:
        if keyword in title:
            print(f"❌ [地区拦截] {title} - 标题命中高危黑名单: {keyword}")
            return False
            
    # 2. 拦截甘孜、阿坝、凉山的非州府所在地
    # 只有明确提到州府（康定、马尔康、西昌）或州直属（州直、州属、州本级）才放行
    for pref, capitals in PREFECTURES_RULES.items():
        if pref in title:
            is_capital_or_direct = any(cap in title for cap in capitals)
            if not is_capital_or_direct:
                print(f"❌ [地区拦截] {title} - 标题命中 {pref} 偏远地区（非州府所在地）")
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

【重要例外：联合招聘豁免权】
如果这是一份包含多个岗位的“联合招聘”公告（例如《XX市下属各区县事业单位联合公开招聘》或《全省XX系统统一考试》），里面虽然包含了一些黑名单县区或乡镇岗位，但也可能包含市级、州级、区级的优质岗位。
对于这种混合了多种岗位的公告，**只要其中存在或可能存在任何一个符合用户条件且不踩红线的好岗位，就必须输出 "ACCEPT"**。绝对不能因为文中提到了部分垃圾岗位就把整个大公告拦截。

如果不是联合招聘，或者即便是联合招聘但没有任何符合条件的岗位，请遵循以下红线：

【绝对拦截红线 - 只要触碰即 REJECT】：
1. "招聘对象：仅限2026年全日制应届毕业生" (用户是非应届生)
2. "学历要求：仅限硕士研究生及以上" (用户是本科学历)
3. "专业要求：仅限计算机、医药、机械等" (用户是会计学)
4. "岗位性质：全部为乡镇岗位 / 偏远基层" (待遇差/发展受限)
5. "全部岗位最低服务年限超过5年" (长期绑定)
6. "全部为三不限岗位" (不限专业/学历/户籍，分数过卷)

注意：如果你在正文的最后看到了【=== 以下为从附件表格中为您专门提取的可能匹配您专业的岗位详情 ===】的内容，这是系统自动从 Excel/PDF 附件中为您提取的专属岗位表！
**你必须将这些附件行数据作为最核心的判断依据！**
如果在附件行数据中发现了“乡镇”、“最低服务年限5年”等红线词汇，即便它是联合招聘，也请直接 REJECT！
如果在附件行数据中发现有完美的岗位，请务必 ACCEPT！

公告标题：{title}
公告正文（包含可能的附件行数据）：
{content[:3000]}
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
