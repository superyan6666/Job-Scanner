#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import time
import random
import re
import hmac
import hashlib
import base64
import urllib.parse
from typing import List, Dict, Optional, Tuple
from requests import Session, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag

# 跨平台文件锁兼容，解决并发写入问题
try:
    import portalocker
    LOCK_ENABLED = True
except ImportError:
    LOCK_ENABLED = False
    print("⚠️  未安装portalocker，文件锁已禁用，并发运行可能导致数据丢失")

# ==================== 配置区域 - 所有自定义修改均在此处 ====================
CONFIG = {
    # 目标城市：BOSS直聘城市ID（默认：成都、重庆、广州、深圳）
    "city_ids": [101250100, 101270100, 101280100, 101280600],
    # 目标岗位关键词（财务岗专属，可自行修改）
    "job_keywords": ["财务", "会计", "出纳", "审计", "税务", "CFA", "FRM", "财务分析", "预算", "成本"],
    # 排除的关键词（岗位/公司名包含这些直接过滤）
    "exclude_keywords": ["外包", "培训", "兼职", "实习", "猎头", "派遣", "代招", "顾问", "销售"],
    # 最低期望薪资（月薪，单位：元）
    "min_salary": 10000,
    # 最多推送多少个最优岗位
    "top_n": 15,
    # 爬取基础间隔（秒），会自动叠加随机抖动和指数退避
    "request_delay": 3,
    # 单页面最大重试次数
    "max_retry": 3,
    # 最多爬取多少页（最新职位集中在前3页，无需过大）
    "max_pages": 3,

    # -------------------------- 新增：学历&工作经验筛选配置 --------------------------
    # 学历白名单（命中任意一个即保留，空列表=不限制）
    "education_allow": ["本科", "硕士", "博士"],
    # 学历黑名单（命中任意一个直接过滤，空列表=不限制）
    "education_deny": ["中专", "高中", "大专"],
    # 工作经验白名单（命中任意一个即保留，空列表=不限制）
    "experience_allow": ["1-3年", "3-5年", "5-10年"],
    # 工作经验黑名单（命中任意一个直接过滤，空列表=不限制）
    "experience_deny": ["应届毕业生", "1年以内", "10年以上", "经验不限"],

    # -------------------------- 推送配置（从环境变量读取，禁止写死在代码里） --------------------------
    "dingtalk_webhook": os.getenv("DINGTALK_WEBHOOK", ""),
    # 钉钉机器人加签密钥（机器人安全设置开启加签时填写，无则留空）
    "dingtalk_secret": os.getenv("DINGTALK_SECRET", ""),

    # -------------------------- 新增：代理配置（从环境变量读取，无需改代码） --------------------------
    # 是否启用代理，自动读取系统环境变量HTTP_PROXY/HTTPS_PROXY
    "proxy_enable": os.getenv("PROXY_ENABLE", "false").lower() == "true",

    # -------------------------- 新增：城市映射配置 --------------------------
    # 城市映射文件路径，无需改代码，扩展城市直接修改json文件
    "city_map_file": "city_map.json",
    # 是否自动从BOSS直聘API动态获取最新城市列表
    "auto_fetch_city_map": False,
}
# ==============================================================================

# -------------------------- 全局常量配置（无需修改） --------------------------
# 随机UA池，每次请求自动切换，大幅降低反爬拦截概率
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0 Safari/537.36",
]

# 反爬检测关键词，命中即判定为触发安全验证
ANTI_CRAWL_KEYWORDS = ["安全验证", "异常访问", "请完成以下验证", "IP地址存在异常", "访问过于频繁"]

# 岗位解析多规则降级匹配（解决平台页面改动解析失败问题）
JOB_PARSE_RULES = {
    "job_list": [
        {"tag": "li", "attrs": {"data-jobid": True}},  # 最稳定：带jobid的节点
        {"tag": "li", "class_contains": "job-card-wrapper"},  # 原规则
        {"tag": "li", "class_contains": "job-card"},  # 降级规则1
        {"tag": "div", "class_contains": "job-item"},  # 降级规则2
    ],
    "job_name": [
        {"tag": "span", "class_contains": "job-name"},
        {"tag": "h3", "class_contains": "job-title"},
        {"tag": "a", "class_contains": "job-name"},
    ],
    "salary": [
        {"tag": "span", "class_contains": "salary"},
        {"tag": "em", "class_contains": "salary"},
        {"tag": "div", "class_contains": "salary"},
    ],
    "company_name": [
        {"tag": "h3", "class_contains": "company-name"},
        {"tag": "a", "class_contains": "company-name"},
        {"tag": "div", "class_contains": "company-info"},
    ],
    "tag_list": [
        {"tag": "ul", "parent_class_contains": "job-info"},
        {"tag": "div", "class_contains": "tag-list"},
        {"tag": "p", "class_contains": "job-tags"},
    ],
    "job_link": [
        {"tag": "a", "class_contains": "job-card-left"},
        {"tag": "a", "href_starts_with": "/job_detail/"},
    ],
    "welfare": [
        {"tag": "div", "class_contains": "info-desc"},
        {"tag": "div", "class_contains": "welfare"},
        {"tag": "p", "class_contains": "welfare"},
    ]
}

# 薪资解析正则表达式（解决边界处理异常问题）
SALARY_PATTERNS = {
    "range": re.compile(r'^(\d+\.?\d*)\s*[-~至]\s*(\d+\.?\d*)\s*([Kk万万薪元])'),
    "fixed": re.compile(r'^(\d+\.?\d*)\s*([Kk万万薪元])'),
    "bonus": re.compile(r'(\d+)\s*薪'),
    "daily": re.compile(r'(\d+\.?\d*)\s*元\s*/\s*天'),
    "hourly": re.compile(r'(\d+\.?\d*)\s*元\s*/\s*小时'),
}

# ==================== 🔧 通用工具函数 ====================
def fuzzy_find_tag(parent: Tag, rules: List[Dict]) -> Optional[Tag]:
    """多规则模糊匹配标签，平台页面改动仍可正常解析，前序规则失效自动降级"""
    if not parent or not rules:
        return None
    
    for rule in rules:
        try:
            tag_name = rule.get("tag", "*")
            candidates = parent.find_all(tag_name)
            
            for candidate in candidates:
                match = True
                # 匹配属性
                if "attrs" in rule:
                    for attr, need_exist in rule["attrs"].items():
                        if need_exist and not candidate.get(attr):
                            match = False
                            break
                # 模糊匹配class
                if "class_contains" in rule:
                    class_list = candidate.get("class", [])
                    if not any(rule["class_contains"] in cls for cls in class_list):
                        match = False
                # 匹配href前缀
                if "href_starts_with" in rule:
                    href = candidate.get("href", "")
                    if not href.startswith(rule["href_starts_with"]):
                        match = False
                # 匹配父节点class
                if "parent_class_contains" in rule:
                    parent_tag = candidate.parent
                    if not parent_tag:
                        match = False
                    else:
                        parent_class = parent_tag.get("class", [])
                        if not any(rule["parent_class_contains"] in cls for cls in parent_class):
                            match = False
                
                if match:
                    return candidate
        except Exception:
            continue
    return None

def extract_experience_education(tags: List[str]) -> Tuple[str, str]:
    """自动识别经验和学历，不固定标签顺序，彻底解决平台标签顺序改动失效问题"""
    experience = ""
    education = ""
    exp_keywords = ["年", "应届", "经验"]
    edu_keywords = ["本科", "大专", "硕士", "博士", "中专", "高中", "学历"]

    for tag in tags:
        tag_clean = tag.strip()
        if any(kw in tag_clean for kw in exp_keywords) and not experience:
            experience = tag_clean
        if any(kw in tag_clean for kw in edu_keywords) and not education:
            education = tag_clean
    return experience, education

def load_city_map(config: Dict) -> Dict[int, str]:
    """加载城市映射表，优先从json文件加载，无文件则用默认值，支持动态拉取"""
    # 优先动态拉取
    if config["auto_fetch_city_map"]:
        try:
            return fetch_boss_city_map()
        except Exception as e:
            print(f"⚠️  动态拉取城市列表失败，使用本地配置: {e}")
    
    # 从json文件加载
    map_file = config["city_map_file"]
    if os.path.exists(map_file):
        try:
            with open(map_file, "r", encoding="utf-8") as f:
                city_map = json.load(f)
                # 转换key为int类型
                return {int(k): v for k, v in city_map.items()}
        except Exception as e:
            print(f"⚠️  城市映射文件加载失败，使用默认配置: {e}")
    
    # 默认城市映射
    return {
        101250100: "成都",
        101270100: "重庆",
        101280100: "广州",
        101280600: "深圳"
    }

def fetch_boss_city_map() -> Dict[int, str]:
    """从BOSS直聘公开API动态获取全量城市列表，无需手动维护城市ID"""
    session = Session()
    session.headers.update({"User-Agent": random.choice(UA_POOL)})
    resp = session.get("https://www.zhipin.com/wapi/zpCommon/data/city.json", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    
    if data.get("code") != 0:
        raise Exception(f"BOSS城市API返回错误: {data.get('message')}")
    
    city_map = {}
    # 解析热门城市+全部城市
    for city_group in data["zpData"]["cityList"]:
        for city in city_group["subLevelModelList"]:
            city_map[city["code"]] = city["name"]
    print(f"✅ 动态拉取到{len(city_map)}个城市数据")
    return city_map

# ==================== 🕷️ BOSS直聘爬虫类（健壮性升级） ====================
class BossSpider:
    def __init__(self, config: Dict):
        self.config = config
        self.city_map = load_city_map(config)
        self.session = self._init_session()
        self.current_delay = config["request_delay"]  # 动态延时，支持指数退避

    def _init_session(self) -> Session:
        """初始化带重试、指数退避、自动UA切换、代理支持的会话，解决请求失败问题"""
        session = Session()
        
        # 指数退避重试策略
        retry_strategy = Retry(
            total=self.config["max_retry"],
            backoff_factor=2,  # 指数退避：1s → 2s → 4s
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # 代理配置
        if self.config["proxy_enable"]:
            proxies = {
                "http": os.getenv("HTTP_PROXY", ""),
                "https": os.getenv("HTTPS_PROXY", "")
            }
            session.proxies.update(proxies)
            print("✅ 代理已启用")

        # 初始化基础请求头
        self._refresh_headers(session)
        return session

    def _refresh_headers(self, session: Session):
        """每次请求刷新请求头，随机切换UA，降低反爬概率"""
        session.headers.update({
            "User-Agent": random.choice(UA_POOL),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })

    def _request_with_anti_crawl(self, url: str) -> Optional[str]:
        """带反爬检测的请求方法，触发安全验证自动指数退避，解决IP异常问题"""
        for retry in range(self.config["max_retry"] + 1):
            try:
                # 刷新UA，添加随机抖动延时
                self._refresh_headers(self.session)
                delay = self.current_delay + random.random() * 2
                time.sleep(delay)

                resp = self.session.get(url, timeout=15)
                resp.raise_for_status()
                html = resp.text

                # 反爬检测
                if any(keyword in html for keyword in ANTI_CRAWL_KEYWORDS):
                    raise Exception(f"触发反爬安全验证，重试次数: {retry+1}")
                
                # 请求成功，重置延时为基础值
                self.current_delay = self.config["request_delay"]
                return html

            except Exception as e:
                print(f"⚠️  请求失败: {e}，URL: {url}")
                # 触发反爬，指数退避增加延时
                self.current_delay *= 2
                # 最后一次重试失败，返回None
                if retry == self.config["max_retry"]:
                    print(f"❌  达到最大重试次数，跳过当前请求")
                    return None
        return None

    def parse_salary(self, salary_str: str) -> float:
        """
        重写薪资解析，使用正则精准提取，解决边界异常问题
        支持：15-25K·14薪、1.5-2.5万、300元/天、15K·年终奖等全格式
        异常格式返回0，不会抛出中断程序
        """
        if not salary_str or not isinstance(salary_str, str):
            return 0
        salary_str = salary_str.strip()
        try:
            # 处理日薪
            daily_match = SALARY_PATTERNS["daily"].search(salary_str)
            if daily_match:
                daily = float(daily_match.group(1))
                return daily * 22  # 按每月22个工作日换算月薪

            # 处理时薪
            hourly_match = SALARY_PATTERNS["hourly"].search(salary_str)
            if hourly_match:
                hourly = float(hourly_match.group(1))
                return hourly * 8 * 22  # 按每天8小时，每月22天换算

            # 拆分薪资主体和年终奖
            main_part = salary_str
            bonus_month = 12
            if '·' in salary_str:
                main_part, bonus_part = salary_str.split('·', 1)
                # 正则提取薪数，无匹配则默认12薪，不会报错
                bonus_match = SALARY_PATTERNS["bonus"].search(bonus_part)
                if bonus_match:
                    bonus_month = int(bonus_match.group(1))
                main_part = main_part.strip()

            # 处理范围薪资
            range_match = SALARY_PATTERNS["range"].search(main_part)
            if range_match:
                low = float(range_match.group(1))
                high = float(range_match.group(2))
                unit = range_match.group(3)
                # 单位换算
                multiplier = 1000 if unit.lower() == 'k' else 10000 if unit in ['万', '万万'] else 1
                avg_monthly = (low + high) / 2 * multiplier
                # 换算成年均月薪
                return round(avg_monthly * bonus_month / 12, 2)

            # 处理固定薪资
            fixed_match = SALARY_PATTERNS["fixed"].search(main_part)
            if fixed_match:
                fixed = float(fixed_match.group(1))
                unit = fixed_match.group(2)
                multiplier = 1000 if unit.lower() == 'k' else 10000 if unit in ['万', '万万'] else 1
                avg_monthly = fixed * multiplier
                return round(avg_monthly * bonus_month / 12, 2)

            # 无匹配格式返回0
            return 0
        except Exception as e:
            print(f"⚠️  薪资解析异常: {e}，原始字符串: {salary_str}")
            return 0

    def _parse_job_card(self, job_card: Tag, city_name: str) -> Optional[Dict]:
        """解析单个职位卡片，单节点解析失败不中断整体任务，多规则降级匹配"""
        try:
            rules = JOB_PARSE_RULES
            job_info = {"city": city_name}

            # 提取岗位唯一ID（核心去重标识）
            job_info["id"] = job_card.get("data-jobid", "")
            if not job_info["id"]:
                return None

            # 多规则匹配各字段
            job_name_tag = fuzzy_find_tag(job_card, rules["job_name"])
            job_info["job_name"] = job_name_tag.get_text(strip=True) if job_name_tag else ""

            salary_tag = fuzzy_find_tag(job_card, rules["salary"])
            job_info["salary_str"] = salary_tag.get_text(strip=True) if salary_tag else ""
            job_info["salary"] = self.parse_salary(job_info["salary_str"])

            company_tag = fuzzy_find_tag(job_card, rules["company_name"])
            job_info["company"] = company_tag.get_text(strip=True) if company_tag else ""

            link_tag = fuzzy_find_tag(job_card, rules["job_link"])
            job_info["link"] = "https://www.zhipin.com" + link_tag.get("href") if link_tag and link_tag.get("href") else ""

            welfare_tag = fuzzy_find_tag(job_card, rules["welfare"])
            job_info["welfare"] = welfare_tag.get_text(strip=True) if welfare_tag else ""

            # 提取经验和学历标签
            tag_list_tag = fuzzy_find_tag(job_card, rules["tag_list"])
            if tag_list_tag:
                tags = [li.get_text(strip=True) for li in tag_list_tag.find_all("li")]
                job_info["tags"] = tags
                job_info["experience"], job_info["education"] = extract_experience_education(tags)
            else:
                job_info["tags"] = []
                job_info["experience"] = ""
                job_info["education"] = ""

            return job_info
        except Exception as e:
            print(f"⚠️  单个岗位解析失败: {e}")
            return None

    def crawl_city(self, city_id: int) -> List[Dict]:
        """爬取单个城市的职位数据，单页面失败不中断整体爬取"""
        city_name = self.city_map.get(city_id, f"未知城市{city_id}")
        print(f"📡 开始爬取 {city_name} 职位...")
        all_jobs = []

        for page in range(1, self.config["max_pages"] + 1):
            # 构建搜索URL
            query = urllib.parse.quote(','.join(self.config["job_keywords"]))
            url = f"https://www.zhipin.com/web/geek/job?query={query}&city={city_id}&page={page}"
            
            # 请求页面
            html = self._request_with_anti_crawl(url)
            if not html:
                print(f"❌ {city_name} 第{page}页获取失败，跳过")
                continue

            # 解析页面
            try:
                soup = BeautifulSoup(html, "lxml")
                # 多规则匹配职位列表
                job_list = None
                for list_rule in JOB_PARSE_RULES["job_list"]:
                    job_list = soup.find_all(list_rule["tag"], list_rule.get("attrs", {}))
                    if job_list:
                        break
                
                if not job_list:
                    print(f"ℹ️ {city_name} 第{page}页无更多职位，结束爬取")
                    break

                # 解析所有职位卡片
                page_jobs = []
                for job_card in job_list:
                    parsed_job = self._parse_job_card(job_card, city_name)
                    if parsed_job:
                        page_jobs.append(parsed_job)
                
                all_jobs.extend(page_jobs)
                print(f"✅ {city_name} 第{page}页爬取完成，获取{len(page_jobs)}个有效职位")

            except Exception as e:
                print(f"❌ {city_name} 第{page}页解析失败: {e}")
                continue

        print(f"📊 {city_name} 爬取完成，累计获取{len(all_jobs)}个职位\n")
        return all_jobs

    def crawl_all_cities(self) -> List[Dict]:
        """爬取所有配置城市的职位"""
        all_jobs = []
        for city_id in self.config["city_ids"]:
            city_jobs = self.crawl_city(city_id)
            all_jobs.extend(city_jobs)
        return all_jobs

# ==================== 🧹 数据处理器（并发安全升级） ====================
class DataProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.history_file = "history.json"
        self.history = self.load_history()

    def load_history(self) -> set:
        """带文件锁的历史记录加载，解决并发读取冲突，异常自动兜底"""
        if not os.path.exists(self.history_file):
            return set()
        
        try:
            with open(self.history_file, "r", encoding="utf-8") as f:
                # 加共享锁，并发读取安全
                if LOCK_ENABLED:
                    portalocker.lock(f, portalocker.LOCK_SH)
                data = json.load(f)
                return set(data)
        except Exception as e:
            print(f"⚠️  历史记录加载失败，使用空集合: {e}")
            return set()

    def save_history(self):
        """带排他文件锁的历史记录保存，彻底解决并发写入覆盖/丢失问题"""
        try:
            # 用w+模式，确保文件不存在时创建
            with open(self.history_file, "w+", encoding="utf-8") as f:
                # 加排他锁，写入时禁止其他进程读写
                if LOCK_ENABLED:
                    portalocker.lock(f, portalocker.LOCK_EX)
                # 先清空文件，再写入新数据
                f.seek(0)
                f.truncate()
                json.dump(list(self.history), f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())  # 强制刷入磁盘，确保数据不丢失
            print("✅ 历史去重记录已安全保存")
        except Exception as e:
            print(f"❌ 历史记录保存失败: {e}")

    def filter_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """全维度过滤，新增学历、工作经验精准筛选"""
        filtered = []
        config = self.config

        for job in jobs:
            # 1. 去重：已推送过的直接跳过
            if job["id"] in self.history:
                continue

            # 2. 薪资过滤
            if job["salary"] < config["min_salary"]:
                continue

            # 3. 排除关键词过滤
            full_text = f"{job['job_name']} {job['company']} {job.get('welfare', '')}"
            if any(kw in full_text for kw in config["exclude_keywords"]):
                continue

            # 4. 岗位关键词匹配
            if not any(kw in job["job_name"] for kw in config["job_keywords"]):
                continue

            # 5. 学历过滤
            edu_allow = config["education_allow"]
            edu_deny = config["education_deny"]
            job_edu = job.get("education", "")
            if edu_allow and not any(edu in job_edu for edu in edu_allow):
                continue
            if edu_deny and any(edu in job_edu for edu in edu_deny):
                continue

            # 6. 工作经验过滤
            exp_allow = config["experience_allow"]
            exp_deny = config["experience_deny"]
            job_exp = job.get("experience", "")
            if exp_allow and not any(exp in job_exp for exp in exp_allow):
                continue
            if exp_deny and any(exp in job_exp for exp in exp_deny):
                continue

            filtered.append(job)

        return filtered

    def score_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """智能打分排序，选出最优岗位"""
        if not jobs:
            return []
        
        scored_jobs = []
        max_salary = max(job["salary"] for job in jobs)
        config = self.config

        for job in jobs:
            score = 0
            # 1. 薪资权重（40分）
            score += (job["salary"] / max_salary) * 40

            # 2. 关键词匹配权重（30分）
            match_count = sum(1 for kw in config["job_keywords"] if kw in job["job_name"])
            score += min(match_count * 6, 30)

            # 3. 福利权重（20分）
            welfare = job.get("welfare", "")
            if "双休" in welfare or "周末双休" in welfare:
                score += 10
            if "五险一金" in welfare:
                score += 10

            # 4. 经验匹配权重（10分）
            exp = job.get("experience", "")
            if any(target_exp in exp for target_exp in config["experience_allow"]):
                score += 10

            job["score"] = round(score, 2)
            scored_jobs.append(job)

        # 按得分降序，同分按薪资降序
        scored_jobs.sort(key=lambda x: (x["score"], x["salary"]), reverse=True)
        return scored_jobs

# ==================== 📢 钉钉推送器（修复43002报错） ====================
class Notifier:
    def __init__(self, config: Dict):
        self.config = config
        self.webhook = config["dingtalk_webhook"]
        self.secret = config["dingtalk_secret"]

    def _generate_dingtalk_sign(self) -> Tuple[str, int]:
        """生成钉钉加签签名，解决安全设置导致的推送失败"""
        if not self.secret:
            return "", 0
        timestamp = round(time.time() * 1000)
        secret_enc = self.secret.encode("utf-8")
        string_to_sign = f"{timestamp}\n{self.secret}"
        string_to_sign_enc = string_to_sign.encode("utf-8")
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return sign, timestamp

    def send_dingtalk(self, jobs: List[Dict]):
        """修复钉钉43002报错，添加正确请求头，支持加签，完善错误处理"""
        if not self.webhook:
            print("ℹ️  未配置钉钉Webhook，跳过推送")
            return
        
        if not jobs:
            print("ℹ️  无符合条件的岗位，跳过推送")
            return

        # 构建Markdown消息
        content = f"# 🎯 今日最优职位推送 (Top {len(jobs)})\n\n"
        for idx, job in enumerate(jobs, 1):
            content += f"### {idx}. {job['job_name']} - {job['company']}\n"
            content += f"- 💰 薪资：{job['salary_str']}\n"
            content += f"- 📍 城市：{job['city']}\n"
            content += f"- 📝 经验：{job.get('experience', '不限')} | 学历：{job.get('education', '不限')}\n"
            content += f"- 🎁 福利：{job.get('welfare', '无')}\n"
            content += f"- 🔗 直达链接：[点击投递]({job['link']})\n\n"

        # 处理加签
        sign, timestamp = self._generate_dingtalk_sign()
        request_url = self.webhook
        if sign and timestamp:
            request_url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"

        # 严格符合钉钉API要求的请求头和数据，修复43002 POST请求错误
        request_headers = {"Content-Type": "application/json;charset=utf-8"}
        request_data = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"今日职位推送 Top {len(jobs)}",
                "text": content
            }
        }

        # 发送请求
        try:
            resp = Session().post(request_url, json=request_data, headers=request_headers, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") == 0:
                print(f"✅ 钉钉推送成功，共推送{len(jobs)}个岗位")
            else:
                print(f"❌ 钉钉推送失败，错误码：{result.get('errcode')}，错误信息：{result.get('errmsg')}")
        except Exception as e:
            print(f"❌ 钉钉推送异常：{e}")

# ==================== 🚀 主程序入口 ====================
def main():
    print("="*50)
    print("🚀 Job Scanner v1.1 健壮性升级版本 启动运行")
    print("="*50 + "\n")

    # 初始化所有模块
    spider = BossSpider(CONFIG)
    processor = DataProcessor(CONFIG)
    notifier = Notifier(CONFIG)

    # 1. 全量爬取职位
    all_jobs = spider.crawl_all_cities()
    print(f"📊 爬取完成，共获取原始岗位 {len(all_jobs)} 个")

    # 2. 全维度过滤
    filtered_jobs = processor.filter_jobs(all_jobs)
    print(f"✅ 过滤完成，剩余符合条件的新岗位 {len(filtered_jobs)} 个")

    if not filtered_jobs:
        print("ℹ️  无符合条件的新岗位，程序结束")
        print("="*50)
        return

    # 3. 智能打分排序，取Top N
    scored_jobs = processor.score_jobs(filtered_jobs)
    top_jobs = scored_jobs[:CONFIG["top_n"]]
    print(f"🎯 排序完成，选出 Top {len(top_jobs)} 最优岗位\n")

    # 4. 推送到钉钉
    notifier.send_dingtalk(top_jobs)

    # 5. 更新历史去重记录
    for job in top_jobs:
        processor.history.add(job["id"])
    processor.save_history()

    print("\n" + "="*50)
    print("🎉 程序运行完成！")
    print("="*50)

if __name__ == "__main__":
    main()
