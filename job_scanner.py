#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import time
import random
import hmac
import hashlib
import base64
import urllib.parse
from typing import List, Dict, Optional, Callable
from requests import Session, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag

# ==================== 🔒 核心配置区 - 所有自定义修改均在此处 ====================
CONFIG = {
    # -------------------------- 基础爬取配置 --------------------------
    # 目标城市：BOSS直聘城市ID（默认：成都、重庆、广州、深圳）
    "city_ids": [101250100, 101270100, 101280100, 101280600],
    # 爬取最大页数（最新职位集中在前3页，无需过大）
    "max_pages": 3,
    # 请求基础间隔（秒），自动叠加随机抖动，避免反爬
    "request_delay": 3,
    # 单页面最大重试次数
    "max_retry": 3,

    # -------------------------- 岗位匹配核心配置 --------------------------
    # 目标岗位关键词（命中任意一个即保留）
    "job_keywords": ["财务", "会计", "出纳", "审计", "税务", "CFA", "FRM", "财务分析", "预算", "成本"],
    # 排除关键词（岗位/公司名/福利命中任意一个直接过滤）
    "exclude_keywords": ["外包", "培训", "兼职", "实习", "猎头", "派遣", "代招", "顾问", "销售"],
    # 最低期望月薪（单位：元）
    "min_salary": 10000,
    # 最终推送最优岗位数量
    "top_n": 15,

    # -------------------------- 新增：学历筛选配置 --------------------------
    # 学历白名单（命中任意一个即保留，空列表=不限制）
    "education_allow": ["本科", "硕士", "博士"],
    # 学历黑名单（命中任意一个直接过滤，空列表=不限制）
    "education_deny": ["中专", "高中", "大专"],

    # -------------------------- 新增：工作经验筛选配置 --------------------------
    # 经验白名单（命中任意一个即保留，空列表=不限制）
    "experience_allow": ["1-3年", "3-5年", "5-10年"],
    # 经验黑名单（命中任意一个直接过滤，空列表=不限制）
    "experience_deny": ["应届毕业生", "1年以内", "10年以上", "经验不限"],

    # -------------------------- 钉钉推送配置（从GitHub Secrets读取，禁止写死） --------------------------
    "dingtalk_webhook": os.getenv("DINGTALK_WEBHOOK", ""),
    # 钉钉机器人加签密钥（机器人安全设置开启加签时填写，无则留空）
    "dingtalk_secret": os.getenv("DINGTALK_SECRET", ""),
}

# -------------------------- 🔧 平台解析规则配置（页面改动仅改此处，无需动核心代码） --------------------------
# 多特征匹配规则：放弃单一class，用「属性+文本+结构」组合匹配，抗页面改动能力拉满
PARSER_RULES = {
    "boss_zhipin": {
        # 职位列表匹配规则：多条件或匹配，命中任意一个即可
        "job_list": [
            {"tag": "li", "attrs": {"data-jobid": True}},  # 优先匹配带jobid的节点（最稳定）
            {"tag": "li", "class_contains": "job-card"},   # 模糊匹配包含job-card的class
            {"tag": "div", "class_contains": "job-item"},  # 降级匹配
        ],
        # 字段解析规则：支持多规则降级匹配，前一个失效自动用后一个
        "fields": {
            "job_id": {"attrs": {"data-jobid": True}},  # 岗位唯一ID（最稳定）
            "job_name": [
                {"tag": "span", "text_contains": CONFIG["job_keywords"]},  # 优先匹配含目标关键词的节点
                {"tag": "span", "class_contains": "job-name"},
                {"tag": "h3", "class_contains": "title"},
            ],
            "salary_str": [
                {"tag": "span", "text_contains": ["K", "万", "薪"]},  # 优先匹配薪资特征文本
                {"tag": "span", "class_contains": "salary"},
            ],
            "company_name": [
                {"tag": "h3", "class_contains": "company-name"},
                {"tag": "a", "class_contains": "company"},
                {"tag": "div", "class_contains": "company-info"},
            ],
            "job_link": {"tag": "a", "attrs": {"href": True}, "href_starts_with": "/job_detail/"},
            "welfare": [
                {"tag": "div", "class_contains": "info-desc"},
                {"tag": "div", "class_contains": "welfare"},
            ],
            # 经验/学历标签：自动识别，不固定顺序，彻底解决标签顺序改动失效问题
            "tag_list": [
                {"tag": "ul", "parent_class_contains": "job-info"},
                {"tag": "div", "class_contains": "tag-list"},
            ]
        },
        # 反爬检测规则：命中则判定为安全验证，终止解析
        "anti_crawl_detect": [
            "安全验证",
            "异常访问",
            "请完成以下验证",
            "IP地址存在异常",
        ]
    }
}
# ==============================================================================

# 随机UA池，每次请求自动切换，降低反爬拦截概率
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0 Safari/537.36",
]

# ==================== 🔨 基础工具类：通用能力封装，与平台无关 ====================
class BaseUtils:
    @staticmethod
    def parse_salary(salary_str: str) -> float:
        """通用薪资解析，兼容所有平台格式，异常返回0不中断流程"""
        if not salary_str:
            return 0
        try:
            salary_str = salary_str.strip()
            multiplier = 1
            bonus_month = 12

            # 处理单位
            if 'K' in salary_str:
                salary_str = salary_str.replace('K', '')
                multiplier = 1000
            elif '万' in salary_str:
                salary_str = salary_str.replace('万', '')
                multiplier = 10000

            # 处理年终奖
            if '·' in salary_str:
                salary_part, bonus_part = salary_str.split('·', 1)
                if '薪' in bonus_part:
                    bonus_month = int(''.join(filter(str.isdigit, bonus_part)))
                salary_str = salary_part
            else:
                salary_str = salary_str

            # 处理薪资范围
            if '-' in salary_str:
                low, high = salary_str.split('-', 1)
                low = float(''.join(filter(lambda x: x.isdigit() or x == '.', low)))
                high = float(''.join(filter(lambda x: x.isdigit() or x == '.', high)))
                avg_monthly = (low + high) / 2 * multiplier
            else:
                fixed = float(''.join(filter(lambda x: x.isdigit() or x == '.', salary_str)))
                avg_monthly = fixed * multiplier

            # 换算成标准月薪
            return avg_monthly * bonus_month / 12
        except Exception as e:
            print(f"薪资解析异常: {e}, 原始字符串: {salary_str}")
            return 0

    @staticmethod
    def fuzzy_find_tag(parent: Tag, rules: List[Dict]) -> Optional[Tag]:
        """
        模糊匹配标签：核心抗改动能力
        按规则顺序匹配，命中任意一个即返回，前序规则失效自动降级
        """
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
                    # 匹配文本包含
                    if "text_contains" in rule:
                        text = candidate.get_text(strip=True)
                        keywords = rule["text_contains"]
                        if not any(kw in text for kw in keywords):
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

    @staticmethod
    def extract_experience_education(tags: List[str]) -> tuple[str, str]:
        """
        自动识别经验和学历，不固定标签顺序，彻底解决平台标签顺序改动失效问题
        返回：(经验, 学历)
        """
        experience = ""
        education = ""
        experience_keywords = ["年", "应届", "经验"]
        education_keywords = ["本科", "大专", "硕士", "博士", "中专", "高中", "学历"]

        for tag in tags:
            tag_clean = tag.strip()
            if any(kw in tag_clean for kw in experience_keywords) and not experience:
                experience = tag_clean
            if any(kw in tag_clean for kw in education_keywords) and not education:
                education = tag_clean
        return experience, education

# ==================== 🕷️ 基础爬虫基类：通用爬取逻辑，与平台无关 ====================
class BaseSpider:
    def __init__(self, config: Dict, parser_rules: Dict):
        self.config = config
        self.parser_rules = parser_rules
        self.session = self._init_session()

    def _init_session(self) -> Session:
        """初始化带重试、自动UA切换的会话，解决反爬和连接异常问题"""
        session = Session()
        # 重试策略：网络异常/5xx/403自动重试
        retry_strategy = Retry(
            total=self.config["max_retry"],
            backoff_factor=1,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        # 初始化基础请求头
        self._refresh_headers(session)
        return session

    def _refresh_headers(self, session: Session):
        """刷新请求头，随机切换UA，降低反爬概率"""
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

    def _request(self, url: str) -> Optional[str]:
        """通用请求方法，带反爬检测、异常处理，解决安全验证和解析失败问题"""
        try:
            # 每次请求随机切换UA
            self._refresh_headers(self.session)
            # 随机抖动延时，避免固定频率被反爬
            delay = self.config["request_delay"] + random.random() * 2
            time.sleep(delay)

            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            html = resp.text

            # 反爬检测：命中安全验证规则直接抛出异常
            detect_rules = self.parser_rules["anti_crawl_detect"]
            if any(keyword in html for keyword in detect_rules):
                print(f"⚠️  检测到平台安全验证，跳过当前请求")
                return None
            
            return html
        except HTTPError as e:
            print(f"❌ 请求失败，状态码: {e.response.status_code if e.response else '未知'}, URL: {url}")
            return None
        except Exception as e:
            print(f"❌ 请求异常: {e}, URL: {url}")
            return None

# ==================== 🎯 BOSS直聘平台适配器：仅平台相关逻辑，新增平台仅需新增适配器 ====================
class BossZhipinSpider(BaseSpider):
    def __init__(self, config: Dict, parser_rules: Dict):
        super().__init__(config, parser_rules["boss_zhipin"])
        self.city_map = {
            101250100: "成都",
            101270100: "重庆",
            101280100: "广州",
            101280600: "深圳"
        }

    def _parse_job_card(self, job_card: Tag, city_name: str) -> Optional[Dict]:
        """解析单个职位卡片，单节点解析失败不影响整体"""
        try:
            rules = self.parser_rules["fields"]
            job_info = {"city": city_name}

            # 1. 提取岗位ID（核心去重标识，最稳定）
            job_info["id"] = job_card.get("data-jobid", "")
            if not job_info["id"]:
                return None

            # 2. 提取岗位名称
            job_name_tag = BaseUtils.fuzzy_find_tag(job_card, rules["job_name"])
            job_info["job_name"] = job_name_tag.get_text(strip=True) if job_name_tag else ""

            # 3. 提取薪资
            salary_tag = BaseUtils.fuzzy_find_tag(job_card, rules["salary_str"])
            job_info["salary_str"] = salary_tag.get_text(strip=True) if salary_tag else ""
            job_info["salary"] = BaseUtils.parse_salary(job_info["salary_str"])

            # 4. 提取公司名称
            company_tag = BaseUtils.fuzzy_find_tag(job_card, rules["company_name"])
            job_info["company"] = company_tag.get_text(strip=True) if company_tag else ""

            # 5. 提取岗位链接
            link_tag = BaseUtils.fuzzy_find_tag(job_card, [rules["job_link"]])
            job_info["link"] = "https://www.zhipin.com" + link_tag.get("href") if link_tag else ""

            # 6. 提取福利
            welfare_tag = BaseUtils.fuzzy_find_tag(job_card, rules["welfare"])
            job_info["welfare"] = welfare_tag.get_text(strip=True) if welfare_tag else ""

            # 7. 自动提取经验和学历（不固定顺序，抗改动）
            tag_list_tag = BaseUtils.fuzzy_find_tag(job_card, rules["tag_list"])
            if tag_list_tag:
                tags = [li.get_text(strip=True) for li in tag_list_tag.find_all("li")]
                job_info["tags"] = tags
                job_info["experience"], job_info["education"] = BaseUtils.extract_experience_education(tags)
            else:
                job_info["tags"] = []
                job_info["experience"] = ""
                job_info["education"] = ""

            return job_info
        except Exception as e:
            print(f"⚠️  单个岗位解析失败: {e}")
            return None

    def crawl_single_city(self, city_id: int) -> List[Dict]:
        """爬取单个城市的所有职位"""
        city_name = self.city_map.get(city_id, f"未知城市{city_id}")
        print(f"📡 开始爬取 {city_name} 职位...")
        all_jobs = []

        for page in range(1, self.config["max_pages"] + 1):
            # 构建搜索URL
            query = urllib.parse.quote(','.join(self.config["job_keywords"]))
            url = f"https://www.zhipin.com/web/geek/job?query={query}&city={city_id}&page={page}"
            
            # 请求页面
            html = self._request(url)
            if not html:
                print(f"❌ {city_name} 第{page}页获取失败，跳过")
                continue

            # 解析页面
            try:
                soup = BeautifulSoup(html, "lxml")
                # 多规则匹配职位列表
                job_list = None
                for list_rule in self.parser_rules["job_list"]:
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
            city_jobs = self.crawl_single_city(city_id)
            all_jobs.extend(city_jobs)
        return all_jobs

# ==================== 🧹 数据处理器：过滤、打分、去重，与平台无关 ====================
class DataProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.history_file = "history.json"
        self.history = self._load_history()

    def _load_history(self) -> set:
        """加载历史推送记录，用于去重"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    return set(json.load(f))
            except Exception:
                return set()
        return set()

    def save_history(self):
        """保存历史推送记录"""
        with open(self.history_file, "w", encoding="utf-8") as f:
            json.dump(list(self.history), f, ensure_ascii=False, indent=2)

    def filter_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """全维度过滤，新增学历、工作经验筛选"""
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

            # 5. 新增：学历过滤
            edu_allow = config["education_allow"]
            edu_deny = config["education_deny"]
            job_edu = job.get("education", "")
            if edu_allow and not any(edu in job_edu for edu in edu_allow):
                continue
            if edu_deny and any(edu in job_edu for edu in edu_deny):
                continue

            # 6. 新增：工作经验过滤
            exp_allow = config["experience_allow"]
            exp_deny = config["experience_deny"]
            job_exp = job.get("experience", "")
            if exp_allow and not any(exp in job_exp for exp in exp_allow):
                continue
            if exp_deny and any(exp in job_exp for exp in exp_deny):
                continue

            filtered.append(job)

        return filtered

    def score_and_sort_jobs(self, jobs: List[Dict]) -> List[Dict]:
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

# ==================== 📢 推送通知器：钉钉推送修复，解决43002报错 ====================
class DingTalkNotifier:
    def __init__(self, config: Dict):
        self.webhook = config["dingtalk_webhook"]
        self.secret = config["dingtalk_secret"]

    def _generate_sign(self) -> tuple[str, int]:
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

    def send(self, jobs: List[Dict]):
        """推送最优岗位到钉钉，修复POST请求错误"""
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
        sign, timestamp = self._generate_sign()
        request_url = self.webhook
        if sign and timestamp:
            request_url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"

        # 构建请求数据（严格符合钉钉API要求，修复43002错误）
        request_data = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"今日职位推送 Top {len(jobs)}",
                "text": content
            }
        }
        request_headers = {"Content-Type": "application/json;charset=utf-8"}

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
    print("🚀 Job Scanner v2.0 高适配版 启动运行")
    print("="*50 + "\n")

    # 初始化所有模块
    spider = BossZhipinSpider(CONFIG, PARSER_RULES)
    processor = DataProcessor(CONFIG)
    notifier = DingTalkNotifier(CONFIG)

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
    scored_jobs = processor.score_and_sort_jobs(filtered_jobs)
    top_jobs = scored_jobs[:CONFIG["top_n"]]
    print(f"🎯 排序完成，选出 Top {len(top_jobs)} 最优岗位\n")

    # 4. 推送到钉钉
    notifier.send(top_jobs)

    # 5. 更新历史去重记录
    for job in top_jobs:
        processor.history.add(job["id"])
    processor.save_history()

    print("\n" + "="*50)
    print("🎉 程序运行完成！")
    print("="*50)

if __name__ == "__main__":
    main()
