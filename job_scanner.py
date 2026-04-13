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
import tempfile
import errno
from typing import List, Dict, Optional, Tuple
from requests import Session, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag

# ==================== 🔒 跨平台并发文件锁 ====================
LOCK_ENABLED = False
try:
    import portalocker
    LOCK_ENABLED = True
    LOCK_MODE = "portalocker"
except ImportError:
    LOCK_ENABLED = True
    LOCK_MODE = "atomic_file"

def acquire_lock(file_path: str, exclusive: bool = True) -> Optional[object]:
    if not LOCK_ENABLED:
        return None
    if LOCK_MODE == "portalocker":
        try:
            file_mode = "r" if not exclusive else "a+"
            lock_flag = portalocker.LOCK_SH if not exclusive else portalocker.LOCK_EX
            lock_file = open(file_path, file_mode, encoding="utf-8")
            portalocker.lock(lock_file, lock_flag)
            return lock_file
        except Exception:
            return None
    if LOCK_MODE == "atomic_file":
        lock_path = f"{file_path}.lock"
        max_retry = 5
        for _ in range(max_retry):
            try:
                lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(lock_fd, f"pid:{os.getpid()}|time:{int(time.time())}".encode())
                os.close(lock_fd)
                return lock_path
            except OSError as e:
                if e.errno == errno.EEXIST:
                    time.sleep(0.5)
                else:
                    break
        return None

def release_lock(lock_obj: Optional[object]):
    if not lock_obj or not LOCK_ENABLED:
        return
    if LOCK_MODE == "portalocker" and hasattr(lock_obj, "close"):
        try:
            portalocker.unlock(lock_obj)
            lock_obj.close()
        except Exception:
            pass
        return
    if LOCK_MODE == "atomic_file" and isinstance(lock_obj, str):
        try:
            if os.path.exists(lock_obj):
                os.remove(lock_obj)
        except Exception:
            pass
        return

# ==================== 🔧 核心配置区 ====================
CONFIG = {
    "city_ids": [101250100, 101270100, 101280100, 101280600],
    "max_pages": 2,
    "request_delay": 4,
    "max_retry": 3,
    "top_n": 15,
    "debug_mode": True,
    "test_mode": os.getenv("TEST_MODE", "false").lower() == "true",  # 新增：测试模式开关

    "job_keywords": ["财务", "会计"],
    "exclude_keywords": ["外包", "培训", "猎头", "派遣"],
    "min_salary": 5000,

    "education_allow": ["本科", "大专", "硕士"],
    "education_deny": [],
    "experience_allow": ["1-3年", "3-5年", "经验不限"],
    "experience_deny": ["应届毕业生"],

    "dingtalk_webhook": os.getenv("DINGTALK_WEBHOOK", ""),
    "dingtalk_secret": os.getenv("DINGTALK_SECRET", ""),

    "proxy_enable": os.getenv("PROXY_ENABLE", "false").lower() == "true",
    "city_map_file": "city_map.json",
    "auto_fetch_city_map": False,
}
# ==============================================================================

# -------------------------- 全局常量 --------------------------
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0 Safari/537.36",
]

ANTI_CRAWL_KEYWORDS = ["安全验证", "异常访问", "请完成以下验证", "IP地址存在异常", "访问过于频繁", "请登录"]

BOSS_PARSE_RULES = {
    "job_list": [
        {"tag": "li", "attrs": {"data-jobid": True}},
        {"tag": "div", "attrs": {"data-jobid": True}},
        {"tag": "li", "class_contains": "job-card"},
        {"tag": "div", "class_contains": "job-item"},
    ],
    "job_name": [
        {"tag": "span", "class_contains": "job-name"},
        {"tag": "a", "class_contains": "job-name"},
        {"tag": "h3", "class_contains": "title"},
    ],
    "salary": [
        {"tag": "span", "class_contains": "salary"},
        {"tag": "em", "text_contains": ["K", "万", "薪"]},
    ],
    "company_name": [
        {"tag": "h3", "class_contains": "company-name"},
        {"tag": "a", "class_contains": "company"},
        {"tag": "div", "class_contains": "company-info"},
    ],
    "tag_list": [
        {"tag": "ul", "parent_class_contains": "job-info"},
        {"tag": "div", "class_contains": "tag-list"},
        {"tag": "p", "class_contains": "job-tags"},
    ],
    "job_link": [
        {"tag": "a", "href_starts_with": "/job_detail/"},
        {"tag": "a", "class_contains": "job-card-left"},
    ],
    "welfare": [
        {"tag": "div", "class_contains": "info-desc"},
        {"tag": "div", "class_contains": "welfare"},
        {"tag": "p", "class_contains": "welfare"},
    ]
}

SALARY_PATTERNS = {
    "range": re.compile(r'^(\d+\.?\d*)\s*[-~至]\s*(\d+\.?\d*)\s*([Kk万薪元])'),
    "fixed": re.compile(r'^(\d+\.?\d*)\s*([Kk万薪元])'),
    "bonus": re.compile(r'(\d+)\s*薪'),
    "daily": re.compile(r'(\d+\.?\d*)\s*元\s*/\s*天'),
    "hourly": re.compile(r'(\d+\.?\d*)\s*元\s*/\s*小时'),
}

DINGTALK_MAX_LENGTH = 3800

# ==================== 🧪 测试排错模块（新增核心功能） ====================
class Tester:
    """独立测试排错类，负责配置校验、钉钉推送测试、全链路排错"""
    
    @staticmethod
    def validate_config() -> Tuple[bool, List[str]]:
        """
        配置自动校验
        :return: (是否通过, 错误/警告信息列表)
        """
        passed = True
        messages = []

        # 1. 检查钉钉Webhook配置
        if not CONFIG["dingtalk_webhook"]:
            passed = False
            messages.append("❌ 【严重】未配置DINGTALK_WEBHOOK环境变量")
        else:
            if "oapi.dingtalk.com" not in CONFIG["dingtalk_webhook"]:
                passed = False
                messages.append("❌ 【严重】DINGTALK_WEBHOOK格式错误，必须包含oapi.dingtalk.com")
            else:
                messages.append("✅ 钉钉Webhook配置格式正确")

        # 2. 检查钉钉加签配置（如果有）
        if CONFIG["dingtalk_secret"]:
            if len(CONFIG["dingtalk_secret"]) < 20:
                messages.append("⚠️  【警告】DINGTALK_SECRET长度异常，可能配置错误")
            else:
                messages.append("✅ 钉钉加签密钥配置正确")
        else:
            messages.append("ℹ️  未配置钉钉加签密钥（如果机器人开启了加签，必须配置）")

        # 3. 检查城市配置
        if not CONFIG["city_ids"]:
            passed = False
            messages.append("❌ 【严重】未配置目标城市")
        else:
            messages.append(f"✅ 目标城市配置正确：{len(CONFIG['city_ids'])}个城市")

        # 4. 检查关键词配置
        if not CONFIG["job_keywords"]:
            passed = False
            messages.append("❌ 【严重】未配置岗位关键词")
        else:
            messages.append(f"✅ 岗位关键词配置正确：{CONFIG['job_keywords']}")

        # 5. 检查依赖库
        try:
            import requests
            import bs4
            import lxml
            messages.append("✅ 所有依赖库已正确安装")
        except ImportError as e:
            passed = False
            messages.append(f"❌ 【严重】依赖库缺失：{e}")

        return passed, messages

    @staticmethod
    def generate_test_jobs() -> List[Dict]:
        """生成测试用的模拟岗位数据，用于测试钉钉推送"""
        return [
            {
                "id": "test_001",
                "job_name": "测试岗位-高级财务经理",
                "company": "字节跳动（测试）",
                "salary_str": "30-50K·14薪",
                "salary": 40000,
                "city": "成都",
                "experience": "3-5年",
                "education": "本科",
                "welfare": "五险一金 周末双休 年终奖 股票期权",
                "link": "https://www.zhipin.com/"
            },
            {
                "id": "test_002",
                "job_name": "测试岗位-财务分析师",
                "company": "阿里巴巴（测试）",
                "salary_str": "20-35K·16薪",
                "salary": 30000,
                "city": "重庆",
                "experience": "1-3年",
                "education": "硕士",
                "welfare": "六险一金 弹性工作 免费三餐",
                "link": "https://www.zhipin.com/"
            }
        ]

    @staticmethod
    def test_dingtalk_push() -> Tuple[bool, str]:
        """
        独立测试钉钉推送
        :return: (是否成功, 结果信息)
        """
        if not CONFIG["dingtalk_webhook"]:
            return False, "未配置DINGTALK_WEBHOOK，无法测试"

        print("\n" + "="*60)
        print("🧪 开始钉钉推送测试")
        print("="*60)

        # 生成测试数据
        test_jobs = Tester.generate_test_jobs()
        notifier = DingTalkNotifier(CONFIG)
        
        try:
            # 发送测试消息
            notifier.send(test_jobs, is_test=True)
            return True, "钉钉推送测试消息已发送，请检查钉钉群"
        except Exception as e:
            return False, f"钉钉推送测试失败：{str(e)}"

    @staticmethod
    def run_full_troubleshooting():
        """运行全链路排错，打印完整排错报告"""
        print("\n" + "="*60)
        print("🔍 开始全链路排错检查")
        print("="*60)

        # 1. 配置校验
        print("\n【1/4】配置校验...")
        config_passed, config_messages = Tester.validate_config()
        for msg in config_messages:
            print(f"  {msg}")

        # 2. 环境变量检查
        print("\n【2/4】环境变量检查...")
        env_vars = ["DINGTALK_WEBHOOK", "DINGTALK_SECRET", "PROXY_ENABLE", "HTTP_PROXY", "HTTPS_PROXY", "TEST_MODE"]
        for var in env_vars:
            value = os.getenv(var, "")
            if var == "DINGTALK_WEBHOOK" and value:
                # 隐藏Webhook的关键部分，只显示前缀
                print(f"  ✅ {var}: {value[:30]}...")
            elif value:
                print(f"  ✅ {var}: {value}")
            else:
                print(f"  ℹ️  {var}: 未配置")

        # 3. 网络连通性检查
        print("\n【3/4】网络连通性检查...")
        test_urls = [
            ("https://www.zhipin.com", "BOSS直聘"),
            ("https://oapi.dingtalk.com", "钉钉API")
        ]
        session = Session()
        for url, name in test_urls:
            try:
                resp = session.head(url, timeout=5)
                print(f"  ✅ {name} 网络连通正常 (状态码: {resp.status_code})")
            except Exception as e:
                print(f"  ❌ {name} 网络连通失败: {e}")

        # 4. 钉钉推送测试（可选）
        print("\n【4/4】钉钉推送测试...")
        if CONFIG["test_mode"]:
            test_passed, test_msg = Tester.test_dingtalk_push()
            print(f"  {test_msg}")
        else:
            print("  ℹ️  测试模式未开启，跳过钉钉推送测试")
            print("  💡 提示：在GitHub Actions中选择「测试模式」运行，或设置TEST_MODE=true环境变量")

        print("\n" + "="*60)
        print("✅ 全链路排错检查完成")
        print("="*60)

# ==================== 🛠️ 通用工具函数 ====================
def fuzzy_find_tag(parent: Tag, rules: List[Dict]) -> Optional[Tag]:
    if not parent or not rules:
        return None
    for rule in rules:
        try:
            tag_name = rule.get("tag", "*")
            candidates = parent.find_all(tag_name)
            for candidate in candidates:
                match = True
                if "attrs" in rule:
                    for attr, need_exist in rule["attrs"].items():
                        if need_exist and not candidate.get(attr):
                            match = False
                            break
                if "class_contains" in rule:
                    class_list = candidate.get("class", [])
                    if not any(rule["class_contains"] in cls for cls in class_list):
                        match = False
                if "href_starts_with" in rule:
                    href = candidate.get("href", "")
                    if not href.startswith(rule["href_starts_with"]):
                        match = False
                if "parent_class_contains" in rule:
                    parent_tag = candidate.parent
                    if not parent_tag or not any(rule["parent_class_contains"] in cls for cls in parent_tag.get("class", [])):
                        match = False
                if "text_contains" in rule:
                    text = candidate.get_text(strip=True)
                    if not any(kw in text for kw in rule["text_contains"]):
                        match = False
                if match:
                    return candidate
        except Exception:
            continue
    return None

def extract_experience_education(tags: List[str]) -> Tuple[str, str]:
    experience, education = "", ""
    exp_keywords = ["年", "应届", "经验"]
    edu_keywords = ["本科", "大专", "硕士", "博士", "中专", "高中", "学历"]
    for tag in tags:
        tag_clean = tag.strip()
        if any(kw in tag_clean for kw in exp_keywords) and not experience:
            experience = tag_clean
        if any(kw in tag_clean for kw in edu_keywords) and not education:
            education = tag_clean
    return experience, education

def check_link_valid(session: Session, link: str) -> bool:
    if not link:
        return False
    if CONFIG["debug_mode"] or CONFIG["test_mode"]:
        return True
    try:
        resp = session.get(link, timeout=3, allow_redirects=True, stream=True)
        resp.close()
        return resp.status_code in [200, 302, 403]
    except Exception:
        return True

def fetch_boss_city_map(spider_session: Session) -> Dict[int, str]:
    url = "https://www.zhipin.com/wapi/zpCommon/data/city.json"
    default_map = {
        101250100: "成都",
        101270100: "重庆",
        101280100: "广州",
        101280600: "深圳"
    }
    try:
        resp = spider_session.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text
        if any(keyword in html for keyword in ANTI_CRAWL_KEYWORDS):
            raise Exception("城市API触发反爬安全验证")
        data = json.loads(html)
        if data.get("code") != 0:
            raise Exception(f"API返回错误: {data.get('message')}")
        city_map = {}
        for city_group in data["zpData"]["cityList"]:
            for city in city_group["subLevelModelList"]:
                city_map[city["code"]] = city["name"]
        return city_map
    except Exception as e:
        if CONFIG["debug_mode"]:
            print(f"⚠️  动态拉取城市列表失败: {e}")
        return default_map

def load_city_map(config: Dict, spider_session: Session) -> Dict[int, str]:
    if config["auto_fetch_city_map"]:
        return fetch_boss_city_map(spider_session)
    map_file = config["city_map_file"]
    if os.path.exists(map_file):
        try:
            lock_obj = acquire_lock(map_file, exclusive=False)
            with open(map_file, "r", encoding="utf-8") as f:
                city_map = json.load(f)
                release_lock(lock_obj)
                return {int(k): v for k, v in city_map.items()}
        except Exception as e:
            if CONFIG["debug_mode"]:
                print(f"⚠️  城市映射文件加载失败: {e}")
            release_lock(lock_obj)
    return {
        101250100: "成都",
        101270100: "重庆",
        101280100: "广州",
        101280600: "深圳"
    }

# ==================== 🕷️ BOSS直聘爬虫核心类 ====================
class BossSpider:
    def __init__(self, config: Dict):
        self.config = config
        self.session = self._init_session()
        self.current_delay = config["request_delay"]
        self.city_map = load_city_map(config, self.session)

    def _init_session(self) -> Session:
        session = Session()
        retry_strategy = Retry(
            total=self.config["max_retry"],
            backoff_factor=2,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        if self.config["proxy_enable"]:
            proxies = {
                "http": os.getenv("HTTP_PROXY", ""),
                "https": os.getenv("HTTPS_PROXY", "")
            }
            session.proxies.update(proxies)
            if CONFIG["debug_mode"]:
                print("✅ 代理已启用")
        self._refresh_headers(session)
        return session

    def _refresh_headers(self, session: Session):
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
        for retry in range(self.config["max_retry"] + 1):
            try:
                self._refresh_headers(self.session)
                delay = self.current_delay + random.random() * 2
                time.sleep(delay)
                resp = self.session.get(url, timeout=15)
                resp.raise_for_status()
                html = resp.text

                if self.config["debug_mode"]:
                    print(f"🔍 调试：页面HTML长度：{len(html)}，是否包含岗位：{'data-jobid' in html}")

                if any(keyword in html for keyword in ANTI_CRAWL_KEYWORDS):
                    raise Exception("触发反爬安全验证")
                self.current_delay = self.config["request_delay"]
                return html
            except Exception as e:
                if self.config["debug_mode"]:
                    print(f"⚠️  请求失败: {e}，重试次数: {retry+1}")
                self.current_delay *= 2
                if retry == self.config["max_retry"]:
                    return None
        return None

    def parse_salary(self, salary_str: str) -> float:
        if not salary_str or not isinstance(salary_str, str):
            return 0
        salary_str = salary_str.strip()
        try:
            daily_match = SALARY_PATTERNS["daily"].search(salary_str)
            if daily_match:
                return float(daily_match.group(1)) * 22
            hourly_match = SALARY_PATTERNS["hourly"].search(salary_str)
            if hourly_match:
                return float(hourly_match.group(1)) * 8 * 22
            main_part = salary_str
            bonus_month = 12
            if '·' in salary_str:
                main_part, bonus_part = salary_str.split('·', 1)
                bonus_match = SALARY_PATTERNS["bonus"].search(bonus_part)
                if bonus_match:
                    bonus_month = int(bonus_match.group(1))
                main_part = main_part.strip()
            range_match = SALARY_PATTERNS["range"].search(main_part)
            if range_match:
                low = float(range_match.group(1))
                high = float(range_match.group(2))
                unit = range_match.group(3)
                multiplier = 1000 if unit.lower() == 'k' else 10000 if unit == '万' else 1
                avg_monthly = (low + high) / 2 * multiplier
                return round(avg_monthly * bonus_month / 12, 2)
            fixed_match = SALARY_PATTERNS["fixed"].search(main_part)
            if fixed_match:
                fixed = float(fixed_match.group(1))
                unit = fixed_match.group(2)
                multiplier = 1000 if unit.lower() == 'k' else 10000 if unit == '万' else 1
                avg_monthly = fixed * multiplier
                return round(avg_monthly * bonus_month / 12, 2)
            return 0
        except Exception as e:
            if self.config["debug_mode"]:
                print(f"⚠️  薪资解析异常: {e}，原始字符串: {salary_str}")
            return 0

    def _parse_job_card(self, job_card: Tag, city_name: str) -> Optional[Dict]:
        try:
            rules = BOSS_PARSE_RULES
            job_info = {"city": city_name}
            job_info["id"] = job_card.get("data-jobid", "")
            if not job_info["id"]:
                return None

            if self.config["debug_mode"]:
                print(f"🔍 调试：解析到岗位ID：{job_info['id']}")

            job_name_tag = fuzzy_find_tag(job_card, rules["job_name"])
            job_info["job_name"] = job_name_tag.get_text(strip=True) if job_name_tag else "未知岗位"

            salary_tag = fuzzy_find_tag(job_card, rules["salary"])
            job_info["salary_str"] = salary_tag.get_text(strip=True) if salary_tag else "薪资面议"
            job_info["salary"] = self.parse_salary(job_info["salary_str"])

            company_tag = fuzzy_find_tag(job_card, rules["company_name"])
            job_info["company"] = company_tag.get_text(strip=True) if company_tag else "未知公司"

            link_tag = fuzzy_find_tag(job_card, rules["job_link"])
            job_info["link"] = "https://www.zhipin.com" + link_tag.get("href") if link_tag and link_tag.get("href") else ""
            if not check_link_valid(self.session, job_info["link"]):
                return None

            welfare_tag = fuzzy_find_tag(job_card, rules["welfare"])
            job_info["welfare"] = welfare_tag.get_text(strip=True) if welfare_tag else "无"

            tag_list_tag = fuzzy_find_tag(job_card, rules["tag_list"])
            if tag_list_tag:
                tags = [li.get_text(strip=True) for li in tag_list_tag.find_all("li")]
                job_info["tags"] = tags
                job_info["experience"], job_info["education"] = extract_experience_education(tags)
            else:
                job_info["tags"] = []
                job_info["experience"] = "经验不限"
                job_info["education"] = "学历不限"

            return job_info
        except Exception as e:
            if self.config["debug_mode"]:
                print(f"⚠️  单个岗位解析失败: {e}")
            return None

    def crawl_city(self, city_id: int) -> List[Dict]:
        city_name = self.city_map.get(city_id, f"未知城市{city_id}")
        if self.config["debug_mode"]:
            print(f"\n📡 开始爬取 {city_name} 职位...")
        all_jobs = []

        for page in range(1, self.config["max_pages"] + 1):
            query = urllib.parse.quote(' '.join(self.config["job_keywords"]))
            url = f"https://www.zhipin.com/web/geek/job?query={query}&city={city_id}&page={page}"
            
            if self.config["debug_mode"]:
                print(f"🔗 爬取URL：{url}")

            html = self._request_with_anti_crawl(url)
            if not html:
                if self.config["debug_mode"]:
                    print(f"❌ {city_name} 第{page}页获取失败，跳过")
                continue

            try:
                soup = BeautifulSoup(html, "lxml")
                job_list = None
                for list_rule in BOSS_PARSE_RULES["job_list"]:
                    job_list = soup.find_all(list_rule["tag"], list_rule.get("attrs", {}))
                    if job_list:
                        if self.config["debug_mode"]:
                            print(f"✅ 匹配到岗位列表，数量：{len(job_list)}")
                        break
                
                if not job_list:
                    if self.config["debug_mode"]:
                        print(f"ℹ️ {city_name} 第{page}页无匹配岗位，结束爬取")
                    break

                page_jobs = []
                for job_card in job_list:
                    parsed_job = self._parse_job_card(job_card, city_name)
                    if parsed_job:
                        page_jobs.append(parsed_job)
                
                all_jobs.extend(page_jobs)
                if self.config["debug_mode"]:
                    print(f"✅ {city_name} 第{page}页解析完成，有效岗位：{len(page_jobs)}个")

            except Exception as e:
                if self.config["debug_mode"]:
                    print(f"❌ {city_name} 第{page}页解析失败: {e}")
                continue

        if self.config["debug_mode"]:
            print(f"📊 {city_name} 爬取完成，累计有效岗位：{len(all_jobs)}个")
        return all_jobs

    def crawl_all_cities(self) -> List[Dict]:
        all_jobs = []
        for city_id in self.config["city_ids"]:
            city_jobs = self.crawl_city(city_id)
            all_jobs.extend(city_jobs)
        return all_jobs

# ==================== 🧹 数据处理器 ====================
class DataProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.history_file = "history.json"
        self.history = self.load_history()

    def load_history(self) -> set:
        if not os.path.exists(self.history_file):
            return set()
        lock_obj = None
        try:
            lock_obj = acquire_lock(self.history_file, exclusive=False)
            with open(self.history_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data)
        except Exception as e:
            if self.config["debug_mode"]:
                print(f"⚠️  历史记录加载失败: {e}")
            return set()
        finally:
            release_lock(lock_obj)

    def save_history(self):
        lock_obj = None
        temp_file = f"{self.history_file}.tmp"
        try:
            lock_obj = acquire_lock(self.history_file, exclusive=True)
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(list(self.history), f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, self.history_file)
            if self.config["debug_mode"]:
                print("✅ 历史去重记录已安全保存")
        except Exception as e:
            if self.config["debug_mode"]:
                print(f"❌ 历史记录保存失败: {e}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
        finally:
            release_lock(lock_obj)

    def filter_jobs(self, jobs: List[Dict]) -> List[Dict]:
        filtered = []
        config = self.config
        if config["debug_mode"]:
            print(f"\n🔍 调试：过滤前总岗位数：{len(jobs)}")

        for idx, job in enumerate(jobs):
            if job["id"] in self.history:
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}已推送过，过滤")
                continue
            if job["salary"] < config["min_salary"]:
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}薪资{job['salary']}低于下限，过滤")
                continue
            full_text = f"{job['job_name']} {job['company']} {job.get('welfare', '')}"
            if any(kw in full_text for kw in config["exclude_keywords"]):
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}命中排除关键词，过滤")
                continue
            if not any(kw in job["job_name"] for kw in config["job_keywords"]):
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}未命中目标关键词，过滤")
                continue
            job_edu = job.get("education", "")
            if config["education_allow"] and not any(edu in job_edu for edu in config["education_allow"]):
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}学历{job_edu}不符合，过滤")
                continue
            if config["education_deny"] and any(edu in job_edu for edu in config["education_deny"]):
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}学历{job_edu}命中黑名单，过滤")
                continue
            job_exp = job.get("experience", "")
            if config["experience_allow"] and not any(exp in job_exp for exp in config["experience_allow"]):
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}经验{job_exp}不符合，过滤")
                continue
            if config["experience_deny"] and any(exp in job_exp for exp in config["experience_deny"]):
                if config["debug_mode"]:
                    print(f"🔍 调试：岗位{idx+1}经验{job_exp}命中黑名单，过滤")
                continue

            filtered.append(job)

        if config["debug_mode"]:
            print(f"🔍 调试：过滤后剩余岗位数：{len(filtered)}")
        return filtered

    def score_jobs(self, jobs: List[Dict]) -> List[Dict]:
        if not jobs:
            return []
        scored_jobs = []
        max_salary = max(job["salary"] for job in jobs)
        config = self.config

        for job in jobs:
            score = 0
            score += (job["salary"] / max_salary) * 40
            match_count = sum(1 for kw in config["job_keywords"] if kw in job["job_name"])
            score += min(match_count * 6, 30)
            welfare = job.get("welfare", "")
            if "双休" in welfare:
                score += 10
            if "五险一金" in welfare:
                score += 10
            if any(exp in job.get("experience", "") for exp in config["experience_allow"]):
                score += 10

            job["score"] = round(score, 2)
            scored_jobs.append(job)

        scored_jobs.sort(key=lambda x: (x["score"], x["salary"]), reverse=True)
        return scored_jobs

# ==================== 📢 钉钉推送器（增强版） ====================
class DingTalkNotifier:
    def __init__(self, config: Dict):
        self.webhook = config["dingtalk_webhook"]
        self.secret = config["dingtalk_secret"]
        self.debug_mode = config["debug_mode"]

    def _generate_sign(self) -> Tuple[str, int]:
        if not self.secret:
            return "", 0
        timestamp = round(time.time() * 1000)
        secret_enc = self.secret.encode("utf-8")
        string_to_sign = f"{timestamp}\n{self.secret}"
        string_to_sign_enc = string_to_sign.encode("utf-8")
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return sign, timestamp

    def _build_single_message(self, jobs: List[Dict], total_count: int, page: int, total_page: int, is_test: bool = False) -> str:
        title_prefix = "🧪【测试】" if is_test else "🎯"
        content = f"# {title_prefix} 今日最优职位推送 (Top {total_count}) [{page}/{total_page}]\n\n"
        for idx, job in enumerate(jobs, 1):
            content += f"### {idx}. {job['job_name']} - {job['company']}\n"
            content += f"- 💰 薪资：{job['salary_str']}\n"
            content += f"- 📍 城市：{job['city']}\n"
            content += f"- 📝 经验：{job.get('experience', '不限')} | 学历：{job.get('education', '不限')}\n"
            content += f"- 🎁 福利：{job.get('welfare', '无')}\n"
            content += f"- 🔗 直达链接：[点击投递]({job['link']})\n\n"
        return content

    def _split_jobs(self, jobs: List[Dict]) -> List[List[Dict]]:
        chunks = []
        current_chunk = []
        current_length = 0
        for job in jobs:
            job_content_length = len(f"{job['job_name']}{job['company']}{job['salary_str']}{job['city']}{job.get('experience','')}{job.get('education','')}{job.get('welfare','')}{job['link']}") + 200
            if current_length + job_content_length > DINGTALK_MAX_LENGTH and current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_length = 0
            current_chunk.append(job)
            current_length += job_content_length
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    def send(self, jobs: List[Dict], is_test: bool = False):
        if not self.webhook:
            print("ℹ️  未配置钉钉Webhook，跳过推送")
            return
        if not jobs:
            print("ℹ️  无符合条件的岗位，跳过推送")
            return

        if self.debug_mode:
            print(f"\n🔍 调试：即将推送的岗位：")
            for idx, job in enumerate(jobs, 1):
                print(f"{idx}. {job['job_name']} | {job['company']} | {job['salary_str']} | {job['city']}")

        job_chunks = self._split_jobs(jobs)
        total_count = len(jobs)
        total_page = len(job_chunks)
        success_count = 0

        for page, chunk in enumerate(job_chunks, 1):
            content = self._build_single_message(chunk, total_count, page, total_page, is_test)
            sign, timestamp = self._generate_sign()
            request_url = self.webhook
            if sign and timestamp:
                request_url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"

            request_headers = {"Content-Type": "application/json;charset=utf-8"}
            request_data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"{'【测试】' if is_test else ''}今日职位推送 Top {total_count}",
                    "text": content
                }
            }

            try:
                if self.debug_mode:
                    print(f"🔍 调试：正在发送钉钉消息 [{page}/{total_page}]...")
                
                resp = Session().post(request_url, json=request_data, headers=request_headers, timeout=10)
                resp.raise_for_status()
                result = resp.json()

                if result.get("errcode") == 0:
                    success_count += 1
                    print(f"✅ 钉钉推送成功 [{page}/{total_page}]")
                else:
                    print(f"❌ 钉钉推送失败 [{page}/{total_page}]")
                    print(f"   错误码：{result.get('errcode')}")
                    print(f"   错误信息：{result.get('errmsg')}")
                    # 钉钉常见错误码提示
                    if result.get("errcode") == 310000:
                        print("   💡 提示：可能是机器人安全设置问题，请检查IP白名单、关键词限制或加签配置")
                    elif result.get("errcode") == 40035:
                        print("   💡 提示：缺少timestamp参数，请检查加签配置")
                    elif result.get("errcode") == 40014:
                        print("   💡 提示：签名错误，请检查DINGTALK_SECRET配置")

            except Exception as e:
                print(f"❌ 钉钉推送异常 [{page}/{total_page}]：{e}")

        if success_count == total_page:
            print(f"🎉 全部{total_page}条消息推送完成，共推送{total_count}个岗位")
        else:
            print(f"⚠️  推送完成，成功{success_count}/{total_page}条")

# ==================== 🚀 主程序入口 ====================
def main():
    print("="*60)
    print("🚀 Job Scanner v1.4 测试排错增强版 启动运行")
    print(f"🐛 调试模式：{'开启' if CONFIG['debug_mode'] else '关闭'}")
    print(f"🧪 测试模式：{'开启' if CONFIG['test_mode'] else '关闭'}")
    print("="*60)

    # 如果是测试模式，先运行全链路排错
    if CONFIG["test_mode"]:
        Tester.run_full_troubleshooting()
        print("\n💡 测试模式下，仅运行排错检查和钉钉推送测试，不执行实际爬取")
        print("="*60)
        return

    # 正常模式：先做基础配置校验
    print("\n【前置检查】配置校验...")
    config_passed, config_messages = Tester.validate_config()
    for msg in config_messages:
        print(f"  {msg}")

    if not config_passed:
        print("\n❌ 配置校验未通过，请检查上述错误后重试")
        print("="*60)
        return

    # 正常爬取流程
    spider = BossSpider(CONFIG)
    processor = DataProcessor(CONFIG)
    notifier = DingTalkNotifier(CONFIG)

    all_jobs = spider.crawl_all_cities()
    print(f"\n📊 全量爬取完成，共获取原始岗位 {len(all_jobs)} 个")

    filtered_jobs = processor.filter_jobs(all_jobs)
    print(f"✅ 过滤完成，剩余符合条件的新岗位 {len(filtered_jobs)} 个")

    if not filtered_jobs:
        print("ℹ️  无符合条件的新岗位，程序结束")
        print("="*60)
        return

    scored_jobs = processor.score_jobs(filtered_jobs)
    top_jobs = scored_jobs[:CONFIG["top_n"]]
    print(f"🎯 排序完成，选出 Top {len(top_jobs)} 最优岗位")

    notifier.send(top_jobs)

    for job in top_jobs:
        processor.history.add(job["id"])
    processor.save_history()

    print("\n" + "="*60)
    print("🎉 程序运行完成！")
    print("="*60)

if __name__ == "__main__":
    main()
