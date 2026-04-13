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

# ==================== 🔒 跨平台并发文件锁（修复Critical级并发写入风险） ====================
# 双重兼容方案：优先portalocker专业锁，降级到原子文件锁，全平台可用，无依赖也能保证并发安全
LOCK_ENABLED = False
# 优先尝试专业跨平台锁
try:
    import portalocker
    LOCK_ENABLED = True
    LOCK_MODE = "portalocker"
    print("✅ 已启用portalocker专业跨平台文件锁")
except ImportError:
    # 降级方案：原子文件锁（兼容Windows/macOS/Linux全平台，无额外依赖）
    LOCK_ENABLED = True
    LOCK_MODE = "atomic_file"
    print("⚠️  未检测到portalocker，已启用原子文件锁（兼容全平台）")

def acquire_lock(file_path: str, exclusive: bool = True) -> Optional[object]:
    """
    跨平台获取文件锁
    :param file_path: 要锁定的文件路径
    :param exclusive: 是否排他锁（写入用True，读取用False）
    :return: 锁对象，用于后续释放
    """
    if not LOCK_ENABLED:
        return None

    # portalocker专业锁模式
    if LOCK_MODE == "portalocker":
        try:
            # 读取用共享锁，写入用排他锁
            file_mode = "r" if not exclusive else "a+"
            lock_flag = portalocker.LOCK_SH if not exclusive else portalocker.LOCK_EX
            lock_file = open(file_path, file_mode, encoding="utf-8")
            portalocker.lock(lock_file, lock_flag)
            return lock_file
        except Exception as e:
            print(f"⚠️  文件锁获取失败: {e}")
            return None

    # 原子文件锁模式（无依赖兼容方案）
    if LOCK_MODE == "atomic_file":
        lock_path = f"{file_path}.lock"
        max_retry = 5
        # 重试获取锁，避免瞬时冲突
        for _ in range(max_retry):
            try:
                # O_CREAT|O_EXCL 保证原子创建，仅一个进程能成功获取锁
                lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(lock_fd, f"pid:{os.getpid()}|time:{int(time.time())}".encode())
                os.close(lock_fd)
                return lock_path
            except OSError as e:
                if e.errno == errno.EEXIST:
                    # 锁已存在，等待后重试
                    time.sleep(0.5)
                else:
                    print(f"⚠️  原子锁获取异常: {e}")
                    break
        print(f"❌  原子锁获取失败，已重试{max_retry}次")
        return None

def release_lock(lock_obj: Optional[object]):
    """跨平台释放文件锁，保证锁一定会被释放"""
    if not lock_obj or not LOCK_ENABLED:
        return

    # 释放portalocker锁
    if LOCK_MODE == "portalocker" and hasattr(lock_obj, "close"):
        try:
            portalocker.unlock(lock_obj)
            lock_obj.close()
        except Exception:
            pass
        return

    # 释放原子文件锁
    if LOCK_MODE == "atomic_file" and isinstance(lock_obj, str):
        try:
            if os.path.exists(lock_obj):
                os.remove(lock_obj)
        except Exception:
            pass
        return

# ==================== 🔧 核心配置区 - 所有自定义修改均在此处 ====================
CONFIG = {
    # -------------------------- 基础爬取配置 --------------------------
    "city_ids": [101250100, 101270100, 101280100, 101280600],  # 成都/重庆/广州/深圳
    "max_pages": 3,  # 单城市最大爬取页数
    "request_delay": 3,  # 基础请求间隔（秒）
    "max_retry": 3,  # 单请求最大重试次数
    "top_n": 15,  # 最终推送最优岗位数量

    # -------------------------- 岗位匹配核心配置 --------------------------
    "job_keywords": ["财务", "会计", "出纳", "审计", "税务", "CFA", "FRM", "财务分析", "预算", "成本"],
    "exclude_keywords": ["外包", "培训", "兼职", "实习", "猎头", "派遣", "代招", "顾问", "销售"],
    "min_salary": 10000,  # 最低期望月薪（元）

    # -------------------------- 学历&工作经验筛选配置 --------------------------
    "education_allow": ["本科", "硕士", "博士"],
    "education_deny": ["中专", "高中", "大专"],
    "experience_allow": ["1-3年", "3-5年", "5-10年"],
    "experience_deny": ["应届毕业生", "1年以内", "10年以上", "经验不限"],

    # -------------------------- 钉钉推送配置 --------------------------
    "dingtalk_webhook": os.getenv("DINGTALK_WEBHOOK", ""),
    "dingtalk_secret": os.getenv("DINGTALK_SECRET", ""),

    # -------------------------- 代理配置 --------------------------
    "proxy_enable": os.getenv("PROXY_ENABLE", "false").lower() == "true",

    # -------------------------- 城市映射配置 --------------------------
    "city_map_file": "city_map.json",  # 外部城市映射文件
    "auto_fetch_city_map": False,  # 是否自动从BOSS API拉取全量城市列表
}
# ==============================================================================

# -------------------------- 全局常量（无需修改） --------------------------
# 随机UA池，每次请求自动切换
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0 Safari/537.36",
]

# 反爬检测关键词
ANTI_CRAWL_KEYWORDS = ["安全验证", "异常访问", "请完成以下验证", "IP地址存在异常", "访问过于频繁"]

# 岗位解析多规则降级匹配（抗页面改动）
BOSS_PARSE_RULES = {
    "job_list": [
        {"tag": "li", "attrs": {"data-jobid": True}},
        {"tag": "li", "class_contains": "job-card-wrapper"},
        {"tag": "li", "class_contains": "job-card"},
        {"tag": "div", "class_contains": "job-item"},
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

# 薪资解析正则（修复边界异常）
SALARY_PATTERNS = {
    "range": re.compile(r'^(\d+\.?\d*)\s*[-~至]\s*(\d+\.?\d*)\s*([Kk万薪元])'),
    "fixed": re.compile(r'^(\d+\.?\d*)\s*([Kk万薪元])'),
    "bonus": re.compile(r'(\d+)\s*薪'),
    "daily": re.compile(r'(\d+\.?\d*)\s*元\s*/\s*天'),
    "hourly": re.compile(r'(\d+\.?\d*)\s*元\s*/\s*小时'),
}

# 钉钉推送最大长度限制（官方4096，留296字符安全余量）
DINGTALK_MAX_LENGTH = 3800

# ==================== 🛠️ 通用工具函数 ====================
def fuzzy_find_tag(parent: Tag, rules: List[Dict]) -> Optional[Tag]:
    """多规则模糊匹配标签，平台页面改动自动降级适配"""
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
                
                if match:
                    return candidate
        except Exception:
            continue
    return None

def extract_experience_education(tags: List[str]) -> Tuple[str, str]:
    """自动识别经验和学历，不固定标签顺序，抗页面改动"""
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
    """校验岗位链接有效性，HEAD请求轻量检测，不触发反爬"""
    if not link:
        return False
    try:
        resp = session.head(link, timeout=3, allow_redirects=True)
        # 200正常/302跳登录页 均为有效链接
        return resp.status_code in [200, 302, 403]
    except Exception:
        # 超时/异常不直接判定无效，保留链接
        return True

def fetch_boss_city_map(spider_session: Session) -> Dict[int, str]:
    """修复Critical级问题：复用爬虫反爬会话，带异常兜底，不会崩溃"""
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

        # 反爬检测
        if any(keyword in html for keyword in ANTI_CRAWL_KEYWORDS):
            raise Exception("城市API触发反爬安全验证")
        
        data = json.loads(html)
        if data.get("code") != 0:
            raise Exception(f"API返回错误: {data.get('message')}")
        
        city_map = {}
        for city_group in data["zpData"]["cityList"]:
            for city in city_group["subLevelModelList"]:
                city_map[city["code"]] = city["name"]
        print(f"✅ 动态拉取到{len(city_map)}个城市数据")
        return city_map
    except Exception as e:
        print(f"⚠️  动态拉取城市列表失败，使用默认配置: {e}")
        return default_map

def load_city_map(config: Dict, spider_session: Session) -> Dict[int, str]:
    """加载城市映射表，优先动态拉取→本地json→默认配置，全链路兜底"""
    # 优先动态拉取
    if config["auto_fetch_city_map"]:
        return fetch_boss_city_map(spider_session)
    
    # 从本地json文件加载
    map_file = config["city_map_file"]
    if os.path.exists(map_file):
        try:
            lock_obj = acquire_lock(map_file, exclusive=False)
            with open(map_file, "r", encoding="utf-8") as f:
                city_map = json.load(f)
                release_lock(lock_obj)
                return {int(k): v for k, v in city_map.items()}
        except Exception as e:
            print(f"⚠️  城市映射文件加载失败，使用默认配置: {e}")
            release_lock(lock_obj)
    
    # 兜底默认配置
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
        self.current_delay = config["request_delay"]  # 动态指数退避延时
        self.city_map = load_city_map(config, self.session)  # 修复：复用反爬会话

    def _init_session(self) -> Session:
        """初始化带反爬、重试、代理的会话，全链路异常兜底"""
        session = Session()
        
        # 指数退避重试策略
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
        """每次请求刷新UA，降低反爬概率"""
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
        """带反爬检测的请求，触发验证自动指数退避"""
        for retry in range(self.config["max_retry"] + 1):
            try:
                self._refresh_headers(self.session)
                # 随机抖动延时，避免固定频率
                delay = self.current_delay + random.random() * 2
                time.sleep(delay)

                resp = self.session.get(url, timeout=15)
                resp.raise_for_status()
                html = resp.text

                # 反爬检测
                if any(keyword in html for keyword in ANTI_CRAWL_KEYWORDS):
                    raise Exception("触发反爬安全验证")
                
                # 请求成功，重置延时
                self.current_delay = self.config["request_delay"]
                return html

            except Exception as e:
                print(f"⚠️  请求失败: {e}，重试次数: {retry+1}")
                # 指数退避，翻倍增加延时
                self.current_delay *= 2
                if retry == self.config["max_retry"]:
                    print(f"❌  达到最大重试次数，跳过当前请求")
                    return None
        return None

    def parse_salary(self, salary_str: str) -> float:
        """全格式薪资解析，修复边界异常，无报错兜底"""
        if not salary_str or not isinstance(salary_str, str):
            return 0
        salary_str = salary_str.strip()
        try:
            # 日薪处理
            daily_match = SALARY_PATTERNS["daily"].search(salary_str)
            if daily_match:
                return float(daily_match.group(1)) * 22  # 按月22工作日换算

            # 时薪处理
            hourly_match = SALARY_PATTERNS["hourly"].search(salary_str)
            if hourly_match:
                return float(hourly_match.group(1)) * 8 * 22  # 按8小时/天换算

            # 拆分薪资主体和年终奖
            main_part = salary_str
            bonus_month = 12
            if '·' in salary_str:
                main_part, bonus_part = salary_str.split('·', 1)
                bonus_match = SALARY_PATTERNS["bonus"].search(bonus_part)
                if bonus_match:
                    bonus_month = int(bonus_match.group(1))
                main_part = main_part.strip()

            # 范围薪资处理
            range_match = SALARY_PATTERNS["range"].search(main_part)
            if range_match:
                low = float(range_match.group(1))
                high = float(range_match.group(2))
                unit = range_match.group(3)
                multiplier = 1000 if unit.lower() == 'k' else 10000 if unit == '万' else 1
                avg_monthly = (low + high) / 2 * multiplier
                return round(avg_monthly * bonus_month / 12, 2)

            # 固定薪资处理
            fixed_match = SALARY_PATTERNS["fixed"].search(main_part)
            if fixed_match:
                fixed = float(fixed_match.group(1))
                unit = fixed_match.group(2)
                multiplier = 1000 if unit.lower() == 'k' else 10000 if unit == '万' else 1
                avg_monthly = fixed * multiplier
                return round(avg_monthly * bonus_month / 12, 2)

            return 0
        except Exception as e:
            print(f"⚠️  薪资解析异常: {e}，原始字符串: {salary_str}")
            return 0

    def _parse_job_card(self, job_card: Tag, city_name: str) -> Optional[Dict]:
        """解析单个岗位，新增链接有效性校验，单节点失败不中断"""
        try:
            rules = BOSS_PARSE_RULES
            job_info = {"city": city_name}

            # 岗位唯一ID（核心去重标识）
            job_info["id"] = job_card.get("data-jobid", "")
            if not job_info["id"]:
                return None

            # 多规则匹配字段
            job_name_tag = fuzzy_find_tag(job_card, rules["job_name"])
            job_info["job_name"] = job_name_tag.get_text(strip=True) if job_name_tag else ""

            salary_tag = fuzzy_find_tag(job_card, rules["salary"])
            job_info["salary_str"] = salary_tag.get_text(strip=True) if salary_tag else ""
            job_info["salary"] = self.parse_salary(job_info["salary_str"])

            company_tag = fuzzy_find_tag(job_card, rules["company_name"])
            job_info["company"] = company_tag.get_text(strip=True) if company_tag else ""

            # 岗位链接+有效性校验
            link_tag = fuzzy_find_tag(job_card, rules["job_link"])
            job_info["link"] = "https://www.zhipin.com" + link_tag.get("href") if link_tag and link_tag.get("href") else ""
            if not check_link_valid(self.session, job_info["link"]):
                return None

            # 福利信息
            welfare_tag = fuzzy_find_tag(job_card, rules["welfare"])
            job_info["welfare"] = welfare_tag.get_text(strip=True) if welfare_tag else ""

            # 经验+学历标签
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
        """爬取单个城市，单页面失败不中断整体任务"""
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
                for list_rule in rules["job_list"]:
                    job_list = soup.find_all(list_rule["tag"], list_rule.get("attrs", {}))
                    if job_list:
                        break
                
                if not job_list:
                    print(f"ℹ️ {city_name} 第{page}页无更多职位，结束爬取")
                    break

                # 解析所有岗位
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
        """爬取所有配置城市"""
        all_jobs = []
        for city_id in self.config["city_ids"]:
            city_jobs = self.crawl_city(city_id)
            all_jobs.extend(city_jobs)
        return all_jobs

# ==================== 🧹 数据处理器（并发安全修复版） ====================
class DataProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.history_file = "history.json"
        self.history = self.load_history()

    def load_history(self) -> set:
        """带锁安全加载历史记录，异常兜底不崩溃"""
        if not os.path.exists(self.history_file):
            return set()
        
        lock_obj = None
        try:
            lock_obj = acquire_lock(self.history_file, exclusive=False)
            with open(self.history_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data)
        except Exception as e:
            print(f"⚠️  历史记录加载失败，使用空集合: {e}")
            return set()
        finally:
            release_lock(lock_obj)

    def save_history(self):
        """原子写入+锁保护，彻底解决并发覆盖/数据丢失问题"""
        lock_obj = None
        temp_file = f"{self.history_file}.tmp"
        try:
            lock_obj = acquire_lock(self.history_file, exclusive=True)
            # 先写入临时文件，保证写入完整
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(list(self.history), f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())  # 强制刷入磁盘，断电不丢失
            
            # 原子替换原文件，全平台兼容，不会出现半写文件
            os.replace(temp_file, self.history_file)
            print("✅ 历史去重记录已安全保存")
        except Exception as e:
            print(f"❌ 历史记录保存失败: {e}")
            # 清理临时文件
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
        finally:
            release_lock(lock_obj)

    def filter_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """全维度过滤，含学历/经验筛选"""
        filtered = []
        config = self.config

        for job in jobs:
            # 去重
            if job["id"] in self.history:
                continue
            # 薪资过滤
            if job["salary"] < config["min_salary"]:
                continue
            # 排除关键词过滤
            full_text = f"{job['job_name']} {job['company']} {job.get('welfare', '')}"
            if any(kw in full_text for kw in config["exclude_keywords"]):
                continue
            # 岗位关键词匹配
            if not any(kw in job["job_name"] for kw in config["job_keywords"]):
                continue
            # 学历过滤
            job_edu = job.get("education", "")
            if config["education_allow"] and not any(edu in job_edu for edu in config["education_allow"]):
                continue
            if config["education_deny"] and any(edu in job_edu for edu in config["education_deny"]):
                continue
            # 经验过滤
            job_exp = job.get("experience", "")
            if config["experience_allow"] and not any(exp in job_exp for exp in config["experience_allow"]):
                continue
            if config["experience_deny"] and any(exp in job_exp for exp in config["experience_deny"]):
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
            # 薪资权重40%
            score += (job["salary"] / max_salary) * 40
            # 关键词匹配权重30%
            match_count = sum(1 for kw in config["job_keywords"] if kw in job["job_name"])
            score += min(match_count * 6, 30)
            # 福利权重20%
            welfare = job.get("welfare", "")
            if "双休" in welfare:
                score += 10
            if "五险一金" in welfare:
                score += 10
            # 经验匹配权重10%
            if any(exp in job.get("experience", "") for exp in config["experience_allow"]):
                score += 10

            job["score"] = round(score, 2)
            scored_jobs.append(job)

        # 按得分降序，同分按薪资降序
        scored_jobs.sort(key=lambda x: (x["score"], x["salary"]), reverse=True)
        return scored_jobs

# ==================== 📢 钉钉推送器（修复长度限制问题） ====================
class DingTalkNotifier:
    def __init__(self, config: Dict):
        self.webhook = config["dingtalk_webhook"]
        self.secret = config["dingtalk_secret"]

    def _generate_sign(self) -> Tuple[str, int]:
        """钉钉加签签名生成"""
        if not self.secret:
            return "", 0
        timestamp = round(time.time() * 1000)
        secret_enc = self.secret.encode("utf-8")
        string_to_sign = f"{timestamp}\n{self.secret}"
        string_to_sign_enc = string_to_sign.encode("utf-8")
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return sign, timestamp

    def _build_single_message(self, jobs: List[Dict], total_count: int, page: int, total_page: int) -> str:
        """构建单条推送消息，保证长度不超限"""
        content = f"# 🎯 今日最优职位推送 (Top {total_count}) [{page}/{total_page}]\n\n"
        for idx, job in enumerate(jobs, 1):
            content += f"### {idx}. {job['job_name']} - {job['company']}\n"
            content += f"- 💰 薪资：{job['salary_str']}\n"
            content += f"- 📍 城市：{job['city']}\n"
            content += f"- 📝 经验：{job.get('experience', '不限')} | 学历：{job.get('education', '不限')}\n"
            content += f"- 🎁 福利：{job.get('welfare', '无')}\n"
            content += f"- 🔗 直达链接：[点击投递]({job['link']})\n\n"
        return content

    def _split_jobs(self, jobs: List[Dict]) -> List[List[Dict]]:
        """自动拆分岗位列表，保证单条消息不超长度限制"""
        chunks = []
        current_chunk = []
        current_length = 0

        for job in jobs:
            # 预计算单个岗位的消息长度（留冗余）
            job_content_length = len(f"{job['job_name']}{job['company']}{job['salary_str']}{job['city']}{job.get('experience','')}{job.get('education','')}{job.get('welfare','')}{job['link']}") + 200
            # 超过限制则新建分片
            if current_length + job_content_length > DINGTALK_MAX_LENGTH and current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_length = 0
            current_chunk.append(job)
            current_length += job_content_length
        
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    def send(self, jobs: List[Dict]):
        """修复版推送，自动拆分长消息，不会被截断"""
        if not self.webhook:
            print("ℹ️  未配置钉钉Webhook，跳过推送")
            return
        if not jobs:
            print("ℹ️  无符合条件的岗位，跳过推送")
            return

        # 拆分岗位列表
        job_chunks = self._split_jobs(jobs)
        total_count = len(jobs)
        total_page = len(job_chunks)
        success_count = 0

        # 逐条推送
        for page, chunk in enumerate(job_chunks, 1):
            content = self._build_single_message(chunk, total_count, page, total_page)
            sign, timestamp = self._generate_sign()
            request_url = self.webhook
            if sign and timestamp:
                request_url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"

            request_headers = {"Content-Type": "application/json;charset=utf-8"}
            request_data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"今日职位推送 Top {total_count}",
                    "text": content
                }
            }

            # 发送请求
            try:
                resp = Session().post(request_url, json=request_data, headers=request_headers, timeout=10)
                resp.raise_for_status()
                result = resp.json()
                if result.get("errcode") == 0:
                    success_count += 1
                    print(f"✅ 钉钉推送成功 [{page}/{total_page}]")
                else:
                    print(f"❌ 钉钉推送失败 [{page}/{total_page}]，错误码：{result.get('errcode')}，信息：{result.get('errmsg')}")
            except Exception as e:
                print(f"❌ 钉钉推送异常 [{page}/{total_page}]：{e}")

        if success_count == total_page:
            print(f"🎉 全部{total_page}条消息推送完成，共推送{total_count}个岗位")
        else:
            print(f"⚠️  推送完成，成功{success_count}/{total_page}条")

# ==================== 🚀 主程序入口 ====================
def main():
    print("="*50)
    print("🚀 Job Scanner v1.2 全量修复稳定版 启动运行")
    print("="*50 + "\n")

    # 初始化模块
    spider = BossSpider(CONFIG)
    processor = DataProcessor(CONFIG)
    notifier = DingTalkNotifier(CONFIG)

    # 1. 全量爬取
    all_jobs = spider.crawl_all_cities()
    print(f"📊 爬取完成，共获取原始岗位 {len(all_jobs)} 个")

    # 2. 全维度过滤
    filtered_jobs = processor.filter_jobs(all_jobs)
    print(f"✅ 过滤完成，剩余符合条件的新岗位 {len(filtered_jobs)} 个")

    if not filtered_jobs:
        print("ℹ️  无符合条件的新岗位，程序结束")
        print("="*50)
        return

    # 3. 智能打分排序
    scored_jobs = processor.score_jobs(filtered_jobs)
    top_jobs = scored_jobs[:CONFIG["top_n"]]
    print(f"🎯 排序完成，选出 Top {len(top_jobs)} 最优岗位\n")

    # 4. 钉钉推送
    notifier.send(top_jobs)

    # 5. 更新历史记录
    for job in top_jobs:
        processor.history.add(job["id"])
    processor.save_history()

    print("\n" + "="*50)
    print("🎉 程序运行完成！")
    print("="*50)

if __name__ == "__main__":
    main()
