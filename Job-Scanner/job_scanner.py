#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import time
import random
import requests
from typing import List, Dict
from bs4 import BeautifulSoup

# ==================== 配置区域 - 你可以在这里修改你的筛选条件 ====================
CONFIG = {
    # 目标城市：BOSS直聘城市ID（默认：成都、重庆、广州、深圳）
    "city_ids": [101250100, 101270100, 101280100, 101280600],
    # 目标岗位关键词（财务岗专属，你可以自己改）
    "job_keywords": ["财务", "会计", "出纳", "审计", "税务", "CFA", "FRM", "财务分析", "预算", "成本"],
    # 排除的关键词（岗位/公司名包含这些直接过滤）
    "exclude_keywords": ["外包", "培训", "兼职", "实习", "猎头", "派遣", "代招", "顾问"],
    # 最低期望薪资（月薪，单位：元）
    "min_salary": 10000,
    # 最多推送多少个最优岗位
    "top_n": 15,
    # 推送配置：钉钉Webhook（从环境变量读取，不要写死在代码里！）
    "dingtalk_webhook": os.getenv("DINGTALK_WEBHOOK", ""),
    # 爬取间隔（秒），避免请求太快被封IP
    "request_delay": 3,
    # 最多爬取多少页（不用太多，最新的职位都在前面）
    "max_pages": 3,
}
# ==============================================================================

# 反爬请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

class BossSpider:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def parse_salary(self, salary_str: str) -> float:
        """解析BOSS的薪资字符串，比如"15-25K·13薪" -> 平均月薪"""
        try:
            # 处理薪级
            if 'K' in salary_str:
                salary_str = salary_str.replace('K', '')
                multiplier = 1000
            elif '万' in salary_str:
                salary_str = salary_str.replace('万', '')
                multiplier = 10000
            else:
                multiplier = 1
            
            # 拆分范围
            if '-' in salary_str:
                low, high = salary_str.split('-')
                # 处理后面的薪数，比如·13薪
                if '·' in high:
                    high, bonus = high.split('·')
                    bonus_month = int(bonus.replace('薪', ''))
                else:
                    bonus_month = 12
                avg = (float(low) + float(high)) / 2 * multiplier
                # 换算成月薪
                return avg * bonus_month / 12
            else:
                # 固定薪资
                if '·' in salary_str:
                    s, bonus = salary_str.split('·')
                    bonus_month = int(bonus.replace('薪', ''))
                else:
                    bonus_month = 12
                return float(s) * multiplier * bonus_month / 12
        except:
            return 0
    
    def crawl_city(self, city_id: int) -> List[Dict]:
        """爬取单个城市的职位数据"""
        jobs = []
        city_name = {
            101250100: "成都",
            101270100: "重庆",
            101280100: "广州",
            101280600: "深圳"
        }.get(city_id, f"未知城市{city_id}")
        
        print(f"开始爬取 {city_name} ...")
        
        for page in range(self.config["max_pages"]):
            try:
                # BOSS直聘搜索接口
                url = f"https://www.zhipin.com/web/geek/job?query={','.join(self.config['job_keywords'])}&city={city_id}&page={page+1}"
                resp = self.session.get(url, timeout=10)
                resp.raise_for_status()
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                job_list = soup.find_all('li', class_='job-card-wrapper')
                
                if not job_list:
                    print(f"{city_name} 第{page+1}页没有更多数据了")
                    break
                
                for job in job_list:
                    try:
                        job_info = {}
                        job_info["city"] = city_name
                        job_info["id"] = job.get('data-jobid')
                        
                        # 岗位名称
                        job_name = job.find('span', class_='job-name')
                        job_info["job_name"] = job_name.get_text(strip=True) if job_name else ""
                        
                        # 薪资
                        salary = job.find('span', class_='salary')
                        salary_str = salary.get_text(strip=True) if salary else ""
                        job_info["salary_str"] = salary_str
                        job_info["salary"] = self.parse_salary(salary_str)
                        
                        # 公司信息
                        company = job.find('h3', class_='company-name')
                        job_info["company"] = company.get_text(strip=True) if company else ""
                        
                        # 岗位标签
                        tag_list = job.find('div', class_='job-info').find('ul')
                        if tag_list:
                            tags = [li.get_text(strip=True) for li in tag_list.find_all('li')]
                            job_info["tags"] = tags
                            job_info["experience"] = tags[0] if len(tags) > 0 else ""
                            job_info["education"] = tags[1] if len(tags) > 1 else ""
                        
                        # 岗位链接
                        link = job.find('a', class_='job-card-left')
                        job_info["link"] = "https://www.zhipin.com" + link.get('href') if link else ""
                        
                        # 福利标签
                        welfare = job.find('div', class_='info-desc')
                        job_info["welfare"] = welfare.get_text(strip=True) if welfare else ""
                        
                        jobs.append(job_info)
                        
                    except Exception as e:
                        print(f"解析岗位失败: {e}")
                        continue
                
                print(f"{city_name} 第{page+1}页完成，已获取 {len(jobs)} 个岗位")
                # 延时，避免反爬
                time.sleep(self.config["request_delay"] + random.random() * 2)
                
            except Exception as e:
                print(f"爬取页面失败: {e}")
                continue
        
        return jobs

class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.history_file = "history.json"
        self.history = self.load_history()
    
    def load_history(self) -> set:
        """加载历史记录，用于去重"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
            except:
                return set()
        return set()
    
    def save_history(self):
        """保存历史记录"""
        with open(self.history_file, 'w', encoding='utf-8') as f:
            json.dump(list(self.history), f, ensure_ascii=False, indent=2)
    
    def filter_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """过滤不符合条件的岗位"""
        filtered = []
        exclude = self.config["exclude_keywords"]
        min_salary = self.config["min_salary"]
        
        for job in jobs:
            # 1. 去重：已经推送过的跳过
            if job["id"] in self.history:
                continue
            
            # 2. 薪资过滤
            if job["salary"] < min_salary:
                continue
            
            # 3. 排除关键词过滤
            text = f"{job['job_name']} {job['company']} {job.get('welfare', '')}"
            if any(keyword in text for keyword in exclude):
                continue
            
            # 4. 岗位关键词匹配（确保是我们要的岗位）
            job_text = job["job_name"]
            if not any(keyword in job_text for keyword in self.config["job_keywords"]):
                continue
            
            filtered.append(job)
        
        return filtered
    
    def score_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """给岗位打分排序，选出最优的"""
        scored = []
        max_salary = max([j["salary"] for j in jobs]) if jobs else 1
        
        for job in jobs:
            score = 0
            
            # 1. 薪资得分（最高40分）
            score += (job["salary"] / max_salary) * 40
            
            # 2. 关键词匹配得分（最高30分）
            match_count = 0
            for keyword in self.config["job_keywords"]:
                if keyword in job["job_name"]:
                    match_count += 1
            score += min(match_count * 5, 30)
            
            # 3. 福利加分：双休/五险一金加分（最高20分）
            welfare = job.get("welfare", "")
            if "双休" in welfare or "周末双休" in welfare:
                score += 10
            if "五险一金" in welfare:
                score += 10
            
            # 4. 经验匹配：如果是1-3年/3-5年的中岗，加分（最高10分）
            exp = job.get("experience", "")
            if "1-3年" in exp or "3-5年" in exp:
                score += 10
            
            job["score"] = score
            scored.append(job)
        
        # 按得分降序排序
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored

class Notifier:
    def __init__(self, config):
        self.config = config
    
    def send_dingtalk(self, jobs: List[Dict]):
        """推送到钉钉"""
        webhook = self.config["dingtalk_webhook"]
        if not webhook:
            print("未配置钉钉Webhook，跳过推送")
            return
        
        # 构建消息内容
        content = f"# 🎯 今日最优职位推送 (Top {len(jobs)})\n\n"
        for i, job in enumerate(jobs, 1):
            content += f"### {i}. {job['job_name']} - {job['company']}\n"
            content += f"- 💰 薪资: {job['salary_str']}\n"
            content += f"- 📍 城市: {job['city']}\n"
            content += f"- 📝 经验: {job.get('experience', '不限')} | 学历: {job.get('education', '不限')}\n"
            content += f"- 🎁 福利: {job.get('welfare', '无')}\n"
            content += f"- 🔗 链接: {job['link']}\n\n"
        
        # 发送请求
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"今日职位推送 Top {len(jobs)}",
                "text": content
            }
        }
        
        try:
            resp = requests.post(webhook, json=data)
            resp.raise_for_status()
            print("钉钉推送成功！")
        except Exception as e:
            print(f"钉钉推送失败: {e}")

def main():
    print("=== Job Scanner 开始运行 ===")
    
    # 初始化模块
    spider = BossSpider(CONFIG)
    processor = DataProcessor(CONFIG)
    notifier = Notifier(CONFIG)
    
    # 1. 爬取所有城市的职位
    all_jobs = []
    for city_id in CONFIG["city_ids"]:
        jobs = spider.crawl_city(city_id)
        all_jobs.extend(jobs)
    
    print(f"共爬取到 {len(all_jobs)} 个原始岗位")
    
    # 2. 过滤
    filtered_jobs = processor.filter_jobs(all_jobs)
    print(f"过滤后剩余 {len(filtered_jobs)} 个新岗位")
    
    if not filtered_jobs:
        print("没有符合条件的新岗位，结束运行")
        return
    
    # 3. 打分排序，取Top15
    scored_jobs = processor.score_jobs(filtered_jobs)
    top_jobs = scored_jobs[:CONFIG["top_n"]]
    print(f"选出 Top {len(top_jobs)} 最优岗位")
    
    # 4. 推送
    notifier.send_dingtalk(top_jobs)
    
    # 5. 更新历史记录
    for job in top_jobs:
        processor.history.add(job["id"])
    processor.save_history()
    
    print("=== 运行完成 ===")

if __name__ == "__main__":
    main()
