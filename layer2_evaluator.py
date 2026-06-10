import time
import re
import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
import db_manager

class Layer2Evaluator:
    def __init__(self):
        self.page = None
        # 本地黑名单（命中直接淘汰，省时省力）
        self.blacklist_keywords = ["外包", "人力", "劳务", "派遣", "人力资源", "人才服务"]

    def start_browser(self):
        co = ChromiumOptions()
        co.set_local_port(9223) # 复用同一个浏览器配置
        co.set_argument('--disable-blink-features=AutomationControlled')
        co.set_user_data_path(r'./edge_profile_drission')
        
        print("[Layer2] 启动/接管浏览器用于背调...")
        self.page = ChromiumPage(co)
        return True

    def evaluate_company_local(self, company_name):
        """本地极速鉴别"""
        for kw in self.blacklist_keywords:
            if kw in company_name:
                return False, f"本地黑名单命中 ({kw})"
        return True, "本地通过"

    def evaluate_company_aiqicha(self, company_name):
        """爱企查自动背调 (免登录查基本信息)"""
        if not self.page:
            self.start_browser()
            
        print(f"  [爱企查] 正在搜索: {company_name}")
        search_url = f"https://aiqicha.baidu.com/s?q={company_name}"
        self.page.get(search_url)
        self.page.wait.load_start()
        time.sleep(2) # 礼貌延迟，防止出验证码
        
        # 提取第一个搜索结果的成立日期
        try:
            # 兼容各种加载慢的情况
            self.page.wait.ele_loaded('.company-list', timeout=5)
            first_card = self.page.ele('.company-list').ele('.wrap', timeout=1)
            
            if not first_card:
                return True, "未在爱企查搜到精确结果，暂定通过"
                
            # 找到成立日期字段
            labels = first_card.eles('.label')
            est_date_text = ""
            for label in labels:
                if "成立时间" in label.text:
                    est_date_text = label.text
                    break
            
            if not est_date_text:
                return True, "未提取到成立时间"
                
            # 提取年份
            match = re.search(r'(\d{4})-\d{2}-\d{2}', est_date_text)
            if match:
                year = int(match.group(1))
                current_year = datetime.datetime.now().year
                age = current_year - year
                print(f"  [爱企查] 公司成立时间: {match.group(0)} (距今 {age} 年)")
                
                if age < 1:
                    return False, f"成立时间小于1年 ({match.group(0)})，风险极高"
                else:
                    return True, f"成立时间 {age} 年，通过"
                    
            return True, "解析成立时间失败，暂定通过"
        except Exception as e:
            print(f"  [爱企查] 抓取异常: {e}")
            return True, "抓取异常，暂定通过"

    def run(self):
        pending_companies = db_manager.get_pending_companies()
        if not pending_companies:
            print("[Layer2] 数据库中没有需要安检的公司。")
            return
            
        print(f"[Layer2] 开始执行质量安检，共需检查 {len(pending_companies)} 家公司...")
        
        for i, company_name in enumerate(pending_companies):
            print(f"\n[{i+1}/{len(pending_companies)}] 安检目标: {company_name}")
            
            # 1. 本地拦截
            passed, reason = self.evaluate_company_local(company_name)
            if not passed:
                print(f"  ❌ 淘汰: {reason}")
                db_manager.update_company(company_name, "REJECTED", tags=reason)
                continue
                
            # 2. 在线背调
            passed, reason = self.evaluate_company_aiqicha(company_name)
            if not passed:
                print(f"  ❌ 淘汰: {reason}")
                db_manager.update_company(company_name, "REJECTED", tags=reason)
            else:
                print(f"  ✅ 通过: {reason}")
                db_manager.update_company(company_name, "APPROVED", score=80, tags=reason)

        print("[Layer2] 安检任务全部完成！")

if __name__ == "__main__":
    evaluator = Layer2Evaluator()
    evaluator.run()
