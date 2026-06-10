import os
import requests
import json

# ==========================================
# 📢 请在此处填写您的 Webhook 机器人地址
# ==========================================
# 如果您不想在 crontab 里配置环境变量，可以直接把链接粘贴到下面单引号里面：

DINGTALK_WEBHOOK = os.environ.get('DINGTALK_WEBHOOK', '这里填写钉钉Webhook链接')
FEISHU_WEBHOOK = os.environ.get('FEISHU_WEBHOOK', 'https://open.feishu.cn/open-apis/bot/v2/hook/90801a7a-a78e-4e28-a3de-c7a061eb8c12')

def send_dingtalk(title, content, url):
    if not DINGTALK_WEBHOOK or '这里填写' in DINGTALK_WEBHOOK:
        return
        
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": "赛博职协: 📢 新招考公告提醒",
            "text": f"### 赛博职协: 📢 新招考公告提醒\n\n**标题：** {title}\n\n**详情：** {content}\n\n[点击查看详情]({url})"
        }
    }
    
    try:
        response = requests.post(DINGTALK_WEBHOOK, headers=headers, data=json.dumps(data))
        print(f"[DingTalk] 推送结果: {response.text}")
    except Exception as e:
        print(f"[DingTalk] 推送失败: {e}")

def send_feishu(title, content, url):
    if not FEISHU_WEBHOOK or '这里填写' in FEISHU_WEBHOOK:
        return
        
    headers = {'Content-Type': 'application/json'}
    data = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "赛博职协: 📢 新招考公告提醒",
                    "content": [
                        [
                            {"tag": "text", "text": f"标题：{title}\n"},
                            {"tag": "text", "text": f"详情：{content}\n"}
                        ],
                        [
                            {"tag": "a", "text": "点击查看详情", "href": url}
                        ]
                    ]
                }
            }
        }
    }
    
    try:
        response = requests.post(FEISHU_WEBHOOK, headers=headers, data=json.dumps(data))
        print(f"[Feishu] 推送结果: {response.text}")
    except Exception as e:
        print(f"[Feishu] 推送失败: {e}")

def notify_all(title, province, publish_date, url):
    print(f"准备推送公告: {title}")
    content = f"[{province}] 发布日期: {publish_date}"
    send_dingtalk(title, content, url)
    send_feishu(title, content, url)

if __name__ == '__main__':
    # 测试用，如果在本地没有设置环境变量则什么都不发生
    notify_all("浙江省杭州市某局编外招聘公告", "浙江", "2026-06-10", "http://zj.offcn.com/test")
