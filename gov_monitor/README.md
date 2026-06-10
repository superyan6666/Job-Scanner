# 官方招录通知监控系统 (Gov Monitor)

这是一个轻量级的 Python 脚本，专门用于在云服务器上定时抓取 **四川、江苏、浙江、广东** 地区的公务员、事业编和编外人员招录通知。

## 部署教程 (基于甲骨文云 / Linux crontab)

### 1. 环境准备
确保您的甲骨文云服务器上安装了 Python 3 和必要的库：
```bash
pip install requests beautifulsoup4
```

### 2. 配置 Webhook
此脚本支持同时推送到 **钉钉** 和 **飞书** 机器人。请在系统中配置环境变量，或者直接运行脚本前声明变量。

### 3. 配置 Crontab 定时任务
建议每 4 小时执行一次。
使用 `crontab -e` 编辑您的定时任务，添加以下行：

```bash
# 每4小时的第0分钟执行一次 (需替换为您实际的路径和 Webhook 地址)
0 */4 * * * export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=您的token" && export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/您的token" && python3 /path/to/gov_monitor/main.py >> /path/to/gov_monitor/run.log 2>&1
```

### 4. 运行逻辑
1. `main.py` 会调用 `crawler.py` 拉取各地中公教育的汇总页。
2. 过滤出标题包含“招聘,招考,遴选,事业编”等字样的有效公告。
3. 对比 `gov_notices.db` (SQLite)，如果 URL 没见过，说明是新的！
4. 调用 `notifier.py` 推送到钉钉和飞书，随后将其存入数据库。
