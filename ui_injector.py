import os
import time
import threading
from DrissionPage import ChromiumPage, ChromiumOptions

import db_manager
from layer1_crawler import Layer1Crawler
from layer2_evaluator import Layer2Evaluator

# 全局状态
is_layer1_running = False
is_layer2_running = False

def run_layer1(pages):
    global is_layer1_running
    is_layer1_running = True
    try:
        crawler = Layer1Crawler(max_pages=pages)
        crawler.run()
    finally:
        is_layer1_running = False

def run_layer2():
    global is_layer2_running
    is_layer2_running = True
    try:
        evaluator = Layer2Evaluator()
        evaluator.run()
    finally:
        is_layer2_running = False

def get_panel_js():
    return """
    if (!document.getElementById('jm-super-panel')) {
        const html = `
        <div id="jm-super-panel" style="position: fixed; bottom: 30px; left: 30px; width: 320px; background: rgba(15, 15, 20, 0.9); backdrop-filter: blur(12px); color: #fff; border-radius: 16px; border: 1px solid rgba(255,255,255,0.15); font-family: 'Segoe UI', system-ui, sans-serif; z-index: 2147483647; box-shadow: 0 10px 40px rgba(0,0,0,0.5); padding: 20px; display: flex; flex-direction: column; gap: 12px; transition: all 0.3s;">
            <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 10px;">
                <h3 style="margin:0; font-size: 18px; font-weight: 600; background: linear-gradient(90deg, #00C6FF, #0072FF); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">🚀 Job Scanner 漏斗中枢</h3>
            </div>
            
            <input type="hidden" id="jm-command" value="">
            
            <div style="font-size: 13px; background: rgba(0,0,0,0.3); padding: 12px; border-radius: 8px; line-height: 1.8;">
                <div style="color: #aaa; margin-bottom: 5px; font-weight: 600;">📊 实时数据库看板</div>
                <div style="display:flex; justify-content: space-between;"><span>发现总岗位:</span> <span id="jm-stat-total" style="color: #0df; font-weight:bold;">0</span></div>
                <div style="display:flex; justify-content: space-between;"><span>待安检公司:</span> <span id="jm-stat-pending" style="color: #fa0; font-weight:bold;">0</span></div>
                <div style="display:flex; justify-content: space-between;"><span>已淘汰外包:</span> <span id="jm-stat-rejected" style="color: #f44; font-weight:bold;">0</span></div>
                <div style="display:flex; justify-content: space-between;"><span>优质好公司:</span> <span id="jm-stat-approved" style="color: #0f4; font-weight:bold;">0</span></div>
            </div>
            
            <div style="display: flex; align-items: center; gap: 10px;">
                <span style="font-size: 13px; color: #ccc;">极速采集页数:</span>
                <input id="jm-pages" type="number" value="5" min="1" max="50" style="width: 60px; background: rgba(0,0,0,0.5); border: 1px solid rgba(255,255,255,0.2); color: #fff; padding: 5px; border-radius: 6px; outline: none; text-align: center;">
            </div>
            
            <button id="jm-btn-layer1" style="background: linear-gradient(90deg, #00C6FF 0%, #0072FF 100%); color: white; border: none; padding: 12px; border-radius: 8px; font-weight: 600; cursor: pointer; transition: 0.3s; box-shadow: 0 4px 15px rgba(0, 114, 255, 0.3);">
                ▶ 执行 Layer 1 (极速扫卡入库)
            </button>
            <button id="jm-btn-layer2" style="background: linear-gradient(90deg, #f12711 0%, #f5af19 100%); color: white; border: none; padding: 12px; border-radius: 8px; font-weight: 600; cursor: pointer; transition: 0.3s; box-shadow: 0 4px 15px rgba(245, 175, 25, 0.3);">
                ▶ 执行 Layer 2 (爱企查自动体检)
            </button>
            
            <div id="jm-log" style="font-size: 11px; color: #888; text-align: center; margin-top: 5px;">就绪</div>
        </div>
        `;
        document.body.insertAdjacentHTML('beforeend', html);

        document.getElementById('jm-btn-layer1').onclick = () => {
            const pages = document.getElementById('jm-pages').value || "5";
            document.getElementById('jm-command').value = "layer1:" + pages;
            document.getElementById('jm-log').innerText = "指令已发送: Layer 1";
        };
        
        document.getElementById('jm-btn-layer2').onclick = () => {
            document.getElementById('jm-command').value = "layer2";
            document.getElementById('jm-log').innerText = "指令已发送: Layer 2";
        };
        
        console.log("Job Scanner 高级悬浮窗注入成功！");
    }
    """

def start_system():
    print("=== 正在启动 Job Scanner 中央枢纽 ===")
    
    print("[1/2] 启动底层 DrissionPage 浏览器引擎...")
    co = ChromiumOptions()
    co.set_local_port(9223)
    co.set_argument('--disable-blink-features=AutomationControlled')
    co.set_argument('--start-maximized')
    co.set_user_data_path(r'./edge_profile_drission')
    
    edge_path = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
    if os.path.exists(edge_path):
        co.set_browser_path(edge_path)
        
    page = ChromiumPage(co)
    page.get("https://www.zhipin.com/web/geek/job")
    
    print("[2/2] 正在挂载高级悬浮 UI，并开启 DOM 轮询守护...")
    js_code = get_panel_js()
    
    try:
        while True:
            time.sleep(1.5)
            # 1. 确保面板存在
            try:
                has_panel = page.run_js("return document.getElementById('jm-super-panel') !== null;")
                if not has_panel and ("/geek/job" in page.url or "/s" in page.url):
                    page.run_js(js_code)
            except:
                continue
                
            # 2. 获取 UI 指令
            try:
                cmd = page.run_js("return document.getElementById('jm-command') ? document.getElementById('jm-command').value : '';")
                if cmd:
                    # 清空前端指令
                    page.run_js("if(document.getElementById('jm-command')) document.getElementById('jm-command').value = '';")
                    
                    if cmd.startswith("layer1") and not is_layer1_running:
                        pages = int(cmd.split(":")[1])
                        threading.Thread(target=run_layer1, args=(pages,), daemon=True).start()
                        page.run_js("if(document.getElementById('jm-log')) document.getElementById('jm-log').innerText = 'Layer 1 已在后台启动';")
                    elif cmd.startswith("layer2") and not is_layer2_running:
                        threading.Thread(target=run_layer2, daemon=True).start()
                        page.run_js("if(document.getElementById('jm-log')) document.getElementById('jm-log').innerText = 'Layer 2 已在后台启动';")
            except Exception as e:
                pass
                
            # 3. 更新统计数据和按钮状态
            try:
                stats = db_manager.get_stats()
                update_js = f"""
                if(document.getElementById('jm-super-panel')) {{
                    document.getElementById('jm-stat-total').innerText = '{stats['total_jobs']}';
                    document.getElementById('jm-stat-pending').innerText = '{stats['pending_companies']}';
                    document.getElementById('jm-stat-rejected').innerText = '{stats['rejected_companies']}';
                    document.getElementById('jm-stat-approved').innerText = '{stats['approved_companies']}';
                    
                    const btn1 = document.getElementById('jm-btn-layer1');
                    const btn2 = document.getElementById('jm-btn-layer2');
                    
                    if ({str(is_layer1_running).lower()}) {{
                        btn1.innerText = '⏳ Layer 1 正在巡航中...';
                        btn1.style.opacity = '0.5';
                        btn1.style.pointerEvents = 'none';
                    }} else {{
                        btn1.innerText = '▶ 执行 Layer 1 (极速扫卡入库)';
                        btn1.style.opacity = '1';
                        btn1.style.pointerEvents = 'auto';
                    }}
                    
                    if ({str(is_layer2_running).lower()}) {{
                        btn2.innerText = '⏳ Layer 2 正在体检中...';
                        btn2.style.opacity = '0.5';
                        btn2.style.pointerEvents = 'none';
                    }} else {{
                        btn2.innerText = '▶ 执行 Layer 2 (爱企查自动体检)';
                        btn2.style.opacity = '1';
                        btn2.style.pointerEvents = 'auto';
                    }}
                }}
                """
                page.run_js(update_js)
            except Exception as e:
                pass

    except KeyboardInterrupt:
        print("系统已安全退出。")

if __name__ == "__main__":
    start_system()
