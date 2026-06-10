@echo off
chcp 65001 >nul
echo ========================================================
echo        Job Scanner - 官方招录通知监控系统 (本地直连版)
echo ========================================================

:: 切换到脚本所在的上级目录
cd /d "%~dp0\.."

:: ==========================================
:: 【必须配置】请将下面的 sk-xxxx 替换为您真实的 API Key
:: ==========================================
set SILICONFLOW_API_KEY=sk-xxxx

if "%SILICONFLOW_API_KEY%"=="sk-xxxx" (
    echo [警告] 您还没有配置 SILICONFLOW_API_KEY！
    echo 请右键编辑 run_scanner.bat，把 sk-xxxx 换成您的真实密钥。
    echo 本次运行 HERMES AI 将被跳过，全部内容直接放行...
    echo.
)

echo [系统] 正在呼叫 HERMES 大脑并进行全网巡航...
python gov_monitor/main.py

echo.
echo [完成] 本次巡航结束。
pause
