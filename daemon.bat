@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: HKEX 公告下载器 - 守护者进程控制脚本 (Windows)
:: 作者：Victor Suen
:: 版本：2.1

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

if "%1"=="" (
    echo 使用方法: %0 [start^|stop^|restart^|status^|test]
    echo.
    echo 可用操作:
    echo   start    - 启动守护者进程
    echo   stop     - 停止守护者进程
    echo   restart  - 重启守护者进程
    echo   status   - 查看守护者进程状态
    echo   test     - 测试守护者进程配置
    echo.
    echo 示例:
    echo   %0 start
    echo   %0 status
    goto :eof
)

set "ACTION=%1"

:: 检查Python是否可用
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请确保Python已安装并添加到PATH环境变量中
    pause
    exit /b 1
)

:: 检查main.py是否存在
if not exist "main.py" (
    echo 错误: 未找到main.py文件
    pause
    exit /b 1
)

:: 执行对应的操作
if "%ACTION%"=="start" (
    echo 启动守护者进程...
    python main.py --daemon-start
) else if "%ACTION%"=="stop" (
    echo 停止守护者进程...
    python main.py --daemon-stop
) else if "%ACTION%"=="restart" (
    echo 重启守护者进程...
    python main.py --daemon-restart
) else if "%ACTION%"=="status" (
    echo 查看守护者进程状态...
    python main.py --daemon-status
) else if "%ACTION%"=="test" (
    echo 测试守护者进程配置...
    python main.py --daemon-test
) else (
    echo 错误: 未知操作 "%ACTION%"
    echo 可用操作: start, stop, restart, status, test
    exit /b 1
)

if errorlevel 1 (
    echo.
    echo 操作失败，请检查错误信息
    pause
)
