@echo off
chcp 65001 >nul
title HKEX 公告下载器

echo.
echo ========================================
echo    HKEX 公告下载器 - 快速启动
echo ========================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: 未找到Python，请先安装Python 3.7+
    pause
    exit /b 1
)

REM 检查配置文件
if not exist "config.yaml" (
    echo ⚠️ 配置文件不存在，正在创建...
    if exist "config_template.yaml" (
        copy "config_template.yaml" "config.yaml" >nul
        echo ✅ 配置文件已创建，请编辑 config.yaml 后重新运行
        pause
        exit /b 0
    ) else (
        echo ❌ 配置模板文件不存在
        pause
        exit /b 1
    )
)

REM 显示菜单
:menu
echo.
echo 请选择操作:
echo 1. 运行配置文件中的任务
echo 2. 下载单个股票公告
echo 3. 检查配置文件
echo 4. 列出所有任务
echo 5. 测试数据库连接
echo 6. 退出
echo.
set /p choice=请输入选项 (1-6): 

if "%choice%"=="1" goto run_config
if "%choice%"=="2" goto single_stock
if "%choice%"=="3" goto check_config
if "%choice%"=="4" goto list_tasks
if "%choice%"=="5" goto test_db
if "%choice%"=="6" goto exit
echo 无效选项，请重新选择
goto menu

:run_config
echo.
echo 🚀 运行配置文件中的任务...
python main.py
goto end

:single_stock
echo.
set /p stock_code=请输入股票代码 (5位数字): 
if "%stock_code%"=="" (
    echo 股票代码不能为空
    goto menu
)
echo 🚀 下载股票 %stock_code% 的公告...
python main.py -s %stock_code%
goto end

:check_config
echo.
echo 🔍 检查配置文件...
python main.py --check-config
goto end

:list_tasks
echo.
echo 📋 列出所有任务...
python main.py --list-tasks
goto end

:test_db
echo.
echo 🔗 测试数据库连接...
python main.py --test-db
goto end

:end
echo.
echo 按任意键返回菜单...
pause >nul
goto menu

:exit
echo.
echo 👋 再见！
pause
