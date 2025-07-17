#!/bin/bash

# HKEX 公告下载器 - 守护者进程控制脚本 (Linux/macOS)
# 作者：Victor Suen
# 版本：2.1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

show_usage() {
    echo "使用方法: $0 [start|stop|restart|status|test]"
    echo ""
    echo "可用操作:"
    echo "  start    - 启动守护者进程"
    echo "  stop     - 停止守护者进程"
    echo "  restart  - 重启守护者进程"
    echo "  status   - 查看守护者进程状态"
    echo "  test     - 测试守护者进程配置"
    echo ""
    echo "示例:"
    echo "  $0 start"
    echo "  $0 status"
}

if [ $# -eq 0 ]; then
    show_usage
    exit 1
fi

ACTION="$1"

# 检查Python是否可用
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo "错误: 未找到Python，请确保Python已安装"
    exit 1
fi

# 优先使用python3
PYTHON_CMD="python3"
if ! command -v python3 &> /dev/null; then
    PYTHON_CMD="python"
fi

# 检查main.py是否存在
if [ ! -f "main.py" ]; then
    echo "错误: 未找到main.py文件"
    exit 1
fi

# 执行对应的操作
case "$ACTION" in
    start)
        echo "启动守护者进程..."
        $PYTHON_CMD main.py --daemon-start
        ;;
    stop)
        echo "停止守护者进程..."
        $PYTHON_CMD main.py --daemon-stop
        ;;
    restart)
        echo "重启守护者进程..."
        $PYTHON_CMD main.py --daemon-restart
        ;;
    status)
        echo "查看守护者进程状态..."
        $PYTHON_CMD main.py --daemon-status
        ;;
    test)
        echo "测试守护者进程配置..."
        $PYTHON_CMD main.py --daemon-test
        ;;
    *)
        echo "错误: 未知操作 '$ACTION'"
        echo "可用操作: start, stop, restart, status, test"
        exit 1
        ;;
esac
