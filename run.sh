#!/bin/bash

# HKEX 公告下载器 - 快速启动脚本

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 检查Python是否安装
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "未找到Python3，请先安装Python 3.7+"
        exit 1
    fi
    print_success "Python检查通过"
}

# 检查配置文件
check_config() {
    if [ ! -f "config.yaml" ]; then
        print_warning "配置文件不存在，正在创建..."
        if [ -f "config_template.yaml" ]; then
            cp config_template.yaml config.yaml
            print_success "配置文件已创建，请编辑 config.yaml 后重新运行"
            exit 0
        else
            print_error "配置模板文件不存在"
            exit 1
        fi
    fi
}

# 显示菜单
show_menu() {
    echo
    echo "========================================"
    echo "    HKEX 公告下载器 - 快速启动"
    echo "========================================"
    echo
    echo "请选择操作:"
    echo "1. 运行配置文件中的任务"
    echo "2. 下载单个股票公告"
    echo "3. 检查配置文件"
    echo "4. 列出所有任务"
    echo "5. 测试数据库连接"
    echo "6. 退出"
    echo
}

# 运行配置任务
run_config_tasks() {
    print_info "运行配置文件中的任务..."
    python3 main.py
}

# 下载单个股票
download_single_stock() {
    echo
    read -p "请输入股票代码 (5位数字): " stock_code
    if [ -z "$stock_code" ]; then
        print_error "股票代码不能为空"
        return
    fi
    print_info "下载股票 $stock_code 的公告..."
    python3 main.py -s "$stock_code"
}

# 检查配置文件
check_config_file() {
    print_info "检查配置文件..."
    python3 main.py --check-config
}

# 列出任务
list_tasks() {
    print_info "列出所有任务..."
    python3 main.py --list-tasks
}

# 测试数据库
test_database() {
    print_info "测试数据库连接..."
    python3 main.py --test-db
}

# 主函数
main() {
    # 检查环境
    check_python
    check_config
    
    while true; do
        show_menu
        read -p "请输入选项 (1-6): " choice
        
        case $choice in
            1)
                run_config_tasks
                ;;
            2)
                download_single_stock
                ;;
            3)
                check_config_file
                ;;
            4)
                list_tasks
                ;;
            5)
                test_database
                ;;
            6)
                print_success "再见！"
                exit 0
                ;;
            *)
                print_error "无效选项，请重新选择"
                ;;
        esac
        
        echo
        read -p "按回车键继续..."
    done
}

# 运行主函数
main
