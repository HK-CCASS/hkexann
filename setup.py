#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告下载器安装脚本
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

def check_python_version():
    """检查Python版本"""
    if sys.version_info < (3, 7):
        print("❌ 错误: 需要Python 3.7或更高版本")
        print(f"当前版本: {sys.version}")
        return False
    print(f"✅ Python版本检查通过: {sys.version}")
    return True

def install_dependencies():
    """安装依赖包"""
    print("\n📦 安装依赖包...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ 依赖包安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 依赖包安装失败: {e}")
        return False

def setup_config():
    """设置配置文件"""
    print("\n⚙️ 设置配置文件...")
    
    config_file = Path("config.yaml")
    template_file = Path("config_template.yaml")
    
    if config_file.exists():
        print("⚠️ config.yaml 已存在，跳过配置文件创建")
        return True
    
    if not template_file.exists():
        print("❌ 配置模板文件不存在")
        return False
    
    try:
        shutil.copy2(template_file, config_file)
        print("✅ 配置文件创建成功")
        print("📝 请编辑 config.yaml 文件以配置您的设置")
        return True
    except Exception as e:
        print(f"❌ 配置文件创建失败: {e}")
        return False

def test_installation():
    """测试安装"""
    print("\n🧪 测试安装...")
    try:
        subprocess.check_call([sys.executable, "main.py", "--check-config"])
        print("✅ 安装测试通过")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 安装测试失败: {e}")
        return False

def main():
    """主函数"""
    print("🚀 HKEX 公告下载器安装程序")
    print("=" * 50)
    
    # 检查Python版本
    if not check_python_version():
        sys.exit(1)
    
    # 安装依赖
    if not install_dependencies():
        sys.exit(1)
    
    # 设置配置文件
    if not setup_config():
        sys.exit(1)
    
    # 测试安装
    if not test_installation():
        print("⚠️ 安装测试失败，但基本安装已完成")
    
    print("\n🎉 安装完成！")
    print("\n📖 使用说明:")
    print("1. 编辑 config.yaml 文件配置您的设置")
    print("2. 运行: python main.py --help 查看帮助")
    print("3. 运行: python main.py 开始下载")

if __name__ == "__main__":
    main()
