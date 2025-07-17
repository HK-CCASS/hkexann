#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试脚本：演示增强的下载功能
"""

import subprocess
import sys

def run_test():
    """运行测试"""
    print("🧪 测试增强的下载功能")
    print("=" * 50)
    
    # 测试单个股票下载
    print("\n📋 测试1：单个股票下载（带详细状态）")
    cmd = [
        sys.executable, "new_stocks.py", 
        "-s", "00328",  # ALCO HOLDINGS
        "--start", "2024-07-01", 
        "--end", "2024-07-17"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        print("✅ 测试完成")
        print("输出预览：")
        lines = result.stdout.split('\n')
        for line in lines[-20:]:  # 显示最后20行
            if line.strip():
                print(f"  {line}")
    except subprocess.TimeoutExpired:
        print("⏰ 测试超时")
    except Exception as e:
        print(f"❌ 测试失败：{e}")

if __name__ == "__main__":
    run_test()
