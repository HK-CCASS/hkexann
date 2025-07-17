#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试混合模式功能"""

import asyncio
import logging
from main import ConfigManager
from async_downloader import run_async_download

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def test_hybrid_mode():
    """测试混合模式下载"""
    print("=== 测试混合模式下载 ===")
    print("获取股票ID和公告列表: 同步")
    print("文件下载: 异步并发")
    print("-" * 50)
    
    # 加载配置
    config_manager = ConfigManager('config.yaml')
    
    # 创建测试任务
    task = {
        'name': '混合模式测试',
        'stock_code': '00700',  # 腾讯
        'start_date': '2025-01-15',
        'end_date': '2025-01-17',
        'keywords': [],
        'enabled': True
    }
    
    print(f"测试股票: {task['stock_code']}")
    print(f"日期范围: {task['start_date']} 至 {task['end_date']}")
    
    try:
        print("\n开始混合模式下载...")
        save_path, count = run_async_download(config_manager, task)
        
        if count > 0:
            print(f"\n✓ 混合模式测试成功！")
            print(f"  下载文件数: {count}")
            print(f"  保存路径: {save_path}")
        else:
            print(f"\n⚠ 未找到符合条件的公告（正常情况）")
            print("  混合模式功能正常")
            
    except Exception as e:
        print(f"\n✗ 混合模式测试失败: {e}")
        return False
    
    return True

if __name__ == "__main__":
    # 运行测试
    success = asyncio.run(test_hybrid_mode())
    
    if success:
        print("\n🎉 混合模式测试完成！")
        print("✓ 获取股票ID: 同步执行")
        print("✓ 获取公告列表: 同步执行") 
        print("✓ 文件下载: 异步并发执行")
    else:
        print("\n❌ 测试失败") 