#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调试异步下载问题"""

import traceback
from main import ConfigManager
from async_downloader import run_async_download

# 加载配置
config_manager = ConfigManager('config.yaml')

# 创建测试任务
task = {
    'name': '调试任务',
    'stock_code': '00700',
    'start_date': '2025-01-15',
    'end_date': '2025-01-17',
    'keywords': [],
    'enabled': True
}

print("配置信息:")
print(f"async.enabled: {config_manager.get('async', 'enabled')}")
print(f"max_concurrent: {config_manager.get('async', 'max_concurrent')}")
print(f"requests_per_second: {config_manager.get('async', 'requests_per_second')}")

print("\n开始测试异步下载...")
try:
    save_path, count = run_async_download(config_manager, task)
    print(f"成功! 下载了 {count} 个文件到 {save_path}")
except Exception as e:
    print(f"错误: {e}")
    traceback.print_exc()