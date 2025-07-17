#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试同步和异步下载性能对比"""

import time
import sys
from main import ConfigManager, HKEXDownloader
from async_downloader import run_async_download

# 测试股票列表（选择5只股票）
test_stocks = ['00700', '00005', '00001', '09988', '02382']

# 创建任务
task = {
    'name': '性能测试',
    'stock_code': test_stocks,
    'start_date': '2025-01-10',
    'end_date': '2025-01-17',
    'keywords': [],
    'enabled': True
}

# 加载配置
config_manager = ConfigManager('config.yaml')

print(f"测试下载 {len(test_stocks)} 只股票的公告")
print(f"股票列表: {', '.join(test_stocks)}")
print(f"日期范围: {task['start_date']} 至 {task['end_date']}")
print("-" * 50)

# 1. 测试同步下载
print("\n1. 同步下载模式:")
sync_start = time.time()
total_sync = 0

downloader = HKEXDownloader(config_manager)
for stock in test_stocks:
    single_task = task.copy()
    single_task['stock_code'] = stock
    try:
        save_path, count = downloader.download_announcements(single_task)
        total_sync += count
        print(f"   {stock}: {count} 个文件")
    except Exception as e:
        print(f"   {stock}: 失败 - {e}")

sync_time = time.time() - sync_start
print(f"同步下载完成！耗时: {sync_time:.2f}秒，共 {total_sync} 个文件")

# 2. 测试异步下载
print("\n2. 异步下载模式:")
async_start = time.time()

try:
    save_path, total_async = run_async_download(config_manager, task)
    async_time = time.time() - async_start
    print(f"异步下载完成！耗时: {async_time:.2f}秒，共 {total_async} 个文件")
except Exception as e:
    print(f"异步下载失败: {e}")
    async_time = 0
    total_async = 0

# 3. 性能对比
if async_time > 0:
    print("\n" + "="*50)
    print("性能对比结果:")
    print(f"同步模式: {sync_time:.2f}秒 ({total_sync} 个文件)")
    print(f"异步模式: {async_time:.2f}秒 ({total_async} 个文件)")
    speedup = sync_time / async_time
    print(f"性能提升: {speedup:.2f}x")
    print(f"节省时间: {sync_time - async_time:.2f}秒 ({(1-async_time/sync_time)*100:.1f}%)")