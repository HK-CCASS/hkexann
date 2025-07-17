#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
异步下载功能测试脚本
用于快速测试异步下载是否正常工作
"""

import asyncio
import time
from datetime import datetime, timedelta
from main import ConfigManager
from async_downloader import AsyncHKEXDownloader, run_async_download


def test_rate_limiter():
    """测试速率限制器"""
    print("=== 测试速率限制器 ===")
    from async_downloader import RateLimiter
    
    async def test():
        limiter = RateLimiter(rate=2, per=1.0)  # 每秒2个请求
        
        print("发送5个请求（每秒限制2个）...")
        start = time.time()
        
        for i in range(5):
            await limiter.acquire()
            print(f"请求 {i+1} 完成，耗时: {time.time() - start:.2f}秒")
        
        total_time = time.time() - start
        print(f"总耗时: {total_time:.2f}秒（预期约2.5秒）\n")
    
    asyncio.run(test())


def test_async_download_single():
    """测试单个股票异步下载"""
    print("=== 测试单个股票异步下载 ===")
    
    try:
        # 加载配置
        config_manager = ConfigManager('config.yaml')
        
        # 创建测试任务
        task = {
            'stock_code': '00700',  # 腾讯
            'start_date': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'end_date': 'today',
            'keywords': [],
            'enabled': True
        }
        
        print(f"下载股票 {task['stock_code']} 最近7天的公告...")
        start_time = time.time()
        
        # 运行异步下载
        save_path, count = run_async_download(config_manager, task)
        
        elapsed = time.time() - start_time
        print(f"下载完成！耗时: {elapsed:.2f}秒")
        print(f"保存路径: {save_path}")
        print(f"下载文件数: {count}\n")
        
    except Exception as e:
        print(f"测试失败: {e}\n")


def test_async_download_multiple():
    """测试多股票批量异步下载"""
    print("=== 测试多股票批量异步下载 ===")
    
    try:
        # 加载配置
        config_manager = ConfigManager('config.yaml')
        
        # 创建批量任务
        task = {
            'stock_code': ['00700', '00941', '09988'],  # 腾讯、中国移动、阿里巴巴
            'start_date': (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d'),
            'end_date': 'today',
            'keywords': [],
            'enabled': True
        }
        
        print(f"批量下载 {len(task['stock_code'])} 只股票最近3天的公告...")
        print(f"股票列表: {', '.join(task['stock_code'])}")
        start_time = time.time()
        
        # 运行异步下载
        save_path, count = run_async_download(config_manager, task)
        
        elapsed = time.time() - start_time
        print(f"批量下载完成！耗时: {elapsed:.2f}秒")
        print(f"保存路径: {save_path}")
        print(f"总下载文件数: {count}")
        print(f"平均每只股票耗时: {elapsed/len(task['stock_code']):.2f}秒\n")
        
    except Exception as e:
        print(f"测试失败: {e}\n")


def compare_sync_vs_async():
    """对比同步和异步下载性能"""
    print("=== 对比同步和异步下载性能 ===")
    
    try:
        from main import HKEXDownloader
        config_manager = ConfigManager('config.yaml')
        
        # 测试股票列表
        stock_codes = ['00700', '00941', '09988', '00005', '01299']
        
        # 任务配置
        task = {
            'start_date': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'end_date': 'today',
            'keywords': [],
            'enabled': True
        }
        
        print(f"测试下载 {len(stock_codes)} 只股票最近7天的公告...")
        
        # 1. 测试同步下载
        print("\n1. 同步下载模式:")
        sync_start = time.time()
        sync_total = 0
        
        downloader = HKEXDownloader(config_manager)
        for code in stock_codes:
            stock_task = task.copy()
            stock_task['stock_code'] = code
            try:
                _, count = downloader.download_announcements(stock_task)
                sync_total += count
                print(f"  - {code}: {count} 个文件")
            except Exception as e:
                print(f"  - {code}: 失败 - {e}")
        
        sync_time = time.time() - sync_start
        print(f"同步下载完成！总耗时: {sync_time:.2f}秒，共 {sync_total} 个文件")
        
        # 2. 测试异步下载
        print("\n2. 异步下载模式:")
        async_task = task.copy()
        async_task['stock_code'] = stock_codes
        
        async_start = time.time()
        save_path, async_total = run_async_download(config_manager, async_task)
        async_time = time.time() - async_start
        
        print(f"异步下载完成！总耗时: {async_time:.2f}秒，共 {async_total} 个文件")
        
        # 3. 性能对比
        print("\n=== 性能对比结果 ===")
        print(f"同步下载: {sync_time:.2f}秒 ({sync_total} 个文件)")
        print(f"异步下载: {async_time:.2f}秒 ({async_total} 个文件)")
        if async_time > 0:
            speedup = sync_time / async_time
            print(f"性能提升: {speedup:.2f}x")
            print(f"节省时间: {sync_time - async_time:.2f}秒 ({(1-async_time/sync_time)*100:.1f}%)")
        
    except Exception as e:
        print(f"对比测试失败: {e}")


def test_async_with_classification():
    """测试带分类功能的异步下载"""
    print("=== 测试带分类功能的异步下载 ===")
    
    try:
        # 创建启用分类的配置
        config_manager = ConfigManager('config.yaml')
        
        # 临时启用分类功能
        original_enabled = config_manager.config['classification']['enabled']
        config_manager.config['classification']['enabled'] = True
        
        task = {
            'stock_code': '00700',
            'start_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            'end_date': 'today',
            'keywords': [],
            'enabled': True
        }
        
        print(f"下载股票 {task['stock_code']} 最近30天的公告（启用分类）...")
        start_time = time.time()
        
        save_path, count = run_async_download(config_manager, task)
        
        elapsed = time.time() - start_time
        print(f"下载完成！耗时: {elapsed:.2f}秒")
        print(f"保存路径: {save_path}")
        print(f"下载文件数: {count}")
        print("文件已按公告类型自动分类到相应文件夹\n")
        
        # 恢复原配置
        config_manager.config['classification']['enabled'] = original_enabled
        
    except Exception as e:
        print(f"测试失败: {e}\n")


def main():
    """运行所有测试"""
    print("HKEX 异步下载功能测试\n")
    
    # 测试速率限制器
    test_rate_limiter()
    
    # 测试单个股票下载
    test_async_download_single()
    
    # 测试多股票批量下载
    test_async_download_multiple()
    
    # 对比同步和异步性能
    compare_sync_vs_async()
    
    # 测试带分类的异步下载
    test_async_with_classification()
    
    print("\n所有测试完成！")


if __name__ == '__main__':
    main()