#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
异步下载器单元测试
测试异步下载功能的各个组件
"""

import asyncio
import pytest
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from async_downloader import RateLimiter, AsyncHKEXDownloader, run_async_download
from main import ConfigManager


@pytest.fixture
def config_manager():
    """创建测试用的配置管理器"""
    config = {
        'async': {
            'enabled': True,
            'max_concurrent': 5,
            'requests_per_second': 2,
            'timeout': 10,
            'min_delay': 0.1,
            'max_delay': 0.2,
            'backoff_on_429': 1
        },
        'settings': {
            'save_path': '/tmp/test_hkex',
            'language': 'zh',
            'max_results': 100,
            'filename_length': 200,
            'verbose_logging': True
        },
        'advanced': {
            'overwrite_existing': False
        },
        'classification': {
            'enabled': False
        }
    }
    
    mock_config = MagicMock(spec=ConfigManager)
    mock_config.config = config
    mock_config.get.side_effect = lambda *args: config.get(args[0]) if len(args) == 1 else config.get(args[0], {}).get(args[1], args[2] if len(args) > 2 else None)
    mock_config.parse_date = lambda date_str: datetime.now() if date_str == 'today' else datetime.strptime(date_str, '%Y-%m-%d')
    
    return mock_config


class TestRateLimiter:
    """测试速率限制器"""
    
    @pytest.mark.asyncio
    async def test_rate_limiter_basic(self):
        """测试基本速率限制功能"""
        limiter = RateLimiter(rate=2, per=1.0)  # 每秒2个请求
        
        start_time = asyncio.get_event_loop().time()
        
        # 快速发出3个请求
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()  # 第3个请求应该被延迟
        
        end_time = asyncio.get_event_loop().time()
        elapsed = end_time - start_time
        
        # 第3个请求应该在约0.5秒后执行
        assert elapsed >= 0.4, f"Expected delay, but only took {elapsed:.2f}s"
    
    @pytest.mark.asyncio
    async def test_rate_limiter_concurrent(self):
        """测试并发请求的速率限制"""
        limiter = RateLimiter(rate=5, per=1.0)  # 每秒5个请求
        
        async def make_request(i):
            await limiter.acquire()
            return i
        
        start_time = asyncio.get_event_loop().time()
        
        # 并发发出10个请求
        tasks = [make_request(i) for i in range(10)]
        results = await asyncio.gather(*tasks)
        
        end_time = asyncio.get_event_loop().time()
        elapsed = end_time - start_time
        
        # 10个请求，每秒5个，应该需要约2秒
        assert elapsed >= 1.8, f"Expected ~2s delay, but only took {elapsed:.2f}s"
        assert len(results) == 10


class TestAsyncHKEXDownloader:
    """测试异步下载器"""
    
    @pytest.mark.asyncio
    async def test_init(self, config_manager):
        """测试初始化"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            assert downloader.max_concurrent == 5
            assert downloader.requests_per_second == 2
            assert downloader.async_timeout == 10
            assert downloader._session is not None
    
    @pytest.mark.asyncio
    async def test_context_manager(self, config_manager):
        """测试上下文管理器"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            assert downloader._session is not None
            assert not downloader._session.closed
        
        # 退出上下文后，会话应该被关闭
        await asyncio.sleep(0.2)  # 等待清理完成
    
    @pytest.mark.asyncio
    async def test_async_get_stockid(self, config_manager):
        """测试异步获取股票ID"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            # Mock HTTP响应
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read.return_value = b'callback({"stockInfo":[{"stockId":"12345"}]})'
            
            with patch.object(downloader._session, 'get', return_value=mock_response):
                stock_id = downloader.async_get_stockid('00700')
                assert stock_id == '12345'
    
    @pytest.mark.asyncio
    async def test_async_get_announcement_list(self, config_manager):
        """测试异步获取公告列表"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            # Mock get_stockid
            with patch.object(downloader, 'async_get_stockid', return_value='12345'):
                # Mock HTTP响应
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.read.return_value = b'''{"result":[{
                    "TITLE": "Test Announcement",
                    "FILE_LINK": "/path/to/pdf",
                    "DATE_TIME": "01/07/2025"
                }]}'''
                
                with patch.object(downloader._session, 'get', return_value=mock_response):
                    announcements = downloader.async_get_announcement_list(
                        '00700',
                        datetime(2025, 1, 1),
                        datetime(2025, 7, 31)
                    )
                    
                    assert len(announcements) == 1
                    assert announcements[0]['title'] == 'Test Announcement'
                    assert announcements[0]['link'] == 'https://www1.hkexnews.hk/path/to/pdf'
                    assert announcements[0]['date'] == '2025-07-01'
    
    @pytest.mark.asyncio
    async def test_async_download_file(self, config_manager):
        """测试异步下载文件"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            # Mock HTTP响应
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read.return_value = b'PDF content here'
            
            with patch.object(downloader._session, 'get', return_value=mock_response):
                with patch('aiofiles.open', create=True) as mock_open:
                    mock_file = AsyncMock()
                    mock_open.return_value.__aenter__.return_value = mock_file
                    
                    success = await downloader.async_download_file(
                        'https://example.com/test.pdf',
                        '/tmp/test.pdf'
                    )
                    
                    assert success is True
                    mock_file.write.assert_called_once_with(b'PDF content here')
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self, config_manager):
        """测试请求速率限制"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            # Mock HTTP响应
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.read.return_value = b'Test content'
            mock_response.raise_for_status = AsyncMock()
            
            with patch.object(downloader._session, 'get', return_value=mock_response) as mock_get:
                # 快速发出多个请求
                start_time = asyncio.get_event_loop().time()
                
                tasks = []
                for i in range(5):
                    task = downloader._fetch_with_retry(f'https://example.com/test{i}')
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks)
                
                end_time = asyncio.get_event_loop().time()
                elapsed = end_time - start_time
                
                # 5个请求，每秒2个，应该需要至少2秒
                assert elapsed >= 2.0, f"Rate limiting not working, took only {elapsed:.2f}s"
                assert len(results) == 5
    
    @pytest.mark.asyncio
    async def test_handle_429_status(self, config_manager):
        """测试处理429状态码（请求过多）"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            # Mock HTTP响应返回429
            mock_response = AsyncMock()
            mock_response.status = 429
            mock_response.raise_for_status = AsyncMock(side_effect=Exception("429"))
            
            with patch.object(downloader._session, 'get', return_value=mock_response):
                with pytest.raises(Exception):
                    await downloader._fetch_with_retry('https://example.com/test')
                
                # 检查统计信息
                assert downloader.stats['rate_limited'] > 0


class TestRunAsyncDownload:
    """测试异步下载运行函数"""
    
    def test_run_async_download(self, config_manager):
        """测试同步包装器函数"""
        task = {
            'stock_code': '00700',
            'start_date': '2025-01-01',
            'end_date': '2025-07-31'
        }
        
        with patch('async_downloader.AsyncHKEXDownloader') as mock_downloader_class:
            mock_downloader = AsyncMock()
            mock_downloader.async_download_announcements.return_value = ('/tmp/HKEX/00700', 10)
            mock_downloader_class.return_value = mock_downloader
            
            # 测试正常运行
            save_path, count = run_async_download(config_manager, task)
            
            assert save_path == '/tmp/HKEX/00700'
            assert count == 10


class TestAsyncBatchDownload:
    """测试批量异步下载"""
    
    @pytest.mark.asyncio
    async def test_download_multiple_stocks(self, config_manager):
        """测试下载多个股票"""
        async with AsyncHKEXDownloader(config_manager) as downloader:
            task = {
                'start_date': '2025-01-01',
                'end_date': '2025-07-31'
            }
            stock_codes = ['00700', '00941', '09988']
            
            # Mock 单个股票下载
            async def mock_download_stock(task):
                return f"/tmp/HKEX/{task['stock_code']}", 5
            
            with patch.object(downloader, 'async_download_stock_announcements', side_effect=mock_download_stock):
                save_path, total_count = await downloader.async_download_multiple_stocks(task, stock_codes)
                
                assert save_path == '/tmp/test_hkex/HKEX'
                assert total_count == 15  # 3 stocks × 5 files each
                assert downloader.stats['success'] == 3


if __name__ == '__main__':
    # 运行测试
    pytest.main([__file__, '-v', '--asyncio-mode=auto'])