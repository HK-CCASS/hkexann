"""
新增股票历史处理功能单元测试

测试实例化复用模式的新增股票历史处理功能的各个组件和方法
"""

import asyncio
import pytest
import unittest.mock as mock
from datetime import datetime, timedelta
from typing import Set, Dict, Any

# 添加项目路径
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from services.monitor.enhanced_announcement_processor import EnhancedAnnouncementProcessor, ProcessingStats


class TestNewStockHistoricalProcessing:
    """新增股票历史处理功能测试"""

    @pytest.fixture
    def mock_config(self):
        """模拟配置"""
        return {
            'new_stock_historical_processing': {
                'enabled': True,
                'days': 3,
                'batch_size': 5,
                'max_concurrent': 2,
                'timeout_minutes': 10,
                'max_retries': 2,
                'enable_filtering': True,
                'auto_start': True
            },
            'scheduler': {
                'stock_sync_interval_hours': 0.5,
                'api_poll_interval_seconds': 60,
                'max_concurrent_processing': 5,
                'enable_auto_stock_sync': True,
                'enable_continuous_monitoring': True
            },
            'error_handling': {
                'max_consecutive_errors': 10,
                'error_backoff_seconds': 60,
                'enable_error_recovery': True
            },
            'common_keywords': {},
            'announcement_categories': {}
        }

    @pytest.fixture
    def mock_processor(self, mock_config):
        """创建模拟的增强公告处理器"""
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            StockDiscoveryManager=mock.MagicMock(),
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(),
            RealtimeVectorProcessor=mock.MagicMock(),
            DualAnnouncementFilter=mock.MagicMock(),
            CorrectedHistoricalProcessor=mock.MagicMock()
        ):
            processor = EnhancedAnnouncementProcessor(mock_config)
            # 模拟已初始化状态
            processor.monitored_stocks = {'00700', '00939', '01398'}
            processor.new_stock_historical_processor = mock.MagicMock()
            return processor

    def test_init_with_new_stock_historical_config(self, mock_config):
        """测试初始化时正确加载新增股票历史处理配置"""
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            StockDiscoveryManager=mock.MagicMock(),
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(),
            RealtimeVectorProcessor=mock.MagicMock()
        ):
            processor = EnhancedAnnouncementProcessor(mock_config)
            
            assert processor.enable_new_stock_historical is True
            assert processor.new_stock_historical_config['days'] == 3
            assert processor.new_stock_historical_config['max_retries'] == 2
            assert processor.new_stock_historical_processor is None  # 初始为None

    def test_processing_stats_extension(self):
        """测试ProcessingStats类的新增字段"""
        stats = ProcessingStats()
        
        # 验证新增字段存在且初始值正确
        assert stats.new_stocks_historical_processed == 0
        assert stats.new_stocks_historical_announcements == 0
        assert stats.new_stocks_historical_downloads == 0
        assert stats.new_stocks_historical_vectorized == 0
        assert stats.new_stocks_historical_errors == 0
        assert stats.last_new_stock_historical_processing_time is None
        assert stats.total_new_stock_historical_processing_time == 0.0

    def test_processing_stats_summary_includes_historical(self):
        """测试统计摘要包含新增股票历史处理数据"""
        stats = ProcessingStats()
        stats.new_stocks_historical_processed = 5
        stats.new_stocks_historical_announcements = 20
        stats.total_new_stock_historical_processing_time = 150.0
        
        summary = stats.get_summary()
        
        assert 'new_stock_historical' in summary
        historical_stats = summary['new_stock_historical']
        assert historical_stats['processed_stocks'] == 5
        assert historical_stats['total_announcements'] == 20
        assert historical_stats['avg_processing_time_per_stock'] == 30.0

    def test_should_retry_historical_processing(self, mock_processor):
        """测试历史处理错误重试判断逻辑"""
        # 可重试的错误
        retryable_errors = [
            ConnectionError("Connection failed"),
            TimeoutError("Request timeout"),
            Exception("ClientConnectorCertificateError: SSL error")
        ]
        
        for error in retryable_errors:
            # 模拟错误类型
            error.__class__.__name__ = error.__class__.__name__.replace('Exception', 'ClientConnectorCertificateError')
            should_retry = mock_processor._should_retry_historical_processing(error)
            assert should_retry is True, f"错误 {error} 应该可以重试"

        # 不可重试的错误
        non_retryable_errors = [
            ValueError("Invalid parameter"),
            RuntimeError("System error"),
            KeyError("Missing key")
        ]
        
        for error in non_retryable_errors:
            should_retry = mock_processor._should_retry_historical_processing(error)
            assert should_retry is False, f"错误 {error} 不应该重试"

    def test_update_new_stock_historical_stats(self, mock_processor):
        """测试统计信息更新方法"""
        new_stocks = {'00700', '00939'}
        result = {
            'relevant_announcements': 10,
            'successfully_downloaded': 8,
            'successfully_vectorized': 7
        }
        processing_time = 45.0
        
        initial_processed = mock_processor.stats.new_stocks_historical_processed
        
        mock_processor._update_new_stock_historical_stats(new_stocks, result, processing_time)
        
        # 验证统计信息更新
        assert mock_processor.stats.new_stocks_historical_processed == initial_processed + 2
        assert mock_processor.stats.new_stocks_historical_announcements == 10
        assert mock_processor.stats.new_stocks_historical_downloads == 8
        assert mock_processor.stats.new_stocks_historical_vectorized == 7
        assert mock_processor.stats.total_new_stock_historical_processing_time == 45.0
        assert mock_processor.stats.last_new_stock_historical_processing_time is not None

    def test_get_new_stock_historical_performance_metrics_no_data(self, mock_processor):
        """测试无数据时的性能指标获取"""
        metrics = mock_processor.get_new_stock_historical_performance_metrics()
        
        assert metrics['status'] == 'no_data'
        assert 'message' in metrics

    def test_get_new_stock_historical_performance_metrics_with_data(self, mock_processor):
        """测试有数据时的性能指标获取"""
        # 设置测试数据
        mock_processor.stats.new_stocks_historical_processed = 5
        mock_processor.stats.new_stocks_historical_announcements = 25
        mock_processor.stats.new_stocks_historical_downloads = 20
        mock_processor.stats.new_stocks_historical_vectorized = 18
        mock_processor.stats.new_stocks_historical_errors = 1
        mock_processor.stats.total_new_stock_historical_processing_time = 150.0
        mock_processor.stats.last_new_stock_historical_processing_time = datetime.now()
        
        metrics = mock_processor.get_new_stock_historical_performance_metrics()
        
        assert metrics['status'] == 'active'
        
        perf = metrics['performance_metrics']
        assert perf['total_processed_stocks'] == 5
        assert perf['average_time_per_stock'] == 30.0
        assert perf['success_rate_percent'] == 80.0  # (5-1)/5 * 100
        
        eff = metrics['efficiency_metrics']
        assert eff['announcements_per_stock'] == 5.0  # 25/5
        assert eff['download_success_rate'] == 80.0  # 20/25 * 100
        assert eff['vectorization_success_rate'] == 90.0  # 18/20 * 100

    def test_get_system_status_includes_historical_performance(self, mock_processor):
        """测试系统状态包含历史处理性能指标"""
        status = mock_processor.get_system_status()
        
        assert 'new_stock_historical_performance' in status
        assert 'configuration' in status
        
        config = status['configuration']
        assert 'new_stock_historical_enabled' in config
        assert 'new_stock_historical_days' in config
        assert 'new_stock_historical_max_retries' in config
        
        components = status['component_status']
        assert 'new_stock_historical_processor' in components

    @pytest.mark.asyncio
    async def test_process_new_stocks_with_historical_processor_success(self, mock_processor):
        """测试新增股票历史处理成功流程"""
        new_stocks = {'00001', '00002'}
        mock_result = {
            'relevant_announcements': 5,
            'successfully_downloaded': 4,
            'successfully_vectorized': 3
        }
        
        # 模拟历史处理器成功返回
        mock_processor.new_stock_historical_processor.process_historical_announcements.return_value = mock_result
        mock_processor.new_stock_historical_processor.monitored_stocks = set()
        
        # 执行测试
        await mock_processor._process_new_stocks_with_historical_processor(new_stocks)
        
        # 验证调用
        mock_processor.new_stock_historical_processor.process_historical_announcements.assert_called_once()
        
        # 验证统计更新
        assert mock_processor.stats.new_stocks_historical_processed == 2
        assert mock_processor.stats.new_stocks_historical_announcements == 5

    @pytest.mark.asyncio
    async def test_process_new_stocks_with_timeout_retry(self, mock_processor):
        """测试超时重试机制"""
        new_stocks = {'00001'}
        
        # 模拟第一次超时，第二次成功
        mock_processor.new_stock_historical_processor.process_historical_announcements.side_effect = [
            asyncio.TimeoutError("Request timeout"),
            {'relevant_announcements': 1, 'successfully_downloaded': 1, 'successfully_vectorized': 1}
        ]
        mock_processor.new_stock_historical_processor.monitored_stocks = set()
        
        # 修改配置以加速测试
        mock_processor.new_stock_historical_config['timeout_minutes'] = 0.001  # 很短的超时
        mock_processor.new_stock_historical_config['max_retries'] = 1
        
        # 执行测试（使用mock的sleep避免实际等待）
        with mock.patch('asyncio.sleep'):
            await mock_processor._process_new_stocks_with_historical_processor(new_stocks)
        
        # 验证重试调用
        assert mock_processor.new_stock_historical_processor.process_historical_announcements.call_count == 2
        assert mock_processor.stats.new_stocks_historical_errors == 1  # 一次超时错误

    @pytest.mark.asyncio
    async def test_process_new_stocks_with_non_retryable_error(self, mock_processor):
        """测试不可重试错误的处理"""
        new_stocks = {'00001'}
        
        # 模拟不可重试的错误
        mock_processor.new_stock_historical_processor.process_historical_announcements.side_effect = ValueError("Invalid parameter")
        mock_processor.new_stock_historical_processor.monitored_stocks = set()
        
        # 执行测试
        await mock_processor._process_new_stocks_with_historical_processor(new_stocks)
        
        # 验证只调用一次（不重试）
        assert mock_processor.new_stock_historical_processor.process_historical_announcements.call_count == 1
        assert mock_processor.stats.new_stocks_historical_errors == 1


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
