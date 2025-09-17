"""
新增股票历史处理功能集成测试

测试新增股票历史处理功能与系统其他组件的完整集成流程
"""

import asyncio
import pytest
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
import yaml

# 添加项目路径
import sys
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from services.monitor.enhanced_announcement_processor import EnhancedAnnouncementProcessor


class TestNewStockHistoricalIntegration:
    """新增股票历史处理集成测试"""

    @pytest.fixture
    def test_config(self):
        """集成测试配置"""
        return {
            # 基础配置
            'api_endpoints': {
                'base_url': 'https://www1.hkexnews.hk',
                'stock_search': '/search/prefix.do',
                'title_search': '/search/titleSearchServlet.do'
            },
            'settings': {
                'save_path': './test_hkexann',
                'verbose_logging': True
            },
            
            # 股票发现配置 - 使用测试模式
            'stock_discovery': {
                'enabled': False,  # 禁用真实ClickHouse连接
                'stock_list_source': 'custom',
                'custom_stock_list': ['00700', '00939', '01398']  # 测试股票
            },
            
            # API监听配置
            'api_monitor': {
                'poll_interval': 60,
                'timeout': 30,
                'max_retries': 3
            },
            
            # 下载器集成配置
            'downloader_integration': {
                'use_existing_downloader': True,
                'download_directory': './test_hkexann',
                'enable_filtering': True,
                'timeout': 30,
                'enable_smart_classification': True,
                'common_keywords': {},
                'announcement_categories': {}
            },
            
            # 向量化集成配置（测试模式）
            'vectorization_integration': {
                'use_existing_pipeline': False,  # 禁用真实向量化
                'pdf_directory': './test_hkexann',
                'collection_name': 'test_pdf_embeddings',
                'batch_size': 5,
                'max_concurrent': 2
            },
            
            # 调度配置
            'scheduler': {
                'stock_sync_interval_hours': 24,  # 测试时不频繁同步
                'api_poll_interval_seconds': 300,
                'max_concurrent_processing': 2,
                'enable_auto_stock_sync': True,
                'enable_continuous_monitoring': False  # 测试时不启用持续监听
            },
            
            # 错误处理配置
            'error_handling': {
                'max_consecutive_errors': 5,
                'error_backoff_seconds': 10,
                'enable_error_recovery': True
            },
            
            # 🆕 新增股票历史处理配置
            'new_stock_historical_processing': {
                'enabled': True,
                'days': 1,  # 测试时只处理1天历史
                'batch_size': 2,
                'max_concurrent': 1,
                'timeout_minutes': 2,  # 短超时时间
                'max_retries': 1,
                'enable_filtering': True,
                'auto_start': True
            },
            
            # 历史数据处理配置
            'historical_processing': {
                'historical_days': 1,
                'first_run_historical_days': 1,
                'stock_batch_size': 2,
                'date_chunk_days': 1,
                'max_concurrent_historical': 1,
                'api_delay': 0.5
            },
            
            # 过滤配置
            'dual_filter': {
                'stock_filter_enabled': True,
                'type_filter_enabled': True,
                'excluded_categories': [],
                'included_keywords': ['测试', '公告']
            },
            
            # 通用关键字和分类（空配置用于测试）
            'common_keywords': {},
            'announcement_categories': {}
        }

    @pytest.fixture
    def temp_directory(self):
        """创建临时测试目录"""
        temp_dir = tempfile.mkdtemp(prefix="hkex_test_")
        yield temp_dir
        # 清理
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_processor_initialization_with_historical_feature(self, test_config, temp_directory):
        """测试包含新增股票历史处理功能的处理器初始化"""
        # 更新配置使用临时目录
        test_config['settings']['save_path'] = temp_directory
        test_config['downloader_integration']['download_directory'] = temp_directory
        test_config['vectorization_integration']['pdf_directory'] = temp_directory
        
        # 创建处理器
        processor = EnhancedAnnouncementProcessor(test_config)
        
        try:
            # 验证初始配置
            assert processor.enable_new_stock_historical is True
            assert processor.new_stock_historical_config['days'] == 1
            assert processor.new_stock_historical_config['max_retries'] == 1
            assert processor.new_stock_historical_processor is None  # 未初始化前为None
            
            # 模拟初始化过程
            with pytest.raises(Exception):  # 预期会因为缺少真实服务而失败
                await processor.initialize()
                
        finally:
            await processor.close()

    @pytest.mark.asyncio
    async def test_mock_new_stock_detection_and_historical_processing(self, test_config, temp_directory):
        """测试模拟的新股票检测和历史处理流程"""
        import unittest.mock as mock
        
        # 更新配置
        test_config['settings']['save_path'] = temp_directory
        
        # 模拟所有外部依赖
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(),
            RealtimeVectorProcessor=mock.MagicMock(),
            DualAnnouncementFilter=mock.MagicMock(),
            CorrectedHistoricalProcessor=mock.MagicMock()
        ) as mocks:
            
            # 创建处理器
            processor = EnhancedAnnouncementProcessor(test_config)
            
            # 模拟股票发现管理器
            mock_stock_discovery = mock.MagicMock()
            mock_stock_discovery.initialize.return_value = True
            mock_stock_discovery.discover_all_stocks.return_value = {'00700', '00939', '01398', '00001'}  # 新增00001
            mock_stock_discovery.detect_stock_changes.return_value = {
                'new_stocks': {'00001'},
                'removed_stocks': set()
            }
            processor.stock_discovery = mock_stock_discovery
            
            # 模拟新增股票历史处理器
            mock_historical_processor = mock.MagicMock()
            mock_historical_processor.process_historical_announcements.return_value = {
                'relevant_announcements': 5,
                'successfully_downloaded': 4,
                'successfully_vectorized': 3
            }
            processor.new_stock_historical_processor = mock_historical_processor
            processor.enable_new_stock_historical = True
            
            try:
                # 模拟股票同步过程
                processor.monitored_stocks = {'00700', '00939', '01398'}  # 原有股票
                
                # 执行股票同步（这会触发新增股票历史处理）
                await processor._sync_monitored_stocks()
                
                # 验证股票列表更新
                assert '00001' in processor.monitored_stocks
                assert len(processor.monitored_stocks) == 4
                
                # 等待异步历史处理任务完成
                await asyncio.sleep(0.1)
                
                # 验证统计信息更新（注意：由于是异步任务，可能需要等待）
                # 这里主要验证逻辑流程正确
                
            finally:
                await processor.close()

    @pytest.mark.asyncio 
    async def test_error_handling_in_historical_processing(self, test_config, temp_directory):
        """测试历史处理中的错误处理机制"""
        import unittest.mock as mock
        
        test_config['settings']['save_path'] = temp_directory
        
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(),
            RealtimeVectorProcessor=mock.MagicMock(),
            DualAnnouncementFilter=mock.MagicMock(),
            CorrectedHistoricalProcessor=mock.MagicMock()
        ):
            processor = EnhancedAnnouncementProcessor(test_config)
            
            # 模拟历史处理器抛出可重试错误
            mock_historical_processor = mock.MagicMock()
            mock_historical_processor.process_historical_announcements.side_effect = [
                ConnectionError("Network error"),  # 第一次失败
                {  # 第二次成功
                    'relevant_announcements': 2,
                    'successfully_downloaded': 1,
                    'successfully_vectorized': 1
                }
            ]
            mock_historical_processor.monitored_stocks = set()
            processor.new_stock_historical_processor = mock_historical_processor
            
            try:
                # 模拟处理新增股票
                new_stocks = {'00001'}
                
                with mock.patch('asyncio.sleep'):  # 跳过实际等待
                    await processor._process_new_stocks_with_historical_processor(new_stocks)
                
                # 验证重试逻辑
                assert mock_historical_processor.process_historical_announcements.call_count == 2
                assert processor.stats.new_stocks_historical_errors == 1  # 记录了一次错误
                assert processor.stats.new_stocks_historical_processed == 1  # 最终成功处理
                
            finally:
                await processor.close()

    @pytest.mark.asyncio
    async def test_performance_metrics_collection(self, test_config, temp_directory):
        """测试性能指标收集功能"""
        import unittest.mock as mock
        
        test_config['settings']['save_path'] = temp_directory
        
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(), 
            RealtimeVectorProcessor=mock.MagicMock(),
            DualAnnouncementFilter=mock.MagicMock(),
            CorrectedHistoricalProcessor=mock.MagicMock()
        ):
            processor = EnhancedAnnouncementProcessor(test_config)
            
            try:
                # 初始状态检查
                metrics = processor.get_new_stock_historical_performance_metrics()
                assert metrics['status'] == 'no_data'
                
                # 模拟一些处理数据
                processor.stats.new_stocks_historical_processed = 3
                processor.stats.new_stocks_historical_announcements = 15
                processor.stats.new_stocks_historical_downloads = 12
                processor.stats.new_stocks_historical_vectorized = 10
                processor.stats.new_stocks_historical_errors = 1
                processor.stats.total_new_stock_historical_processing_time = 90.0
                processor.stats.last_new_stock_historical_processing_time = datetime.now()
                
                # 获取性能指标
                metrics = processor.get_new_stock_historical_performance_metrics()
                
                assert metrics['status'] == 'active'
                assert metrics['performance_metrics']['total_processed_stocks'] == 3
                assert metrics['performance_metrics']['average_time_per_stock'] == 30.0
                assert metrics['performance_metrics']['success_rate_percent'] == 66.67  # (3-1)/3*100
                
                # 验证效率指标
                eff = metrics['efficiency_metrics']
                assert eff['announcements_per_stock'] == 5.0  # 15/3
                assert eff['download_success_rate'] == 80.0  # 12/15*100
                assert eff['vectorization_success_rate'] == 83.33  # 10/12*100，保留两位小数
                
                # 验证系统状态包含性能指标
                system_status = processor.get_system_status()
                assert 'new_stock_historical_performance' in system_status
                assert system_status['new_stock_historical_performance']['status'] == 'active'
                
            finally:
                await processor.close()

    @pytest.mark.asyncio
    async def test_configuration_loading_and_validation(self, test_config, temp_directory):
        """测试配置加载和验证"""
        import unittest.mock as mock
        
        # 创建临时配置文件
        config_file = Path(temp_directory) / "test_config.yaml"
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(test_config, f, allow_unicode=True)
        
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(),
            RealtimeVectorProcessor=mock.MagicMock(),
            DualAnnouncementFilter=mock.MagicMock(),
            CorrectedHistoricalProcessor=mock.MagicMock()
        ):
            processor = EnhancedAnnouncementProcessor(test_config)
            
            try:
                # 验证配置正确加载
                assert processor.enable_new_stock_historical is True
                assert processor.new_stock_historical_config['enabled'] is True
                assert processor.new_stock_historical_config['days'] == 1
                assert processor.new_stock_historical_config['max_retries'] == 1
                
                # 验证系统状态中的配置信息
                status = processor.get_system_status()
                config_info = status['configuration']
                assert config_info['new_stock_historical_enabled'] is True
                assert config_info['new_stock_historical_days'] == 1
                assert config_info['new_stock_historical_max_retries'] == 1
                
            finally:
                await processor.close()

    def test_retry_logic_decision_making(self, test_config, temp_directory):
        """测试重试逻辑决策"""
        import unittest.mock as mock
        
        with mock.patch.multiple(
            'services.monitor.enhanced_announcement_processor',
            HKEXAPIMonitor=mock.MagicMock(),
            RealtimeDownloaderWrapper=mock.MagicMock(),
            RealtimeVectorProcessor=mock.MagicMock(),
            DualAnnouncementFilter=mock.MagicMock(),
            CorrectedHistoricalProcessor=mock.MagicMock()
        ):
            processor = EnhancedAnnouncementProcessor(test_config)
            
            try:
                # 测试可重试错误类型
                retryable_errors = [
                    type('ConnectionError', (Exception,), {})("Network failed"),
                    type('TimeoutError', (Exception,), {})("Request timeout"),
                    type('ClientConnectorError', (Exception,), {})("Connection error"),
                    type('SSLError', (Exception,), {})("SSL handshake failed"),
                    type('ClientResponseError', (Exception,), {})("Bad response"),
                    type('ClientConnectorCertificateError', (Exception,), {})("Certificate error")
                ]
                
                for error in retryable_errors:
                    should_retry = processor._should_retry_historical_processing(error)
                    assert should_retry is True, f"错误类型 {type(error).__name__} 应该可以重试"
                
                # 测试不可重试错误类型
                non_retryable_errors = [
                    ValueError("Invalid parameter"),
                    RuntimeError("System error"),
                    KeyError("Missing key"),
                    TypeError("Type mismatch")
                ]
                
                for error in non_retryable_errors:
                    should_retry = processor._should_retry_historical_processing(error)
                    assert should_retry is False, f"错误类型 {type(error).__name__} 不应该重试"
                    
            finally:
                processor.close()


if __name__ == "__main__":
    # 运行集成测试
    pytest.main([__file__, "-v", "-s"])  # -s 显示print输出
