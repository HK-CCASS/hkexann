"""
增强版公告处理器 - 主控制器
统一协调股票发现、API监听、双重过滤、下载、向量化的完整流程
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field

# 导入所有核心组件
try:
    # 尝试相对导入（当作为模块使用时）
    from .api_monitor import HKEXAPIMonitor
    from .dual_filter import DualAnnouncementFilter
    from .stock_discovery import StockDiscoveryManager
    from .downloader_integration import RealtimeDownloaderWrapper
    from .realtime_vector_processor import RealtimeVectorProcessor
    from .data_flow.corrected_historical_processor import CorrectedHistoricalProcessor
except ImportError:
    # 回退到绝对导入（当直接运行时）
    from services.monitor.api_monitor import HKEXAPIMonitor
    from services.monitor.dual_filter import DualAnnouncementFilter
    from services.monitor.stock_discovery import StockDiscoveryManager
    from services.monitor.downloader_integration import RealtimeDownloaderWrapper
    from services.monitor.realtime_vector_processor import RealtimeVectorProcessor
    from services.monitor.data_flow.corrected_historical_processor import CorrectedHistoricalProcessor

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    """处理统计信息"""
    session_start_time: datetime = field(default_factory=datetime.now)
    total_announcements_fetched: int = 0
    total_announcements_filtered: int = 0
    total_announcements_downloaded: int = 0
    total_announcements_vectorized: int = 0
    total_errors: int = 0
    last_sync_time: Optional[datetime] = None
    last_processing_time: Optional[datetime] = None
    monitored_stocks_count: int = 0
    stock_sync_count: int = 0
    
    def get_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        uptime = datetime.now() - self.session_start_time
        return {
            "session_uptime_seconds": uptime.total_seconds(),
            "session_uptime_formatted": str(uptime),
            "total_announcements_fetched": self.total_announcements_fetched,
            "total_announcements_filtered": self.total_announcements_filtered,
            "total_announcements_downloaded": self.total_announcements_downloaded,
            "total_announcements_vectorized": self.total_announcements_vectorized,
            "total_errors": self.total_errors,
            "monitored_stocks_count": self.monitored_stocks_count,
            "stock_sync_count": self.stock_sync_count,
            "filter_efficiency": (
                (self.total_announcements_fetched - self.total_announcements_filtered) / 
                max(self.total_announcements_fetched, 1) * 100
            ),
            "processing_success_rate": (
                self.total_announcements_vectorized / 
                max(self.total_announcements_filtered, 1) * 100
            ),
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "last_processing_time": self.last_processing_time.isoformat() if self.last_processing_time else None
        }


class EnhancedAnnouncementProcessor:
    """
    增强版公告处理器 - 主控制器
    
    统一协调以下组件的完整工作流程：
    1. StockDiscoveryManager - 从ClickHouse发现和同步监控股票列表
    2. HKEXAPIMonitor - 实时轮询HKEX公告API
    3. DualAnnouncementFilter - 双重过滤（股票+类型）
    4. RealtimeDownloaderWrapper - 异步下载PDF文件
    5. RealtimeVectorProcessor - 向量化处理和存储
    
    核心功能：
    - 自动股票列表同步和变化检测
    - 实时公告监听和过滤
    - 高效并发下载和向量化
    - 完整的错误处理和重试机制
    - 详细的统计和监控
    - 灵活的调度和配置管理
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化增强版公告处理器
        
        Args:
            config: 完整的配置字典
        """
        self.config = config
        self.stats = ProcessingStats()
        
        # 核心调度配置
        scheduler_config = config.get('scheduler', {})
        self.stock_sync_interval = scheduler_config.get('stock_sync_interval_hours', 6) * 3600  # 转为秒
        self.api_poll_interval = scheduler_config.get('api_poll_interval_seconds', 300)
        self.max_concurrent_processing = scheduler_config.get('max_concurrent_processing', 5)
        self.enable_auto_stock_sync = scheduler_config.get('enable_auto_stock_sync', True)
        self.enable_continuous_monitoring = scheduler_config.get('enable_continuous_monitoring', True)
        
        # 错误处理配置
        error_config = config.get('error_handling', {})
        self.max_consecutive_errors = error_config.get('max_consecutive_errors', 10)
        self.error_backoff_seconds = error_config.get('error_backoff_seconds', 60)
        self.enable_error_recovery = error_config.get('enable_error_recovery', True)
        
        # 状态变量
        self.is_running = False
        self.consecutive_errors = 0
        self.last_stock_sync = None
        self.monitored_stocks: Set[str] = set()
        
        # 初始化所有组件
        self._initialize_components()
        
        logger.info(f"增强版公告处理器初始化完成")
        logger.info(f"  股票同步间隔: {self.stock_sync_interval/3600:.1f}小时")
        logger.info(f"  API轮询间隔: {self.api_poll_interval}秒")
        logger.info(f"  最大并发处理: {self.max_concurrent_processing}")
        logger.info(f"  自动股票同步: {self.enable_auto_stock_sync}")
        logger.info(f"  持续监听: {self.enable_continuous_monitoring}")
    
    def _initialize_components(self):
        """初始化所有核心组件"""
        try:
            # 使用importlib直接导入真实的ClickHouse StockDiscoveryManager模块
            import importlib.util
            import os
            
            # 获取当前文件所在目录的stock_discovery.py文件路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            stock_discovery_path = os.path.join(current_dir, 'stock_discovery.py')
            
            # 直接导入真实模块，避开包级别的别名混淆
            spec = importlib.util.spec_from_file_location('real_stock_discovery', stock_discovery_path)
            real_stock_discovery = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(real_stock_discovery)
            
            StockDiscoveryManager = real_stock_discovery.StockDiscoveryManager
            
            # 从stock_discovery配置创建ClickHouse客户端
            stock_discovery_config = self.config.get('stock_discovery', {})
            
            if stock_discovery_config.get('enabled', False):
                logger.info(f"使用ClickHouse股票发现: {stock_discovery_config.get('host', 'localhost')}:{stock_discovery_config.get('port', 8124)}")
                self.stock_discovery = StockDiscoveryManager(stock_discovery_config)
            else:
                logger.warning("ClickHouse股票发现未启用，将使用后备股票列表")
                # 创建一个简单的后备股票发现器
                class FallbackStockDiscovery:
                    def __init__(self):
                        self.fallback_stocks = ['00700', '00941', '00939', '01398', '00388']  # 核心股票
                    
                    async def initialize(self):
                        return True
                    
                    async def discover_all_stocks(self):
                        logger.info(f"使用后备股票列表: {len(self.fallback_stocks)} 只")
                        return set(self.fallback_stocks)
                    
                    async def close(self):
                        pass
                
                self.stock_discovery = FallbackStockDiscovery()
            
            # 初始化API监听器
            self.api_monitor = HKEXAPIMonitor(
                self.config.get('api_monitor', {})
            )
            
            # 初始化下载器（立即初始化）
            self.downloader = RealtimeDownloaderWrapper(self.config)
            
            # 初始化向量化处理器
            self.vector_processor = RealtimeVectorProcessor(self.config)
            
            # 双重过滤器需要在获取股票列表后初始化
            self.dual_filter = None
            
            logger.info("✅ 所有核心组件初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 组件初始化失败: {e}")
            raise
    
    async def initialize(self) -> bool:
        """
        异步初始化所有组件
        
        Returns:
            初始化是否成功
        """
        try:
            logger.info("🚀 开始异步初始化所有组件")
            
            # 1. 初始化股票发现管理器
            logger.info("📊 初始化股票发现管理器...")
            if not await self.stock_discovery.initialize():
                logger.error("❌ 股票发现管理器初始化失败")
                return False
            
            # 2. 执行首次股票同步
            logger.info("🔍 执行首次股票同步...")
            await self._sync_monitored_stocks()
            
            # 3. 初始化双重过滤器
            logger.info("🔬 初始化双重过滤器...")
            self.dual_filter = DualAnnouncementFilter(
                self.monitored_stocks, 
                self.config.get('dual_filter', {})
            )
            
            # 4. 初始化API监听器
            logger.info("📡 初始化API监听器...")
            await self.api_monitor.initialize()
            
            # 5. 初始化历史批量处理器 (修复后的版本)
            logger.info("📚 初始化修复后的历史批量处理器...")
            self.historical_processor = CorrectedHistoricalProcessor(
                hkex_downloader=self.downloader.get_underlying_downloader(),
                dual_filter=self.dual_filter,
                vectorizer=self.vector_processor,
                monitored_stocks=self.monitored_stocks,
                config=self.config.get('historical_processing', {})
            )
            
            # 6. 检查并执行首次历史处理
            if await self.historical_processor.is_first_run():
                logger.info("🚀 检测到首次运行，开始处理历史公告...")
                historical_stats = await self.historical_processor.process_historical_announcements()
                logger.info(f"📊 历史处理完成: {historical_stats}")
                
                # 更新统计信息
                self.stats.total_announcements_downloaded += historical_stats.get('successfully_downloaded', 0)
                self.stats.total_announcements_vectorized += historical_stats.get('successfully_vectorized', 0)
            else:
                logger.info("ℹ️ 非首次运行，跳过历史公告处理")
            
            logger.info(f"✅ 系统初始化完成！监控 {len(self.monitored_stocks)} 只股票")
            return True
            
        except Exception as e:
            logger.error(f"❌ 系统初始化失败: {e}")
            return False
    
    async def _sync_monitored_stocks(self):
        """同步监控股票列表"""
        try:
            logger.info("🔄 开始同步监控股票列表")
            
            # 获取最新股票列表
            new_stocks = await self.stock_discovery.discover_all_stocks()
            
            # 检测变化
            if self.monitored_stocks:
                changes = await self.stock_discovery.detect_stock_changes()
                new_count = len(changes['new_stocks'])
                removed_count = len(changes['removed_stocks'])
                
                if new_count > 0 or removed_count > 0:
                    logger.info(f"📈 股票列表变化: 新增 {new_count}, 移除 {removed_count}")
                    
                    # 更新双重过滤器的股票列表
                    if self.dual_filter:
                        self.dual_filter.update_monitored_stocks(new_stocks)
                        logger.info("🔬 已更新过滤器股票列表")
            
            # 更新本地股票列表
            self.monitored_stocks = new_stocks
            self.stats.monitored_stocks_count = len(new_stocks)
            self.stats.stock_sync_count += 1
            self.stats.last_sync_time = datetime.now()
            self.last_stock_sync = datetime.now()
            
            logger.info(f"✅ 股票同步完成: {len(new_stocks)} 只股票")
            
        except Exception as e:
            logger.error(f"❌ 股票同步失败: {e}")
            self.stats.total_errors += 1
            raise
    
    async def _should_sync_stocks(self) -> bool:
        """检查是否需要同步股票"""
        if not self.enable_auto_stock_sync:
            return False
        
        if not self.last_stock_sync:
            return True
        
        time_since_sync = datetime.now() - self.last_stock_sync
        return time_since_sync.total_seconds() >= self.stock_sync_interval
    
    async def process_announcements_batch(self) -> Dict[str, Any]:
        """
        处理一批公告的完整流程
        
        Returns:
            处理结果统计
        """
        batch_start_time = datetime.now()
        batch_stats = {
            "batch_start_time": batch_start_time.isoformat(),
            "announcements_fetched": 0,
            "announcements_filtered": 0,
            "announcements_downloaded": 0,
            "announcements_vectorized": 0,
            "processing_time_seconds": 0,
            "errors": []
        }
        
        try:
            # 1. 检查是否需要股票同步
            if await self._should_sync_stocks():
                logger.info("⏰ 触发定时股票同步")
                await self._sync_monitored_stocks()
            
            # 2. 获取最新公告（传递监听股票列表）
            logger.info("📡 获取最新公告...")
            stock_codes = list(self.monitored_stocks) if self.monitored_stocks else []
            announcements = await self.api_monitor.fetch_latest_announcements(stock_codes)
            batch_stats["announcements_fetched"] = len(announcements)
            self.stats.total_announcements_fetched += len(announcements)
            
            if not announcements:
                logger.info("ℹ️  暂无新公告")
                return batch_stats
            
            logger.info(f"📥 获取到 {len(announcements)} 条公告")
            
            # 3. 双重过滤
            logger.info("🔬 执行双重过滤...")
            if not self.dual_filter:
                logger.error("❌ 双重过滤器未初始化")
                return batch_stats
            
            filtered_announcements = await self.dual_filter.filter_announcements(announcements)
            batch_stats["announcements_filtered"] = len(filtered_announcements)
            self.stats.total_announcements_filtered += len(filtered_announcements)
            
            if not filtered_announcements:
                logger.info("ℹ️  过滤后无相关公告")
                return batch_stats
            
            logger.info(f"✅ 过滤后得到 {len(filtered_announcements)} 条相关公告")
            
            # 4. 并发处理（下载+向量化）
            logger.info("⚡ 开始并发处理...")
            processing_results = await self._process_announcements_concurrent(filtered_announcements)
            
            # 统计处理结果
            for result in processing_results:
                if result.get('download_success'):
                    batch_stats["announcements_downloaded"] += 1
                    self.stats.total_announcements_downloaded += 1
                
                if result.get('vectorization_success'):
                    batch_stats["announcements_vectorized"] += 1
                    self.stats.total_announcements_vectorized += 1
                
                if result.get('error'):
                    batch_stats["errors"].append(result['error'])
                    self.stats.total_errors += 1
            
            # 重置连续错误计数
            self.consecutive_errors = 0
            
        except Exception as e:
            error_msg = f"批处理失败: {e}"
            logger.error(f"❌ {error_msg}")
            batch_stats["errors"].append(error_msg)
            self.stats.total_errors += 1
            self.consecutive_errors += 1
        
        finally:
            # 计算处理时间
            processing_time = (datetime.now() - batch_start_time).total_seconds()
            batch_stats["processing_time_seconds"] = processing_time
            self.stats.last_processing_time = datetime.now()
        
        return batch_stats
    
    async def _process_announcements_concurrent(self, announcements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        并发处理公告（下载+向量化）
        
        Args:
            announcements: 过滤后的公告列表
            
        Returns:
            处理结果列表
        """
        logger.info(f"🔄 开始并发处理 {len(announcements)} 个公告")
        
        # 限制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent_processing)
        
        async def process_single_announcement(announcement: Dict[str, Any]) -> Dict[str, Any]:
            """处理单个公告"""
            async with semaphore:
                result = {
                    "announcement_id": announcement.get('ID', 'Unknown'),
                    "stock_code": announcement.get('STOCK_CODE', 'Unknown'),
                    "title": announcement.get('TITLE', 'Unknown')[:50] + "...",
                    "download_success": False,
                    "vectorization_success": False,
                    "error": None
                }
                
                try:
                    # 下载PDF
                    download_result = await self.downloader.download_single_announcement(announcement)
                    
                    if download_result.get('success'):
                        result["download_success"] = True
                        result["file_path"] = download_result.get('file_path')
                        result["file_size"] = download_result.get('file_size', 0)
                        
                        # 向量化处理
                        vector_result = await self.vector_processor.process_announcement_pdf(
                            download_result['file_path'],
                            {
                                "announcement_id": announcement.get('ID'),
                                "stock_code": announcement.get('STOCK_CODE'),
                                "source": "enhanced_processor"
                            }
                        )
                        
                        if vector_result.get('success'):
                            result["vectorization_success"] = True
                            result["vectorized_chunks"] = vector_result.get('vectorized_chunks', 0)
                            logger.info(f"✅ 完整处理成功: {result['stock_code']} - {result['vectorized_chunks']} chunks")
                        else:
                            result["error"] = f"向量化失败: {vector_result.get('error')}"
                            logger.error(f"❌ 向量化失败: {result['stock_code']} - {result['error']}")
                    else:
                        result["error"] = f"下载失败: {download_result.get('error')}"
                        logger.error(f"❌ 下载失败: {result['stock_code']} - {result['error']}")
                
                except Exception as e:
                    result["error"] = f"处理异常: {str(e)}"
                    logger.error(f"❌ 处理异常: {result['stock_code']} - {result['error']}")
                
                return result
        
        # 并发执行所有任务
        tasks = [process_single_announcement(ann) for ann in announcements]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理异常结果
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append({
                    "announcement_id": announcements[i].get('ID', 'Unknown'),
                    "stock_code": announcements[i].get('STOCK_CODE', 'Unknown'),
                    "title": "Exception",
                    "download_success": False,
                    "vectorization_success": False,
                    "error": str(result)
                })
            else:
                final_results.append(result)
        
        # 统计结果
        success_count = sum(1 for r in final_results if r.get('vectorization_success'))
        logger.info(f"🎯 并发处理完成: {success_count}/{len(announcements)} 成功")
        
        return final_results
    
    async def run_continuous_monitoring(self):
        """
        运行持续监听模式
        """
        if not self.enable_continuous_monitoring:
            logger.warning("⚠️  持续监听模式已禁用")
            return
        
        logger.info("🔄 启动持续监听模式")
        self.is_running = True
        
        try:
            while self.is_running:
                # 检查连续错误
                if self.consecutive_errors >= self.max_consecutive_errors:
                    if self.enable_error_recovery:
                        logger.warning(f"⚠️  连续错误达到阈值 ({self.consecutive_errors})，等待 {self.error_backoff_seconds} 秒后重试")
                        await asyncio.sleep(self.error_backoff_seconds)
                        self.consecutive_errors = 0  # 重置错误计数
                    else:
                        logger.error(f"❌ 连续错误达到阈值 ({self.consecutive_errors})，停止监听")
                        break
                
                # 执行一轮处理
                batch_result = await self.process_announcements_batch()
                
                # 记录批次结果
                logger.info(f"📊 批次完成: "
                          f"获取 {batch_result['announcements_fetched']}, "
                          f"过滤 {batch_result['announcements_filtered']}, "
                          f"下载 {batch_result['announcements_downloaded']}, "
                          f"向量化 {batch_result['announcements_vectorized']}, "
                          f"耗时 {batch_result['processing_time_seconds']:.1f}秒")
                
                # 等待下次轮询
                if self.is_running:
                    logger.info(f"⏱️  等待 {self.api_poll_interval} 秒进行下次轮询")
                    await asyncio.sleep(self.api_poll_interval)
                    
        except KeyboardInterrupt:
            logger.info("⏹️  收到停止信号")
        except Exception as e:
            logger.error(f"❌ 持续监听异常: {e}")
        finally:
            self.is_running = False
            logger.info("🔚 持续监听模式已停止")
    
    async def run_single_batch(self) -> Dict[str, Any]:
        """
        运行单次批处理
        
        Returns:
            处理结果
        """
        logger.info("🚀 执行单次批处理")
        return await self.process_announcements_batch()
    
    def stop_monitoring(self):
        """停止持续监听"""
        logger.info("⏹️  请求停止持续监听")
        self.is_running = False
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            "system_info": {
                "is_running": self.is_running,
                "consecutive_errors": self.consecutive_errors,
                "monitored_stocks_count": len(self.monitored_stocks),
                "last_stock_sync": self.last_stock_sync.isoformat() if self.last_stock_sync else None
            },
            "configuration": {
                "stock_sync_interval_hours": self.stock_sync_interval / 3600,
                "api_poll_interval_seconds": self.api_poll_interval,
                "max_concurrent_processing": self.max_concurrent_processing,
                "enable_auto_stock_sync": self.enable_auto_stock_sync,
                "enable_continuous_monitoring": self.enable_continuous_monitoring
            },
            "component_status": {
                "stock_discovery": bool(self.stock_discovery),
                "api_monitor": bool(self.api_monitor),
                "dual_filter": bool(self.dual_filter),
                "downloader": bool(self.downloader),
                "vector_processor": bool(self.vector_processor)
            },
            "statistics": self.stats.get_summary()
        }
    
    async def close(self):
        """关闭所有组件和连接"""
        logger.info("🔚 关闭系统组件")
        
        self.is_running = False
        
        try:
            # 关闭股票发现组件
            if self.stock_discovery:
                await self.stock_discovery.close()
            
            # 关闭API监听器
            if self.api_monitor:
                await self.api_monitor.close()
            
            # 关闭下载器的HTTP客户端
            if self.downloader:
                underlying_downloader = self.downloader.get_underlying_downloader()
                if underlying_downloader:
                    await underlying_downloader.close()
                    logger.debug("✅ 下载器HTTP客户端已关闭")
            
            # 关闭嵌入服务的HTTP客户端
            try:
                from services.embeddings import get_embedding_client
                embedding_client = await get_embedding_client()
                if embedding_client:
                    await embedding_client.close()
                    logger.debug("✅ 嵌入服务HTTP客户端已关闭")
            except Exception as e:
                logger.warning(f"关闭嵌入服务客户端时出现警告: {e}")
            
            # 关闭向量处理器
            if self.vector_processor:
                try:
                    await self.vector_processor.close()
                    logger.debug("✅ 向量处理器已关闭")
                except Exception as e:
                    logger.warning(f"关闭向量处理器时出现警告: {e}")
            
            logger.info("✅ 所有组件已关闭")
            
        except Exception as e:
            logger.error(f"❌ 关闭组件时出错: {e}")


# 测试函数
async def test_enhanced_processor():
    """测试增强版公告处理器"""
    config = {
        # API监听配置
        'api_monitor': {
            'base_url': 'https://www1.hkexnews.hk/ncms/json/eds/lcisehk1relsdc_1.json',
            'poll_interval': 300,
            'timeout': 30,
            'max_retries': 3
        },
        
        # 双重过滤配置
        'dual_filter': {
            'enable_stock_filter': True,
            'enable_type_filter': True,
            'announcement_keywords': [
                '供股', '配售', '合股', '股份拆细', '可换股债券'
            ]
        },
        
        # ClickHouse股票发现配置
        'stock_discovery': {
            'host': 'localhost',
            'port': 8124,
            'database': 'hkex_analysis',
            'user': 'root',
            'password': '123456'
        },
        
        # 下载器配置
        'downloader_integration': {
            'use_existing_downloader': True,
            'download_directory': 'test_enhanced_downloads',
            'enable_filtering': False,
            'timeout': 30
        },
        'max_concurrent': 3,
        'requests_per_second': 1,
        
        # 向量化配置
        'vectorization_integration': {
            'use_existing_pipeline': True,
            'pdf_directory': 'test_enhanced_downloads',
            'collection_name': 'hkex_pdf_embeddings_enhanced',
            'batch_size': 10,
            'max_concurrent': 3
        },
        
        # 调度配置
        'scheduler': {
            'stock_sync_interval_hours': 6,
            'api_poll_interval_seconds': 60,  # 测试用较短间隔
            'max_concurrent_processing': 3,
            'enable_auto_stock_sync': True,
            'enable_continuous_monitoring': True
        },
        
        # 错误处理配置
        'error_handling': {
            'max_consecutive_errors': 5,
            'error_backoff_seconds': 30,
            'enable_error_recovery': True
        }
    }
    
    processor = None
    try:
        # 创建处理器
        processor = EnhancedAnnouncementProcessor(config)
        
        # 初始化
        if await processor.initialize():
            print("✅ 系统初始化成功")
            
            # 执行单次批处理测试
            result = await processor.run_single_batch()
            print(f"📊 批处理结果: {result}")
            
            # 获取系统状态
            status = processor.get_system_status()
            print(f"📈 系统状态: {status}")
            
        else:
            print("❌ 系统初始化失败")
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if processor:
            await processor.close()


if __name__ == "__main__":
    asyncio.run(test_enhanced_processor())
