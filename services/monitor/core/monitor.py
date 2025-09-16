#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告实时监听服务核心模块
实现每60秒监听指定股票列表的公告变化
"""

import asyncio
import logging
import os
from pathlib import Path
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import aiofiles
from tenacity import retry, stop_after_attempt, wait_exponential

# 导入项目模块
from main import ConfigManager, DatabaseManager
from async_downloader import AsyncHKEXDownloader
from ..state.tracker import StockTracker
from ..detection.detector import ChangeDetector
from ..utils import ErrorHandler, MonitorLogger, StatisticsCollector, LogLevel


@dataclass
class AnnouncementInfo:
    """公告信息数据类"""
    stock_code: str
    title: str
    date_time: str
    long_text: str
    file_link: str
    hash_key: str


class AnnouncementMonitor:
    """HKEX公告实时监听服务"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化监听服务"""
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.get_config()
        self.monitor_config = self.config.get('monitor', {})
        
        # 检查监听服务是否启用
        if not self.monitor_config.get('enabled', False):
            raise ValueError("监听服务未启用，请在config.yaml中设置monitor.enabled=true")
        
        # 初始化组件
        self._init_logging()
        self._init_statistics()
        self._init_error_handler()
        self._init_database()
        self._init_downloader()
        self._init_tracker()
        self._init_detector()
        
        # 监听状态
        self.is_running = False
        self.last_check_time = None
        self.error_count = 0
        self.stats = {
            'checked': 0,
            'found': 0, 
            'downloaded': 0,
            'processed': 0,
            'errors': 0
        }
        
        # 获取配置
        schedule_config = self.monitor_config.get('schedule', {})
        self.check_interval = schedule_config.get('check_interval', 60)
        self.retry_interval = schedule_config.get('retry_interval', 30)
        self.max_retries = schedule_config.get('max_retries', 3)
        
        # 错误处理配置
        error_config = self.monitor_config.get('monitoring', {}).get('error_handling', {})
        self.error_threshold = error_config.get('error_threshold', 10)
        
        logging.info(f"公告监听服务初始化完成，检查间隔: {self.check_interval}秒")
        
    def _init_logging(self):
        """初始化日志配置"""
        try:
            self.monitor_logger = MonitorLogger(self.config)
            # 保持向后兼容
            self.logger = self.monitor_logger.system_logger
            self.monitor_logger.log_system(LogLevel.INFO, "监听服务日志系统初始化完成")
        except Exception as e:
            # 降级到标准日志
            self.logger = logging.getLogger('monitor')
            self.logger.error(f"MonitorLogger初始化失败，使用标准日志: {e}")
    
    def _init_statistics(self):
        """初始化统计收集器"""
        try:
            self.stats_collector = StatisticsCollector(self.config)
            self.monitor_logger.log_system(LogLevel.INFO, "统计收集器初始化完成")
        except Exception as e:
            self.logger.error(f"统计收集器初始化失败: {e}")
            self.stats_collector = None
    
    def _init_error_handler(self):
        """初始化错误处理器"""
        try:
            self.error_handler = ErrorHandler(self.config)
            self.monitor_logger.log_system(LogLevel.INFO, "错误处理器初始化完成")
        except Exception as e:
            self.logger.error(f"错误处理器初始化失败: {e}")
            self.error_handler = None
            
    def _init_database(self):
        """初始化数据库连接"""
        try:
            self.db_manager = DatabaseManager(self.config)
            if self.monitor_config.get('stock_list', {}).get('from_database', False):
                if not self.db_manager.connect():
                    raise ValueError("无法连接数据库，但配置要求从数据库获取股票列表")
            self.logger.info("数据库管理器初始化成功")
        except Exception as e:
            self.logger.error(f"数据库管理器初始化失败: {e}")
            raise
            
    def _init_downloader(self):
        """初始化下载器"""
        try:
            self.downloader = AsyncHKEXDownloader(self.config_manager)
            self.logger.info("异步下载器初始化成功")
        except Exception as e:
            self.logger.error(f"下载器初始化失败: {e}")
            raise
            
    def _init_tracker(self):
        """初始化状态跟踪器"""
        try:
            self.tracker = StockTracker(self.monitor_config)
            self.logger.info("股票状态跟踪器初始化成功")
        except Exception as e:
            self.logger.error(f"状态跟踪器初始化失败: {e}")
            raise
            
    def _init_detector(self):
        """初始化变化检测器"""
        try:
            self.detector = ChangeDetector(self.monitor_config)
            self.logger.info("变化检测器初始化成功")
        except Exception as e:
            self.logger.error(f"变化检测器初始化失败: {e}")
            raise
    
    async def get_stock_list(self) -> List[str]:
        """获取需要监听的股票列表"""
        stock_config = self.monitor_config.get('stock_list', {})
        
        # 静态股票列表
        static_stocks = stock_config.get('static_stocks', [])
        
        # 从数据库获取
        if stock_config.get('from_database', False):
            try:
                db_stocks = await self._get_stocks_from_database()
                static_stocks.extend(db_stocks)
                self.monitor_logger.log_system(
                    LogLevel.INFO, 
                    f"从数据库获取股票",
                    count=len(db_stocks)
                )
            except Exception as e:
                self.monitor_logger.log_error(
                    LogLevel.ERROR, None, 
                    f"从数据库获取股票列表失败: {e}"
                )
        
        # 去重并返回
        unique_stocks = list(set(static_stocks))
        self.monitor_logger.log_system(
            LogLevel.INFO, 
            f"监听股票列表初始化完成",
            total_count=len(unique_stocks)
        )
        return unique_stocks
    
    async def _get_stocks_from_database(self) -> List[str]:
        """从数据库获取股票列表"""
        if not self.db_manager.is_connected:
            await asyncio.get_event_loop().run_in_executor(
                None, self.db_manager.connect
            )
        
        stock_config = self.monitor_config.get('stock_list', {})
        batch_size = stock_config.get('batch_size', 50)
        
        # 使用自定义查询或默认查询
        custom_query = stock_config.get('database_query')
        if custom_query:
            query = custom_query
        else:
            # 默认查询：获取正常状态的股票
            query = f"""
                SELECT DISTINCT stockCode 
                FROM {self.config['database']['default_table']} 
                WHERE status IN ('normal') 
                LIMIT {batch_size}
            """
        
        def execute_query():
            cursor = self.db_manager.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return [row[0] for row in results]
        
        return await asyncio.get_event_loop().run_in_executor(None, execute_query)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def check_stock_announcements(self, stock_code: str) -> List[AnnouncementInfo]:
        """检查单个股票的公告"""
        start_time = time.time()
        try:
            self.monitor_logger.log_monitor(LogLevel.DEBUG, stock_code, "开始检查公告")
            
            # 使用现有下载器的API接口
            def get_announcements():
                return self.downloader.search_announcements(
                    stock_code=stock_code,
                    start_date="today",  # 只检查今天的公告
                    end_date="today"
                )
            
            # 在线程池中执行同步API调用
            announcements = await asyncio.get_event_loop().run_in_executor(
                None, get_announcements
            )
            
            # 转换为AnnouncementInfo对象
            announcement_infos = []
            for ann in announcements:
                info = AnnouncementInfo(
                    stock_code=stock_code,
                    title=ann.get('TITLE', ''),
                    date_time=ann.get('DATE_TIME', ''),
                    long_text=ann.get('LONG_TEXT', ''),
                    file_link=ann.get('FILE_LINK', ''),
                    hash_key=self.detector.generate_hash(ann)
                )
                announcement_infos.append(info)
            
            # 记录统计
            duration = time.time() - start_time
            self.stats['checked'] += 1
            
            if self.stats_collector:
                self.stats_collector.record_check(
                    stock_code, 
                    success=True, 
                    duration=duration,
                    announcement_count=len(announcement_infos)
                )
            
            self.monitor_logger.log_monitor(
                LogLevel.INFO, 
                stock_code, 
                f"检查完成，找到 {len(announcement_infos)} 条公告",
                duration=duration,
                count=len(announcement_infos)
            )
            
            return announcement_infos
            
        except Exception as e:
            duration = time.time() - start_time
            self.stats['errors'] += 1
            
            # 记录错误统计
            if self.stats_collector:
                self.stats_collector.record_check(stock_code, success=False, duration=duration)
            
            # 记录错误日志
            self.monitor_logger.log_error(LogLevel.ERROR, stock_code, f"检查公告失败: {e}", error_type=type(e).__name__)
            
            raise
    
    async def process_new_announcements(self, new_announcements: List[AnnouncementInfo]):
        """处理新发现的公告"""
        if not new_announcements:
            return
            
        self.monitor_logger.log_system(
            LogLevel.INFO, 
            f"发现新公告",
            count=len(new_announcements)
        )
        self.stats['found'] += len(new_announcements)
        
        # 自动下载
        if self.monitor_config.get('auto_download', {}).get('enabled', True):
            await self._auto_download(new_announcements)
            
        # 自动处理
        if self.monitor_config.get('auto_process', {}).get('enabled', True):
            await self._auto_process(new_announcements)
    
    async def _auto_download(self, announcements: List[AnnouncementInfo]):
        """自动下载新公告"""
        start_time = time.time()
        try:
            download_config = self.monitor_config.get('auto_download', {})
            save_path = download_config.get('save_path', './monitor_downloads')
            immediate = download_config.get('immediate', True)
            
            # 创建保存目录
            import os
            os.makedirs(save_path, exist_ok=True)
            
            self.monitor_logger.log_download(
                LogLevel.INFO, 
                "", 
                f"开始下载 {len(announcements)} 个文件",
                mode="immediate" if immediate else "queue"
            )
            
            if immediate:
                # 立即下载模式
                await self._download_immediately(announcements, save_path)
            else:
                # 队列下载模式
                await self._queue_download(announcements, save_path)
            
            # 记录性能数据
            duration = time.time() - start_time
            if self.stats_collector:
                self.stats_collector.record_performance("auto_download", duration)
                
        except Exception as e:
            duration = time.time() - start_time
            self.monitor_logger.log_error(LogLevel.ERROR, "", f"自动下载处理失败: {e}")
            self.stats['errors'] += 1
            
            if self.stats_collector:
                self.stats_collector.record_performance("auto_download_error", duration)
    
    async def _download_immediately(self, announcements: List[AnnouncementInfo], save_path: str):
        """立即下载模式"""
        performance_config = self.monitor_config.get('performance', {})
        max_concurrent = performance_config.get('max_concurrent_downloads', 3)
        
        # 使用信号量控制并发数
        download_semaphore = asyncio.Semaphore(max_concurrent)
        
        async def download_single(ann: AnnouncementInfo):
            download_start = time.time()
            async with download_semaphore:
                if not ann.file_link:
                    self.monitor_logger.log_download(LogLevel.WARNING, ann.stock_code, f"公告 {ann.title} 没有下载链接")
                    return
                
                try:
                    # 生成文件名
                    filename = self._generate_filename(ann)
                    file_path = os.path.join(save_path, filename)
                    
                    # 检查文件是否已存在
                    if os.path.exists(file_path):
                        self.monitor_logger.log_download(LogLevel.INFO, ann.stock_code, f"文件已存在，跳过下载: {filename}")
                        return
                    
                    # 使用现有异步下载器的方法
                    success = await self._download_file_with_retry(ann.file_link, file_path, ann.title)
                    
                    download_duration = time.time() - download_start
                    if success:
                        self.stats['downloaded'] += 1
                        if self.stats_collector:
                            self.stats_collector.record_download(ann.stock_code, True, 1, download_duration)
                        self.monitor_logger.log_download(LogLevel.INFO, ann.stock_code, f"成功下载: {filename}", duration=download_duration)
                    else:
                        self.stats['errors'] += 1
                        if self.stats_collector:
                            self.stats_collector.record_download(ann.stock_code, False, 0, download_duration)
                        self.monitor_logger.log_download(LogLevel.ERROR, ann.stock_code, f"下载失败: {filename}")
                        
                except Exception as e:
                    download_duration = time.time() - download_start
                    self.stats['errors'] += 1
                    if self.stats_collector:
                        self.stats_collector.record_download(ann.stock_code, False, 0, download_duration)
                    self.monitor_logger.log_error(LogLevel.ERROR, ann.stock_code, f"下载公告 {ann.title} 失败: {e}")
        
        # 并发下载所有公告
        tasks = [download_single(ann) for ann in announcements]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _download_file_with_retry(self, file_url: str, file_path: str, title: str) -> bool:
        """带重试的文件下载"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 确保下载器有会话
                if not hasattr(self.downloader, '_session') or not self.downloader._session:
                    await self.downloader._create_session()
                
                # 使用异步下载器的速率限制器
                await self.downloader.rate_limiter.acquire()
                
                async with self.downloader.semaphore:
                    # 执行下载
                    async with self.downloader._session.get(file_url) as response:
                        if response.status == 200:
                            # 创建目录
                            os.makedirs(os.path.dirname(file_path), exist_ok=True)
                            
                            # 写入文件
                            async with aiofiles.open(file_path, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    await f.write(chunk)
                            
                            return True
                        elif response.status == 429:
                            # 触发速率限制，等待更长时间
                            wait_time = self.downloader.backoff_on_429
                            self.monitor_logger.log_download(
                                LogLevel.WARNING, announcement.stock_code, 
                                f"触发速率限制，等待 {wait_time} 秒"
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            self.monitor_logger.log_download(
                                LogLevel.WARNING, announcement.stock_code,
                                f"HTTP错误 {response.status}: {file_url}"
                            )
                
            except Exception as e:
                self.monitor_logger.log_download(
                    LogLevel.WARNING, announcement.stock_code,
                    f"下载尝试 {attempt + 1} 失败: {e}"
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # 指数退避
                
        return False
    
    async def _queue_download(self, announcements: List[AnnouncementInfo], save_path: str):
        """队列下载模式（预留实现）"""
        # 添加到下载队列，由单独的工作线程处理
        self.monitor_logger.log_download(
            LogLevel.INFO, None,
            f"添加下载任务到队列",
            count=len(announcements)
        )
        # TODO: 实现下载队列机制
        pass
    
    def _generate_filename(self, ann: AnnouncementInfo) -> str:
        """生成下载文件名"""
        import re
        
        # 清理标题作为文件名，包括换行符和控制字符
        safe_title = re.sub(r'[<>:"/\\|?*\r\n\t\v\f]', '_', ann.title)
        safe_title = safe_title[:100]  # 限制长度
        
        # 添加股票代码和时间戳
        timestamp = ann.date_time.replace(':', '-').replace(' ', '_')
        filename = f"{ann.stock_code}_{timestamp}_{safe_title}.pdf"
        
        return filename
    
    async def _auto_process(self, announcements: List[AnnouncementInfo]):
        """自动处理新公告（PDF解析、向量化等）"""
        start_time = time.time()
        try:
            process_config = self.monitor_config.get('auto_process', {})
            pipeline_config = process_config.get('pipeline', {})
            
            if not any(pipeline_config.values()):
                self.monitor_logger.log_process(LogLevel.DEBUG, "", "处理管道已禁用")
                return
                
            # 获取队列配置
            queue_config = process_config.get('queue', {})
            batch_size = queue_config.get('batch_size', 10)
            
            self.monitor_logger.log_process(
                LogLevel.INFO, 
                "", 
                f"开始处理 {len(announcements)} 个公告",
                batch_size=batch_size
            )
            
            # 集成现有的processing pipeline
            await self._process_with_pipeline(announcements, pipeline_config, batch_size)
            
            # 记录性能数据
            duration = time.time() - start_time
            if self.stats_collector:
                self.stats_collector.record_performance("auto_process", duration)
                
        except Exception as e:
            duration = time.time() - start_time
            self.monitor_logger.log_error(LogLevel.ERROR, "", f"自动处理失败: {e}")
            self.stats['errors'] += 1
            
            if self.stats_collector:
                self.stats_collector.record_performance("auto_process_error", duration)
    
    async def _process_with_pipeline(self, announcements: List[AnnouncementInfo], 
                                   pipeline_config: Dict[str, Any], batch_size: int):
        """使用现有管道处理公告"""
        try:
            # 导入处理管道
            from services.document_processor.pipeline import DocumentProcessingPipeline
            
            # 获取下载路径，查找已下载的PDF文件
            download_config = self.monitor_config.get('auto_download', {})
            save_path = download_config.get('save_path', './monitor_downloads')
            
            # 获取性能配置
            performance_config = self.monitor_config.get('performance', {})
            max_concurrent = performance_config.get('max_concurrent_processes', 2)
            
            # 初始化处理管道
            pipeline = DocumentProcessingPipeline(
                pdf_directory=save_path,
                collection_name="monitor_announcements",  # 监听专用集合
                batch_size=batch_size,
                max_concurrent=max_concurrent
            )
            
            # 按批处理公告
            for i in range(0, len(announcements), batch_size):
                batch = announcements[i:i + batch_size]
                await self._process_announcement_batch(batch, pipeline, pipeline_config)
                
        except Exception as e:
            self.monitor_logger.log_error(LogLevel.ERROR, None, f"管道处理失败: {e}")
            raise
    
    async def _process_announcement_batch(self, batch: List[AnnouncementInfo], 
                                        pipeline, pipeline_config: Dict[str, Any]):
        """处理公告批次"""
        semaphore = asyncio.Semaphore(2)  # 限制并发数
        
        async def process_single_announcement(ann: AnnouncementInfo):
            process_start = time.time()
            async with semaphore:
                try:
                    # 查找对应的PDF文件
                    pdf_path = await self._find_downloaded_pdf(ann)
                    
                    if pdf_path and pdf_path.exists():
                        result = await self._process_pdf_file(pdf_path, pipeline, pipeline_config)
                        
                        process_duration = time.time() - process_start
                        if result and result.get('success'):
                            self.stats['processed'] += 1
                            if self.stats_collector:
                                self.stats_collector.record_process(ann.stock_code, True, 1, process_duration)
                            self.monitor_logger.log_process(LogLevel.INFO, ann.stock_code, f"处理完成: {ann.title}", duration=process_duration)
                        else:
                            self.stats['errors'] += 1
                            if self.stats_collector:
                                self.stats_collector.record_process(ann.stock_code, False, 0, process_duration)
                            self.monitor_logger.log_process(LogLevel.WARNING, ann.stock_code, f"处理失败: {ann.title}")
                    else:
                        self.monitor_logger.log_process(LogLevel.WARNING, ann.stock_code, f"未找到PDF文件: {ann.title}")
                        
                except Exception as e:
                    process_duration = time.time() - process_start
                    self.stats['errors'] += 1
                    if self.stats_collector:
                        self.stats_collector.record_process(ann.stock_code, False, 0, process_duration)
                    self.monitor_logger.log_error(LogLevel.ERROR, ann.stock_code, f"处理公告 {ann.title} 异常: {e}")
        
        # 并发处理批次中的所有公告
        tasks = [process_single_announcement(ann) for ann in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _find_downloaded_pdf(self, ann: AnnouncementInfo) -> Optional[Path]:
        """查找已下载的PDF文件"""
        from pathlib import Path
        
        download_config = self.monitor_config.get('auto_download', {})
        save_path = Path(download_config.get('save_path', './monitor_downloads'))
        
        # 生成期望的文件名
        filename = self._generate_filename(ann)
        pdf_path = save_path / filename
        
        if pdf_path.exists():
            return pdf_path
        
        # 如果精确文件名不存在，尝试模糊匹配
        pattern = f"{ann.stock_code}_*_{ann.title[:20]}*.pdf"
        matching_files = list(save_path.glob(pattern.replace(' ', '_')))
        
        if matching_files:
            return matching_files[0]  # 返回第一个匹配的文件
        
        return None
    
    async def _process_pdf_file(self, pdf_path: Path, pipeline, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """处理单个PDF文件"""
        try:
            # 根据配置决定处理步骤
            parsing_enabled = pipeline_config.get('parsing', True)
            vectorization_enabled = pipeline_config.get('vectorization', True)
            embedding_enabled = pipeline_config.get('embedding', True)
            
            result = {
                'success': True,
                'pdf_path': str(pdf_path),
                'parsing_result': None,
                'vectorization_result': None
            }
            
            # PDF解析
            if parsing_enabled:
                parsing_result = await pipeline.process_single_document(pdf_path)
                result['parsing_result'] = parsing_result
                
                if not parsing_result.get('success'):
                    self.monitor_logger.log_process(
                        LogLevel.WARNING, pdf_path.stem.split('_')[0] if '_' in pdf_path.stem else None,
                        f"PDF解析失败: {pdf_path.name}"
                    )
                    result['success'] = False
                    return result
            
            # 向量化和embedding（如果解析成功）
            if vectorization_enabled and embedding_enabled and result['parsing_result']:
                # 向量化通常包含在pipeline中
                self.monitor_logger.log_process(
                    LogLevel.DEBUG, pdf_path.stem.split('_')[0] if '_' in pdf_path.stem else None,
                    f"向量化处理: {pdf_path.name}"
                )
            
            return result
            
        except Exception as e:
            self.monitor_logger.log_error(
                LogLevel.ERROR, pdf_path.stem.split('_')[0] if '_' in pdf_path.stem else None,
                f"PDF处理异常 {pdf_path.name}: {e}"
            )
            return {
                'success': False,
                'error': str(e),
                'pdf_path': str(pdf_path)
            }
    
    async def run_check_cycle(self):
        """运行一次检查周期"""
        try:
            self.monitor_logger.log_system(LogLevel.INFO, "开始监听周期检查")
            start_time = time.time()
            
            # 获取股票列表
            stock_list = await self.get_stock_list()
            if not stock_list:
                self.monitor_logger.log_system(LogLevel.WARNING, "没有找到需要监听的股票")
                return
            
            # 并发检查所有股票
            performance_config = self.monitor_config.get('performance', {})
            max_concurrent = performance_config.get('max_concurrent_checks', 5)
            
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def check_single_stock(stock_code):
                async with semaphore:
                    return await self.check_stock_announcements(stock_code)
            
            # 执行并发检查
            tasks = [check_single_stock(stock) for stock in stock_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理结果
            all_new_announcements = []
            successful_checks = 0
            failed_checks = 0
            
            for i, result in enumerate(results):
                stock_code = stock_list[i]
                
                if isinstance(result, Exception):
                    self.monitor_logger.log_error(
                        LogLevel.ERROR, stock_code, f"股票检查异常: {result}"
                    )
                    # 记录检查失败
                    self.stats_collector.record_check(stock_code, success=False)
                    failed_checks += 1
                    # 增加错误计数
                    await self.tracker.increment_error_count(stock_code)
                    continue
                
                try:
                    # 获取股票状态用于检测变化
                    stock_state = await self.tracker.get_stock_state(stock_code)
                    previous_state = stock_state.to_dict() if stock_state else None
                    
                    # 检测新公告（使用完整版本）
                    detection_result = await self.detector.detect_changes_with_state(
                        stock_code, result, previous_state
                    )
                    
                    # 记录检查成功
                    check_duration = time.time() - start_time
                    self.stats_collector.record_check(
                        stock_code, 
                        success=True, 
                        duration=check_duration,
                        announcement_count=detection_result.total_announcements
                    )
                    successful_checks += 1
                    
                    # 更新股票状态
                    await self.tracker.update_check_time(
                        stock_code,
                        announcement_count=detection_result.total_announcements,
                        announcement_hash=self.detector.strategy.generate_signature(result)
                    )
                    
                    # 重置错误计数（检查成功）
                    if stock_state and stock_state.error_count > 0:
                        await self.tracker.reset_error_count(stock_code)
                    
                    # 记录新公告发现
                    if detection_result.new_announcements:
                        self.stats_collector.record_discovery(
                            stock_code, len(detection_result.new_announcements)
                        )
                    
                    # 添加新公告
                    all_new_announcements.extend(detection_result.new_announcements)
                    
                except Exception as e:
                    self.monitor_logger.log_error(
                        LogLevel.ERROR, stock_code, f"股票状态处理异常: {e}"
                    )
                    self.stats_collector.record_check(stock_code, success=False)
                    failed_checks += 1
                    await self.tracker.increment_error_count(stock_code)
                    continue
            
            # 处理所有新公告
            if all_new_announcements:
                await self.process_new_announcements(all_new_announcements)
            
            # 更新状态
            self.last_check_time = datetime.now()
            elapsed = time.time() - start_time
            
            # 记录周期性能
            self.monitor_logger.log_performance("check_cycle", elapsed)
            
            self.monitor_logger.log_system(
                LogLevel.INFO, 
                f"监听周期完成",
                elapsed=f"{elapsed:.2f}s",
                total_stocks=len(stock_list),
                successful_checks=successful_checks,
                failed_checks=failed_checks,
                new_announcements=len(all_new_announcements)
            )
            
        except Exception as e:
            self.monitor_logger.log_error(LogLevel.ERROR, None, f"监听周期执行失败: {e}")
            self.error_count += 1
            self.stats['errors'] += 1
            
            # 检查错误阈值
            if self.error_count >= self.error_threshold:
                self.monitor_logger.log_error(
                    LogLevel.CRITICAL, None, 
                    f"错误次数超过阈值({self.error_threshold})，暂停监听服务"
                )
                self.is_running = False
                raise
    
    async def start(self):
        """启动监听服务"""
        self.monitor_logger.log_system(LogLevel.INFO, "启动HKEX公告实时监听服务")
        self.is_running = True
        self.error_count = 0
        
        # 记录服务启动
        self.stats_collector.record_service_start()
        
        try:
            while self.is_running:
                try:
                    await self.run_check_cycle()
                    
                    # 重置错误计数（成功后）
                    if self.error_count > 0:
                        self.error_count = 0
                    
                    # 输出统计信息
                    await self._log_stats()
                    
                    # 等待下次检查
                    if self.is_running:
                        self.monitor_logger.log_system(
                            LogLevel.DEBUG, 
                            f"等待下次检查",
                            interval=self.check_interval
                        )
                        await asyncio.sleep(self.check_interval)
                        
                except Exception as e:
                    self.monitor_logger.log_error(LogLevel.ERROR, None, f"监听周期异常: {e}")
                    self.error_count += 1
                    
                    # 等待重试间隔
                    await asyncio.sleep(self.retry_interval)
                    
        except KeyboardInterrupt:
            self.monitor_logger.log_system(LogLevel.INFO, "接收到停止信号")
        except Exception as e:
            self.monitor_logger.log_error(LogLevel.CRITICAL, None, f"监听服务严重错误: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """停止监听服务"""
        self.monitor_logger.log_system(LogLevel.INFO, "正在停止监听服务...")
        self.is_running = False
        
        # 记录服务停止
        self.stats_collector.record_service_stop()
        
        # 清理资源
        try:
            if hasattr(self.downloader, '_session') and self.downloader._session:
                await self.downloader._session.close()
            
            if self.tracker:
                await self.tracker.cleanup()
                
            if self.detector:
                await self.detector.cleanup()
            
            # 输出最终统计报告
            final_report = self.stats_collector.generate_report()
            self.monitor_logger.log_system(
                LogLevel.INFO, 
                "服务停止统计报告", 
                report=final_report
            )
            
            # 清理监控组件
            if hasattr(self, 'monitor_logger') and self.monitor_logger:
                self.monitor_logger.cleanup()
                
            if hasattr(self, 'stats_collector') and self.stats_collector:
                self.stats_collector.cleanup()
                
        except Exception as e:
            self.monitor_logger.log_error(LogLevel.ERROR, None, f"资源清理失败: {e}")
        
        self.monitor_logger.log_system(LogLevel.INFO, "监听服务已停止")
    
    async def _log_stats(self):
        """输出统计信息"""
        stats_config = self.monitor_config.get('monitoring', {}).get('stats', {})
        if not stats_config.get('enabled', True):
            return
            
        interval = stats_config.get('interval', 300)
        current_time = time.time()
        
        if not hasattr(self, '_last_stats_time'):
            self._last_stats_time = current_time
            
        if current_time - self._last_stats_time >= interval:
            # 生成统计报告
            stats_report = self.stats_collector.generate_report()
            
            self.monitor_logger.log_system(
                LogLevel.INFO, 
                "定期监听统计报告",
                **stats_report.get('summary', {})
            )
            
            # 记录性能统计
            performance_stats = self.monitor_logger.get_performance_stats()
            if performance_stats:
                self.monitor_logger.log_system(
                    LogLevel.INFO,
                    "性能统计",
                    performance=performance_stats
                )
            
            # 获取状态跟踪统计
            if self.tracker:
                tracker_stats = await self.tracker.get_stats()
                self.monitor_logger.log_system(
                    LogLevel.INFO,
                    "状态跟踪统计",
                    tracker=tracker_stats
                )
            
            self._last_stats_time = current_time


# 异步主函数
async def main():
    """主函数"""
    monitor = AnnouncementMonitor()
    try:
        await monitor.start()
    except KeyboardInterrupt:
        print("\n监听服务被用户中断")
    except Exception as e:
        print(f"监听服务异常退出: {e}")


if __name__ == "__main__":
    asyncio.run(main())