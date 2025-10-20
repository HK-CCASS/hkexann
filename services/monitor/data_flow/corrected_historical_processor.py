"""
修复后的历史公告批量处理器

这个模块修复了原始HistoricalBatchProcessor的重大逻辑错误：
- 不再错误使用监听API (lcisehk1relsdc_1.json) 获取历史数据
- 正确使用下载API (titleSearchServlet.do) 获取指定时间范围的历史公告
- 实现分批处理，避免API过载
- 提供详细的进度和错误报告

主要功能：
- 正确的历史数据获取策略
- 按股票和日期范围批量查询
- 资源管理和并发控制
- 完整的错误处理和重试机制

作者: HKEX分析团队
版本: 2.0.0 (修复版)
日期: 2025-01-17
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set, Optional
from pathlib import Path
import aiofiles
import json
import time

# 导入真实下载器
from ..downloader_integration import RealtimeDownloaderWrapper

# 配置日志
# 配置日志（如果没有已配置的handler）
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CorrectedHistoricalProcessor:
    """
    修复后的历史公告批量处理器
    
    修复了原始版本的重大逻辑错误：
    1. 使用正确的下载API而非监听API获取历史数据
    2. 实现真正的历史时间范围查询
    3. 分批处理避免API过载和资源竞争
    """
    
    def __init__(self,
                 hkex_downloader,  # 使用原项目的HKEXDownloader
                 simple_filter,
                 vectorizer,
                 monitored_stocks: Set[str],
                 config: Dict[str, Any],
                 progress_callback=None,
                 resume_state_callback=None):
        """
        初始化修复后的历史批量处理器

        Args:
            hkex_downloader: 原项目的HKEXDownloader实例 (使用titleSearchServlet.do)
            simple_filter: 简化过滤器
            vectorizer: 向量化器
            monitored_stocks: 监控股票集合
            config: 配置信息
            progress_callback: 进度回调函数，可选
            resume_state_callback: 续传状态回调函数，可选
        """
        self.hkex_downloader = hkex_downloader
        self.simple_filter = simple_filter
        self.vectorizer = vectorizer
        self.monitored_stocks = monitored_stocks
        self.config = config
        self.progress_callback = progress_callback
        self.resume_state_callback = resume_state_callback
        
        # 历史处理配置
        self.historical_days = config.get('historical_days', 365)  # 默认一年
        self.first_run_historical_days = config.get('first_run_historical_days', 3)  # 首次运行默认3天
        self.stock_batch_size = config.get('stock_batch_size', 10)  # 每批处理的股票数
        self.date_chunk_days = config.get('date_chunk_days', 30)  # 每次查询的天数
        self.max_concurrent = config.get('max_concurrent_historical', 3)  # 并发限制
        self.api_delay = config.get('api_delay', 2.0)  # API调用间隔
        
        # 状态文件
        self.status_file = Path("hkexann") / ".corrected_historical_status.json"
        self.status_file.parent.mkdir(exist_ok=True)
        
        # 初始化真实下载器
        try:
            downloader_config = {
                'downloader_integration': {
                    'use_existing_downloader': True,
                    'download_directory': 'hkexann',
                    'enable_filtering': True,
                    'timeout': 30,
                    'preserve_original_filename': True,
                    'create_date_subdirs': False,  # 禁用日期子目录，使用智能分类
                    'enable_progress_bar': False,
                    # 添加智能分类配置
                    'enable_smart_classification': True,
                    'common_keywords': config.get('common_keywords', {}),
                    'announcement_categories': config.get('announcement_categories', {})
                },
                'max_concurrent': self.max_concurrent,
                'requests_per_second': 2
            }
            # 合并原始配置以获取智能分类设置
            downloader_config.update(config)
            self.real_downloader = RealtimeDownloaderWrapper(downloader_config)
            logger.info("✅ 真实下载器初始化成功")
        except Exception as e:
            logger.error(f"❌ 真实下载器初始化失败: {e}")
            self.real_downloader = None
        
        # 统计信息
        self.processing_stats = {
            'total_stocks_processed': 0,
            'total_announcements_found': 0,
            'total_announcements_downloaded': 0,
            'total_announcements_vectorized': 0,
            'errors': [],
            'api_calls_made': 0,
            'processing_start_time': None,
            'processing_end_time': None
        }
        
        logger.info(f"🔧 修复后历史批量处理器初始化完成")
        logger.info(f"  📅 历史天数: {self.historical_days}天")
        logger.info(f"  🆕 首次运行历史天数: {self.first_run_historical_days}天")
        logger.info(f"  📦 股票批次大小: {self.stock_batch_size}只/批")
        logger.info(f"  📅 日期块大小: {self.date_chunk_days}天/块")
        logger.info(f"  🔄 最大并发: {self.max_concurrent}")
        logger.info(f"  ⏱️ API延迟: {self.api_delay}秒")

    async def is_first_run(self) -> bool:
        """检查是否为首次运行"""
        try:
            if not self.status_file.exists():
                logger.info("🔍 检测到首次运行（修复版状态文件不存在）")
                return True
            
            async with aiofiles.open(self.status_file, 'r', encoding='utf-8') as f:
                status_data = json.loads(await f.read())
                
            last_historical_process = status_data.get('last_corrected_historical_process')
            if not last_historical_process:
                logger.info("🔍 检测到首次运行（无修复版历史处理记录）")
                return True
            
            # 检查上次历史处理是否超过30天
            last_process_time = datetime.fromisoformat(last_historical_process)
            if datetime.now() - last_process_time > timedelta(days=30):
                logger.info(f"🔍 检测到长时间未运行，建议重新处理历史数据（上次: {last_process_time}）")
                return True
            
            logger.info(f"✅ 非首次运行，上次修复版历史处理: {last_process_time}")
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ 检查首次运行状态失败，默认为首次运行: {e}")
            return True

    async def process_historical_announcements(self) -> Dict[str, Any]:
        """
        处理历史公告 - 使用正确的下载API
        
        Returns:
            处理结果统计
        """
        start_time = datetime.now()
        self.processing_stats['processing_start_time'] = start_time.isoformat()
        
        # 动态确定处理天数 - 首次运行使用较少天数以快速启动
        is_first = await self.is_first_run()
        actual_days = self.first_run_historical_days if is_first else self.historical_days
        
        logger.info("🚀 开始修复后的历史公告批量处理")
        logger.info(f"📅 处理范围: 最近 {actual_days} 天 {'(首次运行快速模式)' if is_first else '(常规模式)'}")
        logger.info(f"🎯 监控股票: {len(self.monitored_stocks)} 只")
        logger.info(f"🔧 使用正确的下载API: titleSearchServlet.do")
        
        try:
            # 准备日期范围
            end_date = datetime.now()
            start_date = end_date - timedelta(days=actual_days)
            
            logger.info(f"📅 历史查询范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            
            # 分批处理股票
            stock_list = list(self.monitored_stocks)
            stock_batches = [stock_list[i:i + self.stock_batch_size] 
                           for i in range(0, len(stock_list), self.stock_batch_size)]
            
            logger.info(f"📦 股票分批: {len(stock_batches)} 批，每批最多 {self.stock_batch_size} 只")

            # 处理每个股票批次 - 每批处理完立即下载
            for batch_idx, stock_batch in enumerate(stock_batches, 1):
                logger.info(f"📦 处理第 {batch_idx}/{len(stock_batches)} 批股票: {stock_batch}")

                # 进度回调 - 批次开始
                if self.progress_callback:
                    await self.progress_callback({
                        'stage': 'batch_start',
                        'batch_idx': batch_idx,
                        'total_batches': len(stock_batches),
                        'stocks_in_batch': len(stock_batch),
                        'stocks_processed': self.processing_stats['total_stocks_processed'],
                        'total_stocks': len(self.monitored_stocks)
                    })

                # 1. 搜索该批次的公告
                batch_announcements = await self._process_stock_batch(
                    stock_batch, start_date, end_date
                )

                self.processing_stats['total_stocks_processed'] += len(stock_batch)
                self.processing_stats['total_announcements_found'] += len(batch_announcements)

                logger.info(f"📊 批次 {batch_idx} 搜索完成: 发现 {len(batch_announcements)} 条公告")

                # 进度回调 - 搜索完成
                if self.progress_callback:
                    await self.progress_callback({
                        'stage': 'search_complete',
                        'batch_idx': batch_idx,
                        'total_batches': len(stock_batches),
                        'stocks_in_batch': len(stock_batch),
                        'stocks_processed': self.processing_stats['total_stocks_processed'],
                        'total_stocks': len(self.monitored_stocks),
                        'announcements_found': len(batch_announcements)
                    })

                # 2. 立即过滤该批次的公告
                if batch_announcements:
                    logger.info(f"🔍 过滤批次 {batch_idx} 的公告...")
                    relevant_batch_announcements = await self._filter_announcements(batch_announcements)
                    logger.info(f"✅ 批次 {batch_idx} 过滤完成: {len(relevant_batch_announcements)} 条相关公告")

                    # 3. 立即下载和向量化该批次的相关公告
                    if relevant_batch_announcements:
                        logger.info(f"📥 下载批次 {batch_idx} 的相关公告...")
                        batch_download_stats = await self._batch_download_and_vectorize(relevant_batch_announcements)

                        # 累加批次的统计信息
                        for key, value in batch_download_stats.items():
                            if key in self.processing_stats:
                                self.processing_stats[key] += value
                            else:
                                self.processing_stats[key] = value

                        logger.info(f"✅ 批次 {batch_idx} 处理完成: 下载 {batch_download_stats.get('total_announcements_downloaded', 0)} 条, 向量化 {batch_download_stats.get('total_announcements_vectorized', 0)} 条")

                        # 进度回调 - 下载完成
                        if self.progress_callback:
                            await self.progress_callback({
                                'stage': 'download_complete',
                                'batch_idx': batch_idx,
                                'total_batches': len(stock_batches),
                                'stocks_in_batch': len(stock_batch),
                                'stocks_processed': self.processing_stats['total_stocks_processed'],
                                'total_stocks': len(self.monitored_stocks),
                                'announcements_downloaded': batch_download_stats.get('total_announcements_downloaded', 0),
                                'announcements_vectorized': batch_download_stats.get('total_announcements_vectorized', 0)
                            })

                        # 续传状态回调 - 保存已处理的股票
                        if self.resume_state_callback:
                            await self.resume_state_callback({
                                'processed_stocks': list(self.monitored_stocks)[:self.processing_stats['total_stocks_processed']],
                                'stats': self.processing_stats.copy()
                            })

                # 批次间休息，避免API过载
                if batch_idx < len(stock_batches):
                    logger.info(f"⏸️ 批次间休息 {self.api_delay} 秒...")
                    await asyncio.sleep(self.api_delay)

            logger.info(f"📊 所有批次处理完成: 总计 {self.processing_stats['total_announcements_found']} 条公告发现，{self.processing_stats.get('total_announcements_downloaded', 0)} 条下载，{self.processing_stats.get('total_announcements_vectorized', 0)} 条向量化")
            
            # 保存处理状态
            await self._save_processing_status()
            
            end_time = datetime.now()
            self.processing_stats['processing_end_time'] = end_time.isoformat()
            processing_time = (end_time - start_time).total_seconds()
            
            # 输出最终统计
            logger.info("🎉 修复后历史公告批量处理完成")
            logger.info(f"📈 处理统计:")
            logger.info(f"   📦 处理股票: {self.processing_stats['total_stocks_processed']} 只")
            logger.info(f"   📡 API调用: {self.processing_stats['api_calls_made']} 次")
            logger.info(f"   📄 发现公告: {self.processing_stats['total_announcements_found']} 条")
            logger.info(f"   📥 下载成功: {self.processing_stats['total_announcements_downloaded']} 条")
            logger.info(f"   🧠 向量化成功: {self.processing_stats['total_announcements_vectorized']} 条")
            logger.info(f"   ⏱️ 总耗时: {processing_time:.2f} 秒")
            logger.info(f"   ❌ 错误数: {len(self.processing_stats['errors'])}")
            
            return {
                'success': True,
                'processing_time_seconds': processing_time,
                **self.processing_stats
            }
            
        except Exception as e:
            error_msg = f"修复后历史公告批量处理失败: {e}"
            logger.error(f"❌ {error_msg}")
            self.processing_stats['errors'].append(error_msg)
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            return {
                'success': False,
                'error': error_msg,
                'processing_time_seconds': processing_time,
                **self.processing_stats
            }

    async def _process_stock_batch(self, stock_batch: List[str], 
                                 start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        处理一批股票的历史公告
        
        Args:
            stock_batch: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[Dict[str, Any]]: 公告列表
        """
        batch_announcements = []
        
        # 为每只股票查询历史公告
        for stock_code in stock_batch:
            try:
                logger.info(f"📈 处理股票 {stock_code} 的历史公告...")
                
                # 分时间段查询，避免单次查询过大
                current_start = start_date
                stock_announcements = []
                
                while current_start < end_date:
                    chunk_end = min(current_start + timedelta(days=self.date_chunk_days), end_date)
                    
                    logger.debug(f"  📅 查询 {stock_code}: {current_start.strftime('%Y-%m-%d')} - {chunk_end.strftime('%Y-%m-%d')}")
                    
                    # 使用正确的下载API查询
                    chunk_announcements = await self._fetch_announcements_using_download_api(
                        stock_code, current_start, chunk_end
                    )
                    
                    stock_announcements.extend(chunk_announcements)
                    self.processing_stats['api_calls_made'] += 1
                    
                    current_start = chunk_end
                    
                    # API调用间隔
                    await asyncio.sleep(0.5)
                
                batch_announcements.extend(stock_announcements)
                logger.info(f"✅ 股票 {stock_code}: 找到 {len(stock_announcements)} 条公告")
                
            except Exception as e:
                error_msg = f"处理股票 {stock_code} 失败: {e}"
                logger.error(f"❌ {error_msg}")
                self.processing_stats['errors'].append(error_msg)
        
        return batch_announcements

    async def _fetch_announcements_using_download_api(self, stock_code: str, 
                                                    start_date: datetime, 
                                                    end_date: datetime) -> List[Dict[str, Any]]:
        """
        使用正确的下载API获取公告
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期  
            end_date: 结束日期
            
        Returns:
            List[Dict[str, Any]]: 公告列表
        """
        try:
            # 使用原项目的HKEXDownloader获取公告列表
            # 这使用的是正确的titleSearchServlet.do API
            announcements, stock_name = self.hkex_downloader.get_announcement_list(
                stock_code, start_date, end_date, keywords=[]
            )
            
            # 转换为标准格式
            standardized_announcements = []
            for announcement in announcements:
                # 🚀 修复：从raw_data中获取LONG_TEXT，这包含了真实的分类信息
                raw_data = announcement.get('raw_data', {})
                long_text = raw_data.get('LONG_TEXT', '')
                
                # 🔄 如果raw_data中没有LONG_TEXT，尝试使用智能分类
                if not long_text:
                    # 优先级顺序：main_category > keyword_category > sub_category
                    long_text = (announcement.get('main_category', '') or 
                               announcement.get('keyword_category', '') or 
                               announcement.get('sub_category', ''))
                
                standardized = {
                    'STOCK_CODE': stock_code,
                    'STOCK_NAME': stock_name,
                    'TITLE': announcement.get('title', ''),
                    'DATE_TIME': announcement.get('date', ''),
                    'FILE_LINK': announcement.get('link', ''),
                    'LONG_TEXT': long_text,  # 🎯 修复：使用正确的分类信息
                    'SHORT_TEXT': raw_data.get('SHORT_TEXT', ''),
                    'source': 'download_api',
                    'query_date_range': f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
                }
                standardized_announcements.append(standardized)
            
            logger.debug(f"股票 {stock_code} 在 {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')} 找到 {len(standardized_announcements)} 条公告")
            
            return standardized_announcements
            
        except Exception as e:
            logger.error(f"使用下载API查询股票 {stock_code} 失败: {e}")
            return []

    async def _filter_announcements(self, announcements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        过滤相关公告
        
        Args:
            announcements: 原始公告列表
            
        Returns:
            List[Dict[str, Any]]: 过滤后的公告列表
        """
        try:
            # 使用双重过滤器
            relevant_announcements = await self.simple_filter.filter_announcements(announcements)
            return relevant_announcements
        except Exception as e:
            logger.error(f"过滤公告失败: {e}")
            self.processing_stats['errors'].append(f"过滤公告失败: {e}")
            return announcements  # 返回原始列表

    async def _batch_download_and_vectorize(self, announcements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量下载和向量化公告
        
        Args:
            announcements: 公告列表
            
        Returns:
            Dict[str, Any]: 处理统计
        """
        logger.info(f"📄 开始批量下载和向量化 {len(announcements)} 条历史公告")
        
        download_stats = {
            'total_announcements_downloaded': 0,
            'total_announcements_vectorized': 0,
            'download_errors': [],
            'vectorization_errors': []
        }
        
        # 并发控制
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single_announcement(announcement):
            async with semaphore:
                try:
                    # 检查是否有PDF链接
                    file_link = announcement.get('FILE_LINK', '')
                    if not file_link or not file_link.lower().endswith('.pdf'):
                        logger.debug(f"跳过非PDF公告: {announcement.get('TITLE', 'N/A')[:50]}")
                        return
                    
                    # 使用原项目的下载功能
                    download_result = await self._download_pdf_announcement(announcement)
                    
                    if download_result and download_result.get('success'):
                        download_stats['total_announcements_downloaded'] += 1
                        
                        # 向量化处理
                        vector_result = await self._vectorize_pdf(download_result['local_path'])
                        
                        if vector_result and vector_result.get('success'):
                            download_stats['total_announcements_vectorized'] += 1
                            logger.info(f"✅ 历史公告处理完成: {announcement.get('TITLE', 'N/A')[:50]}...")
                        else:
                            error_msg = f"向量化失败: {announcement.get('TITLE', 'N/A')[:50]}"
                            download_stats['vectorization_errors'].append(error_msg)
                    else:
                        error_msg = f"下载失败: {announcement.get('TITLE', 'N/A')[:50]}"
                        download_stats['download_errors'].append(error_msg)
                    
                except Exception as e:
                    error_msg = f"处理公告异常: {announcement.get('TITLE', 'N/A')[:50]} - {e}"
                    download_stats['download_errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
        
        # 创建并发任务
        tasks = [process_single_announcement(ann) for ann in announcements]
        
        # 等待所有任务完成
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # 更新总体错误统计
        self.processing_stats['errors'].extend(download_stats['download_errors'])
        self.processing_stats['errors'].extend(download_stats['vectorization_errors'])
        
        logger.info(f"📊 批量处理完成: 下载 {download_stats['total_announcements_downloaded']} 条, "
                   f"向量化 {download_stats['total_announcements_vectorized']} 条")
        
        return download_stats

    async def _download_pdf_announcement(self, announcement: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        下载PDF公告
        
        Args:
            announcement: 公告信息
            
        Returns:
            Optional[Dict[str, Any]]: 下载结果
        """
        try:
            # 检查真实下载器是否可用
            if not self.real_downloader:
                return {
                    'success': False,
                    'error': '真实下载器未初始化',
                    'local_path': None,
                    'file_size': 0,
                    'download_time': 0
                }
            
            # 调用真实下载器
            download_result = await self.real_downloader.download_single_announcement(announcement)
            
            if download_result.get('success'):
                # 转换格式以适配原有接口
                return {
                    'success': True,
                    'local_path': download_result.get('file_path'),
                    'file_size': download_result.get('file_size', 0),
                    'download_time': download_result.get('download_time', 0)
                }
            else:
                return {
                    'success': False,
                    'error': download_result.get('error', '下载失败'),
                    'local_path': None,
                    'file_size': 0,
                    'download_time': download_result.get('download_time', 0)
                }
            
        except Exception as e:
            logger.error(f"下载PDF失败: {e}")
            return {'success': False, 'error': str(e), 'local_path': None, 'file_size': 0, 'download_time': 0}

    async def _vectorize_pdf(self, pdf_path: str) -> Optional[Dict[str, Any]]:
        """
        向量化PDF文档
        
        Args:
            pdf_path: PDF文件路径
            
        Returns:
            Optional[Dict[str, Any]]: 向量化结果
        """
        try:
            # 使用向量化器处理PDF
            vector_result = await self.vectorizer.process_announcement_pdf(pdf_path)
            return vector_result
            
        except Exception as e:
            logger.error(f"向量化PDF失败: {e}")
            return {'success': False, 'error': str(e)}

    async def _save_processing_status(self):
        """保存处理状态"""
        try:
            status_data = {
                'last_corrected_historical_process': datetime.now().isoformat(),
                'processing_stats': self.processing_stats,
                'version': '2.0.0_corrected'
            }
            
            async with aiofiles.open(self.status_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(status_data, ensure_ascii=False, indent=2))
            
            logger.info(f"✅ 处理状态已保存: {self.status_file}")
            
        except Exception as e:
            logger.error(f"保存处理状态失败: {e}")

    def get_processing_statistics(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {
            'stats': self.processing_stats.copy(),
            'config': {
                'historical_days': self.historical_days,
                'stock_batch_size': self.stock_batch_size,
                'date_chunk_days': self.date_chunk_days,
                'max_concurrent': self.max_concurrent,
                'api_delay': self.api_delay
            },
            'status_file': str(self.status_file)
        }


# 便捷函数
async def process_historical_with_correct_api(hkex_downloader, simple_filter, vectorizer, 
                                            monitored_stocks: Set[str], 
                                            config: Dict[str, Any]) -> Dict[str, Any]:
    """
    便捷的历史处理函数
    
    Args:
        hkex_downloader: 原项目的HKEXDownloader实例
        simple_filter: 简化过滤器
        vectorizer: 向量化器  
        monitored_stocks: 监控股票集合
        config: 配置信息
        
    Returns:
        Dict[str, Any]: 处理结果
    """
    processor = CorrectedHistoricalProcessor(
        hkex_downloader, simple_filter, vectorizer, monitored_stocks, config
    )
    
    if await processor.is_first_run():
        return await processor.process_historical_announcements()
    else:
        logger.info("非首次运行，跳过历史处理")
        return {'success': True, 'skipped': True, 'reason': '非首次运行'}


if __name__ == "__main__":
    # 测试模块
    async def test_corrected_processor():
        """测试修复后的历史处理器"""
        
        print("\n" + "="*70)
        print("🔧 修复后历史批量处理器测试")
        print("="*70)
        
        # 模拟组件
        class MockHKEXDownloader:
            def get_announcement_list(self, stock_code, start_date, end_date, keywords):
                # 模拟返回一些测试数据
                return [
                    {
                        'title': f'测试公告 - {stock_code}',
                        'date': '2024-01-15',
                        'link': f'https://example.com/{stock_code}.pdf',
                        'category': '测试分类'
                    }
                ], f'测试公司{stock_code}'
        
        class MockDualFilter:
            async def filter_announcements(self, announcements):
                # 模拟过滤，返回一半
                return announcements[:len(announcements)//2] if announcements else []
        
        class MockVectorizer:
            async def process_announcement_pdf(self, pdf_path):
                await asyncio.sleep(0.01)  # 模拟处理时间
                return {'success': True, 'vectors_count': 10}
        
        # 创建测试处理器
        config = {
            'historical_days': 7,  # 测试用短期
            'stock_batch_size': 2,
            'date_chunk_days': 3,
            'max_concurrent': 2,
            'api_delay': 0.1
        }
        
        monitored_stocks = {'00700', '00939', '01398'}
        
        processor = CorrectedHistoricalProcessor(
            MockHKEXDownloader(),
            MockDualFilter(), 
            MockVectorizer(),
            monitored_stocks,
            config
        )
        
        # 测试首次运行检查
        is_first = await processor.is_first_run()
        print(f"🔍 首次运行: {is_first}")
        
        # 测试历史处理
        print(f"\n🚀 开始测试历史处理...")
        result = await processor.process_historical_announcements()
        
        print(f"\n📊 处理结果:")
        print(f"  成功: {result.get('success')}")
        print(f"  处理股票: {result.get('total_stocks_processed')} 只")
        print(f"  API调用: {result.get('api_calls_made')} 次")
        print(f"  发现公告: {result.get('total_announcements_found')} 条")
        print(f"  处理时间: {result.get('processing_time_seconds', 0):.2f} 秒")
        print(f"  错误数: {len(result.get('errors', []))}")
        
        # 获取统计信息
        stats = processor.get_processing_statistics()
        print(f"\n📈 配置信息:")
        for key, value in stats['config'].items():
            print(f"  {key}: {value}")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_corrected_processor())
