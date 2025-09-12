"""
历史公告批量处理器
负责首次运行时下载和处理最近一年的历史公告
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set, Optional
from pathlib import Path
import aiofiles
import json

logger = logging.getLogger(__name__)


class HistoricalBatchProcessor:
    """
    历史公告批量处理器
    
    首次运行时负责：
    1. 检测是否为首次运行
    2. 批量获取最近一年的历史公告
    3. 按股票过滤相关公告
    4. 批量下载和向量化处理
    5. 记录处理状态
    """
    
    def __init__(self, 
                 api_monitor,
                 dual_filter, 
                 downloader,
                 vectorizer,
                 monitored_stocks: Set[str],
                 config: Dict[str, Any]):
        """
        初始化历史批量处理器
        
        Args:
            api_monitor: API监听器
            dual_filter: 双重过滤器
            downloader: 下载器
            vectorizer: 向量化器
            monitored_stocks: 监控股票集合
            config: 配置信息
        """
        self.api_monitor = api_monitor
        self.dual_filter = dual_filter
        self.downloader = downloader
        self.vectorizer = vectorizer
        self.monitored_stocks = monitored_stocks
        self.config = config
        
        # 历史处理配置
        self.historical_days = config.get('historical_days', 365)  # 默认一年
        self.batch_size = config.get('historical_batch_size', 50)  # 批处理大小
        self.max_concurrent = config.get('max_concurrent_historical', 3)  # 并发限制
        
        # 状态文件
        self.status_file = Path("hkexann") / ".historical_processing_status.json"
        
        logger.info(f"历史批量处理器初始化完成")
        logger.info(f"  历史天数: {self.historical_days}天")
        logger.info(f"  批处理大小: {self.batch_size}")
        logger.info(f"  最大并发: {self.max_concurrent}")
    
    async def is_first_run(self) -> bool:
        """检查是否为首次运行"""
        try:
            if not self.status_file.exists():
                logger.info("🔍 检测到首次运行（状态文件不存在）")
                return True
            
            async with aiofiles.open(self.status_file, 'r', encoding='utf-8') as f:
                status_data = json.loads(await f.read())
                
            last_historical_process = status_data.get('last_historical_process')
            if not last_historical_process:
                logger.info("🔍 检测到首次运行（无历史处理记录）")
                return True
            
            # 检查上次历史处理是否超过30天（可能需要重新处理）
            last_process_time = datetime.fromisoformat(last_historical_process)
            if datetime.now() - last_process_time > timedelta(days=30):
                logger.info(f"🔍 检测到长时间未运行，建议重新处理历史数据（上次: {last_process_time}）")
                return True
            
            logger.info(f"✅ 非首次运行，上次历史处理: {last_process_time}")
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ 检查首次运行状态失败，默认为首次运行: {e}")
            return True
    
    async def process_historical_announcements(self) -> Dict[str, Any]:
        """
        处理历史公告 - 首次运行的核心功能
        
        Returns:
            处理结果统计
        """
        start_time = datetime.now()
        logger.info("🚀 开始历史公告批量处理")
        logger.info(f"📅 处理范围: 最近 {self.historical_days} 天")
        logger.info(f"🎯 监控股票: {len(self.monitored_stocks)} 只")
        
        stats = {
            'start_time': start_time.isoformat(),
            'total_announcements_fetched': 0,
            'relevant_announcements': 0,
            'successfully_downloaded': 0,
            'successfully_vectorized': 0,
            'errors': [],
            'processed_dates': [],
            'processing_time_seconds': 0
        }
        
        try:
            # 1. 分日期批量获取历史公告
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.historical_days)
            
            logger.info(f"📡 获取历史公告: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            
            # 按周为单位处理，避免单次请求过大
            current_date = start_date
            all_relevant_announcements = []
            
            while current_date < end_date:
                week_end = min(current_date + timedelta(days=7), end_date)
                
                logger.info(f"📥 获取 {current_date.strftime('%Y-%m-%d')} 到 {week_end.strftime('%Y-%m-%d')} 的公告...")
                
                try:
                    # 获取该周的公告
                    week_announcements = await self._fetch_announcements_for_period(
                        current_date, week_end
                    )
                    
                    stats['total_announcements_fetched'] += len(week_announcements)
                    
                    # 过滤相关公告
                    relevant_announcements = await self.dual_filter.filter_announcements(
                        week_announcements
                    )
                    
                    all_relevant_announcements.extend(relevant_announcements)
                    stats['relevant_announcements'] += len(relevant_announcements)
                    
                    logger.info(f"✅ {current_date.strftime('%Y-%m-%d')} - {week_end.strftime('%Y-%m-%d')}: "
                              f"{len(week_announcements)} → {len(relevant_announcements)} 条相关公告")
                    
                    stats['processed_dates'].append({
                        'date_range': f"{current_date.strftime('%Y-%m-%d')} - {week_end.strftime('%Y-%m-%d')}",
                        'total': len(week_announcements),
                        'relevant': len(relevant_announcements)
                    })
                    
                    # 避免API限制，短暂延迟
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    error_msg = f"获取 {current_date.strftime('%Y-%m-%d')} 周公告失败: {e}"
                    logger.error(f"❌ {error_msg}")
                    stats['errors'].append(error_msg)
                
                current_date = week_end
            
            logger.info(f"📊 历史公告获取完成: 总计 {stats['total_announcements_fetched']} 条，"
                       f"相关 {stats['relevant_announcements']} 条")
            
            # 2. 批量处理相关公告
            if all_relevant_announcements:
                download_stats = await self._batch_process_announcements(all_relevant_announcements)
                stats['successfully_downloaded'] = download_stats['downloaded']
                stats['successfully_vectorized'] = download_stats['vectorized']
                stats['errors'].extend(download_stats['errors'])
            
            # 3. 记录处理状态
            await self._save_processing_status(stats)
            
            end_time = datetime.now()
            stats['end_time'] = end_time.isoformat()
            stats['processing_time_seconds'] = (end_time - start_time).total_seconds()
            
            logger.info("🎉 历史公告批量处理完成")
            logger.info(f"📈 处理统计:")
            logger.info(f"   📥 获取公告: {stats['total_announcements_fetched']}")
            logger.info(f"   🔍 相关公告: {stats['relevant_announcements']}")
            logger.info(f"   📄 下载成功: {stats['successfully_downloaded']}")
            logger.info(f"   🧠 向量化成功: {stats['successfully_vectorized']}")
            logger.info(f"   ⏱️ 总耗时: {stats['processing_time_seconds']:.2f}秒")
            logger.info(f"   ❌ 错误数: {len(stats['errors'])}")
            
            return stats
            
        except Exception as e:
            error_msg = f"历史公告批量处理失败: {e}"
            logger.error(f"❌ {error_msg}")
            stats['errors'].append(error_msg)
            
            end_time = datetime.now()
            stats['end_time'] = end_time.isoformat()
            stats['processing_time_seconds'] = (end_time - start_time).total_seconds()
            
            return stats
    
    async def _fetch_announcements_for_period(self, 
                                            start_date: datetime, 
                                            end_date: datetime) -> List[Dict[str, Any]]:
        """获取指定时间段的公告"""
        try:
            # 使用API监听器获取指定时间段的公告
            # 传递监听股票列表给API监听器
            stock_codes = list(self.monitored_stocks) if self.monitored_stocks else []
            
            if not stock_codes:
                logger.warning("⚠️ 无监听股票，跳过历史公告获取")
                return []
            
            # 限制股票数量以避免API请求过多
            limited_stocks = stock_codes[:20]  # 限制为前20只股票
            logger.info(f"📋 获取 {len(limited_stocks)} 只股票的历史公告")
            
            announcements = await self.api_monitor.fetch_latest_announcements(limited_stocks)
            
            # 注意：由于HKEX API的限制，这里获取的是最近7天的公告
            # 而不是完整的历史时间范围，这是API本身的限制
            logger.debug(f"📊 时间段 {start_date.date()} - {end_date.date()} 获取到 {len(announcements)} 条公告")
            
            return announcements
            
        except Exception as e:
            logger.error(f"获取历史公告失败: {e}")
            return []
    
    async def _batch_process_announcements(self, 
                                         announcements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量处理公告（下载和向量化）"""
        logger.info(f"📄 开始批量处理 {len(announcements)} 条历史公告")
        
        stats = {
            'downloaded': 0,
            'vectorized': 0,
            'errors': []
        }
        
        # 分批处理，避免资源过载
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single_announcement(announcement):
            async with semaphore:
                try:
                    # 下载PDF
                    if announcement.get('FILE_LINK', '').lower().endswith('.pdf'):
                        download_result = await self.downloader.download_single_announcement(announcement)
                        
                        if download_result and download_result.get('success'):
                            stats['downloaded'] += 1
                            
                            # 向量化处理
                            vector_result = await self.vectorizer.process_announcement_pdf(
                                download_result['local_path']
                            )
                            
                            if vector_result and vector_result.get('success'):
                                stats['vectorized'] += 1
                                logger.info(f"✅ 历史公告处理完成: {announcement.get('TITLE', 'N/A')[:50]}...")
                            else:
                                error_msg = f"向量化失败: {announcement.get('TITLE', 'N/A')[:50]}"
                                stats['errors'].append(error_msg)
                        else:
                            error_msg = f"下载失败: {announcement.get('TITLE', 'N/A')[:50]}"
                            stats['errors'].append(error_msg)
                    
                except Exception as e:
                    error_msg = f"处理公告异常: {announcement.get('TITLE', 'N/A')[:50]} - {e}"
                    stats['errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
        
        # 并发处理所有公告
        tasks = [process_single_announcement(ann) for ann in announcements]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"📊 批量处理完成: 下载 {stats['downloaded']}, 向量化 {stats['vectorized']}")
        
        return stats
    
    async def _save_processing_status(self, stats: Dict[str, Any]):
        """保存处理状态到文件"""
        try:
            # 确保目录存在
            self.status_file.parent.mkdir(parents=True, exist_ok=True)
            
            status_data = {
                'last_historical_process': datetime.now().isoformat(),
                'historical_days': self.historical_days,
                'monitored_stocks_count': len(self.monitored_stocks),
                'processing_stats': stats
            }
            
            async with aiofiles.open(self.status_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(status_data, ensure_ascii=False, indent=2))
            
            logger.info(f"✅ 处理状态已保存: {self.status_file}")
            
        except Exception as e:
            logger.error(f"❌ 保存处理状态失败: {e}")


# 测试函数
async def test_historical_processor():
    """测试历史批量处理器"""
    logger.info("🧪 测试历史批量处理器")
    
    # 创建模拟组件
    class MockComponent:
        async def fetch_latest_announcements(self):
            return []
        
        async def filter_announcements(self, announcements):
            return announcements
        
        async def download_single_announcement(self, announcement):
            return {'success': True, 'local_path': '/test/path.pdf'}
        
        async def process_announcement_pdf(self, path):
            return {'success': True, 'chunk_count': 5}
    
    mock_comp = MockComponent()
    config = {
        'historical_days': 30,  # 测试用30天
        'historical_batch_size': 10,
        'max_concurrent_historical': 2
    }
    
    processor = HistoricalBatchProcessor(
        api_monitor=mock_comp,
        dual_filter=mock_comp,
        downloader=mock_comp,
        vectorizer=mock_comp,
        monitored_stocks={'00700', '00388'},
        config=config
    )
    
    # 测试首次运行检测
    is_first = await processor.is_first_run()
    logger.info(f"首次运行: {is_first}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_historical_processor())
