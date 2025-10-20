#!/usr/bin/env python3
"""
手动历史公告补充脚本

复用现有组件，为指定股票手动补充历史公告数据
支持灵活的日期范围、股票列表和处理参数配置

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-09-17
"""

import asyncio
import logging
import argparse
import yaml
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import List, Set, Dict, Any, Optional
from pathlib import Path
from tqdm.asyncio import tqdm

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

# 导入现有组件
from services.monitor.enhanced_announcement_processor import EnhancedAnnouncementProcessor
from services.monitor.data_flow.corrected_historical_processor import CorrectedHistoricalProcessor
from services.monitor.hkex_official_filter import HKEXOfficialFilter
from services.monitor.downloader_integration import RealtimeDownloaderWrapper
from services.monitor.realtime_vector_processor import RealtimeVectorProcessor
from start_enhanced_monitor import load_config, substitute_env_vars

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('manual_historical_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ManualHistoricalBackfillProcessor:
    """
    手动历史公告补充处理器
    
    复用现有组件实现手动的历史公告补充功能，支持：
    - 指定股票列表
    - 自定义日期范围
    - 灵活的处理参数
    - 完整的进度监控
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化手动历史补充处理器"""
        self.config = config
        self.start_time = datetime.now()

        # 处理统计
        self.stats = {
            'total_stocks': 0,
            'processed_stocks': 0,
            'total_announcements': 0,
            'downloaded_announcements': 0,
            'vectorized_announcements': 0,
            'failed_stocks': 0,
            'errors': [],
            'processing_time': 0.0
        }

        # 初始化组件
        self.downloader = None
        self.vector_processor = None
        self.dual_filter = None
        self.historical_processor = None

        # 进度条
        self.stock_progress_bar = None

        # 断点续传相关
        self.session_id = str(uuid.uuid4())
        self.resume_state_file = Path("hkexann") / ".manual_backfill_resume.json"
        self.resume_state_file.parent.mkdir(exist_ok=True)
        self.resume_state = None

        logger.info("🚀 手动历史公告补充处理器初始化完成")

    async def _progress_callback(self, progress_info: Dict[str, Any]):
        """进度回调函数"""
        if self.stock_progress_bar:
            stage = progress_info.get('stage', '')
            batch_idx = progress_info.get('batch_idx', 0)
            total_batches = progress_info.get('total_batches', 1)
            stocks_processed = progress_info.get('stocks_processed', 0)
            total_stocks = progress_info.get('total_stocks', 0)
            stocks_in_batch = progress_info.get('stocks_in_batch', 0)

            if stage == 'batch_start':
                self.stock_progress_bar.set_description(f"🔍 批次 {batch_idx}/{total_batches} - 搜索中")
            elif stage == 'search_complete':
                announcements_found = progress_info.get('announcements_found', 0)
                self.stock_progress_bar.set_description(f"📊 批次 {batch_idx}/{total_batches} - 发现{announcements_found}条公告")
            elif stage == 'download_complete':
                announcements_downloaded = progress_info.get('announcements_downloaded', 0)
                announcements_vectorized = progress_info.get('announcements_vectorized', 0)
                self.stock_progress_bar.set_description(f"📥 批次 {batch_idx}/{total_batches} - 下载{announcements_downloaded}条，向量化{announcements_vectorized}条")
                # 更新进度条
                self.stock_progress_bar.update(stocks_in_batch)

    async def _resume_state_callback(self, progress_data: Dict[str, Any]):
        """续传状态回调函数"""
        try:
            # 如果是续传模式，直接更新状态
            if self.resume_state:
                self.resume_state['progress'].update(progress_data)
                # 实时保存状态
                await self.save_resume_state(
                    self.resume_state['parameters'],
                    self.resume_state['progress']
                )
            # 如果不是续传模式，在 process_stocks_historical 中已经保存了初始状态

        except Exception as e:
            logger.warning(f"⚠️ 续传状态回调失败: {e}")

    async def check_resume_state(self) -> Optional[Dict[str, Any]]:
        """
        检查是否有未完成的处理任务

        Returns:
            Optional[Dict[str, Any]]: 恢复状态，如果没有则返回None
        """
        try:
            if not self.resume_state_file.exists():
                logger.info("🔍 未发现未完成的处理任务")
                return None

            import json
            with open(self.resume_state_file, 'r', encoding='utf-8') as f:
                state_data = json.load(f)

            # 检查状态是否有效
            if state_data.get('status') != 'in_progress':
                logger.info("📋 发现已完成或失败的任务状态")
                return None

            # 检查是否超时（24小时）
            start_time = datetime.fromisoformat(state_data['start_time'])
            if datetime.now() - start_time > timedelta(hours=24):
                logger.warning("⚠️ 发现超时的未完成任务（超过24小时），将重新开始")
                return None

            logger.info(f"🔄 发现未完成的处理任务，会话ID: {state_data['session_id']}")
            logger.info(f"📊 进度: 已处理 {len(state_data['progress']['processed_stocks'])} 只股票")

            return state_data

        except Exception as e:
            logger.warning(f"⚠️ 检查恢复状态失败: {e}")
            return None

    async def save_resume_state(self, parameters: Dict[str, Any], progress: Dict[str, Any]):
        """
        保存断点续传状态

        Args:
            parameters: 处理参数
            progress: 处理进度
        """
        try:
            state_data = {
                'session_id': self.session_id,
                'start_time': self.start_time.isoformat(),
                'parameters': parameters,
                'progress': progress,
                'status': 'in_progress'
            }

            import json
            with open(self.resume_state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.warning(f"⚠️ 保存恢复状态失败: {e}")

    async def mark_resume_completed(self):
        """标记恢复任务为完成"""
        try:
            if self.resume_state_file.exists():
                import json
                with open(self.resume_state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)

                state_data['status'] = 'completed'
                state_data['end_time'] = datetime.now().isoformat()

                with open(self.resume_state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f, ensure_ascii=False, indent=2)

                logger.info("✅ 断点续传状态已标记为完成")

        except Exception as e:
            logger.warning(f"⚠️ 标记完成状态失败: {e}")

    async def initialize(self) -> bool:
        """初始化所有组件"""
        try:
            logger.info("📊 正在初始化处理组件...")
            
            # 1. 初始化下载器
            logger.info("📥 初始化下载器...")
            self.downloader = RealtimeDownloaderWrapper(self.config)
            
            # 2. 初始化向量化处理器
            logger.info("🧠 初始化向量化处理器...")
            self.vector_processor = RealtimeVectorProcessor(self.config)
            
            # 3. 初始化HKEX官方过滤器
            logger.info("🔬 初始化HKEX官方过滤器...")
            self.hkex_filter = HKEXOfficialFilter(self.config)
            await self.hkex_filter.initialize()
            
            # 4. 初始化历史处理器
            logger.info("📚 初始化历史处理器...")
            historical_config = self.config.get('manual_historical_processing', {})
            historical_config.update({
                'common_keywords': self.config.get('common_keywords', {}),
                'announcement_categories': self.config.get('announcement_categories', {})
            })
            
            self.historical_processor = CorrectedHistoricalProcessor(
                hkex_downloader=self.downloader.get_underlying_downloader(),
                simple_filter=self.hkex_filter,
                vectorizer=self.vector_processor,
                monitored_stocks=set(),  # 动态设置
                config=historical_config,
                progress_callback=self._progress_callback,
                resume_state_callback=self._resume_state_callback
            )
            
            logger.info("✅ 所有组件初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"❌ 组件初始化失败: {e}")
            return False

    async def process_stocks_historical(self,
                                      stock_codes: List[str],
                                      start_date: str,
                                      end_date: str,
                                      batch_size: int = 5,
                                      enable_filtering: bool = True) -> Dict[str, Any]:
        """
        为指定股票补充历史公告

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            batch_size: 批处理大小
            enable_filtering: 是否启用过滤

        Returns:
            处理结果统计
        """
        try:
            # 检查是否有未完成的处理任务
            resume_state = await self.check_resume_state()
            if resume_state:
                # 检查参数是否匹配
                params = resume_state['parameters']
                if (params['start_date'] == start_date and
                    params['end_date'] == end_date and
                    params['batch_size'] == batch_size and
                    params['enable_filtering'] == enable_filtering):

                    # 参数匹配，可以续传
                    logger.info("🔄 参数匹配，开始断点续传")
                    processed_stocks = set(resume_state['progress']['processed_stocks'])
                    remaining_stocks = [code for code in stock_codes if code not in processed_stocks]

                    if not remaining_stocks:
                        logger.info("✅ 所有股票已处理完成")
                        await self.mark_resume_completed()
                        return await self._build_resume_result(resume_state)

                    logger.info(f"📋 续传模式: 已处理 {len(processed_stocks)} 只，剩余 {len(remaining_stocks)} 只")
                    stock_codes = remaining_stocks
                    self.resume_state = resume_state
                else:
                    logger.warning("⚠️ 参数不匹配，将重新开始处理")
                    resume_state = None

            if not resume_state:
                logger.info(f"🎯 开始处理 {len(stock_codes)} 只股票的历史公告")
                logger.info(f"📅 日期范围: {start_date} 至 {end_date}")
                logger.info(f"📦 批处理大小: {batch_size}")
                logger.info(f"🔬 启用过滤: {enable_filtering}")

            self.stats['total_stocks'] = len(stock_codes)
            stock_set = set(stock_codes)
            
            # 更新过滤器的股票列表
            if enable_filtering:
                self.dual_filter.update_monitored_stocks(stock_set)
                logger.info(f"🔬 已更新过滤器监控股票: {len(stock_set)} 只")
            
            # 设置历史处理器的目标股票
            original_stocks = self.historical_processor.monitored_stocks.copy()
            self.historical_processor.monitored_stocks = stock_set
            
            # 更新历史处理器的日期配置
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                days_diff = (end_dt - start_dt).days + 1
                
                # 动态调整配置
                self.historical_processor.historical_days = days_diff
                self.historical_processor.stock_batch_size = batch_size
                
                logger.info(f"📊 配置更新: 历史天数={days_diff}, 批处理={batch_size}")
                
            except ValueError as e:
                logger.error(f"❌ 日期格式错误: {e}")
                return self._build_error_result("日期格式错误")
            
            try:
                # 执行历史处理
                logger.info("🚀 开始执行历史公告处理...")
                processing_start = datetime.now()

                # 保存初始处理状态（如果不是续传）
                if not self.resume_state:
                    await self.save_resume_state({
                        'stock_codes': stock_codes,
                        'start_date': start_date,
                        'end_date': end_date,
                        'batch_size': batch_size,
                        'enable_filtering': enable_filtering
                    }, {
                        'processed_stocks': [],
                        'stats': self.stats.copy()
                    })

                # 创建股票处理进度条
                with tqdm(total=len(stock_codes), desc="📊 处理股票", unit="只",
                         bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as self.stock_progress_bar:

                    # 设置初始状态
                    if self.resume_state:
                        self.stock_progress_bar.set_description("🔄 继续处理")
                    else:
                        self.stock_progress_bar.set_description("🚀 开始处理")

                    result = await self.historical_processor.process_historical_announcements()

                    # 确保进度条显示完成状态
                    if self.stock_progress_bar.n < len(stock_codes):
                        self.stock_progress_bar.update(len(stock_codes) - self.stock_progress_bar.n)
                    self.stock_progress_bar.set_description("✅ 处理完成")

                processing_time = (datetime.now() - processing_start).total_seconds()

                # 更新统计信息
                self._update_stats(result, processing_time)

                # 合并续传状态的统计信息
                if self.resume_state:
                    resume_progress = self.resume_state['progress']
                    for key in ['total_announcements', 'downloaded_announcements', 'vectorized_announcements']:
                        if key in self.stats and key in resume_progress.get('stats', {}):
                            self.stats[key] += resume_progress['stats'][key]

                # 标记续传任务为完成
                await self.mark_resume_completed()

                logger.info(f"✅ 历史处理完成，耗时 {processing_time:.1f} 秒")
                logger.info(f"📊 处理结果: {result}")

                return self._build_success_result(result)
                
            finally:
                # 恢复原始股票列表
                self.historical_processor.monitored_stocks = original_stocks
                
        except Exception as e:
            logger.error(f"❌ 历史处理失败: {e}")
            self.stats['errors'].append(str(e))
            return self._build_error_result(str(e))

    def _update_stats(self, result: Dict[str, Any], processing_time: float):
        """更新处理统计"""
        self.stats.update({
            'processed_stocks': self.stats['total_stocks'],
            'total_announcements': result.get('total_announcements_found', 0),
            'downloaded_announcements': result.get('total_announcements_downloaded', 0),
            'vectorized_announcements': result.get('total_announcements_vectorized', 0),
            'processing_time': processing_time
        })

    def _build_success_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """构建成功结果"""
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'status': 'success',
            'summary': {
                'total_stocks': self.stats['total_stocks'],
                'processed_stocks': self.stats['processed_stocks'],
                'total_announcements': self.stats['total_announcements'],
                'downloaded_announcements': self.stats['downloaded_announcements'],
                'vectorized_announcements': self.stats['vectorized_announcements'],
                'processing_time_seconds': self.stats['processing_time'],
                'total_time_seconds': total_time
            },
            'efficiency': {
                'download_success_rate': (
                    self.stats['downloaded_announcements'] / 
                    max(self.stats['total_announcements'], 1) * 100
                ),
                'vectorization_success_rate': (
                    self.stats['vectorized_announcements'] / 
                    max(self.stats['downloaded_announcements'], 1) * 100
                ),
                'avg_time_per_stock': (
                    self.stats['processing_time'] / 
                    max(self.stats['processed_stocks'], 1)
                )
            },
            'raw_result': result
        }

    async def _build_resume_result(self, resume_state: Dict[str, Any]) -> Dict[str, Any]:
        """构建续传完成结果"""
        progress = resume_state['progress']
        total_time = (datetime.now() - self.start_time).total_seconds()

        return {
            'status': 'resumed_completed',
            'summary': {
                'total_stocks': len(resume_state['parameters']['stock_codes']),
                'processed_stocks': len(progress['processed_stocks']),
                'total_announcements': progress['stats'].get('total_announcements_found', 0),
                'downloaded_announcements': progress['stats'].get('total_announcements_downloaded', 0),
                'vectorized_announcements': progress['stats'].get('total_announcements_vectorized', 0),
                'processing_time_seconds': progress['stats'].get('processing_time', 0.0),
                'total_time_seconds': total_time
            },
            'efficiency': {
                'download_success_rate': (
                    progress['stats'].get('total_announcements_downloaded', 0) /
                    max(progress['stats'].get('total_announcements_found', 1), 1) * 100
                ),
                'vectorization_success_rate': (
                    progress['stats'].get('total_announcements_vectorized', 0) /
                    max(progress['stats'].get('total_announcements_downloaded', 1), 1) * 100
                ),
                'avg_time_per_stock': (
                    progress['stats'].get('processing_time', 0.0) /
                    max(len(progress['processed_stocks']), 1)
                )
            },
            'resume_info': {
                'session_id': resume_state['session_id'],
                'original_start_time': resume_state['start_time'],
                'resumed_at': datetime.now().isoformat()
            }
        }

    def _build_error_result(self, error_msg: str) -> Dict[str, Any]:
        """构建错误结果"""
        return {
            'status': 'error',
            'error_message': error_msg,
            'partial_stats': self.stats
        }

    async def close(self):
        """关闭所有组件和连接"""
        logger.info("🔚 关闭处理组件...")
        
        try:
            if self.downloader:
                underlying_downloader = self.downloader.get_underlying_downloader()
                if underlying_downloader:
                    await underlying_downloader.close()
            
            if self.vector_processor:
                await self.vector_processor.close()
                
            logger.info("✅ 所有组件已关闭")
            
        except Exception as e:
            logger.warning(f"⚠️ 关闭组件时出现警告: {e}")


async def parse_stock_list(stock_input: str) -> List[str]:
    """
    解析股票列表输入
    
    支持的格式：
    - 单个股票: "00700"
    - 多个股票: "00700,00939,01398"
    - 文件路径: "stocks.txt" (每行一个股票代码)
    """
    stock_codes = []
    
    if os.path.isfile(stock_input):
        # 从文件读取
        logger.info(f"📄 从文件读取股票列表: {stock_input}")
        with open(stock_input, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # 处理行内注释，取#之前的部分
                if '#' in line:
                    code_part = line.split('#')[0].strip()
                else:
                    code_part = line
                
                if code_part:
                    stock_codes.append(code_part)
    else:
        # 直接解析
        stock_codes = [code.strip() for code in stock_input.split(',') if code.strip()]
    
    # 标准化股票代码格式
    normalized_codes = []
    for code in stock_codes:
        # 移除.HK后缀，补齐前导零
        clean_code = code.replace('.HK', '').replace('.hk', '').strip()
        if clean_code.isdigit():
            normalized_code = clean_code.zfill(5)  # 补齐到5位
            normalized_codes.append(normalized_code)
        else:
            logger.warning(f"⚠️ 跳过无效股票代码: {code}")
    
    logger.info(f"✅ 解析得到 {len(normalized_codes)} 只有效股票")
    return normalized_codes


def create_sample_config() -> Dict[str, Any]:
    """创建示例配置（用于独立运行）"""
    return {
        'api_endpoints': {
            'base_url': 'https://www1.hkexnews.hk',
            'stock_search': '/search/prefix.do',
            'title_search': '/search/titleSearchServlet.do',
            'referer': 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh',
            'callback_param': 'callback',
            'market': 'SEHK'
        },
        'settings': {
            'save_path': './hkexann',
            'verbose_logging': True
        },
        'downloader_integration': {
            'use_existing_downloader': True,
            'download_directory': './hkexann',
            'enable_filtering': True,
            'timeout': 30,
            'enable_smart_classification': True,
            'common_keywords': {},
            'announcement_categories': {}
        },
        'vectorization_integration': {
            'use_existing_pipeline': True,
            'pdf_directory': './hkexann',
            'collection_name': 'pdf_embeddings_v3',
            'batch_size': 15,
            'max_concurrent': 5
        },
        'manual_historical_processing': {
            'historical_days': 30,
            'stock_batch_size': 5,
            'date_chunk_days': 7,
            'max_concurrent_historical': 3,
            'api_delay': 1.0
        },
        'dual_filter': {
            'stock_filter_enabled': True,
            'type_filter_enabled': True,
            'excluded_categories': [],
            'included_keywords': [
                '供股', '配股', '合股', '可转换', '须予披露', '收购', '私有化'
            ]
        },
        'common_keywords': {},
        'announcement_categories': {}
    }


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="手动历史公告补充脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 单个股票，最近7天
  python manual_historical_backfill.py -s 00700 -d 7
  
  # 多个股票，指定日期范围
  python manual_historical_backfill.py -s "00700,00939,01398" --start-date 2025-09-10 --end-date 2025-09-17
  
  # 从文件读取股票列表
  python manual_historical_backfill.py -s stocks.txt -d 30
  
  # 自定义配置文件和批处理大小
  python manual_historical_backfill.py -s 00700 -d 7 -c custom_config.yaml -b 10
  
  # 禁用过滤，获取所有公告
  python manual_historical_backfill.py -s 00700 -d 7 --no-filter
        """)
    
    parser.add_argument('-s', '--stocks', required=True,
                       help='股票代码列表，可以是单个股票(00700)、多个股票(00700,00939)或文件路径(stocks.txt)')
    
    # 日期选项（互斥）
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('-d', '--days', type=int,
                           help='回溯天数（从今天开始往前）')
    date_group.add_argument('--date-range', nargs=2, metavar=('START', 'END'),
                           help='指定日期范围，格式: YYYY-MM-DD YYYY-MM-DD')
    
    parser.add_argument('-c', '--config', default='config.yaml',
                       help='配置文件路径 (默认: config.yaml)')
    parser.add_argument('-b', '--batch-size', type=int, default=5,
                       help='批处理大小 (默认: 5)')
    parser.add_argument('--no-filter', action='store_true',
                       help='禁用公告过滤')
    parser.add_argument('--dry-run', action='store_true',
                       help='试运行模式，只显示会处理的内容，不实际执行')
    parser.add_argument('--output', 
                       help='结果输出文件路径（JSON格式）')
    
    args = parser.parse_args()
    
    # 解析日期范围
    if args.days:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=args.days - 1)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
    else:
        start_date_str, end_date_str = args.date_range
        try:
            datetime.strptime(start_date_str, '%Y-%m-%d')
            datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            logger.error("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
            return 1
    
    # 解析股票列表
    try:
        stock_codes = await parse_stock_list(args.stocks)
        if not stock_codes:
            logger.error("❌ 未找到有效的股票代码")
            return 1
    except Exception as e:
        logger.error(f"❌ 解析股票列表失败: {e}")
        return 1
    
    # 加载配置
    try:
        if os.path.exists(args.config):
            logger.info(f"📄 加载配置文件: {args.config}")
            config = load_config(args.config)
        else:
            logger.warning(f"⚠️ 配置文件不存在，使用示例配置: {args.config}")
            config = create_sample_config()
    except Exception as e:
        logger.error(f"❌ 加载配置失败: {e}")
        return 1
    
    # 显示处理信息
    logger.info("="*70)
    logger.info("🚀 手动历史公告补充脚本启动")
    logger.info("="*70)
    logger.info(f"📊 股票数量: {len(stock_codes)}")
    logger.info(f"📅 日期范围: {start_date_str} 至 {end_date_str}")
    logger.info(f"📦 批处理大小: {args.batch_size}")
    logger.info(f"🔬 启用过滤: {not args.no_filter}")
    logger.info(f"🎯 目标股票: {', '.join(stock_codes[:10])}" + 
                (f" ... (共{len(stock_codes)}只)" if len(stock_codes) > 10 else ""))
    
    if args.dry_run:
        logger.info("🔍 试运行模式，不会实际执行处理")
        logger.info("="*70)
        return 0
    
    # 创建处理器并执行
    processor = ManualHistoricalBackfillProcessor(config)
    
    try:
        # 初始化
        if not await processor.initialize():
            logger.error("❌ 处理器初始化失败")
            return 1
        
        # 执行处理
        result = await processor.process_stocks_historical(
            stock_codes=stock_codes,
            start_date=start_date_str,
            end_date=end_date_str,
            batch_size=args.batch_size,
            enable_filtering=not args.no_filter
        )
        
        # 显示结果
        logger.info("="*70)
        if result['status'] == 'success':
            logger.info("✅ 处理完成")
            summary = result['summary']
            efficiency = result['efficiency']
            
            logger.info(f"📊 处理统计:")
            logger.info(f"  • 目标股票: {summary['total_stocks']} 只")
            logger.info(f"  • 处理股票: {summary['processed_stocks']} 只")
            logger.info(f"  • 发现公告: {summary['total_announcements']} 条")
            logger.info(f"  • 下载成功: {summary['downloaded_announcements']} 条")
            logger.info(f"  • 向量化成功: {summary['vectorized_announcements']} 条")
            logger.info(f"  • 处理耗时: {summary['processing_time_seconds']:.1f} 秒")
            logger.info(f"  • 总计耗时: {summary['total_time_seconds']:.1f} 秒")
            
            logger.info(f"📈 效率指标:")
            logger.info(f"  • 下载成功率: {efficiency['download_success_rate']:.1f}%")
            logger.info(f"  • 向量化成功率: {efficiency['vectorization_success_rate']:.1f}%")
            logger.info(f"  • 平均处理时间: {efficiency['avg_time_per_stock']:.1f} 秒/股票")
            
        else:
            logger.error("❌ 处理失败")
            logger.error(f"错误信息: {result['error_message']}")
        
        # 输出结果文件
        if args.output:
            import json
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"📄 结果已保存到: {args.output}")
        
        logger.info("="*70)
        return 0 if result['status'] == 'success' else 1
        
    except KeyboardInterrupt:
        logger.info("⏹️ 用户中断处理")
        return 1
    except Exception as e:
        logger.error(f"❌ 处理过程中发生异常: {e}")
        return 1
    finally:
        await processor.close()


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("⏹️ 程序被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 程序异常退出: {e}")
        sys.exit(1)
