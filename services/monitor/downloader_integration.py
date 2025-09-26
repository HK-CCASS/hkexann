"""
下载器集成模块
将现有的AsyncHKEXDownloader集成到实时监听系统中
"""

import asyncio
import logging
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# 添加项目根目录到路径，确保可以导入main模块
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from async_downloader import AsyncHKEXDownloader
    from config.settings import settings

    DOWNLOADER_AVAILABLE = True
except ImportError as e:
    DOWNLOADER_AVAILABLE = False
    AsyncHKEXDownloader = None
    IMPORT_ERROR = str(e)

logger = logging.getLogger(__name__)


class RealtimeDownloaderWrapper:
    """
    实时下载器包装器
    
    将现有的AsyncHKEXDownloader适配到实时监听系统中，
    提供单个公告下载和批量下载能力，同时保持原有的
    速率限制、错误处理等功能。
    """

    def __init__(self, config: dict):
        """
        初始化实时下载器包装器
        
        Args:
            config: 下载器配置
        """
        if not DOWNLOADER_AVAILABLE:
            raise ImportError(f"无法导入AsyncHKEXDownloader: {IMPORT_ERROR}")

        self.config = config
        downloader_config = config.get('downloader_integration', {})

        # 下载器配置
        self.use_existing_downloader = downloader_config.get('use_existing_downloader', True)
        self.download_directory = downloader_config.get('download_directory', 'hkexann')
        self.enable_filtering = downloader_config.get('enable_filtering', True)
        self.timeout = downloader_config.get('timeout', 30)

        # 集成配置 - 优化目录结构
        self.preserve_original_filename = downloader_config.get('preserve_original_filename', True)  # 默认使用新格式
        self.create_date_subdirs = downloader_config.get('create_date_subdirs', True)  # 默认创建日期子目录
        self.enable_progress_bar = downloader_config.get('enable_progress_bar', True)
        
        # 🚀 新增：智能分类支持
        self.enable_smart_classification = downloader_config.get('enable_smart_classification', False)
        self.classifier = None
        
        # 初始化智能分类器
        if self.enable_smart_classification:
            try:
                from services.monitor.utils.announcement_classifier import AnnouncementClassifier
                # 优先从downloader_config获取，如果没有则从主config获取
                classifier_config = {
                    'common_keywords': downloader_config.get('common_keywords') or config.get('common_keywords', {}),
                    'announcement_categories': downloader_config.get('announcement_categories') or config.get('announcement_categories', {})
                }
                self.classifier = AnnouncementClassifier(classifier_config)
                logger.info("✅ 智能分类器初始化成功")
            except Exception as e:
                logger.error(f"❌ 智能分类器初始化失败: {e}")
                self.enable_smart_classification = False

        # 初始化下载器
        if self.use_existing_downloader:
            try:
                # 基于新配置系统创建下载器配置
                downloader_config = self._create_downloader_config()

                # 为了兼容现有AsyncHKEXDownloader，创建一个简化的配置对象
                class SimpleConfig:
                    def __init__(self, config_dict):
                        self.config = config_dict

                    def get(self, section, key=None, default=None):
                        # 兼容ConfigManager的get(section, key, default)格式
                        if key is None:
                            # 如果只有一个参数，直接返回配置值
                            return self.config.get(section, default)
                        else:
                            # 如果有两个参数，从section中获取key
                            section_data = self.config.get(section, {})
                            if isinstance(section_data, dict):
                                return section_data.get(key, default)
                            else:
                                return default

                self.config_manager = SimpleConfig(downloader_config)

                # 初始化异步下载器
                self.downloader = AsyncHKEXDownloader(self.config_manager)

                logger.info(f"实时下载器包装器初始化完成")
                logger.info(f"  下载目录: {settings.pdf_data_full_path}")
                logger.info(f"  使用现有downloader: {self.use_existing_downloader}")
                logger.info(f"  启用过滤: {self.enable_filtering}")
                logger.info(f"  保留原始文件名: {self.preserve_original_filename}")
                logger.info(f"  创建日期子目录: {self.create_date_subdirs}")

            except Exception as e:
                logger.error(f"初始化AsyncHKEXDownloader失败: {e}")
                raise
        else:
            logger.warning("已禁用现有downloader集成，下载功能将不可用")
            self.downloader = None
            self.config_manager = None

    def _create_downloader_config(self) -> dict:
        """基于新配置系统创建下载器配置"""
        # 使用新配置系统的PDF路径
        download_directory = str(settings.pdf_data_full_path)

        # 构建下载器需要的配置格式
        downloader_config = {'output_dir': download_directory, 'enable_filter': self.enable_filtering,
            'timeout': self.timeout, 'advanced': {'timeout': self.timeout, 'retry_attempts': 3, 'request_delay': 1,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            'async': {'max_concurrent': self.config.get('max_concurrent', 10),
                'requests_per_second': self.config.get('requests_per_second', 2), 'timeout': self.timeout,
                'min_delay': 0.5, 'max_delay': 1.5, 'backoff_on_429': 60, 'rest': {'enabled': False,  # 实时监听模式不需要休息
                    'work_minutes': 30, 'rest_minutes': 5}}}

        return downloader_config

    async def download_single_announcement(self, announcement: Dict[str, Any]) -> Dict[str, Any]:
        """
        下载单个公告PDF
        
        Args:
            announcement: 公告信息字典，包含URL等信息
            
        Returns:
            下载结果字典
        """
        if not self.downloader:
            return {"success": False, "error": "下载器未初始化", "announcement": announcement}

        try:
            # 提取下载URL
            pdf_url = self._extract_pdf_url(announcement)
            if not pdf_url:
                return {"success": False, "error": "无法提取PDF下载URL", "announcement": announcement}

            logger.info(f"开始下载单个公告: {announcement.get('TITLE', 'Unknown')}")

            # 生成文件路径
            filename = self._generate_filename(announcement)
            download_dir = Path(self.download_directory)
            download_dir.mkdir(parents=True, exist_ok=True)

            # 🚀 智能分类路径生成
            if self.enable_smart_classification and self.classifier:
                try:
                    # 使用智能分类器生成分类路径
                    category_path, keyword_category, priority, confidence = self.classifier.classify_announcement(announcement)
                    
                    if category_path:
                        # 创建分类子目录
                        download_dir = download_dir / category_path
                        
                        # 添加公司信息子目录
                        stock_code = announcement.get('STOCK_CODE', 'UNKNOWN').replace('.HK', '')
                        company_info = f"{stock_code}"
                        download_dir = download_dir / company_info
                        
                        # 添加日期子目录
                        if self.create_date_subdirs:
                            date_formatted = self._format_date(announcement.get('DATE_TIME', ''))
                            if date_formatted:
                                download_dir = download_dir / date_formatted
                        
                        download_dir.mkdir(parents=True, exist_ok=True)
                        
                        logger.info(f"🎯 智能分类: {keyword_category} (优先级: {priority}, 置信度: {confidence:.2f})")
                        logger.info(f"📁 分类路径: {category_path}")
                    else:
                        # 分类失败，使用传统日期目录
                        if self.create_date_subdirs:
                            date_formatted = self._format_date(announcement.get('DATE_TIME', ''))
                            if date_formatted:
                                download_dir = download_dir / date_formatted
                                download_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logger.warning(f"⚠️ 智能分类失败，使用默认路径: {e}")
                    # 分类失败，使用传统日期目录
                    if self.create_date_subdirs:
                        date_formatted = self._format_date(announcement.get('DATE_TIME', ''))
                        if date_formatted:
                            download_dir = download_dir / date_formatted
                            download_dir.mkdir(parents=True, exist_ok=True)
            else:
                # 传统的日期目录模式
                if self.create_date_subdirs:
                    date_formatted = self._format_date(announcement.get('DATE_TIME', ''))
                    if date_formatted:
                        download_dir = download_dir / date_formatted
                        download_dir.mkdir(parents=True, exist_ok=True)

            file_path = download_dir / filename

            # 执行下载
            import time
            start_time = time.time()

            success = await self.downloader.async_download_file(url=pdf_url, filepath=str(file_path),
                title=announcement.get('TITLE', 'Unknown'))

            download_time = time.time() - start_time

            if success:
                # 获取文件大小
                file_size = file_path.stat().st_size if file_path.exists() else 0

                logger.info(f"✅ 公告下载成功: {filename}")
                
                # 如果有多个分类，创建符号链接
                if self.enable_smart_classification and hasattr(self, 'classifier'):
                    try:
                        # 获取所有匹配的关键字
                        all_keywords_str = getattr(self.classifier, '_last_all_keywords', '')
                        if all_keywords_str and '+' in all_keywords_str:
                            # 有多个关键字匹配，需要创建额外的链接
                            all_categories = all_keywords_str.split('+')
                            primary_category = category_path if 'category_path' in locals() else None
                            
                            if primary_category:
                                for category in all_categories:
                                    if category != primary_category:
                                        # 创建次要分类的目录
                                        link_dir = download_dir.parent.parent / category / stock_code
                                        link_dir.mkdir(parents=True, exist_ok=True)
                                        link_path = link_dir / filename
                                        
                                        # 创建符号链接（如果还不存在）
                                        if not link_path.exists():
                                            try:
                                                link_path.symlink_to(file_path)
                                                logger.info(f"📎 创建符号链接: {category}/{stock_code}/{filename}")
                                            except Exception as e:
                                                # Windows可能不支持符号链接，改为复制文件
                                                import shutil
                                                shutil.copy2(file_path, link_path)
                                                logger.info(f"📄 复制文件到: {category}/{stock_code}/{filename}")
                    except Exception as e:
                        logger.debug(f"创建多分类链接时出错: {e}")
                
                return {"success": True, "file_path": str(file_path), "filename": filename, "file_size": file_size,
                    "download_time": download_time, "announcement": announcement}
            else:
                logger.error(f"❌ 公告下载失败: {filename}")
                return {"success": False, "error": "下载失败", "announcement": announcement}

        except Exception as e:
            logger.error(f"❌ 下载单个公告异常: {e}")
            return {"success": False, "error": str(e), "announcement": announcement}

    async def download_batch_announcements(self, announcements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量下载公告PDF
        
        Args:
            announcements: 公告信息列表
            
        Returns:
            下载结果列表
        """
        if not announcements:
            logger.warning("公告列表为空")
            return []

        if not self.downloader:
            return [{"success": False, "error": "下载器未初始化", "announcement": ann} for ann in announcements]

        try:
            logger.info(f"开始批量下载 {len(announcements)} 个公告")

            # 创建下载任务
            download_tasks = []
            for announcement in announcements:
                task = self.download_single_announcement(announcement)
                download_tasks.append(task)

            if not download_tasks:
                logger.warning("没有有效的下载任务")
                return [{"success": False, "error": "无法创建下载任务", "announcement": ann} for ann in announcements]

            # 限制并发数量，避免过载
            semaphore = asyncio.Semaphore(self.downloader.max_concurrent)

            async def download_with_semaphore(task):
                async with semaphore:
                    return await task

            # 并发执行下载任务
            results = await asyncio.gather(*[download_with_semaphore(task) for task in download_tasks],
                return_exceptions=True)

            # 处理异常结果
            formatted_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    formatted_results.append({"success": False, "error": str(result),
                        "announcement": announcements[i] if i < len(announcements) else {}})
                else:
                    formatted_results.append(result)

            # 统计结果
            success_count = sum(1 for r in formatted_results if r.get('success', False))
            failure_count = len(formatted_results) - success_count

            logger.info(f"批量下载完成: 成功 {success_count}, 失败 {failure_count}")

            return formatted_results

        except Exception as e:
            logger.error(f"❌ 批量下载异常: {e}")
            return [{"success": False, "error": str(e), "announcement": ann} for ann in announcements]

    def _extract_pdf_url(self, announcement: Dict[str, Any]) -> Optional[str]:
        """从公告信息中提取PDF下载URL"""
        try:
            # 尝试从不同字段获取URL
            url_fields = ['FILE_LINK', 'file_link', 'url', 'PDF_URL', 'pdf_url', 'LINK']

            for field in url_fields:
                url = announcement.get(field)
                if url and isinstance(url, str) and url.strip():
                    # 确保URL是完整的
                    if not url.startswith('http'):
                        # 添加HKEX基础URL
                        if url.startswith('/'):
                            url = f"https://www1.hkexnews.hk{url}"
                        else:
                            url = f"https://www1.hkexnews.hk/{url}"
                    return url.strip()

            # 如果没有找到URL，尝试从其他字段构建
            file_name = announcement.get('FILE_NAME') or announcement.get('filename')
            if file_name and file_name.endswith('.pdf'):
                # 基于文件名构建URL（这个逻辑可能需要根据实际API调整）
                return f"https://www1.hkexnews.hk/listedco/listconews/sehk/{file_name}"

            return None

        except Exception as e:
            logger.error(f"提取PDF URL失败: {e}")
            return None

    def _create_download_item(self, announcement: Dict[str, Any], pdf_url: str) -> Dict[str, Any]:
        """创建下载项"""
        # 生成文件名
        filename = self._generate_filename(announcement)

        # 包含HKEX分类信息
        metadata = {
            'stock_code': announcement.get('STOCK_CODE', 'Unknown'),
            'title': announcement.get('TITLE', 'Unknown'),
            'date_time': announcement.get('DATE_TIME', ''),
            'announcement_id': announcement.get('ID', ''),
            'source': 'realtime_monitor',
            # HKEX分类信息
            't1_code': announcement.get('T1_CODE', ''),
            't2_code': announcement.get('T2_CODE', ''),
            'hkex_category_name': announcement.get('hkex_category_name', '')  # 如果已解析
        }

        return {'url': pdf_url, 'filename': filename, 'metadata': metadata}

    def _generate_filename(self, announcement: Dict[str, Any]) -> str:
        """生成下载文件名 - 格式: yyyy-mm-dd_股票名称_股票代码_公告标题.pdf"""
        if self.preserve_original_filename:
            # 尝试从公告信息中获取原始文件名
            original_name = announcement.get('FILE_NAME') or announcement.get('filename')
            if original_name and original_name.endswith('.pdf'):
                return original_name

        # 生成标准化文件名: yyyy-mm-dd_股票名称_股票代码_公告标题.pdf
        stock_code = announcement.get('STOCK_CODE', 'Unknown')
        stock_name = announcement.get('STOCK_NAME') or announcement.get('COMPANY_NAME', 'UnknownCompany')
        title = announcement.get('TITLE', 'Announcement')
        date_time = announcement.get('DATE_TIME', '')

        # 提取日期部分并格式化为 yyyy-mm-dd
        date_formatted = self._format_date(date_time)

        # 清理文件名中的特殊字符，包括换行符和控制字符
        stock_name_clean = re.sub(r'[<>:"/\\|?*\s\r\n\t\v\f]', '_', stock_name)[:30]  # 限制长度
        title_clean = re.sub(r'[<>:"/\\|?*\r\n\t\v\f]', '_', title)[:80]  # 限制长度，保留普通空格

        # 新格式: yyyy-mm-dd_股票名称_股票代码_公告标题.pdf
        filename = f"{date_formatted}_{stock_name_clean}_{stock_code}_{title_clean}.pdf"
        return filename

    def _format_date(self, date_time_str: str) -> str:
        """格式化日期时间字符串为 yyyy-mm-dd 格式"""
        if not date_time_str:
            # 如果没有日期，返回 "unknown-date" 而不是当前日期
            logger.warning("公告缺少日期信息，使用 'unknown-date' 标记")
            return "unknown-date"

        try:
            from datetime import datetime

            # 尝试解析不同的日期时间格式，按常见度排序
            date_patterns = [
                '%d/%m/%Y %H:%M',     # 10/09/2025 19:05 (HKEX API 最常见格式)
                '%d/%m/%Y %H:%M:%S',  # 10/09/2025 19:05:00
                '%Y-%m-%d %H:%M:%S',  # 2025-09-11 18:00:00
                '%Y-%m-%d %H:%M',     # 2025-09-11 18:00
                '%Y-%m-%d',           # 2025-09-11
                '%d/%m/%Y',           # 11/09/2025
                '%Y/%m/%d',           # 2025/09/11
                '%d/%m/%Y%H%M',       # 11/09/20251800 (紧凑格式)
                '%Y%m%d%H%M%S',       # 20250911180000
                '%Y%m%d',             # 20250911
            ]

            # 清理输入字符串（去除多余空格）
            date_time_str = date_time_str.strip()

            for pattern in date_patterns:
                try:
                    dt = datetime.strptime(date_time_str, pattern)
                    formatted_date = dt.strftime('%Y-%m-%d')
                    logger.debug(f"成功解析日期: {date_time_str} -> {formatted_date} (使用模式: {pattern})")
                    return formatted_date
                except ValueError:
                    # 尝试部分匹配（处理额外的尾部字符）
                    try:
                        # 计算模式的最小长度
                        min_length = len(datetime.now().strftime(pattern))
                        if len(date_time_str) >= min_length:
                            dt = datetime.strptime(date_time_str[:min_length], pattern)
                            formatted_date = dt.strftime('%Y-%m-%d')
                            logger.debug(f"成功解析日期（部分匹配）: {date_time_str} -> {formatted_date} (使用模式: {pattern})")
                            return formatted_date
                    except:
                        continue

            # 如果都无法解析，记录警告并使用特殊标记
            logger.warning(f"无法解析日期格式: '{date_time_str}'，使用 'unparsed-date' 标记")
            # 尝试提取可能的日期部分作为标记
            safe_str = date_time_str[:20].replace('/', '-').replace(' ', '_').replace(':', '')
            return f"unparsed-{safe_str}"

        except Exception as e:
            logger.error(f"日期格式化异常: {date_time_str}, 错误: {e}")
            # 使用特殊标记而不是当前日期
            return "error-date"

    def get_downloader_stats(self) -> Dict[str, Any]:
        """获取下载器统计信息"""
        if not self.downloader:
            return {"downloader_available": False, "error": "下载器未初始化"}

        return {"downloader_available": True, "download_directory": self.download_directory,
            "use_existing_downloader": self.use_existing_downloader, "enable_filtering": self.enable_filtering,
            "stats": getattr(self.downloader, 'stats', {}),
            "config": {"max_concurrent": getattr(self.downloader, 'max_concurrent', 'Unknown'),
                "requests_per_second": getattr(self.downloader, 'requests_per_second', 'Unknown'),
                "timeout": self.timeout}}

    def get_underlying_downloader(self):
        """
        获取底层的AsyncHKEXDownloader实例
        
        Returns:
            AsyncHKEXDownloader实例，用于需要直接访问原始下载功能的场景
        """
        return self.downloader

    async def test_download(self, test_url: Optional[str] = None) -> Dict[str, Any]:
        """
        测试下载功能
        
        Args:
            test_url: 测试URL，如果不提供则使用模拟数据
            
        Returns:
            测试结果
        """
        if not self.downloader:
            return {"success": False, "error": "下载器未初始化"}

        # 创建测试公告数据
        test_announcement = {'STOCK_CODE': '00700', 'TITLE': '测试公告', 'DATE_TIME': '2025-09-10 18:00:00',
            'FILE_LINK': test_url or 'https://httpbin.org/pdf',  # 使用测试URL
            'ID': 'TEST_001'}

        try:
            result = await self.download_single_announcement(test_announcement)
            return {"success": result.get('success', False), "test_announcement": test_announcement, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e), "test_announcement": test_announcement}


# 用于测试的简单函数
async def test_downloader_integration():
    """测试下载器集成"""
    config = {'downloader_integration': {'use_existing_downloader': True, 'download_directory': 'hkexann',
        'enable_filtering': True, 'timeout': 30, 'preserve_original_filename': True, 'create_date_subdirs': False,
        'enable_progress_bar': False}, 'max_concurrent': 5, 'requests_per_second': 1}

    try:
        wrapper = RealtimeDownloaderWrapper(config)

        # 获取状态
        stats = wrapper.get_downloader_stats()
        print("📊 下载器状态:")
        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    print(f"    {k}: {v}")
            else:
                print(f"  {key}: {value}")

        # 测试下载（使用模拟数据）
        test_result = await wrapper.test_download()
        print("\n🧪 测试结果:")
        for key, value in test_result.items():
            if key == 'result' and isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    if k != 'announcement':
                        print(f"    {k}: {v}")
            elif key != 'test_announcement':
                print(f"  {key}: {value}")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_downloader_integration())
