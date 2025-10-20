"""
向后兼容适配器
提供对现有系统的兼容支持
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import asyncio

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from ..config.unified_config import ConfigManager, UnifiedConfig
from ..file_manager.unified_file_manager import UnifiedFileManager
from ..core.downloader_abstract import UnifiedDownloader, DownloadResult

logger = logging.getLogger(__name__)


class LegacyConfigAdapter:
    """旧版配置适配器"""

    def __init__(self, legacy_config: Any):
        """
        初始化旧版配置适配器

        Args:
            legacy_config: 旧版配置对象（ConfigManager from main.py）
        """
        self.legacy_config = legacy_config
        self.unified_config = self._convert_to_unified()

    def _convert_to_unified(self) -> UnifiedConfig:
        """将旧版配置转换为统一配置"""
        config_manager = ConfigManager()

        # 构建旧版配置字典
        legacy_dict = self._extract_legacy_dict()

        # 使用ConfigManager的转换功能
        config_manager.config = config_manager._convert_legacy_config(legacy_dict)

        return config_manager.config

    def _extract_legacy_dict(self) -> Dict[str, Any]:
        """从旧版配置对象提取字典"""
        legacy_dict = {}

        # 提取settings
        if hasattr(self.legacy_config, 'config'):
            config = self.legacy_config.config
            if 'settings' in config:
                legacy_dict['settings'] = config['settings']
            if 'async' in config:
                legacy_dict['async'] = config['async']
            if 'api_endpoints' in config:
                legacy_dict['api_endpoints'] = config['api_endpoints']
            if 'database' in config:
                legacy_dict['database'] = config['database']
            if 'stock_discovery' in config:
                legacy_dict['stock_discovery'] = config['stock_discovery']
            if 'common_keywords' in config:
                legacy_dict['common_keywords'] = config['common_keywords']
            if 'announcement_categories' in config:
                legacy_dict['announcement_categories'] = config['announcement_categories']
            if 'dual_filter' in config:
                legacy_dict['dual_filter'] = config['dual_filter']
        else:
            # 如果是字典类型
            legacy_dict = dict(self.legacy_config)

        return legacy_dict

    def get_unified_config(self) -> UnifiedConfig:
        """获取统一配置对象"""
        return self.unified_config

    def sync_back(self, unified_config: UnifiedConfig) -> None:
        """
        将统一配置同步回旧版配置

        Args:
            unified_config: 统一配置对象
        """
        config_manager = ConfigManager()
        config_manager.config = unified_config

        # 导出为旧版格式
        legacy_dict = config_manager.export_legacy()

        # 更新旧版配置
        if hasattr(self.legacy_config, 'config'):
            self.legacy_config.config.update(legacy_dict)
        else:
            self.legacy_config.update(legacy_dict)


class MainPyAdapter:
    """main.py下载器适配器"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化main.py适配器

        Args:
            config_path: 配置文件路径
        """
        try:
            # 尝试导入main.py的类
            from main import HKEXDownloader, ConfigManager as LegacyConfigManager

            # main.py的ConfigManager在构造函数中接受config_file参数
            if config_path:
                self.legacy_config_manager = LegacyConfigManager(config_path)
            else:
                self.legacy_config_manager = LegacyConfigManager()

            self.legacy_downloader = HKEXDownloader(self.legacy_config_manager)
            self.available = True
        except ImportError as e:
            logger.warning(f"无法导入main.py组件: {e}")
            self.available = False
            self.legacy_downloader = None
            self.legacy_config_manager = None

        # 创建统一配置
        if self.available:
            adapter = LegacyConfigAdapter(self.legacy_config_manager)
            self.unified_config = adapter.get_unified_config()
        else:
            self.unified_config = UnifiedConfig()

        # 创建统一组件
        self.file_manager = UnifiedFileManager(
            self.unified_config.file_management,
            self.unified_config.filter.classification_rules
        )
        self.downloader = UnifiedDownloader(self.unified_config.to_dict())

    def download_announcements(self, task: Dict[str, Any]) -> Tuple[str, int]:
        """
        使用旧版接口下载公告

        Args:
            task: 下载任务

        Returns:
            (保存路径, 下载数量)
        """
        if not self.available:
            logger.error("旧版下载器不可用")
            return "", 0

        # 调用旧版下载器
        try:
            save_path, count = self.legacy_downloader.download_announcements(task)
            return save_path, count
        except Exception as e:
            logger.error(f"旧版下载器执行失败: {e}")
            return "", 0

    async def download_announcements_unified(self, task: Dict[str, Any]) -> Tuple[str, int]:
        """
        使用统一接口下载公告

        Args:
            task: 下载任务

        Returns:
            (保存路径, 下载数量)
        """
        # 从任务中提取公告列表
        announcements = self._extract_announcements_from_task(task)

        # 批量下载
        results = await self.downloader.download_batch(announcements)

        # 保存文件
        saved_files = []
        for result in results:
            if result.success and hasattr(result, 'content'):
                file_path = self.file_manager.save_file(
                    result.content,
                    result.task.announcement
                )
                saved_files.append(file_path)

        # 返回结果
        base_path = self.unified_config.file_management.base_path
        return base_path, len(saved_files)

    def _extract_announcements_from_task(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从任务中提取公告列表"""
        # 这里需要根据实际的任务格式进行解析
        # 示例实现
        announcements = []

        stock_code = task.get('stock_code')
        if isinstance(stock_code, list):
            stock_codes = stock_code
        else:
            stock_codes = [stock_code]

        # 为每个股票代码创建公告
        for code in stock_codes:
            announcement = {
                'stock_code': code,
                'date': task.get('start_date', ''),
                'url': f"https://www1.hkexnews.hk/...",  # 需要实际的URL生成逻辑
                'title': '',
                'company_name': ''
            }
            announcements.append(announcement)

        return announcements

    def migrate_config(self, output_path: str) -> None:
        """
        迁移配置到新格式

        Args:
            output_path: 输出路径
        """
        config_manager = ConfigManager()
        config_manager.config = self.unified_config
        config_manager.save(output_path)
        logger.info(f"配置已迁移到: {output_path}")


class MonitorAdapter:
    """监控系统适配器"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化监控系统适配器

        Args:
            config: 监控配置
        """
        self.config = config

        try:
            # 尝试导入监控系统组件
            from services.monitor.downloader_integration import RealtimeDownloaderWrapper

            self.monitor_wrapper = RealtimeDownloaderWrapper(config)
            self.available = True
        except ImportError as e:
            logger.warning(f"无法导入监控系统组件: {e}")
            self.available = False
            self.monitor_wrapper = None

        # 创建统一组件
        unified_config = self._convert_monitor_config(config)
        self.file_manager = UnifiedFileManager(
            unified_config.file_management,
            unified_config.filter.classification_rules
        )
        self.downloader = UnifiedDownloader(unified_config.to_dict())

    def _convert_monitor_config(self, monitor_config: Dict[str, Any]) -> UnifiedConfig:
        """转换监控配置到统一格式"""
        config_manager = ConfigManager()

        # 构建配置字典
        config_dict = {
            'file_management': {
                'base_path': monitor_config.get('downloader_integration', {}).get('download_directory', 'hkexann'),
                'create_date_subdirs': monitor_config.get('downloader_integration', {}).get('create_date_subdirs', True),
                'naming_strategy': 'standard' if monitor_config.get('downloader_integration', {}).get('preserve_original_filename', True) else 'original'
            },
            'downloader': {
                'timeout': monitor_config.get('downloader_integration', {}).get('timeout', 30),
                'enable_progress_bar': monitor_config.get('downloader_integration', {}).get('enable_progress_bar', True)
            },
            'filter': {
                'enable_smart_classification': monitor_config.get('downloader_integration', {}).get('enable_smart_classification', False),
                'classification_rules': monitor_config.get('common_keywords', {})
            }
        }

        return ConfigManager.from_dict(config_dict)

    async def download_announcement(self, announcement: Dict[str, Any]) -> Optional[str]:
        """
        下载单个公告（监控系统接口）

        Args:
            announcement: 公告信息

        Returns:
            文件路径或None
        """
        if self.available and self.monitor_wrapper:
            # 使用监控系统下载器
            try:
                result = await self.monitor_wrapper.download_announcement(announcement)
                return result.get('file_path')
            except Exception as e:
                logger.error(f"监控系统下载失败: {e}")

        # 使用统一下载器
        result = await self.downloader.download_single(announcement)

        if result.success:
            # 保存文件
            if hasattr(result, 'content'):
                file_path = self.file_manager.save_file(
                    result.content,
                    result.task.announcement
                )
                return str(file_path)

        return None


class AsyncDownloaderAdapter:
    """async_downloader.py适配器"""

    def __init__(self, config_manager: Any):
        """
        初始化异步下载器适配器

        Args:
            config_manager: 配置管理器
        """
        try:
            # 尝试导入async_downloader
            from async_downloader import AsyncHKEXDownloader

            self.async_downloader = AsyncHKEXDownloader(config_manager)
            self.available = True
        except ImportError as e:
            logger.warning(f"无法导入async_downloader: {e}")
            self.available = False
            self.async_downloader = None

        # 创建统一组件
        adapter = LegacyConfigAdapter(config_manager)
        self.unified_config = adapter.get_unified_config()
        self.file_manager = UnifiedFileManager(
            self.unified_config.file_management,
            self.unified_config.filter.classification_rules
        )
        self.downloader = UnifiedDownloader(self.unified_config.to_dict())

    async def run_download(self, task: Dict[str, Any]) -> Tuple[str, int, int]:
        """
        运行异步下载

        Args:
            task: 下载任务

        Returns:
            (保存路径, 下载数量, 跳过数量)
        """
        if self.available and self.async_downloader:
            # 使用原异步下载器
            try:
                result = await self.async_downloader.run(task)
                return result
            except Exception as e:
                logger.error(f"异步下载器执行失败: {e}")

        # 使用统一下载器
        announcements = self._extract_announcements(task)
        results = await self.downloader.download_batch(announcements)

        downloaded = 0
        skipped = 0

        for result in results:
            if result.success:
                downloaded += 1
            elif result.task.status.value == 'skipped':
                skipped += 1

        base_path = self.unified_config.file_management.base_path
        return base_path, downloaded, skipped

    def _extract_announcements(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从任务提取公告列表"""
        # 实现提取逻辑
        return []


class UnifiedAPIAdapter:
    """统一API适配器 - 提供统一的接口给所有系统"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化统一API适配器

        Args:
            config_path: 配置文件路径
        """
        # 加载统一配置
        self.config_manager = ConfigManager(config_path)

        # 创建各种适配器
        self.main_adapter = MainPyAdapter(config_path)
        self.monitor_adapter = None  # 按需创建
        self.async_adapter = None  # 按需创建

        # 创建统一组件
        self.file_manager = UnifiedFileManager(
            self.config_manager.config.file_management,
            self.config_manager.config.filter.classification_rules
        )
        self.downloader = UnifiedDownloader(self.config_manager.config.to_dict())

    def get_adapter(self, adapter_type: str) -> Any:
        """
        获取特定类型的适配器

        Args:
            adapter_type: 适配器类型 (main, monitor, async)

        Returns:
            适配器实例
        """
        if adapter_type == 'main':
            return self.main_adapter
        elif adapter_type == 'monitor':
            if self.monitor_adapter is None:
                self.monitor_adapter = MonitorAdapter(self.config_manager.config.to_dict())
            return self.monitor_adapter
        elif adapter_type == 'async':
            if self.async_adapter is None:
                # 需要传入旧版config_manager
                self.async_adapter = AsyncDownloaderAdapter(self.main_adapter.legacy_config_manager)
            return self.async_adapter
        else:
            raise ValueError(f"未知的适配器类型: {adapter_type}")

    async def download(
        self,
        task: Dict[str, Any],
        use_adapter: Optional[str] = None
    ) -> Tuple[str, int]:
        """
        统一下载接口

        Args:
            task: 下载任务
            use_adapter: 使用的适配器类型

        Returns:
            (保存路径, 下载数量)
        """
        if use_adapter:
            adapter = self.get_adapter(use_adapter)
            if adapter == self.main_adapter:
                return await adapter.download_announcements_unified(task)
            elif adapter == self.async_adapter:
                path, count, _ = await adapter.run_download(task)
                return path, count

        # 默认使用统一下载器
        announcements = self._parse_task(task)
        results = await self.downloader.download_batch(announcements)

        saved_count = 0
        for result in results:
            if result.success:
                saved_count += 1

        return self.config_manager.config.file_management.base_path, saved_count

    def _parse_task(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """解析任务为公告列表"""
        # 实现任务解析逻辑
        return []

    def migrate_all_configs(self, output_dir: str) -> None:
        """
        迁移所有配置到新格式

        Args:
            output_dir: 输出目录
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 保存不同的配置模板
        profiles = ['default', 'minimal', 'performance', 'archival', 'monitoring']

        for profile in profiles:
            config = self.config_manager.get_profile(profile)
            config_manager = ConfigManager()
            config_manager.config = config
            config_manager.save(str(output_path / f"config_{profile}.yaml"))

        logger.info(f"所有配置模板已保存到: {output_dir}")