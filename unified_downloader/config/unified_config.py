"""
统一配置系统
提供配置接口、验证、默认值管理和配置模板支持
"""

import os
import yaml
import json
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ConfigFormat(Enum):
    """配置格式枚举"""
    YAML = "yaml"
    JSON = "json"
    DICT = "dict"


class FileNamingStrategy(Enum):
    """文件命名策略枚举"""
    STANDARD = "standard"  # 日期_股票代码_公司名称_标题
    ORIGINAL = "original"  # 保留原始文件名
    COMPACT = "compact"    # 股票代码_日期_序号
    CUSTOM = "custom"      # 自定义格式


class DirectoryStructure(Enum):
    """目录结构模式枚举"""
    FLAT = "flat"              # 平铺模式：所有文件在同一目录
    BY_STOCK = "by_stock"      # 按股票分组：/股票代码/文件
    BY_DATE = "by_date"        # 按日期分组：/年/月/日/文件
    BY_CATEGORY = "by_category" # 按分类分组：/分类/文件
    HIERARCHICAL = "hierarchical" # 层级模式：/股票代码/分类/年月/文件


@dataclass
class FileManagementConfig:
    """文件管理配置"""
    # 文件命名配置
    naming_strategy: FileNamingStrategy = FileNamingStrategy.STANDARD
    naming_template: str = "{date}_{stock_code}_{company_name}_{title}"
    max_filename_length: int = 220
    sanitize_filename: bool = True

    # 目录结构配置
    directory_structure: DirectoryStructure = DirectoryStructure.HIERARCHICAL
    base_path: str = "./hkexann"
    create_date_subdirs: bool = True
    create_category_subdirs: bool = True

    # 符号链接配置
    enable_symlinks: bool = False
    symlink_strategies: List[str] = field(default_factory=lambda: ["by_date", "by_category"])

    # 文件组织配置
    group_by_announcement_type: bool = True
    preserve_original_structure: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['naming_strategy'] = self.naming_strategy.value
        data['directory_structure'] = self.directory_structure.value
        return data


@dataclass
class DownloaderConfig:
    """下载器配置"""
    # 基本配置
    max_concurrent: int = 5
    requests_per_second: int = 5
    timeout: int = 30
    max_retries: int = 3

    # 延迟配置
    min_delay: float = 0.88
    max_delay: float = 2.68
    backoff_on_429: int = 60

    # 休息配置
    enable_rest: bool = True
    work_minutes: int = 30
    rest_minutes: int = 5

    # 进度配置
    enable_progress_bar: bool = True
    verbose_logging: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


@dataclass
class FilterConfig:
    """过滤器配置"""
    # 股票过滤
    enable_stock_filter: bool = True
    stock_codes: List[str] = field(default_factory=list)

    # 类型过滤
    enable_type_filter: bool = True
    included_categories: List[str] = field(default_factory=list)
    excluded_categories: List[str] = field(default_factory=lambda: [
        "翌日披露報表", "展示文件", "月報表"
    ])

    # 关键词过滤
    enable_keyword_filter: bool = True
    included_keywords: List[str] = field(default_factory=list)
    excluded_keywords: List[str] = field(default_factory=list)

    # 智能分类
    enable_smart_classification: bool = True
    classification_rules: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


@dataclass
class UnifiedConfig:
    """统一配置主类"""
    # 子配置
    file_management: FileManagementConfig = field(default_factory=FileManagementConfig)
    downloader: DownloaderConfig = field(default_factory=DownloaderConfig)
    filter: FilterConfig = field(default_factory=FilterConfig)

    # API配置
    api_base_url: str = "https://www1.hkexnews.hk"
    api_endpoints: Dict[str, str] = field(default_factory=lambda: {
        "stock_search": "/search/prefix.do",
        "title_search": "/search/titleSearchServlet.do",
        "referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh"
    })

    # 数据库配置
    database_config: Dict[str, Any] = field(default_factory=dict)
    clickhouse_config: Dict[str, Any] = field(default_factory=dict)

    # 元数据
    version: str = "1.0.0"
    profile: str = "default"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "version": self.version,
            "profile": self.profile,
            "file_management": self.file_management.to_dict(),
            "downloader": self.downloader.to_dict(),
            "filter": self.filter.to_dict(),
            "api": {
                "base_url": self.api_base_url,
                "endpoints": self.api_endpoints
            },
            "database": self.database_config,
            "clickhouse": self.clickhouse_config
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UnifiedConfig':
        """从字典创建配置"""
        config = cls()

        # 解析文件管理配置
        if 'file_management' in data:
            fm_data = data['file_management']
            if 'naming_strategy' in fm_data:
                fm_data['naming_strategy'] = FileNamingStrategy(fm_data['naming_strategy'])
            if 'directory_structure' in fm_data:
                fm_data['directory_structure'] = DirectoryStructure(fm_data['directory_structure'])
            config.file_management = FileManagementConfig(**fm_data)

        # 解析下载器配置
        if 'downloader' in data:
            config.downloader = DownloaderConfig(**data['downloader'])

        # 解析过滤器配置
        if 'filter' in data:
            config.filter = FilterConfig(**data['filter'])

        # 解析API配置
        if 'api' in data:
            config.api_base_url = data['api'].get('base_url', config.api_base_url)
            config.api_endpoints.update(data['api'].get('endpoints', {}))

        # 解析数据库配置
        config.database_config = data.get('database', {})
        config.clickhouse_config = data.get('clickhouse', {})

        # 解析元数据
        config.version = data.get('version', config.version)
        config.profile = data.get('profile', config.profile)

        return config


class ConfigValidator:
    """配置验证器"""

    @staticmethod
    def validate(config: UnifiedConfig) -> List[str]:
        """
        验证配置的有效性

        Returns:
            错误消息列表，如果为空则配置有效
        """
        errors = []

        # 验证文件管理配置
        if config.file_management.max_filename_length < 50:
            errors.append("文件名最大长度不能小于50")

        if not config.file_management.base_path:
            errors.append("必须指定基础保存路径")

        # 验证下载器配置
        if config.downloader.max_concurrent < 1:
            errors.append("最大并发数必须大于0")

        if config.downloader.timeout < 5:
            errors.append("超时时间不能小于5秒")

        # 验证过滤器配置
        if config.filter.enable_stock_filter and not config.filter.stock_codes:
            errors.append("启用股票过滤但未指定股票代码")

        # 验证API配置
        if not config.api_base_url:
            errors.append("必须指定API基础URL")

        return errors


class ConfigManager:
    """配置管理器 - 统一配置接口"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = UnifiedConfig()
        self.legacy_mode = False  # 是否为兼容模式

        if config_path:
            self.load(config_path)

    def load(self, path: str, format: Optional[ConfigFormat] = None) -> None:
        """
        加载配置文件

        Args:
            path: 配置文件路径
            format: 配置格式，如果不指定则自动检测
        """
        path_obj = Path(path)

        if not path_obj.exists():
            raise FileNotFoundError(f"配置文件不存在: {path}")

        # 自动检测格式
        if format is None:
            if path_obj.suffix in ['.yaml', '.yml']:
                format = ConfigFormat.YAML
            elif path_obj.suffix == '.json':
                format = ConfigFormat.JSON
            else:
                raise ValueError(f"无法识别的配置文件格式: {path_obj.suffix}")

        # 加载配置
        with open(path, 'r', encoding='utf-8') as f:
            if format == ConfigFormat.YAML:
                data = yaml.safe_load(f)
            elif format == ConfigFormat.JSON:
                data = json.load(f)
            else:
                raise ValueError(f"不支持的配置格式: {format}")

        # 检测是否为旧版配置
        if self._is_legacy_config(data):
            logger.info("检测到旧版配置格式，启用兼容模式")
            self.legacy_mode = True
            self.config = self._convert_legacy_config(data)
        else:
            self.config = UnifiedConfig.from_dict(data)

        # 验证配置
        errors = ConfigValidator.validate(self.config)
        if errors:
            logger.warning(f"配置验证警告: {errors}")

    def save(self, path: Optional[str] = None, format: ConfigFormat = ConfigFormat.YAML) -> None:
        """
        保存配置到文件

        Args:
            path: 保存路径，如果不指定则使用原路径
            format: 保存格式
        """
        save_path = path or self.config_path
        if not save_path:
            raise ValueError("必须指定保存路径")

        data = self.config.to_dict()

        with open(save_path, 'w', encoding='utf-8') as f:
            if format == ConfigFormat.YAML:
                yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
            elif format == ConfigFormat.JSON:
                json.dump(data, f, ensure_ascii=False, indent=2)

    def _is_legacy_config(self, data: Dict[str, Any]) -> bool:
        """检测是否为旧版配置格式"""
        # 旧版配置的特征
        legacy_keys = ['settings', 'download_tasks', 'common_keywords', 'announcement_categories']
        return any(key in data for key in legacy_keys)

    def _convert_legacy_config(self, legacy_data: Dict[str, Any]) -> UnifiedConfig:
        """
        将旧版配置转换为新版统一配置

        Args:
            legacy_data: 旧版配置数据

        Returns:
            统一配置对象
        """
        config = UnifiedConfig()

        # 转换基本设置
        if 'settings' in legacy_data:
            settings = legacy_data['settings']
            config.file_management.base_path = settings.get('save_path', './hkexann')
            config.file_management.max_filename_length = settings.get('filename_length', 220)
            config.downloader.verbose_logging = settings.get('verbose_logging', True)

        # 转换异步下载配置
        if 'async' in legacy_data:
            async_config = legacy_data['async']
            config.downloader.max_concurrent = async_config.get('max_concurrent', 5)
            config.downloader.requests_per_second = async_config.get('requests_per_second', 5)
            config.downloader.timeout = async_config.get('timeout', 30)
            config.downloader.min_delay = async_config.get('min_delay', 0.88)
            config.downloader.max_delay = async_config.get('max_delay', 2.68)
            config.downloader.backoff_on_429 = async_config.get('backoff_on_429', 60)

            # 休息配置
            if 'rest' in async_config:
                rest = async_config['rest']
                config.downloader.enable_rest = rest.get('enabled', True)
                config.downloader.work_minutes = rest.get('work_minutes', 30)
                config.downloader.rest_minutes = rest.get('rest_minutes', 5)

        # 转换API配置
        if 'api_endpoints' in legacy_data:
            endpoints = legacy_data['api_endpoints']
            config.api_base_url = endpoints.get('base_url', config.api_base_url)
            config.api_endpoints.update({
                k: v for k, v in endpoints.items()
                if k != 'base_url'
            })

        # 转换过滤配置
        if 'announcement_categories' in legacy_data:
            categories = legacy_data['announcement_categories']
            if 'excluded_categories' in categories:
                config.filter.excluded_categories = categories['excluded_categories']

        if 'dual_filter' in legacy_data:
            dual_filter = legacy_data['dual_filter']
            config.filter.enable_stock_filter = dual_filter.get('stock_filter_enabled', True)
            config.filter.enable_type_filter = dual_filter.get('type_filter_enabled', True)
            config.filter.included_keywords = dual_filter.get('included_keywords', [])

        # 转换数据库配置
        config.database_config = legacy_data.get('database', {})

        # 转换ClickHouse配置
        if 'stock_discovery' in legacy_data:
            config.clickhouse_config = legacy_data['stock_discovery']

        # 转换分类规则
        if 'common_keywords' in legacy_data:
            config.filter.classification_rules = legacy_data['common_keywords']
            config.filter.enable_smart_classification = True

        return config

    def get_profile(self, profile_name: str) -> UnifiedConfig:
        """
        获取预定义的配置模板

        Args:
            profile_name: 模板名称

        Returns:
            配置对象
        """
        profiles = {
            'default': self._get_default_profile(),
            'minimal': self._get_minimal_profile(),
            'performance': self._get_performance_profile(),
            'archival': self._get_archival_profile(),
            'monitoring': self._get_monitoring_profile()
        }

        if profile_name not in profiles:
            raise ValueError(f"未知的配置模板: {profile_name}")

        return profiles[profile_name]

    def _get_default_profile(self) -> UnifiedConfig:
        """默认配置模板"""
        return UnifiedConfig()

    def _get_minimal_profile(self) -> UnifiedConfig:
        """最小配置模板 - 适合快速测试"""
        config = UnifiedConfig()
        config.profile = 'minimal'
        config.file_management.directory_structure = DirectoryStructure.FLAT
        config.file_management.create_category_subdirs = False
        config.downloader.max_concurrent = 1
        config.downloader.enable_progress_bar = False
        config.filter.enable_smart_classification = False
        return config

    def _get_performance_profile(self) -> UnifiedConfig:
        """性能优化配置模板"""
        config = UnifiedConfig()
        config.profile = 'performance'
        config.downloader.max_concurrent = 10
        config.downloader.requests_per_second = 10
        config.downloader.enable_rest = False
        config.downloader.enable_progress_bar = False
        config.file_management.directory_structure = DirectoryStructure.BY_STOCK
        return config

    def _get_archival_profile(self) -> UnifiedConfig:
        """归档配置模板 - 适合长期存储"""
        config = UnifiedConfig()
        config.profile = 'archival'
        config.file_management.directory_structure = DirectoryStructure.HIERARCHICAL
        config.file_management.create_date_subdirs = True
        config.file_management.create_category_subdirs = True
        config.file_management.enable_symlinks = True
        config.file_management.naming_strategy = FileNamingStrategy.STANDARD
        config.filter.enable_smart_classification = True
        return config

    def _get_monitoring_profile(self) -> UnifiedConfig:
        """监控配置模板 - 适合实时监控"""
        config = UnifiedConfig()
        config.profile = 'monitoring'
        config.downloader.max_concurrent = 3
        config.downloader.enable_rest = True
        config.downloader.work_minutes = 15
        config.downloader.rest_minutes = 5
        config.filter.enable_stock_filter = True
        config.filter.enable_type_filter = True
        config.filter.enable_smart_classification = True
        return config

    def merge(self, other: Union[Dict[str, Any], 'UnifiedConfig']) -> None:
        """
        合并配置

        Args:
            other: 要合并的配置（字典或配置对象）
        """
        if isinstance(other, dict):
            other = UnifiedConfig.from_dict(other)

        # 合并文件管理配置
        for key, value in asdict(other.file_management).items():
            if value is not None:
                setattr(self.config.file_management, key, value)

        # 合并下载器配置
        for key, value in asdict(other.downloader).items():
            if value is not None:
                setattr(self.config.downloader, key, value)

        # 合并过滤器配置
        for key, value in asdict(other.filter).items():
            if value is not None:
                setattr(self.config.filter, key, value)

        # 合并其他配置
        if other.api_base_url:
            self.config.api_base_url = other.api_base_url
        self.config.api_endpoints.update(other.api_endpoints)
        self.config.database_config.update(other.database_config)
        self.config.clickhouse_config.update(other.clickhouse_config)

    def validate(self) -> List[str]:
        """验证当前配置"""
        return ConfigValidator.validate(self.config)

    def export_legacy(self) -> Dict[str, Any]:
        """
        导出为旧版配置格式（向后兼容）

        Returns:
            旧版格式的配置字典
        """
        legacy = {
            'settings': {
                'save_path': self.config.file_management.base_path,
                'filename_length': self.config.file_management.max_filename_length,
                'language': 'zh',
                'max_results': 1000,
                'verbose_logging': self.config.downloader.verbose_logging,
                'log_file': 'hkex_downloader.log'
            },
            'async': {
                'enabled': True,
                'max_concurrent': self.config.downloader.max_concurrent,
                'requests_per_second': self.config.downloader.requests_per_second,
                'timeout': self.config.downloader.timeout,
                'min_delay': self.config.downloader.min_delay,
                'max_delay': self.config.downloader.max_delay,
                'backoff_on_429': self.config.downloader.backoff_on_429,
                'rest': {
                    'enabled': self.config.downloader.enable_rest,
                    'work_minutes': self.config.downloader.work_minutes,
                    'rest_minutes': self.config.downloader.rest_minutes
                }
            },
            'api_endpoints': {
                'base_url': self.config.api_base_url,
                **self.config.api_endpoints
            },
            'announcement_categories': {
                'enabled': self.config.filter.enable_type_filter,
                'excluded_categories': self.config.filter.excluded_categories
            },
            'dual_filter': {
                'stock_filter_enabled': self.config.filter.enable_stock_filter,
                'type_filter_enabled': self.config.filter.enable_type_filter,
                'included_keywords': self.config.filter.included_keywords
            },
            'database': self.config.database_config,
            'stock_discovery': self.config.clickhouse_config
        }

        # 添加分类规则
        if self.config.filter.classification_rules:
            legacy['common_keywords'] = self.config.filter.classification_rules

        return legacy


# 全局配置实例
_global_config: Optional[ConfigManager] = None


def get_config() -> ConfigManager:
    """获取全局配置实例"""
    global _global_config
    if _global_config is None:
        _global_config = ConfigManager()
    return _global_config


def init_config(config_path: Optional[str] = None) -> ConfigManager:
    """
    初始化全局配置

    Args:
        config_path: 配置文件路径

    Returns:
        配置管理器实例
    """
    global _global_config
    _global_config = ConfigManager(config_path)
    return _global_config