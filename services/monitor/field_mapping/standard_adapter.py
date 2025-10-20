"""
字段标准化适配器

这个模块为整个系统提供字段标准化适配功能，确保所有模块间的
数据交换使用统一的字段命名规范，解决file_path vs local_path等问题。

主要功能：
- 系统级字段标准化
- 模块间数据适配
- 向后兼容性保证
- 自动字段转换

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import logging
from typing import Dict, Any, Optional, List, Union, Callable, Type
from dataclasses import dataclass
import functools
import inspect
from pathlib import Path
import sys

# 设置路径
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

try:
    from services.monitor.field_mapping.unified_field_mapper import get_field_mapper
    FIELD_MAPPER_AVAILABLE = True
except ImportError:
    FIELD_MAPPER_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AdapterConfig:
    """适配器配置"""
    auto_fix_download_results: bool = True    # 自动修复下载结果
    preserve_original_fields: bool = True     # 保留原始字段
    strict_validation: bool = False           # 严格验证
    log_transformations: bool = False         # 记录转换过程


class FieldStandardAdapter:
    """
    字段标准化适配器
    
    提供系统级的字段标准化功能：
    1. 自动修复下载器返回结果
    2. 统一公告数据格式
    3. 向后兼容性保证
    4. 装饰器支持
    """
    
    def __init__(self, config: AdapterConfig = None):
        """初始化字段标准化适配器"""
        
        self.config = config or AdapterConfig()
        
        # 获取字段映射器
        if FIELD_MAPPER_AVAILABLE:
            self.field_mapper = get_field_mapper()
        else:
            self.field_mapper = None
            logger.warning("字段映射器不可用，将使用基础适配功能")
        
        # 适配统计
        self.adaptation_stats = {
            'total_adaptations': 0,
            'download_result_fixes': 0,
            'announcement_standardizations': 0,
            'field_mappings': 0,
            'errors': 0
        }
        
        logger.info("🔧 字段标准化适配器初始化完成")

    def adapt_download_result(self, download_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        适配下载器返回结果
        
        解决 file_path vs local_path 不一致问题
        
        Args:
            download_result: 原始下载结果
            
        Returns:
            Dict[str, Any]: 适配后的结果
        """
        self.adaptation_stats['total_adaptations'] += 1
        self.adaptation_stats['download_result_fixes'] += 1
        
        try:
            if self.field_mapper:
                # 使用字段映射器进行标准化
                adapted = self.field_mapper.fix_downloader_result(download_result)
            else:
                # 基础适配逻辑
                adapted = self._basic_download_result_fix(download_result)
            
            if self.config.log_transformations:
                logger.debug(f"下载结果适配: {download_result} -> {adapted}")
            
            return adapted
            
        except Exception as e:
            logger.error(f"下载结果适配失败: {e}")
            self.adaptation_stats['errors'] += 1
            
            # 返回原始结果加基础修复
            return self._basic_download_result_fix(download_result)

    def _basic_download_result_fix(self, download_result: Dict[str, Any]) -> Dict[str, Any]:
        """基础下载结果修复"""
        adapted = download_result.copy()
        
        # 关键修复：file_path <-> local_path 双向兼容
        if 'file_path' in adapted and 'local_path' not in adapted:
            adapted['local_path'] = adapted['file_path']
        elif 'local_path' in adapted and 'file_path' not in adapted:
            adapted['file_path'] = adapted['local_path']
        
        # 确保success字段为布尔值
        if 'success' in adapted:
            adapted['success'] = bool(adapted['success'])
        
        # 确保file_size为整数
        if 'file_size' in adapted:
            try:
                adapted['file_size'] = int(adapted['file_size'])
            except (ValueError, TypeError):
                adapted['file_size'] = 0
        
        return adapted

    def adapt_announcement_data(self, announcement: Dict[str, Any]) -> Dict[str, Any]:
        """
        适配公告数据
        
        Args:
            announcement: 原始公告数据
            
        Returns:
            Dict[str, Any]: 适配后的公告数据
        """
        self.adaptation_stats['total_adaptations'] += 1
        self.adaptation_stats['announcement_standardizations'] += 1
        
        try:
            if self.field_mapper:
                # 使用字段映射器进行标准化
                adapted = self.field_mapper.standardize_announcement_data(announcement)
            else:
                # 基础适配逻辑
                adapted = self._basic_announcement_fix(announcement)
            
            if self.config.log_transformations:
                logger.debug(f"公告数据适配: {announcement} -> {adapted}")
            
            return adapted
            
        except Exception as e:
            logger.error(f"公告数据适配失败: {e}")
            self.adaptation_stats['errors'] += 1
            
            # 返回原始数据加基础修复
            return self._basic_announcement_fix(announcement)

    def _basic_announcement_fix(self, announcement: Dict[str, Any]) -> Dict[str, Any]:
        """基础公告数据修复"""
        adapted = announcement.copy()
        
        # 标准化股票代码
        if 'STOCK_CODE' in adapted:
            stock_code = str(adapted['STOCK_CODE']).strip().lstrip('0')
            if stock_code.endswith('.HK'):
                stock_code = stock_code[:-3]
            adapted['stock_code'] = stock_code.zfill(5) if stock_code.isdigit() else stock_code
        
        # 标准化标题
        if 'TITLE' in adapted:
            adapted['title'] = str(adapted['TITLE']).strip()
        
        # 标准化ID
        if 'ID' in adapted:
            adapted['announcement_id'] = adapted['ID']
        
        return adapted

    def map_fields(self, data: Dict[str, Any], strict: bool = None) -> Dict[str, Any]:
        """
        映射字段到标准格式
        
        Args:
            data: 原始数据
            strict: 是否严格模式
            
        Returns:
            Dict[str, Any]: 映射后的数据
        """
        self.adaptation_stats['total_adaptations'] += 1
        self.adaptation_stats['field_mappings'] += 1
        
        if strict is None:
            strict = self.config.strict_validation
        
        try:
            if self.field_mapper:
                mapping_result = self.field_mapper.map_fields(data, strict_mode=strict)
                
                if mapping_result.success:
                    return mapping_result.mapped_data
                else:
                    logger.warning(f"字段映射部分失败: {mapping_result.errors}")
                    return mapping_result.mapped_data
            else:
                # 基础映射
                return self._basic_field_mapping(data)
                
        except Exception as e:
            logger.error(f"字段映射失败: {e}")
            self.adaptation_stats['errors'] += 1
            return data

    def _basic_field_mapping(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """基础字段映射"""
        mapped = {}
        
        # 基础映射规则
        basic_mappings = {
            'file_path': ['local_path', 'filepath', 'file_location'],
            'file_name': ['filename', 'name'],
            'file_size': ['size', 'filesize'],
            'stock_code': ['STOCK_CODE', 'stockcode', 'symbol'],
            'title': ['TITLE', 'announcement_title'],
            'announcement_id': ['ID', 'id', 'doc_id'],
            'file_link': ['FILE_LINK', 'url', 'download_url'],
            'publish_date': ['DATE_TIME', 'date', 'publish_time'],
            'success': ['status', 'is_success', 'result']
        }
        
        # 反向映射
        alias_to_standard = {}
        for standard, aliases in basic_mappings.items():
            alias_to_standard[standard] = standard
            for alias in aliases:
                alias_to_standard[alias] = standard
        
        # 执行映射
        for key, value in data.items():
            standard_key = alias_to_standard.get(key, key)
            mapped[standard_key] = value
        
        return mapped

    def get_adaptation_statistics(self) -> Dict[str, Any]:
        """
        获取适配统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        total = self.adaptation_stats['total_adaptations']
        error_rate = (
            self.adaptation_stats['errors'] / total * 100
            if total > 0 else 0
        )
        
        return {
            'total_adaptations': total,
            'download_result_fixes': self.adaptation_stats['download_result_fixes'],
            'announcement_standardizations': self.adaptation_stats['announcement_standardizations'],
            'field_mappings': self.adaptation_stats['field_mappings'],
            'errors': self.adaptation_stats['errors'],
            'error_rate': round(error_rate, 2),
            'field_mapper_available': FIELD_MAPPER_AVAILABLE
        }

    # 装饰器支持
    def download_result_adapter(self, func: Callable) -> Callable:
        """
        下载结果适配装饰器
        
        自动适配函数返回的下载结果
        """
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            if isinstance(result, dict):
                return self.adapt_download_result(result)
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, dict):
                return self.adapt_download_result(result)
            return result
        
        # 检查函数是否为协程
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    def announcement_adapter(self, func: Callable) -> Callable:
        """
        公告数据适配装饰器
        
        自动适配函数的公告数据参数和返回值
        """
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # 适配参数中的公告数据
            adapted_kwargs = kwargs.copy()
            for key, value in kwargs.items():
                if key in ['announcement', 'announcement_data'] and isinstance(value, dict):
                    adapted_kwargs[key] = self.adapt_announcement_data(value)
            
            result = await func(*args, **adapted_kwargs)
            
            # 适配返回结果
            if isinstance(result, dict) and 'announcement' in result:
                result['announcement'] = self.adapt_announcement_data(result['announcement'])
            
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # 适配参数中的公告数据
            adapted_kwargs = kwargs.copy()
            for key, value in kwargs.items():
                if key in ['announcement', 'announcement_data'] and isinstance(value, dict):
                    adapted_kwargs[key] = self.adapt_announcement_data(value)
            
            result = func(*args, **adapted_kwargs)
            
            # 适配返回结果
            if isinstance(result, dict) and 'announcement' in result:
                result['announcement'] = self.adapt_announcement_data(result['announcement'])
            
            return result
        
        # 检查函数是否为协程
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper


# 全局适配器实例
_global_adapter: Optional[FieldStandardAdapter] = None


def get_field_adapter() -> FieldStandardAdapter:
    """
    获取全局字段适配器实例
    
    Returns:
        FieldStandardAdapter: 适配器实例
    """
    global _global_adapter
    if _global_adapter is None:
        _global_adapter = FieldStandardAdapter()
    return _global_adapter


# 便捷装饰器
def fix_download_result(func: Callable) -> Callable:
    """
    修复下载结果装饰器
    
    Usage:
        @fix_download_result
        async def download_announcement(self, announcement):
            return await self.downloader.download(announcement)
    """
    return get_field_adapter().download_result_adapter(func)


def standardize_announcement(func: Callable) -> Callable:
    """
    标准化公告数据装饰器
    
    Usage:
        @standardize_announcement
        async def process_announcement(self, announcement):
            return await self.processor.process(announcement)
    """
    return get_field_adapter().announcement_adapter(func)


# 便捷函数
def adapt_download_result(download_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    适配下载结果的便捷函数
    
    Args:
        download_result: 下载结果
        
    Returns:
        Dict[str, Any]: 适配后的结果
    """
    return get_field_adapter().adapt_download_result(download_result)


def adapt_announcement_data(announcement: Dict[str, Any]) -> Dict[str, Any]:
    """
    适配公告数据的便捷函数
    
    Args:
        announcement: 公告数据
        
    Returns:
        Dict[str, Any]: 适配后的数据
    """
    return get_field_adapter().adapt_announcement_data(announcement)


if __name__ == "__main__":
    # 测试模块
    def test_field_standard_adapter():
        """测试字段标准化适配器"""
        
        print("\n" + "="*70)
        print("🔧 字段标准化适配器测试")
        print("="*70)
        
        # 创建适配器
        config = AdapterConfig(
            auto_fix_download_results=True,
            preserve_original_fields=True,
            log_transformations=True
        )
        
        adapter = FieldStandardAdapter(config)
        
        # 测试下载结果适配
        print("\n📥 测试下载结果适配...")
        
        # 模拟不同来源的下载结果
        download_results = [
            # RealtimeDownloaderWrapper格式
            {
                'success': True,
                'file_path': '/path/to/file.pdf',
                'filename': 'announcement.pdf',
                'file_size': '1024'
            },
            # 历史处理器期望格式
            {
                'success': 1,
                'local_path': '/other/path/file.pdf',
                'size': 2048
            },
            # 混合格式
            {
                'status': 'ok',
                'filepath': '/mixed/format.pdf',
                'filesize': '512'
            }
        ]
        
        for i, result in enumerate(download_results):
            adapted = adapter.adapt_download_result(result)
            print(f"  结果{i+1}:")
            print(f"    原始: {result}")
            print(f"    适配: {adapted}")
            print(f"    兼容: file_path={'file_path' in adapted}, local_path={'local_path' in adapted}")
        
        # 测试公告数据适配
        print(f"\n📰 测试公告数据适配...")
        
        announcements = [
            # HKEX API格式
            {
                'STOCK_CODE': '00700',
                'TITLE': '  腾讯控股 - 重要公告  ',
                'DATE_TIME': '2025-09-10 18:00:00',
                'FILE_LINK': 'https://example.com/ann.pdf',
                'ID': 'ANN_001'
            },
            # 其他格式
            {
                'stockcode': '0700',
                'announcement_title': '简化标题',
                'publish_time': '2025-09-10',
                'url': 'https://test.com/doc.pdf',
                'doc_id': 'DOC_001'
            }
        ]
        
        for i, announcement in enumerate(announcements):
            adapted = adapter.adapt_announcement_data(announcement)
            print(f"  公告{i+1}:")
            print(f"    原始: {announcement}")
            print(f"    适配: {adapted}")
        
        # 测试装饰器
        print(f"\n🎭 测试装饰器功能...")
        
        @adapter.download_result_adapter
        def mock_download_function():
            return {
                'success': True,
                'local_path': '/decorator/test.pdf',
                'size': 1536
            }
        
        @adapter.announcement_adapter
        def mock_process_function(announcement):
            return {
                'processed': True,
                'announcement': announcement
            }
        
        # 测试装饰器
        decorated_result = mock_download_function()
        print(f"  装饰器下载结果: {decorated_result}")
        
        test_announcement = {'STOCK_CODE': '00001', 'TITLE': 'Test'}
        decorated_announcement = mock_process_function(announcement=test_announcement)
        print(f"  装饰器公告处理: {decorated_announcement}")
        
        # 显示统计信息
        print(f"\n📊 适配统计信息:")
        stats = adapter.get_adaptation_statistics()
        
        print(f"  总适配次数: {stats['total_adaptations']}")
        print(f"  下载结果修复: {stats['download_result_fixes']}")
        print(f"  公告标准化: {stats['announcement_standardizations']}")
        print(f"  字段映射: {stats['field_mappings']}")
        print(f"  错误率: {stats['error_rate']}%")
        print(f"  字段映射器可用: {stats['field_mapper_available']}")
        
        # 测试便捷函数
        print(f"\n🛠️ 测试便捷函数...")
        
        test_result = {'success': True, 'local_path': '/convenience/test.pdf'}
        adapted_result = adapt_download_result(test_result)
        print(f"  便捷函数结果: {adapted_result}")
        
        print("\n" + "="*70)
    
    # 运行测试
    test_field_standard_adapter()
