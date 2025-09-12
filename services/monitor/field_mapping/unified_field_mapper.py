"""
统一字段映射器

这个模块解决了系统中字段命名不一致的问题，特别是下载器集成中的
file_path vs local_path混用问题。提供统一的字段映射和转换机制。

主要功能：
- 统一字段命名规范
- 字段映射和转换
- 下载器集成修复
- 数据格式标准化

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import logging
from typing import Dict, Any, Optional, List, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import re
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FieldCategory(Enum):
    """字段分类枚举"""
    FILE_SYSTEM = "file_system"           # 文件系统相关字段
    ANNOUNCEMENT = "announcement"         # 公告相关字段
    DOWNLOAD = "download"                # 下载相关字段
    VECTOR = "vector"                    # 向量化相关字段
    METADATA = "metadata"                # 元数据字段
    TIMESTAMP = "timestamp"              # 时间戳字段
    IDENTIFIER = "identifier"            # 标识符字段
    STATUS = "status"                    # 状态字段


@dataclass
class FieldMapping:
    """字段映射规则"""
    standard_name: str                   # 标准字段名
    aliases: List[str]                   # 别名列表
    category: FieldCategory              # 字段分类
    data_type: str                       # 数据类型
    description: str                     # 字段描述
    transformer: Optional[Callable] = None  # 转换函数
    validator: Optional[Callable] = None    # 验证函数
    required: bool = False               # 是否必需
    default_value: Any = None            # 默认值


@dataclass
class MappingResult:
    """映射结果"""
    success: bool
    mapped_data: Dict[str, Any] = field(default_factory=dict)
    unmapped_fields: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class UnifiedFieldMapper:
    """
    统一字段映射器
    
    解决系统中字段命名不一致的问题：
    1. file_path vs local_path 混用
    2. 下载器返回字段不统一
    3. 不同模块间字段名不匹配
    4. 数据格式不标准化
    """
    
    def __init__(self):
        """初始化统一字段映射器"""
        
        # 字段映射规则
        self.field_mappings = self._initialize_field_mappings()
        
        # 反向映射（从别名到标准名）
        self.alias_to_standard = self._build_alias_mapping()
        
        # 字段验证器
        self.validators = self._initialize_validators()
        
        # 字段转换器
        self.transformers = self._initialize_transformers()
        
        # 统计信息
        self.mapping_stats = {
            'total_mappings': 0,
            'successful_mappings': 0,
            'failed_mappings': 0,
            'field_usage': {}
        }
        
        logger.info("🗂️ 统一字段映射器初始化完成")
        self._log_mapping_summary()

    def _initialize_field_mappings(self) -> Dict[str, FieldMapping]:
        """初始化字段映射规则"""
        return {
            # 文件系统相关字段
            'file_path': FieldMapping(
                standard_name='file_path',
                aliases=['local_path', 'filepath', 'file_location', 'path'],
                category=FieldCategory.FILE_SYSTEM,
                data_type='str',
                description='文件完整路径',
                transformer=self._normalize_file_path,
                validator=self._validate_file_path,
                required=True
            ),
            
            'file_name': FieldMapping(
                standard_name='file_name',
                aliases=['filename', 'name', 'file_title'],
                category=FieldCategory.FILE_SYSTEM,
                data_type='str',
                description='文件名（不含路径）',
                transformer=self._normalize_file_name,
                required=True
            ),
            
            'file_size': FieldMapping(
                standard_name='file_size',
                aliases=['size', 'filesize', 'content_length'],
                category=FieldCategory.FILE_SYSTEM,
                data_type='int',
                description='文件大小（字节）',
                transformer=self._normalize_file_size,
                validator=self._validate_file_size,
                default_value=0
            ),
            
            # 公告相关字段
            'stock_code': FieldMapping(
                standard_name='stock_code',
                aliases=['STOCK_CODE', 'stockcode', 'stock_id', 'symbol'],
                category=FieldCategory.ANNOUNCEMENT,
                data_type='str',
                description='股票代码',
                transformer=self._normalize_stock_code,
                validator=self._validate_stock_code,
                required=True
            ),
            
            'announcement_id': FieldMapping(
                standard_name='announcement_id',
                aliases=['ID', 'id', 'announcement_id', 'doc_id'],
                category=FieldCategory.IDENTIFIER,
                data_type='str',
                description='公告唯一标识',
                required=True
            ),
            
            'title': FieldMapping(
                standard_name='title',
                aliases=['TITLE', 'announcement_title', 'document_title'],
                category=FieldCategory.ANNOUNCEMENT,
                data_type='str',
                description='公告标题',
                transformer=self._normalize_title,
                required=True
            ),
            
            'file_link': FieldMapping(
                standard_name='file_link',
                aliases=['FILE_LINK', 'url', 'download_url', 'pdf_url'],
                category=FieldCategory.DOWNLOAD,
                data_type='str',
                description='文件下载链接',
                validator=self._validate_url,
                required=True
            ),
            
            # 时间相关字段
            'publish_date': FieldMapping(
                standard_name='publish_date',
                aliases=['DATE_TIME', 'date', 'publish_time', 'created_at'],
                category=FieldCategory.TIMESTAMP,
                data_type='str',
                description='发布日期时间',
                transformer=self._normalize_datetime,
                required=True
            ),
            
            'download_time': FieldMapping(
                standard_name='download_time',
                aliases=['processing_time', 'elapsed_time'],
                category=FieldCategory.TIMESTAMP,
                data_type='float',
                description='下载耗时（秒）',
                default_value=0.0
            ),
            
            # 下载结果字段
            'success': FieldMapping(
                standard_name='success',
                aliases=['status', 'is_success', 'result'],
                category=FieldCategory.STATUS,
                data_type='bool',
                description='操作是否成功',
                transformer=self._normalize_boolean,
                default_value=False
            ),
            
            'error_message': FieldMapping(
                standard_name='error_message',
                aliases=['error', 'error_msg', 'failure_reason'],
                category=FieldCategory.STATUS,
                data_type='str',
                description='错误信息',
                default_value=""
            ),
            
            # 向量化相关字段
            'chunk_count': FieldMapping(
                standard_name='chunk_count',
                aliases=['chunks', 'total_chunks', 'segment_count'],
                category=FieldCategory.VECTOR,
                data_type='int',
                description='文档块数量',
                default_value=0
            ),
            
            'vector_count': FieldMapping(
                standard_name='vector_count',
                aliases=['vectors', 'embeddings_count'],
                category=FieldCategory.VECTOR,
                data_type='int',
                description='向量数量',
                default_value=0
            ),
            
            # 元数据字段
            'document_type': FieldMapping(
                standard_name='document_type',
                aliases=['doc_type', 'type', 'category'],
                category=FieldCategory.METADATA,
                data_type='str',
                description='文档类型',
                default_value="announcement"
            ),
            
            'company_name': FieldMapping(
                standard_name='company_name',
                aliases=['company', 'corp_name', 'issuer'],
                category=FieldCategory.METADATA,
                data_type='str',
                description='公司名称',
                default_value=""
            )
        }

    def _build_alias_mapping(self) -> Dict[str, str]:
        """构建别名到标准名的映射"""
        alias_mapping = {}
        
        for standard_name, mapping in self.field_mappings.items():
            # 标准名映射到自己
            alias_mapping[standard_name] = standard_name
            
            # 别名映射到标准名
            for alias in mapping.aliases:
                alias_mapping[alias] = standard_name
        
        return alias_mapping

    def _initialize_validators(self) -> Dict[str, Callable]:
        """初始化字段验证器"""
        return {
            'file_path': self._validate_file_path,
            'file_size': self._validate_file_size,
            'stock_code': self._validate_stock_code,
            'file_link': self._validate_url
        }

    def _initialize_transformers(self) -> Dict[str, Callable]:
        """初始化字段转换器"""
        return {
            'file_path': self._normalize_file_path,
            'file_name': self._normalize_file_name,
            'file_size': self._normalize_file_size,
            'stock_code': self._normalize_stock_code,
            'title': self._normalize_title,
            'publish_date': self._normalize_datetime,
            'success': self._normalize_boolean
        }

    def map_fields(self, data: Dict[str, Any], 
                  strict_mode: bool = False) -> MappingResult:
        """
        映射字段到统一格式
        
        Args:
            data: 原始数据字典
            strict_mode: 是否严格模式（严格模式下必需字段缺失会失败）
            
        Returns:
            MappingResult: 映射结果
        """
        result = MappingResult(success=True)
        
        # 更新统计
        self.mapping_stats['total_mappings'] += 1
        
        try:
            # 遍历原始数据
            for original_key, value in data.items():
                # 查找标准字段名
                standard_key = self.alias_to_standard.get(original_key)
                
                if standard_key:
                    # 获取映射规则
                    mapping = self.field_mappings[standard_key]
                    
                    # 转换值
                    try:
                        if mapping.transformer and value is not None:
                            transformed_value = mapping.transformer(value)
                        else:
                            transformed_value = value
                        
                        # 验证值
                        if mapping.validator and transformed_value is not None:
                            if not mapping.validator(transformed_value):
                                result.errors.append(f"字段 {original_key} 验证失败: {value}")
                                continue
                        
                        # 存储映射结果
                        result.mapped_data[standard_key] = transformed_value
                        
                        # 更新使用统计
                        if standard_key not in self.mapping_stats['field_usage']:
                            self.mapping_stats['field_usage'][standard_key] = 0
                        self.mapping_stats['field_usage'][standard_key] += 1
                        
                    except Exception as e:
                        result.errors.append(f"转换字段 {original_key} 时出错: {e}")
                        result.success = False
                else:
                    # 未映射的字段
                    result.unmapped_fields.append(original_key)
                    result.warnings.append(f"未知字段: {original_key}")
            
            # 检查必需字段
            for standard_name, mapping in self.field_mappings.items():
                if mapping.required and standard_name not in result.mapped_data:
                    if strict_mode:
                        result.errors.append(f"缺少必需字段: {standard_name}")
                        result.success = False
                    else:
                        if mapping.default_value is not None:
                            result.mapped_data[standard_name] = mapping.default_value
                            result.warnings.append(f"使用默认值: {standard_name} = {mapping.default_value}")
            
            # 更新成功统计
            if result.success:
                self.mapping_stats['successful_mappings'] += 1
            else:
                self.mapping_stats['failed_mappings'] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"字段映射异常: {e}")
            result.success = False
            result.errors.append(f"映射异常: {e}")
            self.mapping_stats['failed_mappings'] += 1
            return result

    def fix_downloader_result(self, download_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        修复下载器返回结果的字段不一致问题
        
        特别解决 file_path vs local_path 的问题
        
        Args:
            download_result: 下载器返回的原始结果
            
        Returns:
            Dict[str, Any]: 修复后的标准格式结果
        """
        # 映射字段
        mapping_result = self.map_fields(download_result, strict_mode=False)
        
        if not mapping_result.success:
            logger.warning(f"下载结果字段映射部分失败: {mapping_result.errors}")
        
        # 确保关键字段存在
        fixed_result = mapping_result.mapped_data.copy()
        
        # 特殊处理：确保file_path字段存在（向后兼容）
        if 'file_path' in fixed_result:
            # 同时提供local_path别名以兼容旧代码
            fixed_result['local_path'] = fixed_result['file_path']
        
        # 确保success字段为布尔值
        if 'success' not in fixed_result:
            fixed_result['success'] = False
        
        return fixed_result

    def standardize_announcement_data(self, announcement: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化公告数据格式
        
        Args:
            announcement: 原始公告数据
            
        Returns:
            Dict[str, Any]: 标准化后的公告数据
        """
        mapping_result = self.map_fields(announcement, strict_mode=False)
        
        standardized = mapping_result.mapped_data.copy()
        
        # 特殊处理：保留原始字段名以兼容现有代码
        for original_key in ['STOCK_CODE', 'TITLE', 'DATE_TIME', 'FILE_LINK', 'ID']:
            if original_key in announcement:
                standardized[original_key] = announcement[original_key]
        
        return standardized

    # 验证器方法
    def _validate_file_path(self, value: str) -> bool:
        """验证文件路径"""
        if not isinstance(value, str) or not value.strip():
            return False
        
        # 检查路径格式
        try:
            path = Path(value)
            return len(path.parts) > 0
        except Exception:
            return False

    def _validate_file_size(self, value: Union[int, str]) -> bool:
        """验证文件大小"""
        try:
            size = int(value)
            return size >= 0
        except (ValueError, TypeError):
            return False

    def _validate_stock_code(self, value: str) -> bool:
        """验证股票代码"""
        if not isinstance(value, str):
            return False
        
        # 清理股票代码
        clean_code = value.strip().lstrip('0')
        
        # 检查格式：4-5位数字或数字.HK格式
        return bool(re.match(r'^\d{1,5}(\.HK)?$', clean_code))

    def _validate_url(self, value: str) -> bool:
        """验证URL格式"""
        if not isinstance(value, str):
            return False
        
        url_pattern = re.compile(
            r'^https?://'  # http或https
            r'[a-zA-Z0-9.-]+'  # 域名
            r'[/\w\-._~:/?#[\]@!$&\'()*+,;=]*$'  # 路径和参数
        )
        
        return bool(url_pattern.match(value))

    # 转换器方法
    def _normalize_file_path(self, value: str) -> str:
        """标准化文件路径"""
        if not isinstance(value, str):
            return str(value)
        
        # 标准化路径分隔符
        normalized = str(Path(value))
        
        # 移除重复的分隔符
        while '//' in normalized:
            normalized = normalized.replace('//', '/')
        
        return normalized

    def _normalize_file_name(self, value: str) -> str:
        """标准化文件名"""
        if not isinstance(value, str):
            return str(value)
        
        # 从路径中提取文件名
        return Path(value).name

    def _normalize_file_size(self, value: Union[int, str]) -> int:
        """标准化文件大小"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def _normalize_stock_code(self, value: str) -> str:
        """标准化股票代码"""
        if not isinstance(value, str):
            return str(value)
        
        # 移除空格和前导0
        clean_code = value.strip().lstrip('0')
        
        # 处理.HK后缀
        if clean_code.endswith('.HK'):
            clean_code = clean_code[:-3]
        
        # 补充前导0到5位
        if clean_code.isdigit() and len(clean_code) <= 5:
            clean_code = clean_code.zfill(5)
        
        return clean_code

    def _normalize_title(self, value: str) -> str:
        """标准化标题"""
        if not isinstance(value, str):
            return str(value)
        
        # 清理标题
        cleaned = value.strip()
        
        # 移除多余空格
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned

    def _normalize_datetime(self, value: str) -> str:
        """标准化日期时间"""
        if not isinstance(value, str):
            return str(value)
        
        # 简单清理，保持原格式
        return value.strip()

    def _normalize_boolean(self, value: Any) -> bool:
        """标准化布尔值"""
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1', 'success', 'ok')
        
        if isinstance(value, (int, float)):
            return value != 0
        
        return bool(value)

    def get_field_schema(self) -> Dict[str, Any]:
        """
        获取字段schema信息
        
        Returns:
            Dict[str, Any]: 字段schema
        """
        schema = {}
        
        for standard_name, mapping in self.field_mappings.items():
            schema[standard_name] = {
                'aliases': mapping.aliases,
                'category': mapping.category.value,
                'data_type': mapping.data_type,
                'description': mapping.description,
                'required': mapping.required,
                'default_value': mapping.default_value
            }
        
        return schema

    def get_mapping_statistics(self) -> Dict[str, Any]:
        """
        获取映射统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        total = self.mapping_stats['total_mappings']
        success_rate = (
            self.mapping_stats['successful_mappings'] / total * 100
            if total > 0 else 0
        )
        
        # 最常用字段
        sorted_usage = sorted(
            self.mapping_stats['field_usage'].items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            'total_mappings': total,
            'successful_mappings': self.mapping_stats['successful_mappings'],
            'failed_mappings': self.mapping_stats['failed_mappings'],
            'success_rate': round(success_rate, 2),
            'most_used_fields': sorted_usage[:10],
            'total_field_types': len(self.field_mappings),
            'available_aliases': sum(len(m.aliases) for m in self.field_mappings.values())
        }

    def _log_mapping_summary(self):
        """记录映射摘要"""
        logger.info(f"📊 字段映射摘要:")
        logger.info(f"  标准字段: {len(self.field_mappings)}")
        logger.info(f"  总别名数: {len(self.alias_to_standard)}")
        
        # 按分类统计
        category_count = {}
        for mapping in self.field_mappings.values():
            category = mapping.category.value
            category_count[category] = category_count.get(category, 0) + 1
        
        logger.info(f"  分类分布:")
        for category, count in category_count.items():
            logger.info(f"    {category}: {count}")


# 全局映射器实例
_global_field_mapper: Optional[UnifiedFieldMapper] = None


def get_field_mapper() -> UnifiedFieldMapper:
    """
    获取全局字段映射器实例
    
    Returns:
        UnifiedFieldMapper: 映射器实例
    """
    global _global_field_mapper
    if _global_field_mapper is None:
        _global_field_mapper = UnifiedFieldMapper()
    return _global_field_mapper


if __name__ == "__main__":
    # 测试模块
    def test_unified_field_mapper():
        """测试统一字段映射器"""
        
        print("\n" + "="*70)
        print("🗂️ 统一字段映射器测试")
        print("="*70)
        
        # 创建映射器
        mapper = UnifiedFieldMapper()
        
        # 测试下载器结果修复
        print("\n🔧 测试下载器结果修复...")
        
        # 模拟RealtimeDownloaderWrapper返回结果（使用file_path）
        downloader_result = {
            'success': True,
            'file_path': '/Users/test/hkexann/00700/announcement.pdf',
            'filename': 'announcement.pdf',
            'file_size': '1024',
            'download_time': 1.5
        }
        
        fixed_result = mapper.fix_downloader_result(downloader_result)
        print(f"  原始结果: {downloader_result}")
        print(f"  修复结果: {fixed_result}")
        print(f"  包含local_path: {'local_path' in fixed_result}")
        
        # 测试公告数据标准化
        print(f"\n📝 测试公告数据标准化...")
        
        raw_announcement = {
            'STOCK_CODE': '00700',
            'TITLE': '  腾讯控股有限公司 - 季度业绩公告  ',
            'DATE_TIME': '2025-09-10 18:00:00',
            'FILE_LINK': 'https://www1.hkexnews.hk/listedco/listconews/sehk/2025/0910/2025091000123.pdf',
            'ID': 'ANN_2025_001'
        }
        
        standardized = mapper.standardize_announcement_data(raw_announcement)
        print(f"  原始公告: {raw_announcement}")
        print(f"  标准化后: {standardized}")
        
        # 测试错误数据处理
        print(f"\n❌ 测试错误数据处理...")
        
        invalid_data = {
            'file_path': '',  # 无效路径
            'file_size': 'abc',  # 无效大小
            'stock_code': 'INVALID',  # 无效股票代码
            'unknown_field': 'test'  # 未知字段
        }
        
        mapping_result = mapper.map_fields(invalid_data, strict_mode=True)
        print(f"  映射成功: {mapping_result.success}")
        print(f"  错误信息: {mapping_result.errors}")
        print(f"  警告信息: {mapping_result.warnings}")
        print(f"  未映射字段: {mapping_result.unmapped_fields}")
        
        # 显示字段schema
        print(f"\n📋 字段Schema示例:")
        schema = mapper.get_field_schema()
        
        for field_name in ['file_path', 'stock_code', 'success']:
            field_info = schema[field_name]
            print(f"  {field_name}:")
            print(f"    别名: {field_info['aliases']}")
            print(f"    类型: {field_info['data_type']}")
            print(f"    必需: {field_info['required']}")
            print(f"    描述: {field_info['description']}")
        
        # 显示统计信息
        print(f"\n📊 映射统计信息:")
        stats = mapper.get_mapping_statistics()
        
        print(f"  总映射次数: {stats['total_mappings']}")
        print(f"  成功率: {stats['success_rate']}%")
        print(f"  标准字段数: {stats['total_field_types']}")
        print(f"  可用别名数: {stats['available_aliases']}")
        
        if stats['most_used_fields']:
            print(f"  最常用字段:")
            for field, count in stats['most_used_fields'][:5]:
                print(f"    {field}: {count} 次")
        
        print("\n" + "="*70)
    
    # 运行测试
    test_unified_field_mapper()
