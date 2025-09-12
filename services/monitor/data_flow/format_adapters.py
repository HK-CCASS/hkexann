"""
统一数据格式适配器

这个模块负责统一处理来自不同API来源的公告数据，确保系统内部使用统一的数据格式。
解决监听API和下载API返回数据格式不一致的问题。

主要功能：
- 标准化公告数据格式
- 统一字段命名规范
- 数据类型转换和验证
- 错误数据过滤和修复

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import re
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSource(Enum):
    """数据来源枚举"""
    MONITORING_API = "monitoring_api"  # 监听API (lcisehk1relsdc_1.json)
    DOWNLOAD_API = "download_api"      # 下载API (titleSearchServlet.do)
    UNKNOWN = "unknown"


@dataclass
class StandardAnnouncement:
    """标准化公告数据结构"""
    # 核心字段
    stock_code: str
    stock_name: str
    title: str
    file_link: str
    date_time: str
    
    # 分类字段  
    long_text: str = ""
    short_text: str = ""
    
    # 文件信息
    file_size: str = ""
    file_type: str = ""
    
    # 系统字段
    news_id: str = ""
    market: str = "SEHK"
    source: str = ""
    
    # 扩展字段
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class AnnouncementFormatAdapter:
    """
    公告格式适配器
    
    负责将不同API来源的公告数据转换为统一的标准格式。
    """
    
    def __init__(self):
        """初始化格式适配器"""
        # URL基础地址
        self.base_url = "https://www1.hkexnews.hk"
        
        # 字段映射规则
        self.field_mappings = {
            DataSource.MONITORING_API: {
                'stock_code': ['sc'],
                'stock_name': ['sn'],
                'title': ['title'],
                'long_text': ['lTxt'],
                'short_text': ['sTxt'],
                'date_time': ['relTime'],
                'file_link': ['webPath'],  # 需要特殊处理
                'file_size': ['size'],
                'file_type': ['ext'],
                'news_id': ['newsId'],
                'market': ['market']
            },
            DataSource.DOWNLOAD_API: {
                'stock_code': ['stock_code', 'STOCK_CODE'],
                'stock_name': ['stock_name', 'STOCK_NAME'],
                'title': ['title', 'TITLE'],
                'long_text': ['category', 'LONG_TEXT'],
                'short_text': ['SHORT_TEXT'],
                'date_time': ['date', 'DATE_TIME'],
                'file_link': ['link', 'FILE_LINK'],
                'file_size': ['FILE_SIZE'],
                'file_type': ['FILE_TYPE'],
                'news_id': ['NEWS_ID'],
                'market': ['MARKET']
            }
        }
        
        # 数据清洗规则
        self.cleaning_rules = {
            'stock_code': self._clean_stock_code,
            'title': self._clean_title,
            'file_link': self._clean_file_link,
            'date_time': self._clean_date_time,
            'file_size': self._clean_file_size
        }

    def adapt_announcement(self, raw_data: Dict[str, Any], source: DataSource = None) -> Optional[StandardAnnouncement]:
        """
        适配单个公告数据
        
        Args:
            raw_data: 原始公告数据
            source: 数据来源
            
        Returns:
            Optional[StandardAnnouncement]: 标准化后的公告数据
        """
        try:
            # 自动检测数据源
            if source is None:
                source = self._detect_data_source(raw_data)
            
            # 提取字段
            extracted_data = self._extract_fields(raw_data, source)
            
            # 数据清洗
            cleaned_data = self._clean_data(extracted_data)
            
            # 验证必需字段
            if not self._validate_required_fields(cleaned_data):
                logger.warning(f"公告数据缺少必需字段: {cleaned_data}")
                return None
            
            # 创建标准公告对象
            announcement = StandardAnnouncement(
                stock_code=cleaned_data['stock_code'],
                stock_name=cleaned_data['stock_name'],
                title=cleaned_data['title'],
                file_link=cleaned_data['file_link'],
                date_time=cleaned_data['date_time'],
                long_text=cleaned_data.get('long_text', ''),
                short_text=cleaned_data.get('short_text', ''),
                file_size=cleaned_data.get('file_size', ''),
                file_type=cleaned_data.get('file_type', ''),
                news_id=cleaned_data.get('news_id', ''),
                market=cleaned_data.get('market', 'SEHK'),
                source=source.value,
                metadata={
                    'original_data': raw_data,
                    'processing_time': datetime.now().isoformat(),
                    'adapter_version': '1.0.0'
                }
            )
            
            return announcement
            
        except Exception as e:
            logger.error(f"适配公告数据失败: {e}, 原始数据: {raw_data}")
            return None

    def adapt_announcements_batch(self, raw_data_list: List[Dict[str, Any]], 
                                 source: DataSource = None) -> List[StandardAnnouncement]:
        """
        批量适配公告数据
        
        Args:
            raw_data_list: 原始公告数据列表
            source: 数据来源
            
        Returns:
            List[StandardAnnouncement]: 标准化后的公告数据列表
        """
        adapted_announcements = []
        
        for raw_data in raw_data_list:
            adapted = self.adapt_announcement(raw_data, source)
            if adapted:
                adapted_announcements.append(adapted)
        
        logger.info(f"批量适配完成: {len(raw_data_list)} -> {len(adapted_announcements)}")
        return adapted_announcements

    def _detect_data_source(self, raw_data: Dict[str, Any]) -> DataSource:
        """
        自动检测数据来源
        
        Args:
            raw_data: 原始数据
            
        Returns:
            DataSource: 检测到的数据源
        """
        # 监听API特征字段
        monitoring_api_fields = ['webPath', 'newsId', 'relTime', 'lTxt', 'sTxt']
        
        # 下载API特征字段
        download_api_fields = ['FILE_LINK', 'TITLE', 'DATE_TIME']
        
        # 计算匹配分数
        monitoring_score = sum(1 for field in monitoring_api_fields if field in raw_data)
        download_score = sum(1 for field in download_api_fields if field in raw_data)
        
        if monitoring_score > download_score:
            return DataSource.MONITORING_API
        elif download_score > monitoring_score:
            return DataSource.DOWNLOAD_API
        else:
            return DataSource.UNKNOWN

    def _extract_fields(self, raw_data: Dict[str, Any], source: DataSource) -> Dict[str, Any]:
        """
        根据来源提取字段
        
        Args:
            raw_data: 原始数据
            source: 数据来源
            
        Returns:
            Dict[str, Any]: 提取的字段数据
        """
        extracted = {}
        mappings = self.field_mappings.get(source, {})
        
        for target_field, source_fields in mappings.items():
            value = None
            
            # 尝试从多个可能的字段中提取值
            for source_field in source_fields:
                if source_field in raw_data and raw_data[source_field]:
                    value = raw_data[source_field]
                    break
            
            # 特殊处理
            if target_field == 'file_link' and source == DataSource.MONITORING_API:
                # 监听API需要构建完整URL
                web_path = raw_data.get('webPath', '')
                if web_path:
                    value = f"{self.base_url}{web_path}"
                else:
                    value = ''
            elif target_field == 'stock_code' and source == DataSource.MONITORING_API:
                # 监听API股票代码在stock数组中
                stocks = raw_data.get('stock', [])
                if stocks and isinstance(stocks, list):
                    value = stocks[0].get('sc', '')
            elif target_field == 'stock_name' and source == DataSource.MONITORING_API:
                # 监听API股票名称在stock数组中
                stocks = raw_data.get('stock', [])
                if stocks and isinstance(stocks, list):
                    value = stocks[0].get('sn', '')
            
            extracted[target_field] = value or ''
        
        return extracted

    def _clean_data(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        数据清洗
        
        Args:
            extracted_data: 提取的数据
            
        Returns:
            Dict[str, Any]: 清洗后的数据
        """
        cleaned = {}
        
        for field, value in extracted_data.items():
            if field in self.cleaning_rules:
                cleaned[field] = self.cleaning_rules[field](value)
            else:
                cleaned[field] = str(value).strip() if value else ''
        
        return cleaned

    def _validate_required_fields(self, data: Dict[str, Any]) -> bool:
        """
        验证必需字段
        
        Args:
            data: 数据
            
        Returns:
            bool: 是否包含所有必需字段
        """
        required_fields = ['stock_code', 'title', 'file_link']
        
        for field in required_fields:
            if not data.get(field, '').strip():
                return False
        
        return True

    def _clean_stock_code(self, stock_code: str) -> str:
        """清洗股票代码"""
        if not stock_code:
            return ''
        
        # 移除常见后缀
        code = str(stock_code).strip().upper()
        code = re.sub(r'\.(HK|HKEX)$', '', code)
        
        # 验证格式并标准化
        if re.match(r'^\d{4,5}$', code):
            return code.zfill(5)  # 确保5位数格式
        
        return code

    def _clean_title(self, title: str) -> str:
        """清洗标题"""
        if not title:
            return ''
        
        # 移除多余空格和特殊字符
        cleaned = re.sub(r'\s+', ' ', str(title).strip())
        cleaned = cleaned.replace('/', '-')  # 替换可能影响文件名的字符
        
        return cleaned

    def _clean_file_link(self, file_link: str) -> str:
        """清洗文件链接"""
        if not file_link:
            return ''
        
        link = str(file_link).strip()
        
        # 确保是完整URL
        if link.startswith('/'):
            link = f"{self.base_url}{link}"
        elif not link.startswith('http'):
            link = f"{self.base_url}/{link}"
        
        return link

    def _clean_date_time(self, date_time: str) -> str:
        """清洗日期时间"""
        if not date_time:
            return ''
        
        date_str = str(date_time).strip()
        
        # 尝试标准化不同的日期格式
        # 监听API格式: "10/09/2025 19:05"
        if re.match(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}', date_str):
            try:
                dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        # 下载API格式: "2025-01-15"
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str
        
        return date_str

    def _clean_file_size(self, file_size: str) -> str:
        """清洗文件大小"""
        if not file_size:
            return ''
        
        size_str = str(file_size).strip()
        
        # 标准化单位
        size_str = size_str.replace('KB', 'KB').replace('MB', 'MB').replace('GB', 'GB')
        
        return size_str

    def convert_to_dict(self, announcement: StandardAnnouncement) -> Dict[str, Any]:
        """
        将标准公告对象转换为字典
        
        Args:
            announcement: 标准公告对象
            
        Returns:
            Dict[str, Any]: 字典格式的公告数据
        """
        return {
            'STOCK_CODE': announcement.stock_code,
            'STOCK_NAME': announcement.stock_name,
            'TITLE': announcement.title,
            'FILE_LINK': announcement.file_link,
            'DATE_TIME': announcement.date_time,
            'LONG_TEXT': announcement.long_text,
            'SHORT_TEXT': announcement.short_text,
            'FILE_SIZE': announcement.file_size,
            'FILE_TYPE': announcement.file_type,
            'NEWS_ID': announcement.news_id,
            'MARKET': announcement.market,
            'SOURCE': announcement.source,
            'METADATA': announcement.metadata
        }

    def get_adapter_statistics(self) -> Dict[str, Any]:
        """
        获取适配器统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {
            'supported_sources': [source.value for source in DataSource],
            'field_mappings': self.field_mappings,
            'cleaning_rules': list(self.cleaning_rules.keys()),
            'base_url': self.base_url
        }


# 便捷函数
def adapt_monitoring_api_data(raw_data_list: List[Dict[str, Any]]) -> List[StandardAnnouncement]:
    """
    适配监听API数据的便捷函数
    
    Args:
        raw_data_list: 原始监听API数据列表
        
    Returns:
        List[StandardAnnouncement]: 标准化公告列表
    """
    adapter = AnnouncementFormatAdapter()
    return adapter.adapt_announcements_batch(raw_data_list, DataSource.MONITORING_API)


def adapt_download_api_data(raw_data_list: List[Dict[str, Any]]) -> List[StandardAnnouncement]:
    """
    适配下载API数据的便捷函数
    
    Args:
        raw_data_list: 原始下载API数据列表
        
    Returns:
        List[StandardAnnouncement]: 标准化公告列表
    """
    adapter = AnnouncementFormatAdapter()
    return adapter.adapt_announcements_batch(raw_data_list, DataSource.DOWNLOAD_API)


def adapt_mixed_api_data(raw_data_list: List[Dict[str, Any]]) -> List[StandardAnnouncement]:
    """
    适配混合API数据的便捷函数（自动检测来源）
    
    Args:
        raw_data_list: 原始混合API数据列表
        
    Returns:
        List[StandardAnnouncement]: 标准化公告列表
    """
    adapter = AnnouncementFormatAdapter()
    return adapter.adapt_announcements_batch(raw_data_list)


if __name__ == "__main__":
    # 测试模块
    def test_format_adapter():
        """测试格式适配器"""
        
        print("\n" + "="*70)
        print("🔧 统一数据格式适配器测试")
        print("="*70)
        
        adapter = AnnouncementFormatAdapter()
        
        # 测试监听API数据
        print("\n📡 测试监听API数据格式...")
        monitoring_data = {
            "newsId": 11836941,
            "title": "测试公告标题",
            "lTxt": "公告及通告 - 测试分类",
            "sTxt": "简短描述",
            "webPath": "/listedco/listconews/sehk/2025/0910/test.pdf",
            "size": "525KB",
            "ext": "pdf",
            "market": "SEHK",
            "relTime": "10/09/2025 19:05",
            "stock": [{"sc": "00700", "sn": "腾讯控股"}]
        }
        
        adapted_monitoring = adapter.adapt_announcement(monitoring_data, DataSource.MONITORING_API)
        if adapted_monitoring:
            print(f"✅ 监听API适配成功:")
            print(f"   股票代码: {adapted_monitoring.stock_code}")
            print(f"   标题: {adapted_monitoring.title[:50]}...")
            print(f"   文件链接: {adapted_monitoring.file_link}")
            print(f"   来源: {adapted_monitoring.source}")
        
        # 测试下载API数据
        print("\n📥 测试下载API数据格式...")
        download_data = {
            "STOCK_CODE": "00939",
            "STOCK_NAME": "建设银行",
            "TITLE": "测试下载API公告",
            "FILE_LINK": "https://www1.hkexnews.hk/test_download.pdf",
            "DATE_TIME": "2025-01-15",
            "LONG_TEXT": "下载API分类",
            "FILE_SIZE": "1MB",
            "FILE_TYPE": "pdf"
        }
        
        adapted_download = adapter.adapt_announcement(download_data, DataSource.DOWNLOAD_API)
        if adapted_download:
            print(f"✅ 下载API适配成功:")
            print(f"   股票代码: {adapted_download.stock_code}")
            print(f"   标题: {adapted_download.title}")
            print(f"   文件链接: {adapted_download.file_link}")
            print(f"   来源: {adapted_download.source}")
        
        # 测试自动检测
        print("\n🔍 测试自动来源检测...")
        detected_source_1 = adapter._detect_data_source(monitoring_data)
        detected_source_2 = adapter._detect_data_source(download_data)
        
        print(f"监听API数据检测结果: {detected_source_1.value}")
        print(f"下载API数据检测结果: {detected_source_2.value}")
        
        # 测试批量适配
        print("\n📦 测试批量适配...")
        mixed_data = [monitoring_data, download_data]
        batch_adapted = adapter.adapt_announcements_batch(mixed_data)
        
        print(f"批量适配结果: {len(batch_adapted)} 条公告")
        for i, announcement in enumerate(batch_adapted, 1):
            print(f"  {i}. {announcement.stock_code} - {announcement.title[:30]}... ({announcement.source})")
        
        # 测试统计信息
        print("\n📊 适配器统计信息:")
        stats = adapter.get_adapter_statistics()
        print(f"支持的数据源: {stats['supported_sources']}")
        print(f"清洗规则: {stats['cleaning_rules']}")
        
        print("\n" + "="*70)
    
    # 运行测试
    test_format_adapter()
