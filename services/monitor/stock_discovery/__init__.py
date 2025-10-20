"""
股票发现模块

提供多级股票发现系统，包括ClickHouse数据库、配置文件、
港股通列表等多种股票代码获取方式。
"""

from .enhanced_discovery import EnhancedStockDiscoveryManager
from .clickhouse_integration import ClickHouseStockExtractor

# 为了向后兼容，提供别名
StockDiscoveryManager = EnhancedStockDiscoveryManager

__all__ = [
    'EnhancedStockDiscoveryManager',
    'StockDiscoveryManager',  # 添加别名到导出列表
    'ClickHouseStockExtractor'
]
