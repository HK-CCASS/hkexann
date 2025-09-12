"""
数据源抽象基类和具体实现
实现统一的数据源接口，支持CSV、ClickHouse和混合数据源模式
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataSourceType(Enum):
    """数据源类型枚举"""
    CSV = "csv"
    CLICKHOUSE = "clickhouse"
    HYBRID = "hybrid"
    MEMORY = "memory"


class DataSourceStatus(Enum):
    """数据源状态枚举"""
    ACTIVE = "active"
    ERROR = "error"
    UNAVAILABLE = "unavailable"
    DEGRADED = "degraded"


@dataclass
class DataSourceConfig:
    """数据源配置类"""
    source_type: DataSourceType
    name: str
    description: str = ""
    enabled: bool = True
    priority: int = 1  # 数字越小优先级越高
    timeout_seconds: int = 30
    retry_attempts: int = 3
    cache_ttl_seconds: int = 300  # 缓存存活时间
    config_params: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.config_params is None:
            self.config_params = {}


@dataclass 
class DataSourceHealth:
    """数据源健康状态"""
    status: DataSourceStatus
    last_check_time: datetime
    response_time_ms: Optional[float] = None
    error_message: Optional[str] = None
    success_rate: float = 0.0  # 最近的成功率
    total_requests: int = 0
    failed_requests: int = 0


@dataclass
class QueryResult:
    """查询结果封装"""
    success: bool
    data: List[Any]
    total_count: int = 0
    source_name: str = ""
    query_time_ms: float = 0.0
    error_message: Optional[str] = None
    cached: bool = False
    
    
class FinancialEventSource(ABC):
    """
    金融事件数据源抽象基类
    定义统一的数据源接口，支持多种实现方式
    """
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化数据源
        
        Args:
            config: 数据源配置
        """
        self.config = config
        self.name = config.name
        self.logger = logging.getLogger(f'data_source.{self.name}')
        self.health = DataSourceHealth(
            status=DataSourceStatus.UNAVAILABLE,
            last_check_time=datetime.now()
        )
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        
        self.logger.info(f"数据源 {self.name} 初始化完成，类型: {config.source_type.value}")
    
    @abstractmethod
    async def initialize(self) -> bool:
        """
        初始化数据源连接
        
        Returns:
            初始化是否成功
        """
        pass
    
    @abstractmethod
    async def fetch_events(self, event_type: str, stock_code: Optional[str] = None,
                          date_range: Optional[tuple] = None, 
                          limit: Optional[int] = None) -> QueryResult:
        """
        获取金融事件数据
        
        Args:
            event_type: 事件类型 ('rights_issue', 'placement', 'consolidation', 'stock_split')
            stock_code: 股票代码（可选）
            date_range: 日期范围元组 (start_date, end_date)（可选）
            limit: 返回记录数限制（可选）
            
        Returns:
            查询结果
        """
        pass
    
    @abstractmethod
    async def get_available_event_types(self) -> List[str]:
        """
        获取可用的事件类型列表
        
        Returns:
            事件类型列表
        """
        pass
    
    @abstractmethod
    async def get_stock_codes(self, event_type: str) -> List[str]:
        """
        获取指定事件类型的所有股票代码
        
        Args:
            event_type: 事件类型
            
        Returns:
            股票代码列表
        """
        pass
    
    async def health_check(self) -> DataSourceHealth:
        """
        健康检查
        
        Returns:
            健康状态
        """
        try:
            start_time = datetime.now()
            
            # 执行简单的可用性检查
            await self._perform_health_check()
            
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            self.health = DataSourceHealth(
                status=DataSourceStatus.ACTIVE,
                last_check_time=datetime.now(),
                response_time_ms=response_time,
                success_rate=self._calculate_success_rate(),
                total_requests=self.health.total_requests,
                failed_requests=self.health.failed_requests
            )
            
        except Exception as e:
            self.logger.error(f"数据源 {self.name} 健康检查失败: {e}")
            self.health = DataSourceHealth(
                status=DataSourceStatus.ERROR,
                last_check_time=datetime.now(),
                error_message=str(e),
                success_rate=self._calculate_success_rate(),
                total_requests=self.health.total_requests,
                failed_requests=self.health.failed_requests + 1
            )
            
        return self.health
    
    @abstractmethod
    async def _perform_health_check(self):
        """
        执行具体的健康检查逻辑
        子类需要实现此方法
        """
        pass
    
    def _calculate_success_rate(self) -> float:
        """计算成功率"""
        if self.health.total_requests == 0:
            return 0.0
        return (self.health.total_requests - self.health.failed_requests) / self.health.total_requests
    
    def _get_cache_key(self, event_type: str, stock_code: Optional[str] = None,
                      date_range: Optional[tuple] = None) -> str:
        """生成缓存键"""
        key_parts = [event_type]
        if stock_code:
            key_parts.append(stock_code)
        if date_range:
            key_parts.append(f"{date_range[0]}_{date_range[1]}")
        return "|".join(key_parts)
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self._cache_timestamps:
            return False
        
        cache_time = self._cache_timestamps[cache_key]
        ttl = timedelta(seconds=self.config.cache_ttl_seconds)
        
        return datetime.now() - cache_time < ttl
    
    def _get_cached_result(self, cache_key: str) -> Optional[QueryResult]:
        """获取缓存结果"""
        if self._is_cache_valid(cache_key):
            result = self._cache.get(cache_key)
            if result:
                result.cached = True
                return result
        return None
    
    def _set_cache(self, cache_key: str, result: QueryResult):
        """设置缓存"""
        self._cache[cache_key] = result
        self._cache_timestamps[cache_key] = datetime.now()
    
    def _clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        self._cache_timestamps.clear()
    
    async def close(self):
        """关闭数据源连接"""
        self._clear_cache()
        await self._cleanup_resources()
        self.logger.info(f"数据源 {self.name} 已关闭")
    
    @abstractmethod
    async def _cleanup_resources(self):
        """
        清理资源
        子类需要实现此方法
        """
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取数据源状态信息
        
        Returns:
            状态信息字典
        """
        return {
            'name': self.name,
            'type': self.config.source_type.value,
            'enabled': self.config.enabled,
            'priority': self.config.priority,
            'status': self.health.status.value,
            'last_check_time': self.health.last_check_time.isoformat(),
            'response_time_ms': self.health.response_time_ms,
            'success_rate': self.health.success_rate,
            'total_requests': self.health.total_requests,
            'failed_requests': self.health.failed_requests,
            'error_message': self.health.error_message,
            'cache_size': len(self._cache)
        }