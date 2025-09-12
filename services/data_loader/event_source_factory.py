"""
事件数据源工厂类
支持配置驱动的数据源实例创建和管理
"""

import logging
from typing import Dict, Any, Optional, List, Union
from pathlib import Path

from .data_source import FinancialEventSource, DataSourceConfig, DataSourceType
from .csv_event_source import CSVEventSource
from .clickhouse_event_source import ClickHouseEventSource


logger = logging.getLogger(__name__)


class EventSourceFactory:
    """
    事件数据源工厂类
    根据配置创建和管理不同类型的数据源实例
    """
    
    def __init__(self):
        """初始化工厂"""
        self.logger = logging.getLogger('data_source.factory')
        self._registered_sources: Dict[str, FinancialEventSource] = {}
        
        self.logger.info("事件数据源工厂初始化完成")
    
    def create_csv_source(self, name: str, csv_directory: str, 
                         **config_params) -> CSVEventSource:
        """
        创建CSV数据源
        
        Args:
            name: 数据源名称
            csv_directory: CSV文件目录路径
            **config_params: 额外的配置参数
            
        Returns:
            CSV数据源实例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.CSV,
            name=name,
            description=f"CSV数据源 - {csv_directory}",
            config_params=config_params
        )
        
        source = CSVEventSource(config, csv_directory)
        self._registered_sources[name] = source
        
        self.logger.info(f"创建CSV数据源: {name}")
        return source
    
    def create_clickhouse_source(self, name: str, host: str = 'localhost', 
                               port: int = 9000, database: str = 'default',
                               user: str = 'default', password: str = '',
                               **config_params) -> ClickHouseEventSource:
        """
        创建ClickHouse数据源
        
        Args:
            name: 数据源名称
            host: ClickHouse服务器主机
            port: ClickHouse服务器端口
            database: 数据库名称
            user: 用户名
            password: 密码
            **config_params: 额外的配置参数
            
        Returns:
            ClickHouse数据源实例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.CLICKHOUSE,
            name=name,
            description=f"ClickHouse数据源 - {host}:{port}/{database}",
            config_params=config_params
        )
        
        source = ClickHouseEventSource(config, host, port, database, user, password)
        self._registered_sources[name] = source
        
        self.logger.info(f"创建ClickHouse数据源: {name}")
        return source
    
    def create_source_from_config(self, source_config: Dict[str, Any]) -> FinancialEventSource:
        """
        根据配置字典创建数据源
        
        Args:
            source_config: 数据源配置字典
            
        Returns:
            数据源实例
            
        Raises:
            ValueError: 配置无效或不支持的数据源类型
        """
        # 验证必需字段
        if 'name' not in source_config:
            raise ValueError("数据源配置必须包含 'name' 字段")
        
        if 'type' not in source_config:
            raise ValueError("数据源配置必须包含 'type' 字段")
        
        name = source_config['name']
        source_type = source_config['type']
        
        try:
            # 根据类型创建相应的数据源
            if source_type == 'csv' or source_type == DataSourceType.CSV:
                csv_directory = source_config.get('csv_directory', 'event_csv')
                return self.create_csv_source(
                    name=name,
                    csv_directory=csv_directory,
                    **source_config.get('config_params', {})
                )
            
            elif source_type == 'clickhouse' or source_type == DataSourceType.CLICKHOUSE:
                return self.create_clickhouse_source(
                    name=name,
                    host=source_config.get('host', 'localhost'),
                    port=source_config.get('port', 9000),
                    database=source_config.get('database', 'default'),
                    user=source_config.get('user', 'default'),
                    password=source_config.get('password', ''),
                    **source_config.get('config_params', {})
                )
            
            else:
                raise ValueError(f"不支持的数据源类型: {source_type}")
                
        except Exception as e:
            self.logger.error(f"创建数据源 {name} 失败: {e}")
            raise
    
    def create_sources_from_config_list(self, configs: List[Dict[str, Any]]) -> List[FinancialEventSource]:
        """
        从配置列表批量创建数据源
        
        Args:
            configs: 数据源配置列表
            
        Returns:
            数据源实例列表
        """
        sources = []
        
        for config in configs:
            try:
                source = self.create_source_from_config(config)
                sources.append(source)
            except Exception as e:
                self.logger.error(f"创建数据源失败，跳过: {e}")
                continue
        
        self.logger.info(f"批量创建数据源完成，成功: {len(sources)}/{len(configs)}")
        return sources
    
    def get_source(self, name: str) -> Optional[FinancialEventSource]:
        """
        获取已注册的数据源
        
        Args:
            name: 数据源名称
            
        Returns:
            数据源实例，如果不存在则返回None
        """
        return self._registered_sources.get(name)
    
    def list_sources(self) -> List[str]:
        """
        列出所有已注册的数据源名称
        
        Returns:
            数据源名称列表
        """
        return list(self._registered_sources.keys())
    
    def remove_source(self, name: str) -> bool:
        """
        移除数据源
        
        Args:
            name: 数据源名称
            
        Returns:
            是否成功移除
        """
        if name in self._registered_sources:
            source = self._registered_sources.pop(name)
            
            # 异步关闭资源
            try:
                import asyncio
                if asyncio.get_event_loop().is_running():
                    # 在当前事件循环中创建任务
                    asyncio.create_task(source.close())
                else:
                    # 运行新的事件循环
                    asyncio.run(source.close())
            except Exception as e:
                self.logger.warning(f"关闭数据源资源失败: {e}")
            
            self.logger.info(f"移除数据源: {name}")
            return True
        
        return False
    
    async def initialize_all_sources(self) -> Dict[str, bool]:
        """
        初始化所有已注册的数据源
        
        Returns:
            各数据源的初始化结果
        """
        results = {}
        
        for name, source in self._registered_sources.items():
            try:
                success = await source.initialize()
                results[name] = success
                
                if success:
                    self.logger.info(f"数据源 {name} 初始化成功")
                else:
                    self.logger.error(f"数据源 {name} 初始化失败")
                    
            except Exception as e:
                self.logger.error(f"数据源 {name} 初始化异常: {e}")
                results[name] = False
        
        return results
    
    async def health_check_all_sources(self) -> Dict[str, Dict[str, Any]]:
        """
        对所有数据源进行健康检查
        
        Returns:
            各数据源的健康状态
        """
        results = {}
        
        for name, source in self._registered_sources.items():
            try:
                health = await source.health_check()
                results[name] = {
                    'status': health.status.value,
                    'response_time_ms': health.response_time_ms,
                    'error_message': health.error_message,
                    'success_rate': health.success_rate
                }
            except Exception as e:
                self.logger.error(f"数据源 {name} 健康检查失败: {e}")
                results[name] = {
                    'status': 'error',
                    'error_message': str(e),
                    'success_rate': 0.0
                }
        
        return results
    
    async def close_all_sources(self):
        """关闭所有数据源"""
        for name, source in self._registered_sources.items():
            try:
                await source.close()
                self.logger.info(f"关闭数据源: {name}")
            except Exception as e:
                self.logger.error(f"关闭数据源 {name} 失败: {e}")
        
        self._registered_sources.clear()
        self.logger.info("所有数据源已关闭")
    
    def get_source_status_summary(self) -> Dict[str, Any]:
        """
        获取所有数据源的状态摘要
        
        Returns:
            状态摘要信息
        """
        summary = {
            'total_sources': len(self._registered_sources),
            'sources_by_type': {},
            'sources': {}
        }
        
        # 按类型统计
        type_counts = {}
        for source in self._registered_sources.values():
            source_type = source.config.source_type.value
            type_counts[source_type] = type_counts.get(source_type, 0) + 1
        
        summary['sources_by_type'] = type_counts
        
        # 各数据源状态
        for name, source in self._registered_sources.items():
            summary['sources'][name] = source.get_status()
        
        return summary
    
    @staticmethod
    def create_default_factory_from_config(config: Dict[str, Any]) -> 'EventSourceFactory':
        """
        从配置创建默认的工厂实例
        
        Args:
            config: 完整的应用配置
            
        Returns:
            工厂实例
        """
        factory = EventSourceFactory()
        
        # 获取数据源配置
        data_sources_config = config.get('data_sources', {})
        
        # 创建CSV数据源（如果配置存在）
        csv_config = data_sources_config.get('csv')
        if csv_config and csv_config.get('enabled', True):
            try:
                factory.create_csv_source(
                    name=csv_config.get('name', 'csv_default'),
                    csv_directory=csv_config.get('directory', 'event_csv'),
                    **csv_config.get('config_params', {})
                )
            except Exception as e:
                logger.warning(f"创建默认CSV数据源失败: {e}")
        
        # 创建ClickHouse数据源（如果配置存在）
        clickhouse_config = data_sources_config.get('clickhouse')
        if clickhouse_config and clickhouse_config.get('enabled', False):
            try:
                factory.create_clickhouse_source(
                    name=clickhouse_config.get('name', 'clickhouse_default'),
                    host=clickhouse_config.get('host', 'localhost'),
                    port=clickhouse_config.get('port', 9000),
                    database=clickhouse_config.get('database', 'default'),
                    user=clickhouse_config.get('user', 'default'),
                    password=clickhouse_config.get('password', ''),
                    **clickhouse_config.get('config_params', {})
                )
            except Exception as e:
                logger.warning(f"创建默认ClickHouse数据源失败: {e}")
        
        return factory