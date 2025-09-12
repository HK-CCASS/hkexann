"""
ClickHouse事件数据源实现
实现基于ClickHouse数据库的金融事件数据访问
"""

import time
import asyncio
import threading
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from contextlib import asynccontextmanager

try:
    from clickhouse_driver import Client
    from clickhouse_driver.errors import Error as ClickHouseError
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    Client = None
    ClickHouseError = Exception

from .data_source import FinancialEventSource, DataSourceConfig, QueryResult


class ClickHouseEventSource(FinancialEventSource):
    """
    ClickHouse数据库数据源实现
    提供高性能的金融事件数据访问
    """
    
    def __init__(self, config: DataSourceConfig, host: str = 'localhost', 
                 port: int = 9000, database: str = 'default', 
                 user: str = 'default', password: str = ''):
        """
        初始化ClickHouse数据源
        
        Args:
            config: 数据源配置
            host: ClickHouse服务器主机
            port: ClickHouse服务器端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        super().__init__(config)
        
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError("ClickHouse数据源需要安装clickhouse-driver: pip install clickhouse-driver")
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self._client: Optional[Client] = None
        self._connection_pool: List[Client] = []
        self._pool_lock = threading.Lock()
        self._max_connections = 5
        self._connection_retry_attempts = 3
        self._connection_retry_delay = 1.0
        
        # 表名映射
        self.table_mapping = {
            'rights_issue': 'rights_issue',
            'placement': 'placement', 
            'consolidation': 'consolidation',
            'stock_split': 'stock_split'
        }
        
        # 字段映射
        self.field_mapping = {
            'rights_issue': {
                'stock_code': 'stock_code',
                'company_name': 'company_name',
                'announcement_date': 'announcement_date',
                'status': 'status',
                'rights_price': 'rights_price',
                'rights_price_premium': 'rights_price_premium',
                'rights_ratio': 'rights_ratio',
                'current_price': 'current_price',
                'current_price_premium': 'current_price_premium',
                'stock_adjustment': 'stock_adjustment',
                'underwriter': 'underwriter',
                'ex_rights_date': 'ex_rights_date',
                'rights_trading_start': 'rights_trading_start',
                'rights_trading_end': 'rights_trading_end',
                'final_payment_date': 'final_payment_date',
                'result_announcement_date': 'result_announcement_date',
                'allotment_date': 'allotment_date'
            },
            'placement': {
                'stock_code': 'stock_code',
                'company_name': 'company_name', 
                'announcement_date': 'announcement_date',
                'status': 'status',
                'placement_price': 'placement_price',
                'placement_price_premium': 'placement_price_premium',
                'new_shares_ratio': 'new_shares_ratio',
                'current_price': 'current_price',
                'current_price_premium': 'current_price_premium',
                'placement_agent': 'placement_agent',
                'authorization_method': 'authorization_method',
                'placement_method': 'placement_method',
                'completion_date': 'completion_date'
            },
            'consolidation': {
                'stock_code': 'stock_code',
                'company_name': 'company_name',
                'announcement_date': 'announcement_date',
                'status': 'status',
                'temporary_counter': 'temporary_counter',
                'consolidation_ratio': 'consolidation_ratio',
                'effective_date': 'effective_date',
                'other_corporate_actions': 'other_corporate_actions'
            },
            'stock_split': {
                'stock_code': 'stock_code',
                'company_name': 'company_name',
                'announcement_date': 'announcement_date',
                'status': 'status',
                'temporary_counter': 'temporary_counter',
                'split_ratio': 'split_ratio',
                'effective_date': 'effective_date',
                'other_corporate_actions': 'other_corporate_actions'
            }
        }
        
        self.logger.info(f"ClickHouse数据源初始化，连接: {host}:{port}/{database}")
    
    async def initialize(self) -> bool:
        """初始化数据源连接"""
        try:
            # 创建主连接
            self._client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                send_receive_timeout=self.config.timeout_seconds
            )
            
            # 测试连接
            await self._test_connection()
            
            # 验证必要的表是否存在
            await self._verify_tables()
            
            # 初始化连接池
            await self._init_connection_pool()
            
            self.logger.info("ClickHouse数据源初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"ClickHouse数据源初始化失败: {e}")
            return False
    
    async def fetch_events(self, event_type: str, stock_code: Optional[str] = None,
                          date_range: Optional[tuple] = None, 
                          limit: Optional[int] = None) -> QueryResult:
        """获取金融事件数据"""
        start_time = time.time()
        self.health.total_requests += 1
        
        try:
            # 检查缓存
            cache_key = self._get_cache_key(event_type, stock_code, date_range)
            cached_result = self._get_cached_result(cache_key)
            if cached_result:
                return cached_result
            
            # 验证事件类型
            if event_type not in self.table_mapping:
                error_msg = f"不支持的事件类型: {event_type}"
                self.logger.error(error_msg)
                self.health.failed_requests += 1
                return QueryResult(
                    success=False,
                    data=[],
                    source_name=self.name,
                    error_message=error_msg
                )
            
            # 构建查询
            query, params = self._build_query(event_type, stock_code, date_range, limit)
            
            # 执行查询
            result_data = await self._execute_query(query, params)
            
            # 转换结果格式
            formatted_data = self._format_query_results(event_type, result_data)
            
            query_time_ms = (time.time() - start_time) * 1000
            
            result = QueryResult(
                success=True,
                data=formatted_data,
                total_count=len(formatted_data),
                source_name=self.name,
                query_time_ms=query_time_ms
            )
            
            # 缓存结果
            self._set_cache(cache_key, result)
            
            self.logger.debug(f"获取 {event_type} 事件成功，数量: {len(formatted_data)}")
            return result
            
        except Exception as e:
            self.logger.error(f"获取事件数据失败: {e}")
            self.health.failed_requests += 1
            
            return QueryResult(
                success=False,
                data=[],
                source_name=self.name,
                query_time_ms=(time.time() - start_time) * 1000,
                error_message=str(e)
            )
    
    async def get_available_event_types(self) -> List[str]:
        """获取可用的事件类型列表"""
        try:
            available_types = []
            
            for event_type, table_name in self.table_mapping.items():
                if await self._table_exists(table_name):
                    available_types.append(event_type)
            
            return available_types
            
        except Exception as e:
            self.logger.error(f"获取可用事件类型失败: {e}")
            return []
    
    async def get_stock_codes(self, event_type: str) -> List[str]:
        """获取指定事件类型的所有股票代码"""
        try:
            table_name = self.table_mapping.get(event_type)
            if not table_name:
                return []
            
            query = f"SELECT DISTINCT stock_code FROM {table_name} WHERE stock_code != '' ORDER BY stock_code"
            results = await self._execute_query(query, {})
            
            return [row[0] for row in results if row[0]]
            
        except Exception as e:
            self.logger.error(f"获取股票代码失败: {e}")
            return []
    
    async def _perform_health_check(self):
        """执行健康检查"""
        if not self._client:
            raise Exception("ClickHouse客户端未初始化")
        
        # 执行简单查询测试连接
        query = "SELECT 1"
        await self._execute_query(query, {})
    
    async def _cleanup_resources(self):
        """清理资源"""
        # 关闭连接池
        for client in self._connection_pool:
            try:
                if hasattr(client, 'disconnect'):
                    client.disconnect()
            except Exception as e:
                self.logger.warning(f"关闭连接池连接失败: {e}")
        
        self._connection_pool.clear()
        
        # 关闭主连接
        if self._client:
            try:
                if hasattr(self._client, 'disconnect'):
                    self._client.disconnect()
            except Exception as e:
                self.logger.warning(f"关闭主连接失败: {e}")
            self._client = None
    
    async def _test_connection(self):
        """测试数据库连接"""
        try:
            # 在线程中执行同步操作
            def sync_test():
                return self._client.execute("SELECT 1")
            
            result = await asyncio.get_event_loop().run_in_executor(None, sync_test)
            self.logger.info("ClickHouse连接测试成功")
            
        except Exception as e:
            raise Exception(f"ClickHouse连接测试失败: {e}")
    
    async def _verify_tables(self):
        """验证必要的表是否存在"""
        try:
            def sync_check():
                existing_tables = set()
                for table_name in self.table_mapping.values():
                    try:
                        self._client.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                        existing_tables.add(table_name)
                    except Exception:
                        pass
                return existing_tables
            
            existing_tables = await asyncio.get_event_loop().run_in_executor(None, sync_check)
            
            missing_tables = set(self.table_mapping.values()) - existing_tables
            if missing_tables:
                self.logger.warning(f"缺失的表: {missing_tables}")
            else:
                self.logger.info("所有必要的表都存在")
                
        except Exception as e:
            self.logger.error(f"表验证失败: {e}")
            raise
    
    async def _init_connection_pool(self):
        """初始化连接池"""
        try:
            for i in range(self._max_connections):
                client = await self._create_client_with_retry()
                if client:
                    self._connection_pool.append(client)
            
            if not self._connection_pool:
                raise Exception("无法创建任何有效连接")
            
            self.logger.info(f"连接池初始化完成，大小: {len(self._connection_pool)}")
            
        except Exception as e:
            self.logger.error(f"连接池初始化失败: {e}")
            raise
    
    async def _create_client_with_retry(self) -> Optional[Client]:
        """使用重试机制创建客户端连接"""
        for attempt in range(self._connection_retry_attempts):
            try:
                client = Client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    send_receive_timeout=self.config.timeout_seconds
                )
                
                # 测试连接
                def test_connection():
                    return client.execute("SELECT 1")
                
                await asyncio.get_event_loop().run_in_executor(None, test_connection)
                return client
                
            except Exception as e:
                self.logger.warning(f"创建连接失败 (尝试 {attempt + 1}/{self._connection_retry_attempts}): {e}")
                if attempt < self._connection_retry_attempts - 1:
                    await asyncio.sleep(self._connection_retry_delay * (2 ** attempt))
        
        return None
    
    @asynccontextmanager
    async def _get_connection(self):
        """从连接池获取连接的异步上下文管理器"""
        connection = None
        try:
            with self._pool_lock:
                if self._connection_pool:
                    connection = self._connection_pool.pop()
                else:
                    # 连接池为空，尝试创建新连接
                    connection = await self._create_client_with_retry()
            
            if not connection:
                raise Exception("无法获取有效的数据库连接")
            
            yield connection
            
        except Exception as e:
            # 连接有问题，不归还到池中
            if connection:
                try:
                    if hasattr(connection, 'disconnect'):
                        connection.disconnect()
                except:
                    pass
                connection = None
            raise e
            
        finally:
            # 归还连接到池中
            if connection:
                with self._pool_lock:
                    if len(self._connection_pool) < self._max_connections:
                        self._connection_pool.append(connection)
                    else:
                        # 池已满，关闭连接
                        try:
                            if hasattr(connection, 'disconnect'):
                                connection.disconnect()
                        except:
                            pass
    
    def _build_query(self, event_type: str, stock_code: Optional[str] = None,
                    date_range: Optional[tuple] = None, limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        """构建查询语句"""
        table_name = self.table_mapping[event_type]
        fields = self.field_mapping[event_type]
        
        # 选择字段
        select_fields = ', '.join(fields.values())
        query = f"SELECT {select_fields} FROM {table_name}"
        
        # 构建WHERE条件
        conditions = []
        params = {}
        
        if stock_code:
            conditions.append("stock_code = %(stock_code)s")
            params['stock_code'] = stock_code
        
        if date_range and len(date_range) == 2:
            conditions.append("announcement_date >= %(start_date)s")
            conditions.append("announcement_date <= %(end_date)s")
            params['start_date'] = date_range[0]
            params['end_date'] = date_range[1]
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # 排序
        query += " ORDER BY announcement_date DESC, stock_code"
        
        # 限制
        if limit and limit > 0:
            query += f" LIMIT {limit}"
        
        return query, params
    
    async def _execute_query(self, query: str, params: Dict[str, Any]) -> List[tuple]:
        """执行查询"""
        async with self._get_connection() as connection:
            def sync_execute():
                return connection.execute(query, params)
            
            return await asyncio.get_event_loop().run_in_executor(None, sync_execute)
    
    def _format_query_results(self, event_type: str, raw_results: List[tuple]) -> List[Dict[str, Any]]:
        """格式化查询结果"""
        if not raw_results:
            return []
        
        field_names = list(self.field_mapping[event_type].keys())
        formatted_results = []
        
        for row in raw_results:
            result_dict = {}
            for i, value in enumerate(row):
                if i < len(field_names):
                    field_name = field_names[i]
                    # 处理日期类型
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    result_dict[field_name] = value
            
            formatted_results.append(result_dict)
        
        return formatted_results
    
    async def _table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            query = f"SELECT 1 FROM {table_name} LIMIT 1"
            await self._execute_query(query, {})
            return True
        except Exception:
            return False
    
    async def get_table_statistics(self, event_type: str) -> Dict[str, Any]:
        """获取表统计信息"""
        try:
            table_name = self.table_mapping.get(event_type)
            if not table_name:
                return {}
            
            # 获取记录总数
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result = await self._execute_query(count_query, {})
            total_count = count_result[0][0] if count_result else 0
            
            # 获取最新记录时间
            latest_query = f"SELECT MAX(announcement_date) FROM {table_name}"
            latest_result = await self._execute_query(latest_query, {})
            latest_date = latest_result[0][0] if latest_result and latest_result[0][0] else None
            
            # 获取股票代码数量
            stocks_query = f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}"
            stocks_result = await self._execute_query(stocks_query, {})
            unique_stocks = stocks_result[0][0] if stocks_result else 0
            
            return {
                'table_name': table_name,
                'total_records': total_count,
                'unique_stocks': unique_stocks,
                'latest_date': latest_date.isoformat() if latest_date else None
            }
            
        except Exception as e:
            self.logger.error(f"获取表统计信息失败: {e}")
            return {}
    
    def get_connection_info(self) -> Dict[str, Any]:
        """获取连接信息"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'connection_pool_size': len(self._connection_pool),
            'max_connections': self._max_connections
        }