"""
CSV事件数据源实现
基于现有CSV解析器，实现FinancialEventSource接口
"""

import time
from pathlib import Path
from typing import Dict, List, Any, Optional

from .data_source import FinancialEventSource, DataSourceConfig, QueryResult, DataSourceType
from .csv_parser import CSVEventParser, EventRecord


class CSVEventSource(FinancialEventSource):
    """
    CSV文件数据源实现
    使用现有的CSVEventParser作为底层解析引擎
    """
    
    def __init__(self, config: DataSourceConfig, csv_directory: str):
        """
        初始化CSV数据源
        
        Args:
            config: 数据源配置
            csv_directory: CSV文件目录路径
        """
        super().__init__(config)
        
        self.csv_directory = Path(csv_directory)
        self.csv_parser = CSVEventParser()
        self._data_cache: Dict[str, List[EventRecord]] = {}
        self._last_load_time: Optional[float] = None
        
        # CSV文件路径映射
        self.csv_file_mapping = {
            'rights_issue': '供股.csv',
            'placement': '配股.csv',
            'consolidation': '合股.csv',
            'stock_split': '拆股.csv'
        }
        
        self.logger.info(f"CSV数据源初始化，目录: {self.csv_directory}")
    
    async def initialize(self) -> bool:
        """初始化数据源连接"""
        try:
            # 检查CSV目录是否存在
            if not self.csv_directory.exists():
                self.logger.error(f"CSV目录不存在: {self.csv_directory}")
                return False
            
            # 检查关键CSV文件是否存在
            missing_files = []
            for event_type, filename in self.csv_file_mapping.items():
                csv_path = self.csv_directory / filename
                if not csv_path.exists():
                    missing_files.append(filename)
            
            if missing_files:
                self.logger.warning(f"部分CSV文件缺失: {missing_files}")
            else:
                self.logger.info("所有CSV文件都已存在")
            
            # 预加载数据
            await self._load_all_data()
            
            self.logger.info("CSV数据源初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV数据源初始化失败: {e}")
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
                self.logger.debug(f"返回缓存数据: {event_type}")
                return cached_result
            
            # 检查事件类型是否有效
            if event_type not in self.csv_file_mapping:
                error_msg = f"不支持的事件类型: {event_type}"
                self.logger.error(error_msg)
                self.health.failed_requests += 1
                return QueryResult(
                    success=False,
                    data=[],
                    source_name=self.name,
                    error_message=error_msg
                )
            
            # 确保数据已加载
            if event_type not in self._data_cache:
                await self._load_event_data(event_type)
            
            # 获取原始数据
            raw_data = self._data_cache.get(event_type, [])
            
            # 应用过滤条件
            filtered_data = self._apply_filters(raw_data, stock_code, date_range)
            
            # 应用数量限制
            if limit and limit > 0:
                filtered_data = filtered_data[:limit]
            
            # 转换为字典格式
            result_data = [self._record_to_dict(record) for record in filtered_data]
            
            query_time_ms = (time.time() - start_time) * 1000
            
            result = QueryResult(
                success=True,
                data=result_data,
                total_count=len(result_data),
                source_name=self.name,
                query_time_ms=query_time_ms
            )
            
            # 缓存结果
            self._set_cache(cache_key, result)
            
            self.logger.debug(f"获取 {event_type} 事件成功，数量: {len(result_data)}")
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
        available_types = []
        
        for event_type, filename in self.csv_file_mapping.items():
            csv_path = self.csv_directory / filename
            if csv_path.exists():
                available_types.append(event_type)
        
        return available_types
    
    async def get_stock_codes(self, event_type: str) -> List[str]:
        """获取指定事件类型的所有股票代码"""
        try:
            # 确保数据已加载
            if event_type not in self._data_cache:
                await self._load_event_data(event_type)
            
            records = self._data_cache.get(event_type, [])
            stock_codes = list(set(record.stock_code for record in records if record.stock_code))
            stock_codes.sort()
            
            return stock_codes
            
        except Exception as e:
            self.logger.error(f"获取股票代码失败: {e}")
            return []
    
    async def _perform_health_check(self):
        """执行健康检查"""
        # 检查CSV目录是否存在
        if not self.csv_directory.exists():
            raise Exception(f"CSV目录不存在: {self.csv_directory}")
        
        # 检查是否至少有一个CSV文件存在
        has_csv_files = False
        for filename in self.csv_file_mapping.values():
            csv_path = self.csv_directory / filename
            if csv_path.exists():
                has_csv_files = True
                break
        
        if not has_csv_files:
            raise Exception("没有找到任何CSV文件")
    
    async def _cleanup_resources(self):
        """清理资源"""
        self._data_cache.clear()
        self._last_load_time = None
    
    async def _load_all_data(self):
        """加载所有CSV数据"""
        try:
            start_time = time.time()
            
            # 使用现有的解析器加载所有数据
            all_events = self.csv_parser.parse_all_event_csvs(self.csv_directory)
            
            # 更新缓存
            for event_type, records in all_events.items():
                self._data_cache[event_type] = records
            
            self._last_load_time = time.time()
            load_time = (time.time() - start_time) * 1000
            
            total_records = sum(len(records) for records in all_events.values())
            self.logger.info(f"CSV数据加载完成，总记录: {total_records}，耗时: {load_time:.2f}ms")
            
        except Exception as e:
            self.logger.error(f"CSV数据加载失败: {e}")
            raise
    
    async def _load_event_data(self, event_type: str):
        """加载特定事件类型的数据"""
        try:
            csv_filename = self.csv_file_mapping[event_type]
            csv_path = self.csv_directory / csv_filename
            
            if not csv_path.exists():
                self.logger.warning(f"CSV文件不存在: {csv_path}")
                self._data_cache[event_type] = []
                return
            
            # 根据事件类型调用对应的解析方法
            if event_type == 'rights_issue':
                records = self.csv_parser.parse_rights_issue_csv(csv_path)
            elif event_type == 'placement':
                records = self.csv_parser.parse_placement_csv(csv_path)
            elif event_type == 'consolidation':
                records = self.csv_parser.parse_consolidation_csv(csv_path)
            elif event_type == 'stock_split':
                records = self.csv_parser.parse_stock_split_csv(csv_path)
            else:
                records = []
            
            self._data_cache[event_type] = records
            self.logger.info(f"加载 {event_type} 数据完成，记录数: {len(records)}")
            
        except Exception as e:
            self.logger.error(f"加载 {event_type} 数据失败: {e}")
            self._data_cache[event_type] = []
    
    def _apply_filters(self, records: List[EventRecord], stock_code: Optional[str] = None,
                      date_range: Optional[tuple] = None) -> List[EventRecord]:
        """应用过滤条件"""
        filtered_records = records
        
        # 股票代码过滤
        if stock_code:
            filtered_records = [r for r in filtered_records if r.stock_code == stock_code]
        
        # 日期范围过滤
        if date_range and len(date_range) == 2:
            start_date, end_date = date_range
            filtered_records = [
                r for r in filtered_records 
                if r.announcement_date and start_date <= r.announcement_date <= end_date
            ]
        
        return filtered_records
    
    def _record_to_dict(self, record: EventRecord) -> Dict[str, Any]:
        """将记录对象转换为字典格式"""
        result = {
            'stock_code': record.stock_code,
            'company_name': record.company_name,
            'announcement_date': record.announcement_date.isoformat() if record.announcement_date else None,
            'status': record.status,
        }
        
        # 添加类型特定字段
        if hasattr(record, 'rights_price'):
            # 供股记录
            result.update({
                'rights_price': record.rights_price,
                'rights_price_premium': record.rights_price_premium,
                'rights_ratio': record.rights_ratio,
                'current_price': record.current_price,
                'current_price_premium': record.current_price_premium,
                'stock_adjustment': record.stock_adjustment,
                'underwriter': record.underwriter,
                'ex_rights_date': record.ex_rights_date.isoformat() if record.ex_rights_date else None,
                'rights_trading_start': record.rights_trading_start.isoformat() if record.rights_trading_start else None,
                'rights_trading_end': record.rights_trading_end.isoformat() if record.rights_trading_end else None,
                'final_payment_date': record.final_payment_date.isoformat() if record.final_payment_date else None,
                'result_announcement_date': record.result_announcement_date.isoformat() if record.result_announcement_date else None,
                'allotment_date': record.allotment_date.isoformat() if record.allotment_date else None
            })
            
        elif hasattr(record, 'placement_price'):
            # 配股记录
            result.update({
                'placement_price': record.placement_price,
                'placement_price_premium': record.placement_price_premium,
                'new_shares_ratio': record.new_shares_ratio,
                'current_price': record.current_price,
                'current_price_premium': record.current_price_premium,
                'placement_agent': record.placement_agent,
                'authorization_method': record.authorization_method,
                'placement_method': record.placement_method,
                'completion_date': record.completion_date.isoformat() if record.completion_date else None
            })
            
        elif hasattr(record, 'consolidation_ratio'):
            # 合股记录
            result.update({
                'temporary_counter': record.temporary_counter,
                'consolidation_ratio': record.consolidation_ratio,
                'effective_date': record.effective_date.isoformat() if record.effective_date else None,
                'other_corporate_actions': record.other_corporate_actions
            })
            
        elif hasattr(record, 'split_ratio'):
            # 拆股记录
            result.update({
                'temporary_counter': record.temporary_counter,
                'split_ratio': record.split_ratio,
                'effective_date': record.effective_date.isoformat() if record.effective_date else None,
                'other_corporate_actions': record.other_corporate_actions
            })
        
        return result
    
    def get_data_summary(self) -> Dict[str, Any]:
        """获取数据摘要"""
        summary = {
            'csv_directory': str(self.csv_directory),
            'last_load_time': self._last_load_time,
            'event_counts': {}
        }
        
        for event_type, records in self._data_cache.items():
            summary['event_counts'][event_type] = len(records)
        
        summary['total_records'] = sum(summary['event_counts'].values())
        
        return summary