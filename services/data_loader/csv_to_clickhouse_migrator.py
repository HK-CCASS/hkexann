"""
CSV到ClickHouse数据迁移工具
将现有CSV数据批量导入到ClickHouse数据库
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from clickhouse_driver import Client
    from clickhouse_driver.errors import Error as ClickHouseError
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    Client = None
    ClickHouseError = Exception

from .csv_parser import CSVEventParser, EventRecord


logger = logging.getLogger(__name__)


class CSVToClickHouseMigrator:
    """CSV数据到ClickHouse的迁移器"""
    
    def __init__(self, host: str = 'localhost', port: int = 9000, 
                 database: str = 'default', user: str = 'default', 
                 password: str = ''):
        """
        初始化迁移器
        
        Args:
            host: ClickHouse服务器主机
            port: ClickHouse服务器端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError("需要安装clickhouse-driver: pip install clickhouse-driver")
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self._client: Optional[Client] = None
        self.csv_parser = CSVEventParser()
        
        # 表名映射
        self.table_mapping = {
            'rights_issue': 'rights_issue',
            'placement': 'placement',
            'consolidation': 'consolidation',
            'stock_split': 'stock_split'
        }
        
        logger.info(f"ClickHouse迁移器初始化，连接: {host}:{port}/{database}")
    
    async def initialize(self) -> bool:
        """初始化数据库连接"""
        try:
            self._client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            # 测试连接
            result = await self._execute_query("SELECT 1")
            logger.info("ClickHouse连接测试成功")
            return True
            
        except Exception as e:
            logger.error(f"ClickHouse连接失败: {e}")
            return False
    
    async def create_tables(self, schema_file: Path) -> bool:
        """创建数据库表结构"""
        try:
            if not schema_file.exists():
                logger.error(f"Schema文件不存在: {schema_file}")
                return False
            
            # 读取SQL脚本
            schema_sql = schema_file.read_text(encoding='utf-8')
            
            # 分割并执行每个SQL语句
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    await self._execute_query(statement)
                    logger.info(f"执行SQL语句成功: {statement[:50]}...")
            
            logger.info("数据库表结构创建完成")
            return True
            
        except Exception as e:
            logger.error(f"创建表结构失败: {e}")
            return False
    
    async def migrate_csv_data(self, csv_directory: Path, 
                             batch_size: int = 1000) -> Dict[str, int]:
        """
        迁移CSV数据到ClickHouse
        
        Args:
            csv_directory: CSV文件目录
            batch_size: 批量插入大小
            
        Returns:
            各表的插入记录数
        """
        migration_results = {}
        
        try:
            # 解析所有CSV数据
            logger.info("开始解析CSV数据...")
            all_events = self.csv_parser.parse_all_event_csvs(csv_directory)
            
            # 逐个事件类型迁移
            for event_type, records in all_events.items():
                if not records:
                    logger.info(f"跳过空数据: {event_type}")
                    migration_results[event_type] = 0
                    continue
                
                logger.info(f"开始迁移 {event_type} 数据，总数: {len(records)}")
                
                # 批量插入
                inserted_count = await self._batch_insert(event_type, records, batch_size)
                migration_results[event_type] = inserted_count
                
                logger.info(f"{event_type} 迁移完成，插入: {inserted_count}/{len(records)} 条记录")
            
            # 统计总数
            total_inserted = sum(migration_results.values())
            logger.info(f"数据迁移完成，总插入: {total_inserted} 条记录")
            
            return migration_results
            
        except Exception as e:
            logger.error(f"数据迁移失败: {e}")
            raise
    
    async def _batch_insert(self, event_type: str, records: List[EventRecord], 
                          batch_size: int) -> int:
        """批量插入数据"""
        table_name = self.table_mapping[event_type]
        inserted_count = 0
        
        # 分批处理
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                # 转换为插入格式
                insert_data = []
                for record in batch:
                    row_data = self._convert_record_to_clickhouse_format(record)
                    insert_data.append(row_data)
                
                # 构建插入语句
                columns = self._get_table_columns(event_type)
                placeholders = ', '.join(['%s'] * len(columns))
                
                insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)}) 
                VALUES ({placeholders})
                """
                
                # 批量插入
                await self._execute_query(insert_query, insert_data)
                inserted_count += len(batch)
                
                logger.debug(f"批次 {i//batch_size + 1} 插入成功: {len(batch)} 条记录")
                
            except Exception as e:
                logger.error(f"批次插入失败: {e}")
                # 尝试单条插入以找出问题记录
                for record in batch:
                    try:
                        row_data = self._convert_record_to_clickhouse_format(record)
                        await self._execute_query(insert_query, [row_data])
                        inserted_count += 1
                    except Exception as single_error:
                        logger.error(f"单条记录插入失败: {single_error}, 记录: {record}")
        
        return inserted_count
    
    def _convert_record_to_clickhouse_format(self, record: EventRecord) -> tuple:
        """将记录对象转换为ClickHouse插入格式"""
        now = datetime.now()
        
        if hasattr(record, 'rights_price'):
            # 供股记录
            return (
                record.stock_code or '',
                record.company_name or '',
                record.announcement_date,
                record.status or '',
                record.rights_price,
                record.rights_price_premium or '',
                record.rights_ratio or '',
                record.current_price,
                record.current_price_premium or '',
                record.stock_adjustment or '',
                record.underwriter or '',
                record.ex_rights_date,
                record.rights_trading_start,
                record.rights_trading_end,
                record.final_payment_date,
                record.result_announcement_date,
                record.allotment_date,
                now,
                now
            )
            
        elif hasattr(record, 'placement_price'):
            # 配股记录
            return (
                record.stock_code or '',
                record.company_name or '',
                record.announcement_date,
                record.status or '',
                record.placement_price,
                record.placement_price_premium or '',
                record.new_shares_ratio or '',
                record.current_price,
                record.current_price_premium or '',
                record.placement_agent or '',
                record.authorization_method or '',
                record.placement_method or '',
                record.completion_date,
                now,
                now
            )
            
        elif hasattr(record, 'consolidation_ratio'):
            # 合股记录
            return (
                record.stock_code or '',
                record.company_name or '',
                record.announcement_date,
                record.status or '',
                record.temporary_counter or '',
                record.consolidation_ratio or '',
                record.effective_date,
                record.other_corporate_actions or '',
                now,
                now
            )
            
        elif hasattr(record, 'split_ratio'):
            # 拆股记录
            return (
                record.stock_code or '',
                record.company_name or '',
                record.announcement_date,
                record.status or '',
                record.temporary_counter or '',
                record.split_ratio or '',
                record.effective_date,
                record.other_corporate_actions or '',
                now,
                now
            )
        
        raise ValueError(f"未知的记录类型: {type(record)}")
    
    def _get_table_columns(self, event_type: str) -> List[str]:
        """获取表的列名列表"""
        column_mappings = {
            'rights_issue': [
                'stock_code', 'company_name', 'announcement_date', 'status',
                'rights_price', 'rights_price_premium', 'rights_ratio',
                'current_price', 'current_price_premium', 'stock_adjustment',
                'underwriter', 'ex_rights_date', 'rights_trading_start',
                'rights_trading_end', 'final_payment_date', 'result_announcement_date',
                'allotment_date', 'created_at', 'updated_at'
            ],
            'placement': [
                'stock_code', 'company_name', 'announcement_date', 'status',
                'placement_price', 'placement_price_premium', 'new_shares_ratio',
                'current_price', 'current_price_premium', 'placement_agent',
                'authorization_method', 'placement_method', 'completion_date',
                'created_at', 'updated_at'
            ],
            'consolidation': [
                'stock_code', 'company_name', 'announcement_date', 'status',
                'temporary_counter', 'consolidation_ratio', 'effective_date',
                'other_corporate_actions', 'created_at', 'updated_at'
            ],
            'stock_split': [
                'stock_code', 'company_name', 'announcement_date', 'status',
                'temporary_counter', 'split_ratio', 'effective_date',
                'other_corporate_actions', 'created_at', 'updated_at'
            ]
        }
        
        return column_mappings.get(event_type, [])
    
    async def _execute_query(self, query: str, params: Any = None) -> List[Any]:
        """执行查询"""
        def sync_execute():
            if params is None:
                return self._client.execute(query)
            else:
                return self._client.execute(query, params)
        
        return await asyncio.get_event_loop().run_in_executor(None, sync_execute)
    
    async def verify_migration(self) -> Dict[str, Any]:
        """验证迁移结果"""
        results = {}
        
        try:
            for event_type, table_name in self.table_mapping.items():
                # 获取记录总数
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                count_result = await self._execute_query(count_query)
                total_count = count_result[0][0] if count_result else 0
                
                # 获取日期范围
                date_range_query = f"""
                SELECT MIN(announcement_date), MAX(announcement_date) 
                FROM {table_name}
                """
                date_result = await self._execute_query(date_range_query)
                min_date, max_date = date_result[0] if date_result else (None, None)
                
                # 获取唯一股票数
                unique_stocks_query = f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}"
                unique_result = await self._execute_query(unique_stocks_query)
                unique_stocks = unique_result[0][0] if unique_result else 0
                
                results[event_type] = {
                    'table_name': table_name,
                    'total_records': total_count,
                    'date_range': {
                        'min_date': min_date.isoformat() if min_date else None,
                        'max_date': max_date.isoformat() if max_date else None
                    },
                    'unique_stocks': unique_stocks
                }
            
            logger.info(f"迁移验证完成: {results}")
            return results
            
        except Exception as e:
            logger.error(f"验证迁移结果失败: {e}")
            raise
    
    async def close(self):
        """关闭连接"""
        if self._client:
            try:
                if hasattr(self._client, 'disconnect'):
                    self._client.disconnect()
            except Exception as e:
                logger.warning(f"关闭ClickHouse连接失败: {e}")
            self._client = None


# 便捷函数用于直接执行迁移
async def migrate_csv_to_clickhouse(csv_directory: str, 
                                  clickhouse_config: Dict[str, Any],
                                  schema_file: str = None,
                                  batch_size: int = 1000) -> Dict[str, Any]:
    """
    一键执行CSV到ClickHouse的完整迁移流程
    
    Args:
        csv_directory: CSV文件目录路径
        clickhouse_config: ClickHouse连接配置
        schema_file: 数据库schema文件路径
        batch_size: 批量插入大小
        
    Returns:
        迁移结果统计
    """
    migrator = CSVToClickHouseMigrator(**clickhouse_config)
    
    try:
        # 初始化连接
        if not await migrator.initialize():
            raise Exception("无法连接到ClickHouse数据库")
        
        # 创建表结构
        if schema_file:
            schema_path = Path(schema_file)
            if not await migrator.create_tables(schema_path):
                raise Exception("创建表结构失败")
        
        # 执行数据迁移
        csv_path = Path(csv_directory)
        migration_results = await migrator.migrate_csv_data(csv_path, batch_size)
        
        # 验证迁移结果
        verification_results = await migrator.verify_migration()
        
        return {
            'migration_results': migration_results,
            'verification_results': verification_results
        }
        
    finally:
        await migrator.close()