"""
股票发现管理器
从ClickHouse多个财技事件表中发现活跃股票代码
"""

import asyncio
import logging
from typing import Set, Dict, List, Optional, Any
from datetime import datetime, timedelta

try:
    import aiohttp
    import asyncio
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False

logger = logging.getLogger(__name__)


class StockDiscoveryManager:
    """
    智能股票发现管理器
    
    从ClickHouse的8个财技事件表中提取活跃股票代码：
    - consolidation_data (合股)
    - convertible_bond_data (可转债)
    - general_offer_data (全购)
    - halfnew_data (半新股)
    - ipo_data (IPO)
    - placing_data (配股)
    - rights_data (供股)
    - split_data (拆股)
    """
    
    def __init__(self, clickhouse_config: dict):
        """
        初始化股票发现管理器
        
        Args:
            clickhouse_config: ClickHouse连接配置
        """
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError("需要安装aiohttp: pip install aiohttp")
        
        self.clickhouse_config = clickhouse_config
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 财技事件表列表
        self.table_names = [
            'consolidation_data',    # 合股
            'convertible_bond_data', # 可转债
            'general_offer_data',    # 全购
            'halfnew_data',         # 半新股
            'ipo_data',             # IPO
            'placing_data',         # 配股
            'rights_data',          # 供股
            'split_data'            # 拆股
        ]
        
        # 从配置读取参数
        self.host = clickhouse_config.get('host', 'localhost')
        self.port = clickhouse_config.get('port', 8124)
        self.database = clickhouse_config.get('database', 'hkex_analysis')
        self.user = clickhouse_config.get('user', 'root')
        self.password = clickhouse_config.get('password', '123456')
        self.batch_size = clickhouse_config.get('batch_size', 1000)
        self.connection_timeout = clickhouse_config.get('connection_timeout', 30)
        self.query_timeout = clickhouse_config.get('query_timeout', 60)
        self.max_retries = clickhouse_config.get('max_retries', 3)
        
        # 缓存
        self.current_stocks: Set[str] = set()
        self.previous_stocks: Set[str] = set()
        self.last_discovery_time: Optional[datetime] = None
        self.table_stats: Dict[str, int] = {}
        
        logger.info(f"股票发现管理器初始化完成")
        logger.info(f"  ClickHouse: {self.host}:{self.port}/{self.database}")
        logger.info(f"  用户: {self.user}")
        logger.info(f"  监听表数量: {len(self.table_names)}")
        logger.info(f"  批次大小: {self.batch_size}")
        logger.info(f"  查询超时: {self.query_timeout}秒")
    
    async def initialize(self) -> bool:
        """初始化ClickHouse HTTP连接"""
        try:
            # 创建HTTP会话
            timeout = aiohttp.ClientTimeout(total=self.connection_timeout)
            auth = aiohttp.BasicAuth(self.user, self.password)
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                auth=auth
            )
            
            # 测试连接
            result = await self._execute_query("SELECT 1")
            if result and len(result) > 0 and result[0] == ['1']:
                logger.info("ClickHouse HTTP连接测试成功")
                
                # 验证数据库和表
                await self._verify_database_and_tables()
                return True
            else:
                logger.error("ClickHouse连接测试失败")
                return False
                
        except Exception as e:
            logger.error(f"ClickHouse连接初始化失败: {e}")
            return False
    
    async def _execute_query(self, query: str) -> List[List[str]]:
        """执行ClickHouse查询"""
        if not self.session:
            raise Exception("ClickHouse会话未初始化")
        
        url = f"http://{self.host}:{self.port}/"
        
        try:
            async with self.session.post(url, data=query) as response:
                if response.status == 200:
                    text = await response.text()
                    if not text.strip():
                        return []
                    
                    # 解析返回结果
                    lines = text.strip().split('\n')
                    result = []
                    for line in lines:
                        if line.strip():
                            # ClickHouse默认使用Tab分隔
                            result.append(line.split('\t'))
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"ClickHouse查询失败 (状态码: {response.status}): {error_text}")
                    return []
                    
        except Exception as e:
            logger.error(f"执行ClickHouse查询失败: {e}")
            return []
    
    async def _verify_database_and_tables(self):
        """验证数据库和表是否存在"""
        try:
            # 检查数据库
            databases = await self._execute_query("SHOW DATABASES")
            db_list = [db[0] for db in databases]
            
            if self.database not in db_list:
                logger.warning(f"数据库 {self.database} 不存在")
                return
            
            logger.info(f"数据库 {self.database} 验证成功")
            
            # 检查表
            tables = await self._execute_query(f"SHOW TABLES FROM {self.database}")
            table_list = [table[0] for table in tables]
            
            existing_tables = []
            missing_tables = []
            
            for table_name in self.table_names:
                if table_name in table_list:
                    existing_tables.append(table_name)
                else:
                    missing_tables.append(table_name)
            
            logger.info(f"找到 {len(existing_tables)} 个目标表: {existing_tables}")
            
            if missing_tables:
                logger.warning(f"缺失 {len(missing_tables)} 个目标表: {missing_tables}")
            
            # 更新实际可用的表列表
            self.table_names = existing_tables
            
        except Exception as e:
            logger.error(f"验证数据库和表时出错: {e}")
    
    async def discover_all_stocks(self) -> Set[str]:
        """
        从所有表中发现股票代码
        
        Returns:
            标准化的股票代码集合
        """
        if not self.session:
            logger.error("ClickHouse会话未初始化")
            return set()
        
        start_time = datetime.now()
        all_stocks = set()
        self.table_stats = {}
        
        logger.info(f"开始从 {len(self.table_names)} 个表中发现股票代码")
        
        for table_name in self.table_names:
            try:
                stocks = await self._extract_stocks_from_table(table_name)
                all_stocks.update(stocks)
                self.table_stats[table_name] = len(stocks)
                logger.info(f"从表 {table_name} 提取到 {len(stocks)} 只股票")
                
            except Exception as e:
                logger.error(f"从表 {table_name} 提取股票失败: {e}")
                self.table_stats[table_name] = 0
        
        # 标准化股票代码
        normalized_stocks = self.normalize_stock_codes(all_stocks)
        
        # 更新缓存
        self.previous_stocks = self.current_stocks.copy()
        self.current_stocks = normalized_stocks
        self.last_discovery_time = datetime.now()
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"股票发现完成，耗时 {processing_time:.2f}秒")
        logger.info(f"总共发现 {len(normalized_stocks)} 只不重复股票")
        logger.info(f"表统计: {self.table_stats}")
        
        return normalized_stocks
    
    async def _extract_stocks_from_table(self, table_name: str) -> Set[str]:
        """
        从指定表提取股票代码
        
        Args:
            table_name: 表名
            
        Returns:
            股票代码集合
        """
        query = f"""
        SELECT DISTINCT stock_code 
        FROM {self.database}.{table_name} 
        WHERE stock_code IS NOT NULL 
        AND stock_code != ''
        AND stock_code != 'NULL'
        """
        
        try:
            result = await self._execute_query(query)
            stocks = {row[0] for row in result if row and row[0]}  # 过滤空值
            
            logger.debug(f"表 {table_name} 查询结果: {len(stocks)} 条记录")
            return stocks
            
        except Exception as e:
            logger.error(f"查询表 {table_name} 失败: {e}")
            return set()
    
    def normalize_stock_codes(self, raw_codes: Set[str]) -> Set[str]:
        """
        标准化股票代码：去.hk后缀，转为5位数格式
        
        Args:
            raw_codes: 原始股票代码集合
            
        Returns:
            标准化的股票代码集合
        """
        normalized = set()
        
        for code in raw_codes:
            if not code:
                continue
                
            try:
                # 去除.hk后缀（大小写不敏感）
                clean_code = code.upper().replace('.HK', '')
                
                # 去除其他可能的后缀
                clean_code = clean_code.replace('.SZ', '').replace('.SS', '')
                
                # 转为5位数格式（仅限数字代码）
                if clean_code.isdigit():
                    normalized_code = clean_code.zfill(5)
                    normalized.add(normalized_code)
                    
                    if normalized_code != clean_code:
                        logger.debug(f"股票代码标准化: {code} -> {normalized_code}")
                else:
                    # 非数字代码（如特殊代码）也保留
                    normalized.add(clean_code)
                    logger.debug(f"保留非数字股票代码: {code} -> {clean_code}")
                    
            except Exception as e:
                logger.warning(f"处理股票代码时出错: {code}, 错误: {e}")
                continue
        
        return normalized
    
    async def detect_stock_changes(self) -> Dict[str, Set[str]]:
        """
        检测股票变化
        
        Returns:
            包含新增、删除、总股票的字典
        """
        current_stocks = await self.discover_all_stocks()
        
        if self.previous_stocks:
            new_stocks = current_stocks - self.previous_stocks
            removed_stocks = self.previous_stocks - current_stocks
            
            if new_stocks:
                logger.info(f"发现新增股票: {len(new_stocks)} 只")
                logger.debug(f"新增股票代码: {sorted(list(new_stocks))}")
            
            if removed_stocks:
                logger.info(f"发现删除股票: {len(removed_stocks)} 只")
                logger.debug(f"删除股票代码: {sorted(list(removed_stocks))}")
            
            if not new_stocks and not removed_stocks:
                logger.info("股票列表无变化")
        else:
            new_stocks = current_stocks
            removed_stocks = set()
            logger.info(f"首次发现股票: {len(new_stocks)} 只")
        
        return {
            'new_stocks': new_stocks,
            'removed_stocks': removed_stocks,
            'total_stocks': current_stocks,
            'unchanged_stocks': current_stocks & self.previous_stocks if self.previous_stocks else set()
        }
    
    async def get_recent_active_stocks(self, days: int = 30) -> Set[str]:
        """
        获取最近活跃的股票（在指定天数内有事件的股票）
        
        Args:
            days: 天数
            
        Returns:
            活跃股票代码集合
        """
        if not self.session:
            logger.error("ClickHouse会话未初始化")
            return set()
        
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
        
        active_stocks = set()
        
        for table_name in self.table_names:
            try:
                # 假设每个表都有日期字段（可能是announcement_date或类似字段）
                date_fields = ['announcement_date', 'created_at', 'date', 'effective_date']
                
                for date_field in date_fields:
                    try:
                        query = f"""
                        SELECT DISTINCT stock_code 
                        FROM {self.database}.{table_name} 
                        WHERE {date_field} >= '{cutoff_date_str}'
                        AND stock_code IS NOT NULL 
                        AND stock_code != ''
                        """
                        
                        result = await self._execute_query(query)
                        table_stocks = {row[0] for row in result if row and row[0]}
                        
                        if table_stocks:
                            active_stocks.update(table_stocks)
                            logger.debug(f"表 {table_name}.{date_field}: {len(table_stocks)} 只活跃股票")
                            break  # 找到有效的日期字段就跳出
                            
                    except Exception:
                        continue  # 尝试下一个日期字段
                        
            except Exception as e:
                logger.error(f"查询表 {table_name} 的活跃股票失败: {e}")
        
        normalized_active = self.normalize_stock_codes(active_stocks)
        logger.info(f"最近 {days} 天内活跃股票: {len(normalized_active)} 只")
        
        return normalized_active
    
    def get_discovery_stats(self) -> Dict[str, Any]:
        """获取发现统计信息"""
        return {
            "last_discovery_time": self.last_discovery_time.isoformat() if self.last_discovery_time else None,
            "current_stocks_count": len(self.current_stocks),
            "previous_stocks_count": len(self.previous_stocks),
            "table_stats": self.table_stats,
            "monitored_tables": self.table_names,
            "connection_info": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.user
            }
        }
    
    async def close(self):
        """关闭ClickHouse连接"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("ClickHouse连接已关闭")


# 测试函数
async def test_stock_discovery():
    """测试股票发现功能"""
    # 测试配置
    config = {
        "host": "localhost",
        "port": 8124,
        "database": "hkex_analysis",
        "user": "root",
        "password": "123456",
        "batch_size": 1000,
        "connection_timeout": 30,
        "query_timeout": 60,
        "max_retries": 3
    }
    
    discovery = StockDiscoveryManager(config)
    
    try:
        # 初始化
        if await discovery.initialize():
            print("✅ ClickHouse连接成功")
            
            # 发现股票
            stocks = await discovery.discover_all_stocks()
            print(f"📊 发现股票总数: {len(stocks)}")
            
            if stocks:
                sample_stocks = sorted(list(stocks))[:10]
                print(f"📋 股票样例: {sample_stocks}")
            
            # 获取统计信息
            stats = discovery.get_discovery_stats()
            print(f"📈 发现统计: {stats}")
            
        else:
            print("❌ ClickHouse连接失败")
            
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await discovery.close()


if __name__ == "__main__":
    asyncio.run(test_stock_discovery())
