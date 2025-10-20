"""
ClickHouse股票发现集成

这个模块负责从ClickHouse财技事件表中提取股票代码，
实现多级股票发现系统的第1级（主要来源）。

主要功能：
- 连接ClickHouse数据库
- 查询8个财技事件表
- 提取和标准化股票代码
- 提供连接状态监控

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
from typing import Dict, List, Set, Tuple, Optional, Any
import time
from datetime import datetime, timedelta
import aiohttp
import json

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseStockExtractor:
    """
    ClickHouse股票提取器
    
    从ClickHouse财技事件数据库中提取股票代码的专用类。
    """
    
    def __init__(self, host: str = "localhost", port: int = 8124, 
                 user: str = "root", password: str = "123456", 
                 database: str = "hkex_analysis", timeout: int = 30):
        """
        初始化ClickHouse股票提取器
        
        Args:
            host: ClickHouse主机地址
            port: ClickHouse HTTP端口 
            user: 用户名
            password: 密码
            database: 数据库名
            timeout: 连接超时时间
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        
        # 基础URL
        self.base_url = f"http://{host}:{port}"
        
        # 财技事件表定义
        self.financial_tables = {
            'consolidation_events': {
                'description': '合股事件',
                'stock_field': 'stock_code',
                'priority': 1
            },
            'ipo_events': {
                'description': 'IPO事件', 
                'stock_field': 'stock_code',
                'priority': 2
            },
            'rights_events': {
                'description': '供股事件',
                'stock_field': 'stock_code', 
                'priority': 3
            },
            'dividend_events': {
                'description': '分红事件',
                'stock_field': 'stock_code',
                'priority': 4
            },
            'split_events': {
                'description': '拆股事件',
                'stock_field': 'stock_code',
                'priority': 5
            },
            'buyback_events': {
                'description': '回购事件',
                'stock_field': 'stock_code',
                'priority': 6
            },
            'privatization_events': {
                'description': '私有化事件',
                'stock_field': 'stock_code',
                'priority': 7
            },
            'general_events': {
                'description': '一般事件',
                'stock_field': 'stock_code',
                'priority': 8
            }
        }
        
        # 连接状态
        self.connection_status = {
            'is_connected': False,
            'last_check': None,
            'error_message': None,
            'server_info': None
        }

    async def check_connection(self) -> bool:
        """
        检查ClickHouse连接状态
        
        Returns:
            bool: 连接是否成功
        """
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                # 简单的ping查询
                query = "SELECT 1 as ping"
                
                async with session.get(
                    f"{self.base_url}/",
                    params={
                        'query': query,
                        'user': self.user,
                        'password': self.password,
                        'database': self.database
                    }
                ) as response:
                    if response.status == 200:
                        result = await response.text()
                        
                        # 获取服务器信息
                        server_info = await self._get_server_info(session)
                        
                        self.connection_status.update({
                            'is_connected': True,
                            'last_check': datetime.now(),
                            'error_message': None,
                            'server_info': server_info
                        })
                        
                        logger.info(f"ClickHouse连接成功: {self.host}:{self.port}")
                        return True
                    else:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
                        
        except Exception as e:
            error_msg = f"ClickHouse连接失败: {e}"
            logger.error(error_msg)
            
            self.connection_status.update({
                'is_connected': False,
                'last_check': datetime.now(),
                'error_message': error_msg,
                'server_info': None
            })
            
            return False

    async def _get_server_info(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """
        获取ClickHouse服务器信息
        
        Args:
            session: HTTP会话
            
        Returns:
            Dict[str, Any]: 服务器信息
        """
        try:
            # 获取版本信息
            version_query = "SELECT version() as version"
            async with session.get(
                f"{self.base_url}/",
                params={
                    'query': version_query,
                    'user': self.user,
                    'password': self.password,
                    'database': self.database
                }
            ) as response:
                version = (await response.text()).strip()
                
            # 获取数据库列表
            databases_query = "SHOW DATABASES"
            async with session.get(
                f"{self.base_url}/",
                params={
                    'query': databases_query,
                    'user': self.user,
                    'password': self.password
                }
            ) as response:
                databases_text = await response.text()
                databases = [db.strip() for db in databases_text.strip().split('\n')]
                
            return {
                'version': version,
                'databases': databases,
                'target_database_exists': self.database in databases
            }
            
        except Exception as e:
            logger.warning(f"获取服务器信息失败: {e}")
            return {'error': str(e)}

    async def check_tables_exist(self) -> Dict[str, bool]:
        """
        检查财技事件表是否存在
        
        Returns:
            Dict[str, bool]: 表存在状态映射
        """
        if not self.connection_status['is_connected']:
            await self.check_connection()
        
        table_status = {}
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                # 获取数据库中的表列表
                query = f"SHOW TABLES FROM {self.database}"
                
                async with session.get(
                    f"{self.base_url}/",
                    params={
                        'query': query,
                        'user': self.user,
                        'password': self.password
                    }
                ) as response:
                    if response.status == 200:
                        tables_text = await response.text()
                        existing_tables = {table.strip() for table in tables_text.strip().split('\n') if table.strip()}
                        
                        # 检查每个财技表是否存在
                        for table_name in self.financial_tables.keys():
                            table_status[table_name] = table_name in existing_tables
                            
                        logger.info(f"表检查完成: {sum(table_status.values())}/{len(table_status)} 个表存在")
                    else:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
                        
        except Exception as e:
            logger.error(f"检查表存在性失败: {e}")
            # 假设所有表都不存在
            for table_name in self.financial_tables.keys():
                table_status[table_name] = False
        
        return table_status

    async def extract_stocks_from_table(self, table_name: str, limit: int = 10000) -> Set[str]:
        """
        从单个表中提取股票代码
        
        Args:
            table_name: 表名
            limit: 查询限制
            
        Returns:
            Set[str]: 股票代码集合
        """
        if table_name not in self.financial_tables:
            logger.warning(f"未知的财技表: {table_name}")
            return set()
        
        table_config = self.financial_tables[table_name]
        stock_field = table_config['stock_field']
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                # 构建查询
                query = f"""
                SELECT DISTINCT {stock_field} as stock_code
                FROM {self.database}.{table_name}
                WHERE {stock_field} IS NOT NULL 
                AND {stock_field} != ''
                AND length({stock_field}) BETWEEN 4 AND 10
                ORDER BY {stock_field}
                LIMIT {limit}
                """
                
                async with session.get(
                    f"{self.base_url}/",
                    params={
                        'query': query,
                        'user': self.user,
                        'password': self.password
                    }
                ) as response:
                    if response.status == 200:
                        result_text = await response.text()
                        stocks = {line.strip() for line in result_text.strip().split('\n') if line.strip()}
                        
                        logger.debug(f"从 {table_name} 提取到 {len(stocks)} 只股票")
                        return stocks
                    else:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
                        
        except Exception as e:
            logger.error(f"从表 {table_name} 提取股票失败: {e}")
            return set()

    async def extract_optimized_stocks(self, max_stocks: int = 100, priority_mode: str = "mixed") -> Dict[str, Any]:
        """
        优化的股票提取方法，基于实际数据表结构和活跃度
        
        Args:
            max_stocks: 最大股票数量
            priority_mode: 优先模式 ('active' - 最活跃, 'recent' - 最新, 'mixed' - 混合)
            
        Returns:
            Dict[str, Any]: 提取结果包含优选股票列表
        """
        start_time = time.time()
        
        # 检查连接
        if not await self.check_connection():
            return {
                'success': False,
                'error': self.connection_status['error_message'],
                'stocks': set(),
                'extraction_time': time.time() - start_time
            }
        
        logger.info(f"🎯 开始优化股票提取 (模式: {priority_mode}, 最大数量: {max_stocks})")
        
        # 实际存在的财技数据表
        financial_data_tables = {
            'placing_data': {'weight': 3, 'date_field': 'announcement_date'},      # 配售 - 最高权重
            'halfnew_data': {'weight': 2, 'date_field': 'listing_date'},           # 新股 - 高权重  
            'rights_data': {'weight': 2, 'date_field': 'announcement_date'},       # 供股 - 高权重
            'general_offer_data': {'weight': 2, 'date_field': 'announcement_date'}, # 全购 - 高权重
            'convertible_bond_data': {'weight': 1, 'date_field': 'event_date'},    # 可转债 - 中权重
            'consolidation_data': {'weight': 1, 'date_field': 'announcement_date'}, # 合股 - 中权重
            'split_data': {'weight': 1, 'date_field': 'announcement_date'},        # 拆股 - 中权重
        }
        
        all_stocks = set()
        stock_scores = {}
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                
                for table_name, config in financial_data_tables.items():
                    logger.info(f"  📊 分析表: {table_name}")
                    
                    try:
                        # 获取该表的股票和活跃度
                        if priority_mode == "recent":
                            # 优先最新活动的股票
                            query = f"""
                            SELECT stock_code, COUNT(*) as event_count, MAX({config['date_field']}) as latest_date
                            FROM hkex_analysis.{table_name}
                            WHERE {config['date_field']} LIKE '2025%'
                            GROUP BY stock_code
                            ORDER BY latest_date DESC, event_count DESC
                            LIMIT {max_stocks}
                            """
                        elif priority_mode == "active":
                            # 优先最活跃的股票
                            query = f"""
                            SELECT stock_code, COUNT(*) as event_count
                            FROM hkex_analysis.{table_name}
                            GROUP BY stock_code
                            ORDER BY event_count DESC
                            LIMIT {max_stocks}
                            """
                        else:  # mixed mode
                            # 混合模式：活跃度 + 最新性
                            query = f"""
                            SELECT 
                                stock_code, 
                                COUNT(*) as event_count,
                                MAX({config['date_field']}) as latest_date,
                                SUM(CASE WHEN {config['date_field']} LIKE '2025%' THEN 1 ELSE 0 END) as recent_count
                            FROM hkex_analysis.{table_name}
                            GROUP BY stock_code
                            ORDER BY recent_count DESC, event_count DESC, latest_date DESC
                            LIMIT {max_stocks}
                            """
                        
                        async with session.get(
                            f"{self.base_url}/",
                            params={
                                'query': query,
                                'user': self.user,
                                'password': self.password,
                                'database': self.database
                            }
                        ) as response:
                            if response.status == 200:
                                result_text = await response.text()
                                lines = [line.strip() for line in result_text.strip().split('\n') if line.strip()]
                                
                                for line in lines:
                                    parts = line.split('\t')
                                    if len(parts) >= 2:
                                        stock_code = parts[0].strip()
                                        event_count = int(parts[1]) if parts[1].isdigit() else 1
                                        
                                        # 计算股票得分
                                        score = event_count * config['weight']
                                        if priority_mode == "mixed" and len(parts) >= 4:
                                            recent_count = int(parts[3]) if parts[3].isdigit() else 0
                                            score += recent_count * 5  # 2025年活动额外加分
                                        
                                        all_stocks.add(stock_code)
                                        stock_scores[stock_code] = stock_scores.get(stock_code, 0) + score
                                
                                logger.info(f"    ✅ 从 {table_name} 获取 {len(lines)} 个股票")
                            else:
                                logger.warning(f"    ⚠️ 查询 {table_name} 失败: HTTP {response.status}")
                                
                    except Exception as e:
                        logger.warning(f"    ❌ 处理 {table_name} 失败: {e}")
                        continue
                
                # 按得分排序选择最优股票
                sorted_stocks = sorted(stock_scores.items(), key=lambda x: x[1], reverse=True)
                selected_stocks = [stock for stock, score in sorted_stocks[:max_stocks]]
                
                logger.info(f"🎯 优化股票提取完成:")
                logger.info(f"  📈 总发现股票: {len(all_stocks)} 个")
                logger.info(f"  🏆 优选股票: {len(selected_stocks)} 个")
                logger.info(f"  📊 TOP10股票: {selected_stocks[:10]}")
                
                return {
                    'success': True,
                    'stocks': set(selected_stocks),
                    'all_stocks': all_stocks,
                    'stock_scores': dict(sorted_stocks[:max_stocks]),
                    'extraction_time': time.time() - start_time,
                    'priority_mode': priority_mode,
                    'total_discovered': len(all_stocks),
                    'selected_count': len(selected_stocks)
                }
                
        except Exception as e:
            logger.error(f"优化股票提取失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'stocks': set(),
                'extraction_time': time.time() - start_time
            }

    async def extract_all_stocks(self, include_metadata: bool = True) -> Dict[str, Any]:
        """
        从所有财技表中提取股票代码
        
        Args:
            include_metadata: 是否包含详细元数据
            
        Returns:
            Dict[str, Any]: 提取结果
        """
        start_time = time.time()
        
        # 检查连接
        if not await self.check_connection():
            return {
                'success': False,
                'error': self.connection_status['error_message'],
                'stocks': set(),
                'extraction_time': time.time() - start_time
            }
        
        # 检查表存在性
        table_status = await self.check_tables_exist()
        existing_tables = [table for table, exists in table_status.items() if exists]
        
        if not existing_tables:
            return {
                'success': False,
                'error': '没有找到任何财技事件表',
                'stocks': set(),
                'table_status': table_status,
                'extraction_time': time.time() - start_time
            }
        
        # 并发提取股票
        logger.info(f"开始从 {len(existing_tables)} 个财技表提取股票...")
        
        all_stocks = set()
        table_results = {}
        
        # 创建并发任务
        tasks = []
        for table_name in existing_tables:
            task = asyncio.create_task(self.extract_stocks_from_table(table_name))
            tasks.append((table_name, task))
        
        # 等待所有任务完成
        for table_name, task in tasks:
            try:
                table_stocks = await task
                all_stocks.update(table_stocks)
                table_results[table_name] = {
                    'count': len(table_stocks),
                    'description': self.financial_tables[table_name]['description'],
                    'stocks': list(table_stocks) if include_metadata else len(table_stocks)
                }
            except Exception as e:
                logger.error(f"处理表 {table_name} 失败: {e}")
                table_results[table_name] = {
                    'count': 0,
                    'error': str(e),
                    'description': self.financial_tables[table_name]['description']
                }
        
        extraction_time = time.time() - start_time
        
        result = {
            'success': True,
            'stocks': all_stocks,
            'total_count': len(all_stocks),
            'extraction_time': extraction_time,
            'tables_processed': len(existing_tables),
            'table_status': table_status,
            'connection_info': self.connection_status.copy()
        }
        
        if include_metadata:
            result['table_results'] = table_results
            result['extraction_summary'] = {
                'successful_tables': sum(1 for r in table_results.values() if 'error' not in r),
                'failed_tables': sum(1 for r in table_results.values() if 'error' in r),
                'avg_extraction_time': extraction_time / len(existing_tables) if existing_tables else 0
            }
        
        logger.info(f"ClickHouse股票提取完成: {len(all_stocks)} 只股票，耗时 {extraction_time:.2f} 秒")
        
        return result

    async def get_table_statistics(self) -> Dict[str, Any]:
        """
        获取财技表统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        if not await self.check_connection():
            return {
                'success': False,
                'error': self.connection_status['error_message']
            }
        
        stats = {
            'success': True,
            'database': self.database,
            'check_time': datetime.now(),
            'tables': {}
        }
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                for table_name, config in self.financial_tables.items():
                    try:
                        # 获取表的行数和股票数
                        queries = {
                            'total_rows': f"SELECT count() as cnt FROM {self.database}.{table_name}",
                            'unique_stocks': f"SELECT count(DISTINCT {config['stock_field']}) as cnt FROM {self.database}.{table_name} WHERE {config['stock_field']} IS NOT NULL AND {config['stock_field']} != ''"
                        }
                        
                        table_stats = {
                            'description': config['description'],
                            'priority': config['priority']
                        }
                        
                        for stat_name, query in queries.items():
                            async with session.get(
                                f"{self.base_url}/",
                                params={
                                    'query': query,
                                    'user': self.user,
                                    'password': self.password
                                }
                            ) as response:
                                if response.status == 200:
                                    count = int((await response.text()).strip())
                                    table_stats[stat_name] = count
                                else:
                                    table_stats[stat_name] = 0
                        
                        stats['tables'][table_name] = table_stats
                        
                    except Exception as e:
                        logger.warning(f"获取表 {table_name} 统计失败: {e}")
                        stats['tables'][table_name] = {
                            'description': config['description'],
                            'error': str(e)
                        }
                        
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            stats['success'] = False
            stats['error'] = str(e)
        
        return stats

    def get_connection_status(self) -> Dict[str, Any]:
        """
        获取连接状态信息
        
        Returns:
            Dict[str, Any]: 连接状态
        """
        return self.connection_status.copy()


# 便捷函数
async def extract_stocks_from_clickhouse(host: str = "localhost", port: int = 8124,
                                       user: str = "root", password: str = "123456",
                                       database: str = "hkex_analysis") -> Set[str]:
    """
    便捷的ClickHouse股票提取函数
    
    Args:
        host: ClickHouse主机
        port: 端口
        user: 用户名
        password: 密码
        database: 数据库名
        
    Returns:
        Set[str]: 股票代码集合
    """
    extractor = ClickHouseStockExtractor(host, port, user, password, database)
    result = await extractor.extract_all_stocks(include_metadata=False)
    
    if result['success']:
        return result['stocks']
    else:
        logger.error(f"ClickHouse股票提取失败: {result.get('error')}")
        return set()


async def test_clickhouse_connection(host: str = "localhost", port: int = 8124,
                                   user: str = "root", password: str = "123456",
                                   database: str = "hkex_analysis") -> bool:
    """
    测试ClickHouse连接
    
    Args:
        host: ClickHouse主机
        port: 端口
        user: 用户名
        password: 密码
        database: 数据库名
        
    Returns:
        bool: 连接是否成功
    """
    extractor = ClickHouseStockExtractor(host, port, user, password, database)
    return await extractor.check_connection()


if __name__ == "__main__":
    # 测试模块
    async def test_clickhouse_extractor():
        """测试ClickHouse股票提取器"""
        
        print("\n" + "="*70)
        print("🔍 ClickHouse股票提取器测试")
        print("="*70)
        
        # 创建提取器
        extractor = ClickHouseStockExtractor()
        
        # 测试连接
        print("📡 测试ClickHouse连接...")
        connection_ok = await extractor.check_connection()
        print(f"连接状态: {'✅ 成功' if connection_ok else '❌ 失败'}")
        
        if connection_ok:
            # 检查表存在性
            print("\n📋 检查财技事件表...")
            table_status = await extractor.check_tables_exist()
            for table, exists in table_status.items():
                status = "✅ 存在" if exists else "❌ 不存在"
                description = extractor.financial_tables[table]['description']
                print(f"  {table} ({description}): {status}")
            
            # 获取统计信息
            print("\n📊 获取表统计信息...")
            stats = await extractor.get_table_statistics()
            if stats['success']:
                for table, table_stats in stats['tables'].items():
                    if 'error' not in table_stats:
                        print(f"  {table}: {table_stats['total_rows']} 行, {table_stats['unique_stocks']} 只股票")
            
            # 提取股票
            print("\n🔍 提取股票代码...")
            result = await extractor.extract_all_stocks()
            
            if result['success']:
                print(f"✅ 提取成功: {result['total_count']} 只股票")
                print(f"⏱️ 耗时: {result['extraction_time']:.2f} 秒")
                print(f"📋 处理表数: {result['tables_processed']}")
                
                # 显示部分股票
                stocks_list = sorted(list(result['stocks']))
                if stocks_list:
                    print(f"📈 股票样本: {stocks_list[:10]}...")
            else:
                print(f"❌ 提取失败: {result.get('error')}")
        else:
            print("\n⚠️ 无法连接到ClickHouse，跳过详细测试")
            print("这是正常的，因为ClickHouse可能未启动或配置不同")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_clickhouse_extractor())
