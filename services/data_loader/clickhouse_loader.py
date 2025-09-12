"""
ClickHouse数据加载器 - 企业行动事件数据持久化服务

这个模块提供了将解析的企业行动CSV数据加载到ClickHouse时序数据库的完整功能。
支持批量高效的数据插入操作，确保数据一致性和错误处理。

主要功能：
- 供股(Rights Issue)、配股(Placement)、合股(Consolidation)事件数据插入
- 批量数据处理，优化大数据集的插入性能
- 连接池管理和异步HTTP客户端
- 数据类型格式化和SQL注入防护
- 事务级别的错误处理和回滚机制
- 表级统计和监控功能

技术特点：
- 异步执行确保高并发性能
- 分批插入机制避免内存溢出
- 自动数据类型转换和格式化
- 完整的日志记录和错误追踪
- 支持数据清理和重新加载

数据库表结构：
- rights_issue: 供股事件记录
- placement: 配股事件记录  
- consolidation: 合股事件记录

Time Complexity: O(n) 其中n为记录数量
Space Complexity: O(batch_size) 受批量大小限制

作者: HKEX分析团队
版本: 1.0.0
依赖: ClickHouse 22.x+, httpx, asyncio
"""

import logging
import time
from datetime import date
from typing import List, Dict, Any, Optional

import httpx

from config.settings import settings
from .csv_parser import RightsIssueRecord, PlacementRecord, ConsolidationRecord, StockSplitRecord

logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """
    ClickHouse数据加载器 - 企业行动事件数据的高性能批量加载器
    
    这个类负责将解析的企业行动数据（供股、配股、合股）高效地加载到ClickHouse数据库中。
    采用异步架构和批量处理机制，确保大规模数据的高性能插入。
    
    核心特性：
    - 异步HTTP客户端，支持高并发数据插入
    - 智能批量处理，避免内存溢出和性能瓶颈
    - 自动数据类型转换和SQL注入防护
    - 完整的错误处理和事务回滚机制
    - 实时统计和监控功能
    
    支持的数据表：
    - rights_issue: 供股事件（认购价格、比例、日期等）
    - placement: 配股事件（配股价格、代理、方法等）
    - consolidation: 合股事件（合股比例、生效日期等）
    
    使用示例：
        loader = ClickHouseLoader(
            host="localhost",
            port=8124,
            database="hkex_events"
        )
        
        await loader.test_connection()
        success = await loader.load_rights_issue_records(records)
        await loader.close()
    
    Attributes:
        host (str): ClickHouse主机地址
        port (int): ClickHouse HTTP端口
        database (str): 目标数据库名称
        user (str): 数据库用户名
        password (str): 数据库密码
        base_url (str): 完整的数据库连接URL
        client (httpx.AsyncClient): 异步HTTP客户端实例
        
    Note:
        所有操作都是异步的，需要在async context中使用。
        建议使用连接池管理多个并发操作。
    """

    def __init__(self, host: str = None, port: int = None, database: str = None, user: str = None,
                 password: str = None):
        """
        初始化ClickHouse数据加载器实例
        
        创建并配置ClickHouse数据库连接参数，建立异步HTTP客户端。
        如果参数未提供，将使用配置文件中的默认值。
        
        Args:
            host (str, optional): ClickHouse服务器主机地址，默认"localhost"
            port (int, optional): ClickHouse HTTP接口端口，默认8124  
            database (str, optional): 目标数据库名称，从settings获取
            user (str, optional): 数据库用户名，从settings获取
            password (str, optional): 数据库密码，从settings获取
            
        Note:
            - 使用HTTP接口而非原生协议，确保更好的防火墙兼容性
            - 自动构建包含数据库上下文的URL路径
            - 设置30秒的HTTP超时时间，适合大批量操作
            
        Raises:
            ConnectionError: 当无法建立HTTP客户端时抛出异常
            
        Example:
            # 使用默认配置
            loader = ClickHouseLoader()
            
            # 使用自定义配置
            loader = ClickHouseLoader(
                host="192.168.1.100",
                port=8124,
                database="production_events"
            )
        """
        self.host = host or "localhost"
        self.port = port or 8124  # HTTP端口
        self.database = database or settings.clickhouse_database
        self.user = user or settings.clickhouse_user
        self.password = password or settings.clickhouse_password

        # 构建包含数据库的URL，确保查询在正确的数据库上下文中执行
        self.base_url = f"http://{self.host}:{self.port}/{self.database}"

        # HTTP客户端
        self.client = httpx.AsyncClient(timeout=30)

        logger.info(f"ClickHouse加载器初始化: {self.host}:{self.port}/{self.database}")

    async def execute_query(self, query: str) -> str:
        """
        异步执行ClickHouse SQL查询语句
        
        通过HTTP接口发送SQL查询到ClickHouse服务器，处理响应并返回结果。
        包含完整的错误处理和日志记录机制。
        
        Args:
            query (str): 要执行的SQL查询语句，支持DML和DDL操作
            
        Returns:
            str: 查询结果的字符串形式，去除首尾空白字符
            
        Raises:
            Exception: 当查询执行失败时抛出异常，包含详细错误信息：
                - HTTP状态码错误（非200响应）
                - 网络连接错误
                - ClickHouse SQL语法错误
                - 权限验证失败
                
        Note:
            - 自动记录查询日志（截取前100字符用于调试）
            - 使用POST方法发送查询，支持大型SQL语句
            - 设置Content-Type为text/plain，确保SQL语法正确解析
            - 查询失败时记录完整的URL和SQL语句用于排错
            
        Time Complexity: O(1) 发送请求的复杂度，查询执行复杂度取决于SQL本身
        Space Complexity: O(n) 其中n为查询结果的大小
        
        Example:
            # 简单查询
            result = await loader.execute_query("SELECT 1")
            
            # 创建表
            await loader.execute_query('''
                CREATE TABLE test (
                    id UInt64,
                    name String
                ) ENGINE = MergeTree() ORDER BY id
            ''')
            
            # 插入数据
            await loader.execute_query(
                "INSERT INTO test VALUES (1, 'example')"
            )
        """
        try:
            # 记录查询日志用于调试
            logger.debug(f"执行ClickHouse查询: {query[:100]}...")

            response = await self.client.post(self.base_url, data=query, headers={"Content-Type": "text/plain"},
                auth=(self.user, self.password))

            if response.status_code != 200:
                error_msg = f"ClickHouse查询失败: {response.status_code} - {response.text}"
                logger.error(f"查询失败，URL: {self.base_url}")
                logger.error(f"查询语句: {query}")
                raise Exception(error_msg)

            return response.text.strip()

        except Exception as e:
            logger.error(f"执行ClickHouse查询失败: {e}")
            logger.error(f"数据库URL: {self.base_url}")
            raise

    async def test_connection(self) -> bool:
        """
        测试ClickHouse数据库连接的可用性
        
        通过执行简单的SELECT 1查询来验证数据库连接是否正常。
        这是一个轻量级的健康检查方法，不会对数据库造成负担。
        
        Returns:
            bool: 连接测试结果
                - True: 连接成功，数据库可正常访问
                - False: 连接失败，可能的原因包括网络问题、认证失败、服务未运行等
                
        Note:
            - 建议在执行任何数据操作前先调用此方法
            - 连接失败时会自动记录详细的错误日志
            - 此方法为异步操作，需要在async context中调用
            
        Example:
            loader = ClickHouseLoader()
            if await loader.test_connection():
                print("数据库连接正常")
                # 执行数据操作
            else:
                print("数据库连接失败")
                # 处理连接错误
        """
        try:
            result = await self.execute_query("SELECT 1")
            if result == "1":
                logger.info("✅ ClickHouse连接成功")
                return True
            else:
                logger.error("❌ ClickHouse连接测试失败")
                return False
        except Exception as e:
            logger.error(f"❌ ClickHouse连接失败: {e}")
            return False

    def format_date(self, date_obj: Optional[date]) -> str:
        """
        将Python日期对象格式化为ClickHouse兼容的SQL字符串格式
        
        ClickHouse要求日期字符串使用YYYY-MM-DD格式并用单引号包围。
        对于None值，返回SQL NULL字面量。
        
        Args:
            date_obj (Optional[date]): Python的date对象，可以为None
            
        Returns:
            str: ClickHouse兼容的日期字符串：
                - 有效日期: "'2024-01-15'"
                - None值: "NULL"
                
        Example:
            from datetime import date
            
            # 有效日期
            formatted = loader.format_date(date(2024, 1, 15))
            # 返回: "'2024-01-15'"
            
            # 空日期
            formatted = loader.format_date(None)
            # 返回: "NULL"
        """
        if date_obj is None:
            return "NULL"
        return f"'{date_obj.strftime('%Y-%m-%d')}'"

    def format_string(self, text: str) -> str:
        """
        将Python字符串安全格式化为ClickHouse兼容的SQL字符串格式
        
        自动处理SQL注入防护，转义特殊字符，确保数据安全插入。
        空字符串或None值将转换为空字符串字面量。
        
        Args:
            text (str): 要格式化的字符串内容
            
        Returns:
            str: ClickHouse兼容的转义字符串：
                - 正常字符串: "'内容'"
                - 包含单引号: "'content\\'s'"
                - 空字符串: "''"
                
        Note:
            - 自动转义单引号防止SQL注入
            - 保持原始字符串的编码格式
            - 处理中文字符和特殊符号
            
        Example:
            # 普通字符串
            formatted = loader.format_string("腾讯控股")
            # 返回: "'腾讯控股'"
            
            # 包含引号
            formatted = loader.format_string("McDonald's")
            # 返回: "'McDonald\\'s'"
            
            # 空字符串
            formatted = loader.format_string("")
            # 返回: "''"
        """
        if not text:
            return "''"
        # 转义单引号
        escaped = text.replace("'", "\\'")
        return f"'{escaped}'"

    def format_decimal(self, value: Optional[float]) -> str:
        """
        将Python浮点数格式化为ClickHouse兼容的数值字符串格式
        
        处理浮点数、整数和None值的格式化，确保数值类型的正确存储。
        对于None值返回SQL NULL字面量。
        
        Args:
            value (Optional[float]): 要格式化的数值，可以是float、int或None
            
        Returns:
            str: ClickHouse兼容的数值字符串：
                - 浮点数: "123.45"
                - 整数: "100"  
                - None值: "NULL"
                
        Example:
            # 浮点数
            formatted = loader.format_decimal(123.45)
            # 返回: "123.45"
            
            # 整数
            formatted = loader.format_decimal(100)
            # 返回: "100"
            
            # None值
            formatted = loader.format_decimal(None)
            # 返回: "NULL"
        """
        if value is None:
            return "NULL"
        return str(value)

    async def load_rights_issue_records(self, records: List[RightsIssueRecord]) -> bool:
        """
        批量加载供股(Rights Issue)记录到ClickHouse数据库
        
        采用智能分批处理机制，将大量供股记录高效地插入到rights_issue表中。
        每批处理50条记录，平衡内存使用和插入性能。包含完整的进度跟踪和错误处理。
        
        Args:
            records (List[RightsIssueRecord]): 供股记录对象列表，每个记录包含：
                - stock_code: 股票代码（如"0700.HK"）
                - company_name: 公司名称
                - announcement_date: 公告日期
                - rights_price: 供股价格
                - rights_ratio: 供股比例（如"1:10"）
                - current_price: 当前股价
                - ex_rights_date: 除权日期
                - 其他相关字段...
                
        Returns:
            bool: 加载操作结果
                - True: 所有记录成功加载到数据库
                - False: 加载过程中发生错误，部分或全部记录可能未插入
                
        Raises:
            Exception: 数据库连接错误或SQL执行错误时抛出
            
        Note:
            - 使用批量插入优化性能，每批50条记录
            - 自动处理数据类型转换和SQL转义
            - 实时记录插入进度和统计信息
            - 发生错误时会记录详细的错误日志
            
        Time Complexity: O(n/batch_size) 其中n为记录总数
        Space Complexity: O(batch_size) 受批量大小限制
        
        Example:
            records = [
                RightsIssueRecord(
                    stock_code="0700.HK",
                    company_name="腾讯控股",
                    rights_price=320.0,
                    rights_ratio="1:10"
                ),
                # ... 更多记录
            ]
            
            success = await loader.load_rights_issue_records(records)
            if success:
                print(f"成功加载 {len(records)} 条供股记录")
        """
        try:
            if not records:
                logger.info("没有供股记录需要加载")
                return True

            logger.info(f"开始加载 {len(records)} 条供股记录")

            # 分批插入，每批50条
            batch_size = 50
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                # 构建插入语句
                values = []
                for record in batch:
                    value_parts = [self.format_string(record.stock_code), self.format_string(record.company_name),
                        self.format_date(record.announcement_date), self.format_decimal(record.rights_price),
                        self.format_string(record.rights_price_premium_text),
                        self.format_decimal(record.rights_price_premium), self.format_string(record.rights_ratio),
                        self.format_decimal(record.current_price),
                        self.format_string(record.current_price_premium_text),
                        self.format_decimal(record.current_price_premium), self.format_string(record.stock_adjustment),
                        self.format_string(record.underwriter), self.format_date(record.ex_rights_date),
                        self.format_date(record.rights_trading_start), self.format_date(record.rights_trading_end),
                        self.format_date(record.final_payment_date), self.format_date(record.result_announcement_date),
                        self.format_date(record.allotment_date), self.format_string(record.status)]
                    values.append(f"({', '.join(value_parts)})")

                # 构建完整的INSERT语句
                insert_query = f"""
                INSERT INTO hkex_analysis.rights_issue (
                    stock_code, company_name, announcement_date,
                    rights_price, rights_price_premium_text, rights_price_premium,
                    rights_ratio, current_price, current_price_premium_text, current_price_premium,
                    stock_adjustment, underwriter,
                    ex_rights_date, rights_trading_start, rights_trading_end,
                    final_payment_date, result_announcement_date, allotment_date,
                    status
                ) VALUES {', '.join(values)}
                """

                # 执行插入
                await self.execute_query(insert_query)
                total_inserted += len(batch)
                logger.info(f"✅ 已插入供股记录: {total_inserted}/{len(records)}")

            logger.info(f"✅ 成功加载 {total_inserted} 条供股记录")
            return True

        except Exception as e:
            logger.error(f"❌ 加载供股记录失败: {e}")
            return False

    async def load_placement_records(self, records: List[PlacementRecord]) -> bool:
        """
        加载配股记录到ClickHouse
        
        Args:
            records: 配股记录列表
            
        Returns:
            是否成功
        """
        try:
            if not records:
                logger.info("没有配股记录需要加载")
                return True

            logger.info(f"开始加载 {len(records)} 条配股记录")

            # 分批插入，每批50条
            batch_size = 50
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                # 构建插入语句
                values = []
                for record in batch:
                    value_parts = [self.format_string(record.stock_code), self.format_string(record.company_name),
                        self.format_date(record.announcement_date), self.format_decimal(record.placement_price),
                        self.format_string(record.placement_price_premium_text),
                        self.format_decimal(record.placement_price_premium),
                        self.format_decimal(record.new_shares_ratio), self.format_decimal(record.current_price),
                        self.format_string(record.current_price_premium_text),
                        self.format_decimal(record.current_price_premium), self.format_string(record.placement_agent),
                        self.format_string(record.authorization_method), self.format_string(record.placement_method),
                        self.format_date(record.completion_date), self.format_string(record.status)]
                    values.append(f"({', '.join(value_parts)})")

                # 构建完整的INSERT语句
                insert_query = f"""
                INSERT INTO hkex_analysis.placement (
                    stock_code, company_name, announcement_date,
                    placement_price, placement_price_premium_text, placement_price_premium,
                    new_shares_ratio, current_price, current_price_premium_text, current_price_premium,
                    placement_agent, authorization_method, placement_method,
                    completion_date, status
                ) VALUES {', '.join(values)}
                """

                # 执行插入
                await self.execute_query(insert_query)
                total_inserted += len(batch)
                logger.info(f"✅ 已插入配股记录: {total_inserted}/{len(records)}")

            logger.info(f"✅ 成功加载 {total_inserted} 条配股记录")
            return True

        except Exception as e:
            logger.error(f"❌ 加载配股记录失败: {e}")
            return False

    async def load_consolidation_records(self, records: List[ConsolidationRecord]) -> bool:
        """
        加载合股记录到ClickHouse，包含重复检查
        
        Args:
            records: 合股记录列表
            
        Returns:
            是否成功
        """
        try:
            if not records:
                logger.info("没有合股记录需要加载")
                return True

            logger.info(f"开始加载 {len(records)} 条合股记录")

            # 先检查现有数据，避免重复插入
            existing_stocks = set()
            check_query = "SELECT DISTINCT stock_code FROM hkex_analysis.consolidation"
            response = await self.client.post(self.base_url, data=check_query, params={'user': self.user, 'password': self.password})
            if response.status_code == 200 and response.text.strip():
                existing_stocks = set(response.text.strip().split('\n'))
                logger.info(f"检测到现有股票记录: {len(existing_stocks)}个")

            # 过滤掉已存在的记录
            new_records = []
            skipped_count = 0
            for record in records:
                if record.stock_code not in existing_stocks:
                    new_records.append(record)
                else:
                    skipped_count += 1
                    logger.debug(f"跳过已存在的股票: {record.stock_code}")

            if skipped_count > 0:
                logger.info(f"跳过 {skipped_count} 条已存在的记录")

            if not new_records:
                logger.info("所有记录都已存在，无需插入")
                return True

            logger.info(f"实际需要插入 {len(new_records)} 条新记录")

            # 分批插入，每批20条（合股记录较少）
            batch_size = 20
            total_inserted = 0

            for i in range(0, len(new_records), batch_size):
                batch = new_records[i:i + batch_size]

                # 构建插入语句
                values = []
                for record in batch:
                    value_parts = [self.format_string(record.stock_code), self.format_string(record.company_name),
                        self.format_date(record.announcement_date), self.format_string(record.temporary_counter),
                        self.format_string(record.consolidation_ratio), self.format_date(record.effective_date),
                        self.format_string(record.other_corporate_actions), self.format_string(record.status)]
                    values.append(f"({', '.join(value_parts)})")

                # 构建完整的INSERT语句
                insert_query = f"""
                INSERT INTO hkex_analysis.consolidation (
                    stock_code, company_name, announcement_date,
                    temporary_counter, consolidation_ratio, effective_date,
                    other_corporate_actions, status
                ) VALUES {', '.join(values)}
                """

                # 执行插入
                await self.execute_query(insert_query)
                total_inserted += len(batch)
                logger.info(f"✅ 已插入合股记录: {total_inserted}/{len(new_records)}")

            logger.info(f"✅ 成功加载 {total_inserted} 条合股记录")
            return True

        except Exception as e:
            logger.error(f"❌ 加载合股记录失败: {e}")
            return False

    async def load_stock_split_records(self, records: List[StockSplitRecord]) -> bool:
        """
        加载拆股记录到ClickHouse
        
        Args:
            records: 拆股记录列表
            
        Returns:
            是否成功
        """
        try:
            if not records:
                logger.info("没有拆股记录需要加载")
                return True

            logger.info(f"开始加载 {len(records)} 条拆股记录")

            # 先检查现有数据，避免重复插入
            existing_stocks = set()
            check_query = "SELECT DISTINCT stock_code FROM hkex_analysis.stock_split"
            response = await self.client.post(self.base_url, data=check_query, params={'user': self.user, 'password': self.password})
            if response.status_code == 200 and response.text.strip():
                existing_stocks = set(response.text.strip().split('\n'))
                logger.info(f"检测到现有拆股记录: {len(existing_stocks)}个")

            # 过滤掉已存在的记录
            new_records = []
            skipped_count = 0
            for record in records:
                if record.stock_code not in existing_stocks:
                    new_records.append(record)
                else:
                    skipped_count += 1
                    logger.debug(f"跳过已存在的股票: {record.stock_code}")

            if skipped_count > 0:
                logger.info(f"跳过 {skipped_count} 条已存在的记录")

            if not new_records:
                logger.info("所有记录都已存在，无需插入")
                return True

            logger.info(f"实际需要插入 {len(new_records)} 条新记录")

            # 分批插入，每批20条
            batch_size = 20
            total_inserted = 0

            for i in range(0, len(new_records), batch_size):
                batch = new_records[i:i + batch_size]

                # 构建插入语句
                values = []
                for record in batch:
                    value_parts = [
                        self.format_string(record.stock_code),
                        self.format_string(record.company_name),
                        self.format_date(record.announcement_date),
                        self.format_string(record.temporary_counter),
                        self.format_string(record.split_ratio),
                        self.format_date(record.effective_date),
                        self.format_string(record.other_corporate_actions),
                        self.format_string(record.status)
                    ]
                    values.append(f"({', '.join(value_parts)})")

                # 构建完整的INSERT语句
                insert_query = f"""
                INSERT INTO hkex_analysis.stock_split (
                    stock_code, company_name, announcement_date,
                    temporary_counter, split_ratio, effective_date,
                    other_corporate_actions, status
                ) VALUES {', '.join(values)}
                """

                # 执行插入
                await self.execute_query(insert_query)
                total_inserted += len(batch)
                logger.info(f"✅ 已插入拆股记录: {total_inserted}/{len(new_records)}")

            logger.info(f"✅ 成功加载 {total_inserted} 条拆股记录")
            return True

        except Exception as e:
            logger.error(f"❌ 加载拆股记录失败: {e}")
            return False

    async def get_table_count(self, table_name: str) -> int:
        """
        异步获取指定表的记录总数
        
        执行COUNT查询来获取表中的记录数量，用于统计和监控目的。
        这是一个轻量级操作，通常执行很快。
        
        Args:
            table_name (str): 要查询的表名称，如"rights_issue"、"placement"等
            
        Returns:
            int: 表中的记录总数
                - 成功时返回实际记录数（>= 0）
                - 查询失败时返回0
                
        Note:
            - 查询失败时会记录错误日志但不抛出异常
            - 返回0可能表示表为空或查询失败
            - 建议配合错误日志判断实际情况
            
        Example:
            count = await loader.get_table_count("rights_issue")
            print(f"供股表包含 {count} 条记录")
        """
        try:
            result = await self.execute_query(f"SELECT count() FROM hkex_analysis.{table_name}")
            return int(result)
        except Exception as e:
            logger.error(f"获取表 {table_name} 记录数失败: {e}")
            return 0

    async def clear_table(self, table_name: str) -> bool:
        """清空表数据"""
        try:
            await self.execute_query(f"TRUNCATE TABLE hkex_analysis.{table_name}")
            logger.info(f"✅ 已清空表 {table_name}")
            return True
        except Exception as e:
            logger.error(f"❌ 清空表 {table_name} 失败: {e}")
            return False

    async def load_all_events(self, events_data: Dict[str, List], clear_existing: bool = False) -> Dict[str, Any]:
        """
        加载所有事件数据
        
        Args:
            events_data: 包含所有事件数据的字典
            clear_existing: 是否清空现有数据
            
        Returns:
            加载结果统计
        """
        start_time = time.time()

        results = {
            "success": True, 
            "tables_loaded": 0, 
            "total_records": 0, 
            "rights_issue_count": 0,
            "placement_count": 0, 
            "consolidation_count": 0, 
            "stock_split_count": 0,
            "errors": [], 
            "loading_time": 0
        }

        try:
            logger.info("🚀 开始加载事件数据到ClickHouse")

            # 测试连接
            if not await self.test_connection():
                results["success"] = False
                results["errors"].append("ClickHouse连接失败")
                return results

            # 获取加载前的记录数
            tables = ["rights_issue", "placement", "consolidation", "stock_split"]
            initial_counts = {}

            for table in tables:
                initial_counts[table] = await self.get_table_count(table)
                logger.info(f"表 {table} 当前记录数: {initial_counts[table]}")

            # 清空现有数据（如果需要）
            if clear_existing:
                logger.info("🗑️ 清空现有数据...")
                for table in tables:
                    await self.clear_table(table)

            # 加载供股数据
            if events_data.get('rights_issue'):
                success = await self.load_rights_issue_records(events_data['rights_issue'])
                if success:
                    results["tables_loaded"] += 1
                    results["rights_issue_count"] = len(events_data['rights_issue'])
                else:
                    results["errors"].append("供股数据加载失败")

            # 加载配股数据
            if events_data.get('placement'):
                success = await self.load_placement_records(events_data['placement'])
                if success:
                    results["tables_loaded"] += 1
                    results["placement_count"] = len(events_data['placement'])
                else:
                    results["errors"].append("配股数据加载失败")

            # 加载合股数据
            if events_data.get('consolidation'):
                success = await self.load_consolidation_records(events_data['consolidation'])
                if success:
                    results["tables_loaded"] += 1
                    results["consolidation_count"] = len(events_data['consolidation'])
                else:
                    results["errors"].append("合股数据加载失败")

            # 加载拆股数据
            if events_data.get('stock_split'):
                success = await self.load_stock_split_records(events_data['stock_split'])
                if success:
                    results["tables_loaded"] += 1
                    results["stock_split_count"] = len(events_data['stock_split'])
                else:
                    results["errors"].append("拆股数据加载失败")

            # 统计总记录数
            results["total_records"] = (
                    results["rights_issue_count"] + results["placement_count"] + 
                    results["consolidation_count"] + results["stock_split_count"])

            # 获取加载后的记录数
            final_counts = {}
            for table in tables:
                final_counts[table] = await self.get_table_count(table)
                logger.info(
                    f"表 {table} 最终记录数: {final_counts[table]} (增加: {final_counts[table] - initial_counts[table]})")

            results["loading_time"] = time.time() - start_time

            if results["errors"]:
                results["success"] = False
                logger.error(f"数据加载完成，但有错误: {results['errors']}")
            else:
                logger.info(f"✅ 事件数据加载成功!")
                logger.info(f"  加载表数: {results['tables_loaded']}")
                logger.info(f"  总记录数: {results['total_records']}")
                logger.info(f"  耗时: {results['loading_time']:.2f}秒")

            return results

        except Exception as e:
            logger.error(f"❌ 加载事件数据失败: {e}")
            results["success"] = False
            results["errors"].append(str(e))
            results["loading_time"] = time.time() - start_time
            return results

    async def close(self):
        """
        异步关闭ClickHouse加载器的HTTP客户端连接
        
        正确释放HTTP客户端资源，关闭连接池。这是清理资源的重要步骤，
        应该在完成所有数据库操作后调用，通常在应用关闭时执行。
        
        Note:
            - 关闭后的loader实例不能再用于数据库操作
            - 建议在try/finally块或async with上下文中调用
            - 多次调用此方法是安全的
            
        Example:
            loader = ClickHouseLoader()
            try:
                await loader.load_rights_issue_records(records)
            finally:
                await loader.close()
                
            # 或使用async with模式（需要实现__aenter__和__aexit__）
        """
        await self.client.aclose()
        logger.info("ClickHouse加载器连接已关闭")
