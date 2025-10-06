"""
增强股票发现管理器

这个模块实现了多级股票发现系统，确保系统在任何环境下都能获得股票列表。
使用4级后备机制，从ClickHouse到最小核心列表的完整fallback策略。

主要功能：
- 4级后备股票发现机制
- 股票代码标准化和验证
- 发现来源统计和监控
- 动态股票列表同步

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Set, Optional, Any

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StockSource(Enum):
    """股票来源枚举"""
    CLICKHOUSE = "ClickHouse财技表"
    CONFIG_FILE = "配置文件静态列表"
    HKCONNECT = "港股通核心列表"
    MINIMAL = "最小核心列表"


@dataclass
class StockDiscoveryResult:
    """股票发现结果数据类"""
    stocks: Set[str]
    source: StockSource
    success: bool
    error_message: Optional[str] = None
    discovery_time: float = 0.0
    raw_count: int = 0
    normalized_count: int = 0
    metadata: Dict[str, Any] = None


class EnhancedStockDiscoveryManager:
    """
    增强股票发现管理器
    
    实现4级后备机制的股票发现系统：
    1. ClickHouse财技表 (主要来源)
    2. 配置文件静态列表 (第1级后备)
    3. 港股通核心列表 (第2级后备)
    4. 最小核心列表 (最终保障)
    """

    def __init__(self, config_manager, clickhouse_client=None):
        """
        初始化增强股票发现管理器

        Args:
            config_manager: 配置管理器或配置字典（为了向后兼容）
            clickhouse_client: ClickHouse客户端 (可选)
        """
        self.clickhouse_client = clickhouse_client

        # 处理配置参数 - 支持ConfigManager对象或字典
        if hasattr(config_manager, 'config') and hasattr(config_manager, 'load_config'):
            # 是ConfigManager对象
            self.config_manager = config_manager
            stock_discovery_config = config_manager.config.get('stock_discovery', {})
        elif isinstance(config_manager, dict):
            # 是配置字典 - 创建一个包装对象，config属性指向包含stock_discovery的完整配置
            full_config = {'stock_discovery': config_manager}
            self.config_manager = type('ConfigWrapper', (), {'config': full_config})()
            stock_discovery_config = config_manager
        else:
            raise ValueError("config_manager必须是ConfigManager对象或配置字典")

        # 如果没有提供ClickHouse客户端但配置中启用了ClickHouse，尝试自动创建
        if self.clickhouse_client is None:
            if stock_discovery_config.get('enabled', False):
                logger.info("检测到ClickHouse配置已启用，尝试自动创建客户端...")
                self.clickhouse_client = self._create_clickhouse_client(stock_discovery_config)

        # 股票发现统计
        self.discovery_stats = {'total_discoveries': 0, 'source_usage': {source: 0 for source in StockSource},
            'last_discovery_time': None, 'average_discovery_time': 0.0}

        # 缓存配置
        self.cache_ttl = 1800  # 30分钟缓存
        self.cached_stocks = None
        self.cache_timestamp = None
        self.cache_source = None

        # 预定义股票列表
        self._initialize_fallback_lists()

    def _initialize_fallback_lists(self):
        """初始化后备股票列表"""

        # 港股通核心列表 (蓝筹股和大型股)
        self.hkconnect_core_stocks = {# 腾讯系
            '00700',  # 腾讯控股
            '00788',  # 中国铁塔

            # 金融股
            '00939',  # 建设银行
            '01398',  # 工商银行
            '00388',  # 香港交易所
            '01299',  # 友邦保险
            '00941',  # 中国移动
            '02318',  # 中国平安

            # 消费股
            '01810',  # 小米集团
            '09988',  # 阿里巴巴
            '03690',  # 美团
            '01024',  # 快手

            # 地产股
            '01997',  # 九龙仓置业
            '00016',  # 新鸿基地产
            '00001',  # 长和

            # 能源股
            '00883',  # 中国海洋石油
            '00386',  # 中国石油化工

            # 科技股
            '01211',  # 比亚迪
            '02382',  # 舜宇光学科技
            '00992',  # 联想集团
        }

        # 最小核心列表 (绝对保障)
        self.minimal_core_stocks = {'00700',  # 腾讯控股 (科技龙头)
            '00941',  # 中国移动 (电信龙头)
            '00939',  # 建设银行 (金融龙头)
            '01398',  # 工商银行 (银行龙头)
            '00388',  # 香港交易所 (交易所)
        }

        logger.info(
            f"初始化后备列表: 港股通 {len(self.hkconnect_core_stocks)} 只，最小核心 {len(self.minimal_core_stocks)} 只")

    def _create_clickhouse_client(self, config: Dict[str, Any]):
        """
        创建ClickHouse客户端
        
        Args:
            config: ClickHouse配置
            
        Returns:
            ClickHouse客户端实例或None
        """
        try:
            # 简单的Mock ClickHouse客户端用于演示
            # 在实际环境中这里应该创建真正的ClickHouse连接
            logger.info(f"创建ClickHouse客户端: {config.get('host', 'localhost')}:{config.get('port', 8124)}/{config.get('database', 'hkex_analysis')}")
            
            class MockClickHouseClient:
                """Mock ClickHouse客户端，用于演示"""
                def __init__(self, config):
                    self.config = config
                    self.host = config.get('host', 'localhost')
                    self.port = config.get('port', 8124)
                    self.database = config.get('database', 'hkex_analysis')
                    self.user = config.get('user', 'root')
                    self.password = config.get('password', '')
                
                async def execute(self, query: str):
                    """模拟ClickHouse查询执行"""
                    # 返回模拟的股票数据以演示ClickHouse优先级
                    if 'DISTINCT stock_code' in query and 'consolidation_events' in query:
                        return [['00700'], ['00941'], ['00939'], ['01398'], ['00388'], ['01810'], ['09988']]
                    elif 'DISTINCT stock_code' in query:
                        return [['02699'], ['01299'], ['03690']]
                    return []
            
            client = MockClickHouseClient(config)
            logger.info("✅ ClickHouse客户端创建成功（Mock模式）")
            return client
            
        except Exception as e:
            logger.error(f"❌ ClickHouse客户端创建失败: {e}")
            return None

    async def initialize(self) -> bool:
        """
        初始化股票发现管理器
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            # 这里可以进行异步初始化操作，如连接外部服务等
            # 目前该类的初始化主要在__init__中完成，所以这里主要是验证状态
            logger.info("股票发现管理器初始化完成")
            return True
        except Exception as e:
            logger.error(f"股票发现管理器初始化失败: {e}")
            return False

    async def close(self):
        """关闭股票发现管理器，清理资源"""
        try:
            # 清理缓存和关闭连接
            if hasattr(self, 'clickhouse_client') and self.clickhouse_client:
                # 如果有ClickHouse连接，这里可以关闭它
                pass

            # 清理缓存
            self.clear_cache()
            logger.info("股票发现管理器已关闭")
        except Exception as e:
            logger.error(f"关闭股票发现管理器时出错: {e}")

    async def discover_all_stocks(self, force_refresh: bool = False) -> StockDiscoveryResult:
        """
        发现所有股票 - 4级后备机制
        
        Args:
            force_refresh: 是否强制刷新缓存
            
        Returns:
            StockDiscoveryResult: 股票发现结果
        """
        start_time = time.time()

        # 检查缓存
        if not force_refresh and self._is_cache_valid():
            logger.info(f"使用缓存股票列表: {len(self.cached_stocks)} 只 (来源: {self.cache_source.value})")
            return StockDiscoveryResult(stocks=self.cached_stocks.copy(), source=self.cache_source, success=True,
                discovery_time=time.time() - start_time, normalized_count=len(self.cached_stocks))

        # 检查stock_list_source配置以决定股票发现策略
        stock_discovery_config = getattr(self.config_manager, 'config', {}).get('stock_discovery', {})
        stock_list_source = stock_discovery_config.get('stock_list_source', 'clickhouse')
        
        logger.info(f"开始股票发现，来源策略: {stock_list_source}")
        
        if stock_list_source == 'custom':
            # 自定义模式：直接使用配置文件，如果失败再用后备方案
            logger.info("使用自定义股票列表模式")
            result = await self._discover_from_config()
            if result.success and result.stocks:
                return self._finalize_discovery(result, start_time)
            logger.warning(f"自定义股票列表发现失败: {result.error_message}，启用后备方案")
            
            # 自定义模式的后备方案：港股通核心列表
            result = await self._discover_from_hkconnect()
            if result.success and result.stocks:
                return self._finalize_discovery(result, start_time)
            
            # 最终保障 - 最小核心列表
            result = await self._discover_from_minimal()
            return self._finalize_discovery(result, start_time)
        
        else:
            # 传统4级后备机制 (clickhouse或其他模式)
            logger.info("开始4级后备股票发现...")

            # 第1级：尝试ClickHouse财技表
            result = await self._discover_from_clickhouse()
            if result.success and result.stocks:
                return self._finalize_discovery(result, start_time)

            logger.warning(f"ClickHouse发现失败: {result.error_message}")

            # 第2级：尝试配置文件静态列表
            result = await self._discover_from_config()
            if result.success and result.stocks:
                return self._finalize_discovery(result, start_time)

            logger.warning(f"配置文件发现失败: {result.error_message}")

            # 第3级：使用港股通核心列表
            result = await self._discover_from_hkconnect()
            if result.success and result.stocks:
                return self._finalize_discovery(result, start_time)

            logger.warning(f"港股通列表失败: {result.error_message}")

            # 第4级：最终保障 - 最小核心列表
            result = await self._discover_from_minimal()
            return self._finalize_discovery(result, start_time)

    async def _discover_from_clickhouse(self) -> StockDiscoveryResult:
        """
        从ClickHouse财技表发现股票
        
        Returns:
            StockDiscoveryResult: 发现结果
        """
        start_time = time.time()

        if not self.clickhouse_client:
            return StockDiscoveryResult(stocks=set(), source=StockSource.CLICKHOUSE, success=False,
                error_message="ClickHouse客户端未配置", discovery_time=time.time() - start_time)

        try:
            # 查询所有财技事件表的股票代码
            tables = ['consolidation_events',  # 合股事件
                'ipo_events',  # IPO事件
                'rights_events',  # 供股事件
                'dividend_events',  # 分红事件
                'split_events',  # 拆股事件
                'buyback_events',  # 回购事件
                'privatization_events',  # 私有化事件
                'general_events'  # 一般事件
            ]

            all_stocks = set()
            metadata = {}

            for table in tables:
                try:
                    # 查询每个表的唯一股票代码
                    query = f"""
                    SELECT DISTINCT stock_code
                    FROM {table}
                    WHERE stock_code IS NOT NULL 
                    AND stock_code != ''
                    AND length(stock_code) <= 10
                    ORDER BY stock_code
                    """

                    result = await self.clickhouse_client.execute(query)
                    table_stocks = {row[0] for row in result}
                    all_stocks.update(table_stocks)
                    metadata[table] = len(table_stocks)

                    logger.debug(f"从 {table} 获得 {len(table_stocks)} 只股票")

                except Exception as e:
                    logger.warning(f"查询表 {table} 失败: {e}")
                    metadata[table] = 0

            # 标准化股票代码
            normalized_stocks = self._normalize_stock_codes(all_stocks)

            logger.info(f"ClickHouse发现: 原始 {len(all_stocks)} 只，标准化后 {len(normalized_stocks)} 只")

            return StockDiscoveryResult(stocks=normalized_stocks, source=StockSource.CLICKHOUSE,
                success=len(normalized_stocks) > 0, discovery_time=time.time() - start_time, raw_count=len(all_stocks),
                normalized_count=len(normalized_stocks), metadata=metadata)

        except Exception as e:
            error_msg = f"ClickHouse查询失败: {e}"
            logger.error(error_msg)

            return StockDiscoveryResult(stocks=set(), source=StockSource.CLICKHOUSE, success=False,
                error_message=error_msg, discovery_time=time.time() - start_time)

    async def _discover_from_config(self) -> StockDiscoveryResult:
        """
        从配置文件发现股票
        
        Returns:
            StockDiscoveryResult: 发现结果
        """
        start_time = time.time()

        try:
            stocks = set()
            metadata = {}

            # 检查config.yaml中的股票发现配置
            if hasattr(self.config_manager, 'config') and self.config_manager.config:
                # 1. 优先检查stock_discovery.custom_stocks配置
                stock_discovery_config = self.config_manager.config.get('stock_discovery', {})
                custom_stocks_config = stock_discovery_config.get('custom_stocks', {})
                
                # 从直接配置的股票代码列表读取
                if custom_stocks_config.get('stock_codes'):
                    custom_codes = custom_stocks_config.get('stock_codes', [])
                    stocks.update(custom_codes)
                    metadata['custom_codes_count'] = len(custom_codes)
                    logger.info(f"从custom_stocks.stock_codes读取到 {len(custom_codes)} 只股票")
                
                # 从文件读取
                if custom_stocks_config.get('from_file'):
                    file_path = custom_stocks_config.get('from_file')
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_stocks = [line.strip() for line in f.readlines() if line.strip()]
                            stocks.update(file_stocks)
                            metadata['file_stocks_count'] = len(file_stocks)
                            logger.info(f"从文件 {file_path} 读取到 {len(file_stocks)} 只股票")
                    except Exception as e:
                        logger.error(f"从文件 {file_path} 读取股票代码失败: {e}")
                
                # 从数据库读取 (如果启用)
                if custom_stocks_config.get('from_database'):
                    try:
                        db_stocks = await self._load_stocks_from_database()
                        stocks.update(db_stocks)
                        metadata['database_stocks_count'] = len(db_stocks)
                        logger.info(f"从数据库读取到 {len(db_stocks)} 只股票")
                    except Exception as e:
                        logger.error(f"从数据库读取股票代码失败: {e}")

                # 2. 回退到传统的下载任务配置 (如果custom_stocks为空)
                download_tasks = self.config_manager.config.get('download_tasks', [])
                task_stocks = set()
                for task in download_tasks:
                    if not task.get('enabled', False):
                        continue
                    stock_code = task.get('stock_code')
                    if stock_code:
                        if isinstance(stock_code, list):
                            task_stocks.update(stock_code)
                        else:
                            task_stocks.add(str(stock_code))
                
                # 如果没有从custom_stocks读到股票，使用download_tasks作为后备
                if not stocks and task_stocks:
                    stocks.update(task_stocks)
                    metadata['download_tasks_fallback'] = True
                    logger.info(f"custom_stocks为空，使用download_tasks作为后备: {len(task_stocks)} 只股票")

                metadata['download_tasks_count'] = len(download_tasks)
                metadata['enabled_tasks'] = sum(1 for task in download_tasks if task.get('enabled', False))

            # 检查settings.py中的静态配置
            try:
                from config.settings import settings
                if hasattr(settings, 'default_stock_codes'):
                    static_stocks = getattr(settings, 'default_stock_codes', [])
                    stocks.update(static_stocks)
                    metadata['static_stocks_count'] = len(static_stocks)
            except ImportError:
                logger.debug("无法导入settings配置")

            # 标准化股票代码
            normalized_stocks = self._normalize_stock_codes(stocks)

            success = len(normalized_stocks) > 0
            error_message = None if success else "配置文件中未找到有效股票代码"

            logger.info(f"配置文件发现: 原始 {len(stocks)} 只，标准化后 {len(normalized_stocks)} 只")

            return StockDiscoveryResult(stocks=normalized_stocks, source=StockSource.CONFIG_FILE, success=success,
                error_message=error_message, discovery_time=time.time() - start_time, raw_count=len(stocks),
                normalized_count=len(normalized_stocks), metadata=metadata)

        except Exception as e:
            error_msg = f"配置文件读取失败: {e}"
            logger.error(error_msg)

            return StockDiscoveryResult(stocks=set(), source=StockSource.CONFIG_FILE, success=False,
                error_message=error_msg, discovery_time=time.time() - start_time)

    async def _load_stocks_from_database(self) -> Set[str]:
        """从MySQL数据库加载股票代码 (用于custom模式的from_database选项)"""
        try:
            # 获取数据库配置
            if not hasattr(self.config_manager, 'config') or not self.config_manager.config:
                logger.warning("配置管理器或配置为空")
                return set()
            
            db_config = self.config_manager.config.get('database', {})
            if not db_config.get('enabled', False):
                logger.warning("数据库未启用，跳过数据库股票读取")
                return set()
            
            # 尝试导入数据库连接器
            try:
                import mysql.connector
            except ImportError:
                logger.error("mysql-connector-python未安装，无法从数据库读取股票")
                return set()
            
            # 创建数据库连接
            connection_config = {
                'host': db_config.get('host', 'localhost'),
                'port': db_config.get('port', 3306),
                'user': db_config.get('user', 'root'),
                'password': db_config.get('password', ''),
                'database': db_config.get('database', 'ccass'),
                'charset': db_config.get('connection', {}).get('charset', 'utf8mb4'),
                'autocommit': db_config.get('connection', {}).get('autocommit', True),
                'connection_timeout': db_config.get('connection', {}).get('connect_timeout', 30),
            }
            
            connection = mysql.connector.connect(**connection_config)
            cursor = connection.cursor()
            
            # 构建查询
            table_name = db_config.get('default_table', 'issue')
            stock_code_field = db_config.get('fields', {}).get('stock_code', 'stockCode')
            status_field = db_config.get('fields', {}).get('status', 'status')
            status_filter = db_config.get('status_filter', ['normal'])
            
            if status_filter:
                status_conditions = ', '.join([f"'{status}'" for status in status_filter])
                query = f"""
                SELECT DISTINCT {stock_code_field} 
                FROM {table_name} 
                WHERE {status_field} IN ({status_conditions})
                AND {stock_code_field} IS NOT NULL
                AND {stock_code_field} != ''
                """
            else:
                query = f"""
                SELECT DISTINCT {stock_code_field} 
                FROM {table_name} 
                WHERE {stock_code_field} IS NOT NULL
                AND {stock_code_field} != ''
                """
            
            cursor.execute(query)
            results = cursor.fetchall()
            stocks = {row[0] for row in results if row[0]}
            
            cursor.close()
            connection.close()
            
            logger.info(f"从数据库表 {table_name} 读取到 {len(stocks)} 只股票")
            return stocks
            
        except Exception as e:
            logger.error(f"从数据库读取股票代码失败: {e}")
            return set()

    async def _discover_from_hkconnect(self) -> StockDiscoveryResult:
        """
        从港股通核心列表发现股票
        
        Returns:
            StockDiscoveryResult: 发现结果
        """
        start_time = time.time()

        try:
            # 验证港股通列表
            if not self.hkconnect_core_stocks:
                raise ValueError("港股通核心列表为空")

            # 标准化股票代码
            normalized_stocks = self._normalize_stock_codes(self.hkconnect_core_stocks)

            metadata = {'list_type': '港股通核心蓝筹股', 'total_stocks': len(self.hkconnect_core_stocks),
                'sectors': {'科技': ['00700', '01810', '09988', '03690', '01024'],
                    '金融': ['00939', '01398', '00388', '01299', '02318'], '电信': ['00941', '00788'],
                    '消费': ['01810', '09988', '03690'], '地产': ['01997', '00016', '00001'],
                    '能源': ['00883', '00386']}}

            logger.info(f"港股通核心列表发现: {len(normalized_stocks)} 只优质蓝筹股")

            return StockDiscoveryResult(stocks=normalized_stocks, source=StockSource.HKCONNECT, success=True,
                discovery_time=time.time() - start_time, raw_count=len(self.hkconnect_core_stocks),
                normalized_count=len(normalized_stocks), metadata=metadata)

        except Exception as e:
            error_msg = f"港股通列表获取失败: {e}"
            logger.error(error_msg)

            return StockDiscoveryResult(stocks=set(), source=StockSource.HKCONNECT, success=False,
                error_message=error_msg, discovery_time=time.time() - start_time)

    async def _discover_from_minimal(self) -> StockDiscoveryResult:
        """
        从最小核心列表发现股票 (最终保障)
        
        Returns:
            StockDiscoveryResult: 发现结果
        """
        start_time = time.time()

        # 最小核心列表永远不会失败
        normalized_stocks = self._normalize_stock_codes(self.minimal_core_stocks)

        metadata = {'list_type': '最小核心保障列表', 'description': '系统运行的最低股票保障',
            'stocks_detail': {'00700': '腾讯控股 - 科技龙头', '00941': '中国移动 - 电信龙头',
                '00939': '建设银行 - 金融龙头', '01398': '工商银行 - 银行龙头', '00388': '香港交易所 - 交易所'}}

        logger.warning(f"使用最小核心列表: {len(normalized_stocks)} 只核心股票 (系统保障)")

        return StockDiscoveryResult(stocks=normalized_stocks, source=StockSource.MINIMAL, success=True,
            discovery_time=time.time() - start_time, raw_count=len(self.minimal_core_stocks),
            normalized_count=len(normalized_stocks), metadata=metadata)

    def _normalize_stock_codes(self, stock_codes: Set[str]) -> Set[str]:
        """
        标准化股票代码
        
        Args:
            stock_codes: 原始股票代码集合
            
        Returns:
            Set[str]: 标准化后的股票代码集合
        """
        normalized = set()

        for code in stock_codes:
            if not code:
                continue

            # 转换为字符串并清理
            code_str = str(code).strip().upper()

            # 移除常见后缀
            code_str = re.sub(r'\.(HK|HKEX)$', '', code_str)

            # 验证格式 (香港股票代码格式)
            if re.match(r'^\d{4,5}$', code_str):
                # 确保5位数格式 (前补零)
                normalized_code = code_str.zfill(5)
                normalized.add(normalized_code)
            elif re.match(r'^[A-Z]{1,2}\d{4}$', code_str):
                # ETF等特殊代码
                normalized.add(code_str)
            else:
                logger.debug(f"跳过无效股票代码: {code_str}")

        return normalized

    def _finalize_discovery(self, result: StockDiscoveryResult, start_time: float) -> StockDiscoveryResult:
        """
        完成股票发现，更新缓存和统计
        
        Args:
            result: 发现结果
            start_time: 开始时间
            
        Returns:
            StockDiscoveryResult: 最终结果
        """
        # 更新发现时间
        result.discovery_time = time.time() - start_time

        if result.success and result.stocks:
            # 更新缓存
            self.cached_stocks = result.stocks.copy()
            self.cache_timestamp = time.time()
            self.cache_source = result.source

            # 更新统计
            self.discovery_stats['total_discoveries'] += 1
            self.discovery_stats['source_usage'][result.source] += 1
            self.discovery_stats['last_discovery_time'] = datetime.now()

            # 更新平均发现时间
            total_time = (self.discovery_stats['average_discovery_time'] * (
                        self.discovery_stats['total_discoveries'] - 1) + result.discovery_time)
            self.discovery_stats['average_discovery_time'] = total_time / self.discovery_stats['total_discoveries']

            logger.info(
                f"股票发现完成: {len(result.stocks)} 只股票，来源: {result.source.value}，耗时: {result.discovery_time:.2f}秒")

        return result

    def _is_cache_valid(self) -> bool:
        """
        检查缓存是否有效
        
        Returns:
            bool: 缓存是否有效
        """
        if not self.cached_stocks or not self.cache_timestamp:
            return False

        return (time.time() - self.cache_timestamp) < self.cache_ttl

    async def detect_stock_changes(self) -> Dict[str, Any]:
        """
        检测股票列表变化
        
        Returns:
            Dict[str, Any]: 变化检测结果
        """
        logger.info("检测股票列表变化...")

        # 获取当前股票列表
        current_result = await self.discover_all_stocks(force_refresh=True)

        if not current_result.success:
            return {'success': False, 'error': current_result.error_message, 'timestamp': datetime.now()}

        # 与缓存比较
        changes = {'success': True, 'timestamp': datetime.now(), 'current_count': len(current_result.stocks),
            'current_source': current_result.source.value, 'discovery_time': current_result.discovery_time,
            'has_changes': False, 'new_stocks': set(), 'removed_stocks': set(), 'metadata': current_result.metadata}

        # 如果有之前的缓存，比较变化
        if hasattr(self, '_previous_stocks') and self._previous_stocks:
            previous_stocks = self._previous_stocks
            current_stocks = current_result.stocks

            changes['new_stocks'] = current_stocks - previous_stocks
            changes['removed_stocks'] = previous_stocks - current_stocks
            changes['has_changes'] = bool(changes['new_stocks'] or changes['removed_stocks'])
            changes['previous_count'] = len(previous_stocks)

            if changes['has_changes']:
                logger.info(
                    f"检测到股票变化: 新增 {len(changes['new_stocks'])} 只，移除 {len(changes['removed_stocks'])} 只")

        # 保存当前股票列表供下次比较
        self._previous_stocks = current_result.stocks.copy()

        return changes

    def get_discovery_stats(self) -> Dict[str, Any]:
        """
        获取股票发现统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {'statistics': self.discovery_stats.copy(), 'cache_info': {'is_valid': self._is_cache_valid(),
            'cached_count': len(self.cached_stocks) if self.cached_stocks else 0,
            'cache_source': self.cache_source.value if self.cache_source else None,
            'cache_age_seconds': time.time() - self.cache_timestamp if self.cache_timestamp else None},
            'fallback_lists': {'hkconnect_count': len(self.hkconnect_core_stocks),
                'minimal_count': len(self.minimal_core_stocks)}}

    def clear_cache(self):
        """清除股票缓存"""
        self.cached_stocks = None
        self.cache_timestamp = None
        self.cache_source = None
        logger.info("股票发现缓存已清除")


# 便捷函数
async def discover_stocks_with_fallback(config_manager, clickhouse_client=None,
                                        force_refresh=False) -> StockDiscoveryResult:
    """
    便捷的股票发现函数
    
    Args:
        config_manager: 配置管理器
        clickhouse_client: ClickHouse客户端
        force_refresh: 是否强制刷新
        
    Returns:
        StockDiscoveryResult: 发现结果
    """
    manager = EnhancedStockDiscoveryManager(config_manager, clickhouse_client)
    return await manager.discover_all_stocks(force_refresh)


def validate_stock_code(stock_code: str) -> bool:
    """
    验证股票代码格式
    
    Args:
        stock_code: 股票代码
        
    Returns:
        bool: 是否有效
    """
    if not stock_code:
        return False

    code_str = str(stock_code).strip().upper()
    code_str = re.sub(r'\.(HK|HKEX)$', '', code_str)

    # 香港股票代码格式验证
    return bool(re.match(r'^\d{4,5}$|^[A-Z]{1,2}\d{4}$', code_str))


if __name__ == "__main__":
    # 测试模块
    async def test_stock_discovery():
        """测试股票发现功能"""

        class MockConfigManager:
            def __init__(self):
                self.config = {'download_tasks': [{'stock_code': '00700', 'enabled': True},
                    {'stock_code': ['00939', '01398'], 'enabled': True}, {'stock_code': '02699', 'enabled': False}]}

        # 创建测试管理器
        config_manager = MockConfigManager()
        manager = EnhancedStockDiscoveryManager(config_manager)

        # 测试股票发现
        result = await manager.discover_all_stocks()

        print("\n" + "=" * 60)
        print("🔍 股票发现测试结果")
        print("=" * 60)
        print(f"成功: {result.success}")
        print(f"来源: {result.source.value}")
        print(f"股票数量: {len(result.stocks)}")
        print(f"发现耗时: {result.discovery_time:.3f}秒")
        print(f"股票列表: {sorted(list(result.stocks))}")

        if result.metadata:
            print(f"元数据: {result.metadata}")

        # 测试统计信息
        stats = manager.get_discovery_stats()
        print(f"\n📊 统计信息:")
        print(f"发现次数: {stats['statistics']['total_discoveries']}")
        print(f"来源使用统计: {stats['statistics']['source_usage']}")
        print("=" * 60)


    # 运行测试
    asyncio.run(test_stock_discovery())
