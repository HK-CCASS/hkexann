"""
统一Milvus集合管理器

这个模块解决了系统中Milvus集合命名不一致和向量维度验证的问题。
提供统一的集合创建、管理和连接池功能。

主要功能：
- 统一集合命名规范
- 4096维向量维度一致性验证
- 连接池管理防止泄露
- 集合生命周期管理
- 性能监控和健康检查

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import warnings
from typing import Dict, Any, Optional, List, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import threading
from pathlib import Path
import json

# 配置日志
# 配置日志（如果没有已配置的handler）
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 设置路径
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

# 抑制protobuf兼容性警告
warnings.filterwarnings('ignore', category=UserWarning, module='google.protobuf.runtime_version')

try:
    from pymilvus import (
        connections, Collection, FieldSchema, CollectionSchema, DataType,
        utility, MilvusException
    )
    MILVUS_AVAILABLE = True
except ImportError:
    MILVUS_AVAILABLE = False
    logger.warning("Milvus SDK 未安装，将使用模拟模式")

try:
    from config.settings import settings
except ImportError:
    # 创建模拟设置用于测试
    class MockSettings:
        milvus_host = "localhost"
        milvus_port = 19531
        milvus_user = None
        milvus_password = None
        embedding_dimension = 4096
        pdf_embeddings_collection = "pdf_embeddings_v3"
    
    settings = MockSettings()
    logger.warning("使用模拟设置进行测试")


class CollectionType(Enum):
    """集合类型枚举"""
    PDF_EMBEDDINGS = "pdf_embeddings"      # PDF文档嵌入
    REALTIME_ANALYSIS = "realtime_analysis" # 实时分析
    HISTORICAL_DATA = "historical_data"     # 历史数据
    TEST_COLLECTION = "test_collection"     # 测试集合


@dataclass
class CollectionConfig:
    """集合配置"""
    name: str
    description: str
    vector_dimension: int = 4096
    max_length: int = 65535
    enable_dynamic_field: bool = True
    consistency_level: str = "Session"
    
    # 索引配置
    index_type: str = "IVF_PQ"
    metric_type: str = "COSINE"
    nlist: int = 2048
    m: int = 8
    nbits: int = 8
    
    # 分片配置
    shards_num: int = 2
    replica_number: int = 1


@dataclass
class ConnectionInfo:
    """连接信息"""
    connection_name: str
    host: str
    port: int
    user: Optional[str] = None
    password: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_used: datetime = field(default_factory=datetime.now)
    is_active: bool = True
    usage_count: int = 0


@dataclass
class CollectionStats:
    """集合统计信息"""
    name: str
    entity_count: int = 0
    indexed: bool = False
    loaded: bool = False
    last_update: Optional[datetime] = None
    size_bytes: int = 0
    health_status: str = "unknown"


class UnifiedMilvusManager:
    """
    统一Milvus集合管理器
    
    解决系统中的关键问题：
    1. 集合命名不一致 - 统一命名规范
    2. 向量维度验证 - 确保4096维一致性
    3. 连接池管理 - 防止连接泄露
    4. 集合生命周期管理 - 创建、更新、删除
    """
    
    def __init__(self):
        """初始化统一Milvus管理器"""
        
        # 核心配置
        self.host = settings.milvus_host
        self.port = settings.milvus_port
        self.user = settings.milvus_user
        self.password = settings.milvus_password
        self.vector_dimension = settings.embedding_dimension
        
        # 验证向量维度
        if self.vector_dimension != 4096:
            raise ValueError(f"向量维度必须为4096，当前配置为{self.vector_dimension}")
        
        # 连接池管理
        self.connections: Dict[str, ConnectionInfo] = {}
        self.active_collections: Dict[str, Collection] = {}
        self.connection_lock = threading.RLock()
        
        # 集合配置注册表
        self.collection_configs = self._initialize_collection_configs()
        
        # 统计和监控
        self.collection_stats: Dict[str, CollectionStats] = {}
        self.operation_history: List[Dict[str, Any]] = []
        
        # 健康检查
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_check_interval = 60  # 60秒检查一次
        
        logger.info("🗃️ 统一Milvus集合管理器初始化完成")
        self._log_configuration()

    def _log_configuration(self):
        """记录配置信息"""
        logger.info(f"📊 Milvus配置:")
        logger.info(f"  服务器: {self.host}:{self.port}")
        logger.info(f"  向量维度: {self.vector_dimension}")
        logger.info(f"  注册集合: {len(self.collection_configs)}")
        
        if not MILVUS_AVAILABLE:
            logger.warning("⚠️  Milvus SDK不可用，运行在模拟模式")

    def _initialize_collection_configs(self) -> Dict[CollectionType, CollectionConfig]:
        """初始化集合配置"""
        return {
            CollectionType.PDF_EMBEDDINGS: CollectionConfig(
                name=settings.pdf_embeddings_collection,  # 使用配置中的统一名称
                description="PDF文档嵌入向量集合 - 主要业务集合",
                vector_dimension=4096,
                index_type="IVF_PQ",
                nlist=2048,
                shards_num=2
            ),
            CollectionType.REALTIME_ANALYSIS: CollectionConfig(
                name="hkex_realtime_analysis_v1",
                description="实时分析向量集合 - 高频更新数据",
                vector_dimension=4096,
                index_type="IVF_FLAT",  # 实时数据使用更快的索引
                nlist=1024,
                shards_num=1
            ),
            CollectionType.HISTORICAL_DATA: CollectionConfig(
                name="hkex_historical_data_v1", 
                description="历史数据向量集合 - 只读大数据集",
                vector_dimension=4096,
                index_type="IVF_PQ",
                nlist=4096,  # 历史数据使用更多聚类中心
                shards_num=4  # 更多分片支持大数据
            ),
            CollectionType.TEST_COLLECTION: CollectionConfig(
                name="hkex_test_collection",
                description="测试集合 - 开发和测试使用",
                vector_dimension=4096,
                index_type="IVF_FLAT",
                nlist=128,
                shards_num=1
            )
        }

    def get_collection_name(self, collection_type: CollectionType) -> str:
        """
        获取统一的集合名称

        Args:
            collection_type: 集合类型

        Returns:
            str: 统一的集合名称
        """
        if collection_type not in self.collection_configs:
            raise ValueError(f"未知的集合类型: {collection_type}")

        return self.collection_configs[collection_type].name

    def get_collection_type_by_name(self, collection_name: str) -> CollectionType:
        """
        根据集合名称获取集合类型

        Args:
            collection_name: 集合名称

        Returns:
            CollectionType: 集合类型
        """
        for collection_type, config in self.collection_configs.items():
            if config.name == collection_name:
                return collection_type
        raise ValueError(f"未找到集合名称对应的类型: {collection_name}")

    async def create_connection(self, connection_name: str = None) -> str:
        """
        创建Milvus连接
        
        Args:
            connection_name: 连接名称，如果为None则自动生成
            
        Returns:
            str: 连接名称
        """
        if not MILVUS_AVAILABLE:
            logger.warning("Milvus SDK不可用，返回模拟连接")
            return "mock_connection"
        
        if connection_name is None:
            connection_name = f"conn_{int(time.time() * 1000)}"
        
        with self.connection_lock:
            try:
                # 检查连接是否已存在
                if connection_name in self.connections:
                    conn_info = self.connections[connection_name]
                    if conn_info.is_active:
                        conn_info.usage_count += 1
                        conn_info.last_used = datetime.now()
                        logger.debug(f"重用现有连接: {connection_name}")
                        return connection_name
                
                # 创建新连接
                conn_params = {
                    "alias": connection_name,
                    "host": self.host,
                    "port": str(self.port)
                }
                
                if self.user:
                    conn_params["user"] = self.user
                if self.password:
                    conn_params["password"] = self.password
                
                connections.connect(**conn_params)
                
                # 测试连接
                collections_list = utility.list_collections(using=connection_name)
                
                # 记录连接信息
                conn_info = ConnectionInfo(
                    connection_name=connection_name,
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    is_active=True,
                    usage_count=1
                )
                
                self.connections[connection_name] = conn_info
                
                logger.info(f"✅ Milvus连接创建成功: {connection_name}")
                logger.debug(f"发现集合: {len(collections_list)} 个")
                
                return connection_name
                
            except Exception as e:
                logger.error(f"❌ Milvus连接创建失败: {e}")
                raise

    async def close_connection(self, connection_name: str):
        """
        关闭Milvus连接
        
        Args:
            connection_name: 连接名称
        """
        if not MILVUS_AVAILABLE:
            return
        
        with self.connection_lock:
            try:
                if connection_name in self.connections:
                    # 标记连接为非活跃
                    self.connections[connection_name].is_active = False
                    
                    # 关闭连接
                    connections.disconnect(connection_name)
                    
                    # 清理活跃集合
                    collections_to_remove = [
                        coll_name for coll_name, collection in self.active_collections.items()
                        if hasattr(collection, '_using') and collection._using == connection_name
                    ]
                    
                    for coll_name in collections_to_remove:
                        del self.active_collections[coll_name]
                    
                    logger.info(f"🔒 Milvus连接已关闭: {connection_name}")
                    
            except Exception as e:
                logger.warning(f"关闭连接时出错: {e}")

    async def create_collection(self, collection_type: CollectionType, 
                              connection_name: str = None) -> bool:
        """
        创建集合
        
        Args:
            collection_type: 集合类型
            connection_name: 连接名称
            
        Returns:
            bool: 是否创建成功
        """
        if not MILVUS_AVAILABLE:
            logger.warning("Milvus SDK不可用，模拟集合创建成功")
            return True
        
        config = self.collection_configs[collection_type]
        collection_name = config.name
        
        # 确保有有效连接
        if connection_name is None:
            connection_name = await self.create_connection()
        
        try:
            # 检查集合是否已存在
            if utility.has_collection(collection_name, using=connection_name):
                logger.info(f"集合已存在: {collection_name}")
                
                # 加载到活跃集合中
                collection = Collection(collection_name, using=connection_name)
                self.active_collections[collection_name] = collection
                
                # 更新统计信息
                await self._update_collection_stats(collection_name, connection_name)
                
                return True
            
            # 创建字段Schema
            fields = self._create_collection_fields(config)
            
            # 创建集合Schema
            schema = CollectionSchema(
                fields=fields,
                description=config.description,
                enable_dynamic_field=config.enable_dynamic_field
            )
            
            # 创建集合
            collection = Collection(
                name=collection_name,
                schema=schema,
                using=connection_name,
                shards_num=config.shards_num,
                consistency_level=config.consistency_level
            )
            
            # 创建索引
            await self._create_collection_indexes(collection, config)
            
            # 加载集合
            collection.load()
            
            # 添加到活跃集合
            self.active_collections[collection_name] = collection
            
            # 更新统计信息
            await self._update_collection_stats(collection_name, connection_name)
            
            # 记录操作历史
            self.operation_history.append({
                'timestamp': datetime.now().isoformat(),
                'operation': 'create_collection',
                'collection_name': collection_name,
                'collection_type': collection_type.value,
                'success': True
            })
            
            logger.info(f"✅ 集合创建成功: {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 集合创建失败 {collection_name}: {e}")
            
            # 记录失败操作
            self.operation_history.append({
                'timestamp': datetime.now().isoformat(),
                'operation': 'create_collection',
                'collection_name': collection_name,
                'collection_type': collection_type.value,
                'success': False,
                'error': str(e)
            })
            
            return False

    def _create_collection_fields(self, config: CollectionConfig) -> List[FieldSchema]:
        """创建集合字段Schema"""
        fields = [
            # 主键字段
            FieldSchema(
                name="id",
                dtype=DataType.VARCHAR,
                max_length=64,
                is_primary=True,
                description="向量唯一标识符"
            ),
            
            # 向量字段 - 确保4096维
            FieldSchema(
                name="embedding",
                dtype=DataType.FLOAT_VECTOR,
                dim=config.vector_dimension,
                description=f"{config.vector_dimension}维嵌入向量"
            ),
            
            # 业务字段
            FieldSchema(
                name="doc_id",
                dtype=DataType.VARCHAR,
                max_length=128,
                description="文档ID"
            ),
            
            FieldSchema(
                name="chunk_id",
                dtype=DataType.VARCHAR,
                max_length=128,
                description="文档块ID"
            ),
            
            FieldSchema(
                name="stock_code",
                dtype=DataType.VARCHAR,
                max_length=16,
                description="股票代码"
            ),
            
            FieldSchema(
                name="document_type",
                dtype=DataType.VARCHAR,
                max_length=32,
                description="文档类型"
            ),
            
            FieldSchema(
                name="chunk_type",
                dtype=DataType.VARCHAR,
                max_length=32,
                description="文档块类型"
            ),
            
            FieldSchema(
                name="content",
                dtype=DataType.VARCHAR,
                max_length=config.max_length,
                description="文档内容"
            ),
            
            # v3新增字段
            FieldSchema(
                name="publish_date",
                dtype=DataType.VARCHAR,
                max_length=32,
                description="发布日期"
            ),
            
            FieldSchema(
                name="importance_score",
                dtype=DataType.FLOAT,
                description="重要性评分"
            ),
            
            FieldSchema(
                name="page_number",
                dtype=DataType.INT32,
                description="页码"
            ),
            
            FieldSchema(
                name="chunk_length",
                dtype=DataType.INT32,
                description="文档块长度"
            ),
            
            FieldSchema(
                name="created_at",
                dtype=DataType.VARCHAR,
                max_length=32,
                description="创建时间"
            ),
            
            FieldSchema(
                name="updated_at",
                dtype=DataType.VARCHAR,
                max_length=32,
                description="更新时间"
            ),

            # HKEX官方3级分类系统 (更新：与ClickHouse schema保持一致)
            FieldSchema(
                name="hkex_level1_code",
                dtype=DataType.VARCHAR,
                max_length=20,
                description="HKEX 1级分类代码"
            ),
            FieldSchema(
                name="hkex_level1_name",
                dtype=DataType.VARCHAR,
                max_length=500,
                description="HKEX 1级分类名称"
            ),
            FieldSchema(
                name="hkex_level2_code",
                dtype=DataType.VARCHAR,
                max_length=20,
                description="HKEX 2级分组代码"
            ),
            FieldSchema(
                name="hkex_level2_name",
                dtype=DataType.VARCHAR,
                max_length=500,
                description="HKEX 2级分组名称"
            ),
            FieldSchema(
                name="hkex_level3_code",
                dtype=DataType.VARCHAR,
                max_length=20,
                description="HKEX 3级分类代码"
            ),
            FieldSchema(
                name="hkex_level3_name",
                dtype=DataType.VARCHAR,
                max_length=500,
                description="HKEX 3级分类名称"
            ),

            # HKEX分类元数据字段 (支持6种搜索类型)
            FieldSchema(
                name="hkex_classification_confidence",
                dtype=DataType.FLOAT,
                description="HKEX分类置信度"
            ),
            FieldSchema(
                name="hkex_full_path",
                dtype=DataType.VARCHAR,
                max_length=500,
                description="HKEX完整分类路径"
            ),
            FieldSchema(
                name="hkex_classification_method",
                dtype=DataType.VARCHAR,
                max_length=50,
                description="HKEX分类方法标识"
            )
        ]
        
        return fields

    async def _create_collection_indexes(self, collection: Collection, config: CollectionConfig):
        """为集合创建索引"""
        try:
            # 向量索引
            vector_index_params = {
                "metric_type": config.metric_type,
                "index_type": config.index_type,
                "params": {
                    "nlist": config.nlist,
                    "m": config.m,
                    "nbits": config.nbits
                }
            }
            
            collection.create_index(
                field_name="embedding",
                index_params=vector_index_params
            )
            
            logger.info(f"✅ 向量索引创建成功: {config.index_type}")
            
            # 标量字段索引
            scalar_indexes = [
                "stock_code", "document_type", "chunk_type",
                "doc_id", "chunk_id", "publish_date",

                # HKEX 3级分类索引 (Range Search + Aggregation Search + Text Search)
                "hkex_level1_code",        # 1级分类代码范围查询
                "hkex_level1_name",        # 1级分类名称文本搜索
                "hkex_level2_code",        # 2级分类代码范围查询
                "hkex_level2_name",        # 2级分类名称文本搜索
                "hkex_level3_code",        # 3级分类代码范围查询
                "hkex_level3_name",        # 3级分类名称文本搜索

                # HKEX分类元数据索引 (Multi-Vector Search + Aggregation Search)
                "hkex_classification_confidence",  # 置信度范围查询
                "hkex_full_path",                  # 完整路径文本搜索
                "hkex_classification_method"       # 分类方法聚合查询
            ]
            
            for field_name in scalar_indexes:
                try:
                    collection.create_index(field_name=field_name)
                    logger.debug(f"标量索引创建成功: {field_name}")
                except Exception as e:
                    logger.warning(f"标量索引创建失败 {field_name}: {e}")
            
        except Exception as e:
            logger.error(f"索引创建失败: {e}")
            raise

    async def _update_collection_stats(self, collection_name: str, connection_name: str):
        """更新集合统计信息"""
        if not MILVUS_AVAILABLE:
            return
        
        try:
            collection = Collection(collection_name, using=connection_name)
            
            # 获取统计信息
            stats = CollectionStats(
                name=collection_name,
                entity_count=collection.num_entities,
                indexed=len(collection.indexes) > 0,
                loaded=utility.load_state(collection_name, using=connection_name).name == "Loaded",
                last_update=datetime.now(),
                health_status="healthy"
            )
            
            self.collection_stats[collection_name] = stats
            
        except Exception as e:
            logger.warning(f"更新集合统计失败 {collection_name}: {e}")
            
            # 设置错误状态
            error_stats = CollectionStats(
                name=collection_name,
                last_update=datetime.now(),
                health_status="error"
            )
            self.collection_stats[collection_name] = error_stats

    @asynccontextmanager
    async def get_collection(self, collection_type: CollectionType):
        """
        获取集合的异步上下文管理器
        
        Args:
            collection_type: 集合类型
            
        Usage:
            async with manager.get_collection(CollectionType.PDF_EMBEDDINGS) as collection:
                # 使用collection进行操作
                result = collection.search(...)
        """
        collection_name = self.get_collection_name(collection_type)
        connection_name = None
        
        try:
            # 检查活跃集合
            if collection_name in self.active_collections:
                yield self.active_collections[collection_name]
                return
            
            # 创建新连接和集合
            connection_name = await self.create_connection()
            
            # 确保集合存在
            await self.create_collection(collection_type, connection_name)
            
            if collection_name in self.active_collections:
                yield self.active_collections[collection_name]
            else:
                raise RuntimeError(f"集合创建失败: {collection_name}")
                
        except Exception as e:
            logger.error(f"获取集合失败 {collection_name}: {e}")
            raise
        finally:
            # 连接会在池中管理，不需要立即关闭
            pass

    async def validate_vector_dimension(self, vectors: List[List[float]], 
                                      collection_type: CollectionType = None) -> bool:
        """
        验证向量维度
        
        Args:
            vectors: 向量列表
            collection_type: 集合类型（可选）
            
        Returns:
            bool: 是否符合4096维要求
        """
        expected_dim = 4096
        
        for i, vector in enumerate(vectors):
            if len(vector) != expected_dim:
                logger.error(f"向量维度错误 - 索引{i}: 期望{expected_dim}维，实际{len(vector)}维")
                return False
        
        if collection_type:
            config = self.collection_configs[collection_type]
            if config.vector_dimension != expected_dim:
                logger.error(f"集合配置维度错误: {config.vector_dimension} != {expected_dim}")
                return False
        
        logger.debug(f"✅ 向量维度验证通过: {len(vectors)}个向量，{expected_dim}维")
        return True

    async def start_health_monitoring(self):
        """启动健康检查监控"""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_check_loop())

    async def stop_health_monitoring(self):
        """停止健康检查监控"""
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None

    async def _health_check_loop(self):
        """健康检查循环"""
        while True:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self._health_check_interval)
            except Exception as e:
                logger.error(f"健康检查异常: {e}")
                await asyncio.sleep(self._health_check_interval * 2)

    async def _perform_health_check(self):
        """执行健康检查"""
        if not MILVUS_AVAILABLE:
            return
        
        for collection_name in list(self.active_collections.keys()):
            try:
                collection = self.active_collections[collection_name]
                
                # 检查集合状态
                entity_count = collection.num_entities
                load_state = utility.load_state(collection_name)
                
                # 更新健康状态
                if collection_name in self.collection_stats:
                    stats = self.collection_stats[collection_name]
                    stats.entity_count = entity_count
                    stats.loaded = load_state.name == "Loaded"
                    stats.last_update = datetime.now()
                    stats.health_status = "healthy"
                
                logger.debug(f"健康检查通过: {collection_name} ({entity_count} 条记录)")
                
            except Exception as e:
                logger.warning(f"集合健康检查失败 {collection_name}: {e}")
                
                # 标记为不健康
                if collection_name in self.collection_stats:
                    self.collection_stats[collection_name].health_status = "unhealthy"
                
                # 从活跃集合中移除
                self.active_collections.pop(collection_name, None)

    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态
        
        Returns:
            Dict[str, Any]: 系统状态信息
        """
        # 连接统计
        active_connections = sum(1 for conn in self.connections.values() if conn.is_active)
        
        # 集合统计
        healthy_collections = sum(
            1 for stats in self.collection_stats.values() 
            if stats.health_status == "healthy"
        )
        
        # 总向量数
        total_vectors = sum(stats.entity_count for stats in self.collection_stats.values())
        
        return {
            'milvus_available': MILVUS_AVAILABLE,
            'server_info': {
                'host': self.host,
                'port': self.port,
                'vector_dimension': self.vector_dimension
            },
            'connections': {
                'total': len(self.connections),
                'active': active_connections,
                'details': [
                    {
                        'name': conn.connection_name,
                        'is_active': conn.is_active,
                        'usage_count': conn.usage_count,
                        'last_used': conn.last_used.isoformat()
                    }
                    for conn in self.connections.values()
                ]
            },
            'collections': {
                'total': len(self.collection_configs),
                'active': len(self.active_collections),
                'healthy': healthy_collections,
                'total_vectors': total_vectors,
                'details': [
                    {
                        'name': stats.name,
                        'entity_count': stats.entity_count,
                        'health_status': stats.health_status,
                        'loaded': stats.loaded,
                        'last_update': stats.last_update.isoformat() if stats.last_update else None
                    }
                    for stats in self.collection_stats.values()
                ]
            },
            'recent_operations': self.operation_history[-10:] if self.operation_history else []
        }

    # ========================================
    # 6种搜索功能实现
    # ========================================

    async def vector_search(self,
                           collection_name: str,
                           query_vectors: List[List[float]],
                           top_k: int = 10,
                           output_fields: Optional[List[str]] = None,
                           expr: Optional[str] = None,
                           search_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        1. 向量搜索 (Vector Search)
        基于向量相似度的搜索，适用于语义搜索场景
        """
        collection_type = self.get_collection_type_by_name(collection_name)
        async with self.get_collection(collection_type) as collection:
            if output_fields is None:
                output_fields = ["doc_id", "stock_code", "company_name", "text_content", "publish_date", "importance_score"]

            if search_params is None:
                search_params = {
                    "metric_type": "COSINE",
                    "params": {"ef": 128}
                }

            try:
                results = collection.search(
                    data=query_vectors,
                    anns_field="embedding",
                    param=search_params,
                    limit=top_k,
                    expr=expr,
                    output_fields=output_fields,
                    consistency_level="Session"
                )

                return {
                    "search_type": "vector",
                    "total_results": len(results),
                    "results": [
                        {
                            "id": hit.id,
                            "distance": hit.distance,
                            "entity": hit.entity.to_dict() if hasattr(hit.entity, 'to_dict') else dict(hit.entity)
                        } for result in results for hit in result
                    ],
                    "search_params": search_params
                }
            except Exception as e:
                logger.error(f"向量搜索失败: {e}")
                return {"search_type": "vector", "error": str(e), "results": []}

    async def text_search(self,
                         collection_name: str,
                         query_text: str,
                         top_k: int = 10,
                         output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        2. 文本搜索 (Text Search)
        基于文本内容的全文搜索
        """
        collection_type = self.get_collection_type_by_name(collection_name)
        async with self.get_collection(collection_type) as collection:
            if output_fields is None:
                output_fields = ["doc_id", "stock_code", "company_name", "text_content", "publish_date", "importance_score"]

            try:
                # 构建文本匹配表达式
                text_expr = f'text_content like "%{query_text}%"'

                results = collection.query(
                    expr=text_expr,
                    output_fields=output_fields,
                    limit=top_k,
                    consistency_level="Session"
                )

                return {
                    "search_type": "text",
                    "query_text": query_text,
                    "total_results": len(results),
                    "results": results,
                    "text_expr": text_expr
                }
            except Exception as e:
                logger.error(f"文本搜索失败: {e}")
                return {"search_type": "text", "error": str(e), "results": []}

    async def hybrid_search(self,
                           collection_name: str,
                           query_vector: List[float],
                           query_text: str,
                           top_k: int = 10,
                           vector_weight: float = 0.7,
                           text_weight: float = 0.3,
                           output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        3. 混合搜索 (Hybrid Search)
        结合向量相似度和文本匹配的混合搜索
        """
        try:
            # 执行向量搜索
            vector_results = await self.vector_search(
                collection_name=collection_name,
                query_vectors=[query_vector],
                top_k=top_k * 2,  # 获取更多结果用于混合排序
                output_fields=output_fields
            )

            # 执行文本搜索
            text_results = await self.text_search(
                collection_name=collection_name,
                query_text=query_text,
                top_k=top_k * 2,
                output_fields=output_fields
            )

            # 混合评分
            hybrid_scores = {}

            # 向量搜索结果评分
            for result in vector_results.get("results", []):
                doc_id = result["entity"]["doc_id"]
                vector_score = 1.0 - result["distance"]  # 距离越小，相似度越高
                hybrid_scores[doc_id] = {"vector_score": vector_score, "text_score": 0.0, "data": result}

            # 文本搜索结果评分
            max_text_score = len(text_results.get("results", []))
            for i, result in enumerate(text_results.get("results", [])):
                doc_id = result["doc_id"]
                text_score = (max_text_score - i) / max_text_score  # 排名越前，得分越高

                if doc_id in hybrid_scores:
                    hybrid_scores[doc_id]["text_score"] = text_score
                else:
                    hybrid_scores[doc_id] = {
                        "vector_score": 0.0,
                        "text_score": text_score,
                        "data": {"entity": result}
                    }

            # 计算混合得分并排序
            final_results = []
            for doc_id, scores in hybrid_scores.items():
                final_score = (scores["vector_score"] * vector_weight +
                             scores["text_score"] * text_weight)

                result_data = scores["data"]
                result_data["hybrid_score"] = final_score
                result_data["vector_score"] = scores["vector_score"]
                result_data["text_score"] = scores["text_score"]

                final_results.append(result_data)

            # 按混合得分排序并截取前top_k
            final_results.sort(key=lambda x: x["hybrid_score"], reverse=True)
            final_results = final_results[:top_k]

            return {
                "search_type": "hybrid",
                "query_text": query_text,
                "vector_weight": vector_weight,
                "text_weight": text_weight,
                "total_results": len(final_results),
                "results": final_results
            }

        except Exception as e:
            logger.error(f"混合搜索失败: {e}")
            return {"search_type": "hybrid", "error": str(e), "results": []}

    async def range_search(self,
                          collection_name: str,
                          field_conditions: Dict[str, Any],
                          top_k: int = 10,
                          output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        4. 范围搜索 (Range Search)
        基于标量字段的范围条件搜索
        """
        collection_type = self.get_collection_type_by_name(collection_name)
        async with self.get_collection(collection_type) as collection:
            if output_fields is None:
                output_fields = ["doc_id", "stock_code", "company_name", "text_content", "publish_date", "importance_score"]

            try:
                # 构建范围查询表达式
                conditions = []

                for field, condition in field_conditions.items():
                    if isinstance(condition, dict):
                        if "min" in condition and "max" in condition:
                            conditions.append(f"{field} >= {condition['min']} and {field} <= {condition['max']}")
                        elif "min" in condition:
                            conditions.append(f"{field} >= {condition['min']}")
                        elif "max" in condition:
                            conditions.append(f"{field} <= {condition['max']}")
                        elif "eq" in condition:
                            if isinstance(condition["eq"], str):
                                conditions.append(f'{field} == "{condition["eq"]}"')
                            else:
                                conditions.append(f"{field} == {condition['eq']}")
                        elif "in" in condition:
                            values = condition["in"]
                            if isinstance(values[0], str):
                                in_clause = ", ".join([f'"{v}"' for v in values])
                            else:
                                in_clause = ", ".join([str(v) for v in values])
                            conditions.append(f"{field} in [{in_clause}]")
                    else:
                        # 直接值比较
                        if isinstance(condition, str):
                            conditions.append(f'{field} == "{condition}"')
                        else:
                            conditions.append(f"{field} == {condition}")

                expr = " and ".join(conditions)

                results = collection.query(
                    expr=expr,
                    output_fields=output_fields,
                    limit=top_k,
                    consistency_level="Session"
                )

                return {
                    "search_type": "range",
                    "field_conditions": field_conditions,
                    "expr": expr,
                    "total_results": len(results),
                    "results": results
                }

            except Exception as e:
                logger.error(f"范围搜索失败: {e}")
                return {"search_type": "range", "error": str(e), "results": []}

    async def multi_vector_search(self,
                                 collection_name: str,
                                 vector_queries: Dict[str, List[float]],
                                 top_k: int = 10,
                                 combine_method: str = "weighted_sum",
                                 output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        5. 多向量搜索 (Multi-Vector Search)
        支持多个向量字段的联合搜索
        注意：当前schema只有一个向量字段，此方法为未来多向量字段预留
        """
        try:
            if len(vector_queries) == 1:
                # 单向量情况，直接调用向量搜索
                vector_field, query_vector = next(iter(vector_queries.items()))
                return await self.vector_search(
                    collection_name=collection_name,
                    query_vectors=[query_vector],
                    top_k=top_k,
                    output_fields=output_fields
                )

            # 多向量搜索逻辑（当前schema不支持，为未来扩展预留）
            all_results = {}

            for field_name, query_vector in vector_queries.items():
                if field_name == "embedding":  # 当前唯一的向量字段
                    results = await self.vector_search(
                        collection_name=collection_name,
                        query_vectors=[query_vector],
                        top_k=top_k * 2,
                        output_fields=output_fields
                    )
                    all_results[field_name] = results

            # 结果合并逻辑
            if combine_method == "weighted_sum":
                # 加权求和合并
                combined_scores = {}
                weight = 1.0 / len(vector_queries)  # 平均权重

                for field_name, results in all_results.items():
                    for result in results.get("results", []):
                        doc_id = result["entity"]["doc_id"]
                        score = 1.0 - result["distance"]

                        if doc_id in combined_scores:
                            combined_scores[doc_id]["score"] += score * weight
                        else:
                            combined_scores[doc_id] = {
                                "score": score * weight,
                                "data": result
                            }

                # 排序并返回结果
                final_results = sorted(
                    combined_scores.values(),
                    key=lambda x: x["score"],
                    reverse=True
                )[:top_k]

                return {
                    "search_type": "multi_vector",
                    "vector_fields": list(vector_queries.keys()),
                    "combine_method": combine_method,
                    "total_results": len(final_results),
                    "results": [r["data"] for r in final_results]
                }

        except Exception as e:
            logger.error(f"多向量搜索失败: {e}")
            return {"search_type": "multi_vector", "error": str(e), "results": []}

    async def aggregation_search(self,
                                collection_name: str,
                                group_by_field: str,
                                aggregate_field: Optional[str] = None,
                                aggregate_func: str = "count",
                                query_vector: Optional[List[float]] = None,
                                top_k: int = 10,
                                expr: Optional[str] = None) -> Dict[str, Any]:
        """
        6. 聚合搜索 (Aggregation Search)
        支持分组统计和聚合计算的搜索
        """
        try:
            collection_type = self.get_collection_type_by_name(collection_name)
            async with self.get_collection(collection_type) as collection:
                # 首先获取原始数据
                output_fields = [group_by_field]
                if aggregate_field:
                    output_fields.append(aggregate_field)

                # 添加其他有用字段
                additional_fields = ["doc_id", "stock_code", "company_name", "publish_date"]
                for field in additional_fields:
                    if field not in output_fields:
                        output_fields.append(field)

                if query_vector:
                    # 基于向量相似度的聚合搜索
                    search_results = await self.vector_search(
                        collection_name=collection_name,
                        query_vectors=[query_vector],
                        top_k=top_k * 10,  # 获取更多数据用于聚合
                        output_fields=output_fields,
                        expr=expr
                    )
                    raw_data = [r["entity"] for r in search_results.get("results", [])]
                else:
                    # 基于条件的聚合查询
                    raw_data = collection.query(
                        expr=expr or "doc_id != ''",
                        output_fields=output_fields,
                        limit=top_k * 50,  # 获取足够数据用于聚合
                        consistency_level="Session"
                    )

                # 执行聚合计算
                groups = {}
                for record in raw_data:
                    group_key = record.get(group_by_field, "未知")

                    if group_key not in groups:
                        groups[group_key] = {
                            "group_value": group_key,
                            "count": 0,
                            "records": []
                        }

                    groups[group_key]["count"] += 1
                    groups[group_key]["records"].append(record)

                    # 聚合字段计算
                    if aggregate_field and aggregate_field in record:
                        if "aggregate_values" not in groups[group_key]:
                            groups[group_key]["aggregate_values"] = []
                        groups[group_key]["aggregate_values"].append(record[aggregate_field])

                # 计算聚合函数结果
                for group_key, group_data in groups.items():
                    if aggregate_field and "aggregate_values" in group_data:
                        values = group_data["aggregate_values"]
                        if aggregate_func == "sum":
                            group_data["aggregate_result"] = sum(values)
                        elif aggregate_func == "avg":
                            group_data["aggregate_result"] = sum(values) / len(values) if values else 0
                        elif aggregate_func == "min":
                            group_data["aggregate_result"] = min(values) if values else None
                        elif aggregate_func == "max":
                            group_data["aggregate_result"] = max(values) if values else None
                        else:  # count
                            group_data["aggregate_result"] = len(values)

                # 排序并限制结果
                sorted_groups = sorted(
                    groups.values(),
                    key=lambda x: x.get("aggregate_result", x["count"]),
                    reverse=True
                )[:top_k]

                return {
                    "search_type": "aggregation",
                    "group_by_field": group_by_field,
                    "aggregate_field": aggregate_field,
                    "aggregate_func": aggregate_func,
                    "total_groups": len(sorted_groups),
                    "results": sorted_groups
                }

        except Exception as e:
            logger.error(f"聚合搜索失败: {e}")
            return {"search_type": "aggregation", "error": str(e), "results": []}

    async def cleanup_all_connections(self):
        """清理所有连接"""
        logger.info("🧹 开始清理Milvus连接...")

        # 停止健康检查
        await self.stop_health_monitoring()

        # 清理活跃集合
        self.active_collections.clear()

        # 关闭所有连接
        for connection_name in list(self.connections.keys()):
            await self.close_connection(connection_name)

        self.connections.clear()

        logger.info("✅ Milvus连接清理完成")


# 全局管理器实例
_global_milvus_manager: Optional[UnifiedMilvusManager] = None


def get_milvus_manager() -> UnifiedMilvusManager:
    """
    获取全局Milvus管理器实例
    
    Returns:
        UnifiedMilvusManager: 管理器实例
    """
    global _global_milvus_manager
    if _global_milvus_manager is None:
        _global_milvus_manager = UnifiedMilvusManager()
    return _global_milvus_manager


if __name__ == "__main__":
    # 测试模块
    async def test_unified_milvus_manager():
        """测试统一Milvus管理器"""
        
        print("\n" + "="*70)
        print("🗃️ 统一Milvus集合管理器测试")
        print("="*70)
        
        # 创建管理器
        manager = UnifiedMilvusManager()
        
        # 测试集合名称获取
        print("\n📝 测试统一集合命名...")
        for collection_type in CollectionType:
            name = manager.get_collection_name(collection_type)
            print(f"  {collection_type.value}: {name}")
        
        # 测试向量维度验证
        print(f"\n🔍 测试向量维度验证...")
        
        # 正确的4096维向量
        valid_vectors = [[0.1] * 4096, [0.2] * 4096]
        result = await manager.validate_vector_dimension(valid_vectors)
        print(f"  4096维向量验证: {'✅ 通过' if result else '❌ 失败'}")
        
        # 错误的维度向量
        invalid_vectors = [[0.1] * 512, [0.2] * 1024]  # 错误维度
        result = await manager.validate_vector_dimension(invalid_vectors)
        print(f"  错误维度向量验证: {'❌ 正确拒绝' if not result else '✅ 意外通过'}")
        
        # 测试连接管理
        print(f"\n🔗 测试连接管理...")
        try:
            connection_name = await manager.create_connection("test_connection")
            print(f"  连接创建: {'✅ 成功' if connection_name else '❌ 失败'}")
            
            # 测试集合创建
            if MILVUS_AVAILABLE:
                print(f"\n📦 测试集合创建...")
                success = await manager.create_collection(
                    CollectionType.TEST_COLLECTION,
                    connection_name
                )
                print(f"  测试集合创建: {'✅ 成功' if success else '❌ 失败'}")
            
            # 关闭连接
            await manager.close_connection(connection_name)
            print(f"  连接关闭: ✅ 完成")
            
        except Exception as e:
            print(f"  连接测试: ❌ 失败 - {e}")
        
        # 显示系统状态
        print(f"\n📊 系统状态:")
        status = manager.get_system_status()
        
        print(f"  Milvus可用: {status['milvus_available']}")
        print(f"  向量维度: {status['server_info']['vector_dimension']}")
        print(f"  活跃连接: {status['connections']['active']}/{status['connections']['total']}")
        print(f"  注册集合: {status['collections']['total']}")
        print(f"  健康集合: {status['collections']['healthy']}")

        if status['recent_operations']:
            print(f"  最近操作:")
            for op in status['recent_operations'][-3:]:
                print(f"    - {op['operation']}: {op.get('collection_name', 'N/A')} "
                     f"({'✅' if op['success'] else '❌'})")

        # 清理
        await manager.cleanup_all_connections()

        print("\n" + "="*70)

    # ========================================
    # 6种搜索功能实现
    # ========================================

    async def vector_search(self,
                           collection_name: str,
                           query_vectors: List[List[float]],
                           top_k: int = 10,
                           output_fields: Optional[List[str]] = None,
                           expr: Optional[str] = None,
                           search_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        1. 向量搜索 (Vector Search)
        基于向量相似度的搜索，适用于语义搜索场景
        """
        collection_type = self.get_collection_type_by_name(collection_name)
        async with self.get_collection(collection_type) as collection:
            if output_fields is None:
                output_fields = ["doc_id", "stock_code", "company_name", "text_content", "publish_date", "importance_score"]

            if search_params is None:
                search_params = {
                    "metric_type": "COSINE",
                    "params": {"ef": 128}
                }

            try:
                results = collection.search(
                    data=query_vectors,
                    anns_field="embedding",
                    param=search_params,
                    limit=top_k,
                    expr=expr,
                    output_fields=output_fields,
                    consistency_level="Session"
                )

                return {
                    "search_type": "vector",
                    "total_results": len(results),
                    "results": [
                        {
                            "id": hit.id,
                            "distance": hit.distance,
                            "entity": hit.entity.to_dict() if hasattr(hit.entity, 'to_dict') else dict(hit.entity)
                        } for result in results for hit in result
                    ],
                    "search_params": search_params
                }
            except Exception as e:
                logger.error(f"向量搜索失败: {e}")
                return {"search_type": "vector", "error": str(e), "results": []}

    async def text_search(self,
                         collection_name: str,
                         query_text: str,
                         top_k: int = 10,
                         output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        2. 文本搜索 (Text Search)
        基于文本内容的全文搜索
        """
        collection_type = self.get_collection_type_by_name(collection_name)
        async with self.get_collection(collection_type) as collection:
            if output_fields is None:
                output_fields = ["doc_id", "stock_code", "company_name", "text_content", "publish_date", "importance_score"]

            try:
                # 构建文本匹配表达式
                text_expr = f'text_content like "%{query_text}%"'

                results = collection.query(
                    expr=text_expr,
                    output_fields=output_fields,
                    limit=top_k,
                    consistency_level="Session"
                )

                return {
                    "search_type": "text",
                    "query_text": query_text,
                    "total_results": len(results),
                    "results": results,
                    "text_expr": text_expr
                }
            except Exception as e:
                logger.error(f"文本搜索失败: {e}")
                return {"search_type": "text", "error": str(e), "results": []}

    async def hybrid_search(self,
                           collection_name: str,
                           query_vector: List[float],
                           query_text: str,
                           top_k: int = 10,
                           vector_weight: float = 0.7,
                           text_weight: float = 0.3,
                           output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        3. 混合搜索 (Hybrid Search)
        结合向量相似度和文本匹配的混合搜索
        """
        try:
            # 执行向量搜索
            vector_results = await self.vector_search(
                collection_name=collection_name,
                query_vectors=[query_vector],
                top_k=top_k * 2,  # 获取更多结果用于混合排序
                output_fields=output_fields
            )

            # 执行文本搜索
            text_results = await self.text_search(
                collection_name=collection_name,
                query_text=query_text,
                top_k=top_k * 2,
                output_fields=output_fields
            )

            # 混合评分
            hybrid_scores = {}

            # 向量搜索结果评分
            for result in vector_results.get("results", []):
                doc_id = result["entity"]["doc_id"]
                vector_score = 1.0 - result["distance"]  # 距离越小，相似度越高
                hybrid_scores[doc_id] = {"vector_score": vector_score, "text_score": 0.0, "data": result}

            # 文本搜索结果评分
            max_text_score = len(text_results.get("results", []))
            for i, result in enumerate(text_results.get("results", [])):
                doc_id = result["doc_id"]
                text_score = (max_text_score - i) / max_text_score  # 排名越前，得分越高

                if doc_id in hybrid_scores:
                    hybrid_scores[doc_id]["text_score"] = text_score
                else:
                    hybrid_scores[doc_id] = {
                        "vector_score": 0.0,
                        "text_score": text_score,
                        "data": {"entity": result}
                    }

            # 计算混合得分并排序
            final_results = []
            for doc_id, scores in hybrid_scores.items():
                final_score = (scores["vector_score"] * vector_weight +
                             scores["text_score"] * text_weight)

                result_data = scores["data"]
                result_data["hybrid_score"] = final_score
                result_data["vector_score"] = scores["vector_score"]
                result_data["text_score"] = scores["text_score"]

                final_results.append(result_data)

            # 按混合得分排序并截取前top_k
            final_results.sort(key=lambda x: x["hybrid_score"], reverse=True)
            final_results = final_results[:top_k]

            return {
                "search_type": "hybrid",
                "query_text": query_text,
                "vector_weight": vector_weight,
                "text_weight": text_weight,
                "total_results": len(final_results),
                "results": final_results
            }

        except Exception as e:
            logger.error(f"混合搜索失败: {e}")
            return {"search_type": "hybrid", "error": str(e), "results": []}

    async def range_search(self,
                          collection_name: str,
                          field_conditions: Dict[str, Any],
                          top_k: int = 10,
                          output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        4. 范围搜索 (Range Search)
        基于标量字段的范围条件搜索
        """
        collection_type = self.get_collection_type_by_name(collection_name)
        async with self.get_collection(collection_type) as collection:
            if output_fields is None:
                output_fields = ["doc_id", "stock_code", "company_name", "text_content", "publish_date", "importance_score"]

            try:
                # 构建范围查询表达式
                conditions = []

                for field, condition in field_conditions.items():
                    if isinstance(condition, dict):
                        if "min" in condition and "max" in condition:
                            conditions.append(f"{field} >= {condition['min']} and {field} <= {condition['max']}")
                        elif "min" in condition:
                            conditions.append(f"{field} >= {condition['min']}")
                        elif "max" in condition:
                            conditions.append(f"{field} <= {condition['max']}")
                        elif "eq" in condition:
                            if isinstance(condition["eq"], str):
                                conditions.append(f'{field} == "{condition["eq"]}"')
                            else:
                                conditions.append(f"{field} == {condition['eq']}")
                        elif "in" in condition:
                            values = condition["in"]
                            if isinstance(values[0], str):
                                in_clause = ", ".join([f'"{v}"' for v in values])
                            else:
                                in_clause = ", ".join([str(v) for v in values])
                            conditions.append(f"{field} in [{in_clause}]")
                    else:
                        # 直接值比较
                        if isinstance(condition, str):
                            conditions.append(f'{field} == "{condition}"')
                        else:
                            conditions.append(f"{field} == {condition}")

                expr = " and ".join(conditions)

                results = collection.query(
                    expr=expr,
                    output_fields=output_fields,
                    limit=top_k,
                    consistency_level="Session"
                )

                return {
                    "search_type": "range",
                    "field_conditions": field_conditions,
                    "expr": expr,
                    "total_results": len(results),
                    "results": results
                }

            except Exception as e:
                logger.error(f"范围搜索失败: {e}")
                return {"search_type": "range", "error": str(e), "results": []}

    async def multi_vector_search(self,
                                 collection_name: str,
                                 vector_queries: Dict[str, List[float]],
                                 top_k: int = 10,
                                 combine_method: str = "weighted_sum",
                                 output_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        5. 多向量搜索 (Multi-Vector Search)
        支持多个向量字段的联合搜索
        注意：当前schema只有一个向量字段，此方法为未来多向量字段预留
        """
        try:
            if len(vector_queries) == 1:
                # 单向量情况，直接调用向量搜索
                vector_field, query_vector = next(iter(vector_queries.items()))
                return await self.vector_search(
                    collection_name=collection_name,
                    query_vectors=[query_vector],
                    top_k=top_k,
                    output_fields=output_fields
                )

            # 多向量搜索逻辑（当前schema不支持，为未来扩展预留）
            all_results = {}

            for field_name, query_vector in vector_queries.items():
                if field_name == "embedding":  # 当前唯一的向量字段
                    results = await self.vector_search(
                        collection_name=collection_name,
                        query_vectors=[query_vector],
                        top_k=top_k * 2,
                        output_fields=output_fields
                    )
                    all_results[field_name] = results

            # 结果合并逻辑
            if combine_method == "weighted_sum":
                # 加权求和合并
                combined_scores = {}
                weight = 1.0 / len(vector_queries)  # 平均权重

                for field_name, results in all_results.items():
                    for result in results.get("results", []):
                        doc_id = result["entity"]["doc_id"]
                        score = 1.0 - result["distance"]

                        if doc_id in combined_scores:
                            combined_scores[doc_id]["score"] += score * weight
                        else:
                            combined_scores[doc_id] = {
                                "score": score * weight,
                                "data": result
                            }

                # 排序并返回结果
                final_results = sorted(
                    combined_scores.values(),
                    key=lambda x: x["score"],
                    reverse=True
                )[:top_k]

                return {
                    "search_type": "multi_vector",
                    "vector_fields": list(vector_queries.keys()),
                    "combine_method": combine_method,
                    "total_results": len(final_results),
                    "results": [r["data"] for r in final_results]
                }

        except Exception as e:
            logger.error(f"多向量搜索失败: {e}")
            return {"search_type": "multi_vector", "error": str(e), "results": []}

    async def aggregation_search(self,
                                collection_name: str,
                                group_by_field: str,
                                aggregate_field: Optional[str] = None,
                                aggregate_func: str = "count",
                                query_vector: Optional[List[float]] = None,
                                top_k: int = 10,
                                expr: Optional[str] = None) -> Dict[str, Any]:
        """
        6. 聚合搜索 (Aggregation Search)
        支持分组统计和聚合计算的搜索
        """
        try:
            collection_type = self.get_collection_type_by_name(collection_name)
            async with self.get_collection(collection_type) as collection:
                # 首先获取原始数据
                output_fields = [group_by_field]
                if aggregate_field:
                    output_fields.append(aggregate_field)

                # 添加其他有用字段
                additional_fields = ["doc_id", "stock_code", "company_name", "publish_date"]
                for field in additional_fields:
                    if field not in output_fields:
                        output_fields.append(field)

                if query_vector:
                    # 基于向量相似度的聚合搜索
                    search_results = await self.vector_search(
                        collection_name=collection_name,
                        query_vectors=[query_vector],
                        top_k=top_k * 10,  # 获取更多数据用于聚合
                        output_fields=output_fields,
                        expr=expr
                    )
                    raw_data = [r["entity"] for r in search_results.get("results", [])]
                else:
                    # 基于条件的聚合查询
                    raw_data = collection.query(
                        expr=expr or "doc_id != ''",
                        output_fields=output_fields,
                        limit=top_k * 50,  # 获取足够数据用于聚合
                        consistency_level="Session"
                    )

                # 执行聚合计算
                groups = {}
                for record in raw_data:
                    group_key = record.get(group_by_field, "未知")

                    if group_key not in groups:
                        groups[group_key] = {
                            "group_value": group_key,
                            "count": 0,
                            "records": []
                        }

                    groups[group_key]["count"] += 1
                    groups[group_key]["records"].append(record)

                    # 聚合字段计算
                    if aggregate_field and aggregate_field in record:
                        if "aggregate_values" not in groups[group_key]:
                            groups[group_key]["aggregate_values"] = []
                        groups[group_key]["aggregate_values"].append(record[aggregate_field])

                # 计算聚合函数结果
                for group_key, group_data in groups.items():
                    if aggregate_field and "aggregate_values" in group_data:
                        values = group_data["aggregate_values"]
                        if aggregate_func == "sum":
                            group_data["aggregate_result"] = sum(values)
                        elif aggregate_func == "avg":
                            group_data["aggregate_result"] = sum(values) / len(values) if values else 0
                        elif aggregate_func == "min":
                            group_data["aggregate_result"] = min(values) if values else None
                        elif aggregate_func == "max":
                            group_data["aggregate_result"] = max(values) if values else None
                        else:  # count
                            group_data["aggregate_result"] = len(values)

                # 排序并限制结果
                sorted_groups = sorted(
                    groups.values(),
                    key=lambda x: x.get("aggregate_result", x["count"]),
                    reverse=True
                )[:top_k]

                return {
                    "search_type": "aggregation",
                    "group_by_field": group_by_field,
                    "aggregate_field": aggregate_field,
                    "aggregate_func": aggregate_func,
                    "total_groups": len(sorted_groups),
                    "results": sorted_groups
                }

        except Exception as e:
            logger.error(f"聚合搜索失败: {e}")
            return {"search_type": "aggregation", "error": str(e), "results": []}

    # 运行测试
    asyncio.run(test_unified_milvus_manager())
