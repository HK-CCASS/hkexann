"""
Milvus集合管理器
负责创建、管理和操作Milvus向量集合
"""

import logging
import warnings
from typing import List, Dict, Any, Optional

# 抑制 protobuf 兼容性警告
warnings.filterwarnings('ignore', category=UserWarning, module='google.protobuf.runtime_version')

from pymilvus import (
    connections, Collection, FieldSchema, CollectionSchema, DataType,
    utility
)

from config.settings import settings

logger = logging.getLogger(__name__)


class MilvusCollectionManager:
    """Milvus集合管理器"""
    
    def __init__(self, 
                 host: str = None,
                 port: str = None,
                 user: str = None,
                 password: str = None,
                 connection_name: str = "default"):
        """
        初始化Milvus集合管理器
        
        Args:
            host: Milvus主机地址
            port: Milvus端口
            user: 用户名
            password: 密码
            connection_name: 连接名称
        """
        self.host = host or settings.milvus_host
        self.port = port or str(settings.milvus_port)
        self.user = user or settings.milvus_user
        self.password = password or settings.milvus_password
        self.connection_name = connection_name
        
        self.embedding_dim = settings.embedding_dimension
        
        # 连接状态 - 改为持久连接模式
        self._connected = False
        self._auto_connect = True  # 自动连接模式
        
        logger.info(f"初始化Milvus集合管理器 - {self.host}:{self.port}")
    
    def connect(self) -> bool:
        """连接到Milvus"""
        try:
            # 检查连接是否已存在且有效
            if self._connected:
                try:
                    # 测试连接是否有效
                    utility.get_server_version(using=self.connection_name)
                    return True
                except:
                    self._connected = False
            
            # 先断开已有连接
            try:
                connections.disconnect(self.connection_name)
            except:
                pass  # 忽略断开失败的错误
            
            # 建立新连接
            connections.connect(
                alias=self.connection_name,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            
            self._connected = True
            logger.info("✅ Milvus连接成功")
            return True
            
        except Exception as e:
            logger.error(f"❌ Milvus连接失败: {e}")
            self._connected = False
            return False
    
    def disconnect(self, force: bool = False):
        """断开Milvus连接
        
        Args:
            force: 是否强制断开连接，默认False（保持连接复用）
        """
        if force and self._connected:
            try:
                connections.disconnect(self.connection_name)
                self._connected = False
                logger.info("Milvus连接已强制断开")
            except Exception as e:
                logger.error(f"断开Milvus连接失败: {e}")
        elif self._auto_connect:
            # 在自动连接模式下，保持连接不断开
            logger.debug("自动连接模式：保持Milvus连接")
            
    def ensure_connection(self) -> bool:
        """确保Milvus连接有效"""
        if not self._connected or not self._auto_connect:
            return self.connect()
        
        try:
            # 测试连接是否有效
            utility.get_server_version(using=self.connection_name)
            return True
        except:
            logger.warning("Milvus连接已失效，尝试重新连接...")
            self._connected = False
            return self.connect()
    
    def create_pdf_embeddings_collection(self) -> bool:
        """
        创建PDF嵌入向量集合 v3
        
        v3架构增强：
        - 新增publish_date：支持时间范围查询
        - 新增page_number：支持页码级精确定位  
        - 新增importance_score：支持质量优先排序
        - 新增chunk_length：文本长度统计
        - 新增file_path：完整文件路径信息
        - 扩展text_content：20K→50K字符支持大文档
        
        Returns:
            是否创建成功
        """
        collection_name = settings.pdf_embeddings_collection
        
        try:
            # 检查集合是否已存在
            if utility.has_collection(collection_name):
                logger.info(f"集合 {collection_name} 已存在")
                return True
            
            # 定义v3字段模式
            fields = [
                # 主键字段
                FieldSchema(
                    name="id",
                    dtype=DataType.VARCHAR,
                    max_length=64,
                    is_primary=True,
                    description="向量ID"
                ),
                
                # 向量字段
                FieldSchema(
                    name="embedding",
                    dtype=DataType.FLOAT_VECTOR,
                    dim=self.embedding_dim,
                    description="4096维嵌入向量"
                ),
                
                # 文档标识字段
                FieldSchema(
                    name="doc_id",
                    dtype=DataType.VARCHAR,
                    max_length=64,
                    description="文档ID"
                ),
                
                FieldSchema(
                    name="chunk_id",
                    dtype=DataType.VARCHAR,
                    max_length=64,
                    description="文档块ID"
                ),
                
                # 业务字段 (优化)
                FieldSchema(
                    name="stock_code",
                    dtype=DataType.VARCHAR,
                    max_length=10,  # 缩短长度，港股代码最多5位
                    description="股票代码"
                ),
                
                FieldSchema(
                    name="company_name",
                    dtype=DataType.VARCHAR,
                    max_length=500,
                    description="公司名称"
                ),
                
                FieldSchema(
                    name="document_type",
                    dtype=DataType.VARCHAR,
                    max_length=100,
                    description="文档类型"
                ),
                
                FieldSchema(
                    name="document_category",
                    dtype=DataType.VARCHAR,
                    max_length=200,
                    description="文档分类"
                ),

                FieldSchema(
                    name="document_title",
                    dtype=DataType.VARCHAR,
                    max_length=500,
                    description="文档标题"
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
                    description="HKEX 2级分类代码"
                ),

                FieldSchema(
                    name="hkex_level2_name",
                    dtype=DataType.VARCHAR,
                    max_length=500,
                    description="HKEX 2级分类名称"
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
                    max_length=1000,
                    description="HKEX完整分类路径"
                ),

                FieldSchema(
                    name="hkex_classification_method",
                    dtype=DataType.VARCHAR,
                    max_length=50,
                    description="分类方法标识"
                ),
                
                FieldSchema(
                    name="chunk_type",
                    dtype=DataType.VARCHAR,
                    max_length=50,
                    description="块类型"
                ),
                
                # v3新增字段
                FieldSchema(
                    name="publish_date",
                    dtype=DataType.INT64,
                    description="文档发布日期时间戳"
                ),
                
                FieldSchema(
                    name="page_number",
                    dtype=DataType.INT32,
                    description="chunk所在页码"
                ),
                
                FieldSchema(
                    name="file_path",
                    dtype=DataType.VARCHAR,
                    max_length=500,
                    description="原始文件路径"
                ),
                
                FieldSchema(
                    name="chunk_length",
                    dtype=DataType.INT32,
                    description="文本字符数长度"
                ),
                
                FieldSchema(
                    name="importance_score",
                    dtype=DataType.FLOAT,
                    description="chunk重要性评分(0-1)"
                ),
                
                # 内容字段 (扩容)
                FieldSchema(
                    name="text_content",
                    dtype=DataType.VARCHAR,
                    max_length=50000,  # 从20K扩展到50K
                    description="文本内容"
                ),
                
                # 元数据字段
                FieldSchema(
                    name="metadata",
                    dtype=DataType.JSON,
                    description="业务元数据"
                ),
                
                # 时间戳
                FieldSchema(
                    name="created_at",
                    dtype=DataType.INT64,
                    description="创建时间戳"
                )
            ]
            
            # 创建集合模式
            schema = CollectionSchema(
                fields=fields,
                description="PDF文档嵌入向量集合，支持4096维向量"
            )
            
            # 创建集合
            collection = Collection(
                name=collection_name,
                schema=schema,
                using=self.connection_name
            )
            
            logger.info(f"✅ 集合 {collection_name} 创建成功")
            
            # 创建索引
            self._create_pdf_collection_index(collection)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建集合 {collection_name} 失败: {e}")
            return False
    
    def _create_pdf_collection_index(self, collection: Collection):
        """为PDF集合创建索引"""
        try:
            # 向量索引 - 使用IVF_PQ用于大规模数据
            vector_index_params = {
                "metric_type": "COSINE",  # 余弦相似度
                "index_type": "IVF_PQ",   # 倒排文件索引 + 乘积量化
                "params": {
                    "nlist": 2048,       # 增加聚类中心数量以支持更大数据量
                    "m": 8,               # PQ子空间数量
                    "nbits": 8            # 每个子空间的位数
                }
            }
            
            collection.create_index(
                field_name="embedding",
                index_params=vector_index_params
            )
            
            logger.info("✅ 向量索引创建成功")
            
            # v3标量字段索引 - 支持6种搜索类型
            scalar_indexes = [
                # 基础业务字段 (Range Search)
                ("stock_code", {}),
                ("document_type", {}),
                ("chunk_type", {}),
                ("doc_id", {}),
                ("chunk_id", {}),

                # v3关键字段索引 (Range Search + Aggregation Search)
                ("publish_date", {}),     # 时间范围查询
                ("importance_score", {}), # 质量排序
                ("page_number", {}),      # 页码定位
                ("chunk_length", {}),     # 长度过滤

                # HKEX 3级分类索引 (Range Search + Aggregation Search + Text Search)
                ("hkex_level1_code", {}),        # 1级分类代码范围查询
                ("hkex_level1_name", {}),        # 1级分类名称文本搜索
                ("hkex_level2_code", {}),        # 2级分类代码范围查询
                ("hkex_level2_name", {}),        # 2级分类名称文本搜索
                ("hkex_level3_code", {}),        # 3级分类代码范围查询
                ("hkex_level3_name", {}),        # 3级分类名称文本搜索

                # HKEX分类元数据索引 (Multi-Vector Search + Hybrid Search)
                ("hkex_classification_confidence", {}),  # 置信度范围查询
                ("hkex_full_path", {}),                   # 完整路径文本搜索
                ("hkex_classification_method", {})        # 分类方法聚合查询
            ]
            
            for field_name, params in scalar_indexes:
                try:
                    collection.create_index(
                        field_name=field_name,
                        index_params=params
                    )
                    logger.debug(f"标量索引 {field_name} 创建成功")
                except Exception as e:
                    logger.warning(f"标量索引 {field_name} 创建失败: {e}")
            
            logger.info("✅ 所有索引创建完成")
            
        except Exception as e:
            logger.error(f"❌ 创建索引失败: {e}")
    
    
    
    def create_collection_by_name(self, collection_name: str) -> bool:
        """
        按指定名称创建PDF嵌入向量集合
        
        Args:
            collection_name: 集合名称
            
        Returns:
            是否创建成功
        """
        try:
            # 检查集合是否已存在
            if utility.has_collection(collection_name):
                logger.info(f"集合 {collection_name} 已存在")
                return True
            
            # 定义v3字段模式 (与create_pdf_embeddings_collection相同)
            fields = [
                # 主键字段
                FieldSchema(
                    name="id",
                    dtype=DataType.VARCHAR,
                    max_length=64,
                    is_primary=True,
                    description="向量ID"
                ),
                
                # 向量字段
                FieldSchema(
                    name="embedding",
                    dtype=DataType.FLOAT_VECTOR,
                    dim=self.embedding_dim,
                    description="4096维嵌入向量"
                ),
                
                # 文档标识字段
                FieldSchema(
                    name="doc_id",
                    dtype=DataType.VARCHAR,
                    max_length=64,
                    description="文档ID"
                ),
                
                FieldSchema(
                    name="chunk_id",
                    dtype=DataType.VARCHAR,
                    max_length=64,
                    description="文档块ID"
                ),
                
                # 业务字段 (优化)
                FieldSchema(
                    name="stock_code",
                    dtype=DataType.VARCHAR,
                    max_length=10,  # 缩短长度，港股代码最多5位
                    description="股票代码"
                ),
                
                FieldSchema(
                    name="company_name",
                    dtype=DataType.VARCHAR,
                    max_length=500,
                    description="公司名称"
                ),
                
                FieldSchema(
                    name="document_type",
                    dtype=DataType.VARCHAR,
                    max_length=100,
                    description="文档类型"
                ),
                
                FieldSchema(
                    name="document_title",
                    dtype=DataType.VARCHAR,
                    max_length=1000,
                    description="文档标题"
                ),

                # HKEX官方3级分类系统 (与主方法保持一致)
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
                    description="HKEX 2级分类代码"
                ),

                FieldSchema(
                    name="hkex_level2_name",
                    dtype=DataType.VARCHAR,
                    max_length=500,
                    description="HKEX 2级分类名称"
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

                # HKEX分类元数据字段
                FieldSchema(
                    name="hkex_classification_confidence",
                    dtype=DataType.FLOAT,
                    description="HKEX分类置信度"
                ),

                FieldSchema(
                    name="hkex_full_path",
                    dtype=DataType.VARCHAR,
                    max_length=1000,
                    description="HKEX完整分类路径"
                ),

                FieldSchema(
                    name="hkex_classification_method",
                    dtype=DataType.VARCHAR,
                    max_length=50,
                    description="分类方法标识"
                ),

                # 内容字段
                FieldSchema(
                    name="text_content",
                    dtype=DataType.VARCHAR,
                    max_length=50000,  # v3: 大幅扩容支持更长文本
                    description="文本内容"
                ),
                
                FieldSchema(
                    name="chunk_type",
                    dtype=DataType.VARCHAR,
                    max_length=50,
                    description="块类型"
                ),
                
                # 新增字段 v3
                FieldSchema(
                    name="publish_date",
                    dtype=DataType.VARCHAR,  # 使用字符串存储，避免日期解析问题
                    max_length=20,
                    description="发布日期"
                ),
                
                FieldSchema(
                    name="page_number",
                    dtype=DataType.INT64,
                    description="页码"
                ),
                
                FieldSchema(
                    name="importance_score",
                    dtype=DataType.FLOAT,
                    description="重要性评分"
                ),
                
                FieldSchema(
                    name="chunk_length",
                    dtype=DataType.INT64,
                    description="文本长度"
                ),
                
                FieldSchema(
                    name="file_path",
                    dtype=DataType.VARCHAR,
                    max_length=1000,
                    description="文件路径"
                ),
                
                # 时间戳字段
                FieldSchema(
                    name="created_at",
                    dtype=DataType.VARCHAR,
                    max_length=30,
                    description="创建时间"
                ),
                
                FieldSchema(
                    name="vector_id",
                    dtype=DataType.VARCHAR,
                    max_length=50,
                    description="向量标识符"
                )
            ]
            
            # 创建schema
            schema = CollectionSchema(fields, description=f"PDF嵌入向量集合v3 - {collection_name}")
            
            # 创建集合
            collection = Collection(name=collection_name, schema=schema)
            logger.info(f"✅ 集合 {collection_name} 创建成功")
            
            # 创建索引
            self._create_pdf_collection_index(collection)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建集合 {collection_name} 失败: {e}")
            return False
    
    def list_collections(self) -> List[str]:
        """列出所有集合"""
        try:
            collections = utility.list_collections()
            logger.info(f"现有集合: {collections}")
            return collections
        except Exception as e:
            logger.error(f"获取集合列表失败: {e}")
            return []
    
    def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """获取集合信息"""
        try:
            if not utility.has_collection(collection_name):
                return {"error": "集合不存在"}
            
            collection = Collection(collection_name, using=self.connection_name)
            
            # 加载集合
            collection.load()
            
            info = {
                "name": collection_name,
                "description": collection.description,
                "num_entities": collection.num_entities,
                "primary_field": collection.primary_field.name,
                "schema": {
                    "fields": [
                        {
                            "name": field.name,
                            "type": str(field.dtype),
                            "description": field.description
                        }
                        for field in collection.schema.fields
                    ]
                },
                "indexes": []
            }
            
            # 获取索引信息
            for field in collection.schema.fields:
                try:
                    if field.dtype in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
                        index_info = collection.index(field.name)
                        if index_info:
                            info["indexes"].append({
                                "field": field.name,
                                "index_type": index_info.params.get("index_type"),
                                "metric_type": index_info.params.get("metric_type")
                            })
                except:
                    pass
            
            return info
            
        except Exception as e:
            logger.error(f"获取集合 {collection_name} 信息失败: {e}")
            return {"error": str(e)}
    
    def insert_vectors(self, 
                      collection_name: str, 
                      data: List[Dict[str, Any]]) -> bool:
        """插入向量数据"""
        try:
            # 确保连接有效
            if not self.ensure_connection():
                raise Exception("无法建立Milvus连接")
                
            collection = Collection(collection_name, using=self.connection_name)
            
            # 转换数据格式
            insert_data = []
            for field in collection.schema.fields:
                field_data = [record.get(field.name) for record in data]
                insert_data.append(field_data)
            
            # 插入数据
            mr = collection.insert(insert_data)
            collection.flush()
            
            logger.info(f"✅ 向集合 {collection_name} 插入 {len(data)} 条数据")
            return True
            
        except Exception as e:
            logger.error(f"❌ 插入数据到集合 {collection_name} 失败: {e}")
            return False
    
    def search_vectors(self,
                      collection_name: str,
                      query_vectors: List[List[float]],
                      limit: int = 10,
                      search_params: Dict = None) -> List[List[Dict]]:
        """搜索相似向量"""
        try:
            collection = Collection(collection_name, using=self.connection_name)
            collection.load()
            
            if search_params is None:
                search_params = {
                    "metric_type": "COSINE",
                    "params": {"nprobe": 16}
                }
            
            results = collection.search(
                data=query_vectors,
                anns_field="embedding",
                param=search_params,
                limit=limit,
                output_fields=["*"]  # 返回所有字段
            )
            
            # 转换结果格式
            formatted_results = []
            for hits in results:
                hit_list = []
                for hit in hits:
                    hit_data = {
                        "id": hit.id,
                        "distance": hit.distance,
                        "score": 1 - hit.distance,  # 转换为相似度分数
                        "entity": hit.entity
                    }
                    hit_list.append(hit_data)
                formatted_results.append(hit_list)
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"搜索集合 {collection_name} 失败: {e}")
            return []
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


# 全局集合管理器实例
_collection_manager: Optional[MilvusCollectionManager] = None


def get_collection_manager() -> MilvusCollectionManager:
    """获取全局集合管理器实例"""
    global _collection_manager
    if _collection_manager is None:
        _collection_manager = MilvusCollectionManager()
    return _collection_manager
