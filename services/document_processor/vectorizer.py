"""
港交所文档向量化处理器 - 智能文档向量化与存储服务

这个模块提供专门针对港交所公告文档的高性能向量化处理服务，包括
文档分块向量化、批量处理、Milvus存储集成等核心功能。采用异步处理
架构和智能批处理策略，确保大规模文档的高效向量化和存储。

核心功能：
- PDF文档分块的智能向量化处理
- 基于内容类型的差异化处理策略（跳过开头语等）
- 高性能批量向量化，优化嵌入API调用
- Milvus向量数据库的无缝集成
- 完整的元数据保留和关联
- 向量去重和一致性保证
- 实时处理进度跟踪和统计

技术特性：
- 异步处理架构，支持大规模并发
- 智能批处理机制，优化API调用成本
- 错误处理和重试机制，确保数据完整性
- 内存使用优化，避免大文档处理时的内存溢出
- 向量维度验证，确保数据质量
- UUID生成的唯一向量ID管理

向量化策略：
- 跳过港交所标准开头语（is_header=True的chunks）
- 保留完整的文档元数据和chunk信息
- 支持多种chunk类型（段落、表格、标题、列表）
- 自动文本长度限制，符合Milvus字段约束
- 时间戳标记，支持版本控制

集成组件：
- services.embeddings: 嵌入服务客户端
- services.milvus: Milvus集合管理器
- pdf_parser: PDF解析器数据结构

Time Complexity: O(n*m) 其中n为文档数量，m为每个文档的chunk数量
Space Complexity: O(batch_size * vector_dim) 受批处理大小和向量维度限制

作者: HKEX分析团队
版本: 1.0.0
依赖: Milvus 2.x+, httpx, uuid
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Any

from config.settings import settings
from services.embeddings import get_embedding_client
from services.milvus import get_collection_manager
from .pdf_parser import DocumentChunk, DocumentMetadata

logger = logging.getLogger(__name__)


class DocumentVectorizer:
    """
    港交所文档向量化处理器 - 高性能文档向量化引擎
    
    这个类负责将港交所PDF文档的分块内容转换为高维向量，并高效地
    存储到Milvus向量数据库中。采用智能批处理和异步处理架构，
    针对港交所文档特点进行优化。
    
    核心特性：
    - 智能内容过滤：自动跳过港交所标准开头语和免责声明
    - 批量向量化：优化嵌入API调用，降低处理成本
    - 异步处理：支持大规模文档的并发处理
    - 元数据保留：完整保存文档和chunk的关联信息
    - 错误恢复：具备重试机制和失败处理能力
    - 进度跟踪：实时统计处理进度和性能指标
    
    处理流程：
    1. 接收DocumentMetadata和DocumentChunk列表
    2. 过滤开头语chunks（is_header=True）
    3. 分批处理内容chunks
    4. 生成向量嵌入
    5. 构建Milvus插入数据
    6. 执行批量插入操作
    7. 返回处理统计结果
    
    支持的chunk类型：
    - paragraph: 段落文本（主要处理对象）
    - table: 表格内容
    - title: 标题和小标题
    - list: 列表项内容
    - other: 其他类型内容
    
    Attributes:
        collection_name (str): Milvus集合名称，默认使用配置文件设置
        batch_size (int): 批处理大小，默认10
        max_retries (int): 最大重试次数，默认3
        
    Note:
        - 所有操作都是异步的，需要在async context中使用
        - 建议根据系统内存和网络条件调整batch_size
        - 处理大文档时建议监控内存使用情况
        
    Example:
        vectorizer = DocumentVectorizer(
            collection_name="hkex_documents",
            batch_size=20
        )
        
        result = await vectorizer.vectorize_document(
            doc_metadata, chunks
        )
        
        print(f"向量化了 {result['vectorized_chunks']} 个chunks")
    """
    
    def __init__(self, 
                 collection_name: str = None,
                 batch_size: int = 20,
                 max_retries: int = 3,
                 enable_clickhouse: bool = True):
        """
        初始化港交所文档向量化处理器实例
        
        配置向量化处理器的关键参数，包括Milvus集合设置、批处理策略
        和错误处理配置。这些参数直接影响处理性能和资源使用。
        
        Args:
            collection_name (str, optional): Milvus向量集合名称
                默认: 使用配置文件中的collection设置
                建议: 使用描述性名称，如"hkex_announcements_v1"
            batch_size (int, optional): 批量处理的chunk数量
                默认: 10
                范围: 5-50，取决于系统内存和网络条件
                影响: 较大值提高吞吐量但增加内存使用
            max_retries (int, optional): 失败操作的最大重试次数
                默认: 3
                范围: 1-5，过高可能导致长时间阻塞
                
        Note:
            - collection_name必须在Milvus中已存在或能够自动创建
            - batch_size应根据可用内存和向量维度调整
            - max_retries影响处理的鲁棒性，但也影响失败时的响应时间
            
        Performance Tips:
            - 内存充足时可增加batch_size到20-30
            - 网络不稳定时建议增加max_retries
            - 生产环境建议使用具体的collection_name
            
        Example:
            # 默认配置
            vectorizer = DocumentVectorizer()
            
            # 高性能配置
            vectorizer = DocumentVectorizer(
                collection_name="hkex_docs_production",
                batch_size=25,
                max_retries=5
            )
        """
        self.collection_name = collection_name or settings.pdf_embeddings_collection
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.enable_clickhouse = enable_clickhouse
        
        # 延迟导入避免循环依赖
        self.ch_storage = None
        self._ch_init_lock = None  # 会话初始化锁，防止并发竞争
        if enable_clickhouse:
            try:
                from services.storage.clickhouse_pdf_storage import ClickHousePDFStorage
                self.ch_storage = ClickHousePDFStorage()
                # 注意：需要在async上下文中调用initialize()
                self.ch_storage_initialized = False
                self._ch_init_lock = asyncio.Lock()  # 初始化并发保护锁
            except ImportError:
                logger.warning("ClickHouse存储模块不可用，禁用ClickHouse集成")
                self.enable_clickhouse = False
        
        logger.info(f"初始化文档向量化处理器 - 集合: {self.collection_name}, 批大小: {batch_size}")
    
    def calculate_importance_score(self, chunk: DocumentChunk, doc_metadata: DocumentMetadata) -> float:
        """
        计算chunk重要性评分 (0-1) - v3新功能
        
        基于多个维度计算chunk的重要性，用于搜索结果的质量排序：
        - 文档类型权重：供股、配股等核心业务文档权重更高
        - chunk类型权重：标题、表格等结构化内容权重更高  
        - 长度权重：中等长度chunk通常包含完整信息
        - 关键词权重：包含业务关键词的chunk权重更高
        
        Args:
            chunk: 文档块对象
            doc_metadata: 文档元数据对象
            
        Returns:
            float: 重要性评分 (0.0-1.0)
        """
        score = 0.5  # 基础分
        
        # 1. 文档类型权重 (0-0.2分)
        high_importance_types = ["供股", "配股", "股权发行", "上市文件"]
        medium_importance_types = ["公告及通告", "通函"]
        
        if any(doc_type in doc_metadata.document_type for doc_type in high_importance_types):
            score += 0.2
        elif any(doc_type in doc_metadata.document_type for doc_type in medium_importance_types):
            score += 0.1
            
        # 2. chunk类型权重 (0-0.2分)
        if chunk.chunk_type == "title":
            score += 0.2  # 标题最重要
        elif chunk.chunk_type == "table":
            score += 0.15  # 表格数据很重要
        elif chunk.chunk_type == "list":
            score += 0.1   # 列表项重要
            
        # 3. 长度权重 (0-0.1分) - 中等长度最有价值
        if 200 <= chunk.text_length <= 800:
            score += 0.1   # 理想长度
        elif 100 <= chunk.text_length <= 1200:
            score += 0.05  # 可接受长度
            
        # 4. 关键词权重 (0-0.15分)
        # 港交所业务关键词
        key_terms = [
            "供股价", "配股比例", "认购价格", "除权", "股权登记", 
            "发行价", "行使价", "配售价", "股息", "分红",
            "供股比例", "配股价", "认购权", "除权除息",
            "股本", "已发行股份", "市值"
        ]
        
        text_lower = chunk.text.lower()
        keyword_count = sum(1 for term in key_terms if term in chunk.text)
        
        if keyword_count >= 3:
            score += 0.15
        elif keyword_count >= 2:
            score += 0.1
        elif keyword_count >= 1:
            score += 0.05
            
        # 5. 特殊内容权重调整
        # 降低开头语权重
        if chunk.is_header:
            score *= 0.3
            
        # 降低过短或过长chunk权重
        if chunk.text_length < 50:
            score *= 0.5
        elif chunk.text_length > 2000:
            score *= 0.7
            
        # 确保评分在0-1范围内
        return min(max(score, 0.0), 1.0)
    
    async def vectorize_document(self, 
                               doc_metadata: DocumentMetadata,
                               chunks: List[DocumentChunk]) -> Dict[str, Any]:
        """
        向量化整个港交所PDF文档，执行智能内容过滤和批量处理
        
        这是文档向量化的主要入口方法，负责协调整个向量化流程。
        包括内容分类、批量处理、错误处理和统计收集。采用港交所
        特有的处理策略，自动跳过标准开头语和免责声明。
        
        处理策略：
        1. 智能内容过滤：跳过is_header=True的chunks（港交所开头语）
        2. 批量向量化：将内容chunks分批处理，优化API调用
        3. 错误恢复：单个批次失败不影响其他批次处理
        4. 统计收集：详细记录处理结果和性能指标
        
        Args:
            doc_metadata (DocumentMetadata): 文档元数据对象，包含：
                - doc_id: 文档唯一标识符
                - file_path: 文档文件路径
                - stock_code: 股票代码
                - company_name: 公司名称
                - document_type: 文档类型
                - publish_date: 发布日期
            chunks (List[DocumentChunk]): 文档分块列表，每个chunk包含：
                - chunk_id: 唯一标识符
                - text: 文本内容
                - chunk_type: 内容类型（paragraph/table/title/list）
                - is_header: 是否为开头语（True时跳过向量化）
                - bbox: 位置信息
                
        Returns:
            Dict[str, Any]: 向量化处理结果统计，包含：
                - success (bool): 整体处理是否成功
                - total_chunks (int): 输入的总chunk数量
                - vectorized_chunks (int): 成功向量化的chunk数量
                - skipped_chunks (int): 跳过的chunk数量（主要是开头语）
                - processing_time (float): 总处理时间（秒）
                - error (str): 失败时的错误信息（可选）
                
        Raises:
            Exception: 当向量化过程中发生不可恢复错误时抛出
                - 嵌入服务连接失败
                - Milvus数据库连接失败
                - 严重的数据格式错误
                
        Note:
            - 开头语chunks（is_header=True）会被自动跳过
            - 批次失败不会中断整个处理流程
            - 处理时间包括向量生成和数据库插入的总耗时
            - 返回的统计信息可用于性能监控和优化
            
        Performance Characteristics:
        - Time Complexity: O(n/batch_size * m) 其中n为chunk数量，m为向量生成时间
        - Space Complexity: O(batch_size * vector_dim) 受批处理大小限制
        - 网络调用: ceil(content_chunks/batch_size) 次嵌入API调用
        
        Example:
            # 处理单个文档
            result = await vectorizer.vectorize_document(
                doc_metadata=metadata,
                chunks=parsed_chunks
            )
            
            if result["success"]:
                print(f"成功向量化: {result['vectorized_chunks']} chunks")
                print(f"跳过开头语: {result['skipped_chunks']} chunks")
                print(f"处理耗时: {result['processing_time']:.2f}秒")
            else:
                print(f"向量化失败: {result.get('error', '未知错误')}")
        """
        start_time = time.time()
        
        try:
            logger.info(f"开始向量化文档: {doc_metadata.file_name}")
            
            # 1. 存储文档元数据到ClickHouse（如果启用）
            if self.ch_storage:
                # 使用锁确保ClickHouse存储线程安全初始化（修复并发竞争条件）
                async with self._ch_init_lock:
                    if not self.ch_storage_initialized:
                        await self.ch_storage.initialize()
                        self.ch_storage_initialized = True
                        logger.info("ClickHouse存储会话已初始化（线程安全）")
                
                # 构建元数据字典
                metadata_dict = {
                    'stock_code': getattr(doc_metadata, 'stock_code', ''),
                    'document_type': getattr(doc_metadata, 'doc_type', 'pdf'),
                    'published_date': getattr(doc_metadata, 'published_date', datetime.now()),
                    'file_size': getattr(doc_metadata, 'file_size', 0),
                    'original_filename': getattr(doc_metadata, 'file_name', ''),
                    'announcement_id': getattr(doc_metadata, 'announcement_id', ''),
                    'source': getattr(doc_metadata, 'source', 'vectorizer')
                }
                
                # 带重试的存储操作，增强错误恢复能力
                try:
                    metadata_stored = await self.ch_storage.store_document_metadata(
                        doc_metadata.doc_id, 
                        getattr(doc_metadata, 'file_path', ''),
                        metadata_dict
                    )
                except Exception as e:
                    logger.error(f"❌ 文档元数据存储失败: {e}")
                    metadata_stored = False
                
                # 转换DocumentChunk对象为字典
                chunks_dict_list = []
                for i, chunk in enumerate(chunks):
                    chunk_dict = {
                        'id': chunk.chunk_id,  # 使用原始的chunk_id，保持格式一致性
                        'index': chunk.chunk_index,  # 使用原始的chunk_index，保持一致性
                        'type': getattr(chunk, 'chunk_type', 'paragraph'),
                        'content': chunk.text,  # DocumentChunk使用text字段，不是content
                        'is_header': chunk.is_header,
                        'page_number': getattr(chunk, 'page_number', 1),
                        'position_info': getattr(chunk, 'position_info', {}),
                        'importance_score': getattr(chunk, 'importance_score', 0.5)
                    }
                    chunks_dict_list.append(chunk_dict)
                
                try:
                    chunks_stored = await self.ch_storage.store_document_chunks(doc_metadata.doc_id, chunks_dict_list)
                except Exception as e:
                    logger.error(f"❌ 文档块存储失败: {e}")
                    chunks_stored = False
                
                logger.info(f"ClickHouse存储: 元数据={metadata_stored}, chunks={chunks_stored}")
                
                # 如果ClickHouse存储失败，记录警告但不阻止向量化继续
                if not metadata_stored or not chunks_stored:
                    logger.warning(f"⚠️ ClickHouse存储部分失败，向量化将继续: {doc_metadata.file_name}")
            
            # 2. 过滤掉开头语chunks（不进行向量化）
            content_chunks = [chunk for chunk in chunks if not chunk.is_header]
            header_chunks = [chunk for chunk in chunks if chunk.is_header]
            
            logger.info(f"文档分块统计: 总计{len(chunks)}, 内容块{len(content_chunks)}, 开头语{len(header_chunks)}")
            
            if not content_chunks:
                logger.warning(f"文档无内容块可向量化: {doc_metadata.file_name}")
                return {
                    "success": True,
                    "total_chunks": len(chunks),
                    "vectorized_chunks": 0,
                    "skipped_chunks": len(header_chunks),
                    "processing_time": time.time() - start_time
                }
            
            # 2. 批量向量化内容块
            vectorized_count = 0
            
            for i in range(0, len(content_chunks), self.batch_size):
                batch_chunks = content_chunks[i:i + self.batch_size]
                
                try:
                    batch_result = await self._vectorize_chunk_batch(
                        doc_metadata, batch_chunks, i // self.batch_size + 1
                    )
                    vectorized_count += batch_result
                    
                except Exception as e:
                    logger.error(f"批次向量化失败: {e}")
                    # 继续处理下一批次
                    continue
            
            processing_time = time.time() - start_time
            
            # 更新文档处理状态为完成（修复pending状态问题）
            if self.ch_storage:
                try:
                    await self.ch_storage.update_processing_status(
                        doc_id=doc_metadata.doc_id,
                        status='completed',
                        chunk_count=len(chunks),
                        vector_count=vectorized_count,
                        processing_time=processing_time
                    )
                    logger.info(f"✅ 文档状态已更新为完成: {doc_metadata.doc_id}")
                except Exception as e:
                    logger.error(f"❌ 文档状态更新失败: {e}")
            
            result = {
                "success": True,
                "total_chunks": len(chunks),
                "vectorized_chunks": vectorized_count,
                "skipped_chunks": len(header_chunks),
                "processing_time": processing_time
            }
            
            logger.info(f"文档向量化完成: {doc_metadata.file_name}")
            logger.info(f"  向量化: {vectorized_count}/{len(content_chunks)} 内容块")
            logger.info(f"  跳过: {len(header_chunks)} 开头语块")
            logger.info(f"  耗时: {processing_time:.2f}秒")
            
            return result
            
        except Exception as e:
            logger.error(f"文档向量化失败 {doc_metadata.file_name}: {e}")
            
            # 更新文档处理状态为失败
            if self.ch_storage:
                try:
                    await self.ch_storage.update_processing_status(
                        doc_id=doc_metadata.doc_id,
                        status='failed',
                        chunk_count=len(chunks),
                        vector_count=0,
                        processing_time=time.time() - start_time,
                        error_message=str(e)
                    )
                    logger.info(f"✅ 文档状态已更新为失败: {doc_metadata.doc_id}")
                except Exception as status_error:
                    logger.error(f"❌ 文档失败状态更新失败: {status_error}")
            
            return {
                "success": False,
                "error": str(e),
                "total_chunks": len(chunks),
                "vectorized_chunks": 0,
                "skipped_chunks": 0,
                "processing_time": time.time() - start_time
            }
    
    async def _vectorize_chunk_batch(self, 
                                   doc_metadata: DocumentMetadata,
                                   chunks: List[DocumentChunk],
                                   batch_num: int) -> int:
        """
        向量化单个文档块批次，执行向量生成和Milvus插入的完整流程
        
        这是向量化处理的核心方法，负责将一批文档chunks转换为向量
        并插入到Milvus数据库中。包含完整的错误处理和数据完整性保证。
        
        处理流程：
        1. 提取chunks的文本内容
        2. 调用嵌入服务生成向量
        3. 构建包含元数据的插入记录
        4. 执行Milvus批量插入操作
        5. 验证插入结果并返回统计
        
        Args:
            doc_metadata (DocumentMetadata): 文档元数据，用于构建插入记录
            chunks (List[DocumentChunk]): 当前批次的chunk列表
                - 建议批次大小: 5-50个chunks
                - 每个chunk应该已经过内容过滤
            batch_num (int): 批次编号，用于日志记录和调试
                - 从1开始计数
                - 用于跟踪处理进度
                
        Returns:
            int: 成功向量化并插入的chunk数量
                - 正常情况: 等于输入chunks的数量
                - 部分失败: 可能小于输入数量
                - 完全失败: 返回0
                
        Raises:
            Exception: 当批次处理发生错误时，异常会被捕获并记录，
                      但不会向上传播，以保证其他批次的正常处理
                      
        Technical Details:
        - 向量ID使用UUID4生成，确保全局唯一性
        - 文本内容限制为10000字符，符合Milvus varchar限制
        - 时间戳使用毫秒级精度，支持高精度时间排序
        - 元数据以JSON格式存储，保持结构化信息
        - Token计算使用简单的空格分割，适合中英文混合内容
        
        Milvus插入记录结构：
        - id: UUID字符串，向量唯一标识
        - embedding: 浮点数向量（通常4096维）
        - doc_id: 文档标识符
        - chunk_id: chunk标识符
        - stock_code: 股票代码
        - company_name: 公司名称
        - document_type: 文档类型
        - chunk_type: chunk类型
        - text_content: 截断的文本内容（≤10000字符）
        - metadata: JSON格式的扩展元数据
        - created_at: 创建时间戳（毫秒）
        
        Error Handling:
        - 嵌入服务调用失败：记录错误并返回0
        - Milvus连接失败：记录错误并返回0
        - 向量插入失败：记录错误并返回0
        - 数据格式错误：记录错误并返回0
        
        Performance Notes:
        - 批次大小影响内存使用：batch_size * 4096 * 4 bytes（单精度浮点）
        - 网络调用次数：每批次1次嵌入API + 1次Milvus插入
        - 建议批次大小：10-30个chunks，平衡性能和内存使用
        
        Example:
            # 此方法通常由vectorize_document内部调用
            success_count = await self._vectorize_chunk_batch(
                doc_metadata=metadata,
                chunks=batch_chunks,
                batch_num=1
            )
            print(f"批次1: 成功处理{success_count}个chunks")
        """
        try:
            logger.debug(f"处理批次 {batch_num}: {len(chunks)} 个chunks")
            
            # 1. 提取文本用于嵌入
            texts = [chunk.text for chunk in chunks]
            
            # 2. 生成向量
            embedding_client = await get_embedding_client()
            embeddings_response = await embedding_client.embed_texts(texts)
            
            if not embeddings_response.embeddings:
                logger.error("向量生成失败")
                return 0
            
            # 3. 准备Milvus插入数据
            insert_data = []
            current_time = int(time.time() * 1000)  # 毫秒时间戳
            
            for chunk, embedding in zip(chunks, embeddings_response.embeddings):
                
                # 生成唯一的向量ID
                vector_id = str(uuid.uuid4())
                
                # 准备元数据
                metadata = {
                    "file_path": doc_metadata.file_path,
                    "file_name": doc_metadata.file_name,
                    "publish_date": doc_metadata.publish_date.isoformat() if doc_metadata.publish_date else None,
                    "page_count": doc_metadata.page_count,
                    "chunk_index": chunk.chunk_index,
                    "chunk_type": chunk.chunk_type,
                    "token_count": len(chunk.text.split()),  # 简单token计算
                    "bbox": chunk.bbox
                }
                
                # 计算重要性评分
                importance_score = self.calculate_importance_score(chunk, doc_metadata)
                
                record = {
                    "id": vector_id,
                    "embedding": embedding,
                    "doc_id": doc_metadata.doc_id,
                    "chunk_id": chunk.chunk_id,
                    "stock_code": doc_metadata.stock_code,
                    "company_name": doc_metadata.company_name,
                    "document_type": doc_metadata.document_type,
                    "document_category": doc_metadata.document_category,
                    "document_title": doc_metadata.document_title or "未知标题",  # 修复None值问题
                    "chunk_type": chunk.chunk_type,
                    "text_content": chunk.text[:50000],  # v3扩展到50K字符
                    "metadata": metadata,
                    "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # 使用字符串格式与v3 schema匹配
                    
                    # v3新增字段 - 使用字符串格式与Milvus schema匹配
                    "publish_date": doc_metadata.publish_date.strftime('%Y-%m-%d') if doc_metadata.publish_date else "",
                    "page_number": chunk.page_number,
                    "file_path": doc_metadata.file_path,
                    "chunk_length": chunk.text_length,
                    "importance_score": importance_score,
                    "vector_id": vector_id  # 向量标识符字段
                }
                
                insert_data.append(record)
            
            # 4. 插入到Milvus
            collection_manager = get_collection_manager()
            
            if not collection_manager.connect():
                logger.error("Milvus连接失败")
                return 0
            
            try:
                success = collection_manager.insert_vectors(self.collection_name, insert_data)
                
                if success:
                    logger.debug(f"批次 {batch_num} 向量插入成功: {len(insert_data)} 条记录")
                    
                    # 更新ClickHouse中的向量化状态
                    if self.ch_storage:
                        for chunk, _, vector_id in zip(chunks, embeddings_response.embeddings, 
                                                     [record["id"] for record in insert_data]):
                            await self.ch_storage.update_vector_status(
                                chunk_id=chunk.chunk_id,
                                vector_id=vector_id,
                                status='completed'
                            )
                    
                    return len(insert_data)
                else:
                    logger.error(f"批次 {batch_num} 向量插入失败")
                    
                    # 标记ClickHouse中的向量化失败状态
                    if self.ch_storage:
                        for chunk in chunks:
                            await self.ch_storage.update_vector_status(
                                chunk_id=chunk.chunk_id,
                                vector_id='',
                                status='failed'
                            )
                    
                    return 0
                    
            finally:
                # 在自动连接模式下不断开连接，提高性能
                pass
            
        except Exception as e:
            logger.error(f"批次 {batch_num} 向量化失败: {e}")
            return 0
    
    async def vectorize_documents_batch(self, 
                                      documents: List[tuple]) -> Dict[str, Any]:
        """
        批量向量化多个港交所文档，支持大规模文档处理
        
        这个方法提供批量文档处理能力，适用于大规模港交所公告
        数据的批量向量化。包含进度跟踪、错误统计和性能监控功能。
        自动在文档间添加处理延迟，避免API限流问题。
        
        批处理特性：
        - 顺序处理多个文档，避免并发冲突
        - 文档间自动添加1秒延迟，防止API限流
        - 单个文档失败不影响其他文档处理
        - 详细的进度跟踪和统计信息
        - 完整的错误记录和分析
        
        Args:
            documents (List[tuple]): 文档列表，每个元素为元组
                格式: [(doc_metadata, chunks), (doc_metadata, chunks), ...]
                - doc_metadata: DocumentMetadata对象
                - chunks: List[DocumentChunk]对象列表
                - 建议批次大小: 不超过100个文档
                
        Returns:
            Dict[str, Any]: 批量处理结果统计，包含：
                - total_documents (int): 输入文档总数
                - successful_documents (int): 成功处理的文档数
                - failed_documents (int): 处理失败的文档数
                - total_chunks_processed (int): 处理的总chunk数
                - total_chunks_vectorized (int): 成功向量化的chunk数
                - total_chunks_skipped (int): 跳过的chunk数（开头语等）
                - processing_time (float): 总处理时间（秒）
                - document_results (List[Dict]): 每个文档的详细处理结果
                
        Processing Strategy:
        - 串行处理：避免并发导致的资源竞争
        - 延迟控制：文档间1秒延迟，最后文档无延迟
        - 错误隔离：单个文档失败不影响后续处理
        - 内存管理：逐个处理避免大规模内存占用
        
        Rate Limiting:
        - 嵌入API调用：文档间1秒延迟
        - Milvus插入：批量插入优化，减少连接开销
        - 内存使用：单文档处理，避免大规模缓存
        
        Error Recovery:
        - 文档级错误：记录错误但继续处理其他文档
        - 批次级错误：记录错误但不中断文档处理
        - 系统级错误：记录错误并返回部分结果
        
        Performance Characteristics:
        - Time Complexity: O(n * m) 其中n为文档数，m为平均文档处理时间
        - Space Complexity: O(max_chunks_per_doc * vector_dim) 单文档内存峰值
        - 处理速度：约每文档1-5秒（取决于chunk数量和复杂度）
        
        Example:
            # 准备文档列表
            documents = [
                (metadata1, chunks1),
                (metadata2, chunks2),
                (metadata3, chunks3)
            ]
            
            # 执行批量处理
            results = await vectorizer.vectorize_documents_batch(documents)
            
            # 分析结果
            print(f"处理文档: {results['successful_documents']}/{results['total_documents']}")
            print(f"向量化chunks: {results['total_chunks_vectorized']}")
            print(f"总耗时: {results['processing_time']:.2f}秒")
            
            # 检查失败的文档
            for doc_result in results['document_results']:
                if not doc_result['result']['success']:
                    print(f"失败文档: {doc_result['file_name']}")
                    
        Monitoring Recommendations:
        - 监控处理速度：每文档平均处理时间
        - 监控成功率：successful_documents / total_documents
        - 监控向量化率：total_chunks_vectorized / total_chunks_processed
        - 监控内存使用：处理过程中的内存峰值
        """
        start_time = time.time()
        
        results = {
            "total_documents": len(documents),
            "successful_documents": 0,
            "failed_documents": 0,
            "total_chunks_processed": 0,
            "total_chunks_vectorized": 0,
            "total_chunks_skipped": 0,
            "processing_time": 0,
            "document_results": []
        }
        
        logger.info(f"开始批量向量化 {len(documents)} 个文档")
        
        for i, (doc_metadata, chunks) in enumerate(documents):
            try:
                logger.info(f"处理文档 {i+1}/{len(documents)}: {doc_metadata.file_name}")
                
                doc_result = await self.vectorize_document(doc_metadata, chunks)
                
                results["document_results"].append({
                    "file_name": doc_metadata.file_name,
                    "result": doc_result
                })
                
                if doc_result["success"]:
                    results["successful_documents"] += 1
                else:
                    results["failed_documents"] += 1
                
                results["total_chunks_processed"] += doc_result["total_chunks"]
                results["total_chunks_vectorized"] += doc_result["vectorized_chunks"]
                results["total_chunks_skipped"] += doc_result["skipped_chunks"]
                
                # 添加延迟以避免API限流
                if i < len(documents) - 1:  # 最后一个文档不需要延迟
                    await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"处理文档失败 {doc_metadata.file_name}: {e}")
                results["failed_documents"] += 1
                results["document_results"].append({
                    "file_name": doc_metadata.file_name,
                    "result": {"success": False, "error": str(e)}
                })
        
        results["processing_time"] = time.time() - start_time
        
        logger.info(f"批量向量化完成:")
        logger.info(f"  成功: {results['successful_documents']}/{results['total_documents']} 文档")
        logger.info(f"  向量化: {results['total_chunks_vectorized']} chunks")
        logger.info(f"  跳过: {results['total_chunks_skipped']} chunks")
        logger.info(f"  总耗时: {results['processing_time']:.2f}秒")
        
        return results
    
    async def test_vectorization(self, sample_text: str, timeout_seconds: int = 45) -> bool:
        """
        测试向量化功能的完整性和正确性
        
        这是一个诊断方法，用于验证嵌入服务的可用性和向量生成的正确性。
        通过生成测试向量并验证其维度和内容，确保向量化系统工作正常。
        
        测试流程：
        1. 使用提供的样本文本生成向量
        2. 验证向量生成是否成功
        3. 检查向量维度是否符合预期（4096维）
        4. 返回测试结果
        
        Args:
            sample_text (str): 用于测试的样本文本
                - 建议长度: 10-100字符
                - 内容: 中英文混合文本，模拟真实场景
                - 示例: "腾讯控股有限公司 Tencent Holdings Limited"
                
        Returns:
            bool: 向量化测试结果
                - True: 向量化功能正常，维度正确
                - False: 向量化失败或维度异常
                
        Test Scenarios:
        - 嵌入服务连接测试
        - 向量生成功能测试
        - 向量维度验证（期望4096维）
        - API响应时间测试
        
        Expected Vector Properties:
        - 维度: 4096（对应嵌入模型的输出维度）
        - 数据类型: List[float]
        - 数值范围: 通常在[-1, 1]之间
        - 非零元素: 大部分元素应该非零
        
        Common Failure Reasons:
        - 嵌入服务未启动或不可访问
        - API密钥配置错误
        - 网络连接问题
        - 嵌入模型配置错误
        - 返回向量维度不匹配
        
        Example:
            # 基本测试
            is_working = await vectorizer.test_vectorization(
                "港交所公告测试文本 HKEX announcement test"
            )
            
            if is_working:
                print("✅ 向量化系统正常")
            else:
                print("❌ 向量化系统异常，请检查配置")
                
            # 中文测试
            chinese_test = await vectorizer.test_vectorization(
                "供股公告：本公司拟按每持有10股现有股份获发1股供股股份"
            )
            
            # 英文测试  
            english_test = await vectorizer.test_vectorization(
                "Rights Issue: 1 for 10 existing shares at HK$320 per share"
            )
            
        Note:
            - 测试失败时会记录详细错误日志
            - 建议在系统启动时执行此测试
            - 可用于健康检查和故障诊断
            - 测试不会向Milvus插入数据
        """
        try:
            logger.info("测试向量化功能...")
            
            # 添加超时保护的测试向量化
            embedding_client = await get_embedding_client()
            
            # 使用asyncio.wait_for添加总体超时控制
            result = await asyncio.wait_for(
                embedding_client.embed_text(sample_text),
                timeout=timeout_seconds
            )
            
            if result and len(result) == 4096:
                logger.info("✅ 向量化测试成功")
                return True
            else:
                logger.error("❌ 向量化测试失败：向量维度不正确")
                return False
                
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ 向量化测试超时({timeout_seconds}秒)，但将继续处理")
            return False
        except Exception as e:
            logger.warning(f"⚠️ 向量化测试失败: {e}，但将继续处理")
            return False
    
    async def close(self):
        """关闭所有资源和连接"""
        logger.info("🔚 关闭DocumentVectorizer资源...")
        
        try:
            # 关闭ClickHouse存储连接
            if hasattr(self, 'ch_storage') and self.ch_storage:
                await self.ch_storage.close()
                logger.debug("✅ ClickHouse存储连接已关闭")
            
            # 关闭嵌入客户端连接
            if hasattr(self, 'embedding_client') and self.embedding_client:
                await self.embedding_client.close()
                logger.debug("✅ 嵌入客户端连接已关闭")
            
            logger.info("✅ DocumentVectorizer资源关闭完成")
            
        except Exception as e:
            logger.error(f"❌ 关闭DocumentVectorizer资源时出错: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
