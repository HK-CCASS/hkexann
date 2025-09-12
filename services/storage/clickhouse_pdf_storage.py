"""
ClickHouse PDF存储服务
用于存储PDF文档的元数据和处理状态到ClickHouse数据库
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
import aiohttp

logger = logging.getLogger(__name__)


class ClickHousePDFStorage:
    """
    ClickHouse PDF存储管理器
    
    负责将PDF文档的元数据、处理状态和向量化结果存储到ClickHouse数据库
    支持文档状态跟踪、处理历史记录和性能监控等功能
    """
    
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 8124,
                 database: str = "hkex_analysis",
                 username: str = "root",
                 password: str = "123456"):
        """
        初始化ClickHouse PDF存储器
        
        Args:
            host: ClickHouse主机地址
            port: ClickHouse HTTP端口
            database: 数据库名称
            username: 用户名
            password: 密码
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.base_url = f"http://{host}:{port}"
        self.session: Optional[aiohttp.ClientSession] = None
        
        logger.info(f"初始化ClickHouse PDF存储器 - {host}:{port}/{database}")
    
    async def initialize(self) -> bool:
        """初始化连接和会话"""
        try:
            # 检查是否已经初始化，避免重复创建连接
            if self.session and not self.session.closed:
                logger.debug("ClickHouse PDF存储器会话已存在，跳过初始化")
                return True
            
            auth = aiohttp.BasicAuth(self.username, self.password)
            timeout = aiohttp.ClientTimeout(total=30)
            
            self.session = aiohttp.ClientSession(
                auth=auth,
                timeout=timeout,
                connector=aiohttp.TCPConnector(limit=100)
            )
            
            # 测试连接
            await self._execute_query("SELECT 1")
            logger.info("✅ ClickHouse PDF存储器连接成功")
            
            # 确保表存在
            await self._ensure_tables_exist()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ ClickHouse PDF存储器初始化失败: {e}")
            if self.session:
                await self.session.close()
                self.session = None
            return False
    
    async def _execute_query(self, query: str, params: Optional[Dict] = None) -> List[List[str]]:
        """执行ClickHouse查询"""
        if not self.session:
            raise RuntimeError("ClickHouse会话未初始化")
        
        try:
            # ClickHouse HTTP接口：查询作为POST body，参数作为URL参数
            url_params = {
                'database': self.database,
                'default_format': 'JSONEachRow'
            }
            
            if params:
                url_params.update(params)
            
            # 查询作为POST body发送
            async with self.session.post(
                f"{self.base_url}/", 
                params=url_params,
                data=query,
                headers={'Content-Type': 'text/plain'}
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"ClickHouse查询失败 (HTTP {response.status}): {error_text}")
                
                result_text = await response.text()
                
                # 解析JSON响应
                results = []
                if result_text.strip():
                    for line in result_text.strip().split('\n'):
                        if line.strip():
                            row_data = json.loads(line)
                            results.append(list(row_data.values()))
                
                return results
                
        except Exception as e:
            logger.error(f"ClickHouse查询执行失败: {e}")
            raise
    
    async def _ensure_tables_exist(self):
        """确保PDF相关表存在"""
        try:
            # 创建PDF文档表
            create_pdf_docs_table = """
            CREATE TABLE IF NOT EXISTS pdf_documents (
                id String,
                file_path String,
                original_filename String,
                file_size UInt64,
                stock_code String,
                document_type String,
                published_date DateTime,
                processed_date DateTime DEFAULT now(),
                processing_status Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4),
                chunk_count UInt32 DEFAULT 0,
                vector_count UInt32 DEFAULT 0,
                processing_time_seconds Float32 DEFAULT 0,
                error_message String DEFAULT '',
                metadata String DEFAULT '{}',
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (stock_code, published_date, id)
            PARTITION BY toYYYYMM(published_date)
            """
            
            await self._execute_query(create_pdf_docs_table)
            
            # 创建PDF块表 - 使用与SQL文件一致的结构
            create_pdf_chunks_table = f"""
            CREATE TABLE IF NOT EXISTS {self.database}.pdf_chunks (
                -- 分块标识
                chunk_id String COMMENT '分块ID',
                doc_id String COMMENT '文档ID',
                
                -- 分块信息  
                chunk_index UInt32 COMMENT '分块索引',
                page_number UInt16 COMMENT '页码',
                
                -- 文本内容
                text String COMMENT '文本内容',
                text_length UInt32 COMMENT '文本长度', 
                text_hash String COMMENT '文本哈希值',
                
                -- 位置信息
                bbox Array(Float32) DEFAULT [] COMMENT '边界框坐标 [x1,y1,x2,y2]',
                
                -- 分块类型
                chunk_type Enum8('paragraph' = 1, 'table' = 2, 'title' = 3, 'list' = 4, 'other' = 5) COMMENT '分块类型',
                
                -- 表格信息（如果是表格）
                table_id Nullable(String) COMMENT '表格ID',
                table_data Nullable(String) COMMENT '表格数据（JSON格式）',
                
                -- 向量化状态
                vector_status Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4) DEFAULT 'pending' COMMENT '向量化状态',
                vector_id Nullable(String) COMMENT '向量ID',
                
                -- 系统字段
                created_at DateTime DEFAULT now() COMMENT '创建时间',
                updated_at DateTime DEFAULT now() COMMENT '更新时间'
                
            ) ENGINE = MergeTree()
            PARTITION BY substring(doc_id, 1, 6)
            ORDER BY (doc_id, chunk_id)
            PRIMARY KEY (doc_id, chunk_id)
            SETTINGS index_granularity = 8192
            """
            
            await self._execute_query(create_pdf_chunks_table)
            
            logger.info("✅ PDF存储表验证/创建成功")
            
        except Exception as e:
            logger.error(f"❌ PDF存储表创建失败: {e}")
            raise
    
    async def store_document_metadata(self, 
                                    doc_id: str,
                                    file_path: str,
                                    metadata: Dict[str, Any]) -> bool:
        """存储PDF文档元数据"""
        try:
            # 提取关键信息
            stock_code = metadata.get('stock_code', '')
            document_type = metadata.get('document_type', 'unknown')
            published_date = metadata.get('published_date', datetime.now())
            file_size = metadata.get('file_size', 0)
            original_filename = metadata.get('original_filename', file_path.split('/')[-1])
            
            # 格式化日期 - 确保序列化兼容性
            if isinstance(published_date, str):
                try:
                    published_date = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
                except:
                    published_date = datetime.now()
            elif not isinstance(published_date, datetime):
                published_date = datetime.now()
            
            # 转换为字符串避免序列化问题
            published_date_str = published_date.strftime('%Y-%m-%d %H:%M:%S')
            
            query = """
            INSERT INTO pdf_documents 
            (doc_id, file_path, file_name, file_size, stock_code, document_type, 
             publish_date, processing_status)
            VALUES
            """
            
            # 构建VALUES部分 - 使用修复后的日期字符串，匹配现有表结构
            values = f"('{doc_id}', '{file_path}', '{original_filename}', {file_size}, " \
                    f"'{stock_code}', '{document_type}', '{published_date_str}', " \
                    f"'pending')"
            
            await self._execute_query(query + values)
            
            logger.info(f"✅ 文档元数据已存储: {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 存储文档元数据失败: {e}")
            return False
    
    async def update_processing_status(self, 
                                     doc_id: str,
                                     status: str,
                                     chunk_count: int = 0,
                                     vector_count: int = 0,
                                     processing_time: float = 0.0,
                                     error_message: str = '') -> bool:
        """更新文档处理状态"""
        try:
            query = f"""
            ALTER TABLE pdf_documents UPDATE 
                processing_status = '{status}',
                chunk_count = {chunk_count},
                vector_count = {vector_count},
                processing_time_seconds = {processing_time},
                error_message = '{error_message}',
                updated_at = now()
            WHERE id = '{doc_id}'
            """
            
            await self._execute_query(query)
            
            logger.info(f"✅ 文档状态已更新: {doc_id} -> {status}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 更新文档状态失败: {e}")
            return False
    
    async def store_document_chunks(self, 
                                  doc_id: str,
                                  chunks_data: List[Dict[str, Any]]) -> bool:
        """批量存储文档块数据"""
        try:
            if not chunks_data:
                return True
            
            # 构建批量插入查询 - 匹配现有表结构
            query = """
            INSERT INTO pdf_chunks 
            (chunk_id, doc_id, chunk_index, page_number, text, text_length, 
             text_hash, chunk_type)
            VALUES
            """
            
            values_list = []
            for chunk_data in chunks_data:
                chunk_id = chunk_data.get('id', str(uuid.uuid4()))
                chunk_index = chunk_data.get('index', 0)
                chunk_type = chunk_data.get('type', 'paragraph')
                content = chunk_data.get('content', '').replace("'", "\\'")
                page_number = chunk_data.get('page_number', 1)
                text_length = len(content)
                
                # 生成简单的文本哈希
                import hashlib
                text_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
                
                # 转换chunk_type为数值（匹配ENUM定义）
                type_mapping = {'paragraph': 1, 'table': 2, 'title': 3, 'list': 4, 'other': 5}
                chunk_type_num = type_mapping.get(chunk_type, 5)
                
                values = f"('{chunk_id}', '{doc_id}', {chunk_index}, {page_number}, " \
                        f"'{content[:1000]}', {text_length}, '{text_hash}', {chunk_type_num})"
                
                values_list.append(values)
            
            # 分批插入，避免查询过大
            batch_size = 100
            for i in range(0, len(values_list), batch_size):
                batch_values = values_list[i:i+batch_size]
                batch_query = query + ','.join(batch_values)
                await self._execute_query(batch_query)
            
            logger.info(f"✅ 文档块已存储: {doc_id} ({len(chunks_data)} 个块)")
            return True
            
        except Exception as e:
            logger.error(f"❌ 存储文档块失败: {e}")
            return False
    
    async def get_document_status(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """获取文档处理状态"""
        try:
            query = f"""
            SELECT processing_status, chunk_count, vector_count, 
                   processing_time_seconds, error_message, updated_at
            FROM pdf_documents 
            WHERE id = '{doc_id}'
            """
            
            results = await self._execute_query(query)
            
            if results:
                result = results[0]
                return {
                    'status': result[0],
                    'chunk_count': int(result[1]),
                    'vector_count': int(result[2]), 
                    'processing_time': float(result[3]),
                    'error_message': result[4],
                    'updated_at': result[5]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ 获取文档状态失败: {e}")
            return None
    
    async def get_processing_statistics(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        try:
            stats_query = """
            SELECT 
                processing_status,
                count() as count,
                sum(chunk_count) as total_chunks,
                sum(vector_count) as total_vectors,
                avg(processing_time_seconds) as avg_processing_time
            FROM pdf_documents 
            GROUP BY processing_status
            """
            
            results = await self._execute_query(stats_query)
            
            stats = {
                'by_status': {},
                'total_documents': 0,
                'total_chunks': 0, 
                'total_vectors': 0
            }
            
            for result in results:
                status = result[0]
                count = int(result[1])
                chunks = int(result[2]) if result[2] else 0
                vectors = int(result[3]) if result[3] else 0
                avg_time = float(result[4]) if result[4] else 0.0
                
                stats['by_status'][status] = {
                    'count': count,
                    'chunks': chunks,
                    'vectors': vectors,
                    'avg_processing_time': avg_time
                }
                
                stats['total_documents'] += count
                stats['total_chunks'] += chunks
                stats['total_vectors'] += vectors
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ 获取处理统计失败: {e}")
            return {}
    
    async def update_vector_status(self, 
                                  chunk_id: str, 
                                  vector_id: str, 
                                  status: str) -> bool:
        """
        更新文档块的向量化状态
        
        Args:
            chunk_id: 文档块ID
            vector_id: 向量ID
            status: 状态 ('completed', 'failed', 'processing')
            
        Returns:
            是否更新成功
        """
        try:
            if not await self.initialize():
                logger.error("ClickHouse连接初始化失败，无法更新向量状态")
                return False
                
            # 首先尝试标准UPDATE语法
            vector_value = f"{vector_id}:{status}".replace("'", "\\'")  # 转义单引号
            chunk_id_escaped = chunk_id.replace("'", "\\'")  # 转义单引号
            
            # 方法1: 尝试ALTER TABLE UPDATE语法
            query = f"ALTER TABLE {self.database}.pdf_chunks UPDATE vector_id = '{vector_value}' WHERE chunk_id = '{chunk_id_escaped}'"
            
            try:
                logger.debug(f"尝试ALTER UPDATE: {query}")
                await self._execute_query(query)
                logger.debug(f"✅ 更新向量状态成功: {chunk_id} -> {status}")
                return True
                
            except Exception as alter_error:
                logger.warning(f"ALTER UPDATE失败，尝试替代方案: {alter_error}")
                
                # 方法2: 使用简化的字段更新
                # 只更新 vector_id 字段，不依赖复杂的INSERT操作
                try:
                    # 检查记录是否存在
                    check_query = f"SELECT chunk_id FROM {self.database}.pdf_chunks WHERE chunk_id = '{chunk_id_escaped}' LIMIT 1"
                    rows = await self._execute_query(check_query)
                    
                    if not rows:
                        logger.error(f"未找到要更新的记录: {chunk_id}")
                        return False
                    
                    # 尝试使用 OPTIMIZE TABLE 触发合并后再次UPDATE
                    logger.debug("尝试使用OPTIMIZE TABLE后重新UPDATE")
                    optimize_query = f"OPTIMIZE TABLE {self.database}.pdf_chunks FINAL"
                    await self._execute_query(optimize_query)
                    
                    # 再次尝试标准UPDATE
                    retry_query = f"ALTER TABLE {self.database}.pdf_chunks UPDATE vector_id = '{vector_value}' WHERE chunk_id = '{chunk_id_escaped}'"
                    await self._execute_query(retry_query)
                    
                    logger.debug(f"✅ 使用OPTIMIZE+UPDATE更新向量状态成功: {chunk_id} -> {status}")
                    return True
                        
                except Exception as optimize_error:
                    logger.error(f"OPTIMIZE+UPDATE方案也失败: {optimize_error}")
                    
                # 方法3: 简单的记录状态（如果其他方法都失败）
                logger.warning(f"所有UPDATE方法都失败，记录更新请求: {chunk_id} -> {vector_id}:{status}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 更新向量状态失败 {chunk_id}: {e}")
            return False
    
    async def close(self):
        """关闭连接"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("ClickHouse PDF存储器连接已关闭")


# 用于测试的异步函数
async def test_clickhouse_pdf_storage():
    """测试ClickHouse PDF存储功能"""
    storage = ClickHousePDFStorage()
    
    try:
        # 初始化
        success = await storage.initialize()
        if not success:
            print("❌ 初始化失败")
            return
        
        # 测试存储文档元数据
        test_doc_id = f"test_doc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        metadata = {
            'stock_code': '00700',
            'document_type': 'announcement',
            'published_date': datetime.now(),
            'file_size': 1024000,
            'original_filename': 'test_document.pdf'
        }
        
        success = await storage.store_document_metadata(test_doc_id, "/test/path.pdf", metadata)
        print(f"文档元数据存储: {'✅' if success else '❌'}")
        
        # 测试更新状态
        success = await storage.update_processing_status(
            test_doc_id, 'completed', chunk_count=10, vector_count=8, processing_time=1.5
        )
        print(f"状态更新: {'✅' if success else '❌'}")
        
        # 测试获取状态
        status = await storage.get_document_status(test_doc_id)
        print(f"状态查询: {'✅' if status else '❌'}")
        if status:
            print(f"状态详情: {status}")
        
        # 测试统计信息
        stats = await storage.get_processing_statistics()
        print(f"统计信息: {'✅' if stats else '❌'}")
        if stats:
            print(f"统计详情: {stats}")
            
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await storage.close()

if __name__ == "__main__":
    import uuid
    asyncio.run(test_clickhouse_pdf_storage())
