"""
实时向量化处理器
包装现有的DocumentProcessingPipeline，用于实时公告处理
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional

try:
    from services.document_processor.pipeline import DocumentProcessingPipeline
    PIPELINE_AVAILABLE = True
    
    # 尝试导入settings，如果不存在就使用默认值
    try:
        from config.settings import settings
        SETTINGS_AVAILABLE = True
    except ImportError:
        settings = None
        SETTINGS_AVAILABLE = False
        
except ImportError as e:
    PIPELINE_AVAILABLE = False
    SETTINGS_AVAILABLE = False
    DocumentProcessingPipeline = None
    settings = None
    IMPORT_ERROR = str(e)

logger = logging.getLogger(__name__)


class RealtimeVectorProcessor:
    """
    实时公告向量化处理器
    
    完全复用现有的DocumentProcessingPipeline，为实时监听系统提供
    向量化处理能力。支持单个PDF文件的处理，集成现有的：
    - HKEXPDFParser (PDF解析和分块)
    - DocumentVectorizer (文档向量化)
    - SiliconFlowEmbeddingClient (文本嵌入)
    - MilvusCollectionManager (向量存储)
    """
    
    def __init__(self, config: dict):
        """
        初始化实时向量化处理器
        
        Args:
            config: 配置字典
        """
        if not PIPELINE_AVAILABLE:
            raise ImportError(f"无法导入DocumentProcessingPipeline: {IMPORT_ERROR}")
        
        self.config = config
        
        # 从配置获取参数
        vector_config = config.get('vectorization_integration', {})
        self.pdf_directory = vector_config.get('pdf_directory', 'hkexann')
        self.collection_name = vector_config.get('collection_name', 'hkex_pdf_embeddings')
        self.batch_size = vector_config.get('batch_size', 15)
        self.max_concurrent = vector_config.get('max_concurrent', 5)
        self.use_existing_pipeline = vector_config.get('use_existing_pipeline', True)
        
        # 初始化现有的文档处理管道
        if self.use_existing_pipeline:
            try:
                # 确定collection名称
                if self.collection_name:
                    collection_name = self.collection_name
                elif SETTINGS_AVAILABLE and settings and hasattr(settings, 'pdf_embeddings_collection'):
                    collection_name = settings.pdf_embeddings_collection
                else:
                    collection_name = 'hkex_pdf_embeddings'
                
                self.pipeline = DocumentProcessingPipeline(
                    pdf_directory=self.pdf_directory,
                    collection_name=collection_name,
                    batch_size=self.batch_size,
                    max_concurrent=self.max_concurrent
                )
                logger.info(f"实时向量化处理器初始化完成")
                logger.info(f"  PDF目录: {self.pdf_directory}")
                logger.info(f"  集合名称: {self.collection_name}")
                logger.info(f"  批次大小: {self.batch_size}")
                logger.info(f"  最大并发: {self.max_concurrent}")
                logger.info(f"  使用现有pipeline: {self.use_existing_pipeline}")
            except Exception as e:
                logger.error(f"初始化DocumentProcessingPipeline失败: {e}")
                import traceback
                logger.error(f"详细错误堆栈: {traceback.format_exc()}")
                raise
        else:
            logger.warning("已禁用现有pipeline集成，向量化功能将不可用")
            self.pipeline = None
    
    async def process_announcement_pdf(self, pdf_path: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        使用现有pipeline处理单个公告PDF
        
        Args:
            pdf_path: PDF文件路径
            metadata: 可选的元数据信息
            
        Returns:
            处理结果字典
        """
        if not self.pipeline:
            return {
                "success": False,
                "error": "DocumentProcessingPipeline未初始化",
                "file_path": pdf_path
            }
        
        try:
            # 确保路径是Path对象
            pdf_path_obj = Path(pdf_path)
            
            if not pdf_path_obj.exists():
                return {
                    "success": False,
                    "error": f"PDF文件不存在: {pdf_path}",
                    "file_path": pdf_path
                }
            
            logger.info(f"开始处理PDF: {pdf_path_obj.name}")
            
            # 手动构建文档元数据，避免从路径解析
            from services.document_processor.pdf_parser import DocumentMetadata
            
            # 从传入的metadata中获取股票代码
            stock_code = metadata.get('stock_code', '') if metadata else ''
            announcement_id = metadata.get('announcement_id', '') if metadata else ''
            source = metadata.get('source', 'realtime') if metadata else 'realtime'
            
            # 构建文档元数据（使用DocumentMetadata的正确字段）
            doc_metadata = DocumentMetadata(
                doc_id=f"{stock_code}_{announcement_id}_{int(time.time())}",
                file_path=str(pdf_path_obj),
                file_name=pdf_path_obj.name,
                file_size=pdf_path_obj.stat().st_size,
                file_hash="",  # 将在解析时计算
                stock_code=stock_code,
                company_name="",  # 将在解析时提取
                document_type="公告",  # 默认类型
                document_category="",  # 将在解析时提取
                document_title="",  # 将在解析时提取
                publish_date=None,  # 将在解析时提取
                page_count=0  # 将在解析时计算
            )
            
            # 添加自定义属性
            doc_metadata.announcement_id = announcement_id
            doc_metadata.source = source
            
            # 使用pipeline的组件进行处理
            logger.info(f"解析PDF: {pdf_path_obj.name} (股票: {stock_code})")
            
            # parse_pdf返回(metadata, chunks)元组
            parsed_metadata, chunks = self.pipeline.pdf_parser.parse_pdf(pdf_path_obj)
            
            # 更新我们的文档元数据
            doc_metadata.file_hash = parsed_metadata.file_hash
            doc_metadata.company_name = parsed_metadata.company_name
            doc_metadata.document_type = parsed_metadata.document_type
            doc_metadata.document_category = parsed_metadata.document_category
            doc_metadata.document_title = parsed_metadata.document_title
            doc_metadata.publish_date = parsed_metadata.publish_date
            doc_metadata.page_count = parsed_metadata.page_count
            
            # 如果PDF解析器没有正确识别股票代码，使用我们提供的
            if not doc_metadata.stock_code or doc_metadata.stock_code == 'hkexann':
                doc_metadata.stock_code = stock_code
            
            if not chunks:
                return {
                    "success": False,
                    "error": "PDF解析失败，未获得chunks",
                    "file_path": pdf_path
                }
            
            logger.info(f"向量化处理: {len(chunks)} chunks")
            result = await self.pipeline.vectorizer.vectorize_document(doc_metadata, chunks)
            
            # 记录处理结果
            if result.get('success', False):
                logger.info(f"✅ 文档向量化完成: {result.get('file_name', 'Unknown')}")
                logger.info(f"   股票代码: {result.get('stock_code', 'N/A')}")
                logger.info(f"   公司名称: {result.get('company_name', 'N/A')}")
                logger.info(f"   文档类型: {result.get('document_type', 'N/A')}")
                logger.info(f"   向量化chunks: {result.get('vectorized_chunks', 0)}")
                logger.info(f"   跳过chunks: {result.get('skipped_chunks', 0)}")
                logger.info(f"   处理耗时: {result.get('processing_time', 0):.2f}秒")
            else:
                logger.error(f"❌ 文档向量化失败: {result.get('error', 'Unknown error')}")
            
            # 添加额外的元数据（如果提供）
            if metadata:
                result.update({
                    'announcement_metadata': metadata,
                    'processing_timestamp': result.get('processing_time', 0)
                })
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 文档向量化异常: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            
            return {
                "success": False,
                "error": str(e),
                "file_path": pdf_path,
                "processing_time": 0
            }
    
    async def batch_process_announcements(self, pdf_paths: list, metadata_list: Optional[list] = None) -> list:
        """
        批量处理多个公告PDF
        
        Args:
            pdf_paths: PDF文件路径列表
            metadata_list: 可选的元数据列表
            
        Returns:
            处理结果列表
        """
        if not pdf_paths:
            logger.warning("PDF路径列表为空")
            return []
        
        if metadata_list and len(metadata_list) != len(pdf_paths):
            logger.warning(f"元数据列表长度({len(metadata_list)})与PDF路径列表长度({len(pdf_paths)})不匹配")
            metadata_list = None
        
        logger.info(f"开始批量处理 {len(pdf_paths)} 个PDF文件")
        
        # 创建处理任务
        tasks = []
        for i, pdf_path in enumerate(pdf_paths):
            metadata = metadata_list[i] if metadata_list else None
            task = self.process_announcement_pdf(pdf_path, metadata)
            tasks.append(task)
        
        # 并发执行，但限制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_with_semaphore(task):
            async with semaphore:
                return await task
        
        # 执行所有任务
        results = await asyncio.gather(
            *[process_with_semaphore(task) for task in tasks],
            return_exceptions=True
        )
        
        # 统计结果
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get('success', False))
        failure_count = len(results) - success_count
        
        logger.info(f"批量处理完成: 成功 {success_count}, 失败 {failure_count}")
        
        return results
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """获取pipeline状态"""
        if not self.pipeline:
            return {
                "pipeline_available": False,
                "error": "Pipeline未初始化"
            }
        
        return {
            "pipeline_available": True,
            "pdf_directory": self.pdf_directory,
            "collection_name": self.collection_name,
            "batch_size": self.batch_size,
            "max_concurrent": self.max_concurrent,
            "use_existing_pipeline": self.use_existing_pipeline
        }
    
    async def test_processing(self, test_pdf_path: Optional[str] = None) -> Dict[str, Any]:
        """
        测试处理功能
        
        Args:
            test_pdf_path: 测试PDF路径，如果不提供则查找示例文件
            
        Returns:
            测试结果
        """
        if not self.pipeline:
            return {
                "success": False,
                "error": "Pipeline未初始化"
            }
        
        # 如果没有提供测试文件，尝试查找一个示例文件
        if not test_pdf_path:
            pdf_dir = Path(self.pdf_directory)
            if pdf_dir.exists():
                pdf_files = list(pdf_dir.glob("**/*.pdf"))
                if pdf_files:
                    test_pdf_path = str(pdf_files[0])
                    logger.info(f"使用示例PDF文件进行测试: {test_pdf_path}")
                else:
                    return {
                        "success": False,
                        "error": f"在目录 {pdf_dir} 中未找到PDF文件"
                    }
            else:
                return {
                    "success": False,
                    "error": f"PDF目录不存在: {pdf_dir}"
                }
        
        # 执行测试处理
        test_metadata = {
            "test_mode": True,
            "source": "realtime_processor_test"
        }
        
        result = await self.process_announcement_pdf(test_pdf_path, test_metadata)
        
        return {
            "success": result.get('success', False),
            "test_file": test_pdf_path,
            "result": result
        }
    
    async def close(self):
        """关闭所有资源和连接"""
        logger.info("🔚 关闭RealtimeVectorProcessor资源...")
        
        try:
            # 关闭DocumentProcessingPipeline
            if hasattr(self, 'pipeline') and self.pipeline:
                if hasattr(self.pipeline, 'close'):
                    await self.pipeline.close()
                    logger.debug("✅ DocumentProcessingPipeline已关闭")
                else:
                    logger.debug("⚠️ DocumentProcessingPipeline没有close方法")
            
            logger.info("✅ RealtimeVectorProcessor资源关闭完成")
            
        except Exception as e:
            logger.error(f"❌ 关闭RealtimeVectorProcessor资源时出错: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")


# 用于测试的简单函数
async def test_realtime_vector_processor():
    """测试实时向量化处理器"""
    config = {
        'vectorization_integration': {
            'use_existing_pipeline': True,
            'pdf_directory': 'hkexann',
            'collection_name': 'hkex_pdf_embeddings',
            'batch_size': 15,
            'max_concurrent': 5
        }
    }
    
    processor = None
    try:
        processor = RealtimeVectorProcessor(config)
        
        # 获取状态
        status = processor.get_pipeline_status()
        print("📊 Pipeline状态:")
        for key, value in status.items():
            print(f"  {key}: {value}")
        
        # 测试处理（如果有PDF文件）
        test_result = await processor.test_processing()
        print("\n🧪 测试结果:")
        for key, value in test_result.items():
            if key == 'result' and isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    print(f"    {k}: {v}")
            else:
                print(f"  {key}: {value}")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保关闭所有连接
        if processor:
            await processor.close()
            print("✅ 测试完成，所有连接已关闭")


if __name__ == "__main__":
    asyncio.run(test_realtime_vector_processor())
