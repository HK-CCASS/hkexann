"""
文档处理管道
协调PDF解析和向量化的完整流程
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

from config.settings import settings
from .pdf_parser import HKEXPDFParser
from .vectorizer import DocumentVectorizer

logger = logging.getLogger(__name__)


class DocumentProcessingPipeline:
    """文档处理管道"""
    
    def __init__(self, 
                 pdf_directory: str = None,
                 collection_name: str = None,
                 batch_size: int = 15,
                 max_concurrent: int = 5):
        """
        初始化文档处理管道
        
        Args:
            pdf_directory: PDF文档目录路径
            collection_name: Milvus集合名称
            batch_size: 向量化批次大小
            max_concurrent: 最大并发处理数
        """
        # 使用标准HKEX目录结构
        from config.settings import settings
        self.pdf_directory = Path(pdf_directory or str(settings.pdf_data_full_path))
        self.collection_name = collection_name or settings.pdf_embeddings_collection
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        
        # 初始化组件
        self.pdf_parser = HKEXPDFParser()
        
        # 确保Milvus集合存在
        self._ensure_milvus_collection()
        
        self.vectorizer = DocumentVectorizer(
            collection_name=collection_name,
            batch_size=batch_size,
            enable_clickhouse=False  # ClickHouse 未部署，禁用
        )
        
        logger.info(f"文档处理管道初始化完成:")
        logger.info(f"  PDF目录: {self.pdf_directory}")
        logger.info(f"  集合名称: {self.collection_name}")
        logger.info(f"  批次大小: {self.batch_size}")
        logger.info(f"  最大并发: {self.max_concurrent}")
    
    def _ensure_milvus_collection(self):
        """确保Milvus集合存在"""
        try:
            from services.milvus import get_collection_manager
            manager = get_collection_manager()
            connected = manager.connect()
            if not connected:
                logger.error("无法连接到Milvus数据库")
                return
            
            # 根据集合名称选择创建方法
            if self.collection_name == 'pdf_embeddings_v3' or self.collection_name == settings.pdf_embeddings_collection:
                # 使用标准v3架构
                success = manager.create_pdf_embeddings_collection()
            else:
                # 使用旧的create_collection_by_name方法（保持向后兼容）
                success = manager.create_collection_by_name(self.collection_name)
            if success:
                logger.info(f"✅ 已确保Milvus集合存在: {self.collection_name}")
            else:
                logger.error(f"❌ 创建Milvus集合失败: {self.collection_name}")
                
        except Exception as e:
            logger.error(f"❌ 确保Milvus集合时发生错误: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
    
    def discover_pdf_files(self,
                          stock_codes: Optional[List[str]] = None,
                          max_files: Optional[int] = None,
                          min_files: Optional[int] = None,
                          batch_files: Optional[int] = None,
                          random_sample: Optional[int] = None,
                          path_filter: Optional[str] = None) -> List[Path]:
        """
        发现PDF文件

        Args:
            stock_codes: 指定股票代码列表，None表示处理所有
            max_files: 最大文件数量限制
            min_files: 最小文件数量（确保至少处理指定数量）
            batch_files: 分批处理数量
            random_sample: 随机抽样数量
            path_filter: 路径筛选关键词，只保留路径中包含该关键词的文件

        Returns:
            PDF文件路径列表
        """
        logger.info(f"扫描PDF文件: {self.pdf_directory}")
        
        if not self.pdf_directory.exists():
            logger.error(f"PDF目录不存在: {self.pdf_directory}")
            return []
        
        # 查找PDF文件
        pdf_files = []
        
        if stock_codes:
            # 指定股票代码
            for stock_code in stock_codes:
                stock_dir = self.pdf_directory / stock_code
                if stock_dir.exists():
                    pdf_files.extend(stock_dir.rglob("*.pdf"))
        else:
            # 所有PDF文件
            pdf_files = list(self.pdf_directory.rglob("*.pdf"))
        
        # 按修改时间排序（新文件优先）
        pdf_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # 路径筛选
        if path_filter:
            original_count = len(pdf_files)

            # 支持多个关键词筛选（配股相关文档）
            if path_filter == "配股":
                # 配股相关的关键词：配股、配售
                filter_keywords = ["配股", "配售"]
                pdf_files = [f for f in pdf_files if any(keyword in f.as_posix() for keyword in filter_keywords)]
                logger.info(f"路径筛选 '{path_filter}' (包含: {', '.join(filter_keywords)}): {original_count} -> {len(pdf_files)} 个文件")
            else:
                # 其他关键词的筛选保持原有逻辑
                pdf_files = [f for f in pdf_files if path_filter in f.as_posix()]
                logger.info(f"路径筛选 '{path_filter}': {original_count} -> {len(pdf_files)} 个文件")
        
        # 限制数量
        if max_files and len(pdf_files) > max_files:
            pdf_files = pdf_files[:max_files]
            logger.info(f"限制处理文件数量为: {max_files}")
        
        logger.info(f"最终发现 {len(pdf_files)} 个PDF文件")
        
        # 显示文件分布
        if pdf_files:
            stock_counts = {}
            for pdf_file in pdf_files:
                # 假设路径格式为: .../stock_code/.../*.pdf
                parts = pdf_file.parts
                if len(parts) >= 2:
                    stock_code = parts[-3] if len(parts) >= 3 else "UNKNOWN"
                    stock_counts[stock_code] = stock_counts.get(stock_code, 0) + 1
            
            logger.info(f"文件分布: {dict(list(stock_counts.items())[:10])}{'...' if len(stock_counts) > 10 else ''}")
        
        return pdf_files
    
    async def process_single_document(self, pdf_path: Path, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        处理单个PDF文档

        Args:
            pdf_path: PDF文件路径
            metadata: 可选的元数据信息（包含HKEX分类信息）

        Returns:
            处理结果
        """
        start_time = time.time()

        try:
            logger.debug(f"处理文档: {pdf_path.name}")

            # 1. 解析PDF
            doc_metadata, chunks = self.pdf_parser.parse_pdf(pdf_path, metadata)
            
            if not chunks:
                return {
                    "success": False,
                    "error": "无法提取文档内容",
                    "file_path": str(pdf_path),
                    "processing_time": time.time() - start_time
                }
            
            # 2. 向量化
            vectorization_result = await self.vectorizer.vectorize_document(doc_metadata, chunks)
            
            # 3. 合并结果
            result = {
                "success": vectorization_result["success"],
                "file_path": str(pdf_path),
                "file_name": pdf_path.name,
                "doc_id": doc_metadata.doc_id,
                "stock_code": doc_metadata.stock_code,
                "company_name": doc_metadata.company_name,
                "document_type": doc_metadata.document_type,
                "total_chunks": vectorization_result["total_chunks"],
                "vectorized_chunks": vectorization_result["vectorized_chunks"],
                "skipped_chunks": vectorization_result["skipped_chunks"],
                "processing_time": time.time() - start_time
            }
            
            if not vectorization_result["success"]:
                result["error"] = vectorization_result.get("error", "向量化失败")
            
            return result
            
        except Exception as e:
            logger.error(f"处理文档失败 {pdf_path}: {e}")
            return {
                "success": False,
                "error": str(e),
                "file_path": str(pdf_path),
                "processing_time": time.time() - start_time
            }
    
    async def process_documents_batch(self, 
                                    pdf_files: List[Path],
                                    progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        批量处理文档
        
        Args:
            pdf_files: PDF文件列表
            progress_callback: 进度回调函数 callback(current, total, current_result)
            
        Returns:
            整体处理结果
        """
        start_time = time.time()
        
        results = {
            "total_files": len(pdf_files),
            "successful_files": 0,
            "failed_files": 0,
            "total_chunks": 0,
            "vectorized_chunks": 0,
            "skipped_chunks": 0,
            "processing_time": 0,
            "file_results": []
        }
        
        logger.info(f"开始批量处理 {len(pdf_files)} 个PDF文档")
        
        # 使用信号量限制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_with_semaphore(pdf_path: Path, index: int):
            async with semaphore:
                result = await self.process_single_document(pdf_path)
                
                # 更新统计
                if result["success"]:
                    results["successful_files"] += 1
                else:
                    results["failed_files"] += 1
                
                results["total_chunks"] += result.get("total_chunks", 0)
                results["vectorized_chunks"] += result.get("vectorized_chunks", 0)
                results["skipped_chunks"] += result.get("skipped_chunks", 0)
                results["file_results"].append(result)
                
                # 进度回调
                if progress_callback:
                    try:
                        progress_callback(index + 1, len(pdf_files), result)
                    except Exception as e:
                        logger.warning(f"进度回调失败: {e}")
                
                # 记录进度
                if (index + 1) % 10 == 0 or index + 1 == len(pdf_files):
                    logger.info(f"进度: {index + 1}/{len(pdf_files)} ({(index + 1)/len(pdf_files)*100:.1f}%)")
                
                return result
        
        # 并发处理所有文件
        tasks = [
            process_with_semaphore(pdf_path, i) 
            for i, pdf_path in enumerate(pdf_files)
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        results["processing_time"] = time.time() - start_time
        
        # 输出最终统计
        logger.info(f"批量处理完成:")
        logger.info(f"  成功: {results['successful_files']}/{results['total_files']} 文件")
        logger.info(f"  失败: {results['failed_files']} 文件")
        logger.info(f"  向量化: {results['vectorized_chunks']} chunks")
        logger.info(f"  跳过: {results['skipped_chunks']} chunks")
        logger.info(f"  总耗时: {results['processing_time']:.2f}秒")
        logger.info(f"  平均速度: {results['total_files'] / results['processing_time']:.2f} 文件/秒")
        
        return results
    
    async def run_pipeline(self, 
                         stock_codes: Optional[List[str]] = None,
                         max_files: Optional[int] = None,
                         path_filter: Optional[str] = None,
                         progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        运行完整的文档处理管道
        
        Args:
            stock_codes: 指定处理的股票代码
            max_files: 最大文件数量
            path_filter: 路径筛选关键词
            progress_callback: 进度回调函数
            
        Returns:
            处理结果
        """
        pipeline_start_time = time.time()
        
        logger.info("🚀 启动文档处理管道")
        
        try:
            # 1. 测试向量化功能
            logger.info("📊 测试向量化功能...")
            test_result = await self.vectorizer.test_vectorization("测试文本")
            if not test_result:
                logger.warning("⚠️ 向量化功能测试失败，但继续处理流程...")
            else:
                logger.info("✅ 向量化功能测试成功")
            
            # 2. 发现PDF文件
            logger.info("📁 扫描PDF文件...")
            pdf_files = self.discover_pdf_files(stock_codes, max_files, path_filter)
            
            if not pdf_files:
                logger.warning("未发现可处理的PDF文件")
                return {
                    "success": True,
                    "message": "未发现可处理的PDF文件",
                    "total_files": 0,
                    "processing_time": time.time() - pipeline_start_time
                }
            
            # 3. 批量处理文档
            logger.info("⚡ 开始文档处理...")
            results = await self.process_documents_batch(pdf_files, progress_callback)
            
            # 4. 整合最终结果
            final_result = {
                "success": True,
                "pipeline_time": time.time() - pipeline_start_time,
                **results
            }
            
            logger.info(f"🎉 文档处理管道完成! 总耗时: {final_result['pipeline_time']:.2f}秒")
            
            return final_result
            
        except Exception as e:
            logger.error(f"❌ 文档处理管道失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "pipeline_time": time.time() - pipeline_start_time
            }

    async def run_pipeline(self,
                         stock_codes: Optional[List[str]] = None,
                         max_files: Optional[int] = None,
                         min_files: Optional[int] = None,
                         batch_files: Optional[int] = None,
                         random_sample: Optional[int] = None,
                         path_filter: Optional[str] = None,
                         progress_callback: Optional[callable] = None,
                         resume_mode: bool = False,
                         retry_failed: bool = False) -> Dict[str, Any]:
        """
        运行完整的文档处理管道

        Args:
            stock_codes: 指定处理的股票代码
            max_files: 最大文件数量
            min_files: 最小文件数量
            batch_files: 分批处理数量
            random_sample: 随机抽样数量
            path_filter: 路径筛选关键词
            progress_callback: 进度回调函数
            resume_mode: 是否启用断点续传模式（跳过已处理的文档）
            retry_failed: 是否重新处理失败的文档

        Returns:
            处理结果
        """
        pipeline_start_time = time.time()

        logger.info("🚀 启动文档处理管道")

        try:
            # 1. 测试向量化功能
            logger.info("📊 测试向量化功能...")
            test_result = await self.vectorizer.test_vectorization("测试文本")
            if not test_result:
                logger.warning("⚠️ 向量化功能测试失败，但继续处理流程...")
            else:
                logger.info("✅ 向量化功能测试成功")

            # 2. 发现PDF文件
            logger.info("📁 扫描PDF文件...")
            pdf_files = self.discover_pdf_files(
                stock_codes=stock_codes,
                max_files=max_files,
                min_files=min_files,
                batch_files=batch_files,
                random_sample=random_sample,
                path_filter=path_filter
            )

            if not pdf_files:
                logger.warning("未发现可处理的PDF文件")
                return {
                    "success": True,
                    "message": "未发现可处理的PDF文件",
                    "total_files": 0,
                    "processing_time": time.time() - pipeline_start_time
                }

            # 2.1 断点续传处理
            if resume_mode:
                logger.info("🔄 启用断点续传模式，检查已处理的文档...")

                try:
                    # 获取已处理的文档
                    processed_docs = await self.vectorizer.ch_storage.get_processed_documents(stock_codes)
                    original_count = len(pdf_files)

                    # 过滤掉已处理的文档
                    pdf_files = [f for f in pdf_files if str(f) not in processed_docs]

                    skipped_count = original_count - len(pdf_files)
                    logger.info(f"📊 跳过已处理的文档: {skipped_count} 个")

                    if not pdf_files:
                        logger.info("✅ 所有文档都已处理完成")
                        return {
                            "success": True,
                            "message": "所有文档都已处理完成",
                            "total_files": original_count,
                            "skipped_files": skipped_count,
                            "processed_files": 0,
                            "processing_time": time.time() - pipeline_start_time
                        }

                except Exception as e:
                    logger.warning(f"⚠️ 断点续传检查失败，将处理所有文档: {e}")

            # 2.2 重新处理失败的文档
            if retry_failed:
                logger.info("🔄 检查失败的文档...")
                try:
                    failed_docs = await self.vectorizer.ch_storage.get_failed_documents(stock_codes)
                    if failed_docs:
                        logger.info(f"📊 发现 {len(failed_docs)} 个失败的文档，将重新处理")
                        # 这里可以添加重新处理失败文档的逻辑
                        # 暂时只记录日志
                except Exception as e:
                    logger.warning(f"⚠️ 检查失败文档失败: {e}")

            # 3. 批量处理文档
            logger.info("⚡ 开始文档处理...")
            results = await self.process_documents_batch(pdf_files, progress_callback)

            # 4. 整合最终结果
            final_result = {
                "success": True,
                "pipeline_time": time.time() - pipeline_start_time,
                **results
            }

            logger.info(f"🎉 文档处理管道完成! 总耗时: {final_result['pipeline_time']:.2f}秒")

            return final_result

        except Exception as e:
            logger.error(f"❌ 文档处理管道失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "pipeline_time": time.time() - pipeline_start_time
            }
    
    async def close(self):
        """关闭所有资源和连接"""
        logger.info("🔚 关闭DocumentProcessingPipeline资源...")
        
        try:
            # 关闭DocumentVectorizer
            if hasattr(self, 'vectorizer') and self.vectorizer:
                await self.vectorizer.close()
                logger.debug("✅ DocumentVectorizer已关闭")
            
            logger.info("✅ DocumentProcessingPipeline资源关闭完成")
            
        except Exception as e:
            logger.error(f"❌ 关闭DocumentProcessingPipeline资源时出错: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")


def create_progress_logger():
    """创建进度日志记录器"""
    def progress_callback(current: int, total: int, result: Dict[str, Any]):
        status = "✅" if result["success"] else "❌"
        file_name = result.get("file_name", "Unknown")
        
        if result["success"]:
            vectorized = result.get("vectorized_chunks", 0)
            skipped = result.get("skipped_chunks", 0)
            logger.info(f"{status} [{current}/{total}] {file_name} - 向量化: {vectorized}, 跳过: {skipped}")
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"{status} [{current}/{total}] {file_name} - 错误: {error}")
    
    return progress_callback
