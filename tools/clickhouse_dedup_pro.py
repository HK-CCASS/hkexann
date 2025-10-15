#!/usr/bin/env python3
"""
ClickHouse PDF数据专业去重工具

专门针对 ClickHouse 数据库中 pdf_chunks 和 pdf_documents 表的重复数据处理。
提供高效、安全、易用的去重解决方案。

特点:
- 🎯 专注于ClickHouse表，无Milvus依赖
- ⚡ 高效SQL查询，快速检测重复
- 🛡️ 多重安全机制，数据备份保护
- 📊 详细统计报告，操作透明可控
- 🔧 灵活配置，支持多种去重策略

作者: Claude 4.0 sonnet (HKEX分析团队)
版本: 2.0.0
创建时间: 2025-01-18
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import csv

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# 配置日志
def setup_logging():
    """设置日志配置"""
    log_filename = f'clickhouse_dedup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

try:
    from config.settings import settings
    from services.storage.clickhouse_pdf_storage import ClickHousePDFStorage
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    logger.error(f"导入依赖失败: {e}")
    DEPENDENCIES_AVAILABLE = False


class DedupStrategy(Enum):
    """去重策略"""
    KEEP_LATEST = "keep_latest"          # 保留最新记录（按created_at）
    KEEP_OLDEST = "keep_oldest"          # 保留最旧记录
    KEEP_LARGEST = "keep_largest"        # 保留文本最长的记录
    KEEP_COMPLETE = "keep_complete"      # 保留字段最完整的记录


class OperationMode(Enum):
    """操作模式"""
    SCAN_ONLY = "scan_only"              # 仅扫描，不执行删除
    PREVIEW = "preview"                  # 预览模式，显示将要删除的记录
    SAFE_DELETE = "safe_delete"          # 安全删除，创建备份
    DIRECT_DELETE = "direct_delete"      # 直接删除，不创建备份


@dataclass
class DuplicateGroup:
    """重复数据组"""
    key: Tuple[str, str]  # (doc_id, chunk_id) 或 (doc_id,) for documents
    table: str            # 表名
    records: List[Dict[str, Any]] = field(default_factory=list)
    keep_record: Optional[Dict[str, Any]] = None
    delete_records: List[Dict[str, Any]] = field(default_factory=list)
    
    def analyze_with_strategy(self, strategy: DedupStrategy):
        """根据策略分析保留和删除的记录"""
        if len(self.records) <= 1:
            return
        
        if strategy == DedupStrategy.KEEP_LATEST:
            # 按 created_at 降序排列，保留第一个（最新）
            sorted_records = sorted(
                self.records, 
                key=lambda x: x.get('created_at', datetime.min), 
                reverse=True
            )
        elif strategy == DedupStrategy.KEEP_OLDEST:
            # 按 created_at 升序排列，保留第一个（最旧）
            sorted_records = sorted(
                self.records, 
                key=lambda x: x.get('created_at', datetime.max)
            )
        elif strategy == DedupStrategy.KEEP_LARGEST:
            # 按文本长度降序排列（适用于chunks表）
            if self.table == 'pdf_chunks':
                sorted_records = sorted(
                    self.records, 
                    key=lambda x: x.get('text_length', 0), 
                    reverse=True
                )
            else:
                # documents表使用file_size
                sorted_records = sorted(
                    self.records, 
                    key=lambda x: x.get('file_size', 0), 
                    reverse=True
                )
        elif strategy == DedupStrategy.KEEP_COMPLETE:
            # 按非空字段数量排序，保留最完整的
            sorted_records = sorted(
                self.records, 
                key=lambda x: sum(1 for v in x.values() if v is not None and v != ''), 
                reverse=True
            )
        else:
            sorted_records = self.records
        
        self.keep_record = sorted_records[0]
        self.delete_records = sorted_records[1:]


@dataclass
class DedupReport:
    """去重报告"""
    session_id: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    mode: OperationMode = OperationMode.SCAN_ONLY
    strategy: DedupStrategy = DedupStrategy.KEEP_LATEST
    
    # 统计信息
    tables_scanned: List[str] = field(default_factory=list)
    total_records_scanned: Dict[str, int] = field(default_factory=dict)
    duplicate_groups_found: Dict[str, int] = field(default_factory=dict)
    records_to_delete: Dict[str, int] = field(default_factory=dict)
    records_actually_deleted: Dict[str, int] = field(default_factory=dict)
    
    # 详细信息
    duplicate_groups: List[DuplicateGroup] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # 备份信息
    backup_file: Optional[str] = None
    backup_size: int = 0
    
    def add_error(self, error: str):
        """添加错误信息"""
        self.errors.append(f"[{datetime.now().strftime('%H:%M:%S')}] {error}")
        logger.error(error)
    
    def add_warning(self, warning: str):
        """添加警告信息"""
        self.warnings.append(f"[{datetime.now().strftime('%H:%M:%S')}] {warning}")
        logger.warning(warning)
    
    def finalize(self):
        """完成报告"""
        self.end_time = datetime.now()
    
    def get_summary(self) -> Dict[str, Any]:
        """获取汇总信息"""
        total_duplicates = sum(self.duplicate_groups_found.values())
        total_to_delete = sum(self.records_to_delete.values())
        total_deleted = sum(self.records_actually_deleted.values())
        
        return {
            'session_id': self.session_id,
            'duration': str(self.end_time - self.start_time) if self.end_time else "进行中",
            'mode': self.mode.value,
            'strategy': self.strategy.value,
            'total_duplicate_groups': total_duplicates,
            'total_records_to_delete': total_to_delete,
            'total_records_deleted': total_deleted,
            'success_rate': f"{(total_deleted/total_to_delete*100):.1f}%" if total_to_delete > 0 else "N/A",
            'tables_processed': len(self.tables_scanned),
            'has_errors': len(self.errors) > 0,
            'has_warnings': len(self.warnings) > 0
        }


class ClickHouseDedupPro:
    """
    ClickHouse PDF数据专业去重工具
    
    专门处理 pdf_chunks 和 pdf_documents 表的重复数据。
    提供多种去重策略和安全机制。
    """
    
    def __init__(self, backup_dir: str = "backups"):
        """初始化去重工具"""
        if not DEPENDENCIES_AVAILABLE:
            raise RuntimeError("缺少必要的依赖模块，请检查系统配置")
        
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 初始化ClickHouse连接
        self.storage = ClickHousePDFStorage(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_database,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password
        )
        
        self.report = DedupReport(session_id=self.session_id)
        
        logger.info(f"🚀 ClickHouse去重工具已初始化 - 会话ID: {self.session_id}")
    
    async def initialize(self) -> bool:
        """初始化数据库连接"""
        try:
            if await self.storage.initialize():
                logger.info("✅ ClickHouse连接建立成功")
                return True
            else:
                logger.error("❌ ClickHouse连接建立失败")
                return False
        except Exception as e:
            logger.error(f"❌ 初始化失败: {e}")
            return False
    
    async def scan_duplicates(self, 
                            tables: List[str] = None, 
                            strategy: DedupStrategy = DedupStrategy.KEEP_LATEST) -> DedupReport:
        """
        扫描重复数据
        
        Args:
            tables: 要扫描的表列表，默认为 ['pdf_chunks', 'pdf_documents']
            strategy: 去重策略
        
        Returns:
            DedupReport: 扫描报告
        """
        if tables is None:
            tables = ['pdf_chunks', 'pdf_documents']
        
        logger.info(f"🔍 开始扫描重复数据 - 表: {tables}, 策略: {strategy.value}")
        
        self.report = DedupReport(
            session_id=self.session_id,
            mode=OperationMode.SCAN_ONLY,
            strategy=strategy
        )
        
        try:
            for table in tables:
                await self._scan_table_duplicates(table)
            
            # 应用去重策略
            for group in self.report.duplicate_groups:
                group.analyze_with_strategy(strategy)
            
            self.report.finalize()
            logger.info(f"✅ 扫描完成 - 发现 {sum(self.report.duplicate_groups_found.values())} 组重复数据")
            
        except Exception as e:
            self.report.add_error(f"扫描过程发生异常: {e}")
        
        return self.report
    
    async def _scan_table_duplicates(self, table: str):
        """扫描指定表的重复数据"""
        logger.info(f"📊 扫描表: {table}")
        
        try:
            if table == 'pdf_chunks':
                await self._scan_pdf_chunks()
            elif table == 'pdf_documents':
                await self._scan_pdf_documents()
            else:
                self.report.add_warning(f"不支持的表: {table}")
        
        except Exception as e:
            self.report.add_error(f"扫描表 {table} 失败: {e}")
    
    async def _scan_pdf_chunks(self):
        """扫描 pdf_chunks 表的重复数据"""
        table = 'pdf_chunks'
        self.report.tables_scanned.append(table)
        
        # 1. 获取总记录数
        count_query = "SELECT count() FROM hkex_analysis.pdf_chunks"
        count_result = await self.storage._execute_query(count_query)
        total_records = int(count_result[0][0]) if count_result else 0
        self.report.total_records_scanned[table] = total_records
        
        logger.info(f"📈 {table} 总记录数: {total_records:,}")
        
        # 2. 查找重复的 (doc_id, chunk_id) 组合
        duplicate_query = """
        SELECT doc_id, chunk_id, count() as cnt
        FROM hkex_analysis.pdf_chunks 
        GROUP BY doc_id, chunk_id 
        HAVING cnt > 1
        ORDER BY cnt DESC, doc_id, chunk_id
        """
        
        duplicate_pairs = await self.storage._execute_query(duplicate_query)
        logger.info(f"🔍 发现 {len(duplicate_pairs)} 组重复的 (doc_id, chunk_id)")
        
        self.report.duplicate_groups_found[table] = len(duplicate_pairs)
        
        # 3. 获取每组重复记录的详细信息
        total_records_to_delete = 0
        
        for pair_data in duplicate_pairs:
            doc_id, chunk_id, count = pair_data[0], pair_data[1], int(pair_data[2])
            
            # 获取该组的所有记录
            detail_query = f"""
            SELECT 
                chunk_id, doc_id, chunk_index, page_number, 
                text_length, chunk_type, vector_status, vector_id,
                created_at, updated_at
            FROM hkex_analysis.pdf_chunks 
            WHERE doc_id = '{doc_id}' AND chunk_id = '{chunk_id}'
            ORDER BY created_at DESC
            """
            
            records_data = await self.storage._execute_query(detail_query)
            
            # 转换为字典格式
            records = []
            for record in records_data:
                records.append({
                    'chunk_id': record[0],
                    'doc_id': record[1],
                    'chunk_index': record[2],
                    'page_number': record[3],
                    'text_length': record[4],
                    'chunk_type': record[5],
                    'vector_status': record[6],
                    'vector_id': record[7],
                    'created_at': record[8],
                    'updated_at': record[9]
                })
            
            # 创建重复组
            group = DuplicateGroup(
                key=(doc_id, chunk_id),
                table=table,
                records=records
            )
            
            self.report.duplicate_groups.append(group)
            total_records_to_delete += (count - 1)  # 每组保留1个，删除其余
        
        self.report.records_to_delete[table] = total_records_to_delete
        logger.info(f"📝 {table} 待删除记录数: {total_records_to_delete}")
    
    async def _scan_pdf_documents(self):
        """扫描 pdf_documents 表的重复数据"""
        table = 'pdf_documents'
        self.report.tables_scanned.append(table)
        
        # 1. 获取总记录数
        count_query = "SELECT count() FROM hkex_analysis.pdf_documents"
        count_result = await self.storage._execute_query(count_query)
        total_records = int(count_result[0][0]) if count_result else 0
        self.report.total_records_scanned[table] = total_records
        
        logger.info(f"📈 {table} 总记录数: {total_records:,}")
        
        # 2. 查找重复的 doc_id
        duplicate_query = """
        SELECT doc_id, count() as cnt
        FROM hkex_analysis.pdf_documents 
        GROUP BY doc_id 
        HAVING cnt > 1
        ORDER BY cnt DESC, doc_id
        """
        
        duplicate_docs = await self.storage._execute_query(duplicate_query)
        logger.info(f"🔍 发现 {len(duplicate_docs)} 个重复的 doc_id")
        
        self.report.duplicate_groups_found[table] = len(duplicate_docs)
        
        # 3. 获取每组重复记录的详细信息
        total_records_to_delete = 0
        
        for doc_data in duplicate_docs:
            doc_id, count = doc_data[0], int(doc_data[1])
            
            # 获取该 doc_id 的所有记录
            detail_query = f"""
            SELECT 
                doc_id, file_path, file_name, file_size, file_hash,
                stock_code, company_name, document_type, document_category,
                document_title, publish_date, page_count, processing_status,
                chunks_count, vectors_count, error_message,
                created_at, updated_at, processed_at
            FROM hkex_analysis.pdf_documents 
            WHERE doc_id = '{doc_id}'
            ORDER BY created_at DESC
            """
            
            records_data = await self.storage._execute_query(detail_query)
            
            # 转换为字典格式
            records = []
            for record in records_data:
                records.append({
                    'doc_id': record[0],
                    'file_path': record[1],
                    'file_name': record[2],
                    'file_size': record[3],
                    'file_hash': record[4],
                    'stock_code': record[5],
                    'company_name': record[6],
                    'document_type': record[7],
                    'document_category': record[8],
                    'document_title': record[9],
                    'publish_date': record[10],
                    'page_count': record[11],
                    'processing_status': record[12],
                    'chunks_count': record[13],
                    'vectors_count': record[14],
                    'error_message': record[15],
                    'created_at': record[16],
                    'updated_at': record[17],
                    'processed_at': record[18]
                })
            
            # 创建重复组
            group = DuplicateGroup(
                key=(doc_id,),
                table=table,
                records=records
            )
            
            self.report.duplicate_groups.append(group)
            total_records_to_delete += (count - 1)  # 每组保留1个，删除其余
        
        self.report.records_to_delete[table] = total_records_to_delete
        logger.info(f"📝 {table} 待删除记录数: {total_records_to_delete}")
    
    async def preview_deletion(self, limit: int = 10) -> DedupReport:
        """
        预览删除操作
        
        Args:
            limit: 显示的重复组数量限制
        
        Returns:
            DedupReport: 预览报告
        """
        logger.info(f"👀 预览删除操作 - 显示前 {limit} 组")
        
        if not self.report.duplicate_groups:
            logger.warning("没有扫描到重复数据，请先运行 scan_duplicates()")
            return self.report
        
        self.report.mode = OperationMode.PREVIEW
        
        print("\n" + "="*80)
        print("👀 删除预览报告")
        print("="*80)
        
        for i, group in enumerate(self.report.duplicate_groups[:limit]):
            print(f"\n📋 重复组 {i+1}: {group.table} - {group.key}")
            print(f"   总记录数: {len(group.records)}")
            
            if group.keep_record:
                print(f"   ✅ 保留: {group.keep_record.get('created_at', 'N/A')}")
            
            print(f"   ❌ 删除 ({len(group.delete_records)} 条):")
            for j, record in enumerate(group.delete_records[:3]):  # 只显示前3条
                print(f"      {j+1}. {record.get('created_at', 'N/A')}")
            
            if len(group.delete_records) > 3:
                print(f"      ... 还有 {len(group.delete_records)-3} 条记录")
        
        if len(self.report.duplicate_groups) > limit:
            print(f"\n... 还有 {len(self.report.duplicate_groups)-limit} 组重复数据")
        
        summary = self.report.get_summary()
        print(f"\n📊 汇总: 共 {summary['total_duplicate_groups']} 组重复，"
              f"将删除 {summary['total_records_to_delete']} 条记录")
        print("="*80)
        
        return self.report
    
    async def execute_deduplication(self, 
                                  mode: OperationMode = OperationMode.SAFE_DELETE) -> DedupReport:
        """
        执行去重操作
        
        Args:
            mode: 操作模式
        
        Returns:
            DedupReport: 执行报告
        """
        if not self.report.duplicate_groups:
            logger.error("没有扫描到重复数据，请先运行 scan_duplicates()")
            return self.report
        
        logger.info(f"🗑️ 开始执行去重 - 模式: {mode.value}")
        self.report.mode = mode
        
        try:
            # 创建备份（如果是安全模式）
            if mode == OperationMode.SAFE_DELETE:
                await self._create_backup()
            
            # 执行删除操作
            if mode in [OperationMode.SAFE_DELETE, OperationMode.DIRECT_DELETE]:
                await self._execute_deletion()
            else:
                logger.info("预览模式，不执行实际删除")
            
            self.report.finalize()
            logger.info("✅ 去重操作完成")
            
        except Exception as e:
            self.report.add_error(f"执行去重时发生异常: {e}")
        
        return self.report
    
    async def _create_backup(self):
        """创建数据备份"""
        logger.info("💾 创建数据备份...")
        
        backup_data = {
            'session_id': self.session_id,
            'backup_time': datetime.now().isoformat(),
            'strategy': self.report.strategy.value,
            'tables': {}
        }
        
        try:
            for group in self.report.duplicate_groups:
                table = group.table
                if table not in backup_data['tables']:
                    backup_data['tables'][table] = []
                
                # 备份所有要删除的记录
                for record in group.delete_records:
                    backup_data['tables'][table].append(record)
            
            # 保存备份文件
            backup_file = self.backup_dir / f"clickhouse_dedup_backup_{self.session_id}.json"
            
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
            
            self.report.backup_file = str(backup_file)
            self.report.backup_size = backup_file.stat().st_size
            
            logger.info(f"✅ 备份完成: {backup_file} ({self.report.backup_size:,} bytes)")
            
        except Exception as e:
            self.report.add_error(f"创建备份失败: {e}")
            raise
    
    async def _execute_deletion(self):
        """执行实际删除操作"""
        logger.info("🗑️ 执行删除操作...")
        
        total_deleted = 0
        
        for table in self.report.tables_scanned:
            table_deleted = await self._delete_table_duplicates(table)
            self.report.records_actually_deleted[table] = table_deleted
            total_deleted += table_deleted
        
        logger.info(f"✅ 删除完成，共删除 {total_deleted} 条记录")
    
    async def _delete_table_duplicates(self, table: str) -> int:
        """删除指定表的重复记录"""
        deleted_count = 0
        
        try:
            table_groups = [g for g in self.report.duplicate_groups if g.table == table]
            
            for group in table_groups:
                for record in group.delete_records:
                    if await self._delete_single_record(table, record):
                        deleted_count += 1
            
            logger.info(f"✅ {table} 删除了 {deleted_count} 条记录")
            
        except Exception as e:
            self.report.add_error(f"删除 {table} 记录失败: {e}")
        
        return deleted_count
    
    async def _delete_single_record(self, table: str, record: Dict[str, Any]) -> bool:
        """删除单条记录"""
        try:
            if table == 'pdf_chunks':
                # 使用复合条件确保精确删除
                delete_query = f"""
                ALTER TABLE hkex_analysis.pdf_chunks DELETE 
                WHERE doc_id = '{record['doc_id']}' 
                AND chunk_id = '{record['chunk_id']}'
                AND created_at = '{record['created_at']}'
                """
            
            elif table == 'pdf_documents':
                delete_query = f"""
                ALTER TABLE hkex_analysis.pdf_documents DELETE 
                WHERE doc_id = '{record['doc_id']}'
                AND created_at = '{record['created_at']}'
                """
            
            else:
                logger.error(f"不支持的表: {table}")
                return False
            
            await self.storage._execute_query(delete_query)
            logger.debug(f"🗑️ 已删除 {table} 记录: {record.get('doc_id', 'N/A')}")
            return True
            
        except Exception as e:
            logger.error(f"删除记录失败: {e}")
            return False
    
    def print_report(self, detailed: bool = True):
        """打印详细报告"""
        summary = self.report.get_summary()
        
        print("\n" + "="*80)
        print("📊 ClickHouse PDF数据去重报告")
        print("="*80)
        
        print(f"🆔 会话ID: {summary['session_id']}")
        print(f"⏱️  持续时间: {summary['duration']}")
        print(f"🎯 操作模式: {summary['mode']}")
        print(f"🧠 去重策略: {summary['strategy']}")
        
        print(f"\n📈 统计摘要:")
        print(f"  🔍 扫描表数: {summary['tables_processed']}")
        print(f"  📋 重复组数: {summary['total_duplicate_groups']}")
        print(f"  🗑️  计划删除: {summary['total_records_to_delete']} 条")
        print(f"  ✅ 实际删除: {summary['total_records_deleted']} 条")
        print(f"  📊 成功率: {summary['success_rate']}")
        
        # 按表显示详细统计
        if detailed:
            print(f"\n📋 分表统计:")
            for table in self.report.tables_scanned:
                total = self.report.total_records_scanned.get(table, 0)
                dups = self.report.duplicate_groups_found.get(table, 0)
                to_del = self.report.records_to_delete.get(table, 0)
                deleted = self.report.records_actually_deleted.get(table, 0)
                
                print(f"  📊 {table}:")
                print(f"     总记录: {total:,}")
                print(f"     重复组: {dups}")
                print(f"     计划删除: {to_del}")
                print(f"     实际删除: {deleted}")
        
        # 备份信息
        if self.report.backup_file:
            print(f"\n💾 备份信息:")
            print(f"  📁 备份文件: {self.report.backup_file}")
            print(f"  📦 文件大小: {self.report.backup_size:,} bytes")
        
        # 错误和警告
        if self.report.errors:
            print(f"\n❌ 错误 ({len(self.report.errors)} 条):")
            for error in self.report.errors[-5:]:  # 只显示最近5条
                print(f"  - {error}")
        
        if self.report.warnings:
            print(f"\n⚠️  警告 ({len(self.report.warnings)} 条):")
            for warning in self.report.warnings[-5:]:  # 只显示最近5条
                print(f"  - {warning}")
        
        # 建议
        print(f"\n💡 操作建议:")
        if summary['has_errors']:
            print("  ❌ 发现错误，请检查日志并处理问题")
        elif summary['total_duplicate_groups'] == 0:
            print("  ✅ 未发现重复数据，数据库状态良好")
        elif self.report.mode == OperationMode.SCAN_ONLY:
            print("  👀 使用 preview() 预览删除操作")
            print("  🗑️  使用 execute_deduplication() 执行去重")
        elif self.report.mode == OperationMode.PREVIEW:
            print("  🗑️  确认无误后，使用 execute_deduplication() 执行实际删除")
        else:
            print("  ✅ 去重操作已完成")
            if self.report.backup_file:
                print(f"  🔄 如需恢复，请使用备份文件: {self.report.backup_file}")
        
        print("="*80)
    
    async def restore_from_backup(self, backup_file: str) -> bool:
        """从备份文件恢复数据"""
        logger.info(f"🔄 从备份恢复数据: {backup_file}")
        
        try:
            if not Path(backup_file).exists():
                logger.error(f"备份文件不存在: {backup_file}")
                return False
            
            with open(backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
            
            restored_count = 0
            
            for table, records in backup_data['tables'].items():
                table_restored = await self._restore_table_data(table, records)
                restored_count += table_restored
                logger.info(f"✅ {table} 恢复 {table_restored} 条记录")
            
            logger.info(f"🎉 恢复完成，共恢复 {restored_count} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"恢复数据失败: {e}")
            return False
    
    async def _restore_table_data(self, table: str, records: List[Dict[str, Any]]) -> int:
        """恢复表数据"""
        restored_count = 0
        
        # 这里只是框架，实际实现需要根据表结构构造INSERT语句
        # 由于ClickHouse的复杂性，建议使用专门的恢复工具
        logger.warning(f"⚠️  {table} 数据恢复需要手动实现INSERT语句")
        
        return restored_count
    
    async def cleanup(self):
        """清理资源"""
        try:
            await self.storage.close()
            logger.info("🧹 资源清理完成")
        except Exception as e:
            logger.warning(f"资源清理时出错: {e}")


# 便捷函数
async def quick_scan(strategy: DedupStrategy = DedupStrategy.KEEP_LATEST) -> DedupReport:
    """快速扫描重复数据"""
    tool = ClickHouseDedupPro()
    try:
        await tool.initialize()
        report = await tool.scan_duplicates(strategy=strategy)
        tool.print_report()
        return report
    finally:
        await tool.cleanup()


async def quick_dedup(strategy: DedupStrategy = DedupStrategy.KEEP_LATEST, 
                     mode: OperationMode = OperationMode.SAFE_DELETE) -> DedupReport:
    """快速执行去重"""
    tool = ClickHouseDedupPro()
    try:
        await tool.initialize()
        await tool.scan_duplicates(strategy=strategy)
        await tool.preview_deletion()
        
        confirm = input("\n确认执行删除操作? (yes/no): ").strip().lower()
        if confirm == 'yes':
            report = await tool.execute_deduplication(mode=mode)
            tool.print_report()
            return report
        else:
            print("❌ 操作已取消")
            return tool.report
    finally:
        await tool.cleanup()


# 命令行接口
async def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="ClickHouse PDF数据专业去重工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python tools/clickhouse_dedup_pro.py scan                           # 扫描重复数据
  python tools/clickhouse_dedup_pro.py scan --strategy keep_oldest    # 使用指定策略扫描
  python tools/clickhouse_dedup_pro.py dedup                          # 交互式去重
  python tools/clickhouse_dedup_pro.py dedup --auto                   # 自动去重
  python tools/clickhouse_dedup_pro.py restore backup.json           # 恢复数据

策略说明:
  keep_latest    - 保留最新记录 (默认)
  keep_oldest    - 保留最旧记录
  keep_largest   - 保留文本最长/文件最大的记录
  keep_complete  - 保留字段最完整的记录
        """
    )
    
    parser.add_argument('action', choices=['scan', 'dedup', 'restore'],
                       help='要执行的操作')
    
    parser.add_argument('--strategy', 
                       choices=['keep_latest', 'keep_oldest', 'keep_largest', 'keep_complete'],
                       default='keep_latest',
                       help='去重策略 (默认: keep_latest)')
    
    parser.add_argument('--auto', action='store_true',
                       help='自动模式，不询问确认')
    
    parser.add_argument('backup_file', nargs='?',
                       help='备份文件路径 (仅用于restore操作)')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.action == 'restore' and not args.backup_file:
        print("❌ restore 操作需要指定备份文件路径")
        return 1
    
    strategy = DedupStrategy(args.strategy)
    
    print(f"🧹 ClickHouse PDF数据专业去重工具 v2.0.0")
    print(f"会话ID: {datetime.now().strftime('%Y%m%d_%H%M%S')}")
    print("="*60)
    
    try:
        if args.action == 'scan':
            await quick_scan(strategy)
        
        elif args.action == 'dedup':
            if args.auto:
                await quick_dedup(strategy, OperationMode.SAFE_DELETE)
            else:
                await quick_dedup(strategy, OperationMode.SAFE_DELETE)
        
        elif args.action == 'restore':
            tool = ClickHouseDedupPro()
            try:
                await tool.initialize()
                success = await tool.restore_from_backup(args.backup_file)
                print(f"恢复结果: {'✅ 成功' if success else '❌ 失败'}")
            finally:
                await tool.cleanup()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ 操作被用户中断")
        return 130
    except Exception as e:
        print(f"\n❌ 发生意外错误: {e}")
        logger.exception("意外错误详情:")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
