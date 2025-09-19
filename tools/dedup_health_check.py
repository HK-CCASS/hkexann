#!/usr/bin/env python3
"""
去重工具健康检查脚本

在执行去重操作前，检查系统状态和数据一致性。
确保操作环境安全可靠。

检查项目:
- 数据库连接状态
- 数据一致性验证
- 系统资源检查
- 备份空间检查
- 权限验证

作者: Claude 4.0 sonnet
版本: 1.0.0
"""

import asyncio
import logging
import psutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from config.settings import settings
    from services.storage.clickhouse_pdf_storage import ClickHousePDFStorage
    from services.milvus.unified_collection_manager import get_milvus_manager, CollectionType
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    logger.error(f"导入依赖失败: {e}")
    DEPENDENCIES_AVAILABLE = False


class HealthCheckResult:
    """健康检查结果"""
    
    def __init__(self):
        self.checks = {}
        self.warnings = []
        self.errors = []
        self.overall_status = "unknown"
    
    def add_check(self, name: str, status: str, details: str = "", data: Any = None):
        """添加检查结果"""
        self.checks[name] = {
            'status': status,  # 'pass', 'warning', 'fail'
            'details': details,
            'data': data,
            'timestamp': datetime.now()
        }
        
        if status == 'warning':
            self.warnings.append(f"{name}: {details}")
        elif status == 'fail':
            self.errors.append(f"{name}: {details}")
    
    def calculate_overall_status(self):
        """计算总体状态"""
        if self.errors:
            self.overall_status = "fail"
        elif self.warnings:
            self.overall_status = "warning" 
        else:
            self.overall_status = "pass"
    
    def print_report(self):
        """打印健康检查报告"""
        print("\n" + "="*60)
        print("🏥 去重工具健康检查报告")
        print("="*60)
        
        # 总体状态
        status_emoji = {
            "pass": "✅",
            "warning": "⚠️", 
            "fail": "❌",
            "unknown": "❓"
        }
        
        print(f"\n🎯 总体状态: {status_emoji[self.overall_status]} {self.overall_status.upper()}")
        print(f"📅 检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 详细检查结果
        print(f"\n📋 检查详情:")
        for name, result in self.checks.items():
            emoji = status_emoji[result['status']]
            print(f"  {emoji} {name}: {result['details']}")
        
        # 警告信息
        if self.warnings:
            print(f"\n⚠️  警告 ({len(self.warnings)} 项):")
            for warning in self.warnings:
                print(f"  - {warning}")
        
        # 错误信息
        if self.errors:
            print(f"\n❌ 错误 ({len(self.errors)} 项):")
            for error in self.errors:
                print(f"  - {error}")
        
        # 建议
        print(f"\n💡 建议:")
        if self.overall_status == "pass":
            print("  ✅ 系统状态良好，可以安全执行去重操作")
        elif self.overall_status == "warning":
            print("  ⚠️  系统存在警告，建议解决后再执行去重操作")
            print("  📖 详情请查看上方警告信息")
        else:
            print("  ❌ 系统存在严重问题，请先解决错误后再操作")
            print("  🔧 详情请查看上方错误信息")
        
        print("="*60)


class DedupHealthChecker:
    """去重工具健康检查器"""
    
    def __init__(self):
        if not DEPENDENCIES_AVAILABLE:
            raise RuntimeError("缺少必要的依赖模块")
        
        self.result = HealthCheckResult()
        self.clickhouse_storage = None
        self.milvus_manager = None
    
    async def run_all_checks(self) -> HealthCheckResult:
        """运行所有健康检查"""
        logger.info("🏥 开始健康检查...")
        
        try:
            # 基础环境检查
            await self._check_system_resources()
            await self._check_backup_space()
            
            # 数据库连接检查
            await self._check_clickhouse_connection()
            await self._check_milvus_connection()
            
            # 数据一致性检查
            await self._check_data_consistency()
            
            # 权限检查
            await self._check_permissions()
            
        except Exception as e:
            self.result.add_check(
                "overall_check",
                "fail",
                f"检查过程中发生异常: {e}"
            )
        
        # 计算总体状态
        self.result.calculate_overall_status()
        
        logger.info("✅ 健康检查完成")
        return self.result
    
    async def _check_system_resources(self):
        """检查系统资源"""
        try:
            # CPU 使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > 90:
                self.result.add_check(
                    "cpu_usage",
                    "warning", 
                    f"CPU使用率过高: {cpu_percent:.1f}%"
                )
            else:
                self.result.add_check(
                    "cpu_usage",
                    "pass",
                    f"CPU使用率正常: {cpu_percent:.1f}%"
                )
            
            # 内存使用率
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                self.result.add_check(
                    "memory_usage",
                    "warning",
                    f"内存使用率过高: {memory.percent:.1f}%"
                )
            else:
                self.result.add_check(
                    "memory_usage", 
                    "pass",
                    f"内存使用率正常: {memory.percent:.1f}%"
                )
            
            # 磁盘空间
            disk = psutil.disk_usage('/')
            free_gb = disk.free / (1024**3)
            if free_gb < 1:  # 小于1GB
                self.result.add_check(
                    "disk_space",
                    "fail",
                    f"磁盘空间不足: {free_gb:.1f}GB"
                )
            elif free_gb < 5:  # 小于5GB
                self.result.add_check(
                    "disk_space",
                    "warning", 
                    f"磁盘空间较少: {free_gb:.1f}GB"
                )
            else:
                self.result.add_check(
                    "disk_space",
                    "pass",
                    f"磁盘空间充足: {free_gb:.1f}GB"
                )
                
        except Exception as e:
            self.result.add_check(
                "system_resources",
                "fail",
                f"系统资源检查失败: {e}"
            )
    
    async def _check_backup_space(self):
        """检查备份空间"""
        try:
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            
            # 检查备份目录空间
            if backup_dir.exists():
                stat = backup_dir.stat()
                disk = psutil.disk_usage(str(backup_dir))
                free_gb = disk.free / (1024**3)
                
                if free_gb < 0.5:
                    self.result.add_check(
                        "backup_space",
                        "fail",
                        f"备份目录空间不足: {free_gb:.1f}GB"
                    )
                elif free_gb < 2:
                    self.result.add_check(
                        "backup_space",
                        "warning",
                        f"备份目录空间较少: {free_gb:.1f}GB"  
                    )
                else:
                    self.result.add_check(
                        "backup_space",
                        "pass",
                        f"备份目录空间充足: {free_gb:.1f}GB"
                    )
            else:
                self.result.add_check(
                    "backup_space",
                    "fail",
                    "无法创建备份目录"
                )
                
        except Exception as e:
            self.result.add_check(
                "backup_space",
                "fail",
                f"备份空间检查失败: {e}"
            )
    
    async def _check_clickhouse_connection(self):
        """检查 ClickHouse 连接"""
        try:
            self.clickhouse_storage = ClickHousePDFStorage(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_database,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password
            )
            
            if await self.clickhouse_storage.initialize():
                # 测试查询
                result = await self.clickhouse_storage._execute_query("SELECT 1")
                
                # 检查表是否存在
                tables_query = """
                SELECT name FROM system.tables 
                WHERE database = 'hkex_analysis' 
                AND name IN ('pdf_documents', 'pdf_chunks')
                """
                tables = await self.clickhouse_storage._execute_query(tables_query)
                
                if len(tables) >= 2:
                    self.result.add_check(
                        "clickhouse_connection",
                        "pass",
                        f"ClickHouse连接正常，找到 {len(tables)} 个必需表"
                    )
                else:
                    self.result.add_check(
                        "clickhouse_connection",
                        "warning",
                        f"ClickHouse连接正常，但只找到 {len(tables)} 个表"
                    )
            else:
                self.result.add_check(
                    "clickhouse_connection",
                    "fail",
                    "ClickHouse连接初始化失败"
                )
                
        except Exception as e:
            self.result.add_check(
                "clickhouse_connection",
                "fail",
                f"ClickHouse连接检查失败: {e}"
            )
    
    async def _check_milvus_connection(self):
        """检查 Milvus 连接"""
        try:
            self.milvus_manager = get_milvus_manager()
            
            # 创建测试连接
            connection_name = await self.milvus_manager.create_connection("health_check")
            
            # 检查集合是否存在
            async with self.milvus_manager.get_collection(CollectionType.PDF_EMBEDDINGS) as collection:
                entity_count = collection.num_entities
                
                self.result.add_check(
                    "milvus_connection",
                    "pass",
                    f"Milvus连接正常，集合包含 {entity_count} 条记录"
                )
            
            # 清理连接
            await self.milvus_manager.close_connection(connection_name)
            
        except Exception as e:
            self.result.add_check(
                "milvus_connection",
                "fail",
                f"Milvus连接检查失败: {e}"
            )
    
    async def _check_data_consistency(self):
        """检查数据一致性"""
        try:
            if not self.clickhouse_storage or not self.milvus_manager:
                self.result.add_check(
                    "data_consistency",
                    "fail",
                    "数据库连接未建立，无法检查一致性"
                )
                return
            
            # 获取 ClickHouse 中的记录数
            ch_query = "SELECT count() FROM hkex_analysis.pdf_chunks"
            ch_result = await self.clickhouse_storage._execute_query(ch_query)
            ch_count = int(ch_result[0][0]) if ch_result else 0
            
            # 获取 Milvus 中的记录数
            async with self.milvus_manager.get_collection(CollectionType.PDF_EMBEDDINGS) as collection:
                mv_count = collection.num_entities
            
            # 比较记录数
            diff = abs(ch_count - mv_count)
            diff_percent = (diff / max(ch_count, mv_count, 1)) * 100
            
            if diff_percent > 10:
                self.result.add_check(
                    "data_consistency",
                    "warning",
                    f"数据数量差异较大 - ClickHouse: {ch_count}, Milvus: {mv_count} (差异 {diff_percent:.1f}%)"
                )
            elif diff_percent > 1:
                self.result.add_check(
                    "data_consistency",
                    "warning",
                    f"数据数量略有差异 - ClickHouse: {ch_count}, Milvus: {mv_count} (差异 {diff_percent:.1f}%)"
                )
            else:
                self.result.add_check(
                    "data_consistency",
                    "pass", 
                    f"数据数量基本一致 - ClickHouse: {ch_count}, Milvus: {mv_count}"
                )
                
        except Exception as e:
            self.result.add_check(
                "data_consistency",
                "fail",
                f"数据一致性检查失败: {e}"
            )
    
    async def _check_permissions(self):
        """检查操作权限"""
        try:
            if not self.clickhouse_storage:
                self.result.add_check(
                    "permissions",
                    "fail", 
                    "ClickHouse连接未建立，无法检查权限"
                )
                return
            
            # 测试 ClickHouse 删除权限（创建测试表）
            test_table_query = """
            CREATE TABLE IF NOT EXISTS hkex_analysis.test_permissions_table (
                id String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY id
            """
            
            await self.clickhouse_storage._execute_query(test_table_query)
            
            # 插入测试数据
            insert_query = "INSERT INTO hkex_analysis.test_permissions_table (id) VALUES ('test')"
            await self.clickhouse_storage._execute_query(insert_query)
            
            # 测试 ALTER DELETE（我们去重工具使用的语法）
            delete_query = "ALTER TABLE hkex_analysis.test_permissions_table DELETE WHERE id = 'test'"
            await self.clickhouse_storage._execute_query(delete_query)
            
            # 清理测试表
            drop_query = "DROP TABLE IF EXISTS hkex_analysis.test_permissions_table"
            await self.clickhouse_storage._execute_query(drop_query)
            
            self.result.add_check(
                "permissions",
                "pass",
                "数据库操作权限验证通过"
            )
            
        except Exception as e:
            self.result.add_check(
                "permissions",
                "fail",
                f"权限检查失败: {e}"
            )
    
    async def cleanup(self):
        """清理资源"""
        try:
            if self.clickhouse_storage:
                await self.clickhouse_storage.close()
            
            if self.milvus_manager:
                await self.milvus_manager.cleanup_all_connections()
                
        except Exception as e:
            logger.warning(f"清理资源时出错: {e}")


async def main():
    """主函数"""
    print("🏥 去重工具健康检查")
    print("=" * 30)
    
    checker = DedupHealthChecker()
    
    try:
        # 运行健康检查
        result = await checker.run_all_checks()
        
        # 显示报告
        result.print_report()
        
        # 返回退出码
        if result.overall_status == "pass":
            return 0
        elif result.overall_status == "warning":
            return 1
        else:
            return 2
            
    except Exception as e:
        print(f"\n❌ 健康检查过程中发生错误: {e}")
        return 3
    finally:
        await checker.cleanup()


if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
