"""
数据加载管道
统一管理CSV解析和ClickHouse数据加载的完整流程
"""

import logging
import time
from pathlib import Path
from typing import Dict, Any

from .clickhouse_loader import ClickHouseLoader
from .csv_parser import CSVEventParser

logger = logging.getLogger(__name__)


class DataLoadingPipeline:
    """数据加载管道"""

    def __init__(self, csv_directory: str = None, clickhouse_host: str = None, clickhouse_port: int = None):
        """
        初始化数据加载管道
        
        Args:
            csv_directory: CSV文件目录
            clickhouse_host: ClickHouse主机
            clickhouse_port: ClickHouse端口
        """
        self.csv_directory = Path(csv_directory or "event_csv")

        # 初始化组件
        self.csv_parser = CSVEventParser()
        self.clickhouse_loader = ClickHouseLoader(host=clickhouse_host, port=clickhouse_port)

        logger.info(f"数据加载管道初始化完成:")
        logger.info(f"  CSV目录: {self.csv_directory}")
        logger.info(f"  ClickHouse: {self.clickhouse_loader.host}:{self.clickhouse_loader.port}")

    async def validate_csv_files(self) -> Dict[str, bool]:
        """
        验证CSV文件是否存在
        
        Returns:
            文件存在状态字典
        """
        csv_files = {
            'rights_issue': self.csv_directory / '供股.csv', 
            'placement': self.csv_directory / '配股.csv',
            'consolidation': self.csv_directory / '合股.csv',
            'stock_split': self.csv_directory / '拆股.csv'
        }

        file_status = {}

        logger.info("📋 检查CSV文件:")

        for event_type, csv_path in csv_files.items():
            exists = csv_path.exists()
            file_status[event_type] = exists

            status = "✅" if exists else "❌"
            logger.info(f"  {status} {event_type}: {csv_path}")

            if exists:
                # 显示文件大小
                size = csv_path.stat().st_size
                logger.info(f"    文件大小: {size:,} 字节")

        return file_status

    async def parse_csv_data(self) -> Dict[str, Any]:
        """
        解析所有CSV数据
        
        Returns:
            解析结果和统计信息
        """
        start_time = time.time()

        try:
            logger.info("📊 开始解析CSV数据...")

            # 解析所有CSV文件
            events_data = self.csv_parser.parse_all_event_csvs(self.csv_directory)

            # 统计信息
            total_records = sum(len(records) for records in events_data.values())
            parsing_time = time.time() - start_time

            result = {
                "success": True, 
                "events_data": events_data, 
                "total_records": total_records,
                "rights_issue_count": len(events_data.get('rights_issue', [])),
                "placement_count": len(events_data.get('placement', [])),
                "consolidation_count": len(events_data.get('consolidation', [])), 
                "stock_split_count": len(events_data.get('stock_split', [])),
                "parsing_time": parsing_time
            }

            logger.info(f"✅ CSV数据解析完成:")
            logger.info(f"  供股记录: {result['rights_issue_count']}")
            logger.info(f"  配股记录: {result['placement_count']}")
            logger.info(f"  合股记录: {result['consolidation_count']}")
            logger.info(f"  拆股记录: {result['stock_split_count']}")
            logger.info(f"  总记录数: {result['total_records']}")
            logger.info(f"  解析耗时: {result['parsing_time']:.2f}秒")

            return result

        except Exception as e:
            logger.error(f"❌ CSV数据解析失败: {e}")
            return {"success": False, "error": str(e), "events_data": {}, "total_records": 0,
                "parsing_time": time.time() - start_time}

    async def load_data_to_clickhouse(self, events_data: Dict[str, list], clear_existing: bool = False) -> Dict[
        str, Any]:
        """
        将数据加载到ClickHouse
        
        Args:
            events_data: 事件数据
            clear_existing: 是否清空现有数据
            
        Returns:
            加载结果
        """
        try:
            logger.info("🗄️ 开始加载数据到ClickHouse...")

            result = await self.clickhouse_loader.load_all_events(events_data=events_data,
                clear_existing=clear_existing)

            return result

        except Exception as e:
            logger.error(f"❌ 数据加载失败: {e}")
            return {"success": False, "error": str(e), "tables_loaded": 0, "total_records": 0}

    async def run_full_pipeline(self, clear_existing: bool = False, validate_only: bool = False) -> Dict[str, Any]:
        """
        运行完整的数据加载管道
        
        Args:
            clear_existing: 是否清空现有数据
            validate_only: 仅验证，不加载数据
            
        Returns:
            管道执行结果
        """
        pipeline_start_time = time.time()

        logger.info("🚀 启动数据加载管道")

        try:
            # 1. 验证CSV文件
            logger.info("📋 步骤1: 验证CSV文件...")
            file_status = await self.validate_csv_files()

            missing_files = [event_type for event_type, exists in file_status.items() if not exists]
            if missing_files:
                logger.warning(f"⚠️ 缺失CSV文件: {missing_files}")

            available_files = [event_type for event_type, exists in file_status.items() if exists]
            if not available_files:
                return {"success": False, "error": "没有可用的CSV文件",
                    "pipeline_time": time.time() - pipeline_start_time}

            logger.info(f"✅ 可用CSV文件: {available_files}")

            # 2. 解析CSV数据
            logger.info("📊 步骤2: 解析CSV数据...")
            parse_result = await self.parse_csv_data()

            if not parse_result["success"]:
                return {"success": False, "error": f"CSV解析失败: {parse_result.get('error')}",
                    "pipeline_time": time.time() - pipeline_start_time}

            if parse_result["total_records"] == 0:
                return {"success": False, "error": "没有解析到有效数据",
                    "pipeline_time": time.time() - pipeline_start_time}

            # 仅验证模式
            if validate_only:
                logger.info("ℹ️ 验证模式，跳过数据加载")
                return {"success": True, "validate_only": True, "file_status": file_status,
                    "parse_result": parse_result, "pipeline_time": time.time() - pipeline_start_time}

            # 3. 加载数据到ClickHouse
            logger.info("🗄️ 步骤3: 加载数据到ClickHouse...")
            load_result = await self.load_data_to_clickhouse(events_data=parse_result["events_data"],
                clear_existing=clear_existing)

            # 4. 整合最终结果
            pipeline_time = time.time() - pipeline_start_time

            final_result = {"success": load_result["success"], "file_status": file_status, "parse_result": parse_result,
                "load_result": load_result, "pipeline_time": pipeline_time}

            if final_result["success"]:
                logger.info(f"🎉 数据加载管道完成!")
                logger.info(f"  解析记录: {parse_result['total_records']}")
                logger.info(f"  加载记录: {load_result['total_records']}")
                logger.info(f"  加载表数: {load_result['tables_loaded']}")
                logger.info(f"  总耗时: {pipeline_time:.2f}秒")
            else:
                logger.error(f"❌ 数据加载管道失败")
                if load_result.get("errors"):
                    logger.error(f"  错误: {load_result['errors']}")

            return final_result

        except Exception as e:
            logger.error(f"❌ 数据加载管道异常: {e}")
            return {"success": False, "error": str(e), "pipeline_time": time.time() - pipeline_start_time}

    async def get_data_summary(self) -> Dict[str, Any]:
        """
        获取数据摘要统计
        
        Returns:
            数据摘要信息
        """
        try:
            logger.info("📈 获取数据摘要...")

            # 获取各表记录数
            tables = ["rights_issue", "placement", "consolidation", "stock_split"]
            counts = {}

            for table in tables:
                count = await self.clickhouse_loader.get_table_count(table)
                counts[table] = count

            total_records = sum(counts.values())

            summary = {"success": True, "table_counts": counts, "total_records": total_records,
                "timestamp": time.time()}

            logger.info(f"📊 数据摘要:")
            logger.info(f"  供股记录: {counts.get('rights_issue', 0)}")
            logger.info(f"  配股记录: {counts.get('placement', 0)}")
            logger.info(f"  合股记录: {counts.get('consolidation', 0)}")
            logger.info(f"  拆股记录: {counts.get('stock_split', 0)}")
            logger.info(f"  总记录数: {total_records}")

            return summary

        except Exception as e:
            logger.error(f"❌ 获取数据摘要失败: {e}")
            return {"success": False, "error": str(e), "table_counts": {}, "total_records": 0}

    async def close(self):
        """关闭管道资源"""
        await self.clickhouse_loader.close()
        logger.info("数据加载管道已关闭")
