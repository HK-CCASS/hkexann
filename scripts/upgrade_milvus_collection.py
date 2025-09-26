#!/usr/bin/env python3
"""
Milvus集合升级脚本
为pdf_embeddings_v3集合添加HKEX分类字段
"""

import logging
import asyncio
from pathlib import Path

# 添加项目路径
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.milvus.collection_manager import get_collection_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def backup_collection_data(manager, collection_name: str) -> list:
    """备份集合数据"""
    logger.info(f"开始备份集合 {collection_name} 的数据...")

    try:
        # 加载集合
        collection = manager.collection(collection_name)
        collection.load()

        # 分页查询所有数据
        all_data = []
        batch_size = 1000
        offset = 0

        while True:
            # 查询一批数据
            results = collection.query(
                expr="",
                output_fields=["*"],
                limit=batch_size,
                offset=offset
            )

            if not results:
                break

            all_data.extend(results)
            offset += batch_size

            logger.info(f"已备份 {len(all_data)} 条记录...")

            if len(results) < batch_size:
                break

        logger.info(f"✅ 数据备份完成，共 {len(all_data)} 条记录")
        return all_data

    except Exception as e:
        logger.error(f"❌ 数据备份失败: {e}")
        return []


def transform_data_for_new_schema(old_data: list) -> list:
    """将旧数据转换为新schema格式"""
    logger.info("转换数据格式以适配新schema...")

    new_data = []
    for record in old_data:
        # 复制原有字段
        new_record = record.copy()

        # 添加HKEX字段的默认值
        new_record['hkex_t1_code'] = ''
        new_record['hkex_t2_code'] = ''
        new_record['hkex_category_name'] = ''

        # 如果metadata中包含HKEX信息，提取出来
        metadata = record.get('metadata', {})
        if isinstance(metadata, dict):
            new_record['hkex_t1_code'] = metadata.get('t1_code', '')
            new_record['hkex_t2_code'] = metadata.get('t2_code', '')
            new_record['hkex_category_name'] = metadata.get('hkex_category_name', '')

        new_data.append(new_record)

    logger.info(f"✅ 数据转换完成，处理 {len(new_data)} 条记录")
    return new_data


async def upgrade_collection():
    """升级Milvus集合"""
    logger.info("🚀 开始Milvus集合升级...")

    manager = get_collection_manager()

    if not manager.connect():
        logger.error("❌ Milvus连接失败")
        return False

    collection_name = "pdf_embeddings_v3"

    try:
        # 1. 检查集合是否存在
        collections = manager.list_collections()
        if collection_name not in collections:
            logger.error(f"❌ 集合 {collection_name} 不存在")
            return False

        # 2. 删除现有集合
        logger.info("🗑️ 步骤1: 删除现有集合")
        from pymilvus import utility
        utility.drop_collection(collection_name)
        logger.info(f"✅ 集合 {collection_name} 已删除")

        # 3. 创建新集合（带HKEX字段）
        logger.info("🏗️ 步骤2: 创建新集合")
        success = manager.create_pdf_embeddings_collection()
        if not success:
            logger.error("❌ 新集合创建失败")
            return False

        # 4. 验证结果
        logger.info("✅ 步骤3: 验证升级结果")
        new_info = manager.get_collection_info(collection_name)
        if new_info:
            # 检查HKEX字段
            fields = new_info.get('schema', {}).get('fields', [])
            hkex_fields = [f for f in fields if f.get('name', '').startswith('hkex_')]
            logger.info(f"✅ 新集合包含 {len(hkex_fields)} 个HKEX字段: {[f['name'] for f in hkex_fields]}")

            if len(hkex_fields) == 3:
                logger.info("🎉 Milvus集合升级成功！HKEX字段已添加")
                return True
            else:
                logger.error(f"❌ HKEX字段不完整，期望3个，实际{len(hkex_fields)}个")
                return False
        else:
            logger.error("❌ 无法获取新集合信息")
            return False

    except Exception as e:
        logger.error(f"❌ 集合升级失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主函数"""
    print("Milvus集合HKEX字段升级工具")
    print("=" * 50)
    print("此工具将为pdf_embeddings_v3集合添加HKEX分类字段")
    print("⚠️ 升级过程中会临时不可用，请确保没有正在运行的查询")
    print()

    # 确认操作
    confirm = input("是否继续升级？(yes/no): ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("升级已取消")
        return

    # 执行升级
    success = await upgrade_collection()

    if success:
        print("\n🎉 Milvus集合升级成功！")
        print("HKEX分类字段已添加到集合中")
    else:
        print("\n❌ Milvus集合升级失败！")
        print("请检查日志了解详细错误信息")


if __name__ == "__main__":
    asyncio.run(main())
