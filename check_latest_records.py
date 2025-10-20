#!/usr/bin/env python3
"""查询最新的PDF文档记录和HKEX分类字段"""

import asyncio
from services.storage.clickhouse_pdf_storage import ClickHousePDFStorage
import yaml

async def check_latest_records():
    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 从配置获取ClickHouse参数
    ch_config = config.get('stock_discovery', {})

    # 初始化ClickHouse存储器
    storage = ClickHousePDFStorage(
        host=ch_config.get('host', 'localhost'),
        port=ch_config.get('port', 8124),
        database=ch_config.get('database', 'hkex_analysis'),
        username=ch_config.get('user', 'root'),
        password=ch_config.get('password', '123456')
    )

    try:
        # 初始化连接
        await storage.initialize()
        print("✅ ClickHouse连接成功")

        # 查询最新记录
        query = """
        SELECT
            stock_code,
            created_at,
            hkex_classification_method,
            hkex_level1_name,
            hkex_level2_name,
            hkex_level3_name,
            hkex_classification_confidence,
            hkex_full_path,
            file_name
        FROM pdf_documents
        ORDER BY created_at DESC
        LIMIT 15
        """

        # 执行查询
        result = await storage._execute_query(query)

        print("\n📊 最新15条PDF文档记录:")
        print("-" * 150)
        print(f"{'股票代码':<8} {'时间':<19} {'分类方法':<15} {'一级':<20} {'二级':<20} {'三级':<20} {'置信度':<8} {'标题':<30}")
        print("-" * 150)

        for row in result:
            stock_code = row[0] or "N/A"
            created_at = str(row[1])[:19] if row[1] else "N/A"
            classification_method = row[2] or "N/A"
            level1_name = row[3] or "空"
            level2_name = row[4] or "空"
            level3_name = row[5] or "空"
            confidence = f"{row[6]:.2f}" if row[6] else "0.00"
            full_path = row[7] or "空"
            title = (row[8] or "N/A")[:30]

            print(f"{stock_code:<8} {created_at:<19} {classification_method:<15} {level1_name:<20} {level2_name:<20} {level3_name:<20} {confidence:<8} {title:<30}")

        # 统计分析
        print(f"\n📈 统计分析:")

        # 分类方法统计
        classification_methods = {}
        complete_records = 0
        empty_records = 0

        for row in result:
            method = row[2] or "空"
            classification_methods[method] = classification_methods.get(method, 0) + 1

            # 检查分类字段是否完整
            if row[3] and row[4]:  # level1_name 和 level2_name 不为空
                complete_records += 1
            else:
                empty_records += 1

        print(f"  分类方法分布: {classification_methods}")
        print(f"  完整HKEX分类记录: {complete_records}")
        print(f"  空HKEX分类记录: {empty_records}")

        if result:
            completion_rate = (complete_records / len(result)) * 100
            print(f"  HKEX分类完整率: {completion_rate:.1f}%")

        # 查询按时间段分组的统计
        time_query = """
        SELECT
            toStartOfHour(created_at) as hour,
            count(*) as total_count,
            countIf(hkex_level1_name != '') as complete_count,
            countIf(hkex_level1_name = '') as empty_count
        FROM pdf_documents
        WHERE created_at >= now() - INTERVAL 4 HOUR
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 10
        """

        time_result = await storage._execute_query(time_query)

        print(f"\n🕐 按小时统计最近4小时的记录:")
        print("-" * 80)
        print(f"{'小时':<19} {'总记录':<8} {'完整记录':<8} {'空记录':<8} {'完整率':<8}")
        print("-" * 80)

        for row in time_result:
            hour = str(row[0])[:19]
            total = row[1]
            complete = row[2]
            empty = row[3]
            rate = f"{(complete/total)*100:.1f}%" if total > 0 else "0.0%"

            print(f"{hour:<19} {total:<8} {complete:<8} {empty:<8} {rate:<8}")

    except Exception as e:
        print(f"❌ 查询失败: {e}")
    finally:
        if hasattr(storage, 'close'):
            await storage.close()

if __name__ == "__main__":
    asyncio.run(check_latest_records())