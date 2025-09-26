#!/usr/bin/env python3
"""
数据库字段集成测试
测试HKEX分类字段在ClickHouse和Milvus中的完整集成
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from services.storage.clickhouse_pdf_storage import ClickHousePDFStorage
from services.monitor.classification_parser import get_classification_parser
from services.monitor.data_flow.format_adapters import AnnouncementFormatAdapter, StandardAnnouncement

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_classification_parser_integration():
    """测试分类解析器集成"""
    print("=== 测试HKEX分类解析器集成 ===")

    # 获取解析器
    parser = get_classification_parser()
    if not parser.load_classifications():
        print("❌ 分类文件加载失败")
        return False

    # 测试分类查找
    test_codes = ['23400', '26300', '25100']
    for code in test_codes:
        category = parser.get_category_by_code(code)
        if category:
            print(f"✅ 代码 {code}: {category.level3_name}")
        else:
            print(f"❌ 代码 {code}: 未找到")

    return True


def test_format_adapter_integration():
    """测试格式适配器集成"""
    print("\n=== 测试格式适配器集成 ===")

    adapter = AnnouncementFormatAdapter()

    # 模拟带有HKEX分类代码的API数据
    mock_api_data = {
        'sc': '00700',
        'sn': '腾讯控股',
        'title': '测试公告',
        'lTxt': '通告及告示',
        'relTime': '2025-09-26 14:30:00',
        'webPath': '/listedco/listconews/sehk/2025/0926/LTN202509261234.pdf',
        'size': '12345',
        'ext': 'pdf',
        'newsId': '123456',
        'market': 'SEHK',
        't1Code': '20000',  # HKEX 1级分类代码
        't2Code': '23400'   # HKEX 3级分类代码
    }

    print(f"API数据包含字段: {list(mock_api_data.keys())}")
    print(f"t1Code值: '{mock_api_data.get('t1Code')}'")
    print(f"t2Code值: '{mock_api_data.get('t2Code')}'")

    # 适配数据
    announcement = adapter.adapt_announcement(mock_api_data)
    if not announcement:
        print("❌ 数据适配失败")
        return False

    # 验证HKEX字段
    print("✅ 数据适配成功")
    print(f"   股票代码: {announcement.stock_code}")
    print(f"   标题: {announcement.title}")
    print(f"   T1代码: {announcement.t1_code}")
    print(f"   T2代码: {announcement.t2_code}")
    print(f"   LONG_TEXT: {announcement.long_text}")

    # 验证字段存在
    if not announcement.t1_code or not announcement.t2_code:
        print("❌ HKEX分类字段缺失")
        return False

    return True


async def test_clickhouse_storage_integration():
    """测试ClickHouse存储集成"""
    print("\n=== 测试ClickHouse存储集成 ===")

    try:
        # 初始化存储器
        storage = ClickHousePDFStorage()

        # 测试元数据存储
        test_metadata = {
            'stock_code': '00700',
            'company_name': '腾讯控股',
            'document_type': '通函',
            'document_category': '會議/表決',
            'document_title': '股东大会通告',
            'published_date': datetime.now().date(),
            't1_code': '20000',  # HKEX 1级分类代码
            't2_code': '23400',  # HKEX 3级分类代码
            'hkex_category_name': '在股東批准的情況下重選或委任董事'
        }

        # 生成测试文档ID
        doc_id = f"test_hkex_integration_{int(datetime.now().timestamp())}"

        success = await storage.store_document_metadata(
            doc_id=doc_id,
            file_path=f"/tmp/test_{doc_id}.pdf",
            metadata=test_metadata
        )

        if success:
            print("✅ ClickHouse元数据存储成功")
            print(f"   文档ID: {doc_id}")
            print(f"   HKEX T1代码: {test_metadata['t1_code']}")
            print(f"   HKEX T2代码: {test_metadata['t2_code']}")
            print(f"   HKEX分类名称: {test_metadata['hkex_category_name']}")
            return True
        else:
            print("❌ ClickHouse元数据存储失败")
            return False

    except Exception as e:
        print(f"❌ ClickHouse测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_milvus_schema_integration():
    """测试Milvus schema集成"""
    print("\n=== 测试Milvus Schema集成 ===")

    try:
        from services.milvus.collection_manager import get_collection_manager

        # 获取集合管理器
        manager = get_collection_manager()

        if not manager.connect():
            print("❌ Milvus连接失败")
            return False

        # 检查集合是否存在
        collections = manager.list_collections()
        target_collection = "pdf_embeddings_v3"

        if target_collection not in collections:
            print(f"❌ 集合 {target_collection} 不存在")
            return False

        # 获取集合信息
        collection_info = manager.get_collection_info(target_collection)
        if not collection_info:
            print("❌ 获取集合信息失败")
            return False

        # 检查HKEX字段是否存在
        fields = collection_info.get('schema', {}).get('fields', [])
        hkex_fields = ['hkex_t1_code', 'hkex_t2_code', 'hkex_category_name']
        found_fields = []

        for field in fields:
            field_name = field.get('name', '')
            if field_name in hkex_fields:
                found_fields.append(field_name)

        if len(found_fields) == len(hkex_fields):
            print("✅ Milvus schema包含所有HKEX字段:")
            for field in found_fields:
                print(f"   - {field}")
            return True
        else:
            print(f"❌ Milvus schema缺少HKEX字段，找到: {found_fields}")
            return False

    except Exception as e:
        print(f"❌ Milvus测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_data_flow_integration():
    """测试完整数据流程集成"""
    print("\n=== 测试完整数据流程集成 ===")

    try:
        # 1. 创建模拟公告数据（包含HKEX分类）
        mock_announcement = {
            'STOCK_CODE': '00700',
            'STOCK_NAME': '腾讯控股',
            'TITLE': '股东大会通告',
            'LONG_TEXT': '通告及告示',
            'DATE_TIME': '2025-09-26 14:30:00',
            'FILE_LINK': 'https://www1.hkexnews.hk/test.pdf',  # 添加必需字段
            'T1_CODE': '20000',
            'T2_CODE': '23400'
        }

        print("✅ 模拟公告数据创建成功")
        print(f"   股票: {mock_announcement['STOCK_CODE']} - {mock_announcement['STOCK_NAME']}")
        print(f"   分类: T1={mock_announcement['T1_CODE']}, T2={mock_announcement['T2_CODE']}")

        # 2. 测试格式适配
        adapter = AnnouncementFormatAdapter()
        standard_announcement = adapter.adapt_announcement(mock_announcement)

        if not standard_announcement:
            print("❌ 格式适配失败")
            return False

        print("✅ 格式适配成功")
        print(f"   标准化字段: t1_code={standard_announcement.t1_code}, t2_code={standard_announcement.t2_code}")

        # 3. 验证分类解析器可以识别这些代码
        parser = get_classification_parser()
        if parser.load_classifications():
            category = parser.get_category_by_code(mock_announcement['T2_CODE'])
            if category:
                print("✅ 分类解析成功")
                print(f"   分类名称: {category.level3_name}")
                print(f"   完整路径: {category.full_path}")
            else:
                print("❌ 分类解析失败")
                return False

        return True

    except Exception as e:
        print(f"❌ 数据流程测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主测试函数"""
    print("🚀 数据库字段集成测试")
    print("=" * 60)

    try:
        # 运行各项测试
        tests = [
            ("HKEX分类解析器", test_classification_parser_integration),
            ("格式适配器", test_format_adapter_integration),
            ("ClickHouse存储", test_clickhouse_storage_integration),
            ("Milvus Schema", test_milvus_schema_integration),
            ("完整数据流程", test_data_flow_integration)
        ]

        results = []
        for test_name, test_func in tests:
            print(f"\n🔍 运行测试: {test_name}")
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results.append((test_name, result))
            status = "✅ 通过" if result else "❌ 失败"
            print(f"📊 测试结果: {status}")

        # 汇总结果
        print("\n" + "=" * 60)
        print("📈 测试汇总:")

        passed = 0
        total = len(results)

        for test_name, result in results:
            status = "✅" if result else "❌"
            print(f"   {status} {test_name}")
            if result:
                passed += 1

        success_rate = (passed / total) * 100
        print(f"通过率: {success_rate:.1f}%")
        if passed == total:
            print("\n🎉 所有测试通过！数据库字段集成成功")
            return True
        else:
            print(f"\n⚠️  {total - passed} 个测试失败，请检查相关配置")
            return False

    except Exception as e:
        print(f"\n❌ 测试框架异常: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
