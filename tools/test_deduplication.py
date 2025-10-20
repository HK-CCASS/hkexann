#!/usr/bin/env python3
"""
ClickHouse去重工具测试脚本

用于验证去重工具的基本功能，包括连接测试、配置验证等。
这个脚本不会修改任何数据，只进行基本的连接和功能测试。

作者: Claude 4.0 sonnet AI助手
版本: 1.0.0
"""

import asyncio
import sys
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from tools.clickhouse_deduplication_tool import ClickHouseDeduplicationTool
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_connection():
    """测试 ClickHouse 连接"""
    print("🔌 测试 ClickHouse 连接...")
    
    try:
        async with ClickHouseDeduplicationTool() as tool:
            print("✅ 连接成功!")
            return True
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False


async def test_table_verification():
    """测试表验证功能"""
    print("📋 验证数据库表...")
    
    try:
        async with ClickHouseDeduplicationTool() as tool:
            # 这个方法在 initialize() 中已经被调用了
            print("✅ 表验证成功!")
            return True
    except Exception as e:
        print(f"❌ 表验证失败: {e}")
        return False


async def test_analysis_function():
    """测试分析功能（只读操作）"""
    print("🔍 测试重复数据分析...")
    
    try:
        async with ClickHouseDeduplicationTool() as tool:
            # 测试 pdf_documents 表分析
            analysis = await tool.analyze_duplicates('pdf_documents')
            print(f"📊 pdf_documents 分析结果:")
            print(f"   总记录: {analysis['total_records']:,}")
            print(f"   重复记录: {analysis['total_duplicates']:,}")
            print(f"   重复率: {analysis['duplicate_rate']:.2f}%")
            
            # 测试 pdf_chunks 表分析
            analysis = await tool.analyze_duplicates('pdf_chunks') 
            print(f"📊 pdf_chunks 分析结果:")
            print(f"   总记录: {analysis['total_records']:,}")
            print(f"   重复记录: {analysis['total_duplicates']:,}")
            print(f"   重复率: {analysis['duplicate_rate']:.2f}%")
            
            print("✅ 分析功能测试成功!")
            return True
    except Exception as e:
        print(f"❌ 分析功能测试失败: {e}")
        return False


async def run_all_tests():
    """运行所有测试"""
    print("🚀 HKEX ClickHouse 去重工具测试")
    print("=" * 50)
    
    tests = [
        ("连接测试", test_connection),
        ("表验证测试", test_table_verification), 
        ("分析功能测试", test_analysis_function)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 30)
        
        try:
            result = await test_func()
            if result:
                passed += 1
        except Exception as e:
            print(f"❌ 测试异常: {e}")
    
    print(f"\n📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！工具已准备就绪。")
        print("\n💡 后续步骤:")
        print("1. 运行 python tools/run_deduplication.py 开始使用")
        print("2. 或使用命令行: python tools/clickhouse_deduplication_tool.py --analyze-only")
    else:
        print("⚠️  部分测试失败，请检查配置和连接。")
        
    return passed == total


if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(run_all_tests())
    except KeyboardInterrupt:
        print("\n测试已中断")
    except Exception as e:
        print(f"测试执行失败: {e}")











