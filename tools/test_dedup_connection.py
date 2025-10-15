#!/usr/bin/env python3
"""
ClickHouse去重工具连接测试脚本

用于验证新开发的去重工具是否能正常连接数据库和基本功能。
在实际使用前运行此测试确保环境正常。

作者: Claude 4.0 sonnet
版本: 1.0.0
"""

import asyncio
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

async def test_connection():
    """测试数据库连接"""
    print("🔌 测试ClickHouse连接...")
    
    try:
        from tools.clickhouse_dedup_pro import ClickHouseDedupPro
        
        tool = ClickHouseDedupPro()
        
        if await tool.initialize():
            print("✅ ClickHouse连接成功")
            
            # 测试基本查询
            try:
                count_query = "SELECT count() FROM hkex_analysis.pdf_chunks LIMIT 1"
                result = await tool.storage._execute_query(count_query)
                chunks_count = int(result[0][0]) if result else 0
                print(f"📊 pdf_chunks 表记录数: {chunks_count:,}")
                
                count_query = "SELECT count() FROM hkex_analysis.pdf_documents LIMIT 1"
                result = await tool.storage._execute_query(count_query)
                docs_count = int(result[0][0]) if result else 0
                print(f"📊 pdf_documents 表记录数: {docs_count:,}")
                
            except Exception as e:
                print(f"⚠️  查询测试失败: {e}")
            
        else:
            print("❌ ClickHouse连接失败")
            return False
        
        await tool.cleanup()
        return True
        
    except ImportError as e:
        print(f"❌ 导入模块失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 连接测试异常: {e}")
        return False


async def test_scan_function():
    """测试扫描功能"""
    print("\n🔍 测试扫描功能...")
    
    try:
        from tools.clickhouse_dedup_pro import ClickHouseDedupPro, DedupStrategy
        
        tool = ClickHouseDedupPro()
        
        if not await tool.initialize():
            print("❌ 初始化失败")
            return False
        
        # 测试扫描（只扫描少量数据）
        print("📊 执行快速扫描...")
        report = await tool.scan_duplicates()
        
        summary = report.get_summary()
        print(f"✅ 扫描完成")
        print(f"   扫描表数: {summary['tables_processed']}")
        print(f"   重复组数: {summary['total_duplicate_groups']}")
        print(f"   计划删除: {summary['total_records_to_delete']} 条")
        
        await tool.cleanup()
        return True
        
    except Exception as e:
        print(f"❌ 扫描测试失败: {e}")
        return False


async def test_simple_tool():
    """测试简易工具"""
    print("\n🚀 测试简易工具导入...")
    
    try:
        from tools.simple_dedup import scan_only
        print("✅ 简易工具导入成功")
        
        # 这里不实际运行扫描，只测试导入
        return True
        
    except ImportError as e:
        print(f"❌ 简易工具导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 简易工具测试异常: {e}")
        return False


async def main():
    """主测试函数"""
    print("🧪 ClickHouse去重工具测试")
    print("=" * 40)
    
    tests = [
        ("数据库连接", test_connection),
        ("扫描功能", test_scan_function), 
        ("简易工具", test_simple_tool)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 测试: {test_name}")
        try:
            if await test_func():
                print(f"✅ {test_name} - 通过")
                passed += 1
            else:
                print(f"❌ {test_name} - 失败")
        except Exception as e:
            print(f"💥 {test_name} - 异常: {e}")
    
    print(f"\n📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！工具可以正常使用")
        print("\n💡 接下来可以:")
        print("   python tools/simple_dedup.py --scan-only")
        print("   python tools/clickhouse_dedup_pro.py scan")
        return True
    else:
        print("⚠️  部分测试失败，请检查环境配置")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n❌ 测试被中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 测试过程发生异常: {e}")
        sys.exit(1)
