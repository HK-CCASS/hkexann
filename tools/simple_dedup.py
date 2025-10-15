#!/usr/bin/env python3
"""
ClickHouse PDF数据简易去重脚本

提供最简单直接的去重功能，适合日常快速使用。
专门处理 pdf_chunks 和 pdf_documents 表的重复数据。

特点：
- 🚀 一键运行，开箱即用
- 📊 清晰的统计报告
- 🛡️ 自动备份保护
- ⚡ 高效SQL查询

使用方法：
  python tools/simple_dedup.py                    # 交互式操作
  python tools/simple_dedup.py --auto             # 自动去重
  python tools/simple_dedup.py --scan-only        # 仅扫描
  python tools/simple_dedup.py --dry-run          # 预览模式

作者: Claude 4.0 sonnet
版本: 1.0.0
"""

import asyncio
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from tools.clickhouse_dedup_pro import ClickHouseDedupPro, DedupStrategy, OperationMode
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    print("请确保从项目根目录运行此脚本")
    sys.exit(1)


async def scan_only():
    """仅扫描重复数据"""
    print("🔍 扫描重复数据...")
    
    tool = ClickHouseDedupPro()
    try:
        if not await tool.initialize():
            print("❌ 数据库连接失败")
            return False
        
        report = await tool.scan_duplicates()
        tool.print_report()
        
        summary = report.get_summary()
        if summary['total_duplicate_groups'] > 0:
            print(f"\n💡 发现 {summary['total_duplicate_groups']} 组重复数据")
            print("   使用 'python tools/simple_dedup.py --dry-run' 预览删除")
            print("   使用 'python tools/simple_dedup.py --auto' 执行去重")
        else:
            print("\n✅ 数据库状态良好，未发现重复数据！")
        
        return True
        
    finally:
        await tool.cleanup()


async def dry_run():
    """预览删除操作"""
    print("👀 预览删除操作...")
    
    tool = ClickHouseDedupPro()
    try:
        if not await tool.initialize():
            print("❌ 数据库连接失败")
            return False
        
        # 扫描重复数据
        await tool.scan_duplicates()
        
        summary = tool.report.get_summary()
        if summary['total_duplicate_groups'] == 0:
            print("✅ 未发现重复数据")
            return True
        
        # 预览删除
        await tool.preview_deletion()
        
        print(f"\n💡 预览完成:")
        print(f"   🔍 发现 {summary['total_duplicate_groups']} 组重复数据")
        print(f"   🗑️  将删除 {summary['total_records_to_delete']} 条记录")
        print("   使用 'python tools/simple_dedup.py --auto' 执行实际删除")
        
        return True
        
    finally:
        await tool.cleanup()


async def auto_dedup():
    """自动执行去重"""
    print("🤖 自动去重模式...")
    
    tool = ClickHouseDedupPro()
    try:
        if not await tool.initialize():
            print("❌ 数据库连接失败")
            return False
        
        # 扫描重复数据
        print("🔍 扫描重复数据...")
        await tool.scan_duplicates()
        
        summary = tool.report.get_summary()
        if summary['total_duplicate_groups'] == 0:
            print("✅ 未发现重复数据，无需去重")
            return True
        
        print(f"📊 发现 {summary['total_duplicate_groups']} 组重复数据")
        print(f"🗑️  准备删除 {summary['total_records_to_delete']} 条记录")
        
        # 执行去重
        print("🛡️  创建备份并执行删除...")
        report = await tool.execute_deduplication(OperationMode.SAFE_DELETE)
        
        tool.print_report()
        
        final_summary = report.get_summary()
        if final_summary['success_rate'] == '100.0%':
            print(f"\n🎉 去重成功完成!")
            print(f"   ✅ 删除了 {final_summary['total_records_deleted']} 条重复记录")
            if report.backup_file:
                print(f"   💾 备份文件: {report.backup_file}")
        else:
            print(f"\n⚠️  去重部分完成，成功率: {final_summary['success_rate']}")
            print("   请检查日志了解详情")
        
        return True
        
    finally:
        await tool.cleanup()


async def interactive_mode():
    """交互式模式"""
    print("🎯 交互式去重工具")
    print("=" * 40)
    
    while True:
        print("\n请选择操作:")
        print("1. 🔍 扫描重复数据")
        print("2. 👀 预览删除操作")
        print("3. 🗑️  执行去重删除")
        print("4. 📊 查看使用统计")
        print("0. 🚪 退出")
        
        choice = input("\n请输入选择 (0-4): ").strip()
        
        if choice == "1":
            await scan_only()
        
        elif choice == "2":
            await dry_run()
        
        elif choice == "3":
            print("\n⚠️  警告: 此操作将删除重复数据")
            print("   - 会自动创建备份文件")
            print("   - 保留时间戳最新的记录")
            
            confirm = input("\n确认继续？请输入 'YES' 确认: ").strip()
            if confirm == "YES":
                await auto_dedup()
            else:
                print("❌ 操作已取消")
        
        elif choice == "4":
            await show_statistics()
        
        elif choice == "0":
            print("\n👋 再见！")
            break
        
        else:
            print("❌ 无效选择，请重试")


async def show_statistics():
    """显示数据库统计信息"""
    print("📊 数据库统计信息...")
    
    tool = ClickHouseDedupPro()
    try:
        if not await tool.initialize():
            print("❌ 数据库连接失败")
            return
        
        # 获取基本统计
        chunks_count_query = "SELECT count() FROM hkex_analysis.pdf_chunks"
        docs_count_query = "SELECT count() FROM hkex_analysis.pdf_documents"
        
        chunks_result = await tool.storage._execute_query(chunks_count_query)
        docs_result = await tool.storage._execute_query(docs_count_query)
        
        chunks_count = int(chunks_result[0][0]) if chunks_result else 0
        docs_count = int(docs_result[0][0]) if docs_result else 0
        
        print(f"\n📈 数据库概况:")
        print(f"  📄 PDF文档总数: {docs_count:,}")
        print(f"  📝 文档块总数: {chunks_count:,}")
        print(f"  📊 平均每文档块数: {chunks_count//max(docs_count,1):.1f}")
        
        # 快速检查是否有重复
        chunks_dup_query = """
        SELECT count() FROM (
            SELECT doc_id, chunk_id, count() as cnt
            FROM hkex_analysis.pdf_chunks 
            GROUP BY doc_id, chunk_id 
            HAVING cnt > 1
        )
        """
        
        docs_dup_query = """
        SELECT count() FROM (
            SELECT doc_id, count() as cnt
            FROM hkex_analysis.pdf_documents 
            GROUP BY doc_id 
            HAVING cnt > 1
        )
        """
        
        chunks_dup_result = await tool.storage._execute_query(chunks_dup_query)
        docs_dup_result = await tool.storage._execute_query(docs_dup_query)
        
        chunks_dup = int(chunks_dup_result[0][0]) if chunks_dup_result else 0
        docs_dup = int(docs_dup_result[0][0]) if docs_dup_result else 0
        
        print(f"\n🔍 重复数据概况:")
        print(f"  📄 重复文档组数: {docs_dup}")
        print(f"  📝 重复文档块组数: {chunks_dup}")
        
        if chunks_dup > 0 or docs_dup > 0:
            print(f"  ⚠️  建议运行去重操作")
        else:
            print(f"  ✅ 数据状态良好")
        
    finally:
        await tool.cleanup()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="ClickHouse PDF数据简易去重脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python tools/simple_dedup.py                    # 交互式模式
  python tools/simple_dedup.py --scan-only        # 仅扫描重复数据
  python tools/simple_dedup.py --dry-run          # 预览删除操作
  python tools/simple_dedup.py --auto             # 自动执行去重

注意事项:
  - 默认保留时间戳最新的记录
  - 自动模式会创建备份文件
  - 建议先使用 --dry-run 预览操作
        """
    )
    
    parser.add_argument('--scan-only', action='store_true',
                       help='仅扫描重复数据，不执行删除')
    
    parser.add_argument('--dry-run', action='store_true',
                       help='预览删除操作，不执行实际删除')
    
    parser.add_argument('--auto', action='store_true',
                       help='自动执行去重操作')
    
    args = parser.parse_args()
    
    # 显示工具信息
    print("🧹 ClickHouse PDF数据简易去重脚本 v1.0.0")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    try:
        if args.scan_only:
            result = asyncio.run(scan_only())
        elif args.dry_run:
            result = asyncio.run(dry_run())
        elif args.auto:
            result = asyncio.run(auto_dedup())
        else:
            # 默认交互式模式
            result = asyncio.run(interactive_mode())
            result = True  # 交互式模式总是返回成功
        
        sys.exit(0 if result else 1)
        
    except KeyboardInterrupt:
        print("\n❌ 操作被用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 发生意外错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
