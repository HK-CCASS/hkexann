#!/usr/bin/env python3
"""
ClickHouse去重工具 - 快速执行脚本

这个脚本提供了简化的去重操作接口，适合日常维护使用。
包含预定义的安全检查和最佳实践配置。

使用场景：
1. 日常数据库维护
2. 数据导入后的清理
3. 定期重复数据检查
4. 系统性能优化

作者: Claude 4.0 sonnet AI助手
版本: 1.0.0
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import json

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from tools.clickhouse_deduplication_tool import ClickHouseDeduplicationTool
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def quick_analysis():
    """快速分析所有表的重复数据情况"""
    print("🔍 HKEX数据库重复数据分析")
    print("=" * 50)
    
    async with ClickHouseDeduplicationTool() as tool:
        tables = ['pdf_documents', 'pdf_chunks']
        
        for table in tables:
            print(f"\n📊 分析表: {table}")
            print("-" * 30)
            
            try:
                analysis = await tool.analyze_duplicates(table)
                
                print(f"总记录数: {analysis['total_records']:,}")
                print(f"重复组数: {analysis['duplicate_groups']:,}")
                print(f"重复记录: {analysis['total_duplicates']:,}")
                print(f"重复率: {analysis['duplicate_rate']:.2f}%")
                
                if analysis['duplicate_rate'] > 5:
                    print("⚠️  重复率较高，建议执行去重操作")
                elif analysis['duplicate_rate'] > 0:
                    print("ℹ️  发现少量重复，可考虑清理")
                else:
                    print("✅ 数据质量良好，无重复记录")
                    
            except Exception as e:
                print(f"❌ 分析失败: {e}")


async def safe_deduplication():
    """安全去重操作 - 包含完整的备份和验证"""
    print("🛡️  HKEX数据库安全去重操作")
    print("=" * 50)
    
    # 安全确认
    print("\n⚠️  重要提醒:")
    print("1. 此操作将删除重复数据，请确保已了解影响")
    print("2. 系统将自动创建备份表")
    print("3. 建议在低峰期执行此操作")
    
    confirm = input("\n是否继续？[y/N]: ").lower().strip()
    if confirm != 'y':
        print("❌ 操作已取消")
        return
    
    async with ClickHouseDeduplicationTool() as tool:
        tables = ['pdf_documents', 'pdf_chunks']
        all_stats = []
        
        for table in tables:
            print(f"\n🚀 处理表: {table}")
            print("-" * 30)
            
            try:
                # 先分析
                analysis = await tool.analyze_duplicates(table)
                
                if analysis['total_duplicates'] == 0:
                    print(f"✅ 表 {table} 无重复数据，跳过处理")
                    continue
                
                print(f"发现 {analysis['total_duplicates']} 条重复记录")
                
                # 执行去重
                stats = await tool.deduplicate_table(
                    table_name=table,
                    create_backup=True,
                    dry_run=False
                )
                
                all_stats.append(stats)
                
                print(f"✅ 处理完成:")
                print(f"   删除重复: {stats.duplicates_removed:,} 条")
                print(f"   去重率: {stats.deduplication_rate:.2f}%")
                print(f"   备份表: {stats.backup_table_name}")
                
            except Exception as e:
                print(f"❌ 处理失败: {e}")
        
        # 生成最终报告
        if all_stats:
            report = await tool.generate_report(all_stats)
            
            # 保存报告
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f"deduplication_report_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            print(f"\n📄 详细报告已保存: {report_file}")
            
            # 显示摘要
            print(f"\n📊 最终统计:")
            print(f"   处理表数: {len(all_stats)}")
            print(f"   总删除记录: {sum(s.duplicates_removed for s in all_stats):,}")
            print(f"   总处理时间: {sum(s.processing_time for s in all_stats):.1f}s")


async def dry_run_test():
    """试运行测试 - 查看去重效果但不实际修改数据"""
    print("🏃 HKEX数据库去重试运行")
    print("=" * 50)
    print("注意：这是试运行模式，不会实际修改数据\n")
    
    async with ClickHouseDeduplicationTool() as tool:
        tables = ['pdf_documents', 'pdf_chunks']
        
        for table in tables:
            print(f"🔧 试运行表: {table}")
            print("-" * 30)
            
            try:
                stats = await tool.deduplicate_table(
                    table_name=table,
                    create_backup=False,
                    dry_run=True
                )
                
                print(f"模拟结果:")
                print(f"   当前记录: {stats.total_records_before:,}")
                print(f"   将删除: {stats.duplicates_removed:,}")
                print(f"   保留记录: {stats.total_records_after:,}")
                print(f"   预期去重率: {stats.deduplication_rate:.2f}%")
                
                if stats.duplicates_removed > 0:
                    print("💡 建议执行实际去重操作")
                else:
                    print("✅ 无需去重")
                    
            except Exception as e:
                print(f"❌ 试运行失败: {e}")
            
            print()


async def restore_from_backup():
    """从备份恢复数据"""
    print("🔄 从备份恢复数据")
    print("=" * 50)
    
    # 这里需要用户提供备份表名
    backup_table = input("请输入备份表名 (例: pdf_documents_backup_20250918_143000): ").strip()
    
    if not backup_table:
        print("❌ 未提供备份表名")
        return
    
    target_table = input("请输入目标表名 (pdf_documents 或 pdf_chunks): ").strip()
    
    if target_table not in ['pdf_documents', 'pdf_chunks']:
        print("❌ 无效的目标表名")
        return
    
    print(f"\n⚠️  即将从 {backup_table} 恢复到 {target_table}")
    print("这将覆盖当前表的所有数据！")
    
    confirm = input("确认恢复？[y/N]: ").lower().strip()
    if confirm != 'y':
        print("❌ 恢复操作已取消")
        return
    
    async with ClickHouseDeduplicationTool() as tool:
        try:
            # 验证备份表存在
            check_query = f"SELECT count() FROM {tool.database}.{backup_table}"
            result = await tool._execute_query(check_query)
            backup_count = result[0]['count()'] if result else 0
            
            print(f"📋 备份表记录数: {backup_count:,}")
            
            if backup_count == 0:
                print("❌ 备份表为空或不存在")
                return
            
            # 清空目标表
            truncate_query = f"TRUNCATE TABLE {tool.database}.{target_table}"
            await tool._execute_query(truncate_query)
            
            # 从备份恢复数据
            restore_query = f"""
            INSERT INTO {tool.database}.{target_table}
            SELECT * FROM {tool.database}.{backup_table}
            """
            await tool._execute_query(restore_query)
            
            # 验证恢复结果
            verify_query = f"SELECT count() FROM {tool.database}.{target_table}"
            verify_result = await tool._execute_query(verify_query)
            restored_count = verify_result[0]['count()'] if verify_result else 0
            
            print(f"✅ 恢复完成!")
            print(f"   恢复记录数: {restored_count:,}")
            
            if restored_count == backup_count:
                print("✅ 数据完整性验证通过")
            else:
                print("⚠️  记录数不匹配，请检查")
                
        except Exception as e:
            print(f"❌ 恢复失败: {e}")


def show_menu():
    """显示主菜单"""
    print("\n🔧 HKEX ClickHouse 去重工具")
    print("=" * 40)
    print("1. 快速分析重复数据")
    print("2. 安全去重操作 (推荐)")
    print("3. 试运行测试")
    print("4. 从备份恢复数据")
    print("5. 退出")
    print("-" * 40)


async def main():
    """主程序入口"""
    print("🐾 欢迎使用 HKEX ClickHouse 数据去重工具!")
    print("由 Claude 4.0 sonnet 专业AI助手开发\n")
    
    while True:
        show_menu()
        
        try:
            choice = input("请选择操作 [1-5]: ").strip()
            
            if choice == '1':
                await quick_analysis()
            elif choice == '2':
                await safe_deduplication()
            elif choice == '3':
                await dry_run_test()
            elif choice == '4':
                await restore_from_backup()
            elif choice == '5':
                print("👋 再见！数据质量维护愉快!")
                break
            else:
                print("❌ 无效选择，请输入 1-5")
                
        except KeyboardInterrupt:
            print("\n\n👋 操作已中断，再见!")
            break
        except Exception as e:
            print(f"❌ 操作失败: {e}")
        
        input("\n按回车键继续...")


if __name__ == "__main__":
    # Windows兼容性
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序已退出")











