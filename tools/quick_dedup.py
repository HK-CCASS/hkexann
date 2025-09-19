#!/usr/bin/env python3
"""
快速去重脚本

提供简化的命令行接口来执行数据去重操作。
适用于快速检查和清理重复数据。

使用方法:
  python tools/quick_dedup.py scan                    # 扫描重复数据
  python tools/quick_dedup.py preview                 # 预览去重操作  
  python tools/quick_dedup.py remove                  # 执行安全去重
  python tools/quick_dedup.py rollback <backup_file>  # 从备份恢复

作者: Claude 4.0 sonnet
版本: 1.0.0
"""

import asyncio
import argparse
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from tools.data_deduplication_tool import DataDeduplicationTool, DeduplicationMode
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    print("请确保从项目根目录运行此脚本")
    sys.exit(1)


async def scan_duplicates():
    """扫描重复数据"""
    print("🔍 扫描重复数据...")
    
    tool = DataDeduplicationTool()
    
    try:
        if not await tool.initialize():
            print("❌ 初始化失败")
            return False
        
        report = await tool.scan_duplicates()
        tool.print_report(report)
        
        if report.duplicate_groups_found > 0:
            print(f"\n💡 提示: 发现 {report.duplicate_groups_found} 组重复数据")
            print("   使用 'python tools/quick_dedup.py preview' 预览去重操作")
            print("   使用 'python tools/quick_dedup.py remove' 执行安全去重")
        else:
            print("\n✅ 没有发现重复数据，数据库状态良好！")
        
        return True
        
    finally:
        await tool.cleanup()


async def preview_removal():
    """预览去重操作（干运行）"""
    print("🧪 预览去重操作（干运行模式）...")
    
    tool = DataDeduplicationTool()
    
    try:
        if not await tool.initialize():
            print("❌ 初始化失败")
            return False
        
        # 先扫描
        await tool.scan_duplicates()
        
        if tool.current_report.duplicate_groups_found == 0:
            print("✅ 没有发现重复数据")
            return True
        
        # 干运行
        report = await tool.remove_duplicates(DeduplicationMode.DRY_RUN)
        tool.print_report(report)
        
        print(f"\n💡 预览完成:")
        print(f"   将删除 {report.records_to_remove} 条重复记录")
        print("   使用 'python tools/quick_dedup.py remove' 执行实际删除")
        
        return True
        
    finally:
        await tool.cleanup()


async def remove_duplicates():
    """执行安全去重"""
    print("🗑️ 执行安全去重...")
    
    # 确认操作
    print("\n⚠️  警告: 此操作将删除重复数据")
    print("   - 会自动创建备份文件")
    print("   - 保留时间戳最新的记录")
    print("   - 删除其他重复项")
    
    confirm = input("\n确认继续？请输入 'YES' 确认: ").strip()
    if confirm != "YES":
        print("❌ 操作已取消")
        return False
    
    tool = DataDeduplicationTool()
    
    try:
        if not await tool.initialize():
            print("❌ 初始化失败")
            return False
        
        # 先扫描
        await tool.scan_duplicates()
        
        if tool.current_report.duplicate_groups_found == 0:
            print("✅ 没有发现重复数据")
            return True
        
        # 执行安全删除
        report = await tool.remove_duplicates(DeduplicationMode.SAFE_REMOVE)
        tool.print_report(report)
        
        if report.errors:
            print(f"\n❌ 删除过程中发生错误，请检查日志")
            return False
        else:
            print(f"\n✅ 去重完成!")
            print(f"   删除了 {report.records_actually_removed} 条重复记录")
            if report.backup_file_path:
                print(f"   备份文件: {report.backup_file_path}")
                print(f"   如需恢复，使用: python tools/quick_dedup.py rollback {report.backup_file_path}")
        
        return True
        
    finally:
        await tool.cleanup()


async def rollback_data(backup_file: str):
    """从备份恢复数据"""
    print(f"🔄 从备份恢复数据: {backup_file}")
    
    if not Path(backup_file).exists():
        print(f"❌ 备份文件不存在: {backup_file}")
        return False
    
    # 确认操作
    confirm = input("\n⚠️  确认从备份恢复数据？这将重新插入之前删除的记录 (yes/no): ").strip().lower()
    if confirm != "yes":
        print("❌ 操作已取消")
        return False
    
    tool = DataDeduplicationTool()
    
    try:
        if not await tool.initialize():
            print("❌ 初始化失败")
            return False
        
        success = await tool.rollback_from_backup(backup_file)
        
        if success:
            print("✅ 数据恢复成功!")
        else:
            print("❌ 数据恢复失败，请检查日志")
        
        return success
        
    finally:
        await tool.cleanup()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="HKEX 数据去重工具 - 快速命令行接口",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python tools/quick_dedup.py scan                    # 扫描重复数据
  python tools/quick_dedup.py preview                 # 预览去重操作
  python tools/quick_dedup.py remove                  # 执行安全去重
  python tools/quick_dedup.py rollback backup.json   # 从备份恢复

注意事项:
  - 所有操作都会自动连接到配置的数据库
  - remove 操作会自动创建备份文件
  - 建议先使用 scan 和 preview 了解数据状况
        """
    )
    
    parser.add_argument(
        "action",
        choices=["scan", "preview", "remove", "rollback"],
        help="要执行的操作"
    )
    
    parser.add_argument(
        "backup_file",
        nargs="?",
        help="备份文件路径（仅用于 rollback 操作）"
    )
    
    args = parser.parse_args()
    
    # 验证参数
    if args.action == "rollback" and not args.backup_file:
        print("❌ rollback 操作需要指定备份文件路径")
        parser.print_help()
        sys.exit(1)
    
    # 显示工具信息
    print("🧹 HKEX 数据去重工具 v1.0.0")
    print("=" * 40)
    
    try:
        # 执行对应操作
        if args.action == "scan":
            result = asyncio.run(scan_duplicates())
        elif args.action == "preview":
            result = asyncio.run(preview_removal())
        elif args.action == "remove":
            result = asyncio.run(remove_duplicates())
        elif args.action == "rollback":
            result = asyncio.run(rollback_data(args.backup_file))
        
        # 退出码
        sys.exit(0 if result else 1)
        
    except KeyboardInterrupt:
        print("\n\n❌ 操作被用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 发生意外错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
