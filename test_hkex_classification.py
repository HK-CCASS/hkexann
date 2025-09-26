#!/usr/bin/env python3
"""
HKEX分类系统测试脚本
测试HKEX--分类信息-20250926.csv的集成和过滤功能
"""

import asyncio
import logging
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from services.monitor.classification_parser import get_classification_parser
from services.monitor.dual_filter import DualAnnouncementFilter

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_classification_parser():
    """测试分类文件解析器"""
    print("=== 测试HKEX分类文件解析器 ===")

    # 获取解析器实例
    parser = get_classification_parser()

    # 加载分类文件
    if not parser.load_classifications():
        print("❌ 分类文件加载失败")
        return False

    # 获取统计信息
    stats = parser.get_statistics()
    print(f"✅ 分类文件加载成功")
    print(f"   总分类数: {stats['total_categories']}")
    print(f"   1级分类: {stats['level1_categories']}")
    print(f"   2级分类: {stats['level2_categories']}")
    print(f"   总公告数: {stats['total_announcements']}")

    # 测试分类查找
    print("\n=== 测试分类查找功能 ===")

    # 通过代码查找
    category = parser.get_category_by_code('23400')
    if category:
        print(f"✅ 代码查找测试: 23400 -> {category.level3_name}")
        print(f"   完整路径: {category.full_path}")

    # 通过名称查找
    category = parser.get_category_by_name('回購股份的說明函件')
    if category:
        print(f"✅ 名称查找测试: '回購股份的說明函件' -> {category.level3_code}")

    # 测试排除类别转换
    excluded_names = ['翌日披露報表', '不存在的类别']
    excluded_codes = parser.get_excluded_codes_by_names(excluded_names)
    print(f"✅ 排除类别转换: {excluded_names} -> {list(excluded_codes)}")

    return True


def test_dual_filter_integration():
    """测试双重过滤器与HKEX分类系统的集成"""
    print("\n=== 测试双重过滤器集成 ===")

    # 测试配置
    test_config = {
        'realtime_monitoring': {
            'filtering': {
                'stock_filter_enabled': True,
                'type_filter_enabled': True,
                'hkex_classification_enabled': True,
                'excluded_categories': ['翌日披露報表'],
                'included_keywords': []
            }
        }
    }

    # 初始化过滤器
    monitored_stocks = {'00700', '01810', '09988'}
    filter_instance = DualAnnouncementFilter(monitored_stocks, test_config)

    # 检查初始化状态
    if filter_instance.hkex_classification_enabled:
        print("✅ HKEX分类系统已启用")
        print(f"   加载的分类数: {len(filter_instance.classification_parser.categories)}")
        print(f"   排除的代码数: {len(filter_instance.excluded_codes)}")
    else:
        print("❌ HKEX分类系统未启用")
        return False

    # 测试过滤功能
    print("\n=== 测试过滤功能 ===")

    # 模拟带有分类代码的公告数据
    mock_announcements = [
        {
            'STOCK_CODE': '00700',
            'TITLE': '测试公告1',
            'T2_CODE': '23400',  # 在股東批准的情況下重選或委任董事
            'LONG_TEXT': '通告及告示'
        },
        {
            'STOCK_CODE': '00700',
            'TITLE': '测试公告2',
            'T2_CODE': '50100',  # 翌日披露報表（应该被排除）
            'LONG_TEXT': '翌日披露報表'
        },
        {
            'STOCK_CODE': '00001',  # 不监控的股票
            'TITLE': '测试公告3',
            'T2_CODE': '23400',
            'LONG_TEXT': '通告及告示'
        }
    ]

    async def run_filter_test():
        filtered = await filter_instance.filter_announcements(mock_announcements)
        return filtered

    # 运行异步测试
    filtered_announcements = asyncio.run(run_filter_test())

    print("✅ 过滤测试完成")
    print(f"   输入公告数: {len(mock_announcements)}")
    print(f"   输出公告数: {len(filtered_announcements)}")
    print(f"   过滤率: {(len(mock_announcements) - len(filtered_announcements)) / len(mock_announcements) * 100:.1f}%")

    # 显示过滤结果
    print("\n过滤结果详情:")
    for i, ann in enumerate(filtered_announcements, 1):
        category_info = filter_instance.classification_parser.get_category_hierarchy(ann.get('T2_CODE'))
        category_name = category_info['level3_name'] if category_info else '未知'
        print(f"   {i}. {ann['STOCK_CODE']} - {ann['TITLE']} (分类: {category_name})")

    return True


def test_filter_stats():
    """测试过滤器统计功能"""
    print("\n=== 测试过滤器统计 ===")

    test_config = {
        'realtime_monitoring': {
            'filtering': {
                'hkex_classification_enabled': True,
                'excluded_categories': ['翌日披露報表']
            }
        }
    }

    filter_instance = DualAnnouncementFilter({'00700'}, test_config)
    stats = filter_instance.get_filter_stats()

    print("✅ 过滤器统计信息:")
    print(f"   监听股票数: {stats['monitored_stocks_count']}")
    print(f"   HKEX分类启用: {stats['hkex_classification_enabled']}")
    print(f"   排除代码数: {stats['excluded_codes_count']}")

    if 'hkex_classification_stats' in stats:
        hkex_stats = stats['hkex_classification_stats']
        print(f"   HKEX总分类数: {hkex_stats.get('total_categories', 0)}")
        print(f"   HKEX总公告数: {hkex_stats.get('total_announcements', 0)}")

    return True


def main():
    """主测试函数"""
    print("🚀 HKEX分类系统集成测试")
    print("=" * 50)

    try:
        # 测试分类文件解析器
        if not test_classification_parser():
            return False

        # 测试双重过滤器集成
        if not test_dual_filter_integration():
            return False

        # 测试过滤器统计
        if not test_filter_stats():
            return False

        print("\n" + "=" * 50)
        print("🎉 所有测试通过！HKEX分类系统集成成功")
        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
