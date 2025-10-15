import yaml
from services.monitor.hkex_official_filter import HKEXOfficialFilter
import asyncio

async def test_exclusion():
    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 创建过滤器
    filter = HKEXOfficialFilter(config)
    await filter.initialize()

    # 测试需要排除的公告
    test_announcements = [
        {'TITLE': '截至2025年9月30日止股份發行人的證券變動月報表'},  # 应该被排除
        {'TITLE': '2025中期報告'},  # 应该被排除
        {'TITLE': '股東週年大會通告'},  # 应该被排除
        {'TITLE': '建議股本重組及供股'},  # 应该通过 (合股)
        {'TITLE': '暫停買賣'},  # 应该通过 (停牌)
    ]

    print('测试排除功能:')
    print('=' * 50)

    filtered = await filter.filter_announcements(test_announcements)

    print(f'输入公告数量: {len(test_announcements)}')
    print(f'过滤后数量: {len(filtered)}')
    print()

    print('通过的公告:')
    for ann in filtered:
        print(f'  - {ann["TITLE"]}')

if __name__ == "__main__":
    asyncio.run(test_exclusion())
