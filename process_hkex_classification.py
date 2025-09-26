#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
港交所公告分类规则处理脚本
从港交所JSON API获取分类数据并转换为CSV格式
"""

import json
import csv
import requests
from datetime import datetime
import pandas as pd

def fetch_json_data(url):
    """获取JSON数据"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取数据失败: {url}, 错误: {e}")
        return None

def main():
    # 定义数据源URL
    urls = {
        'doc_types': 'https://www.hkexnews.hk/ncms/script/eds/doc_c.json?_=1758528805177',
        'tier_one': 'https://www.hkexnews.hk/ncms/script/eds/tierone_c.json?_=1758528805174',
        'tier_two': 'https://www.hkexnews.hk/ncms/script/eds/tiertwo_c.json?_=1758528805175',
        'tier_two_groups': 'https://www.hkexnews.hk/ncms/script/eds/tiertwogrp_c.json?_=1758528805176'
    }

    # 获取所有数据
    data = {}
    for key, url in urls.items():
        print(f"正在获取 {key} 数据...")
        data[key] = fetch_json_data(url)
        if data[key] is None:
            print(f"无法获取 {key} 数据，脚本终止")
            return

    # 生成时间戳
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 处理文档类型 (doc_c.json)
    doc_types_df = pd.DataFrame(data['doc_types'])
    doc_types_df.columns = ['文档类型代码', '文档类型名称']
    doc_types_df.to_csv(f'hkex_doc_types_{timestamp}.csv', index=False, encoding='utf-8-sig')
    print(f"文档类型数据已保存: hkex_doc_types_{timestamp}.csv ({len(doc_types_df)} 条记录)")

    # 处理一级分类 (tierone_c.json)
    tier_one_df = pd.DataFrame(data['tier_one'])
    tier_one_df.columns = ['一级分类代码', '一级分类名称', '公告数量']
    tier_one_df.to_csv(f'hkex_tier_one_{timestamp}.csv', index=False, encoding='utf-8-sig')
    print(f"一级分类数据已保存: hkex_tier_one_{timestamp}.csv ({len(tier_one_df)} 条记录)")

    # 处理二级分组 (tiertwogrp_c.json)
    tier_two_groups_df = pd.DataFrame(data['tier_two_groups'])
    tier_two_groups_df.columns = ['二级分组代码', '二级分组名称', '所属一级分类代码']
    tier_two_groups_df.to_csv(f'hkex_tier_two_groups_{timestamp}.csv', index=False, encoding='utf-8-sig')
    print(f"二级分组数据已保存: hkex_tier_two_groups_{timestamp}.csv ({len(tier_two_groups_df)} 条记录)")

    # 处理二级分类 (tiertwo_c.json) - 这个数据较复杂
    tier_two_df = pd.DataFrame(data['tier_two'])
    tier_two_df.columns = ['二级分组代码', '公告数量', '二级分类代码', '二级分类名称', '所属一级分类代码']
    # 重新排列列顺序
    tier_two_df = tier_two_df[['所属一级分类代码', '二级分组代码', '二级分类代码', '二级分类名称', '公告数量']]
    tier_two_df.to_csv(f'hkex_tier_two_{timestamp}.csv', index=False, encoding='utf-8-sig')
    print(f"二级分类数据已保存: hkex_tier_two_{timestamp}.csv ({len(tier_two_df)} 条记录)")

    # 创建完整的分类层次结构CSV
    # 合并所有相关数据
    print("\n正在创建完整分类层次结构...")

    # 创建一级分类映射
    tier_one_map = dict(zip(tier_one_df['一级分类代码'], tier_one_df['一级分类名称']))

    # 创建二级分组映射
    tier_two_groups_map = dict(zip(tier_two_groups_df['二级分组代码'], tier_two_groups_df['二级分组名称']))

    # 增强二级分类数据
    enhanced_tier_two = tier_two_df.copy()
    enhanced_tier_two['一级分类名称'] = enhanced_tier_two['所属一级分类代码'].map(tier_one_map)
    enhanced_tier_two['二级分组名称'] = enhanced_tier_two['二级分组代码'].map(tier_two_groups_map)

    # 重新排列列顺序
    enhanced_tier_two = enhanced_tier_two[['所属一级分类代码', '一级分类名称', '二级分组代码', '二级分组名称',
                                         '二级分类代码', '二级分类名称', '公告数量']]

    enhanced_tier_two.to_csv(f'hkex_complete_classification_{timestamp}.csv', index=False, encoding='utf-8-sig')
    print(f"完整分类层次结构已保存: hkex_complete_classification_{timestamp}.csv ({len(enhanced_tier_two)} 条记录)")

    # 保存原始JSON数据
    with open(f'hkex_classification_raw_data_{timestamp}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"原始JSON数据已保存: hkex_classification_raw_data_{timestamp}.json")

    # 生成统计摘要
    print(f"\n=== 港交所公告分类规则统计摘要 ===")
    print(f"获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"文档类型数量: {len(doc_types_df)}")
    print(f"一级分类数量: {len(tier_one_df)}")
    print(f"二级分组数量: {len(tier_two_groups_df)}")
    print(f"二级分类数量: {len(tier_two_df)}")
    print(f"总公告数量: {tier_one_df['公告数量'].sum():,}")

    # 显示一级分类公告数量排行
    print(f"\n=== 一级分类公告数量排行 (前10名) ===")
    top_tier_one = tier_one_df.nlargest(10, '公告数量')
    for _, row in top_tier_one.iterrows():
        print(f"{row['一级分类名称']}: {row['公告数量']:,} 条")

if __name__ == '__main__':
    main()