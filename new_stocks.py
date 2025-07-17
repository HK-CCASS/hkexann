import argparse
import time
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import yaml

# 导入主项目的下载器类
from main import HKEXDownloader, ConfigManager

def read_csv():
    """
    读取csv文件 获取股票代码及名称
    :return: dict - 股票代码到名称的映射
    """
    new_stocks = {}
    stocks_dir = "./stocks_list"

    # 检查目录是否存在
    if not os.path.exists(stocks_dir):
        print(f"错误：目录 {stocks_dir} 不存在")
        return new_stocks

    for i in range(1, 4):
        csv_path = f"{stocks_dir}/s{i}.csv"

        # 检查文件是否存在
        if not os.path.exists(csv_path):
            print(f"警告：文件 {csv_path} 不存在，跳过")
            continue

        try:
            # 使用 utf-8-sig 编码处理 BOM 字符
            df = pd.read_csv(csv_path, encoding='utf-8-sig')

            # 检查必要的列是否存在
            if '代號' not in df.columns or '名稱' not in df.columns:
                print(f"警告：文件 {csv_path} 缺少必要的列（代號、名稱），跳过")
                continue

            # 只需要  代號 名稱
            df = df[['代號', '名稱']]

            for index, row in df.iterrows():
                # 跳过空值行
                if pd.isna(row['代號']) or pd.isna(row['名稱']):
                    continue

                # 代号 去除.hk
                stock_code = str(row['代號']).replace('.hk', '')
                stock_name = str(row['名稱'])

                new_stocks[stock_code] = stock_name

        except Exception as e:
            print(f"错误：读取文件 {csv_path} 时发生异常：{e}")
            continue

    print(f"股票数量为： {len(new_stocks)}")
    return new_stocks


def load_config_manager() -> ConfigManager:
    """加载配置管理器"""
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        print(f"错误：配置文件 {config_path} 不存在")
        print("请复制 config_template.yaml 为 config.yaml 并修改相应设置")
        sys.exit(1)

    try:
        config_manager = ConfigManager(config_path)
        return config_manager
    except Exception as e:
        print(f"错误：加载配置管理器失败：{e}")
        sys.exit(1)


def download_stock_announcements(downloader: HKEXDownloader, stock_code: str, stock_name: str,
                                start_date: str, end_date: str, keywords: List[str] = None) -> tuple[bool, int, str]:
    """下载单个股票的公告"""
    try:
        print(f"\n📈 开始下载股票：{stock_code} ({stock_name})")
        print(f"📅 日期范围：{start_date} ~ {end_date}")

        if keywords:
            print(f"🔍 关键字筛选：{', '.join(keywords)}")
        else:
            print("📄 下载所有公告")

        print("🔄 正在搜索公告...")

        # 创建任务字典
        task = {
            'name': f"{stock_name}公告下载",
            'stock_code': stock_code,
            'start_date': start_date,
            'end_date': end_date,
            'keywords': keywords or [],
            'enabled': True
        }

        # 调用下载器的下载方法
        result = downloader.download_announcements(task)

        # 检查结果
        if isinstance(result, tuple) and len(result) == 2:
            status, count = result
            if "成功" in status or count > 0:
                if count > 0:
                    print(f"📥 找到 {count} 个公告，正在下载...")
                    print(f"✅ {stock_code} ({stock_name}) 下载完成，共下载 {count} 个公告")
                    return True, count, f"成功下载 {count} 个公告"
                else:
                    print(f"📭 未找到符合条件的公告")
                    print(f"✅ {stock_code} ({stock_name}) 搜索完成，无新公告需要下载")
                    return True, 0, "无新公告需要下载"
            else:
                print(f"❌ {stock_code} ({stock_name}) 下载失败：{status}")
                return False, 0, status
        else:
            print(f"✅ {stock_code} ({stock_name}) 下载完成")
            return True, 0, "下载完成"

    except Exception as e:
        error_msg = str(e)
        print(f"❌ 下载股票 {stock_code} 时发生异常：{error_msg}")
        return False, 0, f"异常：{error_msg}"


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从CSV文件批量下载港交所公告')
    parser.add_argument('-s', '--stock', help='单个股票代码（可选，不指定则下载所有CSV中的股票）')
    parser.add_argument('--start', default='2020-01-01', help='开始日期 (格式: YYYY-MM-DD)')
    parser.add_argument('--end', default='2020-12-31', help='结束日期 (格式: YYYY-MM-DD 或 today)')
    parser.add_argument('--keywords', nargs='*', help='关键字筛选（可选）')
    parser.add_argument('--batch-size', type=int, default=10, help='批量处理大小（默认10）')
    parser.add_argument('--delay', type=float, default=2.0, help='股票间延迟时间（秒，默认2.0）')
    parser.add_argument('--dry-run', action='store_true', help='试运行模式，只显示将要下载的股票')
    args = parser.parse_args()

    # 加载配置管理器
    config_manager = load_config_manager()

    # 读取股票数据
    print("📊 正在读取CSV文件中的股票数据...")
    new_stocks = read_csv()

    new_stocks_list = []
    for code, name in new_stocks.items():
        new_stocks_list.append(code)
    print(f"股票数量为： {len(new_stocks_list)}")
    print(new_stocks_list)

    # # 如果没有读取到任何股票数据，退出程序
    # if not new_stocks:
    #     print("❌ 错误：未能读取到任何股票数据")
    #     sys.exit(1)
    #
    # # 确定要下载的股票列表
    # if args.stock:
    #     # 单个股票模式
    #     stock_code = args.stock.strip()
    #
    #     # 如果输入的股票代码包含.hk，自动去除
    #     if stock_code.endswith('.hk'):
    #         stock_code = stock_code.replace('.hk', '')
    #         print(f"🔧 自动去除.hk后缀，使用股票代码：{stock_code}")
    #
    #     if stock_code not in new_stocks:
    #         print(f"❌ 未找到股票代码：{stock_code}")
    #         print("💡 提示：请检查股票代码是否正确，或者该股票是否在CSV文件中")
    #         sys.exit(1)
    #
    #     stocks_to_download = {stock_code: new_stocks[stock_code]}
    # else:
    #     # 批量模式 - 下载所有股票
    #     stocks_to_download = new_stocks
    #
    # print(f"\n📋 准备下载 {len(stocks_to_download)} 只股票的公告")
    # print(f"📅 日期范围：{args.start} ~ {args.end}")
    #
    # if args.keywords:
    #     print(f"🔍 关键字筛选：{', '.join(args.keywords)}")
    #
    # if args.dry_run:
    #     print("\n🔍 试运行模式 - 将要下载的股票列表：")
    #     for i, (code, name) in enumerate(stocks_to_download.items(), 1):
    #         print(f"  {i:3d}. {code} - {name}")
    #     print(f"\n总计：{len(stocks_to_download)} 只股票")
    #     return
    #
    # # 初始化下载器
    # try:
    #     downloader = HKEXDownloader(config_manager)
    #     print("✅ 下载器初始化成功")
    # except Exception as e:
    #     print(f"❌ 下载器初始化失败：{e}")
    #     sys.exit(1)
    #
    # # 开始批量下载
    # print(f"\n🚀 开始批量下载...")
    # start_time = datetime.now()
    #
    # success_count = 0
    # failed_count = 0
    # total_announcements = 0
    # failed_stocks = []
    # download_details = []
    #
    # for i, (stock_code, stock_name) in enumerate(stocks_to_download.items(), 1):
    #     print(f"\n{'='*60}")
    #     print(f"📊 进度：{i}/{len(stocks_to_download)} ({i/len(stocks_to_download)*100:.1f}%)")
    #
    #     success, count, message = download_stock_announcements(
    #         downloader=downloader,
    #         stock_code=stock_code,
    #         stock_name=stock_name,
    #         start_date=args.start,
    #         end_date=args.end,
    #         keywords=args.keywords
    #     )
    #
    #     # 记录详细信息
    #     download_details.append({
    #         'stock_code': stock_code,
    #         'stock_name': stock_name,
    #         'success': success,
    #         'count': count,
    #         'message': message
    #     })
    #
    #     if success:
    #         success_count += 1
    #         total_announcements += count
    #     else:
    #         failed_count += 1
    #         failed_stocks.append(f"{stock_code} ({stock_name}) - {message}")
    #
    #     # 显示当前累计统计
    #     print(f"📈 当前统计：成功 {success_count} | 失败 {failed_count} | 累计下载 {total_announcements} 个公告")
    #
    #     # 添加延迟（除了最后一个）
    #     if i < len(stocks_to_download):
    #         print(f"⏱️  等待 {args.delay} 秒...")
    #         time.sleep(args.delay)
    #
    # # 显示最终统计
    # end_time = datetime.now()
    # duration = end_time - start_time
    #
    # print(f"\n{'='*80}")
    # print("📊 批量下载完成 - 详细统计报告")
    # print(f"{'='*80}")
    # print(f"✅ 成功股票：{success_count} 只")
    # print(f"❌ 失败股票：{failed_count} 只")
    # print(f"📥 总下载公告：{total_announcements} 个")
    # print(f"⏱️  总耗时：{duration}")
    # print(f"📈 平均速度：{len(stocks_to_download)/duration.total_seconds():.2f} 股票/秒")
    #
    # # 显示成功下载的股票详情
    # successful_downloads = [d for d in download_details if d['success'] and d['count'] > 0]
    # if successful_downloads:
    #     print(f"\n📥 成功下载公告的股票 ({len(successful_downloads)} 只)：")
    #     for detail in successful_downloads:
    #         print(f"  ✅ {detail['stock_code']} ({detail['stock_name']}) - {detail['count']} 个公告")
    #
    # # 显示无新公告的股票
    # no_new_announcements = [d for d in download_details if d['success'] and d['count'] == 0]
    # if no_new_announcements:
    #     print(f"\n📭 无新公告的股票 ({len(no_new_announcements)} 只)：")
    #     for detail in no_new_announcements[:10]:  # 只显示前10个
    #         print(f"  📭 {detail['stock_code']} ({detail['stock_name']}) - {detail['message']}")
    #     if len(no_new_announcements) > 10:
    #         print(f"  ... 还有 {len(no_new_announcements) - 10} 只股票无新公告")
    #
    # # 显示失败的股票
    # if failed_stocks:
    #     print(f"\n❌ 失败的股票列表 ({failed_count} 只)：")
    #     for stock in failed_stocks:
    #         print(f"  ❌ {stock}")
    #
    # # 显示下载分布统计
    # if total_announcements > 0:
    #     print(f"\n📊 公告下载分布：")
    #     announcement_counts = {}
    #     for detail in download_details:
    #         if detail['success'] and detail['count'] > 0:
    #             count = detail['count']
    #             announcement_counts[count] = announcement_counts.get(count, 0) + 1
    #
    #     for count in sorted(announcement_counts.keys(), reverse=True):
    #         stock_num = announcement_counts[count]
    #         print(f"  📄 {count} 个公告：{stock_num} 只股票")
    #
    # print(f"\n🎉 批量下载任务完成！")
    # print(f"💾 公告文件保存位置：{downloader.config.get('settings', 'save_path')}")


if __name__ == '__main__':
    main()











