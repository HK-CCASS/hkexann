#!/usr/bin/env python3
"""
HKEX增强公告监听系统启动脚本
基于ClickHouse的新股监听与公告自动处理系统
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any
import argparse
import yaml
from services.monitor.enhanced_announcement_processor import EnhancedAnnouncementProcessor
# 设置日志格式 - 强制配置所有handlers
import logging
import sys

# 清除现有配置
root_logger = logging.getLogger()
root_logger.handlers.clear()

# 强制重新配置 - 修复Windows编码问题
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('enhanced_monitor.log', encoding='utf-8')
    ],
    force=True  # Python 3.8+ 支持，强制重新配置
)

# 设置控制台输出编码（Windows兼容性）
if sys.platform.startswith('win'):
    import os
    os.environ['PYTHONIOENCODING'] = 'utf-8'

logger = logging.getLogger(__name__)


def substitute_env_vars(obj):
    """递归替换配置中的环境变量引用"""
    if isinstance(obj, str):
        # 处理 ${VAR_NAME} 格式的环境变量引用
        import re
        def replace_var(match):
            var_name = match.group(1)
            # 对于DB_PASSWORD，如果环境变量不存在，使用Docker默认密码123456
            if var_name == 'DB_PASSWORD':
                return os.environ.get(var_name, '123456')
            return os.environ.get(var_name, match.group(0))  # 其他变量如果不存在，保持原样
        return re.sub(r'\$\{([^}]+)\}', replace_var, obj)
    elif isinstance(obj, dict):
        return {k: substitute_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [substitute_env_vars(item) for item in obj]
    else:
        return obj


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """加载配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 处理环境变量替换
        config = substitute_env_vars(config)

        # 扩展配置以支持增强监听系统
        enhanced_config = {  # 使用原项目的API端点配置
            'api_endpoints': config.get('api_endpoints',
                                        {'base_url': 'https://www1.hkexnews.hk', 'stock_search': '/search/prefix.do',
                                         'title_search': '/search/titleSearchServlet.do',
                                         'referer': 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh',
                                         'callback_param': 'callback', 'market': 'SEHK'}),

            # API监听配置
            'api_monitor': {'poll_interval': 60,  # 5分钟检查一次
                            'timeout': 30, 'max_retries': 3},

            # 双重过滤配置 - 优先使用 dual_filter 配置节
            'realtime_monitoring': {'filtering': 
                config.get('dual_filter', {
                    'stock_filter_enabled': True, 
                    'type_filter_enabled': True,
                    'excluded_categories': [],
                    'included_keywords': []
                })
            },

            # ClickHouse股票发现配置（从config.yaml的stock_discovery段获取，支持环境变量）
            'stock_discovery': config.get('stock_discovery', {
                'enabled': True,
                'host': 'localhost',
                'port': 8124,
                'database': 'hkex_analysis', 
                'user': 'root',
                'password': os.environ.get('DB_PASSWORD', '123456')  # 从环境变量或使用Docker默认密码
            }),

            # 下载器集成配置
            'downloader_integration': {'use_existing_downloader': True,
                                       'download_directory': config.get('settings', {}).get('save_path', './hkexann'),
                                       'enable_filtering': True,
                                       'timeout': config.get('advanced', {}).get('timeout', 30),
                                       # 🚀 新增：智能分类配置传递 - 统一启用智能分类
                                       'enable_smart_classification': True,  # 启用智能分类，与main.py保持一致
                                       'common_keywords': config.get('common_keywords', {}),
                                       'announcement_categories': config.get('announcement_categories', {})},
            'max_concurrent': config.get('async', {}).get('max_concurrent', 5),
            'requests_per_second': config.get('async', {}).get('requests_per_second', 5),

            # 向量化集成配置
            'vectorization_integration': {'use_existing_pipeline': True,
                                          'pdf_directory': config.get('settings', {}).get('save_path', './hkexann'),
                                          'collection_name': 'pdf_embeddings_v3', 'batch_size': 15,
                                          'max_concurrent': 5},

            # 调度配置
            'scheduler': {'stock_sync_interval_hours': 0.1,  # 半小时同步一次股票列表
                          'api_poll_interval_seconds': 120,  # 1分钟检查一次API
                          'max_concurrent_processing': 5, 'enable_auto_stock_sync': True,
                          'enable_continuous_monitoring': True},

            # 历史数据处理配置
            'historical_processing': {'historical_days': 365,  # 常规模式历史天数
                                      'first_run_historical_days':45,  # 首次运行历史天数，快速启动
                                      'stock_batch_size': 10, 'date_chunk_days': 30, 'max_concurrent_historical': 3,
                                      'api_delay': 2.0},

            # 错误处理配置
            'error_handling': {'max_consecutive_errors': 10, 'error_backoff_seconds': 60,
                               'enable_error_recovery': True},

            # 🆕 新增股票历史处理配置
            'new_stock_historical_processing': config.get('new_stock_historical_processing', {
                'enabled': True,
                'days': 3,
                'batch_size': 5,
                'max_concurrent': 2,
                'timeout_minutes': 10,
                'max_retries': 2,
                'enable_filtering': True,
                'auto_start': True,
                # 🔧 修复：添加强制历史补充选项
                'force_first_run_historical': True,  # 强制为首次发现的所有股票补充历史
                'force_all_stocks_historical': False  # 强制为所有股票补充历史（调试用）
            })}

        # 合并原始配置
        enhanced_config.update(config)

        logger.info("配置文件加载成功")
        return enhanced_config

    except Exception as e:
        logger.error(f"配置文件加载失败: {e}")
        sys.exit(1)


async def start_enhanced_monitor(config: Dict[str, Any]):
    """启动增强监听系统"""
    try:


        logger.info("启动HKEX增强公告监听系统...")
        logger.info("=" * 60)

        # 创建处理器
        processor = EnhancedAnnouncementProcessor(config)

        # 初始化系统
        logger.info("正在初始化系统...")
        if not await processor.initialize():
            logger.error("系统初始化失败")
            return

        logger.info("系统初始化完成！")

        # 显示系统状态
        status = processor.get_system_status()
        logger.info("系统状态:")
        logger.info(f"  监听股票数量: {status.get('system_info', {}).get('monitored_stocks_count', 0)}")
        logger.info(f"  API轮询间隔: {status.get('configuration', {}).get('api_poll_interval_seconds', 0)}秒")
        logger.info(f"  股票同步间隔: {status.get('configuration', {}).get('stock_sync_interval_hours', 0):.1f}小时")
        logger.info(f"  最大并发处理: {status.get('configuration', {}).get('max_concurrent_processing', 0)}")

        # 启动持续监听
        logger.info("开始持续监听...")
        logger.info("=" * 60)

        await processor.run_continuous_monitoring()

    except KeyboardInterrupt:
        logger.info("用户中断，正在停止系统...")
    except Exception as e:
        logger.error(f"系统运行失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'processor' in locals():
            await processor.close()
        logger.info("系统已停止")


def show_help():
    """显示帮助信息"""
    help_text = """
🚀 HKEX增强公告监听系统

基于ClickHouse的新股监听与公告自动处理系统，支持：
• 实时监听HKEX公告API
• 智能双重过滤（股票+类型）
• 异步高速下载
• 完整向量化处理
• 动态股票发现

使用方法:
  python start_enhanced_monitor.py [选项]

选项:
  -h, --help     显示此帮助信息
  -c, --config   指定配置文件路径（默认: config.yaml）
  -t, --test     运行测试模式
  --version      显示版本信息

示例:
  python start_enhanced_monitor.py                    # 使用默认配置启动
  python start_enhanced_monitor.py -c my_config.yaml  # 使用自定义配置
  python start_enhanced_monitor.py -t                 # 运行测试模式

配置文件:
  系统会自动扩展现有的config.yaml配置文件，添加增强监听功能
  
环境要求:
  • Python 3.8+
  • ClickHouse数据库（可选，用于股票发现）
  • Milvus向量数据库（用于向量化存储）
  • SiliconFlow API密钥（用于文本嵌入）

详细文档请参考:
  docs/2025-09-10_基于ClickHouse的新股监听与公告自动处理方案_修正版.md
"""
    print(help_text)


async def run_test_mode():
    """运行测试模式"""
    logger.info("=== 测试模式 ===")

    monitor = None
    try:
        from services.monitor.api_monitor import HKEXAPIMonitor
        from services.monitor.dual_filter import DualAnnouncementFilter

        # 测试API连接
        logger.info("测试API连接...")
        api_config = {'base_url': 'https://www1.hkexnews.hk/ncms/json/eds/lcisehk1relsdc_1.json', 'poll_interval': 300,
                      'timeout': 30, 'max_retries': 3}

        monitor = HKEXAPIMonitor(api_config)
        announcements = await monitor.fetch_latest_announcements()
        logger.info(f"API测试成功，获取到 {len(announcements)} 条公告")

        # 测试过滤功能
        logger.info("测试过滤功能...")
        filter_config = {'realtime_monitoring': {
            'filtering': {'stock_filter_enabled': True, 'type_filter_enabled': True,
                          'excluded_categories': ['翌日披露報表', '展示文件'], 'included_keywords': []}}}

        test_stocks = {'00700', '01810', '09988'}
        dual_filter = DualAnnouncementFilter(test_stocks, filter_config)

        # 使用模拟数据测试
        mock_data = [{'STOCK_CODE': '00700.HK', 'TITLE': '测试公告', 'LONG_TEXT': '通告及告示'},
                     {'STOCK_CODE': '00001.HK', 'TITLE': '排除公告', 'LONG_TEXT': '翌日披露報表'}]

        filtered = await dual_filter.filter_announcements(mock_data)
        logger.info(f"过滤测试成功: {len(mock_data)} -> {len(filtered)} 条")

        logger.info("所有测试通过！系统运行正常")

    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保关闭所有HTTP连接
        if monitor:
            await monitor.close()
            logger.debug("API监听器已关闭")


def main():
    """主函数"""


    parser = argparse.ArgumentParser(description="HKEX增强公告监听系统",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--config', default='config.yaml', help='配置文件路径')
    parser.add_argument('-t', '--test', action='store_true', help='运行测试模式')
    parser.add_argument('--version', action='version', version='HKEX Enhanced Monitor v2.0')

    args = parser.parse_args()

    if args.test:
        asyncio.run(run_test_mode())
        return

    # 检查配置文件
    if not Path(args.config).exists():
        logger.error(f"配置文件不存在: {args.config}")
        logger.info("请确保config.yaml文件存在，或使用 -c 指定其他配置文件")
        sys.exit(1)

    # 加载配置并启动系统
    config = load_config(args.config)
    asyncio.run(start_enhanced_monitor(config))


if __name__ == "__main__":
    main()
