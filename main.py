#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告下载器 - 命令行版本
作者：Victor Suen
版本：2.0（移除GUI版本）
"""

import requests
import json
import os
import sys
import yaml
import argparse
import logging
import time
import pymysql
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Tuple, Dict, Any
from typing import List, Dict, Any, Optional, Tuple


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.is_connected = False

    def connect(self) -> bool:
        """连接数据库"""
        try:
            db_config = self.config.get('database', {})
            if not db_config.get('enabled', False):
                logging.info("数据库功能未启用")
                return False

            connection_params = {
                'host': db_config.get('host', 'localhost'),
                'port': db_config.get('port', 3306),
                'user': db_config.get('user', 'root'),
                'password': db_config.get('password', ''),
                'database': db_config.get('database', 'ccass'),
                'charset': db_config.get('connection', {}).get('charset', 'utf8mb4'),
                'autocommit': db_config.get('connection', {}).get('autocommit', True),
                'connect_timeout': db_config.get('connection', {}).get('connect_timeout', 30),
                'read_timeout': db_config.get('connection', {}).get('read_timeout', 30)
            }

            self.connection = pymysql.connect(**connection_params)
            self.is_connected = True
            logging.info(
                f"成功连接到数据库: {connection_params['host']}:{connection_params['port']}/{connection_params['database']}")
            return True

        except Exception as e:
            logging.error(f"数据库连接失败: {str(e)}")
            self.is_connected = False
            return False

    def disconnect(self):
        """断开数据库连接"""
        if self.connection and self.is_connected:
            try:
                self.connection.close()
                self.is_connected = False
                logging.info("数据库连接已断开")
            except Exception as e:
                logging.warning(f"断开数据库连接时发生错误: {str(e)}")

    def test_connection(self) -> bool:
        """测试数据库连接"""
        if self.connect():
            try:
                if self.connection:
                    with self.connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        result = cursor.fetchone()
                        logging.info("数据库连接测试成功")
                        return True
            except Exception as e:
                logging.error(f"数据库连接测试失败: {str(e)}")
                return False
            finally:
                self.disconnect()
        return False

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """执行SQL查询"""
        if not self.is_connected and not self.connect():
            raise Exception("无法连接到数据库")

        if not self.connection:
            raise Exception("数据库连接无效")

        try:
            max_retries = self.config.get('database', {}).get('connection', {}).get('max_retries', 3)

            for attempt in range(max_retries):
                try:
                    cursor = self.connection.cursor()
                    cursor.execute(query)

                    # 获取列名
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []

                    # 获取数据并转换为字典列表
                    rows = cursor.fetchall()
                    results = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            if i < len(columns):
                                row_dict[columns[i]] = value
                        results.append(row_dict)

                    cursor.close()
                    logging.info(f"SQL查询成功，返回 {len(results)} 条记录")
                    return results

                except pymysql.MySQLError as e:
                    if attempt < max_retries - 1:
                        logging.warning(f"SQL查询失败，第 {attempt + 1} 次重试: {str(e)}")
                        time.sleep(1)
                        # 尝试重新连接
                        self.disconnect()
                        if not self.connect():
                            break
                        continue
                    else:
                        raise Exception(f"SQL查询失败: {str(e)}")

            # 如果所有重试都失败了
            raise Exception("SQL查询失败: 超过最大重试次数")

        except Exception as e:
            logging.error(f"执行SQL查询时发生错误: {str(e)}")
            raise

    def get_stock_codes(self, query: str = None) -> List[str]:
        """获取股票代码列表"""
        try:
            if not query:
                # 使用默认查询
                db_config = self.config.get('database', {})
                table = db_config.get('default_table', 'issue')
                stock_code_field = db_config.get('fields', {}).get('stock_code', 'stockCode')
                stock_name_field = db_config.get('fields', {}).get('stock_name', 'stockName')
                status_field = db_config.get('fields', {}).get('status', 'status')
                status_filter = db_config.get('status_filter', ['normal'])

                # 构建状态过滤条件
                if len(status_filter) == 1:
                    status_condition = f"{status_field} = '{status_filter[0]}'"
                else:
                    status_values = "', '".join(status_filter)
                    status_condition = f"{status_field} IN ('{status_values}')"

                query = f"SELECT {stock_code_field}, {stock_name_field} FROM {table} WHERE {status_condition}"

            results = self.execute_query(query)

            # 提取股票代码
            stock_codes = []
            for row in results:
                # 使用配置的字段映射获取股票代码
                code = self._get_field_value(row, 'stock_code', db_config)
                if not code:
                    # 回退到尝试不同的字段名
                    for key in ['stockCode', 'stock_code', 'code', 'symbol']:
                        if key in row and row[key]:
                            code = str(row[key]).strip()
                            break

                if code and code.isdigit() and len(code) <= 8:  # 适配varchar(8)
                    # 智能格式化股票代码
                    formatted_code = self._format_stock_code(code)
                    if formatted_code:
                        stock_codes.append(formatted_code)
                    else:
                        logging.warning(f"跳过无效的股票代码: {code}")
                else:
                    logging.warning(f"跳过无效的股票代码: {row}")

            logging.info(f"从数据库获取到 {len(stock_codes)} 个有效股票代码")
            return stock_codes

        except Exception as e:
            logging.error(f"获取股票代码失败: {str(e)}")
            return []

    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()

    def _get_field_value(self, row, field_type, db_config):
        """根据配置获取字段值"""
        field_name = db_config.get('fields', {}).get(field_type)
        if field_name and field_name in row and row[field_name]:
            return str(row[field_name]).strip()
        return None

    def _format_stock_code(self, code):
        """智能格式化股票代码"""
        try:
            # 去除前导零后重新格式化
            code_int = int(code)
            if code_int > 0:
                # 标准5位格式
                return str(code_int).zfill(5)
            return None
        except ValueError:
            return None


class AnnouncementClassifier:
    """公告分类识别器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.categories = config.get('announcement_categories', {})
        self.enabled = self.categories.get('enabled', True)

        # 初始化繁简体转换器
        try:
            import opencc
            self.t2s_converter = opencc.OpenCC('t2s')  # 繁体转简体
            self.s2t_converter = opencc.OpenCC('s2t')  # 简体转繁体
            self.opencc_available = True
        except ImportError:
            print("警告：OpenCC库未安装，繁简体转换功能将被禁用")
            self.opencc_available = False
            self.t2s_converter = None
            self.s2t_converter = None

    def _convert_text(self, text: str) -> List[str]:
        """
        将文本转换为繁简体版本

        Args:
            text: 原始文本

        Returns:
            List[str]: 包含原文、繁体、简体的文本列表
        """
        variants = [text]

        if self.opencc_available and self.s2t_converter and self.t2s_converter:
            try:
                # 添加繁体版本
                traditional = self.s2t_converter.convert(text)
                if traditional != text:
                    variants.append(traditional)

                # 添加简体版本
                simplified = self.t2s_converter.convert(text)
                if simplified != text:
                    variants.append(simplified)
            except Exception as e:
                print(f"繁简体转换失败: {e}")

        return variants

    def classify_announcement(self, title: str) -> Tuple[str, str]:
        """
        根据公告标题分类公告

        Args:
            title: 公告标题

        Returns:
            Tuple[str, str]: (主分类, 子分类)
        """
        if not self.enabled:
            return "99_其他", "其他"

        # 清理标题，移除特殊字符但保留中文
        clean_title = re.sub(r'[^\w\s\u4e00-\u9fff\u3400-\u4dbf]', '', title)

        # 生成标题的繁简体版本
        title_variants = self._convert_text(title.lower())
        clean_title_variants = self._convert_text(clean_title.lower())

        # 按优先级排序分类
        best_match = None
        best_priority = float('inf')

        # 遍历一级分类
        for main_category, main_config in self.categories.items():
            if main_category == 'enabled':
                continue

            # 检查是否是新的三层结构
            if isinstance(main_config, dict) and 'code' in main_config:
                # 新的三层结构：一级分类 -> 二级分组 -> 三级分类
                for group_name, group_config in main_config.items():
                    if group_name in ['code', 'name', 'count', 'keywords', 'priority']:
                        continue

                    if isinstance(group_config, dict):
                        # 检查是否有 subcategories
                        if 'subcategories' in group_config:
                            subcategories = group_config['subcategories']
                            for sub_category, config in subcategories.items():
                                if isinstance(config, dict):
                                    keywords = config.get('keywords', [])
                                    priority = config.get('priority', 5)

                                    # 检查关键词匹配
                                    if self._check_keyword_match(keywords, title_variants, clean_title_variants):
                                        if priority < best_priority:
                                            best_match = (main_category, sub_category)
                                            best_priority = priority
                        else:
                            # 直接的子分类
                            keywords = group_config.get('keywords', [])
                            priority = group_config.get('priority', 5)

                            if self._check_keyword_match(keywords, title_variants, clean_title_variants):
                                if priority < best_priority:
                                    best_match = (main_category, group_name)
                                    best_priority = priority
            else:
                # 旧的二层结构：主分类 -> 子分类
                if isinstance(main_config, dict):
                    for sub_category, config in main_config.items():
                        if isinstance(config, dict):
                            keywords = config.get('keywords', [])
                            priority = config.get('priority', 5)

                            if self._check_keyword_match(keywords, title_variants, clean_title_variants):
                                if priority < best_priority:
                                    best_match = (main_category, sub_category)
                                    best_priority = priority

        # 如果没有匹配，返回其他分类
        if best_match is None:
            return "99_其他", "其他"

        return best_match

    def _check_keyword_match(self, keywords: List[str], title_variants: List[str],
                             clean_title_variants: List[str]) -> bool:
        """
        检查关键词是否匹配

        Args:
            keywords: 关键词列表
            title_variants: 标题变体列表
            clean_title_variants: 清理后的标题变体列表

        Returns:
            bool: 是否匹配
        """
        for keyword in keywords:
            # 生成关键词的繁简体版本
            keyword_variants = self._convert_text(keyword.lower())

            # 检查所有变体组合（使用更宽松的匹配）
            for title_var in title_variants + clean_title_variants:
                for keyword_var in keyword_variants:
                    # 使用包含匹配和部分匹配
                    if (keyword_var in title_var or
                            any(kw_part in title_var for kw_part in keyword_var.split() if len(kw_part) > 1)):
                        return True
        return False

    def get_folder_path(self, main_category: str, sub_category: str) -> str:
        """
        获取分类对应的文件夹路径

        Args:
            main_category: 主分类
            sub_category: 子分类

        Returns:
            str: 文件夹路径
        """
        return os.path.join(main_category, sub_category)

    def get_classification_stats(self, announcements: List[Dict]) -> Dict[str, int]:
        """
        获取公告分类统计

        Args:
            announcements: 公告列表

        Returns:
            Dict[str, int]: 分类统计
        """
        stats = {}

        for ann in announcements:
            main_cat, sub_cat = self.classify_announcement(ann['title'])
            full_category = f"{main_cat}/{sub_cat}"
            stats[full_category] = stats.get(full_category, 0) + 1

        return stats


class ConfigManager:
    """配置管理器"""

    def __init__(self, config_file: str = "config.yaml"):
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if not os.path.exists(self.config_file):
            self.create_default_config()

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # 验证配置文件结构
            self.validate_config(config)
            return config

        except Exception as e:
            logging.error(f"加载配置文件失败: {e}")
            print(f"配置文件加载失败，将使用默认配置: {e}")
            return self.get_default_config()

    def get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'settings': {
                'save_path': os.path.join(os.path.expanduser("~"), "Desktop"),
                'filename_length': 220,
                'language': 'zh',
                'max_results': 500,
                'verbose_logging': True,
                'log_file': 'hkex_downloader.log'
            },
            'date_range': {
                'start_date': '2024-01-01',
                'end_date': 'today'
            },
            'download_tasks': [],
            'advanced': {
                'retry_attempts': 3,
                'request_delay': 1,
                'timeout': 30,
                'overwrite_existing': False,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36'
            }
        }

    def create_default_config(self):
        """创建默认配置文件"""
        default_config = self.get_default_config()
        default_config['download_tasks'] = [
            {
                'name': '默认下载任务',
                'stock_code': '00081',
                'start_date': '2024-01-01',
                'end_date': 'today',
                'keywords': [],
                'enabled': True
            }
        ]

        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(default_config, f, default_flow_style=False,
                          allow_unicode=True, indent=2)
            logging.info(f"已创建默认配置文件: {self.config_file}")
        except Exception as e:
            logging.error(f"创建配置文件失败: {e}")

    def validate_config(self, config: Dict[str, Any]):
        """验证配置文件格式"""
        required_sections = ['settings', 'date_range', 'advanced']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"配置文件缺少必需的部分: {section}")

    def get(self, section: str, key: str = None, default=None):
        """获取配置值"""
        if key is None:
            return self.config.get(section, default)
        return self.config.get(section, {}).get(key, default)

    def parse_date(self, date_str: str) -> datetime:
        """解析日期字符串"""
        if date_str.lower() == 'today':
            return datetime.now()
        elif date_str.lower() == 'yesterday':
            return datetime.now() - timedelta(days=1)
        else:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"无效的日期格式: {date_str}，请使用 YYYY-MM-DD 格式")

    def get_database_manager(self) -> Optional[DatabaseManager]:
        """获取数据库管理器实例"""
        if not self.config.get('database', {}).get('enabled', False):
            return None
        return DatabaseManager(self.config)

    def test_database_connection(self) -> bool:
        """测试数据库连接"""
        db_manager = self.get_database_manager()
        if not db_manager:
            logging.info("数据库功能未启用")
            return False
        return db_manager.test_connection()

    def get_stocks_from_database(self, query: str = None) -> List[str]:
        """从数据库获取股票代码列表"""
        db_manager = self.get_database_manager()
        if not db_manager:
            logging.warning("数据库功能未启用，无法获取股票列表")
            return []

        try:
            with db_manager:
                return db_manager.get_stock_codes(query)
        except Exception as e:
            logging.error(f"从数据库获取股票代码失败: {str(e)}")
            return []


class HKEXDownloader:
    """HKEX公告下载器核心类"""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.headers = {
            "User-Agent": self.config.get('advanced', 'user_agent')
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # 设置超时和重试
        self.timeout = self.config.get('advanced', 'timeout', 30)
        self.retry_attempts = self.config.get('advanced', 'retry_attempts', 3)
        self.request_delay = self.config.get('advanced', 'request_delay', 1)

        # 初始化公告分类器
        self.classifier = AnnouncementClassifier(self.config.config)

    def get_stockid(self, stockcode: str) -> str:
        """根据股票代码获取股票ID"""
        for attempt in range(self.retry_attempts):
            try:
                url = f'https://www1.hkexnews.hk/search/prefix.do?&callback=callback&lang=ZH&type=A&name={stockcode}&market=SEHK&_=1653821865437'
                response = self.session.get(url, timeout=self.timeout)

                # 处理 JSONP 响应
                data = response.text[9:-4]
                data_json = json.loads(data)

                if 'stockInfo' not in data_json or not data_json['stockInfo']:
                    raise ValueError(f"未找到股票代码 {stockcode} 对应的信息")

                stockid = data_json["stockInfo"][0]['stockId']
                logging.info(f"股票代码 {stockcode} 对应的 StockID: {stockid}")
                return stockid

            except Exception as e:
                if attempt < self.retry_attempts - 1:
                    logging.warning(f"获取股票信息失败，第 {attempt + 1} 次重试: {e}")
                    time.sleep(self.request_delay)
                    continue
                else:
                    raise Exception(f"获取股票 {stockcode} 信息失败: {str(e)}")

    def get_announcement_list(self, stockcode: str, start_date: datetime,
                              end_date: datetime, keywords: List[str] = None) -> List[Dict]:
        """获取公告列表"""
        try:
            # 获取 stockId
            stockid = self.get_stockid(stockcode)

            # 处理关键字
            search_keyword = keywords[-1] if keywords else ""

            # 准备日期格式
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")

            # 获取语言和最大结果数设置
            language = self.config.get('settings', 'language', 'zh')
            max_results = self.config.get('settings', 'max_results', 500)

            # 构建完整URL
            url = f'https://www1.hkexnews.hk/search/titleSearchServlet.do?sortDir=0&sortByOptions=DateTime&category=0&market=SEHK&stockId={stockid}&documentType=-1&fromDate={start_date_str}&toDate={end_date_str}&title={search_keyword}&searchType=0&t1code=-2&t2Gcode=-2&t2code=-2&rowRange={max_results}&lang={language}'

            logging.info(f"搜索URL: {url}")

            # 发送请求
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code != 200:
                raise Exception(f"无法连接到港交所网站 (状态码: {response.status_code})")

            # 处理响应数据
            try:
                data = response.text
                # 清理响应文本
                data = data.replace('"[{', '[{').replace('}]"', '}]').replace('\\', "").replace('u2013', "-").replace(
                    'u0026', "-")
                data_json = json.loads(data)

                if not data_json or 'result' not in data_json or not data_json['result']:
                    logging.warning("未找到符合条件的公告")
                    return []

                announcements = []
                for item in data_json['result']:
                    try:
                        # 获取标题
                        title = item['TITLE'].replace('/', "-")

                        # 获取 PDF 链接
                        pdflink = item['FILE_LINK']
                        pdf_link = "https://www1.hkexnews.hk" + pdflink

                        # 处理日期
                        anndate = item['DATE_TIME']
                        anndate = anndate[:10].replace('/', "-")
                        date_object = datetime.strptime(anndate, "%d-%m-%Y")
                        formatted_date = date_object.strftime("%Y-%m-%d")

                        announcements.append({
                            'date': formatted_date,
                            'title': title,
                            'link': pdf_link
                        })

                    except Exception as e:
                        logging.warning(f"处理公告项目时发生错误: {str(e)}")
                        continue

                logging.info(f"找到 {len(announcements)} 个符合条件的公告")
                return announcements

            except json.JSONDecodeError as e:
                logging.error(f"JSON 解析错误: {str(e)}")
                raise Exception("服务器响应格式无效")

        except Exception as e:
            raise Exception(f"获取公告列表时发生错误: {str(e)}")

    def download_announcements(self, task: Dict[str, Any]) -> tuple[str, int]:
        """下载公告文件"""
        try:
            # 检查是否从数据库获取股票
            if task.get('from_database', False):
                return self._download_from_database(task)

            # 解析任务参数
            stockcode = task.get('stock_code')
            if not stockcode:
                raise ValueError("任务配置缺少 stock_code")

            return self._download_single_stock(task, stockcode)

        except Exception as e:
            raise Exception(f"下载过程中出现错误: {str(e)}")

    def _download_from_database(self, task: Dict[str, Any]) -> tuple[str, int]:
        """从数据库获取股票列表并下载"""
        query = task.get('query')
        stock_codes = self.config.get_stocks_from_database(query)

        if not stock_codes:
            logging.warning("从数据库未获取到任何股票代码")
            return "", 0

        logging.info(
            f"从数据库获取到 {len(stock_codes)} 个股票代码: {', '.join(stock_codes[:5])}{'...' if len(stock_codes) > 5 else ''}")

        total_downloaded = 0
        base_save_path = self.config.get('settings', 'save_path')

        for i, stock_code in enumerate(stock_codes, 1):
            try:
                logging.info(f"[{i}/{len(stock_codes)}] 处理股票: {stock_code}")

                # 创建临时任务配置
                stock_task = task.copy()
                stock_task['stock_code'] = stock_code
                stock_task.pop('from_database', None)
                stock_task.pop('query', None)

                save_path, count = self._download_single_stock(stock_task, stock_code)
                total_downloaded += count

                if count > 0:
                    logging.info(f"✓ 股票 {stock_code} 下载完成: {count} 个文件")
                else:
                    logging.info(f"- 股票 {stock_code} 无新文件")

            except Exception as e:
                logging.error(f"✗ 股票 {stock_code} 下载失败: {str(e)}")
                continue

        logging.info(f"数据库任务完成！总共下载 {total_downloaded} 个文件")
        return os.path.join(base_save_path, 'HKEX'), total_downloaded

    def _download_single_stock(self, task: Dict[str, Any], stockcode: str) -> tuple[str, int]:
        """下载单个股票的公告"""
        # 解析日期
        start_date = self.config.parse_date(task.get('start_date', '2024-01-01'))
        end_date = self.config.parse_date(task.get('end_date', 'today'))

        keywords = task.get('keywords', [])

        logging.info(f"开始下载任务: {task.get('name', '未命名任务')}")
        logging.info(
            f"股票代码: {stockcode}, 日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")

        # 获取公告列表
        announcements = self.get_announcement_list(stockcode, start_date, end_date, keywords)

        if not announcements:
            logging.warning("未找到符合条件的公告")
            return "", 0

        # 创建保存目录
        save_path = self.config.get('settings', 'save_path')
        base_path = os.path.join(save_path, 'HKEX', stockcode)

        if not os.path.exists(base_path):
            os.makedirs(base_path)
            logging.info(f"创建保存目录: {base_path}")

        # 获取分类统计
        if self.classifier.enabled:
            classification_stats = self.classifier.get_classification_stats(announcements)
            logging.info(f"公告分类统计: {classification_stats}")

        # 下载公告
        download_count = 0
        filename_length = self.config.get('settings', 'filename_length', 220)
        overwrite = self.config.get('advanced', 'overwrite_existing', False)

        total_count = len(announcements)

        for i, ann in enumerate(announcements, 1):
            try:
                # 使用分类器确定文件保存路径
                if self.classifier.enabled:
                    main_category, sub_category = self.classifier.classify_announcement(ann['title'])
                    category_path = self.classifier.get_folder_path(main_category, sub_category)
                    savepath = os.path.join(base_path, category_path)
                else:
                    savepath = base_path

                # 确保分类文件夹存在
                if not os.path.exists(savepath):
                    os.makedirs(savepath)
                    logging.info(f"创建分类目录: {savepath}")

                filepath = os.path.join(
                    savepath,
                    f"{ann['date']}_{stockcode}-{ann['title'][:filename_length]}.pdf"
                )

                # 检查文件是否已存在
                if os.path.exists(filepath) and not overwrite:
                    logging.info(f"跳过已存在的文件 ({i}/{total_count}): {os.path.basename(filepath)}")
                    continue

                # 显示分类信息
                if self.classifier.enabled:
                    logging.info(f"正在下载 ({i}/{total_count}) [{main_category}/{sub_category}]: {ann['title']}")
                else:
                    logging.info(f"正在下载 ({i}/{total_count}): {ann['title']}")

                response = self.session.get(ann['link'], timeout=self.timeout)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    download_count += 1
                    logging.info(f"✓ 成功下载: {os.path.basename(filepath)}")
                else:
                    logging.warning(f"下载失败 (HTTP {response.status_code}): {ann['title']}")

                # 请求间隔
                if self.request_delay > 0:
                    time.sleep(self.request_delay)

            except Exception as e:
                logging.error(f"下载文件时发生错误: {ann['title']} - {str(e)}")
                continue

        logging.info(f"任务完成！成功下载 {download_count}/{total_count} 个文件到 {base_path}")
        return base_path, download_count


class HKEXDownloaderCLI:
    """命令行界面"""

    def __init__(self):
        self.config_manager = None
        self.downloader = None

    def setup_logging(self, config_manager: ConfigManager):
        """设置日志"""
        log_level = logging.INFO if config_manager.get('settings', 'verbose_logging', True) else logging.WARNING
        log_file = config_manager.get('settings', 'log_file')

        # 配置日志格式
        log_format = '%(asctime)s - %(levelname)s - %(message)s'

        handlers = [logging.StreamHandler()]
        if log_file:
            handlers.append(logging.FileHandler(log_file, encoding='utf-8'))

        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=handlers
        )

    def create_parser(self) -> argparse.ArgumentParser:
        """创建命令行参数解析器"""
        parser = argparse.ArgumentParser(
            description='HKEX 公告下载器 - 命令行版本',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
使用示例:
  %(prog)s                                    # 使用配置文件中的任务
  %(prog)s -s 00001                          # 下载单个股票的所有公告
  %(prog)s -s 00001 -k "财务报告"             # 下载指定关键字的公告
  %(prog)s -s 00001 --start 2024-01-01       # 指定开始日期
  %(prog)s --config my_config.yaml           # 使用指定的配置文件
  %(prog)s --check-config                    # 检查配置文件
            """)

        parser.add_argument('-c', '--config',
                            default='config.yaml',
                            help='配置文件路径 (默认: config.yaml)')

        parser.add_argument('-s', '--stock-code',
                            help='股票代码 (5位数字)')

        parser.add_argument('-k', '--keywords',
                            nargs='*',
                            help='搜索关键字')

        parser.add_argument('--start',
                            help='开始日期 (YYYY-MM-DD)')

        parser.add_argument('--end',
                            help='结束日期 (YYYY-MM-DD 或 today)')

        parser.add_argument('--save-path',
                            help='保存路径')

        parser.add_argument('--check-config',
                            action='store_true',
                            help='检查配置文件格式')

        parser.add_argument('--list-tasks',
                            action='store_true',
                            help='列出配置文件中的所有任务')

        parser.add_argument('--run-task',
                            help='运行指定名称的任务')

        parser.add_argument('-v', '--verbose',
                            action='store_true',
                            help='启用详细输出')

        # 数据库相关参数
        parser.add_argument('--test-db',
                            action='store_true',
                            help='测试数据库连接')

        parser.add_argument('--db-stocks',
                            action='store_true',
                            help='从数据库获取所有股票代码并下载')

        parser.add_argument('--db-query',
                            help='自定义数据库查询语句')

        return parser

    def check_config(self, config_file: str):
        """检查配置文件"""
        try:
            config_manager = ConfigManager(config_file)
            print(f"✓ 配置文件 {config_file} 格式正确")

            # 显示配置摘要
            print("\n配置摘要:")
            print(f"  保存路径: {config_manager.get('settings', 'save_path')}")
            print(f"  语言设置: {config_manager.get('settings', 'language')}")
            print(f"  文件名长度: {config_manager.get('settings', 'filename_length')}")

            tasks = config_manager.get('download_tasks') or []
            if isinstance(tasks, list):
                enabled_tasks = [t for t in tasks if isinstance(t, dict) and t.get('enabled', True)]
                print(f"  下载任务: {len(tasks)} 个 (启用: {len(enabled_tasks)} 个)")
            else:
                print(f"  下载任务: 配置格式错误 (应为列表格式)")

        except Exception as e:
            print(f"✗ 配置文件检查失败: {e}")
            sys.exit(1)

    def list_tasks(self, config_manager: ConfigManager):
        """列出所有任务"""
        tasks = config_manager.get('download_tasks') or []
        if not tasks:
            print("配置文件中没有定义任务")
            return

        print("配置文件中的任务:")
        for i, task in enumerate(tasks, 1):
            status = "启用" if task.get('enabled', True) else "禁用"
            print(f"  {i}. {task.get('name', '未命名任务')} [{status}]")
            print(f"     股票代码: {task.get('stock_code', 'N/A')}")
            print(f"     日期范围: {task.get('start_date', 'N/A')} 至 {task.get('end_date', 'N/A')}")
            keywords = task.get('keywords', [])
            if keywords:
                print(f"     关键字: {', '.join(keywords)}")
            print()

    def run_single_task(self, args, config_manager: ConfigManager):
        """运行单个任务"""
        # 构建任务配置
        task = {
            'name': '命令行任务',
            'stock_code': args.stock_code,
            'start_date': args.start or config_manager.get('date_range', 'start_date', '2024-01-01'),
            'end_date': args.end or config_manager.get('date_range', 'end_date', 'today'),
            'keywords': args.keywords or [],
            'enabled': True
        }

        # 如果指定了保存路径，临时更新配置
        if args.save_path:
            config_manager.config['settings']['save_path'] = args.save_path

        downloader = HKEXDownloader(config_manager)

        try:
            save_path, count = downloader.download_announcements(task)
            if count > 0:
                print(f"\n✓ 下载完成！成功下载 {count} 个文件到 {save_path}")
            else:
                print("\n⚠ 未找到符合条件的公告")
        except Exception as e:
            print(f"\n✗ 下载失败: {e}")
            sys.exit(1)

    def run_config_tasks(self, config_manager: ConfigManager):
        """运行配置文件中的任务"""
        tasks = config_manager.get('download_tasks') or []
        enabled_tasks = [t for t in tasks if isinstance(t, dict) and t.get('enabled', True)]

        if not enabled_tasks:
            print("配置文件中没有启用的任务")
            return

        downloader = HKEXDownloader(config_manager)
        total_downloaded = 0

        print(f"开始执行 {len(enabled_tasks)} 个启用的任务:\n")

        for i, task in enumerate(enabled_tasks, 1):
            print(f"[{i}/{len(enabled_tasks)}] 执行任务: {task.get('name', '未命名任务')}")

            try:
                save_path, count = downloader.download_announcements(task)
                total_downloaded += count
                if count > 0:
                    print(f"✓ 任务完成！下载 {count} 个文件到 {save_path}\n")
                else:
                    print("⚠ 未找到符合条件的公告\n")

            except Exception as e:
                print(f"✗ 任务失败: {e}\n")
                continue

        print(f"所有任务执行完成！总共下载 {total_downloaded} 个文件")

    def run_named_task(self, task_name: str, config_manager: ConfigManager):
        """运行指定名称的任务"""
        tasks = config_manager.get('download_tasks') or []
        target_task = None

        for task in tasks:
            if task.get('name') == task_name:
                target_task = task
                break

        if not target_task:
            print(f"未找到名称为 '{task_name}' 的任务")
            sys.exit(1)

        if not target_task.get('enabled', True):
            print(f"任务 '{task_name}' 已禁用")
            sys.exit(1)

        downloader = HKEXDownloader(config_manager)

        try:
            print(f"执行任务: {task_name}")
            save_path, count = downloader.download_announcements(target_task)
            if count > 0:
                print(f"\n✓ 任务完成！成功下载 {count} 个文件到 {save_path}")
            else:
                print("\n⚠ 未找到符合条件的公告")
        except Exception as e:
            print(f"\n✗ 任务失败: {e}")
            sys.exit(1)

    def test_database(self, config_manager: ConfigManager):
        """测试数据库连接"""
        print("正在测试数据库连接...")

        if config_manager.test_database_connection():
            print("✓ 数据库连接测试成功")
        else:
            print("✗ 数据库连接测试失败")
            sys.exit(1)

    def run_database_task(self, args, config_manager: ConfigManager):
        """运行数据库任务"""
        # 构建数据库任务配置
        task = {
            'name': '数据库股票下载任务',
            'from_database': True,
            'query': args.db_query,  # 可能为None，使用默认查询
            'start_date': args.start or config_manager.get('date_range', 'start_date', '2024-01-01'),
            'end_date': args.end or config_manager.get('date_range', 'end_date', 'today'),
            'keywords': args.keywords or [],
            'enabled': True
        }

        # 如果指定了保存路径，临时更新配置
        if args.save_path:
            config_manager.config['settings']['save_path'] = args.save_path

        downloader = HKEXDownloader(config_manager)

        try:
            print("开始从数据库获取股票列表并下载...")
            save_path, count = downloader.download_announcements(task)
            if count > 0:
                print(f"\n✓ 数据库任务完成！成功下载 {count} 个文件到 {save_path}")
            else:
                print("\n⚠ 未找到符合条件的公告")
        except Exception as e:
            print(f"\n✗ 数据库任务失败: {e}")
            sys.exit(1)

    def main(self):
        """主函数"""
        parser = self.create_parser()
        args = parser.parse_args()

        # 检查配置文件
        if args.check_config:
            self.check_config(args.config)
            return

        # 加载配置
        try:
            self.config_manager = ConfigManager(args.config)
        except Exception as e:
            print(f"配置文件加载失败: {e}")
            sys.exit(1)

        # 设置日志
        if args.verbose:
            self.config_manager.config['settings']['verbose_logging'] = True

        self.setup_logging(self.config_manager)

        # 列出任务
        if args.list_tasks:
            self.list_tasks(self.config_manager)
            return

        # 测试数据库连接
        if args.test_db:
            self.test_database(self.config_manager)
            return

        # 从数据库获取股票并下载
        if args.db_stocks:
            self.run_database_task(args, self.config_manager)
            return

        # 运行指定名称的任务
        if args.run_task:
            self.run_named_task(args.run_task, self.config_manager)
            return

        # 运行单个股票下载任务
        if args.stock_code:
            if not args.stock_code.isdigit() or len(args.stock_code) != 5:
                print("错误: 股票代码必须为5位数字")
                sys.exit(1)
            self.run_single_task(args, self.config_manager)
            return

        # 运行配置文件中的任务
        self.run_config_tasks(self.config_manager)


def main():
    """程序入口点"""
    try:
        cli = HKEXDownloaderCLI()
        cli.main()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序运行出错: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
