#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告下载器 - 命令行版本
作者：Victor Suen
版本：2.0（移除GUI版本）
"""

import argparse
import atexit
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pymysql
import requests
import yaml

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

            connection_params = {'host': db_config.get('host', 'localhost'), 'port': db_config.get('port', 3306),
                                 'user': db_config.get('user', 'root'), 'password': db_config.get('password', ''),
                                 'database': db_config.get('database', 'ccass'),
                                 'charset': db_config.get('connection', {}).get('charset', 'utf8mb4'),
                                 'autocommit': db_config.get('connection', {}).get('autocommit', True),
                                 'connect_timeout': db_config.get('connection', {}).get('connect_timeout', 30),
                                 'read_timeout': db_config.get('connection', {}).get('read_timeout', 30)}

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
            stock_codes_set = set()  # 用于去重
            for row in results:
                # 使用配置的字段映射获取股票代码
                code = self._get_field_value(row, 'stock_code', db_config)
                if not code:
                    # 回退到尝试不同的字段名
                    for key in ['stockCode', 'stock_code', 'code', 'symbol']:
                        if key in row and row[key]:
                            code = str(row[key]).strip()
                            break

                if code:
                    # 处理.HK后缀：去除.HK后缀
                    clean_code = code
                    if code.upper().endswith('.HK'):
                        clean_code = code[:-3]  # 去除.HK后缀
                        logging.debug(f"去除.HK后缀: {code} -> {clean_code}")
                    
                    # 验证清理后的代码是否为有效数字格式
                    if clean_code.isdigit() and len(clean_code) <= 8:  # 适配varchar(8)
                        # 智能格式化股票代码
                        formatted_code = self._format_stock_code(clean_code)
                        if formatted_code and formatted_code not in stock_codes_set:
                            stock_codes.append(formatted_code)
                            stock_codes_set.add(formatted_code)  # 添加到去重集合
                        elif formatted_code in stock_codes_set:
                            logging.debug(f"跳过重复股票代码: {formatted_code}")
                        else:
                            logging.warning(f"跳过无效的股票代码: {clean_code}")
                    else:
                        logging.warning(f"跳过无效的股票代码格式: {clean_code} (原始: {code})")

            logging.info(f"从数据库获取到 {len(stock_codes)} 个有效股票代码（已去重）")
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
        # 从配置文件获取错误处理设置
        error_config = self.config.get('error_handling', {})
        return error_config.get('api_error_fallback', None)

    def _format_stock_code(self, code):
        """智能格式化股票代码"""
        try:
            # 去除前导零后重新格式化
            code_int = int(code)
            if code_int > 0:
                # 标准5位格式
                return str(code_int).zfill(5)
            # 从配置文件获取错误处理设置
            error_config = self.config.get('error_handling', {})
            return error_config.get('api_error_fallback', None)
        except ValueError:
            # 从配置文件获取错误处理设置
            error_config = self.config.get('error_handling', {})
            return error_config.get('api_error_fallback', None)


class AnnouncementClassifier:
    """公告分类识别器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.categories = config.get('announcement_categories', {})
        self.enabled = self.categories.get('enabled', True)
        
        # 加载关键字配置
        self.keyword_config = config.get('common_keywords', {})

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

    def _match_keyword_category(self, title: str) -> Tuple[str, str, float]:
        """
        根据公告标题匹配关键字分类，支持多关键字检测
        
        Args:
            title: 公告标题
            
        Returns:
            Tuple[str, str, float]: (主要关键字分类, 所有匹配关键字, 最高置信度)
        """
        if not self.keyword_config or not title:
            return "", "", 0.0
        
        # 将标题转换为繁简体版本进行匹配
        title_variants = self._convert_text(title.lower())
        
        matched_keywords = []  # 存储所有匹配的关键字信息
        
        for category_key, category_config in self.keyword_config.items():
            if not isinstance(category_config, dict):
                continue
                
            # 获取关键字信息
            chinese_keywords = category_config.get('chinese', [])
            english_keywords = category_config.get('english', [])
            folder_name = category_config.get('folder_name', category_key)
            priority = category_config.get('priority', 50)  # 默认优先级50
            weight = category_config.get('weight', 0.5)     # 默认权重0.5
            
            # 检查中文关键字匹配
            for keyword in chinese_keywords:
                for title_variant in title_variants:
                    if keyword.lower() in title_variant:
                        matched_keywords.append({
                            'folder_name': folder_name,
                            'keyword': keyword,
                            'priority': priority,
                            'weight': weight,
                            'confidence': 0.9
                        })
                        break  # 找到匹配就跳出内层循环
            
            # 检查英文关键字匹配
            for keyword in english_keywords:
                for title_variant in title_variants:
                    if keyword.lower() in title_variant:
                        matched_keywords.append({
                            'folder_name': folder_name,
                            'keyword': keyword,
                            'priority': priority,
                            'weight': weight,
                            'confidence': 0.9
                        })
                        break  # 找到匹配就跳出内层循环
        
        if not matched_keywords:
            return "", "", 0.0
        
        # 按优先级和权重排序，选择最佳匹配
        matched_keywords.sort(key=lambda x: (x['priority'], x['weight']), reverse=True)
        
        # 主要分类使用最高优先级的关键字
        primary_match = matched_keywords[0]
        primary_category = primary_match['folder_name']
        
        # 生成所有匹配关键字的描述
        all_keywords = [match['folder_name'] for match in matched_keywords]
        all_keywords_str = "+".join(sorted(set(all_keywords), key=all_keywords.index))  # 去重但保持顺序
        
        # 计算综合置信度
        max_confidence = max(match['confidence'] * match['weight'] for match in matched_keywords)
        
        return primary_category, all_keywords_str, max_confidence

    def classify_announcement_enhanced(self, announcement_data: Dict[str, Any]) -> Tuple[str, str, str, str, float]:
        """
        增强分类：关键字分类 + LONG_TEXT分类，支持多关键字检测
        
        Args:
            announcement_data: 包含标题、日期、股票信息、原始数据等的字典
            
        Returns:
            Tuple[str, str, str, str, float]: (关键字分类, 所有匹配关键字, 主分类, 子分类, 置信度)
        """
        title = announcement_data.get('title', '')
        raw_data = announcement_data.get('raw_data', {})
        long_text = raw_data.get('LONG_TEXT', '')

        # 第一步：多关键字分类匹配
        keyword_category, all_keywords, keyword_confidence = self._match_keyword_category(title)
        
        # 第二步：LONG_TEXT分类
        main_category = ""
        sub_category = ""
        longtext_confidence = 0.0
        
        if long_text:
            # 清理LONG_TEXT
            cleaned_text = self._clean_long_text(long_text)
            if cleaned_text:
                main_category = cleaned_text
                sub_category = cleaned_text
                longtext_confidence = 1.0

        # 如果没有LONG_TEXT，使用标题作为备用
        if not main_category:
            main_category = "其他"
            sub_category = title or "未知"
            longtext_confidence = 0.5

        # 计算总体置信度
        if keyword_category:
            total_confidence = (keyword_confidence + longtext_confidence) / 2
        else:
            total_confidence = longtext_confidence

        return keyword_category, all_keywords, main_category, sub_category, total_confidence

    def _clean_long_text(self, long_text: str) -> str:
        """清理LONG_TEXT中的乱码字符和HTML转义"""
        if not long_text:
            return ""

        # 清理HTML转义字符
        text = long_text
        text = re.sub(r'u003c[^>]*u003e', '', text)  # 移除HTML标签转义
        text = re.sub(r'<[^>]*>', '', text)  # 移除HTML标签
        text = re.sub(r'-#x2f;', '/', text)  # 转换斜杠转义
        text = re.sub(r'u0027', "'", text)  # 转换单引号转义
        text = re.sub(r'&amp;', '&', text)  # 转换&符号转义
        text = re.sub(r'&lt;', '<', text)  # 转换<符号转义
        text = re.sub(r'&gt;', '>', text)  # 转换>符号转义
        text = re.sub(r'&quot;', '"', text)  # 转换双引号转义

        # 清理多余空格
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def get_folder_path(self, keyword_category: str, all_keywords: str, main_category: str, sub_category: str) -> str:
        """
        获取分类对应的文件夹路径，支持多关键字分类的目录结构
        
        Args:
            keyword_category: 主要关键字分类（如：合股、供股等）
            all_keywords: 所有匹配的关键字（如：合股+供股）
            main_category: 主分类 (清理后的LONG_TEXT)
            sub_category: 子分类 (清理后的LONG_TEXT)

        Returns:
            str: 文件夹路径
        """

        # 清理路径中的不安全字符
        def clean_path_name(name: str) -> str:
            if not name:
                return ""
            # 移除或替换文件系统不支持的字符
            name = re.sub(r'[<>:"/\\|?*]', '_', name)  # 替换不安全字符
            name = re.sub(r'\s+', ' ', name)  # 规范化空格
            name = name.strip('._- ')  # 移除首尾的特殊字符
            # 限制长度
            if len(name) > 80:
                name = name[:80] + "..."
            return name or "未知"

        # 分析LONG_TEXT结构，自动分配子目录
        def parse_longtext_structure(longtext: str) -> tuple:
            """解析LONG_TEXT的层级结构"""
            if not longtext:
                return ("", "")
                
            # 处理最常见的格式：一级分类 - [二级分类 / 三级分类]

            # 1. 简单单级分类（如：月報表、展示文件）
            if ' - [' not in longtext and '/' not in longtext:
                return (longtext, "")

            # 2. 带中括号的分类（如：公告及通告 - [董事名單和他們的地位和作用]）
            if ' - [' in longtext and ']' in longtext:
                parts = longtext.split(' - [', 1)
                level1 = parts[0].strip()
                remaining = parts[1].rstrip(']')

                # 检查是否有斜杠分隔的子分类
                if ' / ' in remaining:
                    sub_parts = remaining.split(' / ')
                    level2 = sub_parts[0].strip()
                    level3 = ' / '.join(sub_parts[1:]).strip()
                    return (level1, f"{level2}/{level3}")
                else:
                    return (level1, remaining.strip())

            # 3. 直接斜杠分隔（如：財務報表/環境、社會及管治資料）
            if '/' in longtext:
                parts = longtext.split('/', 1)
                level1 = parts[0].strip()
                level2 = parts[1].strip()
                return (level1, level2)

            # 4. 默认情况
            return (longtext, "")

        # 如果有关键字分类，使用关键字分类作为主目录
        if keyword_category:
            # 决定使用单一关键字还是复合关键字作为文件夹名
            if all_keywords and "+" in all_keywords:
                # 复合分类：使用主要关键字作为主文件夹，但在文件夹名中体现复合性质
                folder_display_name = f"{keyword_category}[{all_keywords}]"
            else:
                # 单一分类：直接使用关键字分类
                folder_display_name = keyword_category
            
            # 关键字分类/原主分类/子分类
            parsed_main, parsed_sub = parse_longtext_structure(main_category)
            
            clean_keyword = clean_path_name(folder_display_name)
            clean_main = clean_path_name(parsed_main)
            clean_sub = clean_path_name(parsed_sub)
            
            path_parts = [clean_keyword]
            if clean_main:
                path_parts.append(clean_main)
            if clean_sub:
                path_parts.append(clean_sub)
                
            return os.path.join(*path_parts)
        else:
            # 没有关键字分类，使用原有的分类结构
            parsed_main, parsed_sub = parse_longtext_structure(main_category)
            
            clean_main = clean_path_name(parsed_main)
            clean_sub = clean_path_name(parsed_sub)
            
            path_parts = [clean_main] if clean_main else ["其他"]
            if clean_sub:
                path_parts.append(clean_sub)
                
            return os.path.join(*path_parts)

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
            keyword_cat, all_keywords, main_cat, sub_cat, confidence = self.classify_announcement_enhanced(ann)
            
            if keyword_cat:
                # 显示所有匹配的关键字信息
                display_keyword = all_keywords if all_keywords and "+" in all_keywords else keyword_cat
                full_category = f"{display_keyword}/{main_cat}/{sub_cat}"
            else:
                full_category = f"{main_cat}/{sub_cat}"
                
            stats[full_category] = stats.get(full_category, 0) + 1

        return stats


class AnnouncementFilter:
    """公告过滤器 - 根据配置排除特定分类的公告"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化过滤器
        
        Args:
            config: 配置字典
        """
        self.excluded_categories = config.get('announcement_categories', {}).get('excluded_categories', [])

    def should_exclude(self, keyword_category: str, main_category: str, sub_category: str = None) -> bool:
        """
        检查是否应该排除该分类的公告
        
        Args:
            keyword_category: 关键字分类
            main_category: 主分类（通常是LONG_TEXT）
            sub_category: 子分类（可选）
            
        Returns:
            bool: True表示应该排除，False表示保留
        """
        # 检查主分类是否在排除列表中
        return main_category in self.excluded_categories

    def filter_announcements(self, announcements: List[Dict], classifier) -> Tuple[List[Dict], List[Dict]]:
        """
        过滤公告列表
        
        Args:
            announcements: 公告列表
            classifier: 分类器实例
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (保留的公告, 排除的公告)
        """
        included = []
        excluded = []

        for ann in announcements:
            keyword_cat, all_keywords, main_cat, sub_cat, confidence = classifier.classify_announcement_enhanced(ann)
            if self.should_exclude(keyword_cat, main_cat, sub_cat):
                excluded.append(ann)
            else:
                included.append(ann)

        return included, excluded


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

            # 处理环境变量替换
            self._replace_env_variables(config)

            # 验证配置文件结构
            self.validate_config(config)
            return config

        except Exception as e:
            logging.error(f"加载配置文件失败: {e}")
            print(f"配置文件加载失败，将使用默认配置: {e}")
            return self.get_default_config()

    def _replace_env_variables(self, config: Any) -> None:
        """递归替换配置中的环境变量"""
        import re

        if isinstance(config, dict):
            for key, value in config.items():
                if isinstance(value, str):
                    # 匹配 ${VAR_NAME} 格式的环境变量
                    pattern = r'\$\{([^}]+)\}'
                    matches = re.findall(pattern, value)
                    for env_var in matches:
                        env_value = os.environ.get(env_var, '')
                        if not env_value:
                            # 如果环境变量不存在，尝试使用默认值
                            if env_var == 'DB_PASSWORD':
                                # 临时使用原密码作为默认值，生产环境应该设置环境变量
                                env_value = '20251688Ma..'
                                logging.warning(f"环境变量 {env_var} 未设置，使用默认值")
                        value = value.replace(f'${{{env_var}}}', env_value)
                    config[key] = value
                elif isinstance(value, (dict, list)):
                    self._replace_env_variables(value)
        elif isinstance(config, list):
            for item in config:
                if isinstance(item, (dict, list)):
                    self._replace_env_variables(item)

    def get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {'settings': {'save_path': os.path.join(os.path.expanduser("~"), "Desktop"), 'filename_length': 220,
                             'language': 'zh', 'max_results': 500, 'verbose_logging': True,
                             'log_file': 'hkex_downloader.log'},
                'date_range': {'start_date': '2024-01-01', 'end_date': 'today'}, 'download_tasks': [],
                'advanced': {'retry_attempts': 3, 'request_delay': 1, 'timeout': 30, 'overwrite_existing': False,
                             'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36'}}

    def create_default_config(self):
        """创建默认配置文件"""
        default_config = self.get_default_config()
        default_config['download_tasks'] = [
            {'name': '默认下载任务', 'stock_code': '00081', 'start_date': '2024-01-01', 'end_date': 'today',
             'keywords': [], 'enabled': True}]

        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(default_config, f, default_flow_style=False, allow_unicode=True, indent=2)
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
        # 获取User-Agent，提供默认值避免None
        user_agent = self.config.get('advanced', 'user_agent') or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        self.headers = {"User-Agent": user_agent}
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # 设置超时和重试
        self.timeout = self.config.get('advanced', 'timeout', 30)
        self.retry_attempts = self.config.get('advanced', 'retry_attempts', 3)
        self.request_delay = self.config.get('advanced', 'request_delay', 1)

        # 初始化公告分类器
        self.classifier = AnnouncementClassifier(self.config.config)

    def get_stockid_and_name(self, stockcode: str) -> tuple[str, str]:
        """根据股票代码获取股票ID和公司名称"""
        # 从配置文件获取API端点
        api_config = self.config.get('api_endpoints') or {}
        base_url = api_config.get('base_url', 'https://www1.hkexnews.hk')
        stock_search = api_config.get('stock_search', '/search/prefix.do')
        market = api_config.get('market', 'SEHK')
        callback = api_config.get('callback_param', 'callback')

        logging.info(f"开始获取股票ID - 股票代码: {stockcode}")

        for attempt in range(self.retry_attempts):
            try:
                url = f'{base_url}{stock_search}?&callback={callback}&lang=ZH&type=A&name={stockcode}&market={market}&_={int(time.time()*1000)}'
                logging.info(f"股票ID查询URL: {url}")
                response = self.session.get(url, timeout=self.timeout)

                # 处理 JSONP 响应
                response_text = response.text.strip()
                if not response_text:
                    raise ValueError("Empty API response")

                if response_text.startswith('callback(') and response_text.endswith('});'):
                    # 动态提取JSON部分
                    start = response_text.find('(')
                    end = response_text.rfind(')')
                    data = response_text[start + 1:end]
                else:
                    raise ValueError(f"Unexpected response format: {response_text[:100]}...")

                data_json = json.loads(data)

                if 'stockInfo' not in data_json or not data_json['stockInfo']:
                    raise ValueError(f"未找到股票代码 {stockcode} 对应的信息")

                stock_info = data_json["stockInfo"][0]
                stockid = stock_info['stockId']
                stock_name = stock_info.get('name', '').strip()

                logging.info(f"股票代码 {stockcode} 对应的 StockID: {stockid}, 公司名称: {stock_name}")
                return stockid, stock_name

            except Exception as e:
                if attempt < self.retry_attempts - 1:
                    logging.warning(f"获取股票信息失败，第 {attempt + 1} 次重试: {e}")
                    time.sleep(self.request_delay)
                    continue
                else:
                    # 记录原始响应内容便于调试
                    if 'response' in locals():
                        logging.error(f"API响应状态: {response.status_code}, 响应头: {dict(response.headers)}")
                        logging.error(f"API响应前100字符: {response.text[:100]}")
                    raise Exception(f"获取股票 {stockcode} 信息失败: {str(e)}")
        return None

    def get_stockid(self, stockcode: str) -> str:
        """根据股票代码获取股票ID（保持向后兼容）"""
        stockid, _ = self.get_stockid_and_name(stockcode)
        return stockid

    def get_announcement_list(self, stockcode: str, start_date: datetime, end_date: datetime,
                              keywords: List[str] = None) -> tuple[List[Dict], str]:
        """获取公告列表和公司名称"""
        try:
            # 获取 stockId 和公司名称
            stockid, stock_name = self.get_stockid_and_name(stockcode)

            # 处理关键字
            search_keyword = keywords[-1] if keywords else ""

            # 准备日期格式
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")

            # 获取语言和最大结果数设置
            language = self.config.get('settings', 'language', 'zh')
            max_results = self.config.get('settings', 'max_results', 500)

            # 从配置文件获取API端点
            api_config = self.config.get('api_endpoints') or {}
            base_url = api_config.get('base_url', 'https://www1.hkexnews.hk')
            title_search = api_config.get('title_search', '/search/titleSearchServlet.do')
            market = api_config.get('market', 'SEHK')
            referer = api_config.get('referer', 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh')

            # 构建完整URL
            url = f'{base_url}{title_search}?sortDir=0&sortByOptions=DateTime&category=0&market={market}&stockId={stockid}&documentType=-1&fromDate={start_date_str}&toDate={end_date_str}&title={search_keyword}&searchType=0&t1code=-2&t2Gcode=-2&t2code=-2&rowRange={max_results}&lang={language}'

            logging.info(f"搜索URL: {url}")

            headers = {"sec-ch-ua-platform": "\"Windows\"", "x-requested-with": "XMLHttpRequest",
                       "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
                       "accept": "text/html, */*; q=0.01",
                       "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
                       "sec-ch-ua-mobile": "?0", "sec-fetch-site": "same-origin", "sec-fetch-mode": "cors",
                       "sec-fetch-dest": "empty", "referer": referer, "accept-encoding": "gzip, deflate, br, zstd",
                       "accept-language": "zh-CN,zh;q=0.9",
                       # "cookie": "TS38b16b21027=086f2721efab2000642dbe64e6deea82232cbbc6623ec72c04dbb365e5fabaffc676ede33e5ea917086e538f3c113000ddf73ed4c79657e7aa42a671f5d22a7baf96133d9aa7f533998a6e8a9ff9aa432321b18be489d59c7c54e761b51a3c42",
                       "priority": "u=0, i"}

            # 发送请求
            response = self.session.get(url, timeout=self.timeout, headers=headers)

            # 处理返回头的 set-cookie
            if 'set-cookie' in response.headers:
                self.session.headers.update({'cookie': response.headers['set-cookie']})
            if response.status_code != 200:
                raise Exception(f"无法连接到港交所网站 (状态码: {response.status_code})")

            # 处理响应数据
            try:
                data = response.text
                # 清理响应文本
                data = data.replace('"[{', '[{').replace('}]"', '}]').replace('\\', "").replace('u2013', "-").replace(
                    'u0026', "-")
                data_json = json.loads(data)
                # logging.info(f"API响应数据: {data_json}")
                if not data_json or 'result' not in data_json or not data_json['result']:
                    logging.warning(
                        f"未找到符合条件的公告 - 股票: {stockcode}, 日期: {start_date_str} 到 {end_date_str}")
                    return [], stock_name

                announcements = []
                
                # 处理result字段可能是字符串的情况
                result_data = data_json['result']
                if isinstance(result_data, str):
                    # 如果result是字符串，尝试解析为JSON数组
                    try:
                        if result_data.strip() == '' or result_data == '[]':
                            result_data = []
                        else:
                            result_data = json.loads(result_data)
                    except:
                        logging.warning(f"无法解析result字段: {result_data}")
                        result_data = []
                elif not isinstance(result_data, list):
                    logging.warning(f"result字段类型异常: {type(result_data)}")
                    result_data = []
                
                for item in result_data:
                    try:
                        # 获取标题
                        title = item['TITLE'].replace('/', "-")

                        # 获取 PDF 链接
                        pdflink = item['FILE_LINK']
                        # 从配置文件获取基础URL
                        api_config = self.config.get('api_endpoints') or {}
                        base_url = api_config.get('base_url', 'https://www1.hkexnews.hk')
                        pdf_link = base_url + pdflink

                        # 处理日期
                        anndate = item['DATE_TIME']
                        anndate = anndate[:10].replace('/', "-")
                        date_object = datetime.strptime(anndate, "%d-%m-%Y")
                        formatted_date = date_object.strftime("%Y-%m-%d")

                        # 在搜索时就进行分类，利用更多上下文信息
                        announcement_data = {'date': formatted_date, 'title': title, 'link': pdf_link,
                            'stock_code': stockcode, 'stock_name': stock_name, # 原始港交所数据，用于增强分类
                            'raw_data': item}

                        # 进行增强分类
                        if hasattr(self, 'classifier') and self.classifier.enabled:
                            keyword_category, all_keywords, main_category, sub_category, confidence = self.classifier.classify_announcement_enhanced(
                                announcement_data)
                            announcement_data.update({'keyword_category': keyword_category, 'all_keywords': all_keywords, 
                                'main_category': main_category, 'sub_category': sub_category,
                                'classification_confidence': confidence})

                        announcements.append(announcement_data)

                    except Exception as e:
                        logging.warning(f"处理公告项目时发生错误: {str(e)}")
                        continue

                logging.info(f"找到 {len(announcements)} 个符合条件的公告")
                return announcements, stock_name

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

            # 支持单个股票代码（字符串）和多个股票代码（列表）
            if isinstance(stockcode, list):
                return self._download_multiple_stocks(task, stockcode)
            else:
                return self._download_single_stock(task, stockcode)

        except Exception as e:
            raise Exception(f"下载过程中出现错误: {str(e)}")

    def _download_from_database(self, task: Dict[str, Any]) -> tuple[str, int]:
        """从数据库获取股票列表并下载"""
        query = task.get('query')

        # 获取数据库任务配置
        db_task_config = task.get('database_config', {})
        batch_size = db_task_config.get('batch_size', 50)
        delay_between_batches = db_task_config.get('delay_between_batches', 5)
        skip_on_error = db_task_config.get('skip_on_error', True)
        refresh_connection = db_task_config.get('refresh_connection', True)
        show_progress = db_task_config.get('show_progress', True)

        # 刷新数据库连接（如果配置了）
        if refresh_connection:
            logging.info("刷新数据库连接...")

        stock_codes = self.config.get_stocks_from_database(query)

        if not stock_codes:
            logging.warning("从数据库未获取到任何股票代码")
            return "", 0

        logging.info(f"从数据库获取到 {len(stock_codes)} 个活跃股票代码")
        if show_progress:
            logging.info(f"股票列表预览: {', '.join(stock_codes[:10])}{'...' if len(stock_codes) > 10 else ''}")

        total_downloaded = 0
        success_count = 0
        error_count = 0
        base_save_path = self.config.get('settings', 'save_path')

        # 分批处理股票
        for batch_start in range(0, len(stock_codes), batch_size):
            batch_end = min(batch_start + batch_size, len(stock_codes))
            batch_codes = stock_codes[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(stock_codes) + batch_size - 1) // batch_size

            logging.info(f"开始处理第 {batch_num}/{total_batches} 批股票 ({len(batch_codes)} 个)")

            for i, stock_code in enumerate(batch_codes, 1):
                global_index = batch_start + i
                try:
                    if show_progress:
                        logging.info(f"[{global_index}/{len(stock_codes)}] 处理股票: {stock_code}")

                    # 创建临时任务配置
                    stock_task = task.copy()
                    stock_task['stock_code'] = stock_code
                    stock_task.pop('from_database', None)
                    stock_task.pop('query', None)
                    stock_task.pop('database_config', None)

                    save_path, count = self._download_single_stock(stock_task, stock_code)
                    total_downloaded += count
                    success_count += 1

                    if count > 0:
                        logging.info(f"✓ 股票 {stock_code} 下载完成: {count} 个文件")
                    else:
                        logging.debug(f"- 股票 {stock_code} 无新文件")

                except Exception as e:
                    error_count += 1
                    if skip_on_error:
                        logging.warning(f"✗ 股票 {stock_code} 下载失败，跳过: {str(e)}")
                        continue
                    else:
                        logging.error(f"✗ 股票 {stock_code} 下载失败: {str(e)}")
                        raise

            # 批次间延迟
            if batch_num < total_batches and delay_between_batches > 0:
                logging.info(f"批次 {batch_num} 完成，等待 {delay_between_batches} 秒后继续...")
                time.sleep(delay_between_batches)

        # 输出统计信息
        logging.info(f"数据库任务完成！")
        logging.info(f"  总股票数: {len(stock_codes)}")
        logging.info(f"  成功处理: {success_count}")
        logging.info(f"  失败数量: {error_count}")
        logging.info(f"  下载文件: {total_downloaded} 个")

        return os.path.join(base_save_path, 'HKEX'), total_downloaded

    def _download_multiple_stocks(self, task: Dict[str, Any], stock_codes: list) -> tuple[str, int]:
        """下载多个股票的公告"""
        if not stock_codes:
            raise ValueError("股票代码列表为空")

        # 验证股票代码格式
        for i, code in enumerate(stock_codes):
            if not isinstance(code, str):
                raise ValueError(f"股票代码列表中第{i + 1}个元素必须为字符串，当前类型: {type(code)}")
            if not code.strip():
                raise ValueError(f"股票代码列表中第{i + 1}个元素不能为空")

        logging.info(f"开始批量下载任务: {task.get('name', '未命名任务')}")
        logging.info(f"股票代码列表: {', '.join(stock_codes)}")

        total_downloaded = 0
        success_count = 0
        error_count = 0
        base_save_path = self.config.get('settings', 'save_path')

        for i, stock_code in enumerate(stock_codes, 1):
            try:
                logging.info(f"[{i}/{len(stock_codes)}] 处理股票: {stock_code}")

                # 为每个股票调用单个股票下载方法
                save_path, count = self._download_single_stock(task, stock_code)

                if count > 0:
                    total_downloaded += count
                    success_count += 1
                    logging.info(f"✓ 股票 {stock_code} 下载完成: {count} 个文件")
                else:
                    success_count += 1
                    logging.info(f"✓ 股票 {stock_code} 无新文件")

            except Exception as e:
                error_count += 1
                logging.error(f"✗ 股票 {stock_code} 下载失败: {str(e)}")  # 继续处理下一个股票，不中断整个任务

        # 输出统计信息
        logging.info(f"批量下载任务完成！")
        logging.info(f"  总股票数: {len(stock_codes)}")
        logging.info(f"  成功处理: {success_count}")
        logging.info(f"  失败数量: {error_count}")
        logging.info(f"  下载文件: {total_downloaded} 个")

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

        # 获取公告列表和公司名称
        announcements, stock_name = self.get_announcement_list(stockcode, start_date, end_date, keywords)

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
                # 使用搜索时已经进行的分类，或进行新的分类
                if self.classifier.enabled:
                    # 优先使用搜索时的分类结果
                    if 'keyword_category' in ann and 'all_keywords' in ann and 'main_category' in ann and 'sub_category' in ann:
                        keyword_category = ann['keyword_category']
                        all_keywords = ann['all_keywords']
                        main_category = ann['main_category']
                        sub_category = ann['sub_category']
                        confidence = ann.get('classification_confidence', 0.7)

                        # 如果置信度较低，重新分类
                        if confidence < 0.8:
                            enhanced_keyword, enhanced_all_keywords, enhanced_main, enhanced_sub, new_confidence = self.classifier.classify_announcement_enhanced(
                                ann)
                            if new_confidence > confidence:
                                keyword_category, all_keywords, main_category, sub_category = enhanced_keyword, enhanced_all_keywords, enhanced_main, enhanced_sub
                    else:
                        # 兜底：使用增强分类（基于LONG_TEXT）
                        keyword_category, all_keywords, main_category, sub_category, confidence = self.classifier.classify_announcement_enhanced(ann)

                    category_path = self.classifier.get_folder_path(keyword_category, all_keywords, main_category, sub_category)
                    savepath = os.path.join(base_path, category_path)
                else:
                    savepath = base_path

                # 确保分类文件夹存在
                if not os.path.exists(savepath):
                    os.makedirs(savepath)
                    logging.info(f"创建分类目录: {savepath}")

                # 清理公司名称和公告标题中的特殊字符
                clean_stock_name = re.sub(r'[<>:"/\\|?*]', '-', stock_name)
                clean_title = re.sub(r'[<>:"/\\|?*]', '-', ann['title'])

                # 新的文件命名格式：时间——股票代码——公司名称-公告名称
                filename = f"{ann['date']}_{stockcode}_{clean_stock_name}_{clean_title[:filename_length]}.pdf"
                filepath = os.path.join(savepath, filename)

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
                    # 检查文件大小
                    content_size = len(response.content)
                    if content_size < 5120:  # 小于5KB的文件可能有问题
                        logging.warning(
                            f"⚠️  文件大小异常: {os.path.basename(filepath)} ({content_size}字节) - 可能下载失败或为错误页面")
                        # 检查内容是否为HTML错误页面
                        content_text = response.content.decode('utf-8', errors='ignore').lower()
                        if '<html' in content_text or 'error' in content_text or '404' in content_text:
                            logging.error(f"❌ 下载到错误页面: {ann['title']} - 跳过保存")
                            continue

                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    download_count += 1

                    # 显示文件大小信息
                    if content_size >= 1024 * 1024:  # >= 1MB
                        size_info = f"({content_size / (1024 * 1024):.2f}MB)"
                    elif content_size >= 1024:  # >= 1KB
                        size_info = f"({content_size / 1024:.1f}KB)"
                    else:
                        size_info = f"({content_size}字节)"

                    logging.info(f"✓ 成功下载: {os.path.basename(filepath)} {size_info}")
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

        logging.basicConfig(level=log_level, format=log_format, handlers=handlers)

    def create_parser(self) -> argparse.ArgumentParser:
        """创建命令行参数解析器"""
        parser = argparse.ArgumentParser(description='HKEX 公告下载器 - 命令行版本',
                                         formatter_class=argparse.RawDescriptionHelpFormatter, epilog="""
使用示例:
  %(prog)s                                    # 使用配置文件中的任务
  %(prog)s -s 00001                          # 下载单个股票的所有公告
  %(prog)s -s 00001 -k "财务报告"             # 下载指定关键字的公告
  %(prog)s -s 00001 --start 2024-01-01       # 指定开始日期
  %(prog)s --config my_config.yaml           # 使用指定的配置文件
  %(prog)s --check-config                    # 检查配置文件
            """)

        parser.add_argument('-c', '--config', default='config.yaml', help='配置文件路径 (默认: config.yaml)')

        parser.add_argument('-s', '--stock-code', help='股票代码 (5位数字)')

        parser.add_argument('-k', '--keywords', nargs='*', help='搜索关键字')

        parser.add_argument('--start', help='开始日期 (YYYY-MM-DD)')

        parser.add_argument('--end', help='结束日期 (YYYY-MM-DD 或 today)')

        parser.add_argument('--save-path', help='保存路径')

        parser.add_argument('--check-config', action='store_true', help='检查配置文件格式')

        parser.add_argument('--list-tasks', action='store_true', help='列出配置文件中的所有任务')

        parser.add_argument('--run-task', help='运行指定名称的任务')

        parser.add_argument('--async', dest='use_async', action='store_true', help='使用异步模式下载（大幅提升速度）')

        parser.add_argument('-v', '--verbose', action='store_true', help='启用详细输出')

        # 数据库相关参数
        parser.add_argument('--test-db', action='store_true', help='测试数据库连接')

        parser.add_argument('--db-stocks', action='store_true', help='从数据库获取所有股票代码并下载')

        parser.add_argument('--db-query', help='自定义数据库查询语句')

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
        task = {'name': '命令行任务', 'stock_code': args.stock_code,
                'start_date': args.start or config_manager.get('date_range', 'start_date', '2024-01-01'),
                'end_date': args.end or config_manager.get('date_range', 'end_date', 'today'),
                'keywords': args.keywords or [], 'enabled': True}

        # 如果指定了保存路径，临时更新配置
        if args.save_path:
            config_manager.config['settings']['save_path'] = args.save_path

        # 检查是否使用异步模式
        import os
        force_async = os.environ.get('HKEX_FORCE_ASYNC', '').lower() in ('true', '1', 'yes', 'on')
        if args.use_async or force_async:
            print("🚀 使用异步模式下载...")
            try:
                from async_downloader import run_async_download
                save_path, count, skipped = run_async_download(config_manager, task)
                if count > 0:
                    if skipped > 0:
                        print(f"\n✓ 异步下载完成！成功下载 {count} 个新文件，跳过 {skipped} 个已存在文件到 {save_path}")
                    else:
                        print(f"\n✓ 异步下载完成！成功下载 {count} 个文件到 {save_path}")
                elif skipped > 0:
                    print(f"\n⚠ 找到 {skipped} 个公告，但文件已存在，无需重新下载")
                else:
                    print("\n⚠ 未找到符合条件的公告")
            except ImportError:
                print("❌ 异步模式需要安装额外依赖：pip install aiohttp aiofiles tenacity tqdm")
                sys.exit(1)
            except Exception as e:
                print(f"\n✗ 异步下载失败: {e}")
                sys.exit(1)
        else:
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

    def run_config_tasks(self, config_manager: ConfigManager, use_async: bool = False):
        """运行配置文件中的任务"""
        tasks = config_manager.get('download_tasks') or []
        enabled_tasks = [t for t in tasks if isinstance(t, dict) and t.get('enabled', True)]

        if not enabled_tasks:
            print("配置文件中没有启用的任务")
            return

        total_downloaded = 0
        print(f"开始执行 {len(enabled_tasks)} 个启用的任务:\n")

        # 检查是否使用异步模式（仅命令行参数或环境变量）
        import os
        force_async = os.environ.get('HKEX_FORCE_ASYNC', '').lower() in ('true', '1', 'yes', 'on')

        if use_async or force_async:
            print("🚀 使用异步模式执行任务...")
            try:
                from async_downloader import run_async_download
                for i, task in enumerate(enabled_tasks, 1):
                    print(f"[{i}/{len(enabled_tasks)}] 执行任务: {task.get('name', '未命名任务')}")
                    try:
                        save_path, count, skipped = run_async_download(config_manager, task)
                        total_downloaded += count
                        if count > 0:
                            if skipped > 0:
                                print(f"✓ 任务完成！下载 {count} 个新文件，跳过 {skipped} 个已存在文件到 {save_path}\n")
                            else:
                                print(f"✓ 任务完成！下载 {count} 个文件到 {save_path}\n")
                        elif skipped > 0:
                            print(f"ℹ️ 找到 {skipped} 个公告，但文件已存在，无需重新下载\n")
                        else:
                            print("⚠ 未找到符合条件的公告\n")
                    except Exception as e:
                        print(f"✗ 任务失败: {e}\n")
                        continue
            except ImportError:
                print("❌ 异步模式需要安装额外依赖，回退到同步模式...")
                use_async = force_async = False

        if not (use_async or force_async):
            downloader = HKEXDownloader(config_manager)
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

    def run_named_task(self, task_name: str, config_manager: ConfigManager, use_async: bool = False):
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

        # 检查是否启用异步
        import os
        force_async = os.environ.get('HKEX_FORCE_ASYNC', '').lower() in ('true', '1', 'yes', 'on')
        if use_async or force_async:
            try:
                from async_downloader import run_async_download
                print(f"🚀 使用异步模式执行任务: {task_name}")
                save_path, count, skipped = run_async_download(config_manager, target_task)
                if count > 0:
                    if skipped > 0:
                        print(f"\n✓ 任务完成！成功下载 {count} 个新文件，跳过 {skipped} 个已存在文件到 {save_path}")
                    else:
                        print(f"\n✓ 任务完成！成功下载 {count} 个文件到 {save_path}")
                elif skipped > 0:
                    print(f"\n⚠ 找到 {skipped} 个公告，但文件已存在，无需重新下载")
                else:
                    print("\n⚠ 未找到符合条件的公告")
            except ImportError:
                print("❌ 异步模式需要安装额外依赖，回退到同步模式...")
                use_async = force_async = False
            except Exception as e:
                print(f"\n✗ 任务失败: {e}")
                sys.exit(1)

        if not (use_async or force_async):
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
        task = {'name': '数据库股票下载任务', 'from_database': True, 'query': args.db_query,  # 可能为None，使用默认查询
                'start_date': args.start or config_manager.get('date_range', 'start_date', '2024-01-01'),
                'end_date': args.end or config_manager.get('date_range', 'end_date', 'today'),
                'keywords': args.keywords or [], 'enabled': True}

        # 如果指定了保存路径，临时更新配置
        if args.save_path:
            config_manager.config['settings']['save_path'] = args.save_path

        # 检查是否使用异步模式
        import os
        force_async = os.environ.get('HKEX_FORCE_ASYNC', '').lower() in ('true', '1', 'yes', 'on')
        use_async = getattr(args, 'use_async', False) or force_async

        if use_async:
            try:
                from async_downloader import run_async_download
                print("🚀 使用异步模式从数据库获取股票列表并下载...")
                save_path, count, skipped = run_async_download(config_manager, task)
                if count > 0:
                    if skipped > 0:
                        print(f"\n✓ 数据库任务完成！成功下载 {count} 个新文件，跳过 {skipped} 个已存在文件到 {save_path}")
                    else:
                        print(f"\n✓ 数据库任务完成！成功下载 {count} 个文件到 {save_path}")
                elif skipped > 0:
                    print(f"\n⚠ 找到 {skipped} 个公告，但文件已存在，无需重新下载")
                else:
                    print("\n⚠ 未找到符合条件的公告")
            except ImportError:
                print("❌ 异步模式需要安装额外依赖，回退到同步模式...")
                use_async = force_async = False
            except Exception as e:
                print(f"\n✗ 数据库任务失败: {e}")
                sys.exit(1)

        if not (use_async or force_async):
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
            self.run_named_task(args.run_task, self.config_manager, use_async=getattr(args, 'use_async', False))
            return

        # 运行单个股票下载任务
        if args.stock_code:
            if not args.stock_code.isdigit() or len(args.stock_code) != 5:
                print("错误: 股票代码必须为5位数字")
                sys.exit(1)
            self.run_single_task(args, self.config_manager)
            return

        # 运行配置文件中的任务
        self.run_config_tasks(self.config_manager, use_async=getattr(args, 'use_async', False))


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
