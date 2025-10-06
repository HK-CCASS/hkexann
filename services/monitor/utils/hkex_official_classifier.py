#!/usr/bin/env python3
"""
HKEX官方3级分类解析器
集成港交所官方API分类数据，提供标准化的3级分类解析功能
"""

import json
import logging
import requests
import asyncio
import aiohttp
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import re

logger = logging.getLogger(__name__)


@dataclass
class HKEXClassification:
    """HKEX分类结果数据类"""
    level1_code: str = ""
    level1_name: str = ""
    level2_code: str = ""  # 二级分组代码
    level2_name: str = ""  # 二级分组名称
    level3_code: str = ""  # 三级分类代码
    level3_name: str = ""  # 三级分类名称
    confidence: float = 0.0
    full_path: str = ""
    classification_method: str = "hkex_official"
    announcement_count: int = 0


class HKEXOfficialClassifier:
    """HKEX官方3级分类解析器"""

    def __init__(self, config: Dict[str, Any]):
        """初始化分类器"""
        self.config = config
        self.cache_dir = Path("./cache/hkex_classification")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # API端点配置
        self.api_urls = {
            'doc_types': 'https://www.hkexnews.hk/ncms/script/eds/doc_c.json',
            'tier_one': 'https://www.hkexnews.hk/ncms/script/eds/tierone_c.json',
            'tier_two': 'https://www.hkexnews.hk/ncms/script/eds/tiertwo_c.json',
            'tier_two_groups': 'https://www.hkexnews.hk/ncms/script/eds/tiertwogrp_c.json'
        }

        # 缓存配置
        self.cache_lifetime_hours = 24  # 缓存24小时
        self.classification_data = {}
        self.classification_map = {}  # 名称到代码的映射
        self.reverse_map = {}  # 代码到名称的映射

        # 初始化状态
        self._initialized = False
        self._last_update = None

        # 获取分类配置
        classification_config = self.config.get('classification', {})
        data_source = classification_config.get('data_source', 'api')
        disable_online_update = classification_config.get('disable_online_update', False)

        # 确定数据源模式
        if data_source == 'csv' or disable_online_update:
            self.data_source_mode = 'csv'
            csv_file = classification_config.get('csv_classification_file', 'HKEX--分类信息-20250926.csv')
            logger.info(f"✅ HKEX官方3级分类解析器初始化完成 (CSV模式: {csv_file})")
        else:
            self.data_source_mode = 'api'
            logger.info("✅ HKEX官方3级分类解析器初始化完成 (API模式)")

    async def initialize(self) -> bool:
        """异步初始化分类器，加载分类数据"""
        try:
            await self._load_classification_data()
            self._build_classification_maps()
            self._initialized = True
            logger.info("✅ HKEX官方分类数据加载完成")
            return True
        except Exception as e:
            logger.error(f"❌ HKEX官方分类数据初始化失败: {e}")
            return False

    def _get_cache_path(self, data_type: str) -> Path:
        """获取缓存文件路径"""
        return self.cache_dir / f"hkex_{data_type}_cache.json"

    def _is_cache_valid(self, data_type: str) -> bool:
        """检查缓存是否有效"""
        cache_file = self._get_cache_path(data_type)
        if not cache_file.exists():
            return False

        try:
            cache_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            return datetime.now() - cache_time < timedelta(hours=self.cache_lifetime_hours)
        except:
            return False

    async def _fetch_classification_data(self, data_type: str) -> Optional[List]:
        """异步获取分类数据"""
        # 检查缓存
        cache_file = self._get_cache_path(data_type)
        if self._is_cache_valid(data_type):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.debug(f"✅ 从缓存加载 {data_type} 数据: {len(data)} 条")
                return data
            except Exception as e:
                logger.warning(f"缓存读取失败 {data_type}: {e}")

        # 从API获取数据
        url = self.api_urls.get(data_type)
        if not url:
            logger.error(f"未知数据类型: {data_type}")
            return None

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()

                        # 缓存数据
                        with open(cache_file, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)

                        logger.info(f"✅ 从API获取 {data_type} 数据: {len(data)} 条")
                        return data
                    else:
                        logger.error(f"API请求失败 {data_type}: HTTP {response.status}")
                        return None
        except Exception as e:
            logger.error(f"获取 {data_type} 数据失败: {e}")
            return None

    async def _load_classification_data(self):
        """加载所有分类数据"""
        # 检查配置，决定使用CSV还是API
        classification_config = self.config.get('classification', {})
        data_source = classification_config.get('data_source', 'api')
        disable_online_update = classification_config.get('disable_online_update', False)

        if data_source == 'csv' or disable_online_update:
            logger.info("🔄 从CSV文件加载HKEX官方分类数据...")
            await self._load_classification_data_from_csv()
        else:
            logger.info("🔄 从API加载HKEX官方分类数据...")
            await self._load_classification_data_from_api()

        self._last_update = datetime.now()

    async def _load_classification_data_from_csv(self):
        """从CSV文件加载分类数据"""
        classification_config = self.config.get('classification', {})
        csv_file = classification_config.get('csv_classification_file', 'HKEX--分类信息-20250926.csv')

        # 如果是相对路径，转换为绝对路径
        if not Path(csv_file).is_absolute():
            csv_file = Path.cwd() / csv_file

        if not Path(csv_file).exists():
            logger.error(f"❌ CSV分类文件不存在: {csv_file}")
            # 初始化空数据
            self.classification_data = {
                'doc_types': [],
                'tier_one': [],
                'tier_two': [],
                'tier_two_groups': []
            }
            return

        try:
            # 初始化数据结构
            tier_one_data = {}  # code -> {code, name}
            tier_two_groups_data = {}  # code -> {code, name, t1code}
            tier_two_data = {}  # code -> {code, name, t2Gcode, t1code}

            # 读取CSV文件
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                row_count = 0

                for row in reader:
                    row_count += 1

                    # 提取各级分类信息
                    level1_code = row.get('1级分类代码', '').strip()
                    level1_name = row.get('1级分类名称', '').strip()
                    level2_code = row.get('2级分组代码', '').strip()
                    level2_name = row.get('2级分组名称', '').strip()
                    level3_code = row.get('3级分类代码', '').strip()
                    level3_name = row.get('3级分类名称', '').strip()
                    announcement_count = row.get('公告数量', '0').strip()

                    # 添加一级分类
                    if level1_code and level1_name:
                        tier_one_data[level1_code] = {
                            'code': level1_code,
                            'name': level1_name
                        }

                    # 添加二级分组
                    if level2_code and level2_name and level1_code:
                        tier_two_groups_data[level2_code] = {
                            'code': level2_code,
                            'name': level2_name,
                            't1code': level1_code
                        }

                    # 添加三级分类
                    if level3_code and level3_name and level2_code and level1_code:
                        tier_two_data[level3_code] = {
                            'code': level3_code,
                            'name': level3_name,
                            't2Gcode': level2_code,
                            't1code': level1_code,
                            'announcement_count': int(announcement_count) if announcement_count.isdigit() else 0
                        }

            # 转换为API格式的数据结构
            self.classification_data = {
                'doc_types': [],  # CSV中不包含doc_types信息，保持空
                'tier_one': list(tier_one_data.values()),
                'tier_two': list(tier_two_data.values()),
                'tier_two_groups': list(tier_two_groups_data.values())
            }

            logger.info(f"✅ 从CSV文件加载分类数据完成:")
            logger.info(f"  - 文件: {csv_file}")
            logger.info(f"  - 总行数: {row_count}")
            logger.info(f"  - 一级分类: {len(tier_one_data)} 条")
            logger.info(f"  - 二级分组: {len(tier_two_groups_data)} 条")
            logger.info(f"  - 三级分类: {len(tier_two_data)} 条")

        except Exception as e:
            logger.error(f"❌ 读取CSV分类文件失败: {e}")
            # 初始化空数据
            self.classification_data = {
                'doc_types': [],
                'tier_one': [],
                'tier_two': [],
                'tier_two_groups': []
            }

    async def _load_classification_data_from_api(self):
        """从API加载分类数据（原有逻辑）"""
        # 并行获取所有数据
        tasks = [
            self._fetch_classification_data('doc_types'),
            self._fetch_classification_data('tier_one'),
            self._fetch_classification_data('tier_two'),
            self._fetch_classification_data('tier_two_groups')
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果
        data_types = ['doc_types', 'tier_one', 'tier_two', 'tier_two_groups']
        for i, (data_type, result) in enumerate(zip(data_types, results)):
            if isinstance(result, Exception):
                logger.error(f"获取 {data_type} 数据失败: {result}")
                self.classification_data[data_type] = []
            else:
                self.classification_data[data_type] = result or []

    def _build_classification_maps(self):
        """构建分类映射表"""
        logger.info("🔄 构建分类映射表...")

        # 构建一级分类映射
        tier_one_data = self.classification_data.get('tier_one', [])
        self.tier_one_map = {}  # code -> name
        self.tier_one_reverse = {}  # name -> code

        for item in tier_one_data:
            # 处理JSON对象格式的数据
            if isinstance(item, dict):
                code = item.get('code', '')
                name = item.get('name', '')
                if code and name:
                    self.tier_one_map[code] = name
                    self.tier_one_reverse[name] = code
            # 处理数组格式的数据（向后兼容）
            elif isinstance(item, list) and len(item) >= 2:
                code, name = item[0], item[1]
                self.tier_one_map[code] = name
                self.tier_one_reverse[name] = code

        # 构建二级分组映射
        tier_two_groups_data = self.classification_data.get('tier_two_groups', [])
        self.tier_two_groups_map = {}  # code -> (name, parent_code)
        self.tier_two_groups_reverse = {}  # name -> code

        for item in tier_two_groups_data:
            # 处理JSON对象格式的数据
            if isinstance(item, dict):
                code = item.get('code', '')
                name = item.get('name', '')
                parent_code = item.get('t1code', '')  # 注意字段名为 t1code
                if code and name and parent_code:
                    self.tier_two_groups_map[code] = (name, parent_code)
                    self.tier_two_groups_reverse[name] = code
            # 处理数组格式的数据（向后兼容）
            elif isinstance(item, list) and len(item) >= 3:
                code, name, parent_code = item[0], item[1], item[2]
                self.tier_two_groups_map[code] = (name, parent_code)
                self.tier_two_groups_reverse[name] = code

        # 构建三级分类映射 (实际是tier_two数据)
        tier_two_data = self.classification_data.get('tier_two', [])
        self.tier_three_map = {}  # code -> (name, group_code, tier_one_code)
        self.tier_three_reverse = {}  # name -> code

        for item in tier_two_data:
            # 处理JSON对象格式的数据
            if isinstance(item, dict):
                code = item.get('code', '')
                name = item.get('name', '')
                group_code = item.get('t2Gcode', '')  # 注意字段名为 t2Gcode
                tier_one_code = item.get('t1code', '')  # 注意字段名为 t1code
                if code and name and group_code and tier_one_code:
                    self.tier_three_map[code] = (name, group_code, tier_one_code)
                    self.tier_three_reverse[name] = code
            # 处理数组格式的数据（向后兼容）
            elif isinstance(item, list) and len(item) >= 5:
                group_code, count, code, name, tier_one_code = item
                self.tier_three_map[code] = (name, group_code, tier_one_code)
                self.tier_three_reverse[name] = code

        logger.info(f"✅ 分类映射构建完成: 一级({len(self.tier_one_map)}), 二级分组({len(self.tier_two_groups_map)}), 三级({len(self.tier_three_map)})")

    def _match_classification_by_title(self, title: str) -> HKEXClassification:
        """基于标题匹配分类 (智能模糊匹配)"""
        if not title or not self._initialized:
            return HKEXClassification()

        title_clean = re.sub(r'\s+', '', title.lower())  # 清理标题
        best_match = HKEXClassification()
        max_confidence = 0.0

        # 预处理标题，提取关键词
        title_keywords = self._extract_keywords(title_clean)

        # 首先尝试三级分类匹配 (最精确)
        for name, code in self.tier_three_reverse.items():
            name_clean = re.sub(r'\s+', '', name.lower())

            # 使用新的相似度计算方法
            confidence = self._calculate_similarity(title_keywords, name)

            if confidence > 0 and confidence > max_confidence:
                max_confidence = confidence
                # 获取完整分类信息
                name_full, group_code, tier_one_code = self.tier_three_map[code]
                group_name, _ = self.tier_two_groups_map.get(group_code, ("", ""))
                tier_one_name = self.tier_one_map.get(tier_one_code, "")

                best_match = HKEXClassification(
                    level1_code=tier_one_code,
                    level1_name=tier_one_name,
                    level2_code=group_code,
                    level2_name=group_name,
                    level3_code=code,
                    level3_name=name_full,
                    confidence=confidence,
                    full_path=f"{tier_one_name}/{group_name}/{name_full}",
                    classification_method="hkex_official"
                )

        # 如果三级匹配不够好，尝试二级分组匹配
        if max_confidence < 0.8:
            for name, code in self.tier_two_groups_reverse.items():
                name_clean = re.sub(r'\s+', '', name.lower())

                # 使用新的相似度计算方法
                confidence = self._calculate_similarity(title_keywords, name)

                if confidence > 0 and confidence > max_confidence:
                    max_confidence = confidence
                    # 获取完整分类信息
                    name_full, tier_one_code = self.tier_two_groups_map[code]
                    tier_one_name = self.tier_one_map.get(tier_one_code, "")

                    best_match = HKEXClassification(
                        level1_code=tier_one_code,
                        level1_name=tier_one_name,
                        level2_code=code,
                        level2_name=name_full,
                        level3_code="",
                        level3_name="",
                        confidence=confidence,
                        full_path=f"{tier_one_name}/{name_full}",
                        classification_method="hkex_official"
                    )

        # 最后尝试一级分类匹配
        if max_confidence < 0.6:
            for name, code in self.tier_one_reverse.items():
                name_clean = re.sub(r'\s+', '', name.lower())

                # 使用新的相似度计算方法
                confidence = self._calculate_similarity(title_keywords, name)

                if confidence > 0 and confidence > max_confidence:
                    max_confidence = confidence

                    best_match = HKEXClassification(
                        level1_code=code,
                        level1_name=name,
                        level2_code="",
                        level2_name="",
                        level3_code="",
                        level3_name="",
                        confidence=confidence,
                        full_path=name,
                        classification_method="hkex_official"
                    )

        return best_match

    def _has_keyword_overlap(self, text1: str, text2: str) -> bool:
        """检查两个文本是否有关键词重叠 - 改进算法"""
        # 简单的关键词重叠检测
        words1 = set(re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', text1))
        words2 = set(re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', text2))

        # 过滤掉太短的词
        words1 = {w for w in words1 if len(w) >= 2}
        words2 = {w for w in words2 if len(w) >= 2}

        if not words1 or not words2:
            return False

        # 计算重叠度 - 放宽条件
        overlap = len(words1.intersection(words2))
        union = len(words1.union(words2))

        # 如果有重叠，就认为匹配，降低阈值到10%
        # 或者如果重叠词的数量>=2，也认为匹配
        return overlap > 0 and (overlap / union >= 0.1 or overlap >= 2)

    def _extract_keywords(self, text: str) -> List[str]:
        """从文本中提取关键词"""
        # 基本的中文分词
        words = re.findall(r'[\u4e00-\u9fff]+', text)

        # 过滤掉过短的词
        keywords = [word for word in words if len(word) >= 2]

        # 智能分词：识别特定词汇
        specific_terms = ['修訂', '協議', '通知', '信函', '申請', '表格', '中期', '報告', '業績', '公告', '股東', '發行', '認購',
                         '一般授權', '新股份', '變更', '非登記', '致股東', '通知信函', '申請表格']

        for term in specific_terms:
            if term in text:
                keywords.append(term)

        # 去重并过滤掉重复的短词
        seen = set()
        filtered_keywords = []
        for keyword in keywords:
            if len(keyword) >= 2 and keyword not in seen:
                seen.add(keyword)
                filtered_keywords.append(keyword)

        return filtered_keywords

    def _calculate_similarity(self, title_keywords: List[str], category_name: str) -> float:
        """计算标题关键词与分类名的相似度"""
        category_clean = re.sub(r'\s+', '', category_name.lower())

        # 精确匹配 - 检查分类名是否完全包含在标题中
        if category_clean in ''.join(title_keywords):
            return 1.0

        # 包含匹配 - 检查分类名是否包含在标题中
        if category_clean in title_keywords[0] if title_keywords else '':
            return 0.9

        # 智能匹配：检查分类名中的关键词汇是否在标题中出现
        category_words = re.findall(r'[\u4e00-\u9fff]+', category_clean)
        title_word_set = set(title_keywords)

        # 计算匹配的词汇数量
        matched_words = []
        for cat_word in category_words:
            if cat_word in title_word_set:
                matched_words.append(cat_word)

        if len(matched_words) > 0:
            # 如果匹配了多个词汇，给更高分
            if len(matched_words) >= 2:
                return 0.8
            # 如果匹配了重要词汇（如"一般授權", "發行", "股份"），给中高分
            elif any(len(word) >= 3 for word in matched_words):
                return 0.6
            else:
                return 0.4

        # 模糊匹配：检查是否有部分词汇匹配
        for cat_word in category_words:
            for title_word in title_keywords:
                if cat_word in title_word or title_word in cat_word:
                    return 0.3

        return 0.0

    def _parse_long_text_classification(self, long_text: str) -> HKEXClassification:
        """
        直接解析LONG_TEXT字段中的HKEX官方分类信息

        Args:
            long_text: LONG_TEXT字段内容，如 '公告及通告 - [配售 -#x2f; 根據一般性授權發行股份]'

        Returns:
            HKEXClassification: 解析出的分类结果，置信度为1.0
        """
        if not long_text:
            return HKEXClassification()

        # 清理HTML实体编码
        cleaned_text = long_text.replace('-#x2f;', '/').replace('u003cbr/u003e', '')

        # 解析格式: "一级分类 - [二级分类]" 或 "一级分类-#x2f;二级分类 - [三级分类]"
        # 匹配模式: 一级分类 - [具体分类]
        pattern = r'^([^-]+?)\s*-\s*\[([^\]]+)\]'
        match = re.match(pattern, cleaned_text)

        if match:
            level1 = match.group(1).strip()
            level2_3 = match.group(2).strip()

            # 如果二级分类中包含斜杠，可能是 "二级/三级" 格式
            if '/' in level2_3:
                parts = level2_3.split('/', 1)
                level2 = parts[0].strip()
                level3 = parts[1].strip() if len(parts) > 1 else ""
            else:
                level2 = level2_3
                level3 = ""

            # 构建完整路径
            if level3:
                full_path = f"{level1}/{level2}/{level3}"
            else:
                full_path = f"{level1}/{level2}"

            # 返回高置信度分类结果
            return HKEXClassification(
                level1_code="PARSED",  # 标记为解析获得
                level1_name=level1,
                level2_code="PARSED",
                level2_name=level2,
                level3_code="PARSED" if level3 else "",
                level3_name=level3,
                confidence=1.0,  # 直接解析，最高置信度
                full_path=full_path,
                classification_method="long_text_parsing"
            )

        return HKEXClassification()

    def classify_announcement(self, announcement: Dict[str, Any]) -> HKEXClassification:
        """
        对公告进行官方HKEX 3级分类

        Args:
            announcement: 公告信息字典

        Returns:
            HKEXClassification: 分类结果
        """
        if not self._initialized:
            logger.warning("分类器未初始化，返回空分类")
            return HKEXClassification()

        title = announcement.get('TITLE', announcement.get('title', ''))

        # 🚀 优先尝试解析LONG_TEXT字段（直接包含HKEX官方分类信息）
        long_text = announcement.get('LONG_TEXT', '')
        if long_text:
            classification = self._parse_long_text_classification(long_text)
            if classification.confidence > 0:
                logger.debug(f"🏷️ HKEX官方分类(LONG_TEXT解析): '{title[:30]}...' → {classification.full_path} (置信度: {classification.confidence:.2f})")
                return classification

        # 回退：如果LONG_TEXT解析失败，尝试SHORT_TEXT
        short_text = announcement.get('SHORT_TEXT', '')
        if short_text:
            classification = self._parse_long_text_classification(short_text)
            if classification.confidence > 0:
                logger.debug(f"🏷️ HKEX官方分类(SHORT_TEXT解析): '{title[:30]}...' → {classification.full_path} (置信度: {classification.confidence:.2f})")
                return classification

        # 最后回退：基于标题进行智能匹配（原有逻辑）
        if title:
            classification = self._match_classification_by_title(title)
            logger.debug(f"🏷️ HKEX官方分类(标题匹配): '{title[:30]}...' → {classification.full_path} (置信度: {classification.confidence:.2f})")
            return classification

        return HKEXClassification()

    def get_classification_stats(self) -> Dict[str, Any]:
        """获取分类器统计信息"""
        if not self._initialized:
            return {"status": "not_initialized"}

        return {
            "status": "initialized",
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "data_counts": {
                "doc_types": len(self.classification_data.get('doc_types', [])),
                "tier_one": len(self.classification_data.get('tier_one', [])),
                "tier_two_groups": len(self.classification_data.get('tier_two_groups', [])),
                "tier_three": len(self.classification_data.get('tier_two', []))  # 注意这里是tier_two数据
            },
            "mapping_counts": {
                "tier_one_mappings": len(self.tier_one_map),
                "tier_two_group_mappings": len(self.tier_two_groups_map),
                "tier_three_mappings": len(self.tier_three_map)
            }
        }

    async def refresh_classification_data(self) -> bool:
        """刷新分类数据"""
        try:
            logger.info("🔄 刷新HKEX官方分类数据...")

            # 清除缓存
            for data_type in self.api_urls.keys():
                cache_file = self._get_cache_path(data_type)
                if cache_file.exists():
                    cache_file.unlink()

            # 重新加载数据
            await self._load_classification_data()
            self._build_classification_maps()

            logger.info("✅ HKEX官方分类数据刷新完成")
            return True
        except Exception as e:
            logger.error(f"❌ 刷新分类数据失败: {e}")
            return False


# 测试函数
async def test_hkex_classifier():
    """测试HKEX官方分类器"""
    import yaml

    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    classifier = HKEXOfficialClassifier(config)

    # 初始化
    if not await classifier.initialize():
        print("初始化失败")
        return

    # 测试公告
    test_announcements = [
        {'TITLE': '須予披露的交易'},
        {'TITLE': '中期業績公告'},
        {'TITLE': '股份回購'},
        {'TITLE': '暫停買賣'},
        {'TITLE': '復牌'},
        {'TITLE': '股本重組'},
        {'TITLE': '供股'},
    ]

    for ann in test_announcements:
        result = classifier.classify_announcement(ann)
        print(f"标题: {ann['TITLE']}")
        print(f"  一级: {result.level1_code} - {result.level1_name}")
        print(f"  二级: {result.level2_code} - {result.level2_name}")
        print(f"  三级: {result.level3_code} - {result.level3_name}")
        print(f"  置信度: {result.confidence:.2f}")
        print(f"  路径: {result.full_path}")
        print()


if __name__ == "__main__":
    asyncio.run(test_hkex_classifier())