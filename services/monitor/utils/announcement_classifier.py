#!/usr/bin/env python3
"""
公告智能分类器模块
从main.py提取的AnnouncementClassifier，用于实时监听系统
已集成HKEX官方3级分类系统 (2025-09-26更新)
"""

import os
import re
import logging
import asyncio
from typing import Dict, List, Tuple, Any, Optional
from .hkex_official_classifier import HKEXOfficialClassifier, HKEXClassification

logger = logging.getLogger(__name__)


class AnnouncementClassifier:
    """
    公告智能分类器 - 混合分类引擎
    集成HKEX官方3级分类系统和关键字分类系统
    """

    def __init__(self, config: Dict[str, Any]):
        """初始化分类器"""
        self.config = config
        self.categories = config.get('announcement_categories', {})
        self.enabled = self.categories.get('enabled', True)

        # 加载关键字配置
        self.keyword_config = config.get('common_keywords', {})

        # 🆕 初始化HKEX官方分类器
        self.hkex_classifier = HKEXOfficialClassifier(config)
        self.hkex_classifier_initialized = False

        # 分类方法配置
        self.use_hkex_official = config.get('classification', {}).get('use_hkex_official', True)
        self.fallback_to_keyword = config.get('classification', {}).get('fallback_to_keyword', True)
        self.hkex_confidence_threshold = config.get('classification', {}).get('hkex_confidence_threshold', 0.6)

        # 初始化繁简体转换器
        try:
            import opencc
            self.t2s_converter = opencc.OpenCC('t2s')  # 繁体转简体
            self.s2t_converter = opencc.OpenCC('s2t')  # 简体转繁体
            self.opencc_available = True
            logger.info("✅ OpenCC繁简体转换器初始化成功")
        except ImportError:
            logger.warning("⚠️ OpenCC库未安装，繁简体转换功能将被禁用")
            self.opencc_available = False
            self.t2s_converter = None
            self.s2t_converter = None

    async def initialize_hkex_classifier(self) -> bool:
        """异步初始化HKEX官方分类器"""
        if not self.use_hkex_official:
            logger.info("⚠️ HKEX官方分类器已禁用")
            return False

        try:
            if await self.hkex_classifier.initialize():
                self.hkex_classifier_initialized = True
                logger.info("✅ HKEX官方3级分类器初始化成功")
                return True
            else:
                logger.error("❌ HKEX官方分类器初始化失败，将使用关键字分类作为后备")
                return False
        except Exception as e:
            logger.error(f"❌ HKEX官方分类器初始化异常: {e}")
            return False

    def _convert_text(self, text: str) -> List[str]:
        """将文本转换为繁简体版本"""
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
                logger.debug(f"繁简体转换失败: {e}")

        return variants

    def _match_keyword_category(self, title: str) -> Tuple[str, str, float]:
        """根据公告标题匹配关键字分类"""
        if not self.keyword_config or not title:
            return "", "", 0.0
        
        # 将标题转换为繁简体版本进行匹配
        title_variants = self._convert_text(title.lower())
        
        matched_keywords = []  # 存储所有匹配的关键字信息
        self._last_all_keywords = ""  # 保存所有匹配的关键字，供外部使用
        
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
        
        # 保存所有匹配的关键字，供外部使用
        self._last_all_keywords = all_keywords_str
        
        # 计算综合置信度
        max_confidence = max(match['confidence'] * match['weight'] for match in matched_keywords)
        
        return primary_category, all_keywords_str, max_confidence

    def get_priority_level(self, priority: int) -> str:
        """根据优先级返回等级标识"""
        if priority >= 85:
            return "🚨特高优先级"
        elif priority >= 70:
            return "🔴高优先级"
        elif priority >= 55:
            return "🟡中优先级"
        else:
            return "🟢低优先级"

    def get_folder_path(self, keyword_category: str, all_keywords: str, 
                       main_category: str, priority: int = 0) -> str:
        """生成智能分类的文件夹路径"""
        
        def clean_path_name(name: str) -> str:
            """清理路径名称，移除特殊字符"""
            if not name:
                return ""
            # 移除路径中的特殊字符，但保留中文字符
            cleaned = re.sub(r'[<>:"/\\|?*]', '-', name)
            # 移除多余的空格和特殊字符
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            return cleaned
        
        # 如果有关键字分类
        if keyword_category:
            # 如果有多个关键字匹配，使用复合分类文件夹
            if all_keywords and "+" in all_keywords:
                # 使用所有匹配的关键字作为复合文件夹名
                folder_display_name = all_keywords.replace("+", "_")  # 用下划线连接多个分类
            else:
                # 只有单一分类，直接使用
                folder_display_name = keyword_category
            
            clean_keyword = clean_path_name(folder_display_name)
            
            return clean_keyword  # 返回分类名称（可能是复合的）
        else:
            # 没有关键字分类，使用默认的"其他"分类
            return "其他"  # 简化为直接返回"其他"

    def classify_announcement(self, announcement: Dict[str, Any]) -> Tuple[str, str, int, float, Optional[HKEXClassification]]:
        """
        对公告进行智能分类 - 混合分类模式
        优先使用HKEX官方3级分类，如果置信度不足则回退到关键字分类

        Args:
            announcement: 公告信息字典，包含TITLE等字段

        Returns:
            Tuple[category_path, keyword_category, priority, confidence, hkex_classification]
        """
        if not self.enabled:
            return "其他", "", 0, 0.0, None

        title = announcement.get('TITLE', announcement.get('title', ''))
        if not title:
            return "其他", "", 0, 0.0, None

        hkex_classification = None
        primary_category_path = ""
        primary_keyword_category = ""
        primary_priority = 0
        primary_confidence = 0.0

        # 🚀 Step 1: 尝试使用HKEX官方3级分类
        if self.use_hkex_official and self.hkex_classifier_initialized:
            try:
                hkex_classification = self.hkex_classifier.classify_announcement(announcement)

                # 如果官方分类有效且置信度足够高
                if (hkex_classification.confidence >= self.hkex_confidence_threshold and
                    hkex_classification.level3_name):
                    # 使用官方分类结果
                    primary_category_path = hkex_classification.level3_name
                    primary_keyword_category = f"HKEX_{hkex_classification.level1_code}"
                    primary_priority = 95  # 官方分类给予最高优先级
                    primary_confidence = hkex_classification.confidence

                    logger.debug(f"🏷️ 使用HKEX官方分类: '{title[:30]}...' → {hkex_classification.full_path} (置信度: {hkex_classification.confidence:.2f})")

                    return primary_category_path, primary_keyword_category, primary_priority, primary_confidence, hkex_classification
            except Exception as e:
                logger.warning(f"⚠️ HKEX官方分类失败: {e}")

        # 🔄 Step 2: 回退到关键字分类（原有逻辑）
        if self.fallback_to_keyword:
            keyword_category, all_keywords, confidence = self._match_keyword_category(title)

            # 获取优先级
            priority = 0
            if keyword_category:
                for category_key, category_config in self.keyword_config.items():
                    if isinstance(category_config, dict):
                        folder_name = category_config.get('folder_name', category_key)
                        if folder_name == keyword_category:
                            priority = category_config.get('priority', 0)
                            break

            # 生成分类路径
            category_path = self.get_folder_path(keyword_category, all_keywords, "", priority)

            logger.debug(f"📊 使用关键字分类: 标题='{title[:30]}...' → 类别='{keyword_category}' → 路径='{category_path}' → 优先级={priority}")

            return category_path, keyword_category, priority, confidence, hkex_classification

        # 🚫 Step 3: 无分类结果
        logger.debug(f"⚠️ 无法分类: '{title[:30]}...'")
        return "其他", "", 0, 0.0, hkex_classification

    def get_classification_stats(self, announcements: List[Dict]) -> Dict[str, int]:
        """获取分类统计信息"""
        stats = {}
        for ann in announcements:
            category_path, keyword_category, priority, confidence, hkex_classification = self.classify_announcement(ann)
            primary_category = keyword_category if keyword_category else "其他"
            stats[primary_category] = stats.get(primary_category, 0) + 1
        return stats


# 用于测试的函数
async def test_classifier():
    """测试分类器功能（支持异步初始化）"""
    import yaml

    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    classifier = AnnouncementClassifier(config)

    # 🆕 异步初始化HKEX官方分类器
    await classifier.initialize_hkex_classifier()

    # 测试公告
    test_announcements = [
        {'TITLE': '建議股本重組及供股'},
        {'TITLE': '暫停買賣'},
        {'TITLE': '委任獨立非執行董事'},
        {'TITLE': '中期業績公告'},
        {'TITLE': '須予披露的交易收購'},
    ]

    for ann in test_announcements:
        category_path, keyword_category, priority, confidence, hkex_classification = classifier.classify_announcement(ann)
        priority_level = classifier.get_priority_level(priority)
        print(f"标题: {ann['TITLE']}")
        print(f"  关键字分类: {keyword_category} | 优先级: {priority} ({priority_level}) | 置信度: {confidence:.2f}")
        print(f"  分类路径: {category_path}")

        # 🆕 显示HKEX官方分类信息
        if hkex_classification and hkex_classification.confidence > 0:
            print(f"  🏷️ HKEX官方分类:")
            print(f"    一级: {hkex_classification.level1_code} - {hkex_classification.level1_name}")
            print(f"    二级: {hkex_classification.level2_code} - {hkex_classification.level2_name}")
            print(f"    三级: {hkex_classification.level3_code} - {hkex_classification.level3_name}")
            print(f"    置信度: {hkex_classification.confidence:.2f}")
            print(f"    完整路径: {hkex_classification.full_path}")
        print()


if __name__ == "__main__":
    asyncio.run(test_classifier())
