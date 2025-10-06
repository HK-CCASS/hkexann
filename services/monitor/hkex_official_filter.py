"""
HKEX官方分类专用过滤器
只保留能够通过HKEX官方3级分类的公告，其他过滤机制暂时禁用
"""

import asyncio
import logging
from typing import List, Dict, Any

from .utils.announcement_classifier import AnnouncementClassifier

logger = logging.getLogger(__name__)


class HKEXOfficialFilter:
    """
    HKEX官方分类专用过滤器
    只允许通过HKEX官方分类系统且置信度满足要求的公告通过
    """

    def __init__(self, config: dict):
        """
        初始化HKEX官方分类过滤器

        Args:
            config: 完整的配置字典
        """
        self.config = config

        # 初始化分类器（复用现有的AnnouncementClassifier）
        self.classifier = AnnouncementClassifier(config)
        self.classifier_initialized = False

        # HKEX官方分类配置
        self.hkex_confidence_threshold = config.get('classification', {}).get('hkex_confidence_threshold', 0.6)
        self.use_hkex_official = config.get('classification', {}).get('use_hkex_official', True)

        logger.info("HKEX官方分类过滤器初始化完成")
        logger.info(f"  置信度阈值: {self.hkex_confidence_threshold}")
        logger.info(f"  官方分类开关: {'启用' if self.use_hkex_official else '禁用'}")

    async def initialize(self) -> bool:
        """异步初始化分类器"""
        if not self.use_hkex_official:
            logger.warning("⚠️ HKEX官方分类已禁用，将不会过滤任何公告")
            return False

        try:
            success = await self.classifier.initialize_hkex_classifier()
            if success:
                self.classifier_initialized = True
                logger.info("✅ HKEX官方分类过滤器初始化成功")
                return True
            else:
                logger.error("❌ HKEX官方分类器初始化失败")
                return False
        except Exception as e:
            logger.error(f"❌ HKEX官方分类器初始化异常: {e}")
            return False

    async def filter_announcements(self, raw_announcements: List[Dict]) -> List[Dict]:
        """
        使用HKEX官方分类过滤公告

        Args:
            raw_announcements: 原始公告列表

        Returns:
            只包含通过HKEX官方分类的公告列表
        """
        if not raw_announcements:
            return []

        if not self.use_hkex_official:
            logger.warning("⚠️ HKEX官方分类已禁用，返回所有公告")
            return raw_announcements

        if not self.classifier_initialized:
            logger.error("❌ HKEX官方分类器未初始化，返回空列表")
            return []

        logger.info(f"🔍 开始HKEX官方分类过滤，输入 {len(raw_announcements)} 条公告")

        filtered_announcements = []
        classification_stats = {'total': len(raw_announcements), 'passed': 0, 'failed_confidence': 0,
            'failed_classification': 0, 'errors': 0}

        # 分类统计
        level1_stats = {}
        level2_stats = {}
        level3_stats = {}

        for i, announcement in enumerate(raw_announcements):
            try:
                # 获取HKEX官方分类结果
                category_path, keyword_category, priority, confidence, hkex_classification = self.classifier.classify_announcement(
                    announcement)

                title = announcement.get('TITLE', announcement.get('title', ''))[:50]

                # 检查是否有有效的HKEX官方分类
                if hkex_classification and hkex_classification.confidence > 0:
                    logger.debug(f"🔍 分类详情 - 标题: {title}")
                    logger.debug(f"   置信度: {hkex_classification.confidence:.2f}")
                    logger.debug(f"   一级: {hkex_classification.level1_code}-{hkex_classification.level1_name}")
                    logger.debug(f"   二级: {hkex_classification.level2_code}-{hkex_classification.level2_name}")
                    logger.debug(f"   三级: {hkex_classification.level3_code}-{hkex_classification.level3_name}")
                    logger.debug(f"   完整路径: {hkex_classification.full_path}")

                    # 检查置信度是否满足阈值
                    if hkex_classification.confidence >= self.hkex_confidence_threshold:
                        # 检查是否有有效的分类（接受2级或3级分类）
                        if hkex_classification.level2_name and (hkex_classification.level3_name or True):
                            # 将HKEX分类结果添加到公告对象中
                            enhanced_announcement = announcement.copy()
                            enhanced_announcement.update({'hkex_level1_code': hkex_classification.level1_code,
                                'hkex_level1_name': hkex_classification.level1_name,
                                'hkex_level2_code': hkex_classification.level2_code,
                                'hkex_level2_name': hkex_classification.level2_name,
                                'hkex_level3_code': hkex_classification.level3_code,
                                'hkex_level3_name': hkex_classification.level3_name,
                                'hkex_full_path': hkex_classification.full_path,
                                'hkex_classification_confidence': hkex_classification.confidence,
                                'hkex_classification_method': 'hkex_official'})
                            filtered_announcements.append(enhanced_announcement)
                            classification_stats['passed'] += 1

                            # 统计各级分类
                            level1_key = f"{hkex_classification.level1_code}-{hkex_classification.level1_name}"
                            level2_key = f"{hkex_classification.level2_code}-{hkex_classification.level2_name}"
                            level3_key = f"{hkex_classification.level3_code}-{hkex_classification.level3_name}"

                            level1_stats[level1_key] = level1_stats.get(level1_key, 0) + 1
                            level2_stats[level2_key] = level2_stats.get(level2_key, 0) + 1
                            level3_stats[level3_key] = level3_stats.get(level3_key, 0) + 1

                            logger.debug(
                                f"✅ 通过官方分类: {title}... → {hkex_classification.full_path} (置信度: {hkex_classification.confidence:.2f})")
                        else:
                            classification_stats['failed_classification'] += 1
                            logger.warning(
                                f"❌ 无有效分类: {title}... (置信度: {hkex_classification.confidence:.2f})")
                            logger.warning(f"   缺少有效分类: level2_name='{hkex_classification.level2_name}', level3_name='{hkex_classification.level3_name}'")
                    else:
                        classification_stats['failed_confidence'] += 1
                        logger.warning(
                            f"❌ 置信度不足: {title}... (置信度: {hkex_classification.confidence:.2f} < {self.hkex_confidence_threshold})")
                else:
                    classification_stats['failed_classification'] += 1
                    logger.warning(f"❌ 无官方分类或置信度为0: {title}...")
                    if hkex_classification:
                        logger.warning(f"   置信度: {hkex_classification.confidence}")
                    else:
                        logger.warning("   hkex_classification 为 None")

            except Exception as e:
                classification_stats['errors'] += 1
                logger.error(f"❌ 处理公告时出错: {e}")
                continue

        # 输出过滤统计信息
        logger.info(f"📊 HKEX官方分类过滤统计:")
        logger.info(f"  总数: {classification_stats['total']}")
        logger.info(f"  ✅ 通过: {classification_stats['passed']}")
        logger.info(f"  ❌ 置信度不足: {classification_stats['failed_confidence']}")
        logger.info(f"  ❌ 分类失败: {classification_stats['failed_classification']}")
        logger.info(f"  ❌ 处理错误: {classification_stats['errors']}")

        if classification_stats['total'] > 0:
            pass_rate = classification_stats['passed'] / classification_stats['total'] * 100
            logger.info(f"  📈 通过率: {pass_rate:.1f}%")

        # 输出分类分布统计
        if level1_stats:
            logger.info(f"🏷️ 一级分类分布: {dict(sorted(level1_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")
        if level2_stats:
            logger.info(f"🏷️ 二级分类分布: {dict(sorted(level2_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")
        if level3_stats:
            logger.info(f"🏷️ 三级分类分布: {dict(sorted(level3_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")

        logger.info(f"🎯 HKEX官方分类过滤完成，输出 {len(filtered_announcements)} 条公告")

        return filtered_announcements

    def get_filter_stats(self) -> Dict[str, Any]:
        """获取过滤器统计信息"""
        return {'filter_type': 'hkex_official', 'use_hkex_official': self.use_hkex_official,
            'classifier_initialized': self.classifier_initialized,
            'confidence_threshold': self.hkex_confidence_threshold,
            'fallback_to_keyword': self.config.get('classification', {}).get('fallback_to_keyword', False),
            'description': '仅保留通过HKEX官方3级分类的公告，支持关键字分类回退'}


# 测试函数
async def test_hkex_official_filter():
    """测试HKEX官方分类过滤器"""
    import yaml

    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 初始化过滤器
    filter = HKEXOfficialFilter(config)
    await filter.initialize()

    # 测试公告
    test_announcements = [{'TITLE': '建議股本重組及供股', 'STOCK_CODE': '00700'},
        {'TITLE': '暫停買賣', 'STOCK_CODE': '00939'}, {'TITLE': '委任獨立非執行董事', 'STOCK_CODE': '01398'},
        {'TITLE': '2025中期業績公告', 'STOCK_CODE': '02318'}, {'TITLE': '須予披露的交易收購', 'STOCK_CODE': '01234'}, ]

    # 执行过滤
    filtered = await filter.filter_announcements(test_announcements)

    print(f"原始公告数量: {len(test_announcements)}")
    print(f"过滤后数量: {len(filtered)}")

    for ann in filtered:
        print(f"  - {ann['TITLE']} ({ann['STOCK_CODE']})")


if __name__ == "__main__":
    asyncio.run(test_hkex_official_filter())
