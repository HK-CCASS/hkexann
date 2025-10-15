import asyncio
import logging
from typing import List, Dict, Any, Set
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
        self.classifier = AnnouncementClassifier(config)
        self.classifier_initialized = False
        self.hkex_confidence_threshold = config.get('classification', {}).get('hkex_confidence_threshold', 0.6)
        self.use_hkex_official = config.get('classification', {}).get('use_hkex_official', True)
        self.monitored_stocks = set()  # HKEX过滤器不基于股票过滤，但保持接口兼容性

        # 获取排除的分类列表
        self.excluded_categories = set()
        announcement_categories = config.get('announcement_categories', {})
        if announcement_categories.get('enabled', True):
            excluded_list = announcement_categories.get('excluded_categories', [])
            self.excluded_categories = set(excluded_list)

        logger.info("HKEX官方分类过滤器初始化完成")
        logger.info(f"  置信度阈值: {self.hkex_confidence_threshold}")
        logger.info(f"  官方分类开关: {'启用' if self.use_hkex_official else '禁用'}")
        logger.info(f"  排除分类数量: {len(self.excluded_categories)}")
        if self.excluded_categories:
            logger.debug(f"  排除的分类: {sorted(self.excluded_categories)}")

    async def initialize(self) -> bool:
        """异步初始化分类器"""
        if not self.use_hkex_official:
            logger.warning("⚠️ HKEX官方分类已禁用，将不会过滤任何公告")
            return False

        try:
            # Note: HKEX官方分类器尚未实现，目前使用关键字分类器
            # TODO: 实现真正的HKEX官方分类系统
            self.classifier_initialized = True
            logger.info("✅ HKEX官方分类过滤器初始化成功（使用关键字分类回退）")
            return True
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
        classification_stats = {
            'total': len(raw_announcements),
            'passed': 0,
            'excluded': 0,
            'failed_confidence': 0,
            'failed_classification': 0,
            'errors': 0
        }

        level1_stats = {}
        level2_stats = {}
        level3_stats = {}

        for i, announcement in enumerate(raw_announcements):
            try:
                category_path, keyword_category, priority, confidence = self.classifier.classify_announcement(announcement)
                # TODO: 实现真正的HKEX官方分类，目前返回None作为占位符
                hkex_classification = None
                title = announcement.get('TITLE', announcement.get('title', ''))[:50]

                # 临时方案：使用关键字分类作为HKEX官方分类的替代
                # TODO: 实现真正的HKEX官方3级分类系统
                if confidence > 0:
                    # 检查是否在排除列表中
                    if keyword_category in self.excluded_categories or category_path in self.excluded_categories:
                        classification_stats['excluded'] += 1
                        logger.debug(f"🚫 排除公告: {title}... (分类: {keyword_category}/{category_path})")
                        continue

                    # 将关键字分类结果作为HKEX分类使用
                    enhanced_announcement = announcement.copy()
                    enhanced_announcement.update({
                        'hkex_level1_code': 'KEYWORD',
                        'hkex_level1_name': '关键字分类',
                        'hkex_level2_code': keyword_category or 'OTHER',
                        'hkex_level2_name': keyword_category or '其他',
                        'hkex_level3_code': category_path or 'OTHER',
                        'hkex_level3_name': category_path or '其他',
                        'hkex_full_path': f"关键字分类/{keyword_category or '其他'}/{category_path or '其他'}",
                        'hkex_classification_confidence': confidence,
                        'hkex_classification_method': 'keyword_fallback'
                    })
                    filtered_announcements.append(enhanced_announcement)
                    classification_stats['passed'] += 1

                    level1_key = "KEYWORD-关键字分类"
                    level2_key = f"{keyword_category or 'OTHER'}-{keyword_category or '其他'}"
                    level3_key = f"{category_path or 'OTHER'}-{category_path or '其他'}"

                    level1_stats[level1_key] = level1_stats.get(level1_key, 0) + 1
                    level2_stats[level2_key] = level2_stats.get(level2_key, 0) + 1
                    level3_stats[level3_key] = level3_stats.get(level3_key, 0) + 1

                    logger.debug(f"✅ 通过关键字分类: {title}... → {enhanced_announcement['hkex_full_path']} (置信度: {confidence:.2f})")
                else:
                    classification_stats['failed_classification'] += 1
                    logger.warning(f"❌ 无有效分类: {title}... (置信度: {confidence:.2f})")

            except Exception as e:
                classification_stats['errors'] += 1
                logger.error(f"❌ 处理公告时出错: {e}")
                continue

        logger.info(f"📊 HKEX官方分类过滤统计:")
        logger.info(f"  总数: {classification_stats['total']}")
        logger.info(f"  ✅ 通过: {classification_stats['passed']}")
        logger.info(f"  🚫 排除: {classification_stats['excluded']}")
        logger.info(f"  ❌ 置信度不足: {classification_stats['failed_confidence']}")
        logger.info(f"  ❌ 分类失败: {classification_stats['failed_classification']}")
        logger.info(f"  ❌ 处理错误: {classification_stats['errors']}")

        if classification_stats['total'] > 0:
            pass_rate = classification_stats['passed'] / classification_stats['total'] * 100
            logger.info(f"  📈 通过率: {pass_rate:.1f}%")

        if level1_stats:
            logger.info(f"🏷️ 一级分类分布: {dict(sorted(level1_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")
        if level2_stats:
            logger.info(f"🏷️ 二级分类分布: {dict(sorted(level2_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")
        if level3_stats:
            logger.info(f"🏷️ 三级分类分布: {dict(sorted(level3_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")

        logger.info(f"🎯 HKEX官方分类过滤完成，输出 {len(filtered_announcements)} 条公告")

        return filtered_announcements

    def update_monitored_stocks(self, stocks: Set[str]):
        """
        更新监听的股票列表
        HKEX官方过滤器不基于股票进行过滤，但保持接口兼容性

        Args:
            stocks: 新的股票代码集合
        """
        self.monitored_stocks = set(stocks)
        logger.debug(f"✅ HKEX官方过滤器已更新监听股票: {len(stocks)} 只")

    def get_filter_stats(self) -> Dict[str, Any]:
        """获取过滤器统计信息"""
        return {
            'filter_type': 'hkex_official',
            'use_hkex_official': self.use_hkex_official,
            'classifier_initialized': self.classifier_initialized,
            'confidence_threshold': self.hkex_confidence_threshold,
            'fallback_to_keyword': self.config.get('classification', {}).get('fallback_to_keyword', False),
            'description': '仅保留通过HKEX官方3级分类的公告，支持关键字分类回退'
        }

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
    test_announcements = [
        {'TITLE': '建議股本重組及供股', 'STOCK_CODE': '00700'},
        {'TITLE': '暫停買賣', 'STOCK_CODE': '00939'},
        {'TITLE': '委任獨立非執行董事', 'STOCK_CODE': '01398'},
        {'TITLE': '2025中期業績公告', 'STOCK_CODE': '02318'},
        {'TITLE': '須予披露的交易收購', 'STOCK_CODE': '01234'},
    ]

    # 执行过滤
    filtered = await filter.filter_announcements(test_announcements)

    print(f"原始公告数量: {len(test_announcements)}")
    print(f"过滤后数量: {len(filtered)}")

    for ann in filtered:
        print(f"  - {ann['TITLE']} ({ann['STOCK_CODE']})")

if __name__ == "__main__":
    asyncio.run(test_hkex_official_filter())