"""
简化的公告过滤器
只保留最核心的过滤功能：股票过滤 + 简单类型过滤
"""

import logging
from typing import List, Dict, Set, Any

logger = logging.getLogger(__name__)


class SimpleAnnouncementFilter:
    """
    简化公告过滤器 - 只保留最核心的功能
    """

    def __init__(self, monitored_stocks: Set[str], config: dict):
        """
        初始化简化过滤器

        Args:
            monitored_stocks: 监听的股票代码集合
            config: 过滤配置
        """
        self.monitored_stocks = set(monitored_stocks)
        self.config = config

        # 从配置读取过滤条件
        self.stock_filter_enabled = config.get('stock_filter_enabled', True)
        self.type_filter_enabled = config.get('type_filter_enabled', True)
        self.excluded_categories = config.get('excluded_categories', [])
        self.included_keywords = config.get('included_keywords', [])

        logger.info("简化过滤器初始化完成")
        logger.info(f"  监听股票数量: {len(self.monitored_stocks)}")
        logger.info(f"  股票过滤: {'启用' if self.stock_filter_enabled else '禁用'}")
        logger.info(f"  类型过滤: {'启用' if self.type_filter_enabled else '禁用'}")
        logger.info(f"  排除类别数量: {len(self.excluded_categories)}")
        logger.info(f"  包含关键字数量: {len(self.included_keywords)}")

    def update_monitored_stocks(self, stocks: Set[str]):
        """更新监听股票列表"""
        old_count = len(self.monitored_stocks)
        self.monitored_stocks = set(stocks)
        new_count = len(self.monitored_stocks)
        logger.info(f"更新监听股票列表: {old_count} -> {new_count}")

    async def filter_announcements(self, raw_announcements: List[Dict]) -> List[Dict]:
        """
        执行简化过滤流程

        Args:
            raw_announcements: 原始公告列表

        Returns:
            过滤后的公告列表
        """
        if not raw_announcements:
            return []

        logger.info(f"开始简化过滤，输入 {len(raw_announcements)} 条公告")

        # 第一阶段：股票过滤
        if self.stock_filter_enabled:
            stock_filtered = await self._filter_by_stock(raw_announcements)
            logger.info(f"股票过滤后剩余: {len(stock_filtered)} 条公告")
        else:
            stock_filtered = raw_announcements

        # 第二阶段：类型过滤
        if self.type_filter_enabled:
            type_filtered = await self._filter_by_type(stock_filtered)
            logger.info(f"类型过滤后剩余: {len(type_filtered)} 条公告")
        else:
            type_filtered = stock_filtered

        # 统计过滤效果
        original_count = len(raw_announcements)
        final_count = len(type_filtered)
        if original_count > 0:
            filter_rate = (original_count - final_count) / original_count * 100
            logger.info(".1f")

        return type_filtered

    async def _filter_by_stock(self, announcements: List[Dict]) -> List[Dict]:
        """第一阶段：股票过滤"""
        if not self.monitored_stocks:
            logger.warning("监听股票列表为空")
            return []

        filtered = []
        stock_stats = {}

        for announcement in announcements:
            try:
                stock_code = self._extract_stock_code(announcement)
                stock_stats[stock_code] = stock_stats.get(stock_code, 0) + 1

                if stock_code in self.monitored_stocks:
                    filtered.append(announcement)
                    logger.debug(f"✅ 保留股票 {stock_code}: {announcement.get('TITLE', '')[:30]}...")
                else:
                    logger.debug(f"❌ 跳过股票 {stock_code}")

            except Exception as e:
                logger.error(f"处理公告时出错: {e}")
                continue

        # 显示统计信息
        if stock_stats:
            logger.info(f"📈 股票公告分布: {dict(sorted(stock_stats.items()))}")

        return filtered

    async def _filter_by_type(self, announcements: List[Dict]) -> List[Dict]:
        """第二阶段：简单类型过滤"""
        filtered = []
        category_stats = {}

        for announcement in announcements:
            try:
                # 获取公告类型（使用LONG_TEXT字段）
                announcement_type = announcement.get('LONG_TEXT', '')

                category_stats[announcement_type] = category_stats.get(announcement_type, 0) + 1

                # 检查排除类别
                if self._is_excluded(announcement_type):
                    logger.debug(f"❌ 排除类型: {announcement_type}")
                    continue

                # 检查包含关键字（如果配置了）
                if self.included_keywords and not self._contains_keywords(announcement_type, announcement):
                    logger.debug(f"❌ 不包含必需关键字: {announcement_type}")
                    continue

                filtered.append(announcement)
                logger.debug(f"✅ 保留类型: {announcement_type}")

            except Exception as e:
                logger.error(f"处理公告类型时出错: {e}")
                continue

        # 显示统计信息
        if category_stats:
            logger.info(f"📊 类型分布: {dict(sorted(category_stats.items(), key=lambda x: x[1], reverse=True)[:10])}")

        return filtered

    def _extract_stock_code(self, announcement: Dict) -> str:
        """提取股票代码"""
        stock_code = announcement.get('STOCK_CODE', '')

        # 处理不同的格式
        if isinstance(stock_code, str):
            # 移除.HK后缀并转换为5位格式
            stock_code = stock_code.replace('.HK', '').strip()
            if len(stock_code) == 4:
                stock_code = '0' + stock_code
        else:
            stock_code = str(stock_code)

        return stock_code

    def _is_excluded(self, announcement_type: str) -> bool:
        """检查是否为排除类型"""
        if not self.excluded_categories:
            return False

        announcement_type = announcement_type.lower().strip()

        for excluded in self.excluded_categories:
            if excluded.lower().strip() in announcement_type:
                return True

        return False

    def _contains_keywords(self, announcement_type: str, announcement: Dict) -> bool:
        """检查是否包含必需关键字"""
        if not self.included_keywords:
            return True

        # 检查LONG_TEXT
        text_to_check = announcement_type.lower()
        for keyword in self.included_keywords:
            if keyword.lower().strip() in text_to_check:
                return True

        # 检查标题
        title = announcement.get('TITLE', '').lower()
        for keyword in self.included_keywords:
            if keyword.lower().strip() in title:
                return True

        return False
