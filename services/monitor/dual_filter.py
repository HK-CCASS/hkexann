"""
双重公告过滤器
实现先股票过滤，再类型过滤的两阶段过滤逻辑
"""

import logging
from typing import List, Dict, Set, Any

logger = logging.getLogger(__name__)


class DualAnnouncementFilter:
    """
    双重公告过滤器 - 先过滤股票，再过滤类型
    
    正确的过滤流程：
    1. 第一阶段：股票过滤 - 只保留监听列表中的股票
    2. 第二阶段：类型过滤 - 按公告类型和关键字筛选
    
    这样可以大幅减少需要类型判断的公告数量，提高效率。
    """
    
    def __init__(self, monitored_stocks: Set[str], config: dict):
        """
        初始化双重过滤器
        
        Args:
            monitored_stocks: 监听的股票代码集合（5位数格式，如 "00700"）
            config: 过滤配置
        """
        self.monitored_stocks = set(monitored_stocks)  # 创建副本避免外部修改
        self.config = config
        
        # 从配置读取过滤条件 - 修复配置路径
        filtering_config = config.get('dual_filter', {})
        self.stock_filter_enabled = filtering_config.get('stock_filter_enabled', True)
        self.type_filter_enabled = filtering_config.get('type_filter_enabled', True)
        self.excluded_categories = filtering_config.get('excluded_categories', [])
        self.included_keywords = filtering_config.get('included_keywords', [])
        
        logger.info(f"双重过滤器初始化完成")
        logger.info(f"  监听股票数量: {len(self.monitored_stocks)}")
        logger.info(f"  股票过滤: {'启用' if self.stock_filter_enabled else '禁用'}")
        logger.info(f"  类型过滤: {'启用' if self.type_filter_enabled else '禁用'}")
        logger.info(f"  排除类别数量: {len(self.excluded_categories)}")
        logger.info(f"  包含关键字数量: {len(self.included_keywords)}")
        
        if self.monitored_stocks:
            logger.debug(f"  监听的股票代码: {sorted(list(self.monitored_stocks))[:10]}{'...' if len(self.monitored_stocks) > 10 else ''}")
    
    def update_monitored_stocks(self, stocks: Set[str]):
        """
        更新监听的股票列表
        
        Args:
            stocks: 新的股票代码集合
        """
        old_count = len(self.monitored_stocks)
        self.monitored_stocks = set(stocks)
        new_count = len(self.monitored_stocks)
        
        logger.info(f"更新监听股票列表: {old_count} -> {new_count}")
        
        if new_count > old_count:
            logger.info(f"新增 {new_count - old_count} 只股票到监听列表")
        elif new_count < old_count:
            logger.info(f"从监听列表移除 {old_count - new_count} 只股票")
    
    async def filter_announcements(self, raw_announcements: List[Dict]) -> List[Dict]:
        """
        执行双重过滤流程
        
        Args:
            raw_announcements: 原始公告列表
            
        Returns:
            过滤后的公告列表
        """
        if not raw_announcements:
            logger.debug("输入公告列表为空，直接返回")
            return []
        
        logger.info(f"开始双重过滤，输入 {len(raw_announcements)} 条公告")
        
        # 阶段1：股票过滤
        if self.stock_filter_enabled:
            stock_filtered = await self.filter_by_stock_list(raw_announcements)
            logger.info(f"阶段1-股票过滤后剩余: {len(stock_filtered)} 条公告")
            # logger.info(f"剩余的股票： {stock_filtered}")
            # logger.info("=====================================================================")
            # for data in stock_filtered:
            #     STOCK_CODE = data.get('STOCK_CODE', 'N/A')
            #     STOCK_NAME = data.get('STOCK_NAME', 'N/A')
            #     LONG_TEXT = data.get('LONG_TEXT', 'N/A')
            #     TITLE = data.get('TITLE', 'N/A').replace('\n', '')
            #     print(f"{STOCK_CODE} {STOCK_NAME} {LONG_TEXT}  {TITLE}")
            # logger.info("=====================================================================  \r\n")
        else:
            stock_filtered = raw_announcements
            logger.info("股票过滤已禁用，跳过阶段1")
        
        # 阶段2：类型过滤
        if self.type_filter_enabled:
            type_filtered = await self.filter_by_announcement_type(stock_filtered)
            logger.info(f"阶段2-类型过滤后剩余: {len(type_filtered)} 条公告")
            # logger.info(f"剩余的公告：  {type_filtered}")
            # logger.info("=====================================================================")
            # for data in type_filtered:
            #     STOCK_CODE = data.get('STOCK_CODE', 'N/A')
            #     STOCK_NAME = data.get('STOCK_NAME', 'N/A')
            #     LONG_TEXT = data.get('LONG_TEXT', 'N/A')
            #     TITLE = data.get('TITLE', 'N/A').replace('\n', '')
            #     print(f"{STOCK_CODE} {STOCK_NAME} {LONG_TEXT}  {TITLE}")
            # logger.info("=====================================================================  \r\n")
        else:
            type_filtered = stock_filtered
            logger.info("类型过滤已禁用，跳过阶段2")
        
        # 统计过滤效果
        original_count = len(raw_announcements)
        final_count = len(type_filtered)
        if original_count > 0:
            filter_rate = (original_count - final_count) / original_count * 100
            logger.info(f"双重过滤完成，过滤率: {filter_rate:.1f}% ({original_count} -> {final_count})")
        
        # 🔥 新增：输出过滤后的详细公告信息
        if type_filtered:
            logger.info(f"📋 过滤后保留的 {len(type_filtered)} 条公告详情:")
            for i, announcement in enumerate(type_filtered, 1):
                stock_code = announcement.get('STOCK_CODE', 'N/A')
                title = announcement.get('TITLE', 'Unknown Title')
                announcement_type = announcement.get('LONG_TEXT', 'Unknown Type')
                date_time = announcement.get('DATE_TIME', 'N/A')
                
                # 截断过长的标题和类型
                title_short = title[:50] + "..." if len(title) > 50 else title
                type_short = announcement_type[:40] + "..." if len(announcement_type) > 40 else announcement_type
                
                logger.info(f"  {i:2d}. [{stock_code}] {title_short}")
                logger.info(f"      📅 {date_time} | 📂 {type_short}")
        else:
            logger.info("📋 过滤后无公告保留")
        
        return type_filtered
    
    async def filter_by_stock_list(self, announcements: List[Dict]) -> List[Dict]:
        """
        第一阶段：按股票列表过滤
        
        Args:
            announcements: 输入公告列表
            
        Returns:
            股票过滤后的公告列表
        """
        if not self.monitored_stocks:
            logger.warning("监听股票列表为空，所有公告将被过滤掉")
            return []
        
        filtered = []
        stock_stats = {}  # 统计各股票的公告数量
        
        for announcement in announcements:
            try:
                stock_code = self.extract_stock_code(announcement)
                
                # 统计
                stock_stats[stock_code] = stock_stats.get(stock_code, 0) + 1
                
                if stock_code in self.monitored_stocks:
                    filtered.append(announcement)
                    logger.debug(f"✅ 股票 {stock_code} 在监听列表中，保留公告: {announcement.get('TITLE', 'Unknown')[:30]}...")
                else:
                    logger.debug(f"❌ 股票 {stock_code} 不在监听列表中，跳过")
                    
            except Exception as e:
                logger.error(f"处理公告时出错: {e}, 公告: {announcement}")
                continue
        
        # 记录股票统计
        if stock_stats:
            logger.info(f"📈 股票公告分布: {dict(sorted(stock_stats.items()))}")
            
            # 显示监控和未监控的股票
            monitored_stocks_found = []
            unmonitored_stocks_found = []
            for stock_code, count in stock_stats.items():
                if stock_code in self.monitored_stocks:
                    monitored_stocks_found.append(f"{stock_code}({count})")
                else:
                    unmonitored_stocks_found.append(f"{stock_code}({count})")
            
            if monitored_stocks_found:
                logger.info(f"✅ 监控股票: {', '.join(monitored_stocks_found)}")
            if unmonitored_stocks_found:
                logger.info(f"❌ 未监控股票: {', '.join(unmonitored_stocks_found)}")
        
        return filtered
    
    async def filter_by_announcement_type(self, announcements: List[Dict]) -> List[Dict]:
        """
        第二阶段：按公告类型过滤

        Args:
            announcements: 股票过滤后的公告列表

        Returns:
            类型过滤后的公告列表
        """
        filtered = []
        category_stats = {}  # 统计各类别的公告数量

        for announcement in announcements:
            try:
                # 第一步：获取或推断公告类型
                announcement_type = self._get_or_infer_announcement_type(announcement)

                # 统计
                category_stats[announcement_type] = category_stats.get(announcement_type, 0) + 1

                # 第二步：检查排除类别
                if self._is_excluded_category(announcement_type):
                    logger.debug(f"❌ 公告类型 '{announcement_type}' 在排除列表中，跳过")
                    continue

                # 第三步：应用过滤策略
                if self._should_keep_announcement(announcement_type, announcement):
                    filtered.append(announcement)
                    logger.debug(f"✅ 公告类型 '{announcement_type}' 通过过滤，保留")
                else:
                    logger.debug(f"❌ 公告不符合过滤条件，跳过")

            except Exception as e:
                logger.error(f"处理公告类型时出错: {e}, 公告: {announcement}")
                continue
        
        # 记录类别统计
        if category_stats:
            logger.info(f"📊 公告类型分布: {dict(sorted(category_stats.items()))}")
            
            # 显示被过滤掉的类型
            excluded_types = []
            kept_types = []
            for ann_type, count in category_stats.items():
                if self._is_excluded_category(ann_type):
                    excluded_types.append(f"{ann_type}({count})")
                else:
                    kept_types.append(f"{ann_type}({count})")
            
            if excluded_types:
                logger.info(f"❌ 被过滤的类型: {', '.join(excluded_types)}")
            if kept_types:
                logger.info(f"✅ 保留的类型: {', '.join(kept_types)}")
        
        return filtered
    
    def _is_excluded_category(self, announcement_type: str) -> bool:
        """
        检查公告类型是否在排除列表中
        
        Args:
            announcement_type: 公告类型字符串
            
        Returns:
            bool: 是否属于排除类别
        """
        if not self.excluded_categories:
            return False  # 如果没有配置排除类别，默认不排除
        
        # 标准化处理
        announcement_type = announcement_type or ""
        announcement_type_lower = announcement_type.lower().strip()
        
        for excluded in self.excluded_categories:
            excluded_lower = excluded.lower().strip()
            if excluded_lower and excluded_lower in announcement_type_lower:
                logger.debug(f"❌ 公告类型 '{announcement_type}' 匹配排除类别 '{excluded}'")
                return True
        
        return False
    
    def _contains_included_keywords(self, announcement_type: str, announcement: Dict = None) -> bool:
        """
        检查公告类型或标题是否包含必需的关键字
        
        Args:
            announcement_type: 公告类型字符串
            announcement: 完整公告数据（可选）
            
        Returns:
            bool: 是否包含任何必需的关键字
        """
        if not self.included_keywords:
            return True  # 如果没有配置关键字，默认通过
        
        # 标准化处理，避免空字符串和大小写问题
        announcement_type = announcement_type or ""
        announcement_type_lower = announcement_type.lower().strip()
        
        # 检查 LONG_TEXT (公告类型)
        for keyword in self.included_keywords:
            keyword_lower = keyword.lower().strip()
            if keyword_lower and keyword_lower in announcement_type_lower:
                logger.debug(f"✅ LONG_TEXT 匹配关键字 '{keyword}': {announcement_type}")
                return True
        
        # 如果提供了完整公告，也检查标题
        if announcement:
            title = announcement.get('TITLE', '') or ""
            title_lower = title.lower().strip()
            
            for keyword in self.included_keywords:
                keyword_lower = keyword.lower().strip()
                if keyword_lower and keyword_lower in title_lower:
                    logger.debug(f"✅ TITLE 匹配关键字 '{keyword}': {title[:50]}...")
                    return True
        
        # 记录不匹配的情况，便于调试
        logger.debug(f"❌ 未匹配任何关键字 - 类型: '{announcement_type}', "
                    f"标题: '{announcement.get('TITLE', '')[:30] if announcement else 'N/A'}...'")
        return False
    
    def extract_stock_code(self, announcement: Dict) -> str:
        """
        从公告数据中提取股票代码
        
        Args:
            announcement: 公告数据字典
            
        Returns:
            标准化的股票代码（5位数格式）
        """
        # 根据HKEX API实际数据结构调整
        stock_code = announcement.get('STOCK_CODE', '')
        
        if not stock_code:
            logger.warning(f"公告缺少股票代码: {announcement.get('TITLE', 'Unknown')}")
            return ''
        
        # 标准化处理
        # 移除.HK后缀
        clean_code = stock_code.upper().replace('.HK', '')
        
        # 确保是数字格式
        if not clean_code.isdigit():
            logger.warning(f"股票代码不是数字格式: {stock_code} -> {clean_code}")
            return clean_code  # 还是返回，可能是特殊代码
        
        # 标准化为5位数格式
        normalized_code = clean_code.zfill(5)
        
        if normalized_code != clean_code:
            logger.debug(f"股票代码标准化: {stock_code} -> {normalized_code}")
        
        return normalized_code
    
    def get_filter_stats(self) -> Dict[str, Any]:
        """获取过滤器统计信息"""
        return {
            "monitored_stocks_count": len(self.monitored_stocks),
            "stock_filter_enabled": self.stock_filter_enabled,
            "type_filter_enabled": self.type_filter_enabled,
            "excluded_categories_count": len(self.excluded_categories),
            "included_keywords_count": len(self.included_keywords),
            "excluded_categories": self.excluded_categories,
            "included_keywords": self.included_keywords
        }
    
    def is_stock_monitored(self, stock_code: str) -> bool:
        """检查股票是否在监听列表中"""
        normalized_code = self.extract_stock_code({'STOCK_CODE': stock_code})
        return normalized_code in self.monitored_stocks
    
    def _classify_by_title(self, title: str) -> str:
        """
        基于标题进行智能分类
        
        Args:
            title: 公告标题
            
        Returns:
            推断的分类类型
        """
        if not title:
            return ""
        
        title_lower = title.lower()
        
        # 基于关键字的简单分类逻辑
        classification_keywords = {
            "月報表": ["月份", "月報", "證券變動月報表"],
            "翌日披露報表": ["翌日披露", "披露報表"],
            "通告及告示": ["建議", "公告", "通告", "委任", "辞任", "收購", "供股", "配股", "合股", "業績"],
            "通函": ["通函", "股東大會", "特別大會"],
            "財務報表": ["財務", "年報", "中期報告", "業績"]
        }
        
        for category, keywords in classification_keywords.items():
            for keyword in keywords:
                if keyword.lower() in title_lower:
                    return category
        
        # 默认分类
        return "通告及告示"
    
    def _get_or_infer_announcement_type(self, announcement: Dict) -> str:
        """
        获取或推断公告类型

        Args:
            announcement: 公告数据

        Returns:
            公告类型字符串
        """
        announcement_type = announcement.get('LONG_TEXT', '')

        # 如果LONG_TEXT为空，尝试使用智能分类
        if not announcement_type:
            title = announcement.get('TITLE', '')
            if title:
                # 使用智能分类器来确定类型
                announcement_type = self._classify_by_title(title)
                announcement['LONG_TEXT_INFERRED'] = announcement_type  # 标记为推断得出

        return announcement_type

    def _should_keep_announcement(self, announcement_type: str, announcement: Dict) -> bool:
        """
        统一的过滤策略判断

        Args:
            announcement_type: 公告类型
            announcement: 公告数据

        Returns:
            是否保留该公告
        """
        # 如果配置了关键字，必须包含关键字才保留
        if self.included_keywords:
            if self._contains_included_keywords(announcement_type, announcement):
                logger.info(f"✅ 公告 '{announcement.get('TITLE', '')[:30]}...' 包含关键字，保留")
                return True
            else:
                logger.info(f"❌ 公告 '{announcement.get('TITLE', '')[:30]}...' 不包含关键字，跳过")
                return False

        # 未配置关键字时的处理逻辑
        if announcement_type == '':
            # 空字符串表示无法分类，使用特殊策略判断
            should_keep = self._should_keep_unclassified(announcement)
            if should_keep:
                logger.info(f"✅ 未分类公告通过策略检查，保留: {announcement.get('TITLE', '')[:30]}...")
            else:
                logger.info(f"❌ 未分类公告被策略过滤: {announcement.get('TITLE', '')[:30]}...")
            return should_keep
        else:
            # 有分类的公告，保留所有未排除的
            return True

    def _should_keep_unclassified(self, announcement: Dict) -> bool:
        """
        判断是否保留未分类的公告

        Args:
            announcement: 公告信息

        Returns:
            是否保留该公告
        """
        title = announcement.get('TITLE', '')

        # 排除明显的无关内容
        exclusion_patterns = [
            "月報表", "翌日披露", "展示文件", "通函","股东大会"
        ]

        for pattern in exclusion_patterns:
            if pattern in title:
                return False

        # 包含重要关键字的保留
        important_patterns = [
            "建議", "公告", "收購", "供股", "配股", "合股", '根據一般性授權發行股份','根據一般','授權'
        ]

        for pattern in important_patterns:
            if pattern in title:
                return True

        # 其他情况默认保留（可以根据需要调整）
        return True
