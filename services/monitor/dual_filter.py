"""
双重公告过滤器
实现先股票过滤，再类型过滤的两阶段过滤逻辑
支持HKEX官方分类代码的精确过滤
"""

import logging
from typing import List, Dict, Set, Any, Optional

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

        # 从配置读取过滤条件
        filtering_config = config.get('realtime_monitoring', {}).get('filtering', {})
        self.stock_filter_enabled = filtering_config.get('stock_filter_enabled', True)
        self.type_filter_enabled = filtering_config.get('type_filter_enabled', True)
        self.excluded_categories = filtering_config.get('excluded_categories', [])
        self.included_keywords = filtering_config.get('included_keywords', [])

        # HKEX分类系统支持
        self.hkex_classification_enabled = filtering_config.get('hkex_classification_enabled', True)
        self.classification_parser = None
        self.excluded_codes: Set[str] = set()

        # 初始化HKEX分类解析器
        if self.hkex_classification_enabled:
            try:
                from .classification_parser import get_classification_parser
                self.classification_parser = get_classification_parser()
                if self.classification_parser.load_classifications():
                    # 将排除的类别名称转换为分类代码
                    self.excluded_codes = self.classification_parser.get_excluded_codes_by_names(self.excluded_categories)
                    logger.info(f"✅ HKEX分类系统已启用，加载了 {len(self.classification_parser.categories)} 个分类")
                    logger.info(f"   排除的分类代码: {len(self.excluded_codes)} 个")
                else:
                    logger.warning("❌ HKEX分类系统启用失败，将使用传统过滤方式")
                    self.hkex_classification_enabled = False
            except Exception as e:
                logger.error(f"初始化HKEX分类系统失败: {e}，将使用传统过滤方式")
                self.hkex_classification_enabled = False

        logger.info(f"双重过滤器初始化完成")
        logger.info(f"  监听股票数量: {len(self.monitored_stocks)}")
        logger.info(f"  股票过滤: {'启用' if self.stock_filter_enabled else '禁用'}")
        logger.info(f"  类型过滤: {'启用' if self.type_filter_enabled else '禁用'}")
        logger.info(f"  HKEX分类系统: {'启用' if self.hkex_classification_enabled else '禁用'}")
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
        支持HKEX分类代码的精确过滤

        Args:
            announcements: 股票过滤后的公告列表

        Returns:
            类型过滤后的公告列表
        """
        filtered = []
        category_stats = {}  # 统计各类别的公告数量
        code_stats = {}  # 统计分类代码的使用情况

        for announcement in announcements:
            try:
                # 优先使用HKEX分类代码进行过滤
                if self.hkex_classification_enabled and self.classification_parser:
                    # 第一步：尝试从API字段获取分类代码
                    category_code = self._extract_category_code(announcement)

                    if category_code:
                        # 使用分类代码进行精确过滤
                        category_stats[category_code] = category_stats.get(category_code, 0) + 1

                        # 检查分类代码是否在排除列表中
                        if self.classification_parser.is_excluded_category(category_code, self.excluded_codes):
                            # 获取分类信息用于日志
                            category_info = self.classification_parser.get_category_hierarchy(category_code)
                            category_name = category_info['level3_name'] if category_info else category_code
                            logger.debug(f"❌ 分类代码 '{category_code}' ({category_name}) 在排除列表中，跳过")
                            continue

                        # 检查包含关键字（如果配置了的话）
                        if self.included_keywords:
                            category_info = self.classification_parser.get_category_hierarchy(category_code)
                            if category_info:
                                category_text = f"{category_info['level1_name']} {category_info['level2_name']} {category_info['level3_name']}"
                                if not self._contains_included_keywords(category_text, announcement):
                                    logger.debug(f"❌ 分类 '{category_text}' 不包含必需的关键字，跳过")
                                    continue

                        # 通过HKEX分类过滤
                        filtered.append(announcement)
                        logger.debug(f"✅ 分类代码 '{category_code}' 通过HKEX分类过滤，保留")
                        continue

                # 降级到传统LONG_TEXT过滤
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
                    logger.debug(f"✅ 公告类型 '{announcement_type}' 通过传统过滤，保留")
                else:
                    logger.debug(f"❌ 公告不符合过滤条件，跳过")

            except Exception as e:
                logger.error(f"处理公告类型时出错: {e}, 公告: {announcement}")
                continue

        # 记录类别统计
        if category_stats:
            logger.info(f"📊 公告类型分布: {dict(sorted(category_stats.items()))}")

            # 显示过滤统计
            excluded_count = 0
            kept_count = 0

            for ann_type, count in category_stats.items():
                # 检查是否为HKEX分类代码
                if self.hkex_classification_enabled and ann_type.isdigit() and len(ann_type) == 5:
                    if self.classification_parser.is_excluded_category(ann_type, self.excluded_codes):
                        excluded_count += count
                    else:
                        kept_count += count
                else:
                    # 传统类型过滤
                    if self._is_excluded_category(ann_type):
                        excluded_count += count
                    else:
                        kept_count += count

            logger.info(f"📈 过滤统计: 保留 {kept_count} 条，过滤 {excluded_count} 条")

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
        stats = {
            "monitored_stocks_count": len(self.monitored_stocks),
            "stock_filter_enabled": self.stock_filter_enabled,
            "type_filter_enabled": self.type_filter_enabled,
            "excluded_categories_count": len(self.excluded_categories),
            "included_keywords_count": len(self.included_keywords),
            "excluded_categories": self.excluded_categories,
            "included_keywords": self.included_keywords,
            "hkex_classification_enabled": self.hkex_classification_enabled,
            "excluded_codes_count": len(self.excluded_codes)
        }

        # 添加HKEX分类系统统计
        if self.hkex_classification_enabled and self.classification_parser:
            classification_stats = self.classification_parser.get_statistics()
            stats["hkex_classification_stats"] = classification_stats
            stats["excluded_codes"] = list(self.excluded_codes)

        return stats
    
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

    def _extract_category_code(self, announcement: Dict) -> Optional[str]:
        """
        从公告中提取HKEX分类代码

        优先级：
        1. T2_CODE (2级分类代码，更精确)
        2. T1_CODE (1级分类代码，备选)

        Args:
            announcement: 公告数据

        Returns:
            分类代码字符串或None
        """
        # 优先使用T2_CODE (3级分类代码)
        t2_code = announcement.get('T2_CODE', '').strip()
        if t2_code and t2_code.isdigit():
            # 标准化为5位数格式
            return t2_code.zfill(5)

        # 备选使用T1_CODE (1级分类代码)
        t1_code = announcement.get('T1_CODE', '').strip()
        if t1_code and t1_code.isdigit():
            # 转换为5位数格式（通常T1_CODE是5位数）
            return t1_code.zfill(5)

        return None

    def _contains_included_keywords(self, category_text: str, announcement: Dict = None) -> bool:
        """
        检查分类文本是否包含必需的关键字

        Args:
            category_text: 分类文本
            announcement: 公告数据（可选）

        Returns:
            是否包含关键字
        """
        if not self.included_keywords:
            return True  # 如果没有配置关键字，默认通过

        category_text_lower = category_text.lower().strip()

        # 检查分类文本
        for keyword in self.included_keywords:
            keyword_lower = keyword.lower().strip()
            if keyword_lower and keyword_lower in category_text_lower:
                logger.debug(f"✅ 分类文本匹配关键字 '{keyword}': {category_text}")
                return True

        # 如果提供了公告，也检查标题
        if announcement:
            title = announcement.get('TITLE', '') or ""
            title_lower = title.lower().strip()

            for keyword in self.included_keywords:
                keyword_lower = keyword.lower().strip()
                if keyword_lower and keyword_lower in title_lower:
                    logger.debug(f"✅ 标题匹配关键字 '{keyword}': {title[:50]}...")
                    return True

        # 记录不匹配的情况
        logger.debug(f"❌ 分类文本和标题均未匹配任何关键字 - 文本: '{category_text[:50]}...', "
                    f"标题: '{announcement.get('TITLE', '')[:30] if announcement else 'N/A'}...'")
        return False
