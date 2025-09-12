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
        
        # 从配置读取过滤条件
        filtering_config = config.get('realtime_monitoring', {}).get('filtering', {})
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
        else:
            stock_filtered = raw_announcements
            logger.info("股票过滤已禁用，跳过阶段1")
        
        # 阶段2：类型过滤
        if self.type_filter_enabled:
            type_filtered = await self.filter_by_announcement_type(stock_filtered)
            logger.info(f"阶段2-类型过滤后剩余: {len(type_filtered)} 条公告")
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
        
        Args:
            announcements: 股票过滤后的公告列表
            
        Returns:
            类型过滤后的公告列表
        """
        filtered = []
        category_stats = {}  # 统计各类别的公告数量
        
        for announcement in announcements:
            try:
                announcement_type = announcement.get('LONG_TEXT', '')
                
                # 统计
                category_stats[announcement_type] = category_stats.get(announcement_type, 0) + 1
                
                # 检查排除类别
                if self._is_excluded_category(announcement_type):
                    logger.debug(f"❌ 公告类型 '{announcement_type}' 在排除列表中，跳过")
                    continue
                
                # 检查包含关键字（如果配置了）
                if self.included_keywords:
                    if self._contains_included_keywords(announcement_type):
                        filtered.append(announcement)
                        logger.debug(f"✅ 公告类型 '{announcement_type}' 包含关键字，保留")
                    else:
                        logger.debug(f"❌ 公告类型 '{announcement_type}' 不包含关键字，跳过")
                else:
                    # 没有配置关键字则保留所有未排除的
                    filtered.append(announcement)
                    logger.debug(f"✅ 公告类型 '{announcement_type}' 通过过滤，保留")
                    
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
        """检查公告类型是否在排除列表中"""
        for excluded in self.excluded_categories:
            if excluded in announcement_type:
                return True
        return False
    
    def _contains_included_keywords(self, announcement_type: str) -> bool:
        """检查公告类型是否包含必需的关键字"""
        for keyword in self.included_keywords:
            if keyword in announcement_type:
                return True
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
