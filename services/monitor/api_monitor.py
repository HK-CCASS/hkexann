"""
HKEX公告API监听器
实时轮询港交所公告API，获取最新公告数据
"""

import asyncio
import logging
import time
import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set

import aiohttp
from aiohttp import ClientTimeout
import pytz

logger = logging.getLogger(__name__)


class HKEXAPIMonitor:
    """
    HKEX公告API监听器 - Bug修复版
    
    实时轮询港交所公告API，获取最新公告数据。
    支持时间戳缓存避免、新公告检测、错误重试等功能。
    
    🔧 主要Bug修复：
    1. 时间戳更新时机问题 - 只有处理成功后才更新
    2. 重复公告检测 - 基于ID缓存避免重复处理
    3. 时区处理问题 - 统一使用香港时区
    4. 文档一致性 - 修正时间窗口描述
    """
    
    def __init__(self, config: dict):
        """
        初始化API监听器
        
        Args:
            config: 配置字典，包含API URL、超时设置等
        """
        # 实时监听API (与下载API不同)
        self.base_url = "https://www1.hkexnews.hk/ncms/json/eds/lcisehk1relsdc_1.json"
        self.config = config
        
        # 🔧 修复：时区和时间戳管理
        self.hk_tz = pytz.timezone('Asia/Hong_Kong')
        self.last_successful_check: Optional[datetime] = None  # 成功处理完成的时间
        self.last_poll_attempt: Optional[datetime] = None      # 最后轮询尝试时间
        
        # 🔧 修复：添加公告ID缓存避免重复
        self.processed_announcement_ids: Set[str] = set()
        self.max_id_cache_size = 1000  # 最大缓存ID数量
        
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 从配置读取参数
        api_config = config.get('realtime_monitoring', {})
        self.check_interval = api_config.get('check_interval', 300)  # 5分钟
        self.timeout = api_config.get('timeout', 30)
        self.retry_attempts = api_config.get('retry_attempts', 3)
        
        # 请求头配置
        headers_config = api_config.get('headers', {})
        self.headers = {
            'User-Agent': headers_config.get('user_agent', 
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),
            'Referer': headers_config.get('referer', 
                'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh'),
            'Accept': headers_config.get('accept', 
                'application/json, text/javascript, */*; q=0.01'),
            'Accept-Language': headers_config.get('accept_language', 
                'zh-CN,zh;q=0.9,en;q=0.8'),
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        logger.info(f"HKEX API监听器(修复版)初始化完成")
        logger.info(f"  API URL: {self.base_url}")
        logger.info(f"  检查间隔: {self.check_interval}秒")
        logger.info(f"  超时时间: {self.timeout}秒")
        logger.info(f"  重试次数: {self.retry_attempts}")
        logger.info(f"  时区: {self.hk_tz}")
        logger.info(f"  ID缓存大小: {self.max_id_cache_size}")
    
    def _convert_news_format(self, raw_news_list: List[Dict]) -> List[Dict[str, Any]]:
        """
        将HKEX API的原始数据格式转换为标准格式
        
        Args:
            raw_news_list: API返回的原始新闻列表
            
        Returns:
            转换后的标准格式公告列表
        """
        converted_announcements = []
        
        for raw_news in raw_news_list:
            try:
                # 提取股票信息
                stocks = raw_news.get('stock', [])
                if not stocks:
                    continue  # 跳过没有股票代码的公告
                
                # 处理多股票公告（为每个股票创建一条记录）
                for stock_info in stocks:
                    stock_code = stock_info.get('sc', '').strip()
                    if not stock_code:
                        continue
                    
                    # 构建标准格式的公告
                    announcement = {
                        'STOCK_CODE': stock_code,
                        'STOCK_NAME': stock_info.get('sn', ''),
                        'TITLE': raw_news.get('title', '').strip(),
                        'LONG_TEXT': raw_news.get('lTxt', ''),
                        'SHORT_TEXT': raw_news.get('sTxt', ''),
                        'DATE_TIME': raw_news.get('relTime', ''),
                        'PUBLISHED_TIME': raw_news.get('relTime', ''),
                        'FILE_LINK': f"https://www1.hkexnews.hk{raw_news.get('webPath', '')}" if raw_news.get('webPath') else '',
                        'FILE_SIZE': raw_news.get('size', ''),
                        'FILE_TYPE': raw_news.get('ext', ''),
                        'NEWS_ID': raw_news.get('newsId', ''),
                        'MARKET': raw_news.get('market', ''),
                        'T1_CODE': raw_news.get('t1Code', ''),
                        'T2_CODE': raw_news.get('t2Code', ''),
                        'IS_MULTI_STOCK': len(stocks) > 1
                    }
                    
                    converted_announcements.append(announcement)
                    
            except Exception as e:
                logger.warning(f"转换公告格式失败: {e}, 原始数据: {raw_news}")
                continue
        
        logger.debug(f"格式转换完成: {len(raw_news_list)} -> {len(converted_announcements)}")
        return converted_announcements
    
    async def initialize(self) -> bool:
        """初始化异步会话"""
        try:
            timeout = ClientTimeout(total=self.timeout)
            # 创建SSL上下文，允许不验证证书（仅用于测试）
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.headers,
                connector=aiohttp.TCPConnector(
                    limit=10,
                    ssl=ssl_context
                )
            )
            logger.info("API监听器会话初始化成功")
            return True
        except Exception as e:
            logger.error(f"API监听器会话初始化失败: {e}")
            return False
    
    async def close(self):
        """关闭异步会话"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("API监听器会话已关闭")
    
    async def fetch_latest_announcements(self, stock_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        获取最新公告数据 (实时监听API) - 修复版
        
        Args:
            stock_codes: 监听的股票代码列表，用于后续过滤（监听API不需要指定股票）
            
        Returns:
            包含新公告的列表（带有_announcement_id和_parsed_datetime字段）
        """
        # 🔧 修复：记录轮询尝试时间
        self.last_poll_attempt = datetime.now(self.hk_tz)
        for attempt in range(self.retry_attempts):
            try:
                # 生成时间戳参数避免缓存
                timestamp = int(time.time() * 1000)
                url = f"{self.base_url}?_={timestamp}"
                
                logger.debug(f"API请求 (尝试 {attempt + 1}/{self.retry_attempts}): {url}")
                
                if not self.session:
                    await self.initialize()
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            raw_news_list = data.get('newsInfoLst', [])
                            
                            if not isinstance(raw_news_list, list):
                                logger.warning(f"API返回数据格式异常: {type(raw_news_list)}")
                                return []
                            
                            logger.info(f"API返回 {len(raw_news_list)} 条原始公告")
                            
                            # 转换为标准格式
                            announcements = self._convert_news_format(raw_news_list)
                            logger.info(f"转换后获得 {len(announcements)} 条有效公告")
                            
                            # 过滤出新公告（基于时间戳）
                            new_announcements = self.filter_new_announcements(announcements)
                            logger.info(f"发现 {len(new_announcements)} 条新公告")
                            
                            return new_announcements
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"API响应JSON解析失败: {e}")
                            if attempt < self.retry_attempts - 1:
                                await asyncio.sleep(2 ** attempt)  # 指数退避
                                continue
                            return []
                            
                    elif response.status == 429:
                        # 速率限制，等待更长时间
                        wait_time = min(60, 2 ** (attempt + 2))
                        logger.warning(f"API速率限制 (429)，等待 {wait_time} 秒")
                        await asyncio.sleep(wait_time)
                        continue
                        
                    else:
                        logger.warning(f"API请求失败，状态码: {response.status}")
                        if attempt < self.retry_attempts - 1:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return []
                        
            except asyncio.TimeoutError:
                logger.warning(f"API请求超时 (尝试 {attempt + 1}/{self.retry_attempts})")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []
                
            except aiohttp.ClientError as e:
                logger.error(f"API请求客户端错误: {e}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []
                
            except Exception as e:
                logger.error(f"API请求未知错误: {e}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []
        
        logger.error(f"所有重试尝试失败，返回空列表")
        return []
    
    def _convert_news_format(self, raw_news_list: List[Dict]) -> List[Dict[str, Any]]:
        """
        转换原始新闻数据为标准格式
        
        Args:
            raw_news_list: 原始新闻列表
            
        Returns:
            标准化的公告列表
        """
        announcements = []
        
        for item in raw_news_list:
            try:
                if not isinstance(item, dict):
                    continue
                
                # 提取股票信息
                stock_info = item.get('stock', [])
                stock_code = ''
                stock_name = ''
                
                if stock_info and isinstance(stock_info, list) and len(stock_info) > 0:
                    first_stock = stock_info[0]
                    stock_code = first_stock.get('sc', '')
                    stock_name = first_stock.get('sn', '')
                    
                    # 标准化股票代码格式
                    if stock_code:
                        stock_code = f"{stock_code}.HK"
                
                # 构建文件链接
                web_path = item.get('webPath', '')
                file_link = f"https://www1.hkexnews.hk{web_path}" if web_path else ''
                
                # 转换为标准格式
                announcement = {
                    'STOCK_CODE': stock_code,
                    'STOCK_NAME': stock_name,
                    'TITLE': item.get('title', ''),
                    'LONG_TEXT': item.get('lTxt', ''),  # 使用详细分类
                    'DATE_TIME': item.get('relTime', ''),  # 正确的时间字段
                    'FILE_LINK': file_link,
                    'raw_data': item
                }
                announcements.append(announcement)
                
            except Exception as e:
                logger.warning(f"转换公告数据失败: {e}")
                continue
        
        return announcements
    
    def _parse_hkex_datetime(self, dt_str: str) -> Optional[datetime]:
        """
        🔧 修复：准确解析HKEX时间并转换为香港时区
        
        Args:
            dt_str: HKEX API时间字符串
            
        Returns:
            香港时区的datetime对象
        """
        if not dt_str:
            return None
            
        try:
            # 主要格式：'10/09/2025 20:58'
            try:
                naive_dt = datetime.strptime(dt_str, '%d/%m/%Y %H:%M')
            except ValueError:
                try:
                    naive_dt = datetime.strptime(dt_str, '%d/%m/%Y %H:%M:%S')
                except ValueError:
                    try:
                        naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        logger.debug(f"无法解析时间格式: {dt_str}")
                        return None
            
            # 🔧 修复：假设HKEX时间为香港时区
            hk_dt = self.hk_tz.localize(naive_dt)
            return hk_dt
            
        except Exception as e:
            logger.error(f"解析HKEX时间失败: {dt_str}, 错误: {e}")
            return None
    
    def _get_announcement_id(self, announcement: Dict) -> str:
        """
        🔧 修复：生成公告唯一ID
        
        Args:
            announcement: 公告数据
            
        Returns:
            唯一公告ID
        """
        # 优先使用API提供的newsId
        if 'NEWS_ID' in announcement and announcement['NEWS_ID']:
            return f"news_{announcement['NEWS_ID']}"
        
        # 备选方案：基于股票代码+标题+时间生成
        stock_code = announcement.get('STOCK_CODE', 'unknown')
        title = announcement.get('TITLE', 'untitled')
        datetime_str = announcement.get('DATE_TIME', 'no_time')
        
        # 创建确定性哈希
        content = f"{stock_code}_{title}_{datetime_str}"
        hash_id = hashlib.md5(content.encode('utf-8')).hexdigest()[:12]
        return f"hash_{hash_id}"
    
    def filter_new_announcements(self, announcements: List[Dict]) -> List[Dict]:
        """
        🔧 修复版：过滤新公告
        
        主要改进：
        1. 正确的时区处理
        2. 基于ID的重复检测
        3. 更精确的时间比较
        4. 不立即更新时间戳（需要手动调用mark_announcements_processed）
        
        Args:
            announcements: 原始公告列表
            
        Returns:
            新公告列表（去重且未处理过的）
        """
        # 🔧 修复：获取香港时区的当前时间
        now_hk = datetime.now(self.hk_tz)
        
        if not self.last_successful_check:
            # 🔧 修复：首次运行获取最近2小时的公告（与注释一致）
            cutoff_time = now_hk - timedelta(hours=2)
            logger.info("首次运行，获取最近2小时的公告")
        else:
            # 🔧 修复：使用上次成功处理的时间作为基准
            cutoff_time = self.last_successful_check
            logger.debug(f"使用上次成功处理时间作为过滤基准: {cutoff_time}")
        
        new_announcements = []
        skipped_ids = []
        
        for announcement in announcements:
            try:
                # 解析公告时间
                dt_str = announcement.get('DATE_TIME', '')
                hk_dt = self._parse_hkex_datetime(dt_str)
                
                if not hk_dt:
                    logger.warning(f"公告时间解析失败: {announcement.get('TITLE', 'Unknown')}")
                    continue
                
                # 🔧 修复：时间比较（都是香港时区）
                if hk_dt <= cutoff_time:
                    continue  # 跳过旧公告
                
                # 🔧 修复：检查公告ID重复
                announcement_id = self._get_announcement_id(announcement)
                if announcement_id in self.processed_announcement_ids:
                    skipped_ids.append(announcement_id)
                    continue  # 跳过已处理的公告
                
                # 添加到新公告列表
                announcement['_announcement_id'] = announcement_id
                announcement['_parsed_datetime'] = hk_dt
                new_announcements.append(announcement)
                
                logger.debug(f"新公告: {announcement.get('STOCK_CODE', 'N/A')} - "
                           f"{announcement.get('TITLE', 'Unknown')[:50]}... "
                           f"[{announcement_id}]")
                    
            except Exception as e:
                logger.error(f"处理公告时出错: {e}, 公告: {announcement}")
                continue
        
        if skipped_ids:
            logger.debug(f"跳过 {len(skipped_ids)} 个重复公告ID")
        
        logger.info(f"过滤结果: {len(announcements)} -> {len(new_announcements)} 条新公告")
        return new_announcements
    
    def mark_announcements_processed(self, announcement_ids: List[str], success_time: datetime = None):
        """
        🔧 新增：标记公告为已处理
        
        Args:
            announcement_ids: 已处理的公告ID列表
            success_time: 处理成功的时间（香港时区）
        """
        if not success_time:
            success_time = datetime.now(self.hk_tz)
        
        # 添加到已处理ID集合
        for aid in announcement_ids:
            self.processed_announcement_ids.add(aid)
        
        # 🔧 修复：只有在处理成功后才更新时间戳
        self.last_successful_check = success_time
        
        # 清理过大的ID缓存
        if len(self.processed_announcement_ids) > self.max_id_cache_size:
            # 保留最近的一半
            ids_list = list(self.processed_announcement_ids)
            keep_count = self.max_id_cache_size // 2
            self.processed_announcement_ids = set(ids_list[-keep_count:])
            logger.debug(f"清理ID缓存，保留 {keep_count} 个ID")
        
        logger.info(f"✅ 标记 {len(announcement_ids)} 个公告为已处理，更新成功时间戳: {success_time}")
    
    async def start_monitoring(self, on_new_announcements_callback):
        """
        启动持续监听
        
        Args:
            on_new_announcements_callback: 发现新公告时的回调函数
        """
        logger.info("启动HKEX API持续监听")
        
        if not await self.initialize():
            logger.error("监听器初始化失败，无法启动监听")
            return
        
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        try:
            while True:
                try:
                    # 获取最新公告
                    new_announcements = await self.fetch_latest_announcements()
                    
                    if new_announcements:
                        logger.info(f"发现 {len(new_announcements)} 条新公告，调用处理回调")
                        try:
                            await on_new_announcements_callback(new_announcements)
                            consecutive_failures = 0  # 重置失败计数
                        except Exception as e:
                            logger.error(f"公告处理回调失败: {e}")
                            consecutive_failures += 1
                    else:
                        logger.debug("本轮检查无新公告")
                        consecutive_failures = 0  # 重置失败计数
                    
                    # 等待下次检查
                    logger.debug(f"等待 {self.check_interval} 秒后进行下次检查")
                    await asyncio.sleep(self.check_interval)
                    
                except Exception as e:
                    consecutive_failures += 1
                    logger.error(f"监听循环出错 (连续失败 {consecutive_failures} 次): {e}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.critical(f"连续失败达到上限 ({max_consecutive_failures})，停止监听")
                        break
                    
                    # 失败后等待较短时间重试
                    wait_time = min(60, 10 * consecutive_failures)
                    logger.info(f"等待 {wait_time} 秒后重试")
                    await asyncio.sleep(wait_time)
                    
        except KeyboardInterrupt:
            logger.info("接收到中断信号，停止监听")
        except Exception as e:
            logger.critical(f"监听循环发生致命错误: {e}")
        finally:
            await self.close()
            logger.info("HKEX API监听已停止")
    
    def get_status(self) -> Dict[str, Any]:
        """获取监听器状态 - 修复版"""
        return {
            "is_initialized": self.session is not None,
            "last_successful_check": self.last_successful_check.isoformat() if self.last_successful_check else None,
            "last_poll_attempt": self.last_poll_attempt.isoformat() if self.last_poll_attempt else None,
            "processed_announcement_count": len(self.processed_announcement_ids),
            "check_interval": self.check_interval,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "timezone": str(self.hk_tz),
            "api_url": self.base_url,
            "version": "fixed"
        }


# 用于测试的简单回调函数
async def test_callback(announcements: List[Dict]):
    """测试用的公告处理回调"""
    print(f"\n=== 收到 {len(announcements)} 条新公告 ===")
    for i, announcement in enumerate(announcements, 1):
        print(f"{i}. {announcement.get('STOCK_CODE', 'N/A')} - {announcement.get('TITLE', 'Unknown')}")
        print(f"   时间: {announcement.get('DATE_TIME', 'N/A')}")
        print(f"   类型: {announcement.get('LONG_TEXT', 'N/A')}")
        print()


# 测试代码
async def test_monitor():
    """测试监听器功能"""
    config = {
        'realtime_monitoring': {
            'check_interval': 30,  # 30秒测试间隔
            'timeout': 10,
            'retry_attempts': 3,
            'headers': {}
        }
    }
    
    monitor = HKEXAPIMonitor(config)
    
    # 测试单次获取
    print("测试单次获取公告...")
    await monitor.initialize()
    announcements = await monitor.fetch_latest_announcements()
    print(f"获取到 {len(announcements)} 条公告")
    
    if announcements:
        print("前3条公告:")
        for i, ann in enumerate(announcements[:3], 1):
            print(f"{i}. {ann.get('STOCK_CODE', 'N/A')} - {ann.get('TITLE', 'Unknown')[:50]}...")
    
    await monitor.close()
    
    # 如果需要测试持续监听，取消下面的注释
    # print("\n开始持续监听测试...")
    # await monitor.start_monitoring(test_callback)


if __name__ == "__main__":
    # 运行测试
    asyncio.run(test_monitor())
