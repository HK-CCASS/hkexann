"""
HKEX公告API监听器
实时轮询港交所公告API，获取最新公告数据
"""

import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import aiohttp
from aiohttp import ClientTimeout

logger = logging.getLogger(__name__)


class HKEXAPIMonitor:
    """
    HKEX公告API监听器
    
    实时轮询港交所公告API，获取最新公告数据。
    支持时间戳缓存避免、新公告检测、错误重试等功能。
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
        self.last_check_timestamp: Optional[datetime] = None
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
        
        logger.info(f"HKEX API监听器初始化完成")
        logger.info(f"  API URL: {self.base_url}")
        logger.info(f"  检查间隔: {self.check_interval}秒")
        logger.info(f"  超时时间: {self.timeout}秒")
        logger.info(f"  重试次数: {self.retry_attempts}")
    
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
        获取最新公告数据 (实时监听API)
        
        Args:
            stock_codes: 监听的股票代码列表，用于后续过滤（监听API不需要指定股票）
            
        Returns:
            包含新公告的列表
        """
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
    
    def filter_new_announcements(self, announcements: List[Dict]) -> List[Dict]:
        """
        过滤出新公告（相对于上次检查）
        
        Args:
            announcements: 原始公告列表
            
        Returns:
            新公告列表
        """
        if not self.last_check_timestamp:
            # 首次运行，返回最近1小时的公告
            cutoff_time = datetime.now() - timedelta(hours=2)
            logger.info("首次运行，获取最近1小时的公告")
        else:
            cutoff_time = self.last_check_timestamp
            logger.debug(f"使用上次检查时间作为过滤基准: {cutoff_time}")
        
        new_announcements = []
        for announcement in announcements:
            try:
                # 解析公告时间：'09/01/2025 18:30:00'
                dt_str = announcement.get('DATE_TIME', '')
                if not dt_str:
                    logger.warning(f"公告缺少时间信息: {announcement.get('TITLE', 'Unknown')}")
                    continue
                
                # 尝试解析时间格式
                try:
                    # HKEX API时间格式：10/09/2025 20:58
                    dt = datetime.strptime(dt_str, '%d/%m/%Y %H:%M')
                except ValueError:
                    # 尝试其他可能的时间格式
                    try:
                        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            # 尝试带秒的格式
                            dt = datetime.strptime(dt_str, '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            try:
                                # 尝试ISO格式
                                dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
                            except ValueError:
                                logger.debug(f"无法解析公告时间格式: {dt_str}")
                                continue
                
                if dt > cutoff_time:
                    new_announcements.append(announcement)
                    logger.debug(f"新公告: {announcement.get('STOCK_CODE', 'N/A')} - {announcement.get('TITLE', 'Unknown')[:50]}...")
                    
            except Exception as e:
                logger.error(f"处理公告时间时出错: {e}, 公告: {announcement}")
                continue
        
        # 更新检查时间戳
        self.last_check_timestamp = datetime.now()
        
        return new_announcements
    
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
        """获取监听器状态"""
        return {
            "is_initialized": self.session is not None,
            "last_check_time": self.last_check_timestamp.isoformat() if self.last_check_timestamp else None,
            "check_interval": self.check_interval,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "api_url": self.base_url
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
