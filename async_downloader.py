#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告异步下载器
提供高性能的并发下载能力，同时精确控制请求频率以避免被封禁
作者：Victor Suen
版本：1.0
"""

import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
from tqdm.asyncio import tqdm

# 导入基础类
from main import HKEXDownloader, ConfigManager, AnnouncementClassifier


class RateLimiter:
    """异步速率限制器"""
    
    def __init__(self, rate: float, per: float = 1.0):
        """
        初始化速率限制器
        
        Args:
            rate: 允许的请求数
            per: 时间窗口（秒）
        """
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """获取许可，可能会等待"""
        async with self._lock:
            current = time.monotonic()
            time_passed = current - self.last_check
            self.last_check = current
            
            # 恢复令牌
            self.allowance += time_passed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate
            
            # 如果没有足够的令牌，等待
            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) * (self.per / self.rate)
                await asyncio.sleep(sleep_time)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0


class AsyncHKEXDownloader(HKEXDownloader):
    """异步HKEX公告下载器"""
    
    def __init__(self, config_manager: ConfigManager):
        """初始化异步下载器"""
        super().__init__(config_manager)
        
        # 异步相关配置
        async_config = self.config.get('async') or {}
        self.max_concurrent = async_config.get('max_concurrent', 10)
        self.requests_per_second = async_config.get('requests_per_second', 2)
        self.async_timeout = async_config.get('timeout', 30)
        self.min_delay = async_config.get('min_delay', 0.5)
        self.max_delay = async_config.get('max_delay', 1.5)
        self.backoff_on_429 = async_config.get('backoff_on_429', 60)
        
        # 创建控制器
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.rate_limiter = RateLimiter(self.requests_per_second)
        
        # 异步会话（延迟创建）
        self._session = None
        
        # 统计信息
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'retried': 0,
            'rate_limited': 0
        }
        
        # 设置异步日志
        self.logger = logging.getLogger('async_downloader')
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()
    
    async def _ensure_session(self):
        """确保会话已创建"""
        if self._session is None:
            # 创建连接器，限制连接数
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent * 2,
                limit_per_host=self.max_concurrent
            )
            
            # 创建会话
            timeout = aiohttp.ClientTimeout(total=self.async_timeout)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=self.headers
            )
    
    async def close(self):
        """关闭会话"""
        if self._session and not self._session.closed:
            await self._session.close()
            # 等待一小段时间确保连接正确关闭
            await asyncio.sleep(0.1)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logging.getLogger('async_downloader'), logging.WARNING)
    )
    async def _fetch_with_retry(self, url: str, **kwargs) -> Tuple[int, bytes]:
        """带重试的异步请求"""
        async with self.semaphore:
            # 速率限制
            await self.rate_limiter.acquire()
            
            # 随机延迟（模拟人类行为）
            delay = random.uniform(self.min_delay, self.max_delay)
            await asyncio.sleep(delay)
            
            # 发送请求
            async with self._session.get(url, **kwargs) as response:
                # 处理429状态码（请求过多）
                if response.status == 429:
                    self.stats['rate_limited'] += 1
                    self.logger.warning(f"收到429状态码，等待{self.backoff_on_429}秒...")
                    await asyncio.sleep(self.backoff_on_429)
                    raise aiohttp.ClientError("Rate limited")
                
                # 确保成功状态码
                response.raise_for_status()
                
                # 返回状态码和内容
                return response.status, await response.read()
    
    def async_get_stockid(self, stockcode: str) -> str:
        """获取股票ID（同步调用父类方法）"""
        self.logger.info(f"[同步] 获取股票代码 {stockcode} 的 StockID")
        return super().get_stockid(stockcode)
    
    def async_get_announcement_list(self, stockcode: str, start_date: datetime, 
                                        end_date: datetime, keywords: List[str] = None) -> List[Dict]:
        """获取公告列表（同步调用父类方法）"""
        self.logger.info(f"[同步] 获取股票 {stockcode} 的公告列表")
        return super().get_announcement_list(stockcode, start_date, end_date, keywords)
    
    async def async_download_file(self, url: str, filepath: str) -> bool:
        """异步下载文件"""
        self.logger.debug(f"[异步] 开始下载文件: {os.path.basename(filepath)}")
        await self._ensure_session()
        
        try:
            status, content = await self._fetch_with_retry(url)
            
            # 确保目录存在
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # 异步写入文件
            async with aiofiles.open(filepath, 'wb') as f:
                await f.write(content)
            
            return True
            
        except Exception as e:
            self.logger.error(f"下载文件失败 {url}: {str(e)}")
            return False
    
    async def async_download_stock_announcements(self, task: Dict[str, Any]) -> Tuple[str, int]:
        """异步下载单个股票的所有公告"""
        stockcode = task.get('stock_code')
        if not stockcode:
            raise ValueError("任务配置缺少 stock_code")
        
        # 解析日期
        start_date = self.config.parse_date(task.get('start_date', '2024-01-01'))
        end_date = self.config.parse_date(task.get('end_date', 'today'))
        keywords = task.get('keywords', [])
        
        self.logger.info(f"开始混合模式下载股票 {stockcode} 的公告（获取列表: 同步，文件下载: 异步）")
        
        # 获取公告列表
        announcements = self.async_get_announcement_list(stockcode, start_date, end_date, keywords)
        
        if not announcements:
            return "", 0
        
        # 创建保存目录
        save_path = self.config.get('settings', 'save_path')
        base_path = os.path.join(save_path, 'HKEX', stockcode)
        
        # 下载公告
        download_count = 0
        filename_length = self.config.get('settings', 'filename_length', 220)
        overwrite = self.config.get('advanced', 'overwrite_existing', False)
        
        # 创建下载任务
        download_tasks = []
        for ann in announcements:
            # 使用分类器确定文件保存路径
            if self.classifier.enabled:
                main_category, sub_category = self.classifier.classify_announcement(ann['title'])
                category_path = self.classifier.get_folder_path(main_category, sub_category)
                savepath = os.path.join(base_path, category_path)
            else:
                savepath = base_path
            
            filepath = os.path.join(savepath, f"{ann['date']}_{stockcode}-{ann['title'][:filename_length]}.pdf")
            
            # 检查文件是否已存在
            if os.path.exists(filepath) and not overwrite:
                continue
            
            download_tasks.append((ann['link'], filepath, ann['title']))
        
        # 并发下载所有文件
        if download_tasks:
            self.logger.info(f"准备下载 {len(download_tasks)} 个新文件")
            
            # 使用进度条
            async def download_with_progress(task_info):
                url, filepath, title = task_info
                success = await self.async_download_file(url, filepath)
                if success:
                    return 1
                return 0
            
            # 并发执行所有下载任务
            results = await asyncio.gather(
                *[download_with_progress(task) for task in download_tasks],
                return_exceptions=True
            )
            
            # 统计成功数量
            for result in results:
                if isinstance(result, int):
                    download_count += result
                else:
                    self.logger.error(f"下载任务异常: {result}")
        
        self.logger.info(f"股票 {stockcode} 完成下载，成功下载 {download_count} 个文件")
        return base_path, download_count
    
    async def async_download_multiple_stocks(self, task: Dict[str, Any], stock_codes: List[str]) -> Tuple[str, int]:
        """异步下载多个股票的公告"""
        if not stock_codes:
            raise ValueError("股票代码列表为空")
        
        self.logger.info(f"开始混合模式批量下载 {len(stock_codes)} 只股票的公告（获取列表: 同步，文件下载: 异步）")
        
        total_downloaded = 0
        base_save_path = self.config.get('settings', 'save_path')
        
        # 创建进度条
        pbar = tqdm(total=len(stock_codes), desc="下载进度", unit="股票")
        
        # 创建所有股票的下载任务
        async def download_single_stock_with_progress(stock_code: str):
            try:
                # 为每个股票创建单独的任务
                stock_task = task.copy()
                stock_task['stock_code'] = stock_code
                
                save_path, count = await self.async_download_stock_announcements(stock_task)
                
                self.stats['success'] += 1
                pbar.update(1)
                pbar.set_postfix({'当前': stock_code, '文件数': count})
                
                return count
                
            except Exception as e:
                self.stats['failed'] += 1
                self.logger.error(f"股票 {stock_code} 下载失败: {str(e)}")
                pbar.update(1)
                return 0
        
        # 并发下载所有股票
        results = await asyncio.gather(
            *[download_single_stock_with_progress(code) for code in stock_codes],
            return_exceptions=True
        )
        
        # 统计结果
        for result in results:
            if isinstance(result, int):
                total_downloaded += result
        
        pbar.close()
        
        # 输出统计信息
        self.logger.info(f"混合模式批量下载完成！")
        self.logger.info(f"  总股票数: {len(stock_codes)}")
        self.logger.info(f"  成功: {self.stats['success']}")
        self.logger.info(f"  失败: {self.stats['failed']}")
        self.logger.info(f"  下载文件: {total_downloaded} 个")
        self.logger.info(f"  速率限制触发: {self.stats['rate_limited']} 次")
        
        return os.path.join(base_save_path, 'HKEX'), total_downloaded
    
    async def async_download_announcements(self, task: Dict[str, Any]) -> Tuple[str, int]:
        """异步下载公告的主入口"""
        try:
            # 检查是否从数据库获取股票
            if task.get('from_database', False):
                # 暂时使用同步方法获取股票列表，然后异步下载
                stock_codes = self.config.get_stocks_from_database(task.get('query'))
                if not stock_codes:
                    return "", 0
                return await self.async_download_multiple_stocks(task, stock_codes)
            
            # 解析任务参数
            stockcode = task.get('stock_code')
            if not stockcode:
                raise ValueError("任务配置缺少 stock_code")
            
            # 支持单个股票代码（字符串）和多个股票代码（列表）
            if isinstance(stockcode, list):
                return await self.async_download_multiple_stocks(task, stockcode)
            else:
                return await self.async_download_stock_announcements(task)
                
        except Exception as e:
            raise Exception(f"异步下载过程中出现错误: {str(e)}")


def run_async_download(config_manager: ConfigManager, task: Dict[str, Any]) -> Tuple[str, int]:
    """运行异步下载任务的同步包装器"""
    async def _run():
        async with AsyncHKEXDownloader(config_manager) as downloader:
            return await downloader.async_download_announcements(task)
    
    return asyncio.run(_run())