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
import re
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

        # 休息功能配置
        rest_config = async_config.get('rest', {})
        self.rest_enabled = rest_config.get('enabled', True)
        self.work_duration = rest_config.get('work_minutes', 30) * 60  # 转换为秒
        self.rest_duration = rest_config.get('rest_minutes', 5) * 60   # 转换为秒

        # 休息管理器
        self.work_start_time = time.time()
        self.last_rest_time = 0
        
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

    def _should_rest(self) -> bool:
        """检查是否需要休息"""
        if not self.rest_enabled:
            return False

        current_time = time.time()
        work_elapsed = current_time - self.work_start_time

        # 如果工作时间超过设定时长，需要休息
        return work_elapsed >= self.work_duration

    async def _take_rest(self):
        """执行休息"""
        if not self.rest_enabled:
            return

        current_time = time.time()
        work_elapsed = current_time - self.work_start_time

        self.logger.info(f"🛌 工作了 {work_elapsed/60:.1f} 分钟，开始休息 {self.rest_duration/60:.0f} 分钟以避免被封禁...")

        # 创建休息进度条
        rest_pbar = tqdm(
            total=self.rest_duration,
            desc="休息中",
            unit="秒",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n:.0f}/{total:.0f} 秒 [剩余: {remaining}]"
        )

        # 分段休息，每秒更新一次进度条
        for i in range(int(self.rest_duration)):
            await asyncio.sleep(1)
            rest_pbar.update(1)

        rest_pbar.close()

        # 重置工作开始时间
        self.work_start_time = time.time()
        self.last_rest_time = current_time

        self.logger.info(f"😊 休息完成，继续工作...")
    
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
                                        end_date: datetime, keywords: List[str] = None) -> tuple[List[Dict], str]:
        """获取公告列表和公司名称（同步调用父类方法）"""
        self.logger.info(f"[同步] 获取股票 {stockcode} 的公告列表")
        return super().get_announcement_list(stockcode, start_date, end_date, keywords)
    
    async def async_download_file(self, url: str, filepath: str, title: str = None) -> bool:
        """异步下载文件"""
        filename = os.path.basename(filepath)
        self.logger.debug(f"[异步] 开始下载文件: {filename}")
        await self._ensure_session()

        try:
            status, content = await self._fetch_with_retry(url)

            # 确保目录存在
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # 异步写入文件
            async with aiofiles.open(filepath, 'wb') as f:
                await f.write(content)

            # 输出成功下载的文件信息
            file_size = len(content)
            size_mb = file_size / (1024 * 1024)
            if title:
                self.logger.info(f"✓ 下载完成: {filename} ({size_mb:.2f}MB) - {title}")
            else:
                self.logger.info(f"✓ 下载完成: {filename} ({size_mb:.2f}MB)")

            return True

        except Exception as e:
            self.logger.error(f"✗ 下载失败: {filename} - {str(e)}")
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
        
        self.logger.info(f"开始混合模式下载股票 {stockcode} 的公告（获取列表: 同步，文件下载: 异步并发）")
        
        # 获取公告列表和公司名称
        announcements, stock_name = self.async_get_announcement_list(stockcode, start_date, end_date, keywords)
        
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
            
            # 清理公司名称和公告标题中的特殊字符
            clean_stock_name = re.sub(r'[<>:"/\\|?*]', '-', stock_name)
            clean_title = re.sub(r'[<>:"/\\|?*]', '-', ann['title'])
            
            # 新的文件命名格式：时间——股票代码——公司名称-公告名称
            filename = f"{ann['date']}——{stockcode}——{clean_stock_name}-{clean_title[:filename_length]}.pdf"
            filepath = os.path.join(savepath, filename)
            
            # 检查文件是否已存在
            if os.path.exists(filepath) and not overwrite:
                continue
            
            download_tasks.append((ann['link'], filepath, ann['title']))
        
        # 并发下载所有文件
        if download_tasks:
            self.logger.info(f"准备下载 {len(download_tasks)} 个新文件")

            # 创建文件下载进度条
            from tqdm.asyncio import tqdm as async_tqdm

            # 使用进度条和详细信息
            async def download_with_progress(task_info, pbar):
                url, filepath, title = task_info
                success = await self.async_download_file(url, filepath, title)
                pbar.update(1)
                if success:
                    return 1
                return 0

            # 创建文件级别的进度条
            file_pbar = async_tqdm(
                total=len(download_tasks),
                desc=f"下载文件[{stockcode}]",
                unit="文件",
                leave=False
            )

            # 并发执行所有下载任务
            results = await asyncio.gather(
                *[download_with_progress(task, file_pbar) for task in download_tasks],
                return_exceptions=True
            )

            file_pbar.close()

            # 统计成功数量
            for result in results:
                if isinstance(result, int):
                    download_count += result
                else:
                    self.logger.error(f"下载任务异常: {result}")
        
        self.logger.info(f"股票 {stockcode} 完成下载，成功下载 {download_count} 个文件")
        return base_path, download_count
    
    async def async_download_multiple_stocks(self, task: Dict[str, Any], stock_codes: List[str]) -> Tuple[str, int]:
        """混合模式下载多个股票的公告（获取列表: 同步顺序，文件下载: 异步并发）"""
        if not stock_codes:
            raise ValueError("股票代码列表为空")

        self.logger.info(f"开始混合模式批量下载 {len(stock_codes)} 只股票的公告（获取列表: 同步顺序，文件下载: 异步并发）")

        total_downloaded = 0
        base_save_path = self.config.get('settings', 'save_path')

        # 创建进度条
        pbar = tqdm(total=len(stock_codes), desc="下载进度", unit="股票")

        # 顺序处理每个股票（获取公告列表是同步的）
        for i, stock_code in enumerate(stock_codes, 1):
            try:
                # 检查是否需要休息
                if self._should_rest():
                    pbar.set_description("休息中...")
                    await self._take_rest()
                    pbar.set_description("下载进度")

                # 为每个股票创建单独的任务
                stock_task = task.copy()
                stock_task['stock_code'] = stock_code

                # 顺序执行单个股票的下载（内部文件下载仍然是异步并发的）
                save_path, count = await self.async_download_stock_announcements(stock_task)

                total_downloaded += count
                self.stats['success'] += 1
                pbar.update(1)
                pbar.set_postfix({'当前': stock_code, '文件数': count, '进度': f'{i}/{len(stock_codes)}'})

            except Exception as e:
                self.stats['failed'] += 1
                self.logger.error(f"股票 {stock_code} 下载失败: {str(e)}")
                pbar.update(1)

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