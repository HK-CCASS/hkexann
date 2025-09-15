"""
下载器抽象层
定义统一的下载器接口和策略模式实现
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, AsyncIterator
from dataclasses import dataclass
from enum import Enum
import logging
from pathlib import Path
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class DownloadStatus(Enum):
    """下载状态枚举"""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class DownloadTask:
    """下载任务数据类"""
    url: str
    announcement: Dict[str, Any]
    priority: int = 0
    retry_count: int = 0
    status: DownloadStatus = DownloadStatus.PENDING
    error_message: Optional[str] = None
    file_path: Optional[Path] = None

    def __lt__(self, other):
        """支持优先级排序"""
        return self.priority > other.priority


@dataclass
class DownloadResult:
    """下载结果数据类"""
    task: DownloadTask
    success: bool
    file_path: Optional[Path] = None
    file_size: int = 0
    download_time: float = 0
    error: Optional[str] = None


class DownloadStrategy(ABC):
    """下载策略抽象基类"""

    @abstractmethod
    async def download(self, task: DownloadTask) -> DownloadResult:
        """
        执行下载任务

        Args:
            task: 下载任务

        Returns:
            下载结果
        """
        pass

    @abstractmethod
    async def validate(self, content: bytes) -> bool:
        """
        验证下载内容

        Args:
            content: 下载的内容

        Returns:
            是否有效
        """
        pass


class StandardDownloadStrategy(DownloadStrategy):
    """标准下载策略"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化标准下载策略

        Args:
            config: 下载配置
        """
        self.config = config
        self.timeout = config.get('timeout', 30)
        self.headers = {
            'User-Agent': config.get('user_agent',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def download(self, task: DownloadTask) -> DownloadResult:
        """执行下载任务"""
        start_time = time.time()
        task.status = DownloadStatus.DOWNLOADING

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    task.url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    response.raise_for_status()
                    content = await response.read()

                    # 验证内容
                    if not await self.validate(content):
                        raise ValueError("下载内容验证失败")

                    download_time = time.time() - start_time
                    task.status = DownloadStatus.COMPLETED

                    return DownloadResult(
                        task=task,
                        success=True,
                        file_size=len(content),
                        download_time=download_time
                    )

        except Exception as e:
            task.status = DownloadStatus.FAILED
            task.error_message = str(e)
            logger.error(f"下载失败 {task.url}: {e}")

            return DownloadResult(
                task=task,
                success=False,
                error=str(e),
                download_time=time.time() - start_time
            )

    async def validate(self, content: bytes) -> bool:
        """验证下载内容"""
        # 检查是否为PDF
        if content[:4] != b'%PDF':
            return False

        # 检查文件大小
        min_size = self.config.get('min_file_size', 1024)  # 最小1KB
        max_size = self.config.get('max_file_size', 100 * 1024 * 1024)  # 最大100MB

        if len(content) < min_size or len(content) > max_size:
            return False

        return True


class RateLimitedDownloadStrategy(StandardDownloadStrategy):
    """速率限制下载策略"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化速率限制下载策略

        Args:
            config: 下载配置
        """
        super().__init__(config)
        self.requests_per_second = config.get('requests_per_second', 5)
        self.min_delay = config.get('min_delay', 0.88)
        self.max_delay = config.get('max_delay', 2.68)
        self.last_request_time = 0
        self.request_count = 0
        self.window_start = time.time()

    async def download(self, task: DownloadTask) -> DownloadResult:
        """执行速率限制的下载"""
        # 应用速率限制
        await self._apply_rate_limit()

        # 执行下载
        return await super().download(task)

    async def _apply_rate_limit(self):
        """应用速率限制"""
        current_time = time.time()

        # 计算当前窗口的请求数
        if current_time - self.window_start >= 1.0:
            # 重置窗口
            self.window_start = current_time
            self.request_count = 0

        # 如果达到速率限制，等待
        if self.request_count >= self.requests_per_second:
            wait_time = 1.0 - (current_time - self.window_start)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                self.window_start = time.time()
                self.request_count = 0

        # 应用最小延迟
        if self.last_request_time > 0:
            elapsed = current_time - self.last_request_time
            if elapsed < self.min_delay:
                await asyncio.sleep(self.min_delay - elapsed)

        # 记录请求
        self.request_count += 1
        self.last_request_time = time.time()


class ProxyDownloadStrategy(RateLimitedDownloadStrategy):
    """代理下载策略"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化代理下载策略

        Args:
            config: 下载配置，包含代理设置
        """
        super().__init__(config)
        self.proxy = config.get('proxy')
        self.proxy_rotation = config.get('proxy_rotation', [])
        self.current_proxy_index = 0

    async def download(self, task: DownloadTask) -> DownloadResult:
        """使用代理执行下载"""
        # 获取当前代理
        proxy = self._get_next_proxy()

        # 如果有代理，修改请求
        if proxy:
            # 注入代理到任务中
            original_url = task.url
            # 这里需要根据实际的代理协议调整
            logger.info(f"使用代理 {proxy} 下载")

        return await super().download(task)

    def _get_next_proxy(self) -> Optional[str]:
        """获取下一个代理"""
        if self.proxy:
            return self.proxy

        if self.proxy_rotation:
            proxy = self.proxy_rotation[self.current_proxy_index]
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_rotation)
            return proxy

        return None


class IDownloader(ABC):
    """下载器接口"""

    @abstractmethod
    async def download_single(self, announcement: Dict[str, Any]) -> DownloadResult:
        """
        下载单个公告

        Args:
            announcement: 公告信息

        Returns:
            下载结果
        """
        pass

    @abstractmethod
    async def download_batch(self, announcements: List[Dict[str, Any]]) -> List[DownloadResult]:
        """
        批量下载公告

        Args:
            announcements: 公告列表

        Returns:
            下载结果列表
        """
        pass

    @abstractmethod
    async def download_stream(self, announcements: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[DownloadResult]:
        """
        流式下载公告

        Args:
            announcements: 公告异步迭代器

        Yields:
            下载结果
        """
        pass

    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取下载统计信息

        Returns:
            统计信息字典
        """
        pass


class UnifiedDownloader(IDownloader):
    """统一下载器实现"""

    def __init__(self, config: Dict[str, Any], strategy: Optional[DownloadStrategy] = None):
        """
        初始化统一下载器

        Args:
            config: 下载器配置
            strategy: 下载策略（可选）
        """
        self.config = config
        self.strategy = strategy or self._create_strategy()

        # 并发控制
        self.max_concurrent = config.get('max_concurrent', 5)
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # 统计信息
        self.stats = {
            'total_downloads': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_downloads': 0,
            'total_bytes': 0,
            'total_time': 0,
            'errors': {}
        }

        # 任务队列
        self.task_queue = asyncio.PriorityQueue()
        self.workers = []

    def _create_strategy(self) -> DownloadStrategy:
        """根据配置创建下载策略"""
        strategy_type = self.config.get('strategy', 'rate_limited')

        if strategy_type == 'standard':
            return StandardDownloadStrategy(self.config)
        elif strategy_type == 'proxy':
            return ProxyDownloadStrategy(self.config)
        else:
            return RateLimitedDownloadStrategy(self.config)

    async def download_single(self, announcement: Dict[str, Any]) -> DownloadResult:
        """下载单个公告"""
        # 创建下载任务
        task = DownloadTask(
            url=announcement.get('url', ''),
            announcement=announcement,
            priority=announcement.get('priority', 0)
        )

        # 使用信号量控制并发
        async with self.semaphore:
            result = await self.strategy.download(task)

        # 更新统计
        self._update_statistics(result)

        return result

    async def download_batch(self, announcements: List[Dict[str, Any]]) -> List[DownloadResult]:
        """批量下载公告"""
        # 创建任务
        tasks = []
        for ann in announcements:
            task = DownloadTask(
                url=ann.get('url', ''),
                announcement=ann,
                priority=ann.get('priority', 0)
            )
            tasks.append(task)

        # 并发下载
        results = await asyncio.gather(
            *[self._download_with_semaphore(task) for task in tasks],
            return_exceptions=True
        )

        # 处理结果
        download_results = []
        for result in results:
            if isinstance(result, Exception):
                # 处理异常
                failed_result = DownloadResult(
                    task=DownloadTask(url='', announcement={}),
                    success=False,
                    error=str(result)
                )
                download_results.append(failed_result)
            else:
                download_results.append(result)
                self._update_statistics(result)

        return download_results

    async def _download_with_semaphore(self, task: DownloadTask) -> DownloadResult:
        """使用信号量控制的下载"""
        async with self.semaphore:
            return await self.strategy.download(task)

    async def download_stream(self, announcements: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[DownloadResult]:
        """流式下载公告"""
        # 启动工作协程
        worker_count = min(self.max_concurrent, 5)
        self.workers = [
            asyncio.create_task(self._worker(f"worker-{i}"))
            for i in range(worker_count)
        ]

        # 生产者：将公告加入队列
        async def producer():
            async for ann in announcements:
                task = DownloadTask(
                    url=ann.get('url', ''),
                    announcement=ann,
                    priority=ann.get('priority', 0)
                )
                await self.task_queue.put(task)

            # 发送结束信号
            for _ in range(worker_count):
                await self.task_queue.put(None)

        # 启动生产者
        producer_task = asyncio.create_task(producer())

        # 收集结果
        result_queue = asyncio.Queue()

        # 修改worker以将结果放入队列
        async def worker_wrapper(name: str):
            while True:
                task = await self.task_queue.get()
                if task is None:
                    break

                result = await self.strategy.download(task)
                self._update_statistics(result)
                await result_queue.put(result)

        # 替换原有workers
        self.workers = [
            asyncio.create_task(worker_wrapper(f"worker-{i}"))
            for i in range(worker_count)
        ]

        # 消费结果
        workers_done = False
        while not workers_done or not result_queue.empty():
            try:
                result = await asyncio.wait_for(result_queue.get(), timeout=1.0)
                yield result
            except asyncio.TimeoutError:
                # 检查workers是否完成
                workers_done = all(w.done() for w in self.workers)

        # 等待所有任务完成
        await producer_task
        await asyncio.gather(*self.workers)

    async def _worker(self, name: str):
        """工作协程"""
        while True:
            task = await self.task_queue.get()
            if task is None:
                break

            try:
                result = await self.strategy.download(task)
                self._update_statistics(result)
                logger.debug(f"{name} 完成下载: {task.url}")
            except Exception as e:
                logger.error(f"{name} 下载失败: {e}")
            finally:
                self.task_queue.task_done()

    def _update_statistics(self, result: DownloadResult):
        """更新统计信息"""
        self.stats['total_downloads'] += 1

        if result.success:
            self.stats['successful_downloads'] += 1
            self.stats['total_bytes'] += result.file_size
            self.stats['total_time'] += result.download_time
        else:
            self.stats['failed_downloads'] += 1
            error_type = type(result.error).__name__ if result.error else 'Unknown'
            self.stats['errors'][error_type] = self.stats['errors'].get(error_type, 0) + 1

    def get_statistics(self) -> Dict[str, Any]:
        """获取下载统计信息"""
        stats = self.stats.copy()

        # 计算平均值
        if stats['successful_downloads'] > 0:
            stats['average_size'] = stats['total_bytes'] / stats['successful_downloads']
            stats['average_time'] = stats['total_time'] / stats['successful_downloads']
            stats['average_speed'] = stats['total_bytes'] / stats['total_time'] if stats['total_time'] > 0 else 0
        else:
            stats['average_size'] = 0
            stats['average_time'] = 0
            stats['average_speed'] = 0

        # 格式化大小
        stats['total_bytes_readable'] = self._format_size(stats['total_bytes'])
        stats['average_size_readable'] = self._format_size(stats.get('average_size', 0))
        stats['average_speed_readable'] = f"{self._format_size(stats.get('average_speed', 0))}/s"

        return stats

    def _format_size(self, size: float) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} TB"


class DownloadOrchestrator:
    """下载编排器 - 协调多个下载器"""

    def __init__(self):
        """初始化下载编排器"""
        self.downloaders: Dict[str, IDownloader] = {}
        self.default_downloader: Optional[str] = None

    def register_downloader(self, name: str, downloader: IDownloader, is_default: bool = False):
        """
        注册下载器

        Args:
            name: 下载器名称
            downloader: 下载器实例
            is_default: 是否设为默认
        """
        self.downloaders[name] = downloader
        if is_default or self.default_downloader is None:
            self.default_downloader = name

    def get_downloader(self, name: Optional[str] = None) -> IDownloader:
        """
        获取下载器

        Args:
            name: 下载器名称，如果不指定则返回默认下载器

        Returns:
            下载器实例
        """
        if name is None:
            name = self.default_downloader

        if name not in self.downloaders:
            raise ValueError(f"下载器不存在: {name}")

        return self.downloaders[name]

    async def download_with_fallback(
        self,
        announcement: Dict[str, Any],
        downloader_order: Optional[List[str]] = None
    ) -> DownloadResult:
        """
        使用失败回退机制下载

        Args:
            announcement: 公告信息
            downloader_order: 下载器尝试顺序

        Returns:
            下载结果
        """
        if downloader_order is None:
            downloader_order = list(self.downloaders.keys())

        last_error = None

        for downloader_name in downloader_order:
            if downloader_name not in self.downloaders:
                continue

            try:
                downloader = self.downloaders[downloader_name]
                result = await downloader.download_single(announcement)

                if result.success:
                    logger.info(f"使用 {downloader_name} 下载成功")
                    return result

                last_error = result.error
            except Exception as e:
                last_error = str(e)
                logger.warning(f"下载器 {downloader_name} 失败: {e}")

        # 所有下载器都失败
        return DownloadResult(
            task=DownloadTask(url=announcement.get('url', ''), announcement=announcement),
            success=False,
            error=f"所有下载器都失败: {last_error}"
        )

    def get_combined_statistics(self) -> Dict[str, Any]:
        """获取所有下载器的合并统计"""
        combined_stats = {
            'downloaders': {},
            'total': {
                'downloads': 0,
                'successful': 0,
                'failed': 0,
                'bytes': 0
            }
        }

        for name, downloader in self.downloaders.items():
            stats = downloader.get_statistics()
            combined_stats['downloaders'][name] = stats

            # 合并总计
            combined_stats['total']['downloads'] += stats.get('total_downloads', 0)
            combined_stats['total']['successful'] += stats.get('successful_downloads', 0)
            combined_stats['total']['failed'] += stats.get('failed_downloads', 0)
            combined_stats['total']['bytes'] += stats.get('total_bytes', 0)

        return combined_stats