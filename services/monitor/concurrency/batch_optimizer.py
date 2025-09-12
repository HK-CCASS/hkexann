"""
批量处理并发优化器

这个模块优化批量处理任务的并发性能，通过智能分批、动态调度和资源平衡，
最大化系统吞吐量同时避免资源过载。

主要功能：
- 智能任务分批策略
- 动态并发度调节
- 资源感知调度
- 性能监控和优化

作者: HKEX分析团队  
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import math
from typing import Dict, Any, List, Callable, Optional, Tuple, Union, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import statistics
import psutil
from concurrent.futures import ThreadPoolExecutor

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

T = TypeVar('T')


class BatchStrategy(Enum):
    """批处理策略枚举"""
    FIXED_SIZE = "fixed_size"           # 固定大小分批
    ADAPTIVE_SIZE = "adaptive_size"     # 自适应大小分批
    RESOURCE_AWARE = "resource_aware"   # 资源感知分批
    TIME_BASED = "time_based"          # 基于时间分批


class ProcessingMode(Enum):
    """处理模式枚举"""
    ASYNC_ONLY = "async_only"          # 仅异步处理
    THREAD_POOL = "thread_pool"        # 线程池处理
    MIXED_MODE = "mixed_mode"          # 混合模式


@dataclass
class BatchConfig:
    """批处理配置"""
    strategy: BatchStrategy = BatchStrategy.ADAPTIVE_SIZE
    base_batch_size: int = 50                    # 基础批大小
    max_batch_size: int = 200                   # 最大批大小
    min_batch_size: int = 10                    # 最小批大小
    max_concurrent_batches: int = 5             # 最大并发批数
    processing_mode: ProcessingMode = ProcessingMode.ASYNC_ONLY
    thread_pool_size: int = 4                   # 线程池大小
    timeout_seconds: float = 300.0              # 批处理超时
    performance_target: float = 0.8             # 性能目标（成功率）
    resource_threshold: float = 0.75            # 资源使用阈值


@dataclass
class BatchMetrics:
    """批处理指标"""
    batch_id: str
    size: int
    start_time: float
    end_time: Optional[float] = None
    success_count: int = 0
    failure_count: int = 0
    processing_time: float = 0.0
    throughput: float = 0.0  # 每秒处理数
    errors: List[str] = field(default_factory=list)


@dataclass 
class SystemResources:
    """系统资源状态"""
    cpu_percent: float
    memory_percent: float
    available_memory_gb: float
    disk_io_percent: float
    network_io_percent: float = 0.0
    timestamp: float = field(default_factory=time.time)


class OptimizedBatchProcessor(Generic[T]):
    """
    优化批量处理器
    
    实现智能的批量处理优化，包括：
    1. 自适应批大小调节
    2. 动态并发控制
    3. 资源感知调度
    4. 性能监控和反馈
    """
    
    def __init__(self, config: BatchConfig = None):
        """
        初始化批量处理器
        
        Args:
            config: 批处理配置
        """
        self.config = config or BatchConfig()
        
        # 处理统计
        self.metrics_history: deque[BatchMetrics] = deque(maxlen=1000)
        self.current_batches: Dict[str, BatchMetrics] = {}
        
        # 自适应参数
        self.current_batch_size = self.config.base_batch_size
        self.current_concurrency = min(self.config.max_concurrent_batches, 3)
        
        # 系统资源监控
        self.resource_history: deque[SystemResources] = deque(maxlen=100)
        
        # 线程池（如果需要）
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        if self.config.processing_mode in [ProcessingMode.THREAD_POOL, ProcessingMode.MIXED_MODE]:
            self.thread_pool = ThreadPoolExecutor(max_workers=self.config.thread_pool_size)
        
        # 信号量控制并发
        self.concurrency_semaphore = asyncio.Semaphore(self.current_concurrency)
        
        # 启动资源监控
        self._monitoring_task: Optional[asyncio.Task] = None
        
        logger.info("⚡ 优化批量处理器初始化完成")
        self._log_config()

    def _log_config(self):
        """记录配置信息"""
        logger.info(f"🔧 批处理配置:")
        logger.info(f"  策略: {self.config.strategy.value}")
        logger.info(f"  基础批大小: {self.config.base_batch_size}")
        logger.info(f"  最大并发: {self.config.max_concurrent_batches}")
        logger.info(f"  处理模式: {self.config.processing_mode.value}")

    async def start_monitoring(self):
        """启动资源监控"""
        if self._monitoring_task is None:
            self._monitoring_task = asyncio.create_task(self._resource_monitoring_loop())

    async def stop_monitoring(self):
        """停止资源监控"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            self._monitoring_task = None

    async def _resource_monitoring_loop(self):
        """资源监控循环"""
        while True:
            try:
                # 收集系统资源信息
                resources = self._collect_system_resources()
                self.resource_history.append(resources)
                
                # 根据资源状况调整并发度
                await self._adjust_concurrency_based_on_resources(resources)
                
                await asyncio.sleep(5.0)  # 5秒间隔
                
            except Exception as e:
                logger.error(f"资源监控异常: {e}")
                await asyncio.sleep(10.0)

    def _collect_system_resources(self) -> SystemResources:
        """收集系统资源状态"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # 内存使用情况
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            available_memory_gb = memory.available / (1024**3)
            
            # 磁盘IO（简化）
            disk_io = psutil.disk_io_counters()
            disk_io_percent = min(100.0, (disk_io.read_bytes + disk_io.write_bytes) / (1024**3) * 10) if disk_io else 0
            
            return SystemResources(
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                available_memory_gb=available_memory_gb,
                disk_io_percent=disk_io_percent
            )
            
        except Exception as e:
            logger.warning(f"收集系统资源失败: {e}")
            return SystemResources(cpu_percent=0, memory_percent=0, available_memory_gb=1.0, disk_io_percent=0)

    async def _adjust_concurrency_based_on_resources(self, resources: SystemResources):
        """根据资源状况调整并发度"""
        # 计算资源压力
        resource_pressure = (
            resources.cpu_percent / 100.0 * 0.4 +
            resources.memory_percent / 100.0 * 0.4 +
            resources.disk_io_percent / 100.0 * 0.2
        )
        
        # 调整并发度
        if resource_pressure > self.config.resource_threshold:
            # 资源紧张，降低并发
            new_concurrency = max(1, self.current_concurrency - 1)
        elif resource_pressure < 0.5:
            # 资源充足，可以增加并发
            new_concurrency = min(self.config.max_concurrent_batches, self.current_concurrency + 1)
        else:
            new_concurrency = self.current_concurrency
        
        if new_concurrency != self.current_concurrency:
            await self._update_concurrency(new_concurrency)
            logger.info(f"📊 根据资源调整并发度: {self.current_concurrency} -> {new_concurrency} "
                       f"(资源压力: {resource_pressure:.2f})")

    async def _update_concurrency(self, new_concurrency: int):
        """更新并发度"""
        # 创建新的信号量
        self.concurrency_semaphore = asyncio.Semaphore(new_concurrency)
        self.current_concurrency = new_concurrency

    def _calculate_optimal_batch_size(self, total_items: int) -> int:
        """计算最优批大小"""
        if self.config.strategy == BatchStrategy.FIXED_SIZE:
            return self.config.base_batch_size
        
        elif self.config.strategy == BatchStrategy.ADAPTIVE_SIZE:
            # 基于历史性能自适应调整
            if len(self.metrics_history) < 3:
                return self.config.base_batch_size
            
            recent_metrics = list(self.metrics_history)[-10:]  # 最近10批
            
            # 计算平均吞吐量
            throughputs = [m.throughput for m in recent_metrics if m.throughput > 0]
            if not throughputs:
                return self.config.base_batch_size
            
            avg_throughput = statistics.mean(throughputs)
            
            # 如果吞吐量下降，减小批大小；如果吞吐量良好，可以尝试增大
            if avg_throughput < 10:  # 每秒处理少于10个
                self.current_batch_size = max(self.config.min_batch_size, int(self.current_batch_size * 0.8))
            elif avg_throughput > 50:  # 每秒处理超过50个
                self.current_batch_size = min(self.config.max_batch_size, int(self.current_batch_size * 1.2))
            
            return self.current_batch_size
        
        elif self.config.strategy == BatchStrategy.RESOURCE_AWARE:
            # 基于当前资源状况调整
            if not self.resource_history:
                return self.config.base_batch_size
            
            latest_resources = self.resource_history[-1]
            resource_factor = 1.0 - (latest_resources.cpu_percent / 100.0 * 0.5 + 
                                    latest_resources.memory_percent / 100.0 * 0.5)
            
            adjusted_size = int(self.config.base_batch_size * max(0.5, resource_factor))
            return max(self.config.min_batch_size, 
                      min(self.config.max_batch_size, adjusted_size))
        
        elif self.config.strategy == BatchStrategy.TIME_BASED:
            # 基于目标处理时间调整批大小
            target_time = 30.0  # 目标30秒处理一批
            
            if len(self.metrics_history) > 0:
                recent_metrics = list(self.metrics_history)[-5:]
                avg_time_per_item = statistics.mean([
                    m.processing_time / m.size for m in recent_metrics 
                    if m.processing_time > 0 and m.size > 0
                ]) if recent_metrics else 1.0
                
                optimal_size = int(target_time / avg_time_per_item)
                return max(self.config.min_batch_size,
                          min(self.config.max_batch_size, optimal_size))
        
        return self.config.base_batch_size

    def _create_batches(self, items: List[T]) -> List[List[T]]:
        """创建批次"""
        if not items:
            return []
        
        batch_size = self._calculate_optimal_batch_size(len(items))
        batches = []
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"📦 创建批次: {len(items)} 项 -> {len(batches)} 批 (批大小: {batch_size})")
        return batches

    async def process_batch(self, batch_items: List[T], 
                          processor_func: Callable,
                          batch_id: str = None,
                          *args, **kwargs) -> Tuple[List[Any], List[Exception]]:
        """
        处理单个批次
        
        Args:
            batch_items: 批次数据
            processor_func: 处理函数
            batch_id: 批次ID
            *args, **kwargs: 处理函数参数
            
        Returns:
            Tuple[List[Any], List[Exception]]: 成功结果和失败异常
        """
        if batch_id is None:
            batch_id = f"batch_{int(time.time() * 1000)}"
        
        # 创建批次指标
        metrics = BatchMetrics(
            batch_id=batch_id,
            size=len(batch_items),
            start_time=time.time()
        )
        
        self.current_batches[batch_id] = metrics
        
        async with self.concurrency_semaphore:
            try:
                if self.config.processing_mode == ProcessingMode.ASYNC_ONLY:
                    results, errors = await self._process_async_batch(
                        batch_items, processor_func, *args, **kwargs
                    )
                elif self.config.processing_mode == ProcessingMode.THREAD_POOL:
                    results, errors = await self._process_thread_batch(
                        batch_items, processor_func, *args, **kwargs
                    )
                else:  # MIXED_MODE
                    results, errors = await self._process_mixed_batch(
                        batch_items, processor_func, *args, **kwargs
                    )
                
                # 更新指标
                metrics.end_time = time.time()
                metrics.processing_time = metrics.end_time - metrics.start_time
                metrics.success_count = len(results)
                metrics.failure_count = len(errors)
                metrics.throughput = metrics.success_count / metrics.processing_time if metrics.processing_time > 0 else 0
                metrics.errors = [str(e) for e in errors[:5]]  # 只保留前5个错误
                
                return results, errors
                
            except Exception as e:
                logger.error(f"批次处理异常 {batch_id}: {e}")
                metrics.end_time = time.time()
                metrics.processing_time = metrics.end_time - metrics.start_time
                metrics.failure_count = len(batch_items)
                metrics.errors = [str(e)]
                
                raise
                
            finally:
                # 移到历史记录
                self.metrics_history.append(metrics)
                self.current_batches.pop(batch_id, None)

    async def _process_async_batch(self, batch_items: List[T], 
                                  processor_func: Callable,
                                  *args, **kwargs) -> Tuple[List[Any], List[Exception]]:
        """异步批处理"""
        tasks = []
        
        for item in batch_items:
            if asyncio.iscoroutinefunction(processor_func):
                task = asyncio.create_task(processor_func(item, *args, **kwargs))
            else:
                # 将同步函数包装为异步
                task = asyncio.create_task(
                    asyncio.get_event_loop().run_in_executor(None, processor_func, item, *args, **kwargs)
                )
            tasks.append(task)
        
        # 等待所有任务完成
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 分离成功和失败
        results = []
        errors = []
        
        for result in task_results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                results.append(result)
        
        return results, errors

    async def _process_thread_batch(self, batch_items: List[T],
                                   processor_func: Callable,
                                   *args, **kwargs) -> Tuple[List[Any], List[Exception]]:
        """线程池批处理"""
        loop = asyncio.get_event_loop()
        tasks = []
        
        for item in batch_items:
            task = loop.run_in_executor(
                self.thread_pool, 
                processor_func, 
                item, 
                *args, 
                **kwargs
            )
            tasks.append(task)
        
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = []
        errors = []
        
        for result in task_results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                results.append(result)
        
        return results, errors

    async def _process_mixed_batch(self, batch_items: List[T],
                                  processor_func: Callable,
                                  *args, **kwargs) -> Tuple[List[Any], List[Exception]]:
        """混合模式批处理"""
        # 简单策略：CPU密集型任务用线程池，IO密集型用异步
        if asyncio.iscoroutinefunction(processor_func):
            return await self._process_async_batch(batch_items, processor_func, *args, **kwargs)
        else:
            return await self._process_thread_batch(batch_items, processor_func, *args, **kwargs)

    async def process_all(self, items: List[T], 
                         processor_func: Callable,
                         *args, **kwargs) -> Tuple[List[Any], List[Exception]]:
        """
        处理所有项目
        
        Args:
            items: 待处理项目列表
            processor_func: 处理函数
            *args, **kwargs: 处理函数参数
            
        Returns:
            Tuple[List[Any], List[Exception]]: 所有成功结果和失败异常
        """
        if not items:
            return [], []
        
        # 启动资源监控
        await self.start_monitoring()
        
        try:
            # 创建批次
            batches = self._create_batches(items)
            
            logger.info(f"🚀 开始批量处理: {len(items)} 项，分为 {len(batches)} 批")
            
            # 并发处理所有批次
            batch_tasks = []
            for i, batch in enumerate(batches):
                batch_id = f"batch_{i+1}_{int(time.time() * 1000)}"
                task = asyncio.create_task(
                    self.process_batch(batch, processor_func, batch_id, *args, **kwargs)
                )
                batch_tasks.append(task)
            
            # 等待所有批次完成
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 合并结果
            all_results = []
            all_errors = []
            
            for batch_result in batch_results:
                if isinstance(batch_result, Exception):
                    logger.error(f"批次处理异常: {batch_result}")
                    all_errors.append(batch_result)
                else:
                    results, errors = batch_result
                    all_results.extend(results)
                    all_errors.extend(errors)
            
            # 记录最终统计
            success_rate = len(all_results) / len(items) * 100 if items else 0
            logger.info(f"✅ 批量处理完成: 成功 {len(all_results)}/{len(items)} ({success_rate:.1f}%)")
            
            return all_results, all_errors
            
        finally:
            await self.stop_monitoring()

    def get_processing_stats(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        if not self.metrics_history:
            return {
                'total_batches': 0,
                'total_items': 0,
                'average_batch_size': 0,
                'average_throughput': 0,
                'success_rate': 0,
                'current_config': {
                    'batch_size': self.current_batch_size,
                    'concurrency': self.current_concurrency,
                    'strategy': self.config.strategy.value
                }
            }
        
        recent_metrics = list(self.metrics_history)[-50:]  # 最近50批
        
        total_items = sum(m.size for m in recent_metrics)
        total_successes = sum(m.success_count for m in recent_metrics)
        throughputs = [m.throughput for m in recent_metrics if m.throughput > 0]
        
        # 系统资源统计
        resource_stats = {}
        if self.resource_history:
            recent_resources = list(self.resource_history)[-20:]  # 最近20次
            resource_stats = {
                'avg_cpu_percent': statistics.mean([r.cpu_percent for r in recent_resources]),
                'avg_memory_percent': statistics.mean([r.memory_percent for r in recent_resources]),
                'avg_memory_available_gb': statistics.mean([r.available_memory_gb for r in recent_resources])
            }
        
        return {
            'total_batches': len(recent_metrics),
            'total_items': total_items,
            'total_successes': total_successes,
            'success_rate': (total_successes / total_items * 100) if total_items > 0 else 0,
            'average_batch_size': statistics.mean([m.size for m in recent_metrics]),
            'average_throughput': statistics.mean(throughputs) if throughputs else 0,
            'average_processing_time': statistics.mean([m.processing_time for m in recent_metrics]),
            'current_config': {
                'batch_size': self.current_batch_size,
                'concurrency': self.current_concurrency,
                'strategy': self.config.strategy.value,
                'processing_mode': self.config.processing_mode.value
            },
            'system_resources': resource_stats,
            'active_batches': len(self.current_batches)
        }

    async def shutdown(self):
        """关闭批量处理器"""
        await self.stop_monitoring()
        
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        
        logger.info("🔄 批量处理器已关闭")


if __name__ == "__main__":
    # 测试模块
    async def test_batch_processor():
        """测试批量处理器"""
        
        print("\n" + "="*70)
        print("⚡ 优化批量处理器测试")
        print("="*70)
        
        # 创建处理器
        config = BatchConfig(
            strategy=BatchStrategy.ADAPTIVE_SIZE,
            base_batch_size=20,
            max_concurrent_batches=3,
            processing_mode=ProcessingMode.ASYNC_ONLY
        )
        
        processor = OptimizedBatchProcessor[int](config)
        
        # 模拟处理函数
        async def mock_process_item(item: int, delay: float = 0.01) -> str:
            await asyncio.sleep(delay)
            if item % 37 == 0:  # 偶尔失败
                raise ValueError(f"处理项目 {item} 失败")
            return f"处理结果_{item}"
        
        # 测试小批量
        print("\n📦 测试小批量处理...")
        small_items = list(range(50))
        start_time = time.time()
        
        results, errors = await processor.process_all(
            small_items,
            mock_process_item,
            0.005  # 5ms延迟
        )
        
        elapsed = time.time() - start_time
        print(f"✅ 小批量完成: 成功 {len(results)}, 失败 {len(errors)}, 耗时 {elapsed:.2f}s")
        
        # 测试大批量
        print(f"\n🚀 测试大批量处理...")
        large_items = list(range(500))
        start_time = time.time()
        
        results, errors = await processor.process_all(
            large_items,
            mock_process_item,
            0.002  # 2ms延迟
        )
        
        elapsed = time.time() - start_time
        throughput = len(results) / elapsed
        print(f"✅ 大批量完成: 成功 {len(results)}, 失败 {len(errors)}, 耗时 {elapsed:.2f}s")
        print(f"📊 吞吐量: {throughput:.1f} 项/秒")
        
        # 显示统计信息
        print(f"\n📈 处理统计:")
        stats = processor.get_processing_stats()
        
        print(f"  总批次: {stats['total_batches']}")
        print(f"  平均批大小: {stats['average_batch_size']:.1f}")
        print(f"  成功率: {stats['success_rate']:.1f}%")
        print(f"  平均吞吐量: {stats['average_throughput']:.1f} 项/秒")
        print(f"  当前并发度: {stats['current_config']['concurrency']}")
        
        if stats['system_resources']:
            print(f"  系统资源:")
            print(f"    CPU: {stats['system_resources']['avg_cpu_percent']:.1f}%")
            print(f"    内存: {stats['system_resources']['avg_memory_percent']:.1f}%")
        
        # 关闭处理器
        await processor.shutdown()
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_batch_processor())
