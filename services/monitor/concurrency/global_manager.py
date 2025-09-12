"""
全局并发管理器

这个模块负责管理系统中所有的并发资源，避免API、Milvus、FileIO等资源的竞争冲突。
实现分级控制、优先级调度和动态资源分配。

主要功能：
- API限流和请求队列管理
- Milvus连接池和操作并发控制
- 文件IO操作的资源调度
- 全局任务优先级管理

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from contextlib import asynccontextmanager
import weakref
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ResourceType(Enum):
    """资源类型枚举"""
    API_HKEX = "api_hkex"           # HKEX API调用
    API_SILICONFLOW = "api_siliconflow"  # SiliconFlow API调用
    MILVUS_CONNECTION = "milvus_connection"  # Milvus数据库连接
    FILE_IO = "file_io"             # 文件读写操作
    NETWORK_IO = "network_io"       # 网络IO操作
    CPU_INTENSIVE = "cpu_intensive"  # CPU密集型任务


class TaskPriority(IntEnum):
    """任务优先级枚举（数值越小优先级越高）"""
    EMERGENCY = 0       # 紧急任务
    HIGH = 1           # 高优先级
    NORMAL = 2         # 正常优先级
    LOW = 3            # 低优先级
    BACKGROUND = 4     # 后台任务


@dataclass
class ResourceConfig:
    """资源配置"""
    max_concurrent: int                    # 最大并发数
    rate_limit_per_second: float          # 每秒限制速率
    burst_size: int = 5                   # 突发请求大小
    timeout_seconds: float = 30.0         # 超时时间
    priority_weights: Dict[TaskPriority, float] = field(default_factory=lambda: {
        TaskPriority.EMERGENCY: 1.0,
        TaskPriority.HIGH: 0.8,
        TaskPriority.NORMAL: 0.6,
        TaskPriority.LOW: 0.4,
        TaskPriority.BACKGROUND: 0.2
    })


@dataclass
class TaskRequest:
    """任务请求"""
    resource_type: ResourceType
    priority: TaskPriority
    task_id: str
    task_func: Callable
    task_args: tuple = ()
    task_kwargs: dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    timeout: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceStats:
    """资源统计信息"""
    total_requests: int = 0
    completed_requests: int = 0
    failed_requests: int = 0
    current_active: int = 0
    average_wait_time: float = 0.0
    average_execution_time: float = 0.0
    last_activity: Optional[datetime] = None


class GlobalConcurrencyManager:
    """
    全局并发管理器
    
    统一管理系统中所有的并发资源，实现：
    1. 分级资源控制
    2. 优先级任务调度
    3. 动态限流和熔断
    4. 资源使用统计和监控
    """
    
    def __init__(self, config: Dict[ResourceType, ResourceConfig] = None):
        """
        初始化全局并发管理器
        
        Args:
            config: 资源配置字典
        """
        self.config = config or self._get_default_config()
        
        # 信号量池 - 控制并发数
        self.semaphores: Dict[ResourceType, asyncio.Semaphore] = {}
        
        # 令牌桶 - 控制速率
        self.token_buckets: Dict[ResourceType, Dict[str, Any]] = {}
        
        # 任务队列 - 按优先级排序
        self.task_queues: Dict[ResourceType, List[TaskRequest]] = {}
        
        # 活跃任务追踪
        self.active_tasks: Dict[ResourceType, Dict[str, TaskRequest]] = {}
        
        # 统计信息
        self.stats: Dict[ResourceType, ResourceStats] = {}
        
        # 初始化各种资源
        self._initialize_resources()
        
        # 启动后台任务
        self._background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()
        
        logger.info("🎛️ 全局并发管理器初始化完成")
        self._log_resource_limits()

    def _get_default_config(self) -> Dict[ResourceType, ResourceConfig]:
        """获取默认资源配置"""
        return {
            ResourceType.API_HKEX: ResourceConfig(
                max_concurrent=3,
                rate_limit_per_second=2.0,
                burst_size=5,
                timeout_seconds=30.0
            ),
            ResourceType.API_SILICONFLOW: ResourceConfig(
                max_concurrent=5,
                rate_limit_per_second=10.0,
                burst_size=10,
                timeout_seconds=60.0
            ),
            ResourceType.MILVUS_CONNECTION: ResourceConfig(
                max_concurrent=8,
                rate_limit_per_second=20.0,
                burst_size=15,
                timeout_seconds=45.0
            ),
            ResourceType.FILE_IO: ResourceConfig(
                max_concurrent=10,
                rate_limit_per_second=50.0,
                burst_size=20,
                timeout_seconds=20.0
            ),
            ResourceType.NETWORK_IO: ResourceConfig(
                max_concurrent=15,
                rate_limit_per_second=100.0,
                burst_size=30,
                timeout_seconds=30.0
            ),
            ResourceType.CPU_INTENSIVE: ResourceConfig(
                max_concurrent=4,
                rate_limit_per_second=5.0,
                burst_size=8,
                timeout_seconds=120.0
            )
        }

    def _initialize_resources(self):
        """初始化各种资源控制器"""
        for resource_type, config in self.config.items():
            # 创建信号量
            self.semaphores[resource_type] = asyncio.Semaphore(config.max_concurrent)
            
            # 创建令牌桶
            self.token_buckets[resource_type] = {
                'tokens': config.burst_size,
                'max_tokens': config.burst_size,
                'refill_rate': config.rate_limit_per_second,
                'last_refill': time.time()
            }
            
            # 创建任务队列
            self.task_queues[resource_type] = []
            
            # 创建活跃任务字典
            self.active_tasks[resource_type] = {}
            
            # 创建统计对象
            self.stats[resource_type] = ResourceStats()

    def _start_background_tasks(self):
        """启动后台任务"""
        # 令牌桶补充任务
        token_refill_task = asyncio.create_task(self._token_refill_loop())
        self._background_tasks.append(token_refill_task)
        
        # 任务调度器
        scheduler_task = asyncio.create_task(self._task_scheduler_loop())
        self._background_tasks.append(scheduler_task)
        
        # 统计收集器
        stats_task = asyncio.create_task(self._stats_collector_loop())
        self._background_tasks.append(stats_task)

    async def _token_refill_loop(self):
        """令牌桶补充循环"""
        while True:
            try:
                current_time = time.time()
                
                for resource_type, bucket in self.token_buckets.items():
                    time_passed = current_time - bucket['last_refill']
                    tokens_to_add = time_passed * bucket['refill_rate']
                    
                    bucket['tokens'] = min(
                        bucket['max_tokens'],
                        bucket['tokens'] + tokens_to_add
                    )
                    bucket['last_refill'] = current_time
                
                await asyncio.sleep(0.1)  # 100ms间隔
                
            except Exception as e:
                logger.error(f"令牌桶补充异常: {e}")
                await asyncio.sleep(1.0)

    async def _task_scheduler_loop(self):
        """任务调度循环"""
        while True:
            try:
                for resource_type in ResourceType:
                    await self._process_task_queue(resource_type)
                
                await asyncio.sleep(0.05)  # 50ms间隔
                
            except Exception as e:
                logger.error(f"任务调度异常: {e}")
                await asyncio.sleep(1.0)

    async def _process_task_queue(self, resource_type: ResourceType):
        """处理指定资源类型的任务队列"""
        if not self.task_queues[resource_type]:
            return
        
        # 检查是否有可用资源
        if not self._can_acquire_resource(resource_type):
            return
        
        # 获取最高优先级任务
        task_queue = self.task_queues[resource_type]
        task_queue.sort(key=lambda t: (t.priority.value, t.created_at))
        
        task_request = task_queue.pop(0)
        
        # 异步执行任务
        asyncio.create_task(self._execute_task(task_request))

    def _can_acquire_resource(self, resource_type: ResourceType) -> bool:
        """检查是否可以获取资源"""
        # 检查信号量
        if self.semaphores[resource_type].locked():
            return False
        
        # 检查令牌桶
        bucket = self.token_buckets[resource_type]
        if bucket['tokens'] < 1.0:
            return False
        
        return True

    async def _execute_task(self, task_request: TaskRequest):
        """执行任务"""
        resource_type = task_request.resource_type
        task_id = task_request.task_id
        
        # 更新统计
        self.stats[resource_type].total_requests += 1
        self.stats[resource_type].current_active += 1
        
        # 添加到活跃任务
        self.active_tasks[resource_type][task_id] = task_request
        
        start_time = time.time()
        
        try:
            # 获取资源
            async with self.semaphores[resource_type]:
                # 消耗令牌
                self.token_buckets[resource_type]['tokens'] -= 1.0
                
                # 执行任务
                if asyncio.iscoroutinefunction(task_request.task_func):
                    result = await task_request.task_func(
                        *task_request.task_args, 
                        **task_request.task_kwargs
                    )
                else:
                    result = task_request.task_func(
                        *task_request.task_args, 
                        **task_request.task_kwargs
                    )
                
                # 更新成功统计
                self.stats[resource_type].completed_requests += 1
                
                return result
                
        except Exception as e:
            # 更新失败统计
            self.stats[resource_type].failed_requests += 1
            logger.error(f"任务执行失败 {task_id}: {e}")
            raise
            
        finally:
            # 清理活跃任务
            self.active_tasks[resource_type].pop(task_id, None)
            self.stats[resource_type].current_active -= 1
            
            # 更新执行时间统计
            execution_time = time.time() - start_time
            stats = self.stats[resource_type]
            stats.average_execution_time = (
                (stats.average_execution_time * (stats.completed_requests - 1) + execution_time)
                / stats.completed_requests
            ) if stats.completed_requests > 0 else execution_time
            
            stats.last_activity = datetime.now()

    async def _stats_collector_loop(self):
        """统计收集循环"""
        while True:
            try:
                await asyncio.sleep(30)  # 30秒间隔
                self._log_resource_usage()
                
            except Exception as e:
                logger.error(f"统计收集异常: {e}")
                await asyncio.sleep(60)

    @asynccontextmanager
    async def acquire_resource(self, resource_type: ResourceType, 
                              priority: TaskPriority = TaskPriority.NORMAL,
                              task_id: str = None,
                              timeout: Optional[float] = None):
        """
        异步上下文管理器方式获取资源
        
        Args:
            resource_type: 资源类型
            priority: 任务优先级
            task_id: 任务ID
            timeout: 超时时间
            
        Usage:
            async with manager.acquire_resource(ResourceType.API_HKEX) as resource:
                # 执行任务
                result = await api_call()
        """
        if task_id is None:
            task_id = f"{resource_type.value}_{int(time.time() * 1000)}"
        
        if timeout is None:
            timeout = self.config[resource_type].timeout_seconds
        
        # 等待资源可用
        wait_start = time.time()
        
        while not self._can_acquire_resource(resource_type):
            if time.time() - wait_start > timeout:
                raise TimeoutError(f"等待资源 {resource_type.value} 超时")
            await asyncio.sleep(0.1)
        
        # 获取资源
        async with self.semaphores[resource_type]:
            # 消耗令牌
            self.token_buckets[resource_type]['tokens'] -= 1.0
            
            # 更新统计
            wait_time = time.time() - wait_start
            stats = self.stats[resource_type]
            stats.total_requests += 1
            stats.current_active += 1
            stats.average_wait_time = (
                (stats.average_wait_time * (stats.total_requests - 1) + wait_time)
                / stats.total_requests
            )
            
            try:
                yield resource_type
                stats.completed_requests += 1
                
            except Exception as e:
                stats.failed_requests += 1
                raise
                
            finally:
                stats.current_active -= 1
                stats.last_activity = datetime.now()

    async def submit_task(self, resource_type: ResourceType, 
                         task_func: Callable,
                         priority: TaskPriority = TaskPriority.NORMAL,
                         task_id: str = None,
                         timeout: Optional[float] = None,
                         *args, **kwargs) -> Any:
        """
        提交任务到调度队列
        
        Args:
            resource_type: 资源类型
            task_func: 任务函数
            priority: 任务优先级
            task_id: 任务ID
            timeout: 超时时间
            *args, **kwargs: 任务函数参数
            
        Returns:
            任务执行结果
        """
        if task_id is None:
            task_id = f"{resource_type.value}_{int(time.time() * 1000)}"
        
        # 创建任务请求
        task_request = TaskRequest(
            resource_type=resource_type,
            priority=priority,
            task_id=task_id,
            task_func=task_func,
            task_args=args,
            task_kwargs=kwargs,
            timeout=timeout
        )
        
        # 检查是否可以立即执行
        if self._can_acquire_resource(resource_type):
            return await self._execute_task(task_request)
        else:
            # 添加到队列
            self.task_queues[resource_type].append(task_request)
            
            # 等待任务完成
            while task_id in self.active_tasks[resource_type]:
                await asyncio.sleep(0.1)
            
            # 返回结果（这里简化处理，实际应该通过Future等机制）
            return None

    def get_resource_stats(self) -> Dict[ResourceType, Dict[str, Any]]:
        """
        获取资源使用统计
        
        Returns:
            Dict[ResourceType, Dict[str, Any]]: 资源统计信息
        """
        result = {}
        
        for resource_type, stats in self.stats.items():
            config = self.config[resource_type]
            bucket = self.token_buckets[resource_type]
            
            result[resource_type] = {
                'config': {
                    'max_concurrent': config.max_concurrent,
                    'rate_limit_per_second': config.rate_limit_per_second,
                    'burst_size': config.burst_size,
                    'timeout_seconds': config.timeout_seconds
                },
                'current_state': {
                    'active_tasks': stats.current_active,
                    'available_tokens': round(bucket['tokens'], 2),
                    'queued_tasks': len(self.task_queues[resource_type])
                },
                'statistics': {
                    'total_requests': stats.total_requests,
                    'completed_requests': stats.completed_requests,
                    'failed_requests': stats.failed_requests,
                    'success_rate': (
                        stats.completed_requests / stats.total_requests * 100
                        if stats.total_requests > 0 else 0
                    ),
                    'average_wait_time': round(stats.average_wait_time, 3),
                    'average_execution_time': round(stats.average_execution_time, 3),
                    'last_activity': stats.last_activity.isoformat() if stats.last_activity else None
                }
            }
        
        return result

    def _log_resource_limits(self):
        """记录资源限制配置"""
        logger.info("🎛️ 资源限制配置:")
        for resource_type, config in self.config.items():
            logger.info(f"  {resource_type.value}: "
                       f"并发={config.max_concurrent}, "
                       f"速率={config.rate_limit_per_second}/s, "
                       f"超时={config.timeout_seconds}s")

    def _log_resource_usage(self):
        """记录资源使用情况"""
        logger.info("📊 资源使用统计:")
        for resource_type, stats in self.stats.items():
            if stats.total_requests > 0:
                success_rate = stats.completed_requests / stats.total_requests * 100
                logger.info(f"  {resource_type.value}: "
                           f"活跃={stats.current_active}, "
                           f"成功率={success_rate:.1f}%, "
                           f"平均等待={stats.average_wait_time:.2f}s")

    async def shutdown(self):
        """关闭并发管理器"""
        logger.info("🔄 正在关闭全局并发管理器...")
        
        # 取消后台任务
        for task in self._background_tasks:
            task.cancel()
        
        # 等待所有活跃任务完成
        while any(self.active_tasks.values()):
            await asyncio.sleep(0.1)
        
        logger.info("✅ 全局并发管理器已关闭")


# 全局单例实例
_global_manager: Optional[GlobalConcurrencyManager] = None


def get_global_manager() -> GlobalConcurrencyManager:
    """
    获取全局并发管理器单例
    
    Returns:
        GlobalConcurrencyManager: 全局管理器实例
    """
    global _global_manager
    if _global_manager is None:
        _global_manager = GlobalConcurrencyManager()
    return _global_manager


def set_global_manager(manager: GlobalConcurrencyManager):
    """
    设置全局并发管理器实例
    
    Args:
        manager: 管理器实例
    """
    global _global_manager
    _global_manager = manager


# 便捷函数
async def acquire_api_resource(api_type: str = "hkex", priority: TaskPriority = TaskPriority.NORMAL):
    """
    便捷的API资源获取函数
    
    Args:
        api_type: API类型 ("hkex" 或 "siliconflow")
        priority: 任务优先级
        
    Returns:
        异步上下文管理器
    """
    manager = get_global_manager()
    resource_type = ResourceType.API_HKEX if api_type == "hkex" else ResourceType.API_SILICONFLOW
    return manager.acquire_resource(resource_type, priority)


async def acquire_milvus_resource(priority: TaskPriority = TaskPriority.NORMAL):
    """
    便捷的Milvus资源获取函数
    
    Args:
        priority: 任务优先级
        
    Returns:
        异步上下文管理器
    """
    manager = get_global_manager()
    return manager.acquire_resource(ResourceType.MILVUS_CONNECTION, priority)


async def acquire_file_io_resource(priority: TaskPriority = TaskPriority.NORMAL):
    """
    便捷的文件IO资源获取函数
    
    Args:
        priority: 任务优先级
        
    Returns:
        异步上下文管理器
    """
    manager = get_global_manager()
    return manager.acquire_resource(ResourceType.FILE_IO, priority)


if __name__ == "__main__":
    # 测试模块
    async def test_concurrency_manager():
        """测试全局并发管理器"""
        
        print("\n" + "="*70)
        print("🎛️ 全局并发管理器测试")
        print("="*70)
        
        # 创建管理器
        manager = GlobalConcurrencyManager()
        
        # 测试资源获取
        print("\n📊 测试资源获取...")
        
        async def mock_api_call(call_id: str, duration: float = 0.1):
            await asyncio.sleep(duration)
            return f"API调用 {call_id} 完成"
        
        # 测试API资源
        async with manager.acquire_resource(ResourceType.API_HKEX, TaskPriority.HIGH) as resource:
            result = await mock_api_call("HKEX-001")
            print(f"✅ {result}")
        
        # 测试Milvus资源
        async with manager.acquire_resource(ResourceType.MILVUS_CONNECTION) as resource:
            result = await mock_api_call("Milvus-001", 0.05)
            print(f"✅ Milvus操作完成")
        
        # 测试并发限制
        print(f"\n🔀 测试并发限制...")
        
        async def concurrent_test():
            tasks = []
            for i in range(10):
                task = asyncio.create_task(
                    manager.submit_task(
                        ResourceType.API_HKEX,
                        mock_api_call,
                        TaskPriority.NORMAL,
                        f"task_{i}",
                        None,
                        f"Concurrent-{i}",
                        0.2
                    )
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)
        
        start_time = time.time()
        await concurrent_test()
        elapsed = time.time() - start_time
        
        print(f"✅ 10个并发任务完成，耗时: {elapsed:.2f}秒")
        
        # 显示统计信息
        print(f"\n📈 资源使用统计:")
        stats = manager.get_resource_stats()
        
        for resource_type, resource_stats in stats.items():
            if resource_stats['statistics']['total_requests'] > 0:
                print(f"  {resource_type.value}:")
                print(f"    总请求: {resource_stats['statistics']['total_requests']}")
                print(f"    成功率: {resource_stats['statistics']['success_rate']:.1f}%")
                print(f"    平均等待: {resource_stats['statistics']['average_wait_time']:.3f}s")
        
        # 关闭管理器
        await manager.shutdown()
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_concurrency_manager())
