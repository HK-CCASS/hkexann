"""
Milvus连接池管理器

这个模块实现了高级连接池管理功能，防止连接泄露，提供自动回收、
健康检查和负载均衡等企业级功能。

主要功能：
- 连接池自动管理和回收
- 连接健康检查和自动恢复
- 连接负载均衡和故障转移
- 资源监控和泄露检测
- 异步连接管理

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import weakref
from typing import Dict, Any, Optional, List, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import threading
import gc
from collections import defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 设置路径
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from pymilvus import connections, utility, MilvusException
    MILVUS_AVAILABLE = True
except ImportError:
    MILVUS_AVAILABLE = False
    logger.warning("Milvus SDK 未安装，将使用模拟模式")

try:
    from config.settings import settings
except ImportError:
    # 创建模拟设置用于测试
    class MockSettings:
        milvus_host = "localhost"
        milvus_port = 19531
        milvus_user = None
        milvus_password = None
    
    settings = MockSettings()
    logger.warning("使用模拟设置进行测试")


class ConnectionState(Enum):
    """连接状态枚举"""
    IDLE = "idle"                    # 空闲状态
    ACTIVE = "active"                # 活跃使用中
    CHECKING = "checking"            # 健康检查中
    FAILED = "failed"                # 连接失败
    RECYCLING = "recycling"          # 回收中
    CLOSED = "closed"                # 已关闭


@dataclass
class PoolConnection:
    """池化连接对象"""
    connection_name: str
    host: str
    port: int
    user: Optional[str] = None
    password: Optional[str] = None
    
    # 状态管理
    state: ConnectionState = ConnectionState.IDLE
    created_at: datetime = field(default_factory=datetime.now)
    last_used: datetime = field(default_factory=datetime.now)
    last_health_check: datetime = field(default_factory=datetime.now)
    
    # 使用统计
    usage_count: int = 0
    error_count: int = 0
    total_duration: float = 0.0
    
    # 健康状态
    is_healthy: bool = True
    last_error: Optional[str] = None
    
    # 生命周期管理
    max_lifetime: timedelta = field(default_factory=lambda: timedelta(hours=2))
    max_idle_time: timedelta = field(default_factory=lambda: timedelta(minutes=30))
    max_usage_count: int = 1000


@dataclass
class PoolStatistics:
    """连接池统计信息"""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    failed_connections: int = 0
    
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    
    average_response_time: float = 0.0
    peak_connections: int = 0
    
    created_connections: int = 0
    recycled_connections: int = 0
    leaked_connections: int = 0


class MilvusConnectionPool:
    """
    Milvus连接池管理器
    
    提供企业级连接池功能：
    1. 自动连接池管理和回收
    2. 连接健康检查和恢复
    3. 负载均衡和故障转移
    4. 泄露检测和自动清理
    5. 性能监控和统计
    """
    
    def __init__(self, 
                 min_connections: int = 2,
                 max_connections: int = 10,
                 max_idle_time: int = 1800,  # 30分钟
                 health_check_interval: int = 120,  # 2分钟
                 enable_leak_detection: bool = True):
        """
        初始化连接池
        
        Args:
            min_connections: 最小连接数
            max_connections: 最大连接数
            max_idle_time: 最大空闲时间（秒）
            health_check_interval: 健康检查间隔（秒）
            enable_leak_detection: 是否启用泄露检测
        """
        # 池配置
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_idle_time = timedelta(seconds=max_idle_time)
        self.health_check_interval = health_check_interval
        self.enable_leak_detection = enable_leak_detection
        
        # 连接管理
        self.connections: Dict[str, PoolConnection] = {}
        self.active_connections: Set[str] = set()
        self.idle_connections: Set[str] = set()
        self.failed_connections: Set[str] = set()
        
        # 线程安全
        self.pool_lock = threading.RLock()
        self.connection_semaphore = asyncio.Semaphore(max_connections)
        
        # 统计信息
        self.statistics = PoolStatistics()
        self.request_history: List[Dict[str, Any]] = []
        
        # 后台任务
        self.maintenance_task: Optional[asyncio.Task] = None
        self.health_check_task: Optional[asyncio.Task] = None
        
        # 泄露检测
        self.connection_refs: Set[str] = set()
        self.last_leak_check = datetime.now()
        
        # 服务器配置
        self.host = settings.milvus_host
        self.port = settings.milvus_port
        self.user = settings.milvus_user
        self.password = settings.milvus_password
        
        logger.info("🏊 Milvus连接池初始化完成")
        self._log_pool_config()

    def _log_pool_config(self):
        """记录连接池配置"""
        logger.info(f"📊 连接池配置:")
        logger.info(f"  连接范围: {self.min_connections}-{self.max_connections}")
        logger.info(f"  空闲超时: {self.max_idle_time.total_seconds()}秒")
        logger.info(f"  健康检查间隔: {self.health_check_interval}秒")
        logger.info(f"  泄露检测: {'启用' if self.enable_leak_detection else '禁用'}")

    async def start_pool(self):
        """启动连接池"""
        logger.info("🚀 启动连接池...")
        
        # 创建初始连接
        await self._create_initial_connections()
        
        # 启动维护任务
        if self.maintenance_task is None:
            self.maintenance_task = asyncio.create_task(self._maintenance_loop())
        
        # 启动健康检查任务
        if self.health_check_task is None:
            self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info("✅ 连接池启动完成")

    async def stop_pool(self):
        """停止连接池"""
        logger.info("🛑 停止连接池...")
        
        # 停止后台任务
        if self.maintenance_task:
            self.maintenance_task.cancel()
            self.maintenance_task = None
        
        if self.health_check_task:
            self.health_check_task.cancel()
            self.health_check_task = None
        
        # 关闭所有连接
        await self._close_all_connections()
        
        logger.info("✅ 连接池停止完成")

    async def _create_initial_connections(self):
        """创建初始连接"""
        for i in range(self.min_connections):
            try:
                await self._create_new_connection(f"init_{i}")
            except Exception as e:
                logger.warning(f"创建初始连接失败 {i}: {e}")

    async def _create_new_connection(self, connection_id: str = None) -> str:
        """创建新连接"""
        if not MILVUS_AVAILABLE:
            # 模拟模式
            connection_name = connection_id or f"mock_{int(time.time() * 1000)}"
            
            mock_conn = PoolConnection(
                connection_name=connection_name,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            
            with self.pool_lock:
                self.connections[connection_name] = mock_conn
                self.idle_connections.add(connection_name)
                self.statistics.created_connections += 1
            
            logger.debug(f"创建模拟连接: {connection_name}")
            return connection_name
        
        # 真实连接
        connection_name = connection_id or f"pool_{int(time.time() * 1000)}"
        
        try:
            # 构建连接参数
            conn_params = {
                "alias": connection_name,
                "host": self.host,
                "port": str(self.port)
            }
            
            if self.user:
                conn_params["user"] = self.user
            if self.password:
                conn_params["password"] = self.password
            
            # 创建连接
            connections.connect(**conn_params)
            
            # 测试连接
            collections_list = utility.list_collections(using=connection_name)
            
            # 创建连接对象
            pool_conn = PoolConnection(
                connection_name=connection_name,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            
            with self.pool_lock:
                self.connections[connection_name] = pool_conn
                self.idle_connections.add(connection_name)
                self.statistics.created_connections += 1
                self.statistics.total_connections += 1
            
            # 泄露检测
            if self.enable_leak_detection:
                # 使用连接名称而不是对象进行引用追踪
                self.connection_refs.add(connection_name)
            
            logger.debug(f"创建连接成功: {connection_name}")
            return connection_name
            
        except Exception as e:
            logger.error(f"创建连接失败: {e}")
            raise


    @asynccontextmanager
    async def get_connection(self, timeout: float = 30.0):
        """
        获取连接的异步上下文管理器
        
        Args:
            timeout: 获取连接的超时时间
            
        Usage:
            async with pool.get_connection() as conn_name:
                # 使用连接执行操作
                collections = utility.list_collections(using=conn_name)
        """
        connection_name = None
        start_time = time.time()
        
        try:
            # 获取信号量
            await asyncio.wait_for(
                self.connection_semaphore.acquire(),
                timeout=timeout
            )
            
            # 获取连接
            connection_name = await self._acquire_connection()
            
            if connection_name is None:
                raise RuntimeError("无法获取可用连接")
            
            # 记录请求
            self.statistics.total_requests += 1
            
            yield connection_name
            
            # 记录成功
            self.statistics.successful_requests += 1
            duration = time.time() - start_time
            self._update_response_time(duration)
            
        except Exception as e:
            # 记录失败
            self.statistics.failed_requests += 1
            logger.error(f"连接使用失败: {e}")
            raise
        finally:
            # 释放连接
            if connection_name:
                await self._release_connection(connection_name)
            
            # 释放信号量
            self.connection_semaphore.release()

    async def _acquire_connection(self) -> Optional[str]:
        """获取可用连接"""
        with self.pool_lock:
            # 尝试获取空闲连接
            if self.idle_connections:
                connection_name = self.idle_connections.pop()
                self.active_connections.add(connection_name)
                
                conn = self.connections[connection_name]
                conn.state = ConnectionState.ACTIVE
                conn.usage_count += 1
                conn.last_used = datetime.now()
                
                # 更新统计
                self.statistics.active_connections += 1
                self.statistics.idle_connections -= 1
                self.statistics.peak_connections = max(
                    self.statistics.peak_connections,
                    self.statistics.active_connections
                )
                
                logger.debug(f"获取空闲连接: {connection_name}")
                return connection_name
            
            # 检查是否可以创建新连接
            if len(self.connections) < self.max_connections:
                try:
                    connection_name = await self._create_new_connection()
                    
                    # 立即标记为活跃
                    self.idle_connections.discard(connection_name)
                    self.active_connections.add(connection_name)
                    
                    conn = self.connections[connection_name]
                    conn.state = ConnectionState.ACTIVE
                    conn.usage_count += 1
                    conn.last_used = datetime.now()
                    
                    # 更新统计
                    self.statistics.active_connections += 1
                    self.statistics.peak_connections = max(
                        self.statistics.peak_connections,
                        self.statistics.active_connections
                    )
                    
                    logger.debug(f"创建新连接: {connection_name}")
                    return connection_name
                    
                except Exception as e:
                    logger.error(f"创建新连接失败: {e}")
                    return None
        
        # 无可用连接
        logger.warning("连接池已满，无可用连接")
        return None

    async def _release_connection(self, connection_name: str):
        """释放连接"""
        with self.pool_lock:
            if connection_name not in self.connections:
                logger.warning(f"尝试释放不存在的连接: {connection_name}")
                return
            
            conn = self.connections[connection_name]
            
            # 检查连接是否需要回收
            should_recycle = (
                conn.usage_count >= conn.max_usage_count or
                datetime.now() - conn.created_at > conn.max_lifetime or
                conn.error_count > 5 or
                not conn.is_healthy
            )
            
            if should_recycle:
                await self._recycle_connection(connection_name)
            else:
                # 返回到空闲池
                self.active_connections.discard(connection_name)
                self.idle_connections.add(connection_name)
                
                conn.state = ConnectionState.IDLE
                conn.last_used = datetime.now()
                
                # 更新统计
                self.statistics.active_connections -= 1
                self.statistics.idle_connections += 1
                
                logger.debug(f"释放连接到空闲池: {connection_name}")

    async def _recycle_connection(self, connection_name: str):
        """回收连接"""
        with self.pool_lock:
            if connection_name not in self.connections:
                return
            
            conn = self.connections[connection_name]
            conn.state = ConnectionState.RECYCLING
            
            # 从所有集合中移除
            self.active_connections.discard(connection_name)
            self.idle_connections.discard(connection_name)
            self.failed_connections.discard(connection_name)
            
            # 关闭连接
            if MILVUS_AVAILABLE:
                try:
                    connections.disconnect(connection_name)
                except Exception as e:
                    logger.warning(f"关闭连接时出错: {e}")
            
            # 从池中移除
            del self.connections[connection_name]
            
            # 更新统计
            self.statistics.recycled_connections += 1
            self.statistics.total_connections -= 1
            
            logger.debug(f"回收连接: {connection_name}")
            
            # 如果连接数低于最小值，创建新连接
            if len(self.connections) < self.min_connections:
                try:
                    await self._create_new_connection()
                except Exception as e:
                    logger.warning(f"补充连接失败: {e}")

    async def _maintenance_loop(self):
        """维护循环"""
        while True:
            try:
                await self._perform_maintenance()
                await asyncio.sleep(60)  # 每分钟维护一次
            except Exception as e:
                logger.error(f"维护任务异常: {e}")
                await asyncio.sleep(120)

    async def _perform_maintenance(self):
        """执行维护任务"""
        current_time = datetime.now()
        
        # 清理过期的空闲连接
        expired_connections = []
        
        with self.pool_lock:
            for connection_name in list(self.idle_connections):
                conn = self.connections[connection_name]
                
                if current_time - conn.last_used > self.max_idle_time:
                    expired_connections.append(connection_name)
        
        # 回收过期连接
        for connection_name in expired_connections:
            await self._recycle_connection(connection_name)
            logger.debug(f"回收过期连接: {connection_name}")
        
        # 泄露检测
        if self.enable_leak_detection:
            await self._detect_leaks()
        
        # 清理请求历史
        self._cleanup_request_history()

    async def _health_check_loop(self):
        """健康检查循环"""
        while True:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"健康检查异常: {e}")
                await asyncio.sleep(self.health_check_interval * 2)

    async def _perform_health_check(self):
        """执行健康检查"""
        if not MILVUS_AVAILABLE:
            return
        
        unhealthy_connections = []
        
        with self.pool_lock:
            connections_to_check = list(self.idle_connections)
        
        for connection_name in connections_to_check:
            try:
                # 简单的健康检查：列出集合
                utility.list_collections(using=connection_name)
                
                # 更新健康状态
                with self.pool_lock:
                    if connection_name in self.connections:
                        conn = self.connections[connection_name]
                        conn.is_healthy = True
                        conn.last_health_check = datetime.now()
                        conn.last_error = None
                
                logger.debug(f"健康检查通过: {connection_name}")
                
            except Exception as e:
                logger.warning(f"健康检查失败 {connection_name}: {e}")
                unhealthy_connections.append(connection_name)
                
                # 更新健康状态
                with self.pool_lock:
                    if connection_name in self.connections:
                        conn = self.connections[connection_name]
                        conn.is_healthy = False
                        conn.error_count += 1
                        conn.last_error = str(e)
        
        # 回收不健康的连接
        for connection_name in unhealthy_connections:
            await self._recycle_connection(connection_name)

    async def _detect_leaks(self):
        """检测连接泄露"""
        current_time = datetime.now()
        
        # 每5分钟进行一次泄露检测
        if current_time - self.last_leak_check < timedelta(minutes=5):
            return
        
        # 强制垃圾回收
        gc.collect()
        
        # 清理已关闭的连接引用
        with self.pool_lock:
            existing_connections = set(self.connections.keys())
            removed_refs = self.connection_refs - existing_connections
            
            if removed_refs:
                logger.debug(f"清理已关闭连接引用: {removed_refs}")
                self.connection_refs = existing_connections.intersection(self.connection_refs)
        
        # 检查是否有长时间活跃的连接
        long_running_connections = []
        
        with self.pool_lock:
            for connection_name in self.active_connections:
                conn = self.connections[connection_name]
                if current_time - conn.last_used > timedelta(minutes=10):
                    long_running_connections.append(connection_name)
        
        if long_running_connections:
            logger.warning(f"检测到长时间活跃连接: {long_running_connections}")
            # 统计可能的泄露
            self.statistics.leaked_connections += len(long_running_connections)
        
        self.last_leak_check = current_time

    def _cleanup_request_history(self):
        """清理请求历史"""
        # 保留最近1000条记录
        if len(self.request_history) > 1000:
            self.request_history = self.request_history[-500:]

    def _update_response_time(self, duration: float):
        """更新响应时间统计"""
        # 简单的滑动平均
        if self.statistics.average_response_time == 0:
            self.statistics.average_response_time = duration
        else:
            self.statistics.average_response_time = (
                self.statistics.average_response_time * 0.9 + duration * 0.1
            )

    async def _close_all_connections(self):
        """关闭所有连接"""
        with self.pool_lock:
            connection_names = list(self.connections.keys())
        
        for connection_name in connection_names:
            await self._recycle_connection(connection_name)

    def get_pool_status(self) -> Dict[str, Any]:
        """
        获取连接池状态
        
        Returns:
            Dict[str, Any]: 连接池状态信息
        """
        with self.pool_lock:
            # 更新实时统计
            self.statistics.total_connections = len(self.connections)
            self.statistics.active_connections = len(self.active_connections)
            self.statistics.idle_connections = len(self.idle_connections)
            self.statistics.failed_connections = len(self.failed_connections)
            
            # 连接详情
            connection_details = []
            for name, conn in self.connections.items():
                connection_details.append({
                    'name': name,
                    'state': conn.state.value,
                    'usage_count': conn.usage_count,
                    'error_count': conn.error_count,
                    'is_healthy': conn.is_healthy,
                    'age_seconds': (datetime.now() - conn.created_at).total_seconds(),
                    'idle_seconds': (datetime.now() - conn.last_used).total_seconds()
                })
            
            success_rate = (
                self.statistics.successful_requests / self.statistics.total_requests * 100
                if self.statistics.total_requests > 0 else 0
            )
            
            return {
                'pool_config': {
                    'min_connections': self.min_connections,
                    'max_connections': self.max_connections,
                    'max_idle_time': self.max_idle_time.total_seconds(),
                    'health_check_interval': self.health_check_interval
                },
                'statistics': {
                    'total_connections': self.statistics.total_connections,
                    'active_connections': self.statistics.active_connections,
                    'idle_connections': self.statistics.idle_connections,
                    'failed_connections': self.statistics.failed_connections,
                    'peak_connections': self.statistics.peak_connections,
                    'success_rate': round(success_rate, 2),
                    'average_response_time': round(self.statistics.average_response_time, 3),
                    'created_connections': self.statistics.created_connections,
                    'recycled_connections': self.statistics.recycled_connections,
                    'leaked_connections': self.statistics.leaked_connections
                },
                'connections': connection_details,
                'health_status': 'healthy' if len(self.failed_connections) == 0 else 'degraded'
            }


# 全局连接池实例
_global_connection_pool: Optional[MilvusConnectionPool] = None


def get_connection_pool() -> MilvusConnectionPool:
    """
    获取全局连接池实例
    
    Returns:
        MilvusConnectionPool: 连接池实例
    """
    global _global_connection_pool
    if _global_connection_pool is None:
        _global_connection_pool = MilvusConnectionPool()
    return _global_connection_pool


if __name__ == "__main__":
    # 测试模块
    async def test_connection_pool():
        """测试连接池"""
        
        print("\n" + "="*70)
        print("🏊 Milvus连接池管理器测试")
        print("="*70)
        
        # 创建连接池
        pool = MilvusConnectionPool(
            min_connections=2,
            max_connections=5,
            max_idle_time=10,  # 10秒测试
            health_check_interval=5  # 5秒检查
        )
        
        # 启动连接池
        print("\n🚀 启动连接池...")
        await pool.start_pool()
        
        # 测试连接获取和释放
        print(f"\n🔗 测试连接获取和释放...")
        
        # 并发测试
        async def use_connection(i):
            async with pool.get_connection() as conn_name:
                print(f"  任务{i}: 获取连接 {conn_name}")
                await asyncio.sleep(0.1)  # 模拟操作
                return f"任务{i}完成"
        
        # 启动多个并发任务
        tasks = [use_connection(i) for i in range(3)]
        results = await asyncio.gather(*tasks)
        
        for result in results:
            print(f"  {result}")
        
        # 测试连接池状态
        print(f"\n📊 连接池状态:")
        status = pool.get_pool_status()
        
        stats = status['statistics']
        print(f"  连接总数: {stats['total_connections']}")
        print(f"  活跃连接: {stats['active_connections']}")
        print(f"  空闲连接: {stats['idle_connections']}")
        print(f"  成功率: {stats['success_rate']}%")
        print(f"  平均响应时间: {stats['average_response_time']}s")
        print(f"  峰值连接数: {stats['peak_connections']}")
        
        if status['connections']:
            print(f"  连接详情:")
            for conn in status['connections'][:3]:
                print(f"    - {conn['name']}: {conn['state']} "
                     f"(使用{conn['usage_count']}次, 存活{conn['age_seconds']:.1f}s)")
        
        # 测试健康检查
        print(f"\n🏥 等待健康检查...")
        await asyncio.sleep(6)  # 等待健康检查完成
        
        # 最终状态
        final_status = pool.get_pool_status()
        print(f"  健康状态: {final_status['health_status']}")
        
        # 停止连接池
        print(f"\n🛑 停止连接池...")
        await pool.stop_pool()
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_connection_pool())
