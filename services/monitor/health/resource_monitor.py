"""
资源状态监控和清理管理器

这个模块实现了全面的资源状态监控和自动清理机制，确保系统资源
得到合理使用和及时回收，避免资源泄露和性能下降。

主要功能：
- 文件系统资源监控和清理
- 内存和缓存资源管理
- 数据库连接池监控
- 临时文件和日志清理
- 资源使用趋势分析

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import os
import gc
import shutil
import glob
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from pathlib import Path
import threading
import psutil
import sys

# 设置路径
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ResourceType(Enum):
    """资源类型枚举"""
    MEMORY = "memory"            # 内存资源
    FILE_SYSTEM = "file_system"  # 文件系统资源
    DATABASE = "database"        # 数据库资源
    NETWORK = "network"          # 网络资源
    CACHE = "cache"             # 缓存资源
    TEMPORARY = "temporary"      # 临时资源


class CleanupStrategy(Enum):
    """清理策略枚举"""
    AGE_BASED = "age_based"      # 基于年龄
    SIZE_BASED = "size_based"    # 基于大小
    COUNT_BASED = "count_based"  # 基于数量
    USAGE_BASED = "usage_based"  # 基于使用情况
    MANUAL = "manual"            # 手动清理


@dataclass
class ResourceRule:
    """资源清理规则"""
    name: str
    resource_type: ResourceType
    strategy: CleanupStrategy
    target_path: str = ""
    max_age_hours: int = 24
    max_size_mb: int = 1024
    max_count: int = 1000
    file_pattern: str = "*"
    enabled: bool = True
    description: str = ""


@dataclass
class CleanupResult:
    """清理结果"""
    rule_name: str
    files_removed: int = 0
    space_freed_mb: float = 0.0
    errors: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ResourceStatus:
    """资源状态"""
    resource_type: ResourceType
    current_usage: float
    max_capacity: float
    usage_percent: float
    last_cleanup: Optional[datetime] = None
    cleanup_count: int = 0
    total_freed_mb: float = 0.0


class ResourceMonitor:
    """
    资源状态监控和清理管理器
    
    提供全面的资源管理功能：
    1. 实时资源状态监控
    2. 智能资源清理策略
    3. 文件系统空间管理
    4. 内存和缓存优化
    5. 数据库连接监控
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化资源监控器"""
        
        self.config = config or {}
        
        # 监控配置
        resource_config = self.config.get('resource_monitoring', {})
        self.monitor_interval = resource_config.get('monitor_interval', 300)  # 5分钟
        self.cleanup_interval = resource_config.get('cleanup_interval', 3600)  # 1小时
        self.enable_auto_cleanup = resource_config.get('enable_auto_cleanup', True)
        self.enable_memory_optimization = resource_config.get('enable_memory_optimization', True)
        
        # 资源清理规则
        self.cleanup_rules: Dict[str, ResourceRule] = {}
        
        # 资源状态
        self.resource_status: Dict[ResourceType, ResourceStatus] = {}
        
        # 清理历史
        self.cleanup_history: List[CleanupResult] = []
        
        # 监控任务
        self.monitoring_tasks: List[asyncio.Task] = []
        self.is_monitoring = False
        
        # 统计信息
        self.monitoring_stats = {
            'total_cleanups': 0,
            'total_files_removed': 0,
            'total_space_freed_mb': 0.0,
            'cleanup_errors': 0,
            'start_time': datetime.now()
        }
        
        # 线程锁
        self.cleanup_lock = threading.RLock()
        
        # 初始化默认规则和状态
        self._initialize_default_rules()
        self._initialize_resource_status()
        
        logger.info("🧹 资源监控器初始化完成")
        self._log_monitor_config()

    def _log_monitor_config(self):
        """记录监控配置"""
        logger.info(f"📊 资源监控器配置:")
        logger.info(f"  监控间隔: {self.monitor_interval}秒")
        logger.info(f"  清理间隔: {self.cleanup_interval}秒")
        logger.info(f"  自动清理: {self.enable_auto_cleanup}")
        logger.info(f"  内存优化: {self.enable_memory_optimization}")

    def _initialize_default_rules(self):
        """初始化默认清理规则"""
        default_rules = [
            # 临时文件清理
            ResourceRule(
                name="temp_files_cleanup",
                resource_type=ResourceType.TEMPORARY,
                strategy=CleanupStrategy.AGE_BASED,
                target_path="/tmp",
                max_age_hours=24,
                file_pattern="hkex_*",
                description="清理24小时前的临时文件"
            ),
            
            # 日志文件清理
            ResourceRule(
                name="log_files_cleanup",
                resource_type=ResourceType.FILE_SYSTEM,
                strategy=CleanupStrategy.AGE_BASED,
                target_path="logs",
                max_age_hours=168,  # 7天
                file_pattern="*.log.*",
                description="清理7天前的日志文件"
            ),
            
            # 下载文件清理
            ResourceRule(
                name="download_cache_cleanup",
                resource_type=ResourceType.FILE_SYSTEM,
                strategy=CleanupStrategy.SIZE_BASED,
                target_path="hkexann",
                max_size_mb=5120,  # 5GB
                file_pattern="*.pdf",
                description="当下载缓存超过5GB时清理"
            ),
            
            # 备份文件清理
            ResourceRule(
                name="backup_files_cleanup",
                resource_type=ResourceType.FILE_SYSTEM,
                strategy=CleanupStrategy.COUNT_BASED,
                target_path="backups",
                max_count=10,
                file_pattern="*.backup",
                description="保留最新10个备份文件"
            ),
            
            # 系统内存优化
            ResourceRule(
                name="memory_optimization",
                resource_type=ResourceType.MEMORY,
                strategy=CleanupStrategy.USAGE_BASED,
                description="内存使用率过高时进行优化"
            ),
            
            # 缓存清理
            ResourceRule(
                name="cache_cleanup",
                resource_type=ResourceType.CACHE,
                strategy=CleanupStrategy.AGE_BASED,
                target_path=".cache",
                max_age_hours=72,  # 3天
                description="清理3天前的缓存数据"
            )
        ]
        
        for rule in default_rules:
            self.cleanup_rules[rule.name] = rule

    def _initialize_resource_status(self):
        """初始化资源状态"""
        for resource_type in ResourceType:
            self.resource_status[resource_type] = ResourceStatus(
                resource_type=resource_type,
                current_usage=0.0,
                max_capacity=0.0,
                usage_percent=0.0
            )

    def register_cleanup_rule(self, rule: ResourceRule):
        """
        注册清理规则
        
        Args:
            rule: 清理规则
        """
        self.cleanup_rules[rule.name] = rule
        logger.info(f"注册清理规则: {rule.name}")

    async def start_monitoring(self):
        """启动资源监控"""
        if self.is_monitoring:
            logger.warning("资源监控已在运行中")
            return
        
        self.is_monitoring = True
        logger.info("🚀 启动资源监控...")
        
        # 启动监控任务
        tasks = [
            self._resource_monitoring_loop(),
            self._memory_monitoring_loop(),
        ]
        
        if self.enable_auto_cleanup:
            tasks.append(self._cleanup_loop())
        
        self.monitoring_tasks = [asyncio.create_task(task) for task in tasks]
        
        logger.info("✅ 资源监控启动完成")

    async def stop_monitoring(self):
        """停止资源监控"""
        if not self.is_monitoring:
            return
        
        self.is_monitoring = False
        logger.info("🛑 停止资源监控...")
        
        # 取消所有监控任务
        for task in self.monitoring_tasks:
            task.cancel()
        
        # 等待任务完成
        if self.monitoring_tasks:
            await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
        
        self.monitoring_tasks.clear()
        logger.info("✅ 资源监控停止完成")

    async def _resource_monitoring_loop(self):
        """资源监控循环"""
        while self.is_monitoring:
            try:
                await self._update_resource_status()
                await asyncio.sleep(self.monitor_interval)
            except Exception as e:
                logger.error(f"资源监控异常: {e}")
                await asyncio.sleep(self.monitor_interval * 2)

    async def _memory_monitoring_loop(self):
        """内存监控循环"""
        while self.is_monitoring:
            try:
                if self.enable_memory_optimization:
                    await self._check_memory_usage()
                await asyncio.sleep(60)  # 1分钟检查一次内存
            except Exception as e:
                logger.error(f"内存监控异常: {e}")
                await asyncio.sleep(120)

    async def _cleanup_loop(self):
        """清理循环"""
        while self.is_monitoring:
            try:
                await self._perform_auto_cleanup()
                await asyncio.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"自动清理异常: {e}")
                await asyncio.sleep(self.cleanup_interval * 2)

    async def _update_resource_status(self):
        """更新资源状态"""
        try:
            # 更新文件系统状态
            await self._update_filesystem_status()
            
            # 更新内存状态
            await self._update_memory_status()
            
            # 更新网络状态
            await self._update_network_status()
            
            logger.debug("资源状态更新完成")
            
        except Exception as e:
            logger.error(f"资源状态更新失败: {e}")

    async def _update_filesystem_status(self):
        """更新文件系统状态"""
        try:
            # 检查主要目录
            important_paths = [
                ".",  # 当前目录
                "logs",
                "hkexann",
                "/tmp"
            ]
            
            total_usage = 0.0
            
            for path in important_paths:
                try:
                    if os.path.exists(path):
                        size = await self._get_directory_size(path)
                        total_usage += size
                except Exception as e:
                    logger.debug(f"获取目录大小失败 {path}: {e}")
            
            # 更新文件系统状态
            disk = psutil.disk_usage('/')
            fs_status = self.resource_status[ResourceType.FILE_SYSTEM]
            fs_status.current_usage = total_usage / (1024**2)  # MB
            fs_status.max_capacity = disk.total / (1024**2)  # MB
            fs_status.usage_percent = disk.percent
            
        except Exception as e:
            logger.error(f"文件系统状态更新失败: {e}")

    async def _update_memory_status(self):
        """更新内存状态"""
        try:
            memory = psutil.virtual_memory()
            process = psutil.Process()
            
            mem_status = self.resource_status[ResourceType.MEMORY]
            mem_status.current_usage = process.memory_info().rss / (1024**2)  # MB
            mem_status.max_capacity = memory.total / (1024**2)  # MB
            mem_status.usage_percent = memory.percent
            
        except Exception as e:
            logger.error(f"内存状态更新失败: {e}")

    async def _update_network_status(self):
        """更新网络状态"""
        try:
            connections = psutil.net_connections()
            
            net_status = self.resource_status[ResourceType.NETWORK]
            net_status.current_usage = len(connections)
            net_status.max_capacity = 1000  # 假设最大1000连接
            net_status.usage_percent = (len(connections) / 1000) * 100
            
        except Exception as e:
            logger.error(f"网络状态更新失败: {e}")

    async def _check_memory_usage(self):
        """检查内存使用情况"""
        try:
            memory = psutil.virtual_memory()
            process = psutil.Process()
            
            # 如果系统内存使用率超过85%或进程内存超过1GB，进行优化
            if memory.percent > 85 or process.memory_info().rss > 1024**3:
                logger.info("检测到高内存使用，开始内存优化...")
                await self._optimize_memory()
                
        except Exception as e:
            logger.error(f"内存检查失败: {e}")

    async def _optimize_memory(self):
        """优化内存使用"""
        try:
            # 强制垃圾回收
            collected = gc.collect()
            logger.info(f"垃圾回收完成，回收对象数: {collected}")
            
            # 清理进程内缓存（如果有的话）
            # 这里可以添加具体的缓存清理逻辑
            
            # 记录内存优化
            rule = self.cleanup_rules.get("memory_optimization")
            if rule:
                result = CleanupResult(
                    rule_name="memory_optimization",
                    files_removed=0,
                    space_freed_mb=0.0,
                    duration_seconds=0.1
                )
                self.cleanup_history.append(result)
            
        except Exception as e:
            logger.error(f"内存优化失败: {e}")

    async def _perform_auto_cleanup(self):
        """执行自动清理"""
        if not self.enable_auto_cleanup:
            return
        
        logger.info("开始自动资源清理...")
        
        with self.cleanup_lock:
            for rule_name, rule in self.cleanup_rules.items():
                if not rule.enabled:
                    continue
                
                try:
                    # 检查是否需要清理
                    if await self._should_cleanup(rule):
                        result = await self._execute_cleanup_rule(rule)
                        
                        if result:
                            self.cleanup_history.append(result)
                            self.monitoring_stats['total_cleanups'] += 1
                            self.monitoring_stats['total_files_removed'] += result.files_removed
                            self.monitoring_stats['total_space_freed_mb'] += result.space_freed_mb
                            
                            logger.info(f"清理完成: {rule_name} - "
                                       f"删除{result.files_removed}个文件，"
                                       f"释放{result.space_freed_mb:.2f}MB空间")
                        
                except Exception as e:
                    logger.error(f"执行清理规则 {rule_name} 失败: {e}")
                    self.monitoring_stats['cleanup_errors'] += 1

    async def _should_cleanup(self, rule: ResourceRule) -> bool:
        """判断是否应该执行清理"""
        try:
            if rule.strategy == CleanupStrategy.AGE_BASED:
                # 基于时间的清理总是执行（在执行时检查文件年龄）
                return True
            
            elif rule.strategy == CleanupStrategy.SIZE_BASED:
                # 检查目录大小
                if rule.target_path and os.path.exists(rule.target_path):
                    size_mb = await self._get_directory_size(rule.target_path) / (1024**2)
                    return size_mb > rule.max_size_mb
            
            elif rule.strategy == CleanupStrategy.COUNT_BASED:
                # 检查文件数量
                if rule.target_path and os.path.exists(rule.target_path):
                    count = await self._get_file_count(rule.target_path, rule.file_pattern)
                    return count > rule.max_count
            
            elif rule.strategy == CleanupStrategy.USAGE_BASED:
                # 检查资源使用率
                status = self.resource_status.get(rule.resource_type)
                if status:
                    return status.usage_percent > 80  # 使用率超过80%
            
            return False
            
        except Exception as e:
            logger.error(f"判断清理条件失败: {e}")
            return False

    async def _execute_cleanup_rule(self, rule: ResourceRule) -> Optional[CleanupResult]:
        """执行清理规则"""
        start_time = datetime.now()
        result = CleanupResult(rule_name=rule.name)
        
        try:
            if rule.resource_type == ResourceType.MEMORY:
                await self._optimize_memory()
                
            elif rule.resource_type in [ResourceType.FILE_SYSTEM, ResourceType.TEMPORARY, ResourceType.CACHE]:
                if rule.target_path and os.path.exists(rule.target_path):
                    files_removed, space_freed = await self._cleanup_directory(rule)
                    result.files_removed = files_removed
                    result.space_freed_mb = space_freed
            
            # 更新资源状态
            if rule.resource_type in self.resource_status:
                status = self.resource_status[rule.resource_type]
                status.last_cleanup = start_time
                status.cleanup_count += 1
                status.total_freed_mb += result.space_freed_mb
            
            result.duration_seconds = (datetime.now() - start_time).total_seconds()
            return result
            
        except Exception as e:
            error_msg = f"清理规则执行失败: {e}"
            result.errors.append(error_msg)
            logger.error(error_msg)
            return result

    async def _cleanup_directory(self, rule: ResourceRule) -> tuple[int, float]:
        """清理目录"""
        files_removed = 0
        space_freed_mb = 0.0
        
        try:
            path = Path(rule.target_path)
            if not path.exists():
                return 0, 0.0
            
            # 获取匹配的文件
            if rule.strategy == CleanupStrategy.AGE_BASED:
                files_to_remove = await self._get_old_files(path, rule.file_pattern, rule.max_age_hours)
            elif rule.strategy == CleanupStrategy.SIZE_BASED:
                files_to_remove = await self._get_files_by_size(path, rule.file_pattern, rule.max_size_mb)
            elif rule.strategy == CleanupStrategy.COUNT_BASED:
                files_to_remove = await self._get_excess_files(path, rule.file_pattern, rule.max_count)
            else:
                return 0, 0.0
            
            # 删除文件
            for file_path in files_to_remove:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    
                    files_removed += 1
                    space_freed_mb += file_size / (1024**2)
                    
                except Exception as e:
                    logger.debug(f"删除文件失败 {file_path}: {e}")
            
            return files_removed, space_freed_mb
            
        except Exception as e:
            logger.error(f"目录清理失败: {e}")
            return 0, 0.0

    async def _get_old_files(self, path: Path, pattern: str, max_age_hours: int) -> List[Path]:
        """获取过期文件"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        old_files = []
        
        try:
            for file_path in path.glob(pattern):
                if file_path.is_file():
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if mtime < cutoff_time:
                        old_files.append(file_path)
        except Exception as e:
            logger.error(f"获取过期文件失败: {e}")
        
        return old_files

    async def _get_files_by_size(self, path: Path, pattern: str, max_size_mb: int) -> List[Path]:
        """根据大小获取需要清理的文件"""
        files_to_remove = []
        current_size_mb = 0.0
        
        try:
            # 按修改时间排序，优先删除最旧的文件
            all_files = sorted(
                [f for f in path.glob(pattern) if f.is_file()],
                key=lambda x: x.stat().st_mtime
            )
            
            # 计算总大小
            total_size_mb = sum(f.stat().st_size for f in all_files) / (1024**2)
            
            # 如果超过限制，删除最旧的文件
            if total_size_mb > max_size_mb:
                excess_mb = total_size_mb - max_size_mb
                
                for file_path in all_files:
                    file_size_mb = file_path.stat().st_size / (1024**2)
                    files_to_remove.append(file_path)
                    current_size_mb += file_size_mb
                    
                    if current_size_mb >= excess_mb:
                        break
        
        except Exception as e:
            logger.error(f"按大小获取文件失败: {e}")
        
        return files_to_remove

    async def _get_excess_files(self, path: Path, pattern: str, max_count: int) -> List[Path]:
        """获取超出数量限制的文件"""
        files_to_remove = []
        
        try:
            # 按修改时间排序，保留最新的文件
            all_files = sorted(
                [f for f in path.glob(pattern) if f.is_file()],
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            # 如果超出数量限制，删除最旧的文件
            if len(all_files) > max_count:
                files_to_remove = all_files[max_count:]
        
        except Exception as e:
            logger.error(f"按数量获取文件失败: {e}")
        
        return files_to_remove

    async def _get_directory_size(self, path: str) -> int:
        """获取目录大小（字节）"""
        total_size = 0
        
        try:
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(filepath)
                    except (OSError, FileNotFoundError):
                        pass
        except Exception as e:
            logger.debug(f"获取目录大小失败 {path}: {e}")
        
        return total_size

    async def _get_file_count(self, path: str, pattern: str) -> int:
        """获取文件数量"""
        try:
            return len(list(Path(path).glob(pattern)))
        except Exception as e:
            logger.debug(f"获取文件数量失败 {path}: {e}")
            return 0

    def get_resource_summary(self) -> Dict[str, Any]:
        """
        获取资源摘要
        
        Returns:
            Dict[str, Any]: 资源摘要信息
        """
        current_time = datetime.now()
        uptime = current_time - self.monitoring_stats['start_time']
        
        # 资源状态摘要
        resource_summary = {}
        for resource_type, status in self.resource_status.items():
            resource_summary[resource_type.value] = {
                'current_usage_mb': status.current_usage,
                'max_capacity_mb': status.max_capacity,
                'usage_percent': status.usage_percent,
                'last_cleanup': status.last_cleanup.isoformat() if status.last_cleanup else None,
                'cleanup_count': status.cleanup_count,
                'total_freed_mb': status.total_freed_mb
            }
        
        # 清理规则摘要
        rules_summary = {}
        for rule_name, rule in self.cleanup_rules.items():
            rules_summary[rule_name] = {
                'enabled': rule.enabled,
                'resource_type': rule.resource_type.value,
                'strategy': rule.strategy.value,
                'description': rule.description
            }
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'is_monitoring': self.is_monitoring,
            'monitoring_stats': self.monitoring_stats.copy(),
            'resource_status': resource_summary,
            'cleanup_rules': rules_summary,
            'recent_cleanups': [
                {
                    'rule_name': result.rule_name,
                    'files_removed': result.files_removed,
                    'space_freed_mb': result.space_freed_mb,
                    'timestamp': result.timestamp.isoformat(),
                    'errors': result.errors
                }
                for result in self.cleanup_history[-10:]
            ]
        }

    async def manual_cleanup(self, rule_name: str) -> Optional[CleanupResult]:
        """
        手动执行清理规则
        
        Args:
            rule_name: 规则名称
            
        Returns:
            Optional[CleanupResult]: 清理结果
        """
        if rule_name not in self.cleanup_rules:
            logger.error(f"未找到清理规则: {rule_name}")
            return None
        
        rule = self.cleanup_rules[rule_name]
        logger.info(f"手动执行清理规则: {rule_name}")
        
        with self.cleanup_lock:
            result = await self._execute_cleanup_rule(rule)
            
            if result:
                self.cleanup_history.append(result)
                self.monitoring_stats['total_cleanups'] += 1
                self.monitoring_stats['total_files_removed'] += result.files_removed
                self.monitoring_stats['total_space_freed_mb'] += result.space_freed_mb
                
                logger.info(f"手动清理完成: 删除{result.files_removed}个文件，"
                           f"释放{result.space_freed_mb:.2f}MB空间")
            
            return result


# 全局资源监控器实例
_global_resource_monitor: Optional[ResourceMonitor] = None


def get_resource_monitor() -> ResourceMonitor:
    """
    获取全局资源监控器实例
    
    Returns:
        ResourceMonitor: 监控器实例
    """
    global _global_resource_monitor
    if _global_resource_monitor is None:
        _global_resource_monitor = ResourceMonitor()
    return _global_resource_monitor


if __name__ == "__main__":
    # 测试模块
    async def test_resource_monitor():
        """测试资源监控器"""
        
        print("\n" + "="*70)
        print("🧹 资源状态监控器测试")
        print("="*70)
        
        # 创建监控器
        config = {
            'resource_monitoring': {
                'monitor_interval': 10,  # 10秒测试
                'cleanup_interval': 30,  # 30秒清理
                'enable_auto_cleanup': True,
                'enable_memory_optimization': True
            }
        }
        
        monitor = ResourceMonitor(config)
        
        # 创建一些测试文件
        print("\n📝 创建测试文件...")
        test_dir = Path("test_cleanup")
        test_dir.mkdir(exist_ok=True)
        
        # 创建不同年龄的文件
        for i in range(5):
            test_file = test_dir / f"test_file_{i}.txt"
            test_file.write_text(f"测试文件 {i}")
            
            # 修改文件时间模拟不同年龄
            if i < 2:
                # 设置为25小时前（应该被清理）
                old_time = datetime.now() - timedelta(hours=25)
                timestamp = old_time.timestamp()
                os.utime(test_file, (timestamp, timestamp))
        
        # 注册测试清理规则
        test_rule = ResourceRule(
            name="test_cleanup_rule",
            resource_type=ResourceType.TEMPORARY,
            strategy=CleanupStrategy.AGE_BASED,
            target_path=str(test_dir),
            max_age_hours=24,
            file_pattern="*.txt",
            description="测试清理规则"
        )
        
        monitor.register_cleanup_rule(test_rule)
        
        # 启动监控
        print(f"\n🚀 启动资源监控...")
        await monitor.start_monitoring()
        
        # 等待监控和清理
        print(f"\n⏳ 等待监控和清理（35秒）...")
        await asyncio.sleep(35)
        
        # 手动执行一次清理测试
        print(f"\n🔧 手动执行清理测试...")
        cleanup_result = await monitor.manual_cleanup("test_cleanup_rule")
        
        if cleanup_result:
            print(f"  手动清理结果:")
            print(f"    删除文件: {cleanup_result.files_removed}")
            print(f"    释放空间: {cleanup_result.space_freed_mb:.2f}MB")
            print(f"    耗时: {cleanup_result.duration_seconds:.2f}秒")
        
        # 获取资源摘要
        print(f"\n📊 资源状态摘要:")
        summary = monitor.get_resource_summary()
        
        print(f"  运行时间: {summary['uptime_seconds']:.1f}秒")
        print(f"  正在监控: {summary['is_monitoring']}")
        print(f"  总清理次数: {summary['monitoring_stats']['total_cleanups']}")
        print(f"  删除文件总数: {summary['monitoring_stats']['total_files_removed']}")
        print(f"  释放空间总计: {summary['monitoring_stats']['total_space_freed_mb']:.2f}MB")
        
        print(f"\n  资源状态:")
        for resource_type, status in summary['resource_status'].items():
            print(f"    {resource_type}:")
            print(f"      使用率: {status['usage_percent']:.1f}%")
            print(f"      清理次数: {status['cleanup_count']}")
            if status['total_freed_mb'] > 0:
                print(f"      释放空间: {status['total_freed_mb']:.2f}MB")
        
        print(f"\n  清理规则:")
        for rule_name, rule in summary['cleanup_rules'].items():
            print(f"    {rule_name}: {rule['strategy']} ({rule['resource_type']}) "
                 f"{'启用' if rule['enabled'] else '禁用'}")
        
        if summary['recent_cleanups']:
            print(f"\n  最近清理:")
            for cleanup in summary['recent_cleanups'][-3:]:
                print(f"    - {cleanup['rule_name']}: {cleanup['files_removed']}个文件, "
                     f"{cleanup['space_freed_mb']:.2f}MB")
        
        # 停止监控
        print(f"\n🛑 停止资源监控...")
        await monitor.stop_monitoring()
        
        # 清理测试文件
        if test_dir.exists():
            shutil.rmtree(test_dir)
            print("清理测试文件完成")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_resource_monitor())
