"""
系统健康监控器

这个模块实现了全面的系统健康检查和性能监控体系，包括资源监控、
服务健康检查、性能指标收集和智能告警机制。

主要功能：
- 系统资源监控（CPU、内存、磁盘、网络）
- 服务健康检查（数据库、API、文件系统）
- 性能指标收集和分析
- 智能告警和异常检测
- 监控数据可视化和报告

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import psutil
import traceback
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys
import os
import threading
from collections import defaultdict, deque
import statistics

# 设置路径
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """健康状态枚举"""
    HEALTHY = "healthy"          # 健康
    WARNING = "warning"          # 警告
    CRITICAL = "critical"        # 严重
    UNKNOWN = "unknown"          # 未知


class ComponentType(Enum):
    """组件类型枚举"""
    SYSTEM = "system"            # 系统资源
    DATABASE = "database"        # 数据库
    API = "api"                 # API服务
    FILE_SYSTEM = "file_system"  # 文件系统
    NETWORK = "network"          # 网络
    SERVICE = "service"          # 应用服务


class AlertLevel(IntEnum):
    """告警级别（数值越小越严重）"""
    CRITICAL = 1    # 严重告警
    HIGH = 2        # 高级告警
    MEDIUM = 3      # 中级告警
    LOW = 4         # 低级告警
    INFO = 5        # 信息告警


@dataclass
class HealthMetric:
    """健康指标"""
    name: str
    value: float
    status: HealthStatus
    threshold_warning: float = 0.8
    threshold_critical: float = 0.9
    unit: str = ""
    description: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ComponentHealth:
    """组件健康状态"""
    name: str
    component_type: ComponentType
    status: HealthStatus
    metrics: List[HealthMetric] = field(default_factory=list)
    last_check: datetime = field(default_factory=datetime.now)
    error_message: str = ""
    response_time: float = 0.0


@dataclass
class SystemAlert:
    """系统告警"""
    id: str
    level: AlertLevel
    title: str
    message: str
    component: str
    timestamp: datetime = field(default_factory=datetime.now)
    resolved: bool = False
    resolution_time: Optional[datetime] = None


@dataclass
class PerformanceSnapshot:
    """性能快照"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    network_io: Dict[str, float]
    process_count: int
    active_connections: int


class SystemHealthMonitor:
    """
    系统健康监控器
    
    提供全面的系统监控功能：
    1. 实时资源监控（CPU、内存、磁盘、网络）
    2. 服务健康检查（数据库、API、文件系统）
    3. 性能指标收集和趋势分析
    4. 智能告警和异常检测
    5. 监控数据持久化和可视化
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化系统健康监控器"""
        
        self.config = config or {}
        
        # 监控配置
        monitoring_config = self.config.get('monitoring', {})
        self.check_interval = monitoring_config.get('check_interval', 30)  # 30秒
        self.alert_threshold = monitoring_config.get('alert_threshold', 0.8)
        self.history_retention = monitoring_config.get('history_retention_hours', 24)
        self.enable_alerts = monitoring_config.get('enable_alerts', True)
        self.enable_performance_tracking = monitoring_config.get('enable_performance_tracking', True)
        
        # 组件健康状态
        self.component_health: Dict[str, ComponentHealth] = {}
        
        # 性能历史数据
        self.performance_history: deque = deque(maxlen=2880)  # 24小时（30秒间隔）
        
        # 告警管理
        self.active_alerts: Dict[str, SystemAlert] = {}
        self.alert_history: List[SystemAlert] = []
        
        # 指标收集器
        self.metric_collectors: Dict[str, Callable] = {}
        
        # 健康检查器
        self.health_checkers: Dict[str, Callable] = {}
        
        # 监控任务
        self.monitoring_tasks: List[asyncio.Task] = []
        self.is_monitoring = False
        
        # 统计信息
        self.monitoring_stats = {
            'total_checks': 0,
            'failed_checks': 0,
            'alerts_generated': 0,
            'alerts_resolved': 0,
            'uptime_start': datetime.now()
        }
        
        # 初始化监控组件
        self._initialize_collectors()
        self._initialize_health_checkers()
        
        logger.info("🔍 系统健康监控器初始化完成")
        self._log_monitor_config()

    def _log_monitor_config(self):
        """记录监控配置"""
        logger.info(f"📊 监控配置:")
        logger.info(f"  检查间隔: {self.check_interval}秒")
        logger.info(f"  告警阈值: {self.alert_threshold}")
        logger.info(f"  历史保留: {self.history_retention}小时")
        logger.info(f"  启用告警: {self.enable_alerts}")
        logger.info(f"  性能追踪: {self.enable_performance_tracking}")

    def _initialize_collectors(self):
        """初始化指标收集器"""
        self.metric_collectors = {
            'cpu': self._collect_cpu_metrics,
            'memory': self._collect_memory_metrics,
            'disk': self._collect_disk_metrics,
            'network': self._collect_network_metrics,
            'process': self._collect_process_metrics
        }

    def _initialize_health_checkers(self):
        """初始化健康检查器"""
        self.health_checkers = {
            'system_resources': self._check_system_resources,
            'file_system': self._check_file_system,
            'network_connectivity': self._check_network_connectivity,
            'process_health': self._check_process_health
        }

    async def start_monitoring(self):
        """启动监控"""
        if self.is_monitoring:
            logger.warning("监控已在运行中")
            return
        
        self.is_monitoring = True
        logger.info("🚀 启动系统健康监控...")
        
        # 启动各种监控任务
        tasks = [
            self._resource_monitoring_loop(),
            self._health_check_loop(),
            self._alert_processing_loop(),
            self._cleanup_loop()
        ]
        
        if self.enable_performance_tracking:
            tasks.append(self._performance_tracking_loop())
        
        self.monitoring_tasks = [asyncio.create_task(task) for task in tasks]
        
        logger.info("✅ 系统健康监控启动完成")

    async def stop_monitoring(self):
        """停止监控"""
        if not self.is_monitoring:
            return
        
        self.is_monitoring = False
        logger.info("🛑 停止系统健康监控...")
        
        # 取消所有监控任务
        for task in self.monitoring_tasks:
            task.cancel()
        
        # 等待任务完成
        if self.monitoring_tasks:
            await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
        
        self.monitoring_tasks.clear()
        logger.info("✅ 系统健康监控停止完成")

    async def _resource_monitoring_loop(self):
        """资源监控循环"""
        while self.is_monitoring:
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"资源监控异常: {e}")
                await asyncio.sleep(self.check_interval * 2)

    async def _health_check_loop(self):
        """健康检查循环"""
        while self.is_monitoring:
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"健康检查异常: {e}")
                await asyncio.sleep(self.check_interval * 2)

    async def _alert_processing_loop(self):
        """告警处理循环"""
        while self.is_monitoring:
            try:
                await self._process_alerts()
                await asyncio.sleep(10)  # 10秒检查一次告警
            except Exception as e:
                logger.error(f"告警处理异常: {e}")
                await asyncio.sleep(30)

    async def _performance_tracking_loop(self):
        """性能追踪循环"""
        while self.is_monitoring:
            try:
                await self._track_performance()
                await asyncio.sleep(30)  # 30秒记录一次性能快照
            except Exception as e:
                logger.error(f"性能追踪异常: {e}")
                await asyncio.sleep(60)

    async def _cleanup_loop(self):
        """清理循环"""
        while self.is_monitoring:
            try:
                await self._cleanup_old_data()
                await asyncio.sleep(3600)  # 1小时清理一次
            except Exception as e:
                logger.error(f"数据清理异常: {e}")
                await asyncio.sleep(3600)

    async def _collect_system_metrics(self):
        """收集系统指标"""
        try:
            # 收集各类指标
            for name, collector in self.metric_collectors.items():
                try:
                    metrics = await collector()
                    if metrics:
                        component_name = f"system_{name}"
                        
                        # 创建或更新组件健康状态
                        if component_name not in self.component_health:
                            self.component_health[component_name] = ComponentHealth(
                                name=component_name,
                                component_type=ComponentType.SYSTEM,
                                status=HealthStatus.HEALTHY
                            )
                        
                        component = self.component_health[component_name]
                        component.metrics = metrics
                        component.last_check = datetime.now()
                        
                        # 评估健康状态
                        component.status = self._evaluate_component_health(metrics)
                        
                        self.monitoring_stats['total_checks'] += 1
                        
                except Exception as e:
                    logger.error(f"收集 {name} 指标失败: {e}")
                    self.monitoring_stats['failed_checks'] += 1
                    
        except Exception as e:
            logger.error(f"系统指标收集异常: {e}")

    async def _perform_health_checks(self):
        """执行健康检查"""
        for name, checker in self.health_checkers.items():
            try:
                start_time = time.time()
                health_result = await checker()
                response_time = time.time() - start_time
                
                if health_result:
                    component_name = f"health_{name}"
                    
                    if component_name not in self.component_health:
                        self.component_health[component_name] = ComponentHealth(
                            name=component_name,
                            component_type=ComponentType.SERVICE,
                            status=HealthStatus.HEALTHY
                        )
                    
                    component = self.component_health[component_name]
                    component.status = health_result.get('status', HealthStatus.HEALTHY)
                    component.error_message = health_result.get('error', '')
                    component.response_time = response_time
                    component.last_check = datetime.now()
                
                self.monitoring_stats['total_checks'] += 1
                
            except Exception as e:
                logger.error(f"健康检查 {name} 失败: {e}")
                self.monitoring_stats['failed_checks'] += 1

    async def _process_alerts(self):
        """处理告警"""
        if not self.enable_alerts:
            return
        
        # 检查组件健康状态生成告警
        for component_name, component in self.component_health.items():
            if component.status in [HealthStatus.WARNING, HealthStatus.CRITICAL]:
                alert_id = f"{component_name}_{component.status.value}"
                
                if alert_id not in self.active_alerts:
                    # 生成新告警
                    alert = SystemAlert(
                        id=alert_id,
                        level=AlertLevel.HIGH if component.status == HealthStatus.CRITICAL else AlertLevel.MEDIUM,
                        title=f"{component.name} 健康状态异常",
                        message=component.error_message or f"组件状态: {component.status.value}",
                        component=component.name
                    )
                    
                    self.active_alerts[alert_id] = alert
                    self.alert_history.append(alert)
                    self.monitoring_stats['alerts_generated'] += 1
                    
                    logger.warning(f"🚨 新告警: {alert.title}")
            
            elif component.status == HealthStatus.HEALTHY:
                # 检查是否有待解决的告警可以关闭
                alerts_to_resolve = [
                    alert_id for alert_id in self.active_alerts.keys()
                    if alert_id.startswith(component_name)
                ]
                
                for alert_id in alerts_to_resolve:
                    alert = self.active_alerts[alert_id]
                    alert.resolved = True
                    alert.resolution_time = datetime.now()
                    
                    del self.active_alerts[alert_id]
                    self.monitoring_stats['alerts_resolved'] += 1
                    
                    logger.info(f"✅ 告警已解决: {alert.title}")

    async def _track_performance(self):
        """追踪性能"""
        try:
            # 创建性能快照
            snapshot = PerformanceSnapshot(
                timestamp=datetime.now(),
                cpu_percent=psutil.cpu_percent(),
                memory_percent=psutil.virtual_memory().percent,
                disk_percent=psutil.disk_usage('/').percent,
                network_io=dict(psutil.net_io_counters()._asdict()),
                process_count=len(psutil.pids()),
                active_connections=len(psutil.net_connections())
            )
            
            # 添加到历史记录
            self.performance_history.append(snapshot)
            
            logger.debug(f"性能快照: CPU={snapshot.cpu_percent:.1f}% "
                        f"内存={snapshot.memory_percent:.1f}% "
                        f"磁盘={snapshot.disk_percent:.1f}%")
                        
        except Exception as e:
            logger.error(f"性能追踪失败: {e}")

    async def _cleanup_old_data(self):
        """清理旧数据"""
        current_time = datetime.now()
        retention_threshold = current_time - timedelta(hours=self.history_retention)
        
        # 清理告警历史
        self.alert_history = [
            alert for alert in self.alert_history
            if alert.timestamp > retention_threshold
        ]
        
        # 清理性能历史（deque自动限制大小）
        logger.debug("数据清理完成")

    # 指标收集器方法
    async def _collect_cpu_metrics(self) -> List[HealthMetric]:
        """收集CPU指标"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            load_avg = os.getloadavg() if hasattr(os, 'getloadavg') else [0, 0, 0]
            
            metrics = [
                HealthMetric(
                    name="cpu_usage",
                    value=cpu_percent / 100,
                    status=self._get_threshold_status(cpu_percent / 100),
                    threshold_warning=0.8,
                    threshold_critical=0.9,
                    unit="%",
                    description="CPU使用率"
                ),
                HealthMetric(
                    name="cpu_count",
                    value=cpu_count,
                    status=HealthStatus.HEALTHY,
                    unit="cores",
                    description="CPU核心数"
                ),
                HealthMetric(
                    name="load_average_1m",
                    value=load_avg[0],
                    status=self._get_threshold_status(load_avg[0] / cpu_count),
                    threshold_warning=0.8,
                    threshold_critical=1.0,
                    unit="",
                    description="1分钟平均负载"
                )
            ]
            
            return metrics
            
        except Exception as e:
            logger.error(f"CPU指标收集失败: {e}")
            return []

    async def _collect_memory_metrics(self) -> List[HealthMetric]:
        """收集内存指标"""
        try:
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            metrics = [
                HealthMetric(
                    name="memory_usage",
                    value=memory.percent / 100,
                    status=self._get_threshold_status(memory.percent / 100),
                    threshold_warning=0.8,
                    threshold_critical=0.9,
                    unit="%",
                    description="内存使用率"
                ),
                HealthMetric(
                    name="memory_available",
                    value=memory.available / (1024**3),  # GB
                    status=HealthStatus.HEALTHY,
                    unit="GB",
                    description="可用内存"
                ),
                HealthMetric(
                    name="swap_usage",
                    value=swap.percent / 100,
                    status=self._get_threshold_status(swap.percent / 100),
                    threshold_warning=0.5,
                    threshold_critical=0.8,
                    unit="%",
                    description="交换分区使用率"
                )
            ]
            
            return metrics
            
        except Exception as e:
            logger.error(f"内存指标收集失败: {e}")
            return []

    async def _collect_disk_metrics(self) -> List[HealthMetric]:
        """收集磁盘指标"""
        try:
            disk = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            
            metrics = [
                HealthMetric(
                    name="disk_usage",
                    value=disk.percent / 100,
                    status=self._get_threshold_status(disk.percent / 100),
                    threshold_warning=0.8,
                    threshold_critical=0.9,
                    unit="%",
                    description="磁盘使用率"
                ),
                HealthMetric(
                    name="disk_free",
                    value=disk.free / (1024**3),  # GB
                    status=HealthStatus.HEALTHY,
                    unit="GB",
                    description="磁盘可用空间"
                )
            ]
            
            if disk_io:
                metrics.extend([
                    HealthMetric(
                        name="disk_read_bytes",
                        value=disk_io.read_bytes / (1024**2),  # MB
                        status=HealthStatus.HEALTHY,
                        unit="MB",
                        description="磁盘读取总量"
                    ),
                    HealthMetric(
                        name="disk_write_bytes",
                        value=disk_io.write_bytes / (1024**2),  # MB
                        status=HealthStatus.HEALTHY,
                        unit="MB",
                        description="磁盘写入总量"
                    )
                ])
            
            return metrics
            
        except Exception as e:
            logger.error(f"磁盘指标收集失败: {e}")
            return []

    async def _collect_network_metrics(self) -> List[HealthMetric]:
        """收集网络指标"""
        try:
            network_io = psutil.net_io_counters()
            connections = psutil.net_connections()
            
            metrics = [
                HealthMetric(
                    name="network_bytes_sent",
                    value=network_io.bytes_sent / (1024**2),  # MB
                    status=HealthStatus.HEALTHY,
                    unit="MB",
                    description="网络发送总量"
                ),
                HealthMetric(
                    name="network_bytes_recv",
                    value=network_io.bytes_recv / (1024**2),  # MB
                    status=HealthStatus.HEALTHY,
                    unit="MB",
                    description="网络接收总量"
                ),
                HealthMetric(
                    name="network_connections",
                    value=len(connections),
                    status=self._get_threshold_status(len(connections) / 1000),  # 假设1000为警告阈值
                    threshold_warning=0.8,
                    threshold_critical=0.9,
                    unit="",
                    description="网络连接数"
                )
            ]
            
            return metrics
            
        except Exception as e:
            logger.error(f"网络指标收集失败: {e}")
            return []

    async def _collect_process_metrics(self) -> List[HealthMetric]:
        """收集进程指标"""
        try:
            process_count = len(psutil.pids())
            current_process = psutil.Process()
            
            metrics = [
                HealthMetric(
                    name="process_count",
                    value=process_count,
                    status=self._get_threshold_status(process_count / 1000),  # 假设1000为警告阈值
                    threshold_warning=0.8,
                    threshold_critical=0.9,
                    unit="",
                    description="系统进程总数"
                ),
                HealthMetric(
                    name="current_process_cpu",
                    value=current_process.cpu_percent(),
                    status=self._get_threshold_status(current_process.cpu_percent() / 100),
                    threshold_warning=0.5,
                    threshold_critical=0.8,
                    unit="%",
                    description="当前进程CPU使用率"
                ),
                HealthMetric(
                    name="current_process_memory",
                    value=current_process.memory_percent(),
                    status=self._get_threshold_status(current_process.memory_percent() / 100),
                    threshold_warning=0.3,
                    threshold_critical=0.5,
                    unit="%",
                    description="当前进程内存使用率"
                )
            ]
            
            return metrics
            
        except Exception as e:
            logger.error(f"进程指标收集失败: {e}")
            return []

    # 健康检查器方法
    async def _check_system_resources(self) -> Dict[str, Any]:
        """检查系统资源"""
        try:
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            disk_percent = psutil.disk_usage('/').percent
            
            # 评估整体系统健康状态
            if cpu_percent > 90 or memory_percent > 90 or disk_percent > 90:
                status = HealthStatus.CRITICAL
                error = f"系统资源严重不足: CPU={cpu_percent:.1f}% 内存={memory_percent:.1f}% 磁盘={disk_percent:.1f}%"
            elif cpu_percent > 80 or memory_percent > 80 or disk_percent > 80:
                status = HealthStatus.WARNING
                error = f"系统资源紧张: CPU={cpu_percent:.1f}% 内存={memory_percent:.1f}% 磁盘={disk_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                error = ""
            
            return {
                'status': status,
                'error': error,
                'metrics': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory_percent,
                    'disk_percent': disk_percent
                }
            }
            
        except Exception as e:
            return {
                'status': HealthStatus.CRITICAL,
                'error': f"系统资源检查失败: {e}"
            }

    async def _check_file_system(self) -> Dict[str, Any]:
        """检查文件系统"""
        try:
            # 检查关键目录
            critical_paths = [
                '/tmp',
                str(Path.cwd()),  # 当前工作目录
                str(Path.home())  # 用户主目录
            ]
            
            issues = []
            for path in critical_paths:
                try:
                    path_obj = Path(path)
                    if not path_obj.exists():
                        issues.append(f"路径不存在: {path}")
                    elif not os.access(path, os.R_OK | os.W_OK):
                        issues.append(f"路径无读写权限: {path}")
                except Exception as e:
                    issues.append(f"路径检查失败 {path}: {e}")
            
            if issues:
                status = HealthStatus.WARNING
                error = "; ".join(issues)
            else:
                status = HealthStatus.HEALTHY
                error = ""
            
            return {
                'status': status,
                'error': error
            }
            
        except Exception as e:
            return {
                'status': HealthStatus.CRITICAL,
                'error': f"文件系统检查失败: {e}"
            }

    async def _check_network_connectivity(self) -> Dict[str, Any]:
        """检查网络连通性"""
        try:
            # 简单的网络检查
            import socket
            
            test_hosts = [
                ('8.8.8.8', 53),  # Google DNS
                ('1.1.1.1', 53),  # Cloudflare DNS
            ]
            
            connectivity_ok = False
            for host, port in test_hosts:
                try:
                    socket.create_connection((host, port), timeout=5)
                    connectivity_ok = True
                    break
                except Exception:
                    continue
            
            if connectivity_ok:
                status = HealthStatus.HEALTHY
                error = ""
            else:
                status = HealthStatus.WARNING
                error = "网络连通性检查失败"
            
            return {
                'status': status,
                'error': error
            }
            
        except Exception as e:
            return {
                'status': HealthStatus.CRITICAL,
                'error': f"网络检查失败: {e}"
            }

    async def _check_process_health(self) -> Dict[str, Any]:
        """检查进程健康"""
        try:
            current_process = psutil.Process()
            
            # 检查进程状态
            status_info = current_process.status()
            cpu_percent = current_process.cpu_percent()
            memory_percent = current_process.memory_percent()
            
            if status_info == psutil.STATUS_ZOMBIE:
                status = HealthStatus.CRITICAL
                error = "进程状态异常: 僵尸进程"
            elif cpu_percent > 80:
                status = HealthStatus.WARNING
                error = f"进程CPU使用率过高: {cpu_percent:.1f}%"
            elif memory_percent > 50:
                status = HealthStatus.WARNING
                error = f"进程内存使用率过高: {memory_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                error = ""
            
            return {
                'status': status,
                'error': error,
                'metrics': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory_percent,
                    'status': status_info
                }
            }
            
        except Exception as e:
            return {
                'status': HealthStatus.CRITICAL,
                'error': f"进程健康检查失败: {e}"
            }

    def _get_threshold_status(self, value: float, 
                            warning_threshold: float = None,
                            critical_threshold: float = None) -> HealthStatus:
        """根据阈值获取状态"""
        warning_threshold = warning_threshold or self.alert_threshold
        critical_threshold = critical_threshold or 0.9
        
        if value >= critical_threshold:
            return HealthStatus.CRITICAL
        elif value >= warning_threshold:
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY

    def _evaluate_component_health(self, metrics: List[HealthMetric]) -> HealthStatus:
        """评估组件健康状态"""
        if not metrics:
            return HealthStatus.UNKNOWN
        
        # 获取最严重的状态
        statuses = [metric.status for metric in metrics]
        
        if HealthStatus.CRITICAL in statuses:
            return HealthStatus.CRITICAL
        elif HealthStatus.WARNING in statuses:
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY

    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态概览
        
        Returns:
            Dict[str, Any]: 系统状态信息
        """
        current_time = datetime.now()
        uptime = current_time - self.monitoring_stats['uptime_start']
        
        # 组件状态统计
        component_stats = {
            'total': len(self.component_health),
            'healthy': len([c for c in self.component_health.values() if c.status == HealthStatus.HEALTHY]),
            'warning': len([c for c in self.component_health.values() if c.status == HealthStatus.WARNING]),
            'critical': len([c for c in self.component_health.values() if c.status == HealthStatus.CRITICAL])
        }
        
        # 整体健康状态
        if component_stats['critical'] > 0:
            overall_status = HealthStatus.CRITICAL
        elif component_stats['warning'] > 0:
            overall_status = HealthStatus.WARNING
        else:
            overall_status = HealthStatus.HEALTHY
        
        # 性能摘要
        performance_summary = {}
        if self.performance_history:
            latest = self.performance_history[-1]
            performance_summary = {
                'cpu_percent': latest.cpu_percent,
                'memory_percent': latest.memory_percent,
                'disk_percent': latest.disk_percent,
                'process_count': latest.process_count,
                'active_connections': latest.active_connections
            }
        
        return {
            'overall_status': overall_status.value,
            'monitoring_enabled': self.is_monitoring,
            'uptime_seconds': uptime.total_seconds(),
            'last_check': current_time.isoformat(),
            'component_stats': component_stats,
            'active_alerts': len(self.active_alerts),
            'performance_summary': performance_summary,
            'monitoring_stats': self.monitoring_stats.copy(),
            'components': {
                name: {
                    'status': component.status.value,
                    'last_check': component.last_check.isoformat(),
                    'response_time': component.response_time,
                    'error_message': component.error_message,
                    'metrics_count': len(component.metrics)
                }
                for name, component in self.component_health.items()
            }
        }

    def get_performance_trends(self, hours: int = 1) -> Dict[str, Any]:
        """
        获取性能趋势分析
        
        Args:
            hours: 分析时间范围（小时）
            
        Returns:
            Dict[str, Any]: 性能趋势信息
        """
        if not self.performance_history:
            return {'error': '无性能历史数据'}
        
        # 获取指定时间范围内的数据
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_data = [
            snapshot for snapshot in self.performance_history
            if snapshot.timestamp > cutoff_time
        ]
        
        if not recent_data:
            return {'error': f'无{hours}小时内的性能数据'}
        
        # 计算趋势
        cpu_values = [s.cpu_percent for s in recent_data]
        memory_values = [s.memory_percent for s in recent_data]
        disk_values = [s.disk_percent for s in recent_data]
        
        trends = {
            'time_range_hours': hours,
            'data_points': len(recent_data),
            'cpu': {
                'current': cpu_values[-1],
                'average': statistics.mean(cpu_values),
                'max': max(cpu_values),
                'min': min(cpu_values),
                'trend': 'increasing' if cpu_values[-1] > cpu_values[0] else 'decreasing'
            },
            'memory': {
                'current': memory_values[-1],
                'average': statistics.mean(memory_values),
                'max': max(memory_values),
                'min': min(memory_values),
                'trend': 'increasing' if memory_values[-1] > memory_values[0] else 'decreasing'
            },
            'disk': {
                'current': disk_values[-1],
                'average': statistics.mean(disk_values),
                'max': max(disk_values),
                'min': min(disk_values),
                'trend': 'increasing' if disk_values[-1] > disk_values[0] else 'decreasing'
            }
        }
        
        return trends

    def get_alerts_summary(self) -> Dict[str, Any]:
        """
        获取告警摘要
        
        Returns:
            Dict[str, Any]: 告警摘要信息
        """
        return {
            'active_alerts': len(self.active_alerts),
            'total_alerts_generated': self.monitoring_stats['alerts_generated'],
            'total_alerts_resolved': self.monitoring_stats['alerts_resolved'],
            'alerts': [
                {
                    'id': alert.id,
                    'level': alert.level.name,
                    'title': alert.title,
                    'component': alert.component,
                    'timestamp': alert.timestamp.isoformat(),
                    'resolved': alert.resolved
                }
                for alert in list(self.active_alerts.values()) + self.alert_history[-10:]
            ]
        }


# 全局监控器实例
_global_system_monitor: Optional[SystemHealthMonitor] = None


def get_system_monitor() -> SystemHealthMonitor:
    """
    获取全局系统监控器实例
    
    Returns:
        SystemHealthMonitor: 监控器实例
    """
    global _global_system_monitor
    if _global_system_monitor is None:
        _global_system_monitor = SystemHealthMonitor()
    return _global_system_monitor


if __name__ == "__main__":
    # 测试模块
    async def test_system_health_monitor():
        """测试系统健康监控器"""
        
        print("\n" + "="*70)
        print("🔍 系统健康监控器测试")
        print("="*70)
        
        # 创建监控器
        config = {
            'monitoring': {
                'check_interval': 5,  # 5秒测试
                'alert_threshold': 0.7,
                'history_retention_hours': 1,
                'enable_alerts': True,
                'enable_performance_tracking': True
            }
        }
        
        monitor = SystemHealthMonitor(config)
        
        # 启动监控
        print("\n🚀 启动系统监控...")
        await monitor.start_monitoring()
        
        # 等待收集一些数据
        print(f"\n⏳ 等待数据收集（10秒）...")
        await asyncio.sleep(10)
        
        # 获取系统状态
        print(f"\n📊 系统状态概览:")
        status = monitor.get_system_status()
        
        print(f"  整体状态: {status['overall_status']}")
        print(f"  监控启用: {status['monitoring_enabled']}")
        print(f"  运行时间: {status['uptime_seconds']:.1f}秒")
        print(f"  组件统计: {status['component_stats']}")
        print(f"  活跃告警: {status['active_alerts']}")
        
        if status['performance_summary']:
            perf = status['performance_summary']
            print(f"  性能摘要:")
            print(f"    CPU: {perf['cpu_percent']:.1f}%")
            print(f"    内存: {perf['memory_percent']:.1f}%")
            print(f"    磁盘: {perf['disk_percent']:.1f}%")
            print(f"    进程数: {perf['process_count']}")
        
        print(f"  组件详情:")
        for name, component in status['components'].items():
            print(f"    {name}: {component['status']} "
                 f"(响应时间: {component['response_time']:.3f}s)")
        
        # 获取性能趋势
        print(f"\n📈 性能趋势分析:")
        trends = monitor.get_performance_trends(hours=1)
        
        if 'error' not in trends:
            for metric in ['cpu', 'memory', 'disk']:
                trend_data = trends[metric]
                print(f"  {metric.upper()}:")
                print(f"    当前: {trend_data['current']:.1f}%")
                print(f"    平均: {trend_data['average']:.1f}%")
                print(f"    趋势: {trend_data['trend']}")
        else:
            print(f"  {trends['error']}")
        
        # 获取告警摘要
        print(f"\n🚨 告警摘要:")
        alerts = monitor.get_alerts_summary()
        
        print(f"  活跃告警: {alerts['active_alerts']}")
        print(f"  总生成告警: {alerts['total_alerts_generated']}")
        print(f"  总解决告警: {alerts['total_alerts_resolved']}")
        
        if alerts['alerts']:
            print(f"  最近告警:")
            for alert in alerts['alerts'][-3:]:
                print(f"    - {alert['title']} ({alert['level']}) "
                     f"{'已解决' if alert['resolved'] else '活跃'}")
        
        # 停止监控
        print(f"\n🛑 停止系统监控...")
        await monitor.stop_monitoring()
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_system_health_monitor())
