"""
性能指标收集器

这个模块实现了全面的性能指标收集和智能告警机制，为系统运维
提供详细的性能数据和预警功能。

主要功能：
- 业务指标收集（公告处理、下载、向量化）
- 性能指标聚合和分析
- 智能阈值监控和告警
- 性能趋势预测和异常检测
- 可视化数据导出

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import json
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field, asdict
from enum import Enum, IntEnum
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict, deque
import statistics
import threading
import sys

# 设置路径
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricType(Enum):
    """指标类型枚举"""
    COUNTER = "counter"          # 计数器（只增不减）
    GAUGE = "gauge"             # 仪表（可增可减）
    HISTOGRAM = "histogram"      # 直方图（分布统计）
    TIMER = "timer"             # 计时器（时间测量）
    RATE = "rate"               # 速率（单位时间内的变化）


class MetricUnit(Enum):
    """指标单位枚举"""
    COUNT = "count"             # 计数
    PERCENT = "percent"         # 百分比
    SECONDS = "seconds"         # 秒
    MILLISECONDS = "ms"         # 毫秒
    BYTES = "bytes"            # 字节
    KILOBYTES = "kb"           # 千字节
    MEGABYTES = "mb"           # 兆字节
    PER_SECOND = "per_second"   # 每秒
    PER_MINUTE = "per_minute"   # 每分钟


class AlertCondition(Enum):
    """告警条件枚举"""
    GREATER_THAN = "gt"         # 大于
    LESS_THAN = "lt"           # 小于
    EQUALS = "eq"              # 等于
    NOT_EQUALS = "ne"          # 不等于
    INCREASE_RATE = "inc_rate"  # 增长率
    DECREASE_RATE = "dec_rate"  # 下降率


@dataclass
class MetricDefinition:
    """指标定义"""
    name: str
    metric_type: MetricType
    unit: MetricUnit
    description: str
    labels: List[str] = field(default_factory=list)
    aggregation_window: int = 60  # 聚合时间窗口（秒）
    retention_hours: int = 24     # 数据保留时间（小时）


@dataclass
class MetricValue:
    """指标值"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class AlertRule:
    """告警规则"""
    name: str
    metric_name: str
    condition: AlertCondition
    threshold: float
    duration_minutes: int = 5     # 持续时间
    severity: str = "warning"     # 严重程度
    description: str = ""
    enabled: bool = True


@dataclass
class MetricAlert:
    """指标告警"""
    rule_name: str
    metric_name: str
    current_value: float
    threshold: float
    condition: AlertCondition
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    severity: str = "warning"
    message: str = ""


class MetricsCollector:
    """
    性能指标收集器
    
    提供企业级的指标收集和告警功能：
    1. 多类型指标收集（计数器、仪表、直方图、计时器）
    2. 智能聚合和分析
    3. 灵活的告警规则配置
    4. 高效的数据存储和查询
    5. 性能趋势分析和预测
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化指标收集器"""
        
        self.config = config or {}
        
        # 收集配置
        metrics_config = self.config.get('metrics', {})
        self.collection_interval = metrics_config.get('collection_interval', 30)
        self.aggregation_interval = metrics_config.get('aggregation_interval', 60)
        self.enable_alerts = metrics_config.get('enable_alerts', True)
        self.max_metrics_in_memory = metrics_config.get('max_metrics_in_memory', 10000)
        
        # 指标定义
        self.metric_definitions: Dict[str, MetricDefinition] = {}
        
        # 指标数据存储
        self.raw_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.aggregated_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=2880))  # 24小时
        
        # 告警规则和状态
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, MetricAlert] = {}
        self.alert_history: List[MetricAlert] = []
        
        # 收集任务
        self.collection_tasks: List[asyncio.Task] = []
        self.is_collecting = False
        
        # 统计信息
        self.collection_stats = {
            'total_metrics_collected': 0,
            'total_alerts_triggered': 0,
            'total_alerts_resolved': 0,
            'collection_errors': 0,
            'start_time': datetime.now()
        }
        
        # 线程锁
        self.metrics_lock = threading.RLock()
        
        # 初始化默认指标和告警规则
        self._initialize_default_metrics()
        self._initialize_default_alerts()
        
        logger.info("📊 性能指标收集器初始化完成")
        self._log_collector_config()

    def _log_collector_config(self):
        """记录收集器配置"""
        logger.info(f"📈 指标收集器配置:")
        logger.info(f"  收集间隔: {self.collection_interval}秒")
        logger.info(f"  聚合间隔: {self.aggregation_interval}秒")
        logger.info(f"  启用告警: {self.enable_alerts}")
        logger.info(f"  内存限制: {self.max_metrics_in_memory}条")

    def _initialize_default_metrics(self):
        """初始化默认指标定义"""
        default_metrics = [
            # 业务指标
            MetricDefinition(
                name="announcements_processed",
                metric_type=MetricType.COUNTER,
                unit=MetricUnit.COUNT,
                description="已处理公告总数",
                labels=["stock_code", "document_type"]
            ),
            MetricDefinition(
                name="announcements_download_success",
                metric_type=MetricType.COUNTER,
                unit=MetricUnit.COUNT,
                description="公告下载成功数",
                labels=["stock_code"]
            ),
            MetricDefinition(
                name="announcements_download_failed",
                metric_type=MetricType.COUNTER,
                unit=MetricUnit.COUNT,
                description="公告下载失败数",
                labels=["stock_code", "error_type"]
            ),
            MetricDefinition(
                name="download_duration",
                metric_type=MetricType.TIMER,
                unit=MetricUnit.SECONDS,
                description="公告下载耗时",
                labels=["stock_code", "file_size_category"]
            ),
            MetricDefinition(
                name="vectorization_success",
                metric_type=MetricType.COUNTER,
                unit=MetricUnit.COUNT,
                description="向量化成功数",
                labels=["stock_code"]
            ),
            MetricDefinition(
                name="vectorization_duration",
                metric_type=MetricType.TIMER,
                unit=MetricUnit.SECONDS,
                description="向量化处理耗时",
                labels=["stock_code", "chunk_count_category"]
            ),
            
            # 系统指标
            MetricDefinition(
                name="api_requests_total",
                metric_type=MetricType.COUNTER,
                unit=MetricUnit.COUNT,
                description="API请求总数",
                labels=["endpoint", "status_code"]
            ),
            MetricDefinition(
                name="api_response_duration",
                metric_type=MetricType.TIMER,
                unit=MetricUnit.MILLISECONDS,
                description="API响应时间",
                labels=["endpoint"]
            ),
            MetricDefinition(
                name="database_connections_active",
                metric_type=MetricType.GAUGE,
                unit=MetricUnit.COUNT,
                description="活跃数据库连接数",
                labels=["database_type"]
            ),
            MetricDefinition(
                name="queue_size",
                metric_type=MetricType.GAUGE,
                unit=MetricUnit.COUNT,
                description="队列长度",
                labels=["queue_name"]
            ),
            
            # 性能指标
            MetricDefinition(
                name="memory_usage_app",
                metric_type=MetricType.GAUGE,
                unit=MetricUnit.MEGABYTES,
                description="应用内存使用量",
                labels=["component"]
            ),
            MetricDefinition(
                name="processing_rate",
                metric_type=MetricType.RATE,
                unit=MetricUnit.PER_MINUTE,
                description="处理速率",
                labels=["operation_type"]
            ),
            MetricDefinition(
                name="error_rate",
                metric_type=MetricType.RATE,
                unit=MetricUnit.PER_MINUTE,
                description="错误率",
                labels=["component", "error_type"]
            )
        ]
        
        for metric in default_metrics:
            self.metric_definitions[metric.name] = metric

    def _initialize_default_alerts(self):
        """初始化默认告警规则"""
        default_alerts = [
            AlertRule(
                name="high_download_failure_rate",
                metric_name="announcements_download_failed",
                condition=AlertCondition.GREATER_THAN,
                threshold=10,
                duration_minutes=5,
                severity="warning",
                description="下载失败率过高"
            ),
            AlertRule(
                name="slow_download_performance",
                metric_name="download_duration",
                condition=AlertCondition.GREATER_THAN,
                threshold=30,  # 30秒
                duration_minutes=3,
                severity="warning",
                description="下载性能过慢"
            ),
            AlertRule(
                name="vectorization_failure_spike",
                metric_name="vectorization_success",
                condition=AlertCondition.LESS_THAN,
                threshold=1,  # 少于1个成功
                duration_minutes=10,
                severity="critical",
                description="向量化服务异常"
            ),
            AlertRule(
                name="high_api_response_time",
                metric_name="api_response_duration",
                condition=AlertCondition.GREATER_THAN,
                threshold=5000,  # 5秒
                duration_minutes=5,
                severity="warning",
                description="API响应时间过长"
            ),
            AlertRule(
                name="high_error_rate",
                metric_name="error_rate",
                condition=AlertCondition.GREATER_THAN,
                threshold=5,  # 每分钟5个错误
                duration_minutes=3,
                severity="critical",
                description="系统错误率过高"
            ),
            AlertRule(
                name="memory_usage_high",
                metric_name="memory_usage_app",
                condition=AlertCondition.GREATER_THAN,
                threshold=1024,  # 1GB
                duration_minutes=5,
                severity="warning",
                description="应用内存使用量过高"
            )
        ]
        
        for alert in default_alerts:
            self.alert_rules[alert.name] = alert

    def register_metric(self, metric_def: MetricDefinition):
        """
        注册指标定义
        
        Args:
            metric_def: 指标定义
        """
        self.metric_definitions[metric_def.name] = metric_def
        logger.info(f"注册指标: {metric_def.name} ({metric_def.metric_type.value})")

    def register_alert_rule(self, alert_rule: AlertRule):
        """
        注册告警规则
        
        Args:
            alert_rule: 告警规则
        """
        self.alert_rules[alert_rule.name] = alert_rule
        logger.info(f"注册告警规则: {alert_rule.name}")

    def record_metric(self, metric_name: str, value: float, 
                     labels: Dict[str, str] = None,
                     timestamp: datetime = None):
        """
        记录指标值
        
        Args:
            metric_name: 指标名称
            value: 指标值
            labels: 标签
            timestamp: 时间戳
        """
        if metric_name not in self.metric_definitions:
            logger.warning(f"未定义的指标: {metric_name}")
            return
        
        labels = labels or {}
        timestamp = timestamp or datetime.now()
        
        metric_value = MetricValue(
            timestamp=timestamp,
            value=value,
            labels=labels
        )
        
        with self.metrics_lock:
            self.raw_metrics[metric_name].append(metric_value)
            self.collection_stats['total_metrics_collected'] += 1
        
        logger.debug(f"记录指标: {metric_name}={value} {labels}")

    def increment_counter(self, metric_name: str, 
                         labels: Dict[str, str] = None,
                         increment: float = 1.0):
        """
        增加计数器指标
        
        Args:
            metric_name: 指标名称
            labels: 标签
            increment: 增量
        """
        self.record_metric(metric_name, increment, labels)

    def record_timer(self, metric_name: str, duration: float,
                    labels: Dict[str, str] = None):
        """
        记录计时器指标
        
        Args:
            metric_name: 指标名称
            duration: 持续时间（秒）
            labels: 标签
        """
        self.record_metric(metric_name, duration, labels)

    def set_gauge(self, metric_name: str, value: float,
                 labels: Dict[str, str] = None):
        """
        设置仪表指标
        
        Args:
            metric_name: 指标名称
            value: 指标值
            labels: 标签
        """
        self.record_metric(metric_name, value, labels)

    async def start_collection(self):
        """启动指标收集"""
        if self.is_collecting:
            logger.warning("指标收集已在运行中")
            return
        
        self.is_collecting = True
        logger.info("🚀 启动指标收集...")
        
        # 启动收集任务
        tasks = [
            self._aggregation_loop(),
            self._alert_evaluation_loop(),
            self._cleanup_loop()
        ]
        
        self.collection_tasks = [asyncio.create_task(task) for task in tasks]
        
        logger.info("✅ 指标收集启动完成")

    async def stop_collection(self):
        """停止指标收集"""
        if not self.is_collecting:
            return
        
        self.is_collecting = False
        logger.info("🛑 停止指标收集...")
        
        # 取消所有收集任务
        for task in self.collection_tasks:
            task.cancel()
        
        # 等待任务完成
        if self.collection_tasks:
            await asyncio.gather(*self.collection_tasks, return_exceptions=True)
        
        self.collection_tasks.clear()
        logger.info("✅ 指标收集停止完成")

    async def _aggregation_loop(self):
        """聚合循环"""
        while self.is_collecting:
            try:
                await self._aggregate_metrics()
                await asyncio.sleep(self.aggregation_interval)
            except Exception as e:
                logger.error(f"指标聚合异常: {e}")
                self.collection_stats['collection_errors'] += 1
                await asyncio.sleep(self.aggregation_interval * 2)

    async def _alert_evaluation_loop(self):
        """告警评估循环"""
        while self.is_collecting:
            try:
                if self.enable_alerts:
                    await self._evaluate_alerts()
                await asyncio.sleep(30)  # 30秒评估一次
            except Exception as e:
                logger.error(f"告警评估异常: {e}")
                await asyncio.sleep(60)

    async def _cleanup_loop(self):
        """清理循环"""
        while self.is_collecting:
            try:
                await self._cleanup_old_metrics()
                await asyncio.sleep(3600)  # 1小时清理一次
            except Exception as e:
                logger.error(f"指标清理异常: {e}")
                await asyncio.sleep(3600)

    async def _aggregate_metrics(self):
        """聚合指标"""
        current_time = datetime.now()
        
        with self.metrics_lock:
            for metric_name, raw_values in self.raw_metrics.items():
                if not raw_values:
                    continue
                
                metric_def = self.metric_definitions.get(metric_name)
                if not metric_def:
                    continue
                
                # 获取聚合窗口内的数据
                window_start = current_time - timedelta(seconds=metric_def.aggregation_window)
                window_values = [
                    mv for mv in raw_values
                    if mv.timestamp >= window_start
                ]
                
                if not window_values:
                    continue
                
                # 根据指标类型进行聚合
                aggregated_value = self._aggregate_by_type(metric_def.metric_type, window_values)
                
                if aggregated_value is not None:
                    # 存储聚合结果
                    agg_metric = MetricValue(
                        timestamp=current_time,
                        value=aggregated_value,
                        labels={}  # 聚合后可能需要合并标签
                    )
                    
                    self.aggregated_metrics[metric_name].append(agg_metric)
        
        logger.debug("指标聚合完成")

    def _aggregate_by_type(self, metric_type: MetricType, values: List[MetricValue]) -> Optional[float]:
        """根据指标类型聚合数据"""
        if not values:
            return None
        
        numeric_values = [v.value for v in values]
        
        if metric_type == MetricType.COUNTER:
            # 计数器：求和
            return sum(numeric_values)
        elif metric_type == MetricType.GAUGE:
            # 仪表：最新值
            return values[-1].value
        elif metric_type == MetricType.TIMER:
            # 计时器：平均值
            return statistics.mean(numeric_values)
        elif metric_type == MetricType.HISTOGRAM:
            # 直方图：平均值（简化）
            return statistics.mean(numeric_values)
        elif metric_type == MetricType.RATE:
            # 速率：每分钟的计数
            if len(values) < 2:
                return 0.0
            
            time_span = (values[-1].timestamp - values[0].timestamp).total_seconds() / 60  # 分钟
            if time_span <= 0:
                return 0.0
            
            return sum(numeric_values) / time_span
        else:
            return statistics.mean(numeric_values)

    async def _evaluate_alerts(self):
        """评估告警规则"""
        current_time = datetime.now()
        
        for rule_name, rule in self.alert_rules.items():
            if not rule.enabled:
                continue
            
            try:
                # 获取指标的最新聚合值
                metric_values = self.aggregated_metrics.get(rule.metric_name)
                if not metric_values:
                    continue
                
                # 检查是否满足告警条件
                should_alert = self._check_alert_condition(rule, metric_values)
                
                alert_key = f"{rule_name}_{rule.metric_name}"
                
                if should_alert and alert_key not in self.active_alerts:
                    # 触发新告警
                    current_value = metric_values[-1].value if metric_values else 0
                    
                    alert = MetricAlert(
                        rule_name=rule_name,
                        metric_name=rule.metric_name,
                        current_value=current_value,
                        threshold=rule.threshold,
                        condition=rule.condition,
                        triggered_at=current_time,
                        severity=rule.severity,
                        message=f"{rule.description}: {rule.metric_name}={current_value:.2f} {rule.condition.value} {rule.threshold}"
                    )
                    
                    self.active_alerts[alert_key] = alert
                    self.alert_history.append(alert)
                    self.collection_stats['total_alerts_triggered'] += 1
                    
                    logger.warning(f"🚨 触发告警: {alert.message}")
                
                elif not should_alert and alert_key in self.active_alerts:
                    # 解决告警
                    alert = self.active_alerts[alert_key]
                    alert.resolved_at = current_time
                    
                    del self.active_alerts[alert_key]
                    self.collection_stats['total_alerts_resolved'] += 1
                    
                    logger.info(f"✅ 告警已解决: {alert.rule_name}")
                    
            except Exception as e:
                logger.error(f"评估告警规则 {rule_name} 失败: {e}")

    def _check_alert_condition(self, rule: AlertRule, metric_values: deque) -> bool:
        """检查告警条件"""
        if not metric_values:
            return False
        
        current_time = datetime.now()
        duration_threshold = current_time - timedelta(minutes=rule.duration_minutes)
        
        # 获取持续时间内的值
        recent_values = [
            mv for mv in metric_values
            if mv.timestamp >= duration_threshold
        ]
        
        if not recent_values:
            return False
        
        # 检查条件
        if rule.condition == AlertCondition.GREATER_THAN:
            return all(mv.value > rule.threshold for mv in recent_values)
        elif rule.condition == AlertCondition.LESS_THAN:
            return all(mv.value < rule.threshold for mv in recent_values)
        elif rule.condition == AlertCondition.EQUALS:
            return all(abs(mv.value - rule.threshold) < 0.001 for mv in recent_values)
        elif rule.condition == AlertCondition.NOT_EQUALS:
            return all(abs(mv.value - rule.threshold) >= 0.001 for mv in recent_values)
        else:
            return False

    async def _cleanup_old_metrics(self):
        """清理旧指标数据"""
        current_time = datetime.now()
        
        with self.metrics_lock:
            for metric_name, metric_def in self.metric_definitions.items():
                retention_threshold = current_time - timedelta(hours=metric_def.retention_hours)
                
                # 清理原始指标
                if metric_name in self.raw_metrics:
                    raw_queue = self.raw_metrics[metric_name]
                    # deque会自动限制大小，这里只是示例
                    
                # 清理聚合指标
                if metric_name in self.aggregated_metrics:
                    agg_queue = self.aggregated_metrics[metric_name]
                    while agg_queue and agg_queue[0].timestamp < retention_threshold:
                        agg_queue.popleft()
        
        # 清理告警历史
        retention_threshold = current_time - timedelta(days=7)  # 保留7天
        self.alert_history = [
            alert for alert in self.alert_history
            if alert.triggered_at > retention_threshold
        ]
        
        logger.debug("指标数据清理完成")

    def get_metric_statistics(self) -> Dict[str, Any]:
        """
        获取指标统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        current_time = datetime.now()
        uptime = current_time - self.collection_stats['start_time']
        
        metric_counts = {}
        with self.metrics_lock:
            for metric_name in self.metric_definitions.keys():
                raw_count = len(self.raw_metrics.get(metric_name, []))
                agg_count = len(self.aggregated_metrics.get(metric_name, []))
                metric_counts[metric_name] = {
                    'raw_count': raw_count,
                    'aggregated_count': agg_count
                }
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'is_collecting': self.is_collecting,
            'total_metric_definitions': len(self.metric_definitions),
            'total_alert_rules': len(self.alert_rules),
            'active_alerts': len(self.active_alerts),
            'collection_stats': self.collection_stats.copy(),
            'metric_counts': metric_counts
        }

    def get_recent_metrics(self, metric_name: str, hours: int = 1) -> List[Dict[str, Any]]:
        """
        获取最近的指标数据
        
        Args:
            metric_name: 指标名称
            hours: 时间范围（小时）
            
        Returns:
            List[Dict[str, Any]]: 指标数据
        """
        if metric_name not in self.aggregated_metrics:
            return []
        
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(hours=hours)
        
        recent_metrics = [
            {
                'timestamp': mv.timestamp.isoformat(),
                'value': mv.value,
                'labels': mv.labels
            }
            for mv in self.aggregated_metrics[metric_name]
            if mv.timestamp > cutoff_time
        ]
        
        return recent_metrics

    def get_alerts_summary(self) -> Dict[str, Any]:
        """
        获取告警摘要
        
        Returns:
            Dict[str, Any]: 告警摘要
        """
        return {
            'active_alerts': len(self.active_alerts),
            'total_rules': len(self.alert_rules),
            'enabled_rules': len([r for r in self.alert_rules.values() if r.enabled]),
            'alerts': [
                {
                    'rule_name': alert.rule_name,
                    'metric_name': alert.metric_name,
                    'current_value': alert.current_value,
                    'threshold': alert.threshold,
                    'severity': alert.severity,
                    'triggered_at': alert.triggered_at.isoformat(),
                    'resolved_at': alert.resolved_at.isoformat() if alert.resolved_at else None,
                    'message': alert.message
                }
                for alert in list(self.active_alerts.values()) + self.alert_history[-10:]
            ]
        }

    # 便捷装饰器
    def timer_decorator(self, metric_name: str, labels: Dict[str, str] = None):
        """
        计时装饰器
        
        Usage:
            @metrics.timer_decorator("api_response_duration", {"endpoint": "process"})
            async def process_announcement(self, announcement):
                return await self.process(announcement)
        """
        def decorator(func):
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start_time
                    self.record_timer(metric_name, duration, labels)
            
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start_time
                    self.record_timer(metric_name, duration, labels)
            
            import inspect
            if inspect.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
        
        return decorator


# 全局指标收集器实例
_global_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """
    获取全局指标收集器实例
    
    Returns:
        MetricsCollector: 收集器实例
    """
    global _global_metrics_collector
    if _global_metrics_collector is None:
        _global_metrics_collector = MetricsCollector()
    return _global_metrics_collector


if __name__ == "__main__":
    # 测试模块
    async def test_metrics_collector():
        """测试指标收集器"""
        
        print("\n" + "="*70)
        print("📊 性能指标收集器测试")
        print("="*70)
        
        # 创建收集器
        config = {
            'metrics': {
                'collection_interval': 10,
                'aggregation_interval': 30,
                'enable_alerts': True,
                'max_metrics_in_memory': 1000
            }
        }
        
        collector = MetricsCollector(config)
        
        # 启动收集
        print("\n🚀 启动指标收集...")
        await collector.start_collection()
        
        # 模拟指标数据
        print(f"\n📈 模拟指标数据...")
        
        # 业务指标
        for i in range(10):
            collector.increment_counter("announcements_processed", {"stock_code": "00700"})
            collector.increment_counter("announcements_download_success", {"stock_code": "00700"})
            collector.record_timer("download_duration", 2.5 + i * 0.3, {"stock_code": "00700"})
            collector.record_timer("vectorization_duration", 15.0 + i * 2, {"stock_code": "00700"})
            
            # 系统指标
            collector.increment_counter("api_requests_total", {"endpoint": "/process", "status_code": "200"})
            collector.record_timer("api_response_duration", 1500 + i * 100)
            collector.set_gauge("database_connections_active", 25 + i, {"database_type": "clickhouse"})
            collector.set_gauge("memory_usage_app", 512 + i * 10, {"component": "processor"})
            
            await asyncio.sleep(0.5)
        
        # 模拟一些异常指标触发告警
        print(f"\n🚨 模拟异常指标...")
        for i in range(5):
            collector.increment_counter("announcements_download_failed", {"error_type": "timeout"})
            collector.record_timer("download_duration", 35.0, {"stock_code": "00001"})  # 超过30秒阈值
            collector.set_gauge("memory_usage_app", 1200, {"component": "processor"})  # 超过1GB阈值
        
        # 等待聚合和告警评估
        print(f"\n⏳ 等待数据聚合和告警评估（35秒）...")
        await asyncio.sleep(35)
        
        # 获取统计信息
        print(f"\n📊 指标统计信息:")
        stats = collector.get_metric_statistics()
        
        print(f"  运行时间: {stats['uptime_seconds']:.1f}秒")
        print(f"  正在收集: {stats['is_collecting']}")
        print(f"  指标定义: {stats['total_metric_definitions']}")
        print(f"  告警规则: {stats['total_alert_rules']}")
        print(f"  活跃告警: {stats['active_alerts']}")
        print(f"  收集统计: {stats['collection_stats']}")
        
        # 获取指标数据示例
        print(f"\n📈 最近指标数据示例:")
        download_metrics = collector.get_recent_metrics("download_duration", hours=1)
        if download_metrics:
            print(f"  下载耗时指标 (最近{len(download_metrics)}个点):")
            for metric in download_metrics[-3:]:
                print(f"    {metric['timestamp']}: {metric['value']:.2f}秒")
        
        # 获取告警摘要
        print(f"\n🚨 告警摘要:")
        alerts = collector.get_alerts_summary()
        
        print(f"  活跃告警: {alerts['active_alerts']}")
        print(f"  启用规则: {alerts['enabled_rules']}/{alerts['total_rules']}")
        
        if alerts['alerts']:
            print(f"  最近告警:")
            for alert in alerts['alerts'][-5:]:
                status = "已解决" if alert['resolved_at'] else "活跃"
                print(f"    - {alert['rule_name']}: {alert['message']} ({status})")
        
        # 测试装饰器
        print(f"\n🎭 测试计时装饰器...")
        
        @collector.timer_decorator("test_operation_duration", {"operation": "test"})
        async def test_operation():
            await asyncio.sleep(0.1)  # 模拟100ms操作
            return "操作完成"
        
        result = await test_operation()
        print(f"  装饰器结果: {result}")
        
        # 停止收集
        print(f"\n🛑 停止指标收集...")
        await collector.stop_collection()
        
        # 最终统计
        final_stats = collector.get_metric_statistics()
        print(f"  最终收集指标数: {final_stats['collection_stats']['total_metrics_collected']}")
        print(f"  最终触发告警数: {final_stats['collection_stats']['total_alerts_triggered']}")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_metrics_collector())
