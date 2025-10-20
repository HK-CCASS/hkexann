#!/usr/bin/env python3
"""
Milvus性能监控和优化模块

提供对6种搜索类型的全面性能监控：
1. Vector Search - 向量搜索性能监控
2. Text Search - 文本搜索性能监控
3. Hybrid Search - 混合搜索性能监控
4. Range Search - 范围搜索性能监控
5. Multi-Vector Search - 多向量搜索性能监控
6. Aggregation Search - 聚合搜索性能监控

主要功能：
- 实时延迟监控 (latency tracking)
- 吞吐量统计 (throughput metrics)
- 资源使用监控 (resource usage)
- 性能基准测试 (benchmarking)
- 自动优化建议 (optimization recommendations)

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import time
import logging
import statistics
import psutil
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class SearchType(Enum):
    """搜索类型枚举"""
    VECTOR = "vector"
    TEXT = "text"
    HYBRID = "hybrid"
    RANGE = "range"
    MULTI_VECTOR = "multi_vector"
    AGGREGATION = "aggregation"


class PerformanceLevel(Enum):
    """性能等级"""
    EXCELLENT = "excellent"    # < 50ms
    GOOD = "good"             # 50-200ms
    ACCEPTABLE = "acceptable" # 200-500ms
    POOR = "poor"             # 500-1000ms
    CRITICAL = "critical"     # > 1000ms


@dataclass
class SearchMetrics:
    """单次搜索性能指标"""
    search_type: SearchType
    latency_ms: float
    collection_name: str
    query_size: int = 0
    result_count: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    success: bool = True
    error_message: Optional[str] = None

    # 搜索特定参数
    top_k: Optional[int] = None
    vector_dimension: Optional[int] = None
    text_length: Optional[int] = None
    filter_expression: Optional[str] = None


@dataclass
class PerformanceStats:
    """性能统计数据"""
    search_type: SearchType
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    # 延迟统计 (毫秒)
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    median_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    # 吞吐量统计
    queries_per_second: float = 0.0
    peak_qps: float = 0.0

    # 资源使用统计
    avg_memory_usage_mb: float = 0.0
    peak_memory_usage_mb: float = 0.0
    avg_cpu_usage_percent: float = 0.0
    peak_cpu_usage_percent: float = 0.0

    # 时间范围
    start_time: datetime = field(default_factory=datetime.now)
    last_update: datetime = field(default_factory=datetime.now)

    # 性能等级
    performance_level: PerformanceLevel = PerformanceLevel.GOOD


@dataclass
class OptimizationRecommendation:
    """优化建议"""
    search_type: SearchType
    issue_type: str
    priority: str  # high, medium, low
    description: str
    solution: str
    expected_improvement: str


class PerformanceMonitor:
    """Milvus性能监控器"""

    def __init__(self, max_metrics_history: int = 10000):
        """初始化性能监控器"""
        self.max_metrics_history = max_metrics_history

        # 性能指标存储 - 使用deque实现固定大小的循环缓冲区
        self.metrics_history: Dict[SearchType, deque] = {
            search_type: deque(maxlen=max_metrics_history)
            for search_type in SearchType
        }

        # 实时统计数据
        self.performance_stats: Dict[SearchType, PerformanceStats] = {
            search_type: PerformanceStats(search_type=search_type)
            for search_type in SearchType
        }

        # 系统监控
        self._monitoring_active = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._stats_lock = threading.RLock()

        # QPS计算窗口 (滑动窗口)
        self._qps_window_seconds = 60
        self._qps_timestamps: Dict[SearchType, deque] = {
            search_type: deque()
            for search_type in SearchType
        }

        # 性能基准 (毫秒)
        self.performance_thresholds = {
            SearchType.VECTOR: {
                PerformanceLevel.EXCELLENT: 50,
                PerformanceLevel.GOOD: 200,
                PerformanceLevel.ACCEPTABLE: 500,
                PerformanceLevel.POOR: 1000
            },
            SearchType.TEXT: {
                PerformanceLevel.EXCELLENT: 30,
                PerformanceLevel.GOOD: 100,
                PerformanceLevel.ACCEPTABLE: 300,
                PerformanceLevel.POOR: 800
            },
            SearchType.HYBRID: {
                PerformanceLevel.EXCELLENT: 100,
                PerformanceLevel.GOOD: 300,
                PerformanceLevel.ACCEPTABLE: 800,
                PerformanceLevel.POOR: 1500
            },
            SearchType.RANGE: {
                PerformanceLevel.EXCELLENT: 20,
                PerformanceLevel.GOOD: 80,
                PerformanceLevel.ACCEPTABLE: 250,
                PerformanceLevel.POOR: 600
            },
            SearchType.MULTI_VECTOR: {
                PerformanceLevel.EXCELLENT: 150,
                PerformanceLevel.GOOD: 400,
                PerformanceLevel.ACCEPTABLE: 1000,
                PerformanceLevel.POOR: 2000
            },
            SearchType.AGGREGATION: {
                PerformanceLevel.EXCELLENT: 200,
                PerformanceLevel.GOOD: 500,
                PerformanceLevel.ACCEPTABLE: 1200,
                PerformanceLevel.POOR: 2500
            }
        }

        logger.info("🔍 Milvus性能监控器初始化完成")
        logger.info(f"  最大指标历史数量: {max_metrics_history}")
        logger.info(f"  QPS计算窗口: {self._qps_window_seconds}秒")

    def record_search_performance(self,
                                metrics: SearchMetrics) -> None:
        """记录搜索性能指标"""
        with self._stats_lock:
            # 添加系统资源使用情况
            if metrics.memory_usage_mb == 0.0:
                metrics.memory_usage_mb = psutil.Process().memory_info().rss / (1024 * 1024)
            if metrics.cpu_usage_percent == 0.0:
                metrics.cpu_usage_percent = psutil.cpu_percent()

            # 存储指标
            self.metrics_history[metrics.search_type].append(metrics)

            # 更新QPS时间戳
            current_time = time.time()
            self._qps_timestamps[metrics.search_type].append(current_time)

            # 清理旧的QPS时间戳 (超过窗口期的)
            cutoff_time = current_time - self._qps_window_seconds
            while (self._qps_timestamps[metrics.search_type] and
                   self._qps_timestamps[metrics.search_type][0] < cutoff_time):
                self._qps_timestamps[metrics.search_type].popleft()

            # 更新统计数据
            self._update_performance_stats(metrics)

        logger.debug(f"📊 记录{metrics.search_type.value}搜索性能: "
                    f"{metrics.latency_ms:.1f}ms, 成功: {metrics.success}")

    def _update_performance_stats(self, metrics: SearchMetrics) -> None:
        """更新性能统计数据"""
        stats = self.performance_stats[metrics.search_type]
        history = list(self.metrics_history[metrics.search_type])

        # 基本计数统计
        stats.total_requests += 1
        if metrics.success:
            stats.successful_requests += 1
        else:
            stats.failed_requests += 1

        # 只对成功的请求计算延迟统计
        successful_metrics = [m for m in history if m.success]
        if not successful_metrics:
            return

        latencies = [m.latency_ms for m in successful_metrics]
        memory_usages = [m.memory_usage_mb for m in successful_metrics]
        cpu_usages = [m.cpu_usage_percent for m in successful_metrics]

        # 延迟统计
        stats.min_latency_ms = min(latencies)
        stats.max_latency_ms = max(latencies)
        stats.avg_latency_ms = statistics.mean(latencies)

        if len(latencies) >= 2:
            stats.median_latency_ms = statistics.median(latencies)
            if len(latencies) >= 20:  # 足够的样本计算百分位数
                sorted_latencies = sorted(latencies)
                stats.p95_latency_ms = sorted_latencies[int(0.95 * len(sorted_latencies))]
                stats.p99_latency_ms = sorted_latencies[int(0.99 * len(sorted_latencies))]

        # QPS计算 (基于滑动窗口)
        qps_count = len(self._qps_timestamps[metrics.search_type])
        if qps_count > 1:
            time_span = (self._qps_timestamps[metrics.search_type][-1] -
                        self._qps_timestamps[metrics.search_type][0])
            if time_span > 0:
                current_qps = qps_count / time_span
                stats.queries_per_second = current_qps
                stats.peak_qps = max(stats.peak_qps, current_qps)

        # 资源使用统计
        if memory_usages:
            stats.avg_memory_usage_mb = statistics.mean(memory_usages)
            stats.peak_memory_usage_mb = max(memory_usages)

        if cpu_usages:
            stats.avg_cpu_usage_percent = statistics.mean(cpu_usages)
            stats.peak_cpu_usage_percent = max(cpu_usages)

        # 确定性能等级
        stats.performance_level = self._determine_performance_level(
            metrics.search_type, stats.avg_latency_ms
        )

        stats.last_update = datetime.now()

    def _determine_performance_level(self,
                                   search_type: SearchType,
                                   avg_latency_ms: float) -> PerformanceLevel:
        """确定性能等级"""
        thresholds = self.performance_thresholds[search_type]

        if avg_latency_ms <= thresholds[PerformanceLevel.EXCELLENT]:
            return PerformanceLevel.EXCELLENT
        elif avg_latency_ms <= thresholds[PerformanceLevel.GOOD]:
            return PerformanceLevel.GOOD
        elif avg_latency_ms <= thresholds[PerformanceLevel.ACCEPTABLE]:
            return PerformanceLevel.ACCEPTABLE
        elif avg_latency_ms <= thresholds[PerformanceLevel.POOR]:
            return PerformanceLevel.POOR
        else:
            return PerformanceLevel.CRITICAL

    def get_performance_summary(self) -> Dict[str, Any]:
        """获取性能总览"""
        with self._stats_lock:
            summary = {
                "timestamp": datetime.now().isoformat(),
                "monitoring_duration_hours": (
                    (datetime.now() - min(
                        stats.start_time for stats in self.performance_stats.values()
                    )).total_seconds() / 3600
                ),
                "search_types": {}
            }

            total_requests = 0
            total_successful = 0
            total_failed = 0

            for search_type, stats in self.performance_stats.items():
                if stats.total_requests == 0:
                    continue

                # 累计统计
                total_requests += stats.total_requests
                total_successful += stats.successful_requests
                total_failed += stats.failed_requests

                # 个别搜索类型统计
                summary["search_types"][search_type.value] = {
                    "requests": {
                        "total": stats.total_requests,
                        "successful": stats.successful_requests,
                        "failed": stats.failed_requests,
                        "success_rate": (stats.successful_requests / stats.total_requests * 100)
                                      if stats.total_requests > 0 else 0
                    },
                    "latency": {
                        "min_ms": stats.min_latency_ms if stats.min_latency_ms != float('inf') else 0,
                        "max_ms": stats.max_latency_ms,
                        "avg_ms": round(stats.avg_latency_ms, 2),
                        "median_ms": round(stats.median_latency_ms, 2),
                        "p95_ms": round(stats.p95_latency_ms, 2),
                        "p99_ms": round(stats.p99_latency_ms, 2)
                    },
                    "throughput": {
                        "current_qps": round(stats.queries_per_second, 2),
                        "peak_qps": round(stats.peak_qps, 2)
                    },
                    "resources": {
                        "avg_memory_mb": round(stats.avg_memory_usage_mb, 1),
                        "peak_memory_mb": round(stats.peak_memory_usage_mb, 1),
                        "avg_cpu_percent": round(stats.avg_cpu_usage_percent, 1),
                        "peak_cpu_percent": round(stats.peak_cpu_usage_percent, 1)
                    },
                    "performance_level": stats.performance_level.value,
                    "last_update": stats.last_update.isoformat()
                }

            # 总体统计
            summary["overall"] = {
                "total_requests": total_requests,
                "successful_requests": total_successful,
                "failed_requests": total_failed,
                "overall_success_rate": (total_successful / total_requests * 100)
                                       if total_requests > 0 else 0
            }

            return summary

    def get_optimization_recommendations(self) -> List[OptimizationRecommendation]:
        """获取优化建议"""
        recommendations = []

        with self._stats_lock:
            for search_type, stats in self.performance_stats.items():
                if stats.total_requests == 0:
                    continue

                # 基于性能等级的建议
                if stats.performance_level == PerformanceLevel.CRITICAL:
                    recommendations.append(OptimizationRecommendation(
                        search_type=search_type,
                        issue_type="critical_latency",
                        priority="high",
                        description=f"{search_type.value}搜索延迟严重超标: {stats.avg_latency_ms:.1f}ms",
                        solution="考虑增加索引分片、优化查询参数或升级硬件资源",
                        expected_improvement="预期延迟减少50-70%"
                    ))

                elif stats.performance_level == PerformanceLevel.POOR:
                    recommendations.append(OptimizationRecommendation(
                        search_type=search_type,
                        issue_type="poor_latency",
                        priority="medium",
                        description=f"{search_type.value}搜索延迟偏高: {stats.avg_latency_ms:.1f}ms",
                        solution="优化索引参数或调整查询策略",
                        expected_improvement="预期延迟减少30-50%"
                    ))

                # 基于成功率的建议
                success_rate = (stats.successful_requests / stats.total_requests * 100) \
                             if stats.total_requests > 0 else 0
                if success_rate < 95:
                    recommendations.append(OptimizationRecommendation(
                        search_type=search_type,
                        issue_type="low_success_rate",
                        priority="high",
                        description=f"{search_type.value}搜索成功率偏低: {success_rate:.1f}%",
                        solution="检查查询参数有效性和集合健康状态",
                        expected_improvement="预期成功率提升至99%+"
                    ))

                # 基于QPS的建议
                if stats.queries_per_second > 0:
                    if search_type == SearchType.VECTOR and stats.queries_per_second < 10:
                        recommendations.append(OptimizationRecommendation(
                            search_type=search_type,
                            issue_type="low_throughput",
                            priority="low",
                            description=f"{search_type.value}搜索吞吐量偏低: {stats.queries_per_second:.1f} QPS",
                            solution="考虑并行化查询或优化索引结构",
                            expected_improvement="预期吞吐量提升2-5倍"
                        ))

                # 基于内存使用的建议
                if stats.peak_memory_usage_mb > 1000:  # 超过1GB
                    recommendations.append(OptimizationRecommendation(
                        search_type=search_type,
                        issue_type="high_memory_usage",
                        priority="medium",
                        description=f"{search_type.value}搜索内存使用过高: {stats.peak_memory_usage_mb:.1f}MB",
                        solution="优化查询结果字段选择，减少不必要的数据传输",
                        expected_improvement="预期内存使用减少30-50%"
                    ))

        # 按优先级排序
        priority_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda x: priority_order.get(x.priority, 3))

        return recommendations

    async def start_monitoring(self) -> None:
        """启动性能监控"""
        if self._monitoring_active:
            logger.warning("性能监控已在运行中")
            return

        self._monitoring_active = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("🚀 Milvus性能监控启动")

    async def stop_monitoring(self) -> None:
        """停止性能监控"""
        self._monitoring_active = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Milvus性能监控停止")

    async def _monitor_loop(self) -> None:
        """监控循环"""
        while self._monitoring_active:
            try:
                # 每30秒输出一次性能摘要
                await asyncio.sleep(30)

                summary = self.get_performance_summary()
                recommendations = self.get_optimization_recommendations()

                # 记录性能摘要
                active_search_types = [st for st in summary["search_types"]
                                     if summary["search_types"][st]["requests"]["total"] > 0]

                if active_search_types:
                    logger.info("📊 性能监控报告:")
                    logger.info(f"  监控时长: {summary['monitoring_duration_hours']:.1f}小时")
                    logger.info(f"  总请求数: {summary['overall']['total_requests']}")
                    logger.info(f"  总成功率: {summary['overall']['overall_success_rate']:.1f}%")

                    for search_type in active_search_types:
                        st_data = summary["search_types"][search_type]
                        logger.info(f"  {search_type}: {st_data['latency']['avg_ms']:.1f}ms平均, "
                                  f"{st_data['throughput']['current_qps']:.1f} QPS, "
                                  f"性能等级: {st_data['performance_level']}")

                # 输出优化建议
                high_priority_recommendations = [r for r in recommendations if r.priority == "high"]
                if high_priority_recommendations:
                    logger.warning("⚠️ 高优先级优化建议:")
                    for rec in high_priority_recommendations[:3]:  # 最多显示3条
                        logger.warning(f"  {rec.description}")
                        logger.warning(f"    解决方案: {rec.solution}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"性能监控循环错误: {e}")

    def export_performance_data(self, filepath: str) -> None:
        """导出性能数据到JSON文件"""
        try:
            summary = self.get_performance_summary()
            recommendations = self.get_optimization_recommendations()

            export_data = {
                "performance_summary": summary,
                "optimization_recommendations": [
                    {
                        "search_type": rec.search_type.value,
                        "issue_type": rec.issue_type,
                        "priority": rec.priority,
                        "description": rec.description,
                        "solution": rec.solution,
                        "expected_improvement": rec.expected_improvement
                    }
                    for rec in recommendations
                ],
                "export_timestamp": datetime.now().isoformat()
            }

            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)

            logger.info(f"📁 性能数据已导出至: {filepath}")

        except Exception as e:
            logger.error(f"导出性能数据失败: {e}")

    def reset_statistics(self) -> None:
        """重置所有统计数据"""
        with self._stats_lock:
            for search_type in SearchType:
                self.metrics_history[search_type].clear()
                self._qps_timestamps[search_type].clear()
                self.performance_stats[search_type] = PerformanceStats(search_type=search_type)

        logger.info("🔄 性能统计数据已重置")


# 全局性能监控器实例
performance_monitor = PerformanceMonitor()


def monitor_search_performance(search_type: SearchType):
    """
    搜索性能监控装饰器

    用法:
    @monitor_search_performance(SearchType.VECTOR)
    async def vector_search(...):
        ...
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            success = True
            error_message = None
            result_count = 0

            try:
                result = await func(*args, **kwargs)

                # 尝试从结果中提取信息
                if isinstance(result, dict):
                    result_count = len(result.get('results', []))

                return result

            except Exception as e:
                success = False
                error_message = str(e)
                raise

            finally:
                # 计算性能指标
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000

                # 创建性能指标记录
                metrics = SearchMetrics(
                    search_type=search_type,
                    latency_ms=latency_ms,
                    collection_name=kwargs.get('collection_name', 'unknown'),
                    result_count=result_count,
                    success=success,
                    error_message=error_message,
                    top_k=kwargs.get('top_k'),
                    vector_dimension=len(kwargs.get('query_vector', [])) if kwargs.get('query_vector') else None,
                    text_length=len(kwargs.get('query_text', '')) if kwargs.get('query_text') else None,
                    filter_expression=kwargs.get('expr')
                )

                # 记录到性能监控器
                performance_monitor.record_search_performance(metrics)

        return wrapper
    return decorator


async def test_performance_monitor():
    """测试性能监控器"""
    logger.info("🧪 开始性能监控器测试")

    # 启动监控
    await performance_monitor.start_monitoring()

    # 模拟一些搜索操作
    import random

    for i in range(50):
        search_type = random.choice(list(SearchType))
        latency_ms = random.uniform(10, 500)
        success = random.random() > 0.1  # 90% 成功率

        metrics = SearchMetrics(
            search_type=search_type,
            latency_ms=latency_ms,
            collection_name="test_collection",
            result_count=random.randint(1, 100) if success else 0,
            success=success,
            error_message="模拟错误" if not success else None
        )

        performance_monitor.record_search_performance(metrics)
        await asyncio.sleep(0.1)

    # 等待一些监控输出
    await asyncio.sleep(5)

    # 获取性能总览
    summary = performance_monitor.get_performance_summary()
    print("\n📊 性能测试总览:")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    # 获取优化建议
    recommendations = performance_monitor.get_optimization_recommendations()
    print(f"\n💡 优化建议 ({len(recommendations)}条):")
    for rec in recommendations[:5]:  # 显示前5条
        print(f"  {rec.priority.upper()}: {rec.description}")
        print(f"    解决方案: {rec.solution}")
        print()

    # 导出数据
    performance_monitor.export_performance_data("./performance_test_report.json")

    # 停止监控
    await performance_monitor.stop_monitoring()

    logger.info("✅ 性能监控器测试完成")


if __name__ == "__main__":
    asyncio.run(test_performance_monitor())