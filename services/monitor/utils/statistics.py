#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监听服务统计系统
提供全面的统计数据收集、分析和报告功能
"""

import time
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
from enum import Enum


class MetricType(Enum):
    """指标类型枚举"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricData:
    """指标数据类"""
    name: str
    metric_type: MetricType
    value: float
    timestamp: float
    tags: Dict[str, str]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'type': self.metric_type.value,
            'value': self.value,
            'timestamp': self.timestamp,
            'tags': self.tags
        }


@dataclass
class SystemStats:
    """系统统计数据类"""
    total_checks: int
    successful_checks: int
    failed_checks: int
    total_downloads: int
    successful_downloads: int
    failed_downloads: int
    total_processes: int
    successful_processes: int
    failed_processes: int
    uptime_seconds: float
    last_update_time: float
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @property
    def check_success_rate(self) -> float:
        """检查成功率"""
        if self.total_checks == 0:
            return 0.0
        return self.successful_checks / self.total_checks
    
    @property
    def download_success_rate(self) -> float:
        """下载成功率"""
        if self.total_downloads == 0:
            return 0.0
        return self.successful_downloads / self.total_downloads
    
    @property
    def process_success_rate(self) -> float:
        """处理成功率"""
        if self.total_processes == 0:
            return 0.0
        return self.successful_processes / self.total_processes


@dataclass
class StockStats:
    """股票统计数据类"""
    stock_code: str
    check_count: int
    announcement_count: int
    download_count: int
    process_count: int
    error_count: int
    last_check_time: float
    last_announcement_time: float
    avg_check_duration: float
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


class StatisticsCollector:
    """统计数据收集器"""
    
    def __init__(self, config: Dict[str, Any]):
        """初始化统计收集器"""
        self.config = config
        
        # 统计配置
        stats_config = config.get('monitoring', {}).get('statistics', {})
        self.enable_detailed_stats = stats_config.get('enable_detailed_stats', True)
        self.stats_retention_hours = stats_config.get('retention_hours', 168)  # 7天
        self.export_interval_minutes = stats_config.get('export_interval_minutes', 60)
        self.stats_file = Path(stats_config.get('stats_file', 'logs/statistics.json'))
        
        # 创建统计目录
        self.stats_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 系统统计
        self.system_stats = SystemStats(
            total_checks=0,
            successful_checks=0,
            failed_checks=0,
            total_downloads=0,
            successful_downloads=0,
            failed_downloads=0,
            total_processes=0,
            successful_processes=0,
            failed_processes=0,
            uptime_seconds=0,
            last_update_time=time.time()
        )
        
        # 股票统计
        self.stock_stats: Dict[str, StockStats] = {}
        
        # 指标数据
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        
        # 时间序列数据
        self.time_series: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        
        # 性能数据
        self.performance_data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # 启动时间
        self.start_time = time.time()
        
        # 定期导出任务
        self._export_task = None
        
    async def start(self):
        """启动统计收集器"""
        # 加载历史数据
        await self._load_stats()
        
        # 启动定期导出任务
        if self.export_interval_minutes > 0:
            self._export_task = asyncio.create_task(self._periodic_export())
    
    def record_metric(self, name: str, value: float, metric_type: MetricType,
                     tags: Optional[Dict[str, str]] = None):
        """记录指标"""
        if tags is None:
            tags = {}
        
        metric = MetricData(
            name=name,
            metric_type=metric_type,
            value=value,
            timestamp=time.time(),
            tags=tags
        )
        
        # 添加到指标队列
        self.metrics[name].append(metric)
        
        # 添加到时间序列（仅数值）
        self.time_series[name].append((metric.timestamp, value))
        
        # 清理过期数据
        self._cleanup_time_series(name)
    
    def record_check(self, stock_code: str, success: bool, duration: float = 0,
                    announcement_count: int = 0):
        """记录检查操作"""
        # 更新系统统计
        self.system_stats.total_checks += 1
        if success:
            self.system_stats.successful_checks += 1
        else:
            self.system_stats.failed_checks += 1
        self.system_stats.last_update_time = time.time()
        
        # 更新股票统计
        if stock_code not in self.stock_stats:
            self.stock_stats[stock_code] = StockStats(
                stock_code=stock_code,
                check_count=0,
                announcement_count=0,
                download_count=0,
                process_count=0,
                error_count=0,
                last_check_time=0,
                last_announcement_time=0,
                avg_check_duration=0
            )
        
        stock_stat = self.stock_stats[stock_code]
        stock_stat.check_count += 1
        stock_stat.last_check_time = time.time()
        
        if announcement_count > 0:
            stock_stat.announcement_count += announcement_count
            stock_stat.last_announcement_time = time.time()
        
        if not success:
            stock_stat.error_count += 1
        
        # 更新平均检查时长
        if duration > 0:
            if stock_stat.avg_check_duration == 0:
                stock_stat.avg_check_duration = duration
            else:
                # 简单移动平均
                stock_stat.avg_check_duration = (
                    stock_stat.avg_check_duration * 0.9 + duration * 0.1
                )
        
        # 记录指标
        self.record_metric(
            'check_duration',
            duration,
            MetricType.TIMER,
            {'stock_code': stock_code, 'success': str(success)}
        )
        
        self.record_metric(
            'announcements_found',
            announcement_count,
            MetricType.GAUGE,
            {'stock_code': stock_code}
        )
    
    def record_download(self, stock_code: str, success: bool, file_count: int = 0,
                       duration: float = 0):
        """记录下载操作"""
        # 更新系统统计
        self.system_stats.total_downloads += 1
        if success:
            self.system_stats.successful_downloads += 1
        else:
            self.system_stats.failed_downloads += 1
        
        # 更新股票统计
        if stock_code in self.stock_stats:
            if success:
                self.stock_stats[stock_code].download_count += file_count
            else:
                self.stock_stats[stock_code].error_count += 1
        
        # 记录指标
        self.record_metric(
            'download_duration',
            duration,
            MetricType.TIMER,
            {'stock_code': stock_code, 'success': str(success)}
        )
        
        if file_count > 0:
            self.record_metric(
                'files_downloaded',
                file_count,
                MetricType.COUNTER,
                {'stock_code': stock_code}
            )
    
    def record_process(self, stock_code: str, success: bool, file_count: int = 0,
                      duration: float = 0):
        """记录处理操作"""
        # 更新系统统计
        self.system_stats.total_processes += 1
        if success:
            self.system_stats.successful_processes += 1
        else:
            self.system_stats.failed_processes += 1
        
        # 更新股票统计
        if stock_code in self.stock_stats:
            if success:
                self.stock_stats[stock_code].process_count += file_count
            else:
                self.stock_stats[stock_code].error_count += 1
        
        # 记录指标
        self.record_metric(
            'process_duration',
            duration,
            MetricType.TIMER,
            {'stock_code': stock_code, 'success': str(success)}
        )
        
        if file_count > 0:
            self.record_metric(
                'files_processed',
                file_count,
                MetricType.COUNTER,
                {'stock_code': stock_code}
            )
    
    def record_performance(self, operation: str, duration: float, 
                         context: Optional[Dict[str, str]] = None):
        """记录性能数据"""
        self.performance_data[operation].append({
            'duration': duration,
            'timestamp': time.time(),
            'context': context or {}
        })
    
    def get_system_stats(self) -> Dict[str, Any]:
        """获取系统统计"""
        # 更新运行时间
        self.system_stats.uptime_seconds = time.time() - self.start_time
        
        stats = self.system_stats.to_dict()
        
        # 添加成功率
        stats['check_success_rate'] = self.system_stats.check_success_rate
        stats['download_success_rate'] = self.system_stats.download_success_rate
        stats['process_success_rate'] = self.system_stats.process_success_rate
        
        return stats
    
    def get_stock_stats(self, stock_code: Optional[str] = None) -> Dict[str, Any]:
        """获取股票统计"""
        if stock_code:
            if stock_code in self.stock_stats:
                return self.stock_stats[stock_code].to_dict()
            else:
                return {}
        else:
            return {
                code: stats.to_dict() 
                for code, stats in self.stock_stats.items()
            }
    
    def get_performance_stats(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """获取性能统计"""
        if operation:
            if operation in self.performance_data:
                data = list(self.performance_data[operation])
                durations = [item['duration'] for item in data]
                
                if durations:
                    return {
                        'operation': operation,
                        'count': len(durations),
                        'avg_duration': sum(durations) / len(durations),
                        'min_duration': min(durations),
                        'max_duration': max(durations),
                        'p50': self._percentile(durations, 50),
                        'p90': self._percentile(durations, 90),
                        'p95': self._percentile(durations, 95),
                        'p99': self._percentile(durations, 99)
                    }
            
            return {'operation': operation, 'count': 0}
        else:
            stats = {}
            for op, data in self.performance_data.items():
                durations = [item['duration'] for item in data]
                if durations:
                    stats[op] = {
                        'count': len(durations),
                        'avg_duration': sum(durations) / len(durations),
                        'min_duration': min(durations),
                        'max_duration': max(durations)
                    }
            return stats
    
    def get_time_series(self, metric_name: str, hours: int = 24) -> List[Tuple[float, float]]:
        """获取时间序列数据"""
        if metric_name not in self.time_series:
            return []
        
        cutoff_time = time.time() - (hours * 3600)
        return [
            (timestamp, value) 
            for timestamp, value in self.time_series[metric_name]
            if timestamp > cutoff_time
        ]
    
    def get_top_stocks(self, metric: str = 'announcement_count', limit: int = 10) -> List[Dict[str, Any]]:
        """获取排名前列的股票"""
        if metric not in ['check_count', 'announcement_count', 'download_count', 'process_count', 'error_count']:
            return []
        
        stocks = [
            {
                'stock_code': stats.stock_code,
                'value': getattr(stats, metric),
                'stats': stats.to_dict()
            }
            for stats in self.stock_stats.values()
        ]
        
        stocks.sort(key=lambda x: x['value'], reverse=True)
        return stocks[:limit]
    
    def generate_report(self) -> Dict[str, Any]:
        """生成统计报告"""
        report = {
            'report_time': datetime.now().isoformat(),
            'uptime_hours': (time.time() - self.start_time) / 3600,
            'system_stats': self.get_system_stats(),
            'performance_stats': self.get_performance_stats(),
            'top_stocks': {
                'by_announcements': self.get_top_stocks('announcement_count', 5),
                'by_checks': self.get_top_stocks('check_count', 5),
                'by_errors': self.get_top_stocks('error_count', 5)
            },
            'total_stocks_monitored': len(self.stock_stats)
        }
        
        return report
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """计算百分位数"""
        if not data:
            return 0.0
        
        data_sorted = sorted(data)
        index = (percentile / 100.0) * (len(data_sorted) - 1)
        
        if index.is_integer():
            return data_sorted[int(index)]
        else:
            lower_index = int(index)
            upper_index = lower_index + 1
            weight = index - lower_index
            return data_sorted[lower_index] * (1 - weight) + data_sorted[upper_index] * weight
    
    def _cleanup_time_series(self, metric_name: str):
        """清理过期的时间序列数据"""
        cutoff_time = time.time() - (self.stats_retention_hours * 3600)
        
        self.time_series[metric_name] = [
            (timestamp, value)
            for timestamp, value in self.time_series[metric_name]
            if timestamp > cutoff_time
        ]
    
    async def _load_stats(self):
        """加载历史统计数据"""
        try:
            if self.stats_file.exists():
                def load_file():
                    with open(self.stats_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                
                data = await asyncio.get_event_loop().run_in_executor(None, load_file)
                
                # 恢复系统统计
                if 'system_stats' in data:
                    sys_data = data['system_stats']
                    for key, value in sys_data.items():
                        if hasattr(self.system_stats, key):
                            setattr(self.system_stats, key, value)
                
                # 恢复股票统计
                if 'stock_stats' in data:
                    for stock_code, stock_data in data['stock_stats'].items():
                        self.stock_stats[stock_code] = StockStats(**stock_data)
                
                print(f"加载历史统计数据：{len(self.stock_stats)} 只股票")
                
        except Exception as e:
            print(f"加载历史统计数据失败: {e}")
    
    async def _save_stats(self):
        """保存统计数据"""
        try:
            data = {
                'save_time': datetime.now().isoformat(),
                'system_stats': self.system_stats.to_dict(),
                'stock_stats': {
                    code: stats.to_dict()
                    for code, stats in self.stock_stats.items()
                }
            }
            
            def save_file():
                with open(self.stats_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            
            await asyncio.get_event_loop().run_in_executor(None, save_file)
            
        except Exception as e:
            print(f"保存统计数据失败: {e}")
    
    async def _periodic_export(self):
        """定期导出统计数据"""
        while True:
            try:
                await asyncio.sleep(self.export_interval_minutes * 60)
                await self._save_stats()
                
                # 清理过期数据
                for metric_name in list(self.time_series.keys()):
                    self._cleanup_time_series(metric_name)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"定期导出统计数据失败: {e}")
    
    async def export_report(self, file_path: Path):
        """导出统计报告"""
        try:
            report = self.generate_report()
            
            def save_report():
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
            
            await asyncio.get_event_loop().run_in_executor(None, save_report)
            print(f"统计报告导出到: {file_path}")
            
        except Exception as e:
            print(f"导出统计报告失败: {e}")
    
    async def cleanup(self):
        """清理资源"""
        try:
            # 停止定期导出任务
            if self._export_task:
                self._export_task.cancel()
                try:
                    await self._export_task
                except asyncio.CancelledError:
                    pass
            
            # 保存最终统计数据
            await self._save_stats()
            
            # 导出最终报告
            final_report_path = self.stats_file.parent / f"final_report_{int(time.time())}.json"
            await self.export_report(final_report_path)
            
        except Exception as e:
            print(f"统计收集器清理失败: {e}")