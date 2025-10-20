#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监听服务日志系统
提供结构化日志记录、日志轮转和日志分析功能
"""

import logging
import logging.handlers
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class LogType(Enum):
    """日志类型枚举"""
    SYSTEM = "system"
    MONITOR = "monitor"
    DOWNLOAD = "download"
    PROCESS = "process"
    ERROR = "error"
    PERFORMANCE = "performance"


@dataclass
class LogEntry:
    """日志条目数据类"""
    timestamp: float
    level: str
    log_type: str
    stock_code: Optional[str]
    message: str
    context: Dict[str, Any]
    duration: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False)


class MonitorLogger:
    """监听服务专用日志器"""
    
    def __init__(self, config: Dict[str, Any]):
        """初始化日志器"""
        self.config = config
        
        # 日志配置
        log_config = config.get('monitoring', {}).get('logging', {})
        self.log_level = getattr(logging, log_config.get('level', 'INFO').upper())
        self.log_dir = Path(log_config.get('log_dir', 'logs'))
        self.max_file_size = log_config.get('max_file_size', 10 * 1024 * 1024)  # 10MB
        self.backup_count = log_config.get('backup_count', 5)
        self.enable_structured_logging = log_config.get('enable_structured_logging', True)
        self.enable_performance_logging = log_config.get('enable_performance_logging', True)
        
        # 创建日志目录
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化日志器
        self._init_loggers()
        
        # 日志缓存
        self._log_entries: List[LogEntry] = []
        self._max_cache_size = 1000
        
        # 性能追踪
        self._performance_data: Dict[str, List[float]] = {}
        
        self.system_logger.info("监听服务日志系统初始化完成")
    
    def _init_loggers(self):
        """初始化各类型日志器"""
        # 系统日志
        self.system_logger = self._create_logger(
            'monitor.system', 
            self.log_dir / 'system.log'
        )
        
        # 监听日志
        self.monitor_logger = self._create_logger(
            'monitor.monitor',
            self.log_dir / 'monitor.log'
        )
        
        # 下载日志
        self.download_logger = self._create_logger(
            'monitor.download',
            self.log_dir / 'download.log'
        )
        
        # 处理日志
        self.process_logger = self._create_logger(
            'monitor.process',
            self.log_dir / 'process.log'
        )
        
        # 错误日志
        self.error_logger = self._create_logger(
            'monitor.error',
            self.log_dir / 'error.log'
        )
        
        # 性能日志
        self.performance_logger = self._create_logger(
            'monitor.performance',
            self.log_dir / 'performance.log'
        )
        
        # 结构化日志
        if self.enable_structured_logging:
            self.structured_logger = self._create_logger(
                'monitor.structured',
                self.log_dir / 'structured.log',
                use_json_formatter=True
            )
    
    def _create_logger(self, name: str, log_file: Path, 
                      use_json_formatter: bool = False) -> logging.Logger:
        """创建日志器"""
        logger = logging.getLogger(name)
        logger.setLevel(self.log_level)
        
        # 避免重复添加handler
        if logger.handlers:
            return logger
        
        # 文件handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(self.log_level)
        
        # 格式器
        if use_json_formatter:
            formatter = logging.Formatter('%(message)s')
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def log_system(self, level: LogLevel, message: str, **kwargs):
        """记录系统日志"""
        self._log(LogType.SYSTEM, level, None, message, kwargs)
    
    def log_monitor(self, level: LogLevel, stock_code: str, message: str, **kwargs):
        """记录监听日志"""
        self._log(LogType.MONITOR, level, stock_code, message, kwargs)
    
    def log_download(self, level: LogLevel, stock_code: str, message: str, **kwargs):
        """记录下载日志"""
        self._log(LogType.DOWNLOAD, level, stock_code, message, kwargs)
    
    def log_process(self, level: LogLevel, stock_code: str, message: str, **kwargs):
        """记录处理日志"""
        self._log(LogType.PROCESS, level, stock_code, message, kwargs)
    
    def log_error(self, level: LogLevel, stock_code: Optional[str], message: str, **kwargs):
        """记录错误日志"""
        self._log(LogType.ERROR, level, stock_code, message, kwargs)
    
    def log_performance(self, operation: str, duration: float, stock_code: Optional[str] = None, **kwargs):
        """记录性能日志"""
        if self.enable_performance_logging:
            # 记录到性能数据
            if operation not in self._performance_data:
                self._performance_data[operation] = []
            self._performance_data[operation].append(duration)
            
            # 保持最近100次记录
            if len(self._performance_data[operation]) > 100:
                self._performance_data[operation] = self._performance_data[operation][-100:]
            
            # 记录日志
            message = f"操作 {operation} 耗时 {duration:.3f}s"
            self._log(LogType.PERFORMANCE, LogLevel.INFO, stock_code, message, kwargs, duration)
    
    def _log(self, log_type: LogType, level: LogLevel, stock_code: Optional[str], 
             message: str, context: Dict[str, Any], duration: Optional[float] = None):
        """内部日志记录方法"""
        # 创建日志条目
        entry = LogEntry(
            timestamp=time.time(),
            level=level.name,
            log_type=log_type.value,
            stock_code=stock_code,
            message=message,
            context=context,
            duration=duration
        )
        
        # 缓存日志条目
        self._log_entries.append(entry)
        if len(self._log_entries) > self._max_cache_size:
            self._log_entries = self._log_entries[-self._max_cache_size:]
        
        # 选择对应的日志器
        logger_map = {
            LogType.SYSTEM: self.system_logger,
            LogType.MONITOR: self.monitor_logger,
            LogType.DOWNLOAD: self.download_logger,
            LogType.PROCESS: self.process_logger,
            LogType.ERROR: self.error_logger,
            LogType.PERFORMANCE: self.performance_logger
        }
        
        logger = logger_map.get(log_type, self.system_logger)
        
        # 格式化消息
        if stock_code:
            formatted_message = f"[{stock_code}] {message}"
        else:
            formatted_message = message
        
        if context:
            formatted_message += f" | Context: {json.dumps(context, ensure_ascii=False)}"
        
        # 记录到对应日志器
        logger.log(level.value, formatted_message)
        
        # 记录到结构化日志
        if self.enable_structured_logging:
            self.structured_logger.info(entry.to_json())
    
    def get_recent_logs(self, log_type: Optional[LogType] = None, 
                       stock_code: Optional[str] = None,
                       level: Optional[LogLevel] = None,
                       limit: int = 100) -> List[LogEntry]:
        """获取最近的日志条目"""
        logs = self._log_entries
        
        # 过滤条件
        if log_type:
            logs = [log for log in logs if log.log_type == log_type.value]
        
        if stock_code:
            logs = [log for log in logs if log.stock_code == stock_code]
        
        if level:
            logs = [log for log in logs if log.level == level.name]
        
        # 按时间排序并限制数量
        logs.sort(key=lambda x: x.timestamp, reverse=True)
        return logs[:limit]
    
    def get_performance_stats(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """获取性能统计"""
        if operation:
            if operation in self._performance_data:
                durations = self._performance_data[operation]
                return {
                    'operation': operation,
                    'count': len(durations),
                    'avg_duration': sum(durations) / len(durations),
                    'min_duration': min(durations),
                    'max_duration': max(durations),
                    'recent_durations': durations[-10:]  # 最近10次
                }
            else:
                return {'operation': operation, 'count': 0}
        else:
            stats = {}
            for op, durations in self._performance_data.items():
                stats[op] = {
                    'count': len(durations),
                    'avg_duration': sum(durations) / len(durations) if durations else 0,
                    'min_duration': min(durations) if durations else 0,
                    'max_duration': max(durations) if durations else 0
                }
            return stats
    
    def get_log_stats(self, hours: int = 24) -> Dict[str, Any]:
        """获取日志统计"""
        cutoff_time = time.time() - (hours * 3600)
        recent_logs = [log for log in self._log_entries if log.timestamp > cutoff_time]
        
        stats = {
            'total_logs': len(recent_logs),
            'by_type': {},
            'by_level': {},
            'by_stock': {},
            'time_range_hours': hours
        }
        
        for log in recent_logs:
            # 按类型统计
            log_type = log.log_type
            stats['by_type'][log_type] = stats['by_type'].get(log_type, 0) + 1
            
            # 按级别统计
            level = log.level
            stats['by_level'][level] = stats['by_level'].get(level, 0) + 1
            
            # 按股票统计
            if log.stock_code:
                stock = log.stock_code
                stats['by_stock'][stock] = stats['by_stock'].get(stock, 0) + 1
        
        return stats
    
    def cleanup_old_logs(self, days: int = 30):
        """清理旧日志条目"""
        cutoff_time = time.time() - (days * 24 * 3600)
        original_count = len(self._log_entries)
        
        self._log_entries = [
            log for log in self._log_entries 
            if log.timestamp > cutoff_time
        ]
        
        cleaned_count = original_count - len(self._log_entries)
        if cleaned_count > 0:
            self.log_system(
                LogLevel.INFO, 
                f"清理了 {cleaned_count} 条过期日志条目"
            )
    
    def export_logs(self, file_path: Path, log_type: Optional[LogType] = None,
                   hours: int = 24) -> bool:
        """导出日志到文件"""
        try:
            logs = self.get_recent_logs(log_type, limit=10000)
            cutoff_time = time.time() - (hours * 3600)
            logs = [log for log in logs if log.timestamp > cutoff_time]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    [log.to_dict() for log in logs],
                    f, 
                    ensure_ascii=False,
                    indent=2
                )
            
            self.log_system(
                LogLevel.INFO,
                f"导出了 {len(logs)} 条日志到 {file_path}"
            )
            return True
            
        except Exception as e:
            self.log_error(
                LogLevel.ERROR,
                None,
                f"导出日志失败: {e}"
            )
            return False
    
    def cleanup(self):
        """清理资源"""
        try:
            # 输出最终统计
            stats = self.get_log_stats()
            performance_stats = self.get_performance_stats()
            
            self.log_system(
                LogLevel.INFO,
                "日志系统清理",
                log_stats=stats,
                performance_stats=performance_stats
            )
            
            # 关闭所有handler
            for logger_name in ['monitor.system', 'monitor.monitor', 'monitor.download', 
                              'monitor.process', 'monitor.error', 'monitor.performance']:
                logger = logging.getLogger(logger_name)
                for handler in logger.handlers[:]:
                    handler.close()
                    logger.removeHandler(handler)
                    
        except Exception as e:
            print(f"日志系统清理失败: {e}")


# 性能追踪装饰器
def track_performance(logger: MonitorLogger, operation: str):
    """性能追踪装饰器"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                logger.log_performance(operation, duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.log_performance(f"{operation}_error", duration)
                raise
        
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.log_performance(operation, duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.log_performance(f"{operation}_error", duration)
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator