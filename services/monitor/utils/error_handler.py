#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监听服务错误处理和重试机制
提供统一的错误处理、重试策略和熔断器功能
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass
from enum import Enum
from functools import wraps

import aiohttp
from tenacity import (
    retry, stop_after_attempt, wait_exponential, 
    retry_if_exception_type, before_sleep_log,
    RetryError
)


class ErrorType(Enum):
    """错误类型枚举"""
    NETWORK_ERROR = "network"
    API_ERROR = "api"
    DATABASE_ERROR = "database"
    PROCESSING_ERROR = "processing"
    UNKNOWN_ERROR = "unknown"


class ActionType(Enum):
    """错误处理动作类型"""
    RETRY = "retry"
    SKIP = "skip"
    STOP = "stop"


@dataclass
class ErrorStats:
    """错误统计信息"""
    error_type: ErrorType
    count: int
    first_occurrence: float
    last_occurrence: float
    consecutive_failures: int


class CircuitBreakerError(Exception):
    """熔断器错误"""
    pass


class CircuitBreaker:
    """熔断器实现"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        """
        初始化熔断器
        
        Args:
            failure_threshold: 失败阈值
            recovery_timeout: 恢复超时时间（秒）
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'closed'  # closed, open, half_open
        
    async def call(self, func: Callable, *args, **kwargs):
        """通过熔断器调用函数"""
        if self.state == 'open':
            # 检查是否可以尝试恢复
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half_open'
            else:
                raise CircuitBreakerError("熔断器开启状态，拒绝调用")
        
        try:
            result = await func(*args, **kwargs)
            
            # 成功时重置计数器
            if self.state == 'half_open':
                self.state = 'closed'
                self.failure_count = 0
                
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'open'
                
            raise e


class ErrorHandler:
    """统一错误处理器"""
    
    def __init__(self, config: Dict[str, Any]):
        """初始化错误处理器"""
        self.config = config
        self.logger = logging.getLogger('monitor.error_handler')
        
        # 错误配置
        error_config = config.get('monitoring', {}).get('error_handling', {})
        self.api_error_action = ActionType(error_config.get('on_api_error', 'retry'))
        self.download_error_action = ActionType(error_config.get('on_download_error', 'skip'))
        self.process_error_action = ActionType(error_config.get('on_process_error', 'skip'))
        self.error_threshold = error_config.get('error_threshold', 10)
        
        # 错误统计
        self.error_stats: Dict[str, ErrorStats] = {}
        self.total_errors = 0
        
        # 熔断器
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        self.logger.info("错误处理器初始化完成")
    
    def classify_error(self, exception: Exception) -> ErrorType:
        """错误分类"""
        if isinstance(exception, (aiohttp.ClientError, aiohttp.ServerTimeoutError)):
            return ErrorType.NETWORK_ERROR
        elif isinstance(exception, aiohttp.ClientResponseError):
            return ErrorType.API_ERROR
        elif 'database' in str(exception).lower() or 'mysql' in str(exception).lower():
            return ErrorType.DATABASE_ERROR
        elif 'process' in str(exception).lower() or 'parse' in str(exception).lower():
            return ErrorType.PROCESSING_ERROR
        else:
            return ErrorType.UNKNOWN_ERROR
    
    def get_action_for_error(self, error_type: ErrorType, context: str = "") -> ActionType:
        """根据错误类型获取处理动作"""
        if error_type == ErrorType.API_ERROR:
            return self.api_error_action
        elif error_type == ErrorType.NETWORK_ERROR:
            return ActionType.RETRY  # 网络错误默认重试
        elif error_type == ErrorType.PROCESSING_ERROR:
            return self.process_error_action
        elif context == 'download':
            return self.download_error_action
        else:
            return ActionType.SKIP
    
    def record_error(self, exception: Exception, context: str = ""):
        """记录错误"""
        error_type = self.classify_error(exception)
        key = f"{context}_{error_type.value}" if context else error_type.value
        
        current_time = time.time()
        
        if key in self.error_stats:
            stats = self.error_stats[key]
            stats.count += 1
            stats.last_occurrence = current_time
            stats.consecutive_failures += 1
        else:
            self.error_stats[key] = ErrorStats(
                error_type=error_type,
                count=1,
                first_occurrence=current_time,
                last_occurrence=current_time,
                consecutive_failures=1
            )
        
        self.total_errors += 1
        
        self.logger.warning(
            f"错误记录 [{context}]: {error_type.value} - {exception}"
        )
    
    def reset_error_stats(self, context: str = ""):
        """重置错误统计（成功时调用）"""
        if context:
            keys_to_reset = [k for k in self.error_stats.keys() if k.startswith(context)]
            for key in keys_to_reset:
                self.error_stats[key].consecutive_failures = 0
        else:
            for stats in self.error_stats.values():
                stats.consecutive_failures = 0
    
    def should_stop_service(self) -> bool:
        """检查是否应该停止服务"""
        return self.total_errors >= self.error_threshold
    
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """获取或创建熔断器"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker()
        return self.circuit_breakers[name]
    
    async def handle_with_retry(self, func: Callable, context: str = "", 
                               max_attempts: int = 3, *args, **kwargs):
        """带重试的错误处理"""
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            retry=retry_if_exception_type((
                aiohttp.ClientError,
                aiohttp.ServerTimeoutError,
                ConnectionError,
                TimeoutError
            )),
            before_sleep=before_sleep_log(self.logger, logging.WARNING)
        )
        async def _execute():
            try:
                result = await func(*args, **kwargs)
                self.reset_error_stats(context)
                return result
            except Exception as e:
                self.record_error(e, context)
                error_type = self.classify_error(e)
                action = self.get_action_for_error(error_type, context)
                
                if action == ActionType.STOP:
                    self.logger.critical(f"严重错误，停止操作: {e}")
                    raise
                elif action == ActionType.SKIP:
                    self.logger.warning(f"跳过错误: {e}")
                    return None
                else:  # RETRY
                    self.logger.info(f"重试操作: {e}")
                    raise
        
        try:
            return await _execute()
        except RetryError as e:
            self.logger.error(f"重试失败，达到最大次数: {e}")
            return None
    
    async def safe_execute(self, func: Callable, context: str = "", 
                          use_circuit_breaker: bool = False, *args, **kwargs):
        """安全执行函数"""
        if use_circuit_breaker:
            breaker = self.get_circuit_breaker(context)
            try:
                return await breaker.call(func, *args, **kwargs)
            except CircuitBreakerError as e:
                self.logger.warning(f"熔断器阻止执行 [{context}]: {e}")
                return None
        else:
            try:
                result = await func(*args, **kwargs)
                self.reset_error_stats(context)
                return result
            except Exception as e:
                self.record_error(e, context)
                error_type = self.classify_error(e)
                action = self.get_action_for_error(error_type, context)
                
                if action == ActionType.STOP:
                    raise
                else:
                    self.logger.warning(f"忽略错误 [{context}]: {e}")
                    return None
    
    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误摘要"""
        summary = {
            'total_errors': self.total_errors,
            'error_types': {},
            'consecutive_failures_by_context': {},
            'circuit_breaker_states': {}
        }
        
        # 按错误类型统计
        for key, stats in self.error_stats.items():
            error_type = stats.error_type.value
            if error_type not in summary['error_types']:
                summary['error_types'][error_type] = 0
            summary['error_types'][error_type] += stats.count
            
            # 连续失败统计
            if stats.consecutive_failures > 0:
                summary['consecutive_failures_by_context'][key] = stats.consecutive_failures
        
        # 熔断器状态
        for name, breaker in self.circuit_breakers.items():
            summary['circuit_breaker_states'][name] = {
                'state': breaker.state,
                'failure_count': breaker.failure_count
            }
        
        return summary
    
    def cleanup(self):
        """清理资源"""
        self.logger.info(f"错误处理器清理，总错误数: {self.total_errors}")


# 装饰器函数
def with_error_handling(handler: ErrorHandler, context: str = "", max_attempts: int = 3):
    """错误处理装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await handler.handle_with_retry(
                func, context, max_attempts, *args, **kwargs
            )
        return wrapper
    return decorator


def with_circuit_breaker(handler: ErrorHandler, context: str):
    """熔断器装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await handler.safe_execute(
                func, context, use_circuit_breaker=True, *args, **kwargs
            )
        return wrapper
    return decorator