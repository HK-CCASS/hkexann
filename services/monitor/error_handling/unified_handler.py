"""
统一错误处理框架

这个模块提供了全系统统一的错误处理机制，包括错误分类、智能重试、
熔断保护和优雅降级等功能。确保系统在各种异常情况下都能稳定运行。

主要功能：
- 智能错误分类和处理策略
- 指数退避重试机制
- 熔断器保护和服务降级
- 错误上报和监控集成

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import traceback
from typing import Dict, Any, Optional, List, Callable, Union, Type, Tuple
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from datetime import datetime, timedelta
import random
import json
from pathlib import Path

# 配置日志
# 配置日志（如果没有已配置的handler）
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """错误分类枚举"""
    NETWORK = "network"                 # 网络连接错误
    API_LIMIT = "api_limit"            # API限流错误
    API_AUTH = "api_auth"              # API认证错误
    DATA_FORMAT = "data_format"        # 数据格式错误
    DATABASE = "database"              # 数据库错误
    FILE_IO = "file_io"               # 文件IO错误
    SYSTEM = "system"                  # 系统错误
    VALIDATION = "validation"          # 数据验证错误
    BUSINESS = "business"              # 业务逻辑错误
    UNKNOWN = "unknown"                # 未知错误


class ErrorSeverity(IntEnum):
    """错误严重程度（数值越小越严重）"""
    CRITICAL = 1    # 严重错误，影响核心功能
    HIGH = 2        # 高级错误，影响重要功能
    MEDIUM = 3      # 中级错误，影响部分功能
    LOW = 4         # 低级错误，轻微影响
    INFO = 5        # 信息性错误，不影响功能


class RetryStrategy(Enum):
    """重试策略枚举"""
    NONE = "none"                      # 不重试
    FIXED_DELAY = "fixed_delay"        # 固定延迟重试
    EXPONENTIAL_BACKOFF = "exponential_backoff"  # 指数退避重试
    LINEAR_BACKOFF = "linear_backoff"  # 线性退避重试
    ADAPTIVE = "adaptive"              # 自适应重试


@dataclass
class ErrorConfig:
    """错误处理配置"""
    max_retries: int = 3
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0            # 基础延迟时间（秒）
    max_delay: float = 60.0            # 最大延迟时间（秒）
    backoff_factor: float = 2.0        # 退避因子
    jitter: bool = True                # 是否添加随机抖动
    circuit_breaker_threshold: int = 5 # 熔断器阈值
    circuit_breaker_timeout: int = 60  # 熔断器恢复时间（秒）
    enable_fallback: bool = True       # 是否启用降级
    alert_threshold: int = 10          # 告警阈值


@dataclass
class ErrorRecord:
    """错误记录"""
    timestamp: datetime = field(default_factory=datetime.now)
    error_type: str = ""
    error_message: str = ""
    error_category: ErrorCategory = ErrorCategory.UNKNOWN
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    context: Dict[str, Any] = field(default_factory=dict)
    stack_trace: str = ""
    retry_count: int = 0
    resolved: bool = False
    resolution_time: Optional[datetime] = None


@dataclass
class CircuitBreakerState:
    """熔断器状态"""
    state: str = "CLOSED"              # CLOSED, OPEN, HALF_OPEN
    failure_count: int = 0
    last_failure_time: float = 0
    success_count: int = 0
    reset_timeout: int = 60


class UnifiedErrorHandler:
    """
    统一错误处理器
    
    提供全系统统一的错误处理机制，包括：
    1. 智能错误分类
    2. 自适应重试策略
    3. 熔断器保护
    4. 服务降级
    5. 错误监控和告警
    """
    
    def __init__(self, config: Dict[ErrorCategory, ErrorConfig] = None):
        """
        初始化统一错误处理器
        
        Args:
            config: 错误处理配置
        """
        self.config = config or self._get_default_config()
        
        # 错误记录
        self.error_history: List[ErrorRecord] = []
        self.error_stats: Dict[ErrorCategory, Dict[str, Any]] = {}
        
        # 熔断器状态
        self.circuit_breakers: Dict[str, CircuitBreakerState] = {}
        
        # 降级服务注册
        self.fallback_handlers: Dict[str, Callable] = {}
        
        # 错误分类器
        self.error_classifiers = self._initialize_classifiers()
        
        # 初始化统计
        self._initialize_stats()
        
        logger.info("🛡️ 统一错误处理器初始化完成")

    def _get_default_config(self) -> Dict[ErrorCategory, ErrorConfig]:
        """获取默认错误处理配置"""
        return {
            ErrorCategory.NETWORK: ErrorConfig(
                max_retries=3,
                retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
                base_delay=1.0,
                circuit_breaker_threshold=5
            ),
            ErrorCategory.API_LIMIT: ErrorConfig(
                max_retries=5,
                retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
                base_delay=5.0,
                max_delay=300.0,
                circuit_breaker_threshold=3
            ),
            ErrorCategory.API_AUTH: ErrorConfig(
                max_retries=1,
                retry_strategy=RetryStrategy.NONE,
                circuit_breaker_threshold=2,
                enable_fallback=False
            ),
            ErrorCategory.DATA_FORMAT: ErrorConfig(
                max_retries=2,
                retry_strategy=RetryStrategy.FIXED_DELAY,
                base_delay=0.5,
                circuit_breaker_threshold=10
            ),
            ErrorCategory.DATABASE: ErrorConfig(
                max_retries=3,
                retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
                base_delay=2.0,
                circuit_breaker_threshold=5
            ),
            ErrorCategory.FILE_IO: ErrorConfig(
                max_retries=3,
                retry_strategy=RetryStrategy.LINEAR_BACKOFF,
                base_delay=1.0,
                circuit_breaker_threshold=5
            ),
            ErrorCategory.SYSTEM: ErrorConfig(
                max_retries=2,
                retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
                base_delay=3.0,
                circuit_breaker_threshold=3
            ),
            ErrorCategory.VALIDATION: ErrorConfig(
                max_retries=1,
                retry_strategy=RetryStrategy.NONE,
                enable_fallback=True
            ),
            ErrorCategory.BUSINESS: ErrorConfig(
                max_retries=2,
                retry_strategy=RetryStrategy.FIXED_DELAY,
                base_delay=1.0,
                enable_fallback=True
            ),
            ErrorCategory.UNKNOWN: ErrorConfig(
                max_retries=1,
                retry_strategy=RetryStrategy.FIXED_DELAY,
                base_delay=1.0
            )
        }

    def _initialize_classifiers(self) -> Dict[ErrorCategory, List[Callable]]:
        """初始化错误分类器"""
        return {
            ErrorCategory.NETWORK: [
                lambda e: isinstance(e, (ConnectionError, TimeoutError)),
                lambda e: "connection" in str(e).lower(),
                lambda e: "timeout" in str(e).lower(),
                lambda e: "network" in str(e).lower(),
            ],
            ErrorCategory.API_LIMIT: [
                lambda e: "rate limit" in str(e).lower(),
                lambda e: "too many requests" in str(e).lower(),
                lambda e: "429" in str(e),
                lambda e: "quota exceeded" in str(e).lower(),
            ],
            ErrorCategory.API_AUTH: [
                lambda e: "unauthorized" in str(e).lower(),
                lambda e: "authentication" in str(e).lower(),
                lambda e: "401" in str(e),
                lambda e: "403" in str(e),
                lambda e: "invalid token" in str(e).lower(),
            ],
            ErrorCategory.DATA_FORMAT: [
                lambda e: isinstance(e, (ValueError, TypeError, json.JSONDecodeError)),
                lambda e: "json" in str(e).lower(),
                lambda e: "format" in str(e).lower(),
                lambda e: "parse" in str(e).lower(),
            ],
            ErrorCategory.DATABASE: [
                lambda e: "database" in str(e).lower(),
                lambda e: "sql" in str(e).lower(),
                lambda e: "connection" in str(e).lower() and "db" in str(e).lower(),
                lambda e: "mysql" in str(e).lower(),
                lambda e: "clickhouse" in str(e).lower(),
            ],
            ErrorCategory.FILE_IO: [
                lambda e: isinstance(e, (FileNotFoundError, PermissionError, OSError)),
                lambda e: "file" in str(e).lower(),
                lambda e: "directory" in str(e).lower(),
                lambda e: "permission" in str(e).lower(),
            ],
            ErrorCategory.VALIDATION: [
                lambda e: "validation" in str(e).lower(),
                lambda e: "invalid" in str(e).lower(),
                lambda e: "required" in str(e).lower(),
            ]
        }

    def _initialize_stats(self):
        """初始化错误统计"""
        for category in ErrorCategory:
            self.error_stats[category] = {
                'total_count': 0,
                'retry_count': 0,
                'success_after_retry': 0,
                'fallback_used': 0,
                'last_occurrence': None,
                'avg_resolution_time': 0.0
            }

    def classify_error(self, error: Exception, context: Dict[str, Any] = None) -> Tuple[ErrorCategory, ErrorSeverity]:
        """
        分类错误
        
        Args:
            error: 异常对象
            context: 错误上下文
            
        Returns:
            Tuple[ErrorCategory, ErrorSeverity]: 错误分类和严重程度
        """
        context = context or {}
        
        # 尝试各种分类器
        for category, classifiers in self.error_classifiers.items():
            for classifier in classifiers:
                try:
                    if classifier(error):
                        severity = self._determine_severity(error, category, context)
                        return category, severity
                except Exception:
                    continue
        
        # 默认分类
        severity = self._determine_severity(error, ErrorCategory.UNKNOWN, context)
        return ErrorCategory.UNKNOWN, severity

    def _determine_severity(self, error: Exception, category: ErrorCategory, 
                          context: Dict[str, Any]) -> ErrorSeverity:
        """确定错误严重程度"""
        # 基于错误类型的基础严重程度
        base_severity = {
            ErrorCategory.NETWORK: ErrorSeverity.MEDIUM,
            ErrorCategory.API_LIMIT: ErrorSeverity.MEDIUM,
            ErrorCategory.API_AUTH: ErrorSeverity.HIGH,
            ErrorCategory.DATA_FORMAT: ErrorSeverity.LOW,
            ErrorCategory.DATABASE: ErrorSeverity.HIGH,
            ErrorCategory.FILE_IO: ErrorSeverity.MEDIUM,
            ErrorCategory.SYSTEM: ErrorSeverity.CRITICAL,
            ErrorCategory.VALIDATION: ErrorSeverity.LOW,
            ErrorCategory.BUSINESS: ErrorSeverity.MEDIUM,
            ErrorCategory.UNKNOWN: ErrorSeverity.MEDIUM
        }.get(category, ErrorSeverity.MEDIUM)
        
        # 根据上下文调整严重程度
        if context.get('is_critical_path', False):
            base_severity = ErrorSeverity(max(1, base_severity - 1))
        
        if context.get('retry_count', 0) > 2:
            base_severity = ErrorSeverity(max(1, base_severity - 1))
        
        return base_severity

    async def handle_error(self, error: Exception, 
                          operation_name: str,
                          operation_func: Callable = None,
                          context: Dict[str, Any] = None,
                          *args, **kwargs) -> Any:
        """
        处理错误
        
        Args:
            error: 异常对象
            operation_name: 操作名称
            operation_func: 操作函数（用于重试）
            context: 错误上下文
            *args, **kwargs: 操作函数参数
            
        Returns:
            操作结果或降级结果
        """
        context = context or {}
        
        # 分类错误
        category, severity = self.classify_error(error, context)
        
        # 创建错误记录
        error_record = ErrorRecord(
            error_type=type(error).__name__,
            error_message=str(error),
            error_category=category,
            severity=severity,
            context=context,
            stack_trace=traceback.format_exc(),
            retry_count=context.get('retry_count', 0)
        )
        
        # 记录错误
        self._record_error(error_record)
        
        # 检查熔断器
        if self._is_circuit_breaker_open(operation_name):
            logger.warning(f"熔断器开启，跳过操作: {operation_name}")
            return await self._execute_fallback(operation_name, context, *args, **kwargs)
        
        # 决定是否重试
        config = self.config.get(category, self.config[ErrorCategory.UNKNOWN])
        retry_count = context.get('retry_count', 0)
        
        if retry_count < config.max_retries and operation_func and self._should_retry(error, category):
            # 执行重试
            return await self._execute_retry(
                operation_func, operation_name, category, 
                retry_count, context, *args, **kwargs
            )
        else:
            # 更新熔断器
            self._update_circuit_breaker(operation_name, False)
            
            # 执行降级
            if config.enable_fallback:
                return await self._execute_fallback(operation_name, context, *args, **kwargs)
            else:
                # 重新抛出异常
                raise error

    def _record_error(self, error_record: ErrorRecord):
        """记录错误"""
        self.error_history.append(error_record)
        
        # 更新统计
        stats = self.error_stats[error_record.error_category]
        stats['total_count'] += 1
        stats['last_occurrence'] = error_record.timestamp
        
        # 保持历史记录在合理范围内
        if len(self.error_history) > 10000:
            self.error_history = self.error_history[-5000:]
        
        # 检查是否需要告警
        self._check_alert_conditions(error_record)

    def _should_retry(self, error: Exception, category: ErrorCategory) -> bool:
        """判断是否应该重试"""
        # 某些错误类型不应该重试
        no_retry_conditions = [
            category == ErrorCategory.API_AUTH,
            category == ErrorCategory.VALIDATION,
            "unauthorized" in str(error).lower(),
            "forbidden" in str(error).lower(),
        ]
        
        return not any(no_retry_conditions)

    async def _execute_retry(self, operation_func: Callable, 
                           operation_name: str,
                           category: ErrorCategory,
                           retry_count: int,
                           context: Dict[str, Any],
                           *args, **kwargs) -> Any:
        """执行重试"""
        config = self.config[category]
        
        # 计算延迟时间
        delay = self._calculate_retry_delay(config, retry_count)
        
        logger.warning(f"重试操作 {operation_name} (第{retry_count+1}次), 延迟{delay:.2f}秒")
        
        # 延迟
        await asyncio.sleep(delay)
        
        # 更新上下文
        new_context = context.copy()
        new_context['retry_count'] = retry_count + 1
        
        try:
            # 执行操作
            if asyncio.iscoroutinefunction(operation_func):
                result = await operation_func(*args, **kwargs)
            else:
                result = operation_func(*args, **kwargs)
            
            # 重试成功，更新统计
            self.error_stats[category]['success_after_retry'] += 1
            self._update_circuit_breaker(operation_name, True)
            
            logger.info(f"重试成功: {operation_name}")
            return result
            
        except Exception as retry_error:
            # 重试失败，递归处理
            return await self.handle_error(
                retry_error, operation_name, operation_func,
                new_context, *args, **kwargs
            )

    def _calculate_retry_delay(self, config: ErrorConfig, retry_count: int) -> float:
        """计算重试延迟时间"""
        if config.retry_strategy == RetryStrategy.FIXED_DELAY:
            delay = config.base_delay
        elif config.retry_strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = config.base_delay * (retry_count + 1)
        elif config.retry_strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = config.base_delay * (config.backoff_factor ** retry_count)
        else:
            delay = config.base_delay
        
        # 限制最大延迟
        delay = min(delay, config.max_delay)
        
        # 添加随机抖动
        if config.jitter:
            jitter = random.uniform(0, delay * 0.1)
            delay += jitter
        
        return delay

    def _is_circuit_breaker_open(self, operation_name: str) -> bool:
        """检查熔断器是否开启"""
        if operation_name not in self.circuit_breakers:
            return False
        
        breaker = self.circuit_breakers[operation_name]
        current_time = time.time()
        
        if breaker.state == "OPEN":
            if current_time - breaker.last_failure_time > breaker.reset_timeout:
                breaker.state = "HALF_OPEN"
                breaker.success_count = 0
                logger.info(f"熔断器进入半开状态: {operation_name}")
            else:
                return True
        
        return False

    def _update_circuit_breaker(self, operation_name: str, success: bool):
        """更新熔断器状态"""
        if operation_name not in self.circuit_breakers:
            self.circuit_breakers[operation_name] = CircuitBreakerState()
        
        breaker = self.circuit_breakers[operation_name]
        
        if success:
            breaker.failure_count = 0
            if breaker.state == "HALF_OPEN":
                breaker.success_count += 1
                if breaker.success_count >= 3:  # 连续成功3次
                    breaker.state = "CLOSED"
                    logger.info(f"熔断器关闭: {operation_name}")
        else:
            breaker.failure_count += 1
            breaker.last_failure_time = time.time()
            
            # 检查是否需要开启熔断器
            threshold = 5  # 默认阈值
            if breaker.failure_count >= threshold:
                breaker.state = "OPEN"
                logger.warning(f"熔断器开启: {operation_name}")

    async def _execute_fallback(self, operation_name: str, 
                              context: Dict[str, Any],
                              *args, **kwargs) -> Any:
        """执行降级操作"""
        if operation_name in self.fallback_handlers:
            try:
                fallback_func = self.fallback_handlers[operation_name]
                
                logger.info(f"执行降级操作: {operation_name}")
                
                if asyncio.iscoroutinefunction(fallback_func):
                    result = await fallback_func(context, *args, **kwargs)
                else:
                    result = fallback_func(context, *args, **kwargs)
                
                # 更新统计
                category = context.get('error_category', ErrorCategory.UNKNOWN)
                self.error_stats[category]['fallback_used'] += 1
                
                return result
                
            except Exception as fallback_error:
                logger.error(f"降级操作失败 {operation_name}: {fallback_error}")
                return None
        else:
            logger.warning(f"未找到降级处理器: {operation_name}")
            return None

    def register_fallback(self, operation_name: str, fallback_func: Callable):
        """
        注册降级处理器
        
        Args:
            operation_name: 操作名称
            fallback_func: 降级处理函数
        """
        self.fallback_handlers[operation_name] = fallback_func
        logger.info(f"注册降级处理器: {operation_name}")

    def _check_alert_conditions(self, error_record: ErrorRecord):
        """检查告警条件"""
        # 严重错误立即告警
        if error_record.severity <= ErrorSeverity.HIGH:
            self._send_alert(error_record, "严重错误")
        
        # 检查错误频率
        category_stats = self.error_stats[error_record.error_category]
        if category_stats['total_count'] % 10 == 0:  # 每10个错误告警一次
            self._send_alert(error_record, f"{error_record.error_category.value}错误频发")

    def _send_alert(self, error_record: ErrorRecord, alert_type: str):
        """发送告警"""
        alert_message = {
            'alert_type': alert_type,
            'error_category': error_record.error_category.value,
            'error_message': error_record.error_message,
            'severity': error_record.severity.name,
            'timestamp': error_record.timestamp.isoformat(),
            'context': error_record.context
        }
        
        # 这里可以集成实际的告警系统
        logger.warning(f"🚨 错误告警: {alert_type} - {error_record.error_message}")

    def get_error_statistics(self) -> Dict[str, Any]:
        """
        获取错误统计信息
        
        Returns:
            Dict[str, Any]: 错误统计信息
        """
        total_errors = len(self.error_history)
        
        # 按分类统计
        category_stats = {}
        for category, stats in self.error_stats.items():
            if stats['total_count'] > 0:
                category_stats[category.value] = {
                    'count': stats['total_count'],
                    'retry_success_rate': (
                        stats['success_after_retry'] / stats['retry_count'] * 100
                        if stats['retry_count'] > 0 else 0
                    ),
                    'fallback_usage': stats['fallback_used'],
                    'last_occurrence': stats['last_occurrence'].isoformat() if stats['last_occurrence'] else None
                }
        
        # 熔断器状态
        circuit_breaker_stats = {}
        for name, breaker in self.circuit_breakers.items():
            circuit_breaker_stats[name] = {
                'state': breaker.state,
                'failure_count': breaker.failure_count,
                'success_count': breaker.success_count
            }
        
        return {
            'summary': {
                'total_errors': total_errors,
                'active_circuit_breakers': len([b for b in self.circuit_breakers.values() if b.state != "CLOSED"]),
                'fallback_handlers': len(self.fallback_handlers)
            },
            'by_category': category_stats,
            'circuit_breakers': circuit_breaker_stats,
            'recent_errors': [
                {
                    'category': record.error_category.value,
                    'message': record.error_message[:100],
                    'severity': record.severity.name,
                    'timestamp': record.timestamp.isoformat()
                }
                for record in self.error_history[-10:]  # 最近10个错误
            ]
        }


# 全局错误处理器实例
_global_error_handler: Optional[UnifiedErrorHandler] = None


def get_error_handler() -> UnifiedErrorHandler:
    """
    获取全局错误处理器实例
    
    Returns:
        UnifiedErrorHandler: 错误处理器实例
    """
    global _global_error_handler
    if _global_error_handler is None:
        _global_error_handler = UnifiedErrorHandler()
    return _global_error_handler


# 装饰器
def error_handler(operation_name: str = None, 
                 fallback_func: Callable = None,
                 context: Dict[str, Any] = None):
    """
    错误处理装饰器
    
    Args:
        operation_name: 操作名称
        fallback_func: 降级函数
        context: 错误上下文
        
    Usage:
        @error_handler("api_call", fallback_func=default_response)
        async def call_api():
            return await some_api_call()
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            handler = get_error_handler()
            op_name = operation_name or func.__name__
            
            # 注册降级处理器
            if fallback_func:
                handler.register_fallback(op_name, fallback_func)
            
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except Exception as e:
                return await handler.handle_error(
                    e, op_name, func, context, *args, **kwargs
                )
        
        return wrapper
    return decorator


if __name__ == "__main__":
    # 测试模块
    async def test_error_handler():
        """测试统一错误处理器"""
        
        print("\n" + "="*70)
        print("🛡️ 统一错误处理器测试")
        print("="*70)
        
        # 创建错误处理器
        handler = UnifiedErrorHandler()
        
        # 模拟降级函数
        def fallback_api_call(context, *args, **kwargs):
            return {"status": "fallback", "data": "默认数据"}
        
        # 注册降级处理器
        handler.register_fallback("test_api", fallback_api_call)
        
        # 模拟各种错误
        print("\n🔍 测试错误分类...")
        
        errors_to_test = [
            (ConnectionError("网络连接失败"), "网络错误"),
            (ValueError("JSON解析失败"), "数据格式错误"),
            (Exception("Rate limit exceeded"), "API限流错误"),
            (FileNotFoundError("文件未找到"), "文件IO错误"),
        ]
        
        for error, description in errors_to_test:
            category, severity = handler.classify_error(error)
            print(f"  {description}: {category.value} ({severity.name})")
        
        # 测试重试机制
        print(f"\n🔄 测试重试机制...")
        
        retry_count = 0
        
        async def failing_operation():
            nonlocal retry_count
            retry_count += 1
            if retry_count < 3:
                raise ConnectionError(f"连接失败 (尝试 {retry_count})")
            return "操作成功"
        
        try:
            result = await handler.handle_error(
                ConnectionError("初始连接失败"),
                "test_retry",
                failing_operation,
                {"is_critical_path": True}
            )
            print(f"✅ 重试成功: {result}, 总尝试次数: {retry_count}")
        except Exception as e:
            print(f"❌ 重试失败: {e}")
        
        # 测试熔断器
        print(f"\n⚡ 测试熔断器...")
        
        # 连续失败触发熔断
        for i in range(6):
            try:
                await handler.handle_error(
                    Exception(f"服务不可用 {i+1}"),
                    "test_circuit_breaker",
                    None,  # 不提供重试函数
                    {"test": True}
                )
            except Exception:
                pass
        
        # 测试降级
        print(f"\n🛟 测试服务降级...")
        
        fallback_result = await handler.handle_error(
            Exception("服务完全不可用"),
            "test_api",
            None,
            {"need_fallback": True}
        )
        
        print(f"降级结果: {fallback_result}")
        
        # 显示统计信息
        print(f"\n📊 错误处理统计:")
        stats = handler.get_error_statistics()
        
        print(f"  总错误数: {stats['summary']['total_errors']}")
        print(f"  活跃熔断器: {stats['summary']['active_circuit_breakers']}")
        print(f"  降级处理器: {stats['summary']['fallback_handlers']}")
        
        if stats['by_category']:
            print(f"  按分类统计:")
            for category, category_stats in stats['by_category'].items():
                print(f"    {category}: {category_stats['count']} 次")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_error_handler())
