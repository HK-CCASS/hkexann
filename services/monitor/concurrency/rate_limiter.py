"""
API限流和资源竞争避免机制

这个模块实现了智能的API限流策略和资源竞争避免机制，
确保系统在高并发场景下能够稳定运行，避免API限制和资源冲突。

主要功能：
- 自适应API限流控制
- 资源竞争检测和避免
- API降级和熔断机制
- 请求优先级和智能调度

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
import random
from typing import Dict, Any, Optional, List, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import statistics
from collections import deque, defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIType(Enum):
    """API类型枚举"""
    HKEX_MONITORING = "hkex_monitoring"     # HKEX监听API
    HKEX_DOWNLOAD = "hkex_download"         # HKEX下载API
    SILICONFLOW = "siliconflow"             # SiliconFlow API
    MILVUS = "milvus"                       # Milvus API
    CLICKHOUSE = "clickhouse"               # ClickHouse API


class LimitingStrategy(Enum):
    """限流策略枚举"""
    TOKEN_BUCKET = "token_bucket"           # 令牌桶算法
    SLIDING_WINDOW = "sliding_window"       # 滑动窗口算法
    ADAPTIVE = "adaptive"                   # 自适应算法
    CIRCUIT_BREAKER = "circuit_breaker"     # 熔断器


@dataclass
class APICall:
    """API调用记录"""
    timestamp: float
    api_type: APIType
    success: bool
    response_time: float
    error_type: Optional[str] = None
    retry_count: int = 0


@dataclass
class LimitingConfig:
    """限流配置"""
    base_rate_limit: int = 60               # 基础速率限制（每分钟）
    burst_capacity: int = 10               # 突发容量
    window_size_seconds: int = 60          # 窗口大小（秒）
    max_retries: int = 3                   # 最大重试次数
    backoff_factor: float = 2.0            # 退避因子
    circuit_breaker_threshold: int = 5     # 熔断器阈值
    circuit_breaker_timeout: int = 60      # 熔断器超时（秒）
    adaptive_factor: float = 0.1           # 自适应因子


class IntelligentRateLimiter:
    """
    智能API限流器
    
    实现多种限流策略的组合，根据API响应情况自动调整限流参数，
    避免API限制同时最大化吞吐量。
    """
    
    def __init__(self, config: Dict[APIType, LimitingConfig] = None):
        """
        初始化智能限流器
        
        Args:
            config: API限流配置
        """
        self.config = config or self._get_default_config()
        
        # 令牌桶
        self.token_buckets: Dict[APIType, Dict[str, Any]] = {}
        
        # 滑动窗口
        self.sliding_windows: Dict[APIType, deque] = {}
        
        # 熔断器状态
        self.circuit_breakers: Dict[APIType, Dict[str, Any]] = {}
        
        # API调用历史
        self.call_history: Dict[APIType, deque] = {}
        
        # 自适应参数
        self.adaptive_params: Dict[APIType, Dict[str, Any]] = {}
        
        # 初始化各API的限流器
        self._initialize_limiters()
        
        logger.info("🚦 智能API限流器初始化完成")

    def _get_default_config(self) -> Dict[APIType, LimitingConfig]:
        """获取默认限流配置"""
        return {
            APIType.HKEX_MONITORING: LimitingConfig(
                base_rate_limit=30,  # 每分钟30次
                burst_capacity=5,
                window_size_seconds=60,
                max_retries=3,
                circuit_breaker_threshold=3
            ),
            APIType.HKEX_DOWNLOAD: LimitingConfig(
                base_rate_limit=20,  # 每分钟20次
                burst_capacity=3,
                window_size_seconds=60,
                max_retries=3,
                circuit_breaker_threshold=5
            ),
            APIType.SILICONFLOW: LimitingConfig(
                base_rate_limit=100,  # 每分钟100次
                burst_capacity=20,
                window_size_seconds=60,
                max_retries=3,
                circuit_breaker_threshold=10
            ),
            APIType.MILVUS: LimitingConfig(
                base_rate_limit=200,  # 每分钟200次
                burst_capacity=50,
                window_size_seconds=60,
                max_retries=2,
                circuit_breaker_threshold=15
            ),
            APIType.CLICKHOUSE: LimitingConfig(
                base_rate_limit=150,  # 每分钟150次
                burst_capacity=30,
                window_size_seconds=60,
                max_retries=3,
                circuit_breaker_threshold=10
            )
        }

    def _initialize_limiters(self):
        """初始化各API的限流器"""
        for api_type, config in self.config.items():
            # 初始化令牌桶
            self.token_buckets[api_type] = {
                'tokens': config.burst_capacity,
                'max_tokens': config.burst_capacity,
                'refill_rate': config.base_rate_limit / 60.0,  # 每秒补充速率
                'last_refill': time.time()
            }
            
            # 初始化滑动窗口
            self.sliding_windows[api_type] = deque(maxlen=config.base_rate_limit * 2)
            
            # 初始化熔断器
            self.circuit_breakers[api_type] = {
                'state': 'CLOSED',  # CLOSED, OPEN, HALF_OPEN
                'failure_count': 0,
                'last_failure_time': 0,
                'success_count': 0
            }
            
            # 初始化调用历史
            self.call_history[api_type] = deque(maxlen=1000)
            
            # 初始化自适应参数
            self.adaptive_params[api_type] = {
                'current_rate_limit': config.base_rate_limit,
                'success_rate': 1.0,
                'avg_response_time': 0.0,
                'last_adjustment': time.time()
            }

    async def acquire_permission(self, api_type: APIType, 
                                priority: int = 5,
                                max_wait_time: float = 30.0) -> bool:
        """
        获取API调用许可
        
        Args:
            api_type: API类型
            priority: 优先级（1-10，数值越小优先级越高）
            max_wait_time: 最大等待时间
            
        Returns:
            bool: 是否获得调用许可
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            # 检查熔断器状态
            if not self._check_circuit_breaker(api_type):
                await asyncio.sleep(1.0)
                continue
            
            # 检查令牌桶
            if self._consume_token(api_type):
                # 检查滑动窗口
                if self._check_sliding_window(api_type):
                    return True
                else:
                    # 归还令牌
                    self._return_token(api_type)
            
            # 根据优先级调整等待时间
            wait_time = max(0.1, 1.0 / priority)
            await asyncio.sleep(wait_time)
        
        logger.warning(f"获取API许可超时: {api_type.value}")
        return False

    def _consume_token(self, api_type: APIType) -> bool:
        """消耗令牌"""
        bucket = self.token_buckets[api_type]
        current_time = time.time()
        
        # 补充令牌
        time_passed = current_time - bucket['last_refill']
        tokens_to_add = time_passed * bucket['refill_rate']
        bucket['tokens'] = min(bucket['max_tokens'], bucket['tokens'] + tokens_to_add)
        bucket['last_refill'] = current_time
        
        # 消耗令牌
        if bucket['tokens'] >= 1.0:
            bucket['tokens'] -= 1.0
            return True
        
        return False

    def _return_token(self, api_type: APIType):
        """归还令牌"""
        bucket = self.token_buckets[api_type]
        bucket['tokens'] = min(bucket['max_tokens'], bucket['tokens'] + 1.0)

    def _check_sliding_window(self, api_type: APIType) -> bool:
        """检查滑动窗口"""
        current_time = time.time()
        window = self.sliding_windows[api_type]
        config = self.config[api_type]
        
        # 移除过期记录
        while window and current_time - window[0] > config.window_size_seconds:
            window.popleft()
        
        # 检查是否超过限制
        current_rate = self.adaptive_params[api_type]['current_rate_limit']
        if len(window) < current_rate:
            window.append(current_time)
            return True
        
        return False

    def _check_circuit_breaker(self, api_type: APIType) -> bool:
        """检查熔断器状态"""
        breaker = self.circuit_breakers[api_type]
        current_time = time.time()
        config = self.config[api_type]
        
        if breaker['state'] == 'OPEN':
            # 检查是否可以进入半开状态
            if current_time - breaker['last_failure_time'] > config.circuit_breaker_timeout:
                breaker['state'] = 'HALF_OPEN'
                breaker['success_count'] = 0
                logger.info(f"熔断器进入半开状态: {api_type.value}")
            else:
                return False
        
        return True

    def record_api_call(self, api_type: APIType, success: bool, 
                       response_time: float, error_type: str = None,
                       retry_count: int = 0):
        """
        记录API调用结果
        
        Args:
            api_type: API类型
            success: 是否成功
            response_time: 响应时间
            error_type: 错误类型
            retry_count: 重试次数
        """
        call = APICall(
            timestamp=time.time(),
            api_type=api_type,
            success=success,
            response_time=response_time,
            error_type=error_type,
            retry_count=retry_count
        )
        
        self.call_history[api_type].append(call)
        
        # 更新熔断器状态
        self._update_circuit_breaker(api_type, success)
        
        # 更新自适应参数
        self._update_adaptive_params(api_type)

    def _update_circuit_breaker(self, api_type: APIType, success: bool):
        """更新熔断器状态"""
        breaker = self.circuit_breakers[api_type]
        config = self.config[api_type]
        
        if success:
            breaker['failure_count'] = 0
            if breaker['state'] == 'HALF_OPEN':
                breaker['success_count'] += 1
                if breaker['success_count'] >= 3:  # 连续3次成功
                    breaker['state'] = 'CLOSED'
                    logger.info(f"熔断器关闭: {api_type.value}")
        else:
            breaker['failure_count'] += 1
            breaker['last_failure_time'] = time.time()
            
            if breaker['failure_count'] >= config.circuit_breaker_threshold:
                breaker['state'] = 'OPEN'
                logger.warning(f"熔断器开启: {api_type.value}")

    def _update_adaptive_params(self, api_type: APIType):
        """更新自适应参数"""
        history = self.call_history[api_type]
        if len(history) < 10:  # 需要足够的样本
            return
        
        current_time = time.time()
        params = self.adaptive_params[api_type]
        config = self.config[api_type]
        
        # 只考虑最近的调用
        recent_calls = [call for call in history 
                       if current_time - call.timestamp < 300]  # 5分钟内
        
        if not recent_calls:
            return
        
        # 计算成功率
        success_rate = sum(1 for call in recent_calls if call.success) / len(recent_calls)
        
        # 计算平均响应时间
        response_times = [call.response_time for call in recent_calls if call.success]
        avg_response_time = statistics.mean(response_times) if response_times else 0
        
        # 更新参数
        params['success_rate'] = success_rate
        params['avg_response_time'] = avg_response_time
        
        # 自适应调整速率限制
        if current_time - params['last_adjustment'] > 60:  # 每分钟最多调整一次
            self._adjust_rate_limit(api_type, success_rate, avg_response_time)
            params['last_adjustment'] = current_time

    def _adjust_rate_limit(self, api_type: APIType, success_rate: float, avg_response_time: float):
        """自适应调整速率限制"""
        params = self.adaptive_params[api_type]
        config = self.config[api_type]
        current_limit = params['current_rate_limit']
        
        # 基于成功率和响应时间调整
        if success_rate > 0.95 and avg_response_time < 2.0:
            # 高成功率，低延迟 -> 可以增加速率
            new_limit = min(current_limit * 1.1, config.base_rate_limit * 2)
        elif success_rate < 0.8 or avg_response_time > 10.0:
            # 低成功率或高延迟 -> 降低速率
            new_limit = max(current_limit * 0.8, config.base_rate_limit * 0.5)
        else:
            # 保持当前速率
            new_limit = current_limit
        
        if abs(new_limit - current_limit) > 1:
            params['current_rate_limit'] = int(new_limit)
            logger.info(f"自适应调整 {api_type.value} 速率限制: {current_limit} -> {new_limit}")

    async def execute_with_retry(self, api_type: APIType, 
                                api_func: Callable, 
                                *args, **kwargs) -> Any:
        """
        带重试的API调用执行
        
        Args:
            api_type: API类型
            api_func: API函数
            *args, **kwargs: 函数参数
            
        Returns:
            API调用结果
        """
        config = self.config[api_type]
        
        for attempt in range(config.max_retries + 1):
            # 获取调用许可
            if not await self.acquire_permission(api_type):
                raise Exception(f"无法获取API调用许可: {api_type.value}")
            
            start_time = time.time()
            
            try:
                # 执行API调用
                if asyncio.iscoroutinefunction(api_func):
                    result = await api_func(*args, **kwargs)
                else:
                    result = api_func(*args, **kwargs)
                
                response_time = time.time() - start_time
                
                # 记录成功调用
                self.record_api_call(api_type, True, response_time, retry_count=attempt)
                
                return result
                
            except Exception as e:
                response_time = time.time() - start_time
                error_type = type(e).__name__
                
                # 记录失败调用
                self.record_api_call(api_type, False, response_time, error_type, attempt)
                
                if attempt == config.max_retries:
                    logger.error(f"API调用最终失败 {api_type.value}: {e}")
                    raise
                
                # 指数退避
                wait_time = (config.backoff_factor ** attempt) + random.uniform(0, 1)
                logger.warning(f"API调用失败 {api_type.value}, 第{attempt+1}次重试, 等待{wait_time:.2f}秒: {e}")
                await asyncio.sleep(wait_time)

    def get_limiter_stats(self) -> Dict[APIType, Dict[str, Any]]:
        """
        获取限流器统计信息
        
        Returns:
            Dict[APIType, Dict[str, Any]]: 统计信息
        """
        stats = {}
        current_time = time.time()
        
        for api_type in APIType:
            history = self.call_history[api_type]
            recent_calls = [call for call in history 
                           if current_time - call.timestamp < 300]  # 5分钟内
            
            if recent_calls:
                success_rate = sum(1 for call in recent_calls if call.success) / len(recent_calls)
                response_times = [call.response_time for call in recent_calls if call.success]
                avg_response_time = statistics.mean(response_times) if response_times else 0
            else:
                success_rate = 1.0
                avg_response_time = 0.0
            
            bucket = self.token_buckets[api_type]
            breaker = self.circuit_breakers[api_type]
            params = self.adaptive_params[api_type]
            
            stats[api_type] = {
                'rate_limiting': {
                    'base_limit': self.config[api_type].base_rate_limit,
                    'current_limit': params['current_rate_limit'],
                    'available_tokens': round(bucket['tokens'], 2),
                    'window_usage': len(self.sliding_windows[api_type])
                },
                'circuit_breaker': {
                    'state': breaker['state'],
                    'failure_count': breaker['failure_count'],
                    'success_count': breaker['success_count']
                },
                'performance': {
                    'recent_calls': len(recent_calls),
                    'success_rate': round(success_rate * 100, 2),
                    'avg_response_time': round(avg_response_time, 3),
                    'total_calls': len(history)
                }
            }
        
        return stats

    def reset_limiter(self, api_type: APIType):
        """重置指定API的限流器"""
        config = self.config[api_type]
        
        # 重置令牌桶
        self.token_buckets[api_type]['tokens'] = config.burst_capacity
        
        # 清空滑动窗口
        self.sliding_windows[api_type].clear()
        
        # 重置熔断器
        self.circuit_breakers[api_type] = {
            'state': 'CLOSED',
            'failure_count': 0,
            'last_failure_time': 0,
            'success_count': 0
        }
        
        # 重置自适应参数
        self.adaptive_params[api_type]['current_rate_limit'] = config.base_rate_limit
        
        logger.info(f"重置限流器: {api_type.value}")


# 全局限流器实例
_global_limiter: Optional[IntelligentRateLimiter] = None


def get_rate_limiter() -> IntelligentRateLimiter:
    """
    获取全局限流器实例
    
    Returns:
        IntelligentRateLimiter: 限流器实例
    """
    global _global_limiter
    if _global_limiter is None:
        _global_limiter = IntelligentRateLimiter()
    return _global_limiter


# 便捷装饰器
def rate_limited(api_type: APIType, priority: int = 5):
    """
    API限流装饰器
    
    Args:
        api_type: API类型
        priority: 优先级
        
    Usage:
        @rate_limited(APIType.HKEX_MONITORING)
        async def call_hkex_api():
            return await some_api_call()
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            limiter = get_rate_limiter()
            return await limiter.execute_with_retry(api_type, func, *args, **kwargs)
        return wrapper
    return decorator


if __name__ == "__main__":
    # 测试模块
    async def test_rate_limiter():
        """测试智能限流器"""
        
        print("\n" + "="*70)
        print("🚦 智能API限流器测试")
        print("="*70)
        
        # 创建限流器
        limiter = IntelligentRateLimiter()
        
        # 模拟API调用
        async def mock_api_call(success_rate: float = 0.9, delay: float = 0.1):
            await asyncio.sleep(delay)
            if random.random() < success_rate:
                return "API调用成功"
            else:
                raise Exception("API调用失败")
        
        # 测试正常调用
        print("\n📊 测试正常API调用...")
        for i in range(5):
            try:
                result = await limiter.execute_with_retry(
                    APIType.HKEX_MONITORING,
                    mock_api_call,
                    0.95,  # 95%成功率
                    0.1    # 100ms延迟
                )
                print(f"✅ 调用 {i+1}: {result}")
            except Exception as e:
                print(f"❌ 调用 {i+1}: {e}")
        
        # 测试高并发
        print(f"\n🔀 测试高并发调用...")
        
        async def concurrent_calls():
            tasks = []
            for i in range(20):
                task = asyncio.create_task(
                    limiter.execute_with_retry(
                        APIType.HKEX_MONITORING,
                        mock_api_call,
                        0.8,   # 80%成功率
                        0.05   # 50ms延迟
                    )
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            print(f"并发调用结果: {success_count}/{len(results)} 成功")
        
        await concurrent_calls()
        
        # 测试熔断器
        print(f"\n⚡ 测试熔断器...")
        
        # 连续失败调用触发熔断
        for i in range(6):
            try:
                await limiter.execute_with_retry(
                    APIType.HKEX_DOWNLOAD,
                    mock_api_call,
                    0.0,   # 0%成功率
                    0.1
                )
            except Exception as e:
                print(f"失败调用 {i+1}: {type(e).__name__}")
        
        # 显示统计信息
        print(f"\n📈 限流器统计信息:")
        stats = limiter.get_limiter_stats()
        
        for api_type, api_stats in stats.items():
            if api_stats['performance']['total_calls'] > 0:
                print(f"  {api_type.value}:")
                print(f"    当前限制: {api_stats['rate_limiting']['current_limit']}/分钟")
                print(f"    熔断器状态: {api_stats['circuit_breaker']['state']}")
                print(f"    成功率: {api_stats['performance']['success_rate']:.1f}%")
                print(f"    平均响应时间: {api_stats['performance']['avg_response_time']:.3f}s")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_rate_limiter())
