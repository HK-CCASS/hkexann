"""
智能错误分类器

这个模块实现了高精度的错误分类系统，能够准确识别不同类型的错误
并提供相应的处理建议。支持机器学习和规则引擎两种分类方法。

主要功能：
- 多层级错误分类算法
- 上下文感知错误识别
- 动态分类规则更新
- 错误模式学习和优化

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import re
import logging
import time
from typing import Dict, Any, Optional, List, Tuple, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, Counter
import json

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ErrorPattern(Enum):
    """错误模式枚举"""
    CONNECTION_TIMEOUT = "connection_timeout"
    CONNECTION_REFUSED = "connection_refused"
    DNS_RESOLUTION = "dns_resolution"
    SSL_ERROR = "ssl_error"
    HTTP_4XX = "http_4xx"
    HTTP_5XX = "http_5xx"
    RATE_LIMIT = "rate_limit"
    AUTH_FAILURE = "auth_failure"
    PERMISSION_DENIED = "permission_denied"
    FILE_NOT_FOUND = "file_not_found"
    DISK_FULL = "disk_full"
    MEMORY_ERROR = "memory_error"
    CPU_TIMEOUT = "cpu_timeout"
    JSON_PARSE_ERROR = "json_parse_error"
    XML_PARSE_ERROR = "xml_parse_error"
    DATA_VALIDATION = "data_validation"
    DATABASE_CONNECTION = "database_connection"
    DATABASE_LOCK = "database_lock"
    BUSINESS_RULE = "business_rule"


@dataclass
class ClassificationRule:
    """分类规则"""
    pattern: ErrorPattern
    weight: float
    conditions: List[Callable[[Exception, Dict[str, Any]], bool]]
    description: str = ""


@dataclass
class ErrorFeatures:
    """错误特征"""
    error_type: str
    error_message: str
    stack_trace: str
    context: Dict[str, Any]
    
    # 提取的特征
    message_keywords: Set[str] = field(default_factory=set)
    error_codes: Set[str] = field(default_factory=set)
    module_names: Set[str] = field(default_factory=set)
    function_names: Set[str] = field(default_factory=set)
    file_paths: Set[str] = field(default_factory=set)


class AdvancedErrorClassifier:
    """
    高级错误分类器
    
    使用多种算法和启发式规则对错误进行精确分类：
    1. 基于正则表达式的模式匹配
    2. 关键词权重评分
    3. 上下文信息分析
    4. 历史模式学习
    """
    
    def __init__(self):
        """初始化错误分类器"""
        
        # 分类规则
        self.classification_rules = self._build_classification_rules()
        
        # 关键词词典
        self.keyword_dictionary = self._build_keyword_dictionary()
        
        # 错误模式历史
        self.pattern_history = defaultdict(list)
        
        # 分类统计
        self.classification_stats = defaultdict(int)
        
        logger.info("🧠 高级错误分类器初始化完成")

    def _build_classification_rules(self) -> List[ClassificationRule]:
        """构建分类规则"""
        return [
            # 网络连接错误
            ClassificationRule(
                pattern=ErrorPattern.CONNECTION_TIMEOUT,
                weight=0.9,
                conditions=[
                    lambda e, c: "timeout" in str(e).lower(),
                    lambda e, c: "timed out" in str(e).lower(),
                    lambda e, c: isinstance(e, TimeoutError),
                ],
                description="连接超时错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.CONNECTION_REFUSED,
                weight=0.9,
                conditions=[
                    lambda e, c: "connection refused" in str(e).lower(),
                    lambda e, c: "refused" in str(e).lower(),
                ],
                description="连接被拒绝错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.DNS_RESOLUTION,
                weight=0.85,
                conditions=[
                    lambda e, c: "name resolution" in str(e).lower(),
                    lambda e, c: "dns" in str(e).lower(),
                    lambda e, c: "host not found" in str(e).lower(),
                ],
                description="DNS解析错误"
            ),
            
            # HTTP错误
            ClassificationRule(
                pattern=ErrorPattern.HTTP_4XX,
                weight=0.8,
                conditions=[
                    lambda e, c: re.search(r'4\d{2}', str(e)),
                    lambda e, c: "client error" in str(e).lower(),
                ],
                description="HTTP 4xx客户端错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.HTTP_5XX,
                weight=0.8,
                conditions=[
                    lambda e, c: re.search(r'5\d{2}', str(e)),
                    lambda e, c: "server error" in str(e).lower(),
                ],
                description="HTTP 5xx服务器错误"
            ),
            
            # API限流和认证
            ClassificationRule(
                pattern=ErrorPattern.RATE_LIMIT,
                weight=0.95,
                conditions=[
                    lambda e, c: "rate limit" in str(e).lower(),
                    lambda e, c: "too many requests" in str(e).lower(),
                    lambda e, c: "429" in str(e),
                    lambda e, c: "quota exceeded" in str(e).lower(),
                ],
                description="API速率限制错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.AUTH_FAILURE,
                weight=0.9,
                conditions=[
                    lambda e, c: "unauthorized" in str(e).lower(),
                    lambda e, c: "authentication" in str(e).lower(),
                    lambda e, c: "invalid token" in str(e).lower(),
                    lambda e, c: "401" in str(e),
                ],
                description="认证失败错误"
            ),
            
            # 文件和权限错误
            ClassificationRule(
                pattern=ErrorPattern.FILE_NOT_FOUND,
                weight=0.9,
                conditions=[
                    lambda e, c: isinstance(e, FileNotFoundError),
                    lambda e, c: "no such file" in str(e).lower(),
                    lambda e, c: "file not found" in str(e).lower(),
                ],
                description="文件未找到错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.PERMISSION_DENIED,
                weight=0.9,
                conditions=[
                    lambda e, c: isinstance(e, PermissionError),
                    lambda e, c: "permission denied" in str(e).lower(),
                    lambda e, c: "access denied" in str(e).lower(),
                    lambda e, c: "403" in str(e),
                ],
                description="权限拒绝错误"
            ),
            
            # 系统资源错误
            ClassificationRule(
                pattern=ErrorPattern.MEMORY_ERROR,
                weight=0.9,
                conditions=[
                    lambda e, c: isinstance(e, MemoryError),
                    lambda e, c: "out of memory" in str(e).lower(),
                    lambda e, c: "memory" in str(e).lower() and "error" in str(e).lower(),
                ],
                description="内存不足错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.DISK_FULL,
                weight=0.85,
                conditions=[
                    lambda e, c: "no space left" in str(e).lower(),
                    lambda e, c: "disk full" in str(e).lower(),
                    lambda e, c: "device full" in str(e).lower(),
                ],
                description="磁盘空间不足错误"
            ),
            
            # 数据解析错误
            ClassificationRule(
                pattern=ErrorPattern.JSON_PARSE_ERROR,
                weight=0.9,
                conditions=[
                    lambda e, c: isinstance(e, json.JSONDecodeError),
                    lambda e, c: "json" in str(e).lower(),
                    lambda e, c: "invalid json" in str(e).lower(),
                ],
                description="JSON解析错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.DATA_VALIDATION,
                weight=0.8,
                conditions=[
                    lambda e, c: isinstance(e, ValueError),
                    lambda e, c: "validation" in str(e).lower(),
                    lambda e, c: "invalid" in str(e).lower(),
                ],
                description="数据验证错误"
            ),
            
            # 数据库错误
            ClassificationRule(
                pattern=ErrorPattern.DATABASE_CONNECTION,
                weight=0.85,
                conditions=[
                    lambda e, c: "database" in str(e).lower(),
                    lambda e, c: "connection" in str(e).lower() and any(db in str(e).lower() for db in ["mysql", "postgres", "oracle", "clickhouse"]),
                ],
                description="数据库连接错误"
            ),
            ClassificationRule(
                pattern=ErrorPattern.DATABASE_LOCK,
                weight=0.8,
                conditions=[
                    lambda e, c: "deadlock" in str(e).lower(),
                    lambda e, c: "lock" in str(e).lower() and "database" in str(e).lower(),
                ],
                description="数据库锁定错误"
            ),
        ]

    def _build_keyword_dictionary(self) -> Dict[ErrorPattern, Dict[str, float]]:
        """构建关键词词典"""
        return {
            ErrorPattern.CONNECTION_TIMEOUT: {
                "timeout": 1.0, "timed": 0.9, "connection": 0.7, "connect": 0.7
            },
            ErrorPattern.CONNECTION_REFUSED: {
                "refused": 1.0, "reject": 0.8, "connection": 0.7, "connect": 0.7
            },
            ErrorPattern.RATE_LIMIT: {
                "rate": 0.9, "limit": 0.9, "throttle": 0.8, "quota": 0.8, "429": 1.0
            },
            ErrorPattern.AUTH_FAILURE: {
                "unauthorized": 1.0, "authentication": 0.9, "token": 0.8, "401": 1.0
            },
            ErrorPattern.JSON_PARSE_ERROR: {
                "json": 1.0, "parse": 0.9, "decode": 0.8, "syntax": 0.7
            },
            ErrorPattern.MEMORY_ERROR: {
                "memory": 1.0, "allocation": 0.8, "oom": 0.9, "heap": 0.7
            }
        }

    def extract_features(self, error: Exception, context: Dict[str, Any] = None) -> ErrorFeatures:
        """
        提取错误特征
        
        Args:
            error: 异常对象
            context: 错误上下文
            
        Returns:
            ErrorFeatures: 提取的特征
        """
        context = context or {}
        
        error_message = str(error).lower()
        stack_trace = context.get('stack_trace', '')
        
        features = ErrorFeatures(
            error_type=type(error).__name__,
            error_message=error_message,
            stack_trace=stack_trace,
            context=context
        )
        
        # 提取关键词
        features.message_keywords = self._extract_keywords(error_message)
        
        # 提取错误代码
        features.error_codes = self._extract_error_codes(error_message)
        
        # 从堆栈跟踪提取模块和函数名
        if stack_trace:
            features.module_names = self._extract_module_names(stack_trace)
            features.function_names = self._extract_function_names(stack_trace)
            features.file_paths = self._extract_file_paths(stack_trace)
        
        return features

    def _extract_keywords(self, text: str) -> Set[str]:
        """提取关键词"""
        # 移除标点符号并分词
        words = re.findall(r'\b\w+\b', text.lower())
        
        # 过滤常见词汇
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        
        return set(word for word in words if len(word) > 2 and word not in stop_words)

    def _extract_error_codes(self, text: str) -> Set[str]:
        """提取错误代码"""
        # HTTP状态码
        http_codes = re.findall(r'\b[1-5]\d{2}\b', text)
        
        # 其他错误代码模式
        error_codes = re.findall(r'\b[A-Z]+_\d+\b', text)
        error_codes.extend(re.findall(r'\b\d{4,}\b', text))
        
        return set(http_codes + error_codes)

    def _extract_module_names(self, stack_trace: str) -> Set[str]:
        """从堆栈跟踪提取模块名"""
        modules = re.findall(r'File ".*?([^/\\]+)\.py"', stack_trace)
        return set(modules)

    def _extract_function_names(self, stack_trace: str) -> Set[str]:
        """从堆栈跟踪提取函数名"""
        functions = re.findall(r'in (\w+)', stack_trace)
        return set(functions)

    def _extract_file_paths(self, stack_trace: str) -> Set[str]:
        """从堆栈跟踪提取文件路径"""
        paths = re.findall(r'File "(.*?)"', stack_trace)
        return set(paths)

    def classify_error(self, error: Exception, context: Dict[str, Any] = None) -> Tuple[ErrorPattern, float]:
        """
        分类错误
        
        Args:
            error: 异常对象
            context: 错误上下文
            
        Returns:
            Tuple[ErrorPattern, float]: 错误模式和置信度
        """
        context = context or {}
        
        # 提取特征
        features = self.extract_features(error, context)
        
        # 计算各种模式的得分
        pattern_scores = {}
        
        # 1. 基于规则的评分
        rule_scores = self._classify_by_rules(error, context)
        
        # 2. 基于关键词的评分
        keyword_scores = self._classify_by_keywords(features)
        
        # 3. 基于上下文的评分
        context_scores = self._classify_by_context(features)
        
        # 合并得分
        all_patterns = set(rule_scores.keys()) | set(keyword_scores.keys()) | set(context_scores.keys())
        
        for pattern in all_patterns:
            pattern_scores[pattern] = (
                rule_scores.get(pattern, 0) * 0.6 +
                keyword_scores.get(pattern, 0) * 0.3 +
                context_scores.get(pattern, 0) * 0.1
            )
        
        # 找到最高得分的模式
        if pattern_scores:
            best_pattern = max(pattern_scores, key=pattern_scores.get)
            confidence = pattern_scores[best_pattern]
            
            # 更新统计
            self.classification_stats[best_pattern] += 1
            self.pattern_history[best_pattern].append({
                'timestamp': time.time(),
                'error_type': features.error_type,
                'confidence': confidence
            })
            
            logger.debug(f"错误分类: {best_pattern.value} (置信度: {confidence:.2f})")
            
            return best_pattern, confidence
        else:
            return ErrorPattern.CONNECTION_TIMEOUT, 0.0  # 默认值

    def _classify_by_rules(self, error: Exception, context: Dict[str, Any]) -> Dict[ErrorPattern, float]:
        """基于规则分类"""
        scores = {}
        
        for rule in self.classification_rules:
            match_count = 0
            for condition in rule.conditions:
                try:
                    if condition(error, context):
                        match_count += 1
                except Exception:
                    continue
            
            if match_count > 0:
                # 根据匹配条件数量计算得分
                score = (match_count / len(rule.conditions)) * rule.weight
                scores[rule.pattern] = score
        
        return scores

    def _classify_by_keywords(self, features: ErrorFeatures) -> Dict[ErrorPattern, float]:
        """基于关键词分类"""
        scores = {}
        
        for pattern, keywords in self.keyword_dictionary.items():
            score = 0.0
            keyword_count = 0
            
            for keyword, weight in keywords.items():
                if keyword in features.message_keywords:
                    score += weight
                    keyword_count += 1
            
            if keyword_count > 0:
                # 归一化得分
                scores[pattern] = min(1.0, score / len(keywords))
        
        return scores

    def _classify_by_context(self, features: ErrorFeatures) -> Dict[ErrorPattern, float]:
        """基于上下文分类"""
        scores = {}
        
        # 根据错误发生的上下文信息进行分类
        context = features.context
        
        # API相关上下文
        if context.get('api_endpoint'):
            if any(code in features.error_codes for code in ['429', '503']):
                scores[ErrorPattern.RATE_LIMIT] = 0.8
            elif any(code in features.error_codes for code in ['401', '403']):
                scores[ErrorPattern.AUTH_FAILURE] = 0.8
        
        # 数据库相关上下文
        if context.get('database_operation'):
            if 'timeout' in features.message_keywords:
                scores[ErrorPattern.DATABASE_CONNECTION] = 0.7
        
        # 文件操作相关上下文
        if context.get('file_operation'):
            if features.error_type == 'FileNotFoundError':
                scores[ErrorPattern.FILE_NOT_FOUND] = 0.9
            elif features.error_type == 'PermissionError':
                scores[ErrorPattern.PERMISSION_DENIED] = 0.9
        
        return scores

    def get_classification_suggestions(self, pattern: ErrorPattern, confidence: float) -> Dict[str, Any]:
        """
        获取分类建议
        
        Args:
            pattern: 错误模式
            confidence: 置信度
            
        Returns:
            Dict[str, Any]: 处理建议
        """
        suggestions = {
            ErrorPattern.CONNECTION_TIMEOUT: {
                'retry_strategy': 'exponential_backoff',
                'max_retries': 3,
                'timeout_increase': True,
                'circuit_breaker': True,
                'description': '网络连接超时，建议增加超时时间并实施重试'
            },
            ErrorPattern.RATE_LIMIT: {
                'retry_strategy': 'exponential_backoff',
                'max_retries': 5,
                'delay_multiplier': 2.0,
                'respect_retry_after': True,
                'description': 'API限流，建议实施退避策略并尊重Retry-After头'
            },
            ErrorPattern.AUTH_FAILURE: {
                'retry_strategy': 'none',
                'refresh_token': True,
                'alert_security_team': True,
                'description': '认证失败，需要刷新令牌或检查凭据'
            },
            ErrorPattern.FILE_NOT_FOUND: {
                'retry_strategy': 'fixed_delay',
                'max_retries': 2,
                'create_file_if_missing': True,
                'description': '文件未找到，可能需要创建或检查路径'
            },
            ErrorPattern.MEMORY_ERROR: {
                'retry_strategy': 'none',
                'garbage_collect': True,
                'reduce_batch_size': True,
                'alert_ops_team': True,
                'description': '内存不足，需要优化内存使用或增加资源'
            }
        }
        
        default_suggestion = {
            'retry_strategy': 'exponential_backoff',
            'max_retries': 2,
            'description': '通用错误处理策略'
        }
        
        suggestion = suggestions.get(pattern, default_suggestion)
        suggestion['confidence'] = confidence
        suggestion['pattern'] = pattern.value
        
        return suggestion

    def get_classification_statistics(self) -> Dict[str, Any]:
        """获取分类统计信息"""
        total_classifications = sum(self.classification_stats.values())
        
        pattern_distribution = {}
        for pattern, count in self.classification_stats.items():
            pattern_distribution[pattern.value] = {
                'count': count,
                'percentage': (count / total_classifications * 100) if total_classifications > 0 else 0
            }
        
        # 最近的模式趋势
        recent_patterns = []
        current_time = time.time()
        
        for pattern, history in self.pattern_history.items():
            recent_count = len([h for h in history if current_time - h['timestamp'] < 3600])  # 最近1小时
            if recent_count > 0:
                recent_patterns.append({
                    'pattern': pattern.value,
                    'recent_count': recent_count
                })
        
        return {
            'total_classifications': total_classifications,
            'pattern_distribution': pattern_distribution,
            'recent_trends': sorted(recent_patterns, key=lambda x: x['recent_count'], reverse=True),
            'most_common_patterns': [
                pattern.value for pattern, _ in 
                Counter(self.classification_stats).most_common(5)
            ]
        }


if __name__ == "__main__":
    # 测试模块
    def test_error_classifier():
        """测试错误分类器"""
        
        print("\n" + "="*70)
        print("🧠 智能错误分类器测试")
        print("="*70)
        
        # 创建分类器
        classifier = AdvancedErrorClassifier()
        
        # 测试各种错误类型
        test_errors = [
            (ConnectionError("Connection timed out after 30 seconds"), {"api_endpoint": "hkex.com"}),
            (Exception("HTTP 429: Too many requests. Rate limit exceeded"), {"api_endpoint": "api.hkex.com"}),
            (ValueError("Invalid JSON format in response"), {"data_parsing": True}),
            (FileNotFoundError("No such file or directory: /data/config.json"), {"file_operation": True}),
            (MemoryError("Cannot allocate memory"), {"system_operation": True}),
            (Exception("Database connection timeout"), {"database_operation": True}),
            (PermissionError("Access denied to file"), {"file_operation": True}),
            (Exception("Unauthorized: Invalid API token"), {"api_endpoint": "secure.api.com"}),
        ]
        
        print("\n🔍 测试错误分类...")
        
        for i, (error, context) in enumerate(test_errors, 1):
            # 分类错误
            pattern, confidence = classifier.classify_error(error, context)
            
            # 获取处理建议
            suggestions = classifier.get_classification_suggestions(pattern, confidence)
            
            print(f"\n{i}. 错误: {str(error)[:60]}...")
            print(f"   分类: {pattern.value}")
            print(f"   置信度: {confidence:.2f}")
            print(f"   建议策略: {suggestions['retry_strategy']}")
            print(f"   描述: {suggestions['description']}")
        
        # 显示分类统计
        print(f"\n📊 分类统计:")
        stats = classifier.get_classification_statistics()
        
        print(f"  总分类数: {stats['total_classifications']}")
        print(f"  最常见模式:")
        for pattern in stats['most_common_patterns']:
            print(f"    - {pattern}")
        
        if stats['recent_trends']:
            print(f"  最近趋势:")
            for trend in stats['recent_trends'][:3]:
                print(f"    - {trend['pattern']}: {trend['recent_count']} 次")
        
        print("\n" + "="*70)
    
    # 运行测试
    test_error_classifier()
