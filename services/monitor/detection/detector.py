#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告变化检测算法
实现多种检测策略：hash、content、timestamp
支持高效的增量检测和状态管理
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

from ..state.tracker import StockTracker


@dataclass
class DetectionResult:
    """检测结果数据类"""
    stock_code: str
    new_announcements: List[Any]
    total_announcements: int
    detection_method: str
    detection_time: float
    changes_detected: bool


class DetectionStrategy(ABC):
    """检测策略抽象基类"""
    
    @abstractmethod
    async def detect_changes(self, stock_code: str, current_announcements: List[Any], 
                           previous_state: Optional[Dict[str, Any]]) -> DetectionResult:
        """检测变化"""
        pass
    
    @abstractmethod
    def generate_signature(self, announcements: List[Any]) -> str:
        """生成签名"""
        pass


class HashDetectionStrategy(DetectionStrategy):
    """基于Hash的检测策略"""
    
    def __init__(self, hash_fields: List[str]):
        self.hash_fields = hash_fields
        self.logger = logging.getLogger('monitor.detector.hash')
    
    def generate_signature(self, announcements: List[Any]) -> str:
        """生成公告列表的hash签名"""
        if not announcements:
            return ""
        
        # 提取关键字段并排序
        hash_data = []
        for ann in announcements:
            fields = {}
            for field in self.hash_fields:
                fields[field] = ann.get(field, '')
            hash_data.append(fields)
        
        # 按股票代码和时间排序确保一致性
        hash_data.sort(key=lambda x: (x.get('stockCode', ''), x.get('dateTime', '')))
        
        # 生成hash
        content = json.dumps(hash_data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    async def detect_changes(self, stock_code: str, current_announcements: List[Any], 
                           previous_state: Optional[Dict[str, Any]]) -> DetectionResult:
        """基于hash检测变化"""
        current_hash = self.generate_signature(current_announcements)
        current_count = len(current_announcements)
        detection_time = time.time()
        
        if previous_state is None:
            # 首次检测，所有公告都是新的
            new_announcements = current_announcements
            changes_detected = True
            self.logger.info(f"股票 {stock_code} 首次检测，发现 {current_count} 条公告")
        else:
            previous_hash = previous_state.get('last_announcement_hash', '')
            previous_count = previous_state.get('last_announcement_count', 0)
            
            if current_hash != previous_hash:
                # Hash不同，检测具体变化
                new_announcements = self._find_new_announcements(
                    current_announcements, previous_state
                )
                changes_detected = len(new_announcements) > 0
                
                self.logger.info(
                    f"股票 {stock_code} 检测到变化，"
                    f"总数: {current_count}({previous_count}), "
                    f"新增: {len(new_announcements)}"
                )
            else:
                # Hash相同，无变化
                new_announcements = []
                changes_detected = False
                
                self.logger.debug(f"股票 {stock_code} 无变化，公告数: {current_count}")
        
        return DetectionResult(
            stock_code=stock_code,
            new_announcements=new_announcements,
            total_announcements=current_count,
            detection_method='hash',
            detection_time=detection_time,
            changes_detected=changes_detected
        )
    
    def _find_new_announcements(self, current_announcements: List[Any], 
                               previous_state: Dict[str, Any]) -> List[Any]:
        """查找新公告（简化版本，基于时间戳）"""
        # 获取上次检查时间
        last_check_time = previous_state.get('last_check_time', 0)
        last_check_dt = datetime.fromtimestamp(last_check_time)
        
        new_announcements = []
        for ann in current_announcements:
            try:
                # 解析公告时间
                date_time_str = ann.get('DATE_TIME', '')
                if date_time_str:
                    # 尝试解析港交所的时间格式
                    ann_dt = self._parse_hkex_datetime(date_time_str)
                    if ann_dt and ann_dt > last_check_dt:
                        new_announcements.append(ann)
                        
            except Exception as e:
                self.logger.warning(f"解析公告时间失败: {e}")
                # 如果时间解析失败，保守地认为是新公告
                new_announcements.append(ann)
        
        return new_announcements
    
    def _parse_hkex_datetime(self, date_time_str: str) -> Optional[datetime]:
        """解析港交所的日期时间格式"""
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S", 
            "%d/%m/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%d-%m-%Y"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_time_str.strip(), fmt)
            except ValueError:
                continue
        
        return None


class ContentDetectionStrategy(DetectionStrategy):
    """基于内容的检测策略"""
    
    def __init__(self, content_fields: List[str]):
        self.content_fields = content_fields
        self.logger = logging.getLogger('monitor.detector.content')
    
    def generate_signature(self, announcements: List[Any]) -> str:
        """生成内容签名"""
        if not announcements:
            return ""
        
        # 提取内容字段
        content_items = []
        for ann in announcements:
            content = {}
            for field in self.content_fields:
                content[field] = ann.get(field, '')
            content_items.append(content)
        
        # 排序并生成hash
        content_items.sort(key=lambda x: x.get('TITLE', ''))
        content = json.dumps(content_items, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    async def detect_changes(self, stock_code: str, current_announcements: List[Any], 
                           previous_state: Optional[Dict[str, Any]]) -> DetectionResult:
        """基于内容检测变化"""
        current_signature = self.generate_signature(current_announcements)
        current_count = len(current_announcements)
        detection_time = time.time()
        
        if previous_state is None:
            new_announcements = current_announcements
            changes_detected = True
        else:
            previous_signature = previous_state.get('content_signature', '')
            
            if current_signature != previous_signature:
                # 内容有变化，进行详细比较
                new_announcements = self._compare_content(
                    current_announcements, previous_state
                )
                changes_detected = len(new_announcements) > 0
            else:
                new_announcements = []
                changes_detected = False
        
        return DetectionResult(
            stock_code=stock_code,
            new_announcements=new_announcements,
            total_announcements=current_count,
            detection_method='content',
            detection_time=detection_time,
            changes_detected=changes_detected
        )
    
    def _compare_content(self, current_announcements: List[Any], 
                        previous_state: Dict[str, Any]) -> List[Any]:
        """比较内容找出新公告"""
        # 简化实现：基于标题去重
        previous_titles = set(previous_state.get('announcement_titles', []))
        
        new_announcements = []
        for ann in current_announcements:
            title = ann.get('TITLE', '')
            if title not in previous_titles:
                new_announcements.append(ann)
        
        return new_announcements


class TimestampDetectionStrategy(DetectionStrategy):
    """基于时间戳的检测策略"""
    
    def __init__(self, time_threshold_minutes: int = 60):
        self.time_threshold_minutes = time_threshold_minutes
        self.logger = logging.getLogger('monitor.detector.timestamp')
    
    def generate_signature(self, announcements: List[Any]) -> str:
        """生成时间戳签名"""
        if not announcements:
            return ""
        
        timestamps = []
        for ann in announcements:
            date_time = ann.get('DATE_TIME', '')
            timestamps.append(date_time)
        
        timestamps.sort()
        content = '|'.join(timestamps)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    async def detect_changes(self, stock_code: str, current_announcements: List[Any], 
                           previous_state: Optional[Dict[str, Any]]) -> DetectionResult:
        """基于时间戳检测变化"""
        current_count = len(current_announcements)
        detection_time = time.time()
        
        # 获取时间阈值
        threshold_dt = datetime.now() - timedelta(minutes=self.time_threshold_minutes)
        
        # 查找最近的公告
        recent_announcements = []
        for ann in current_announcements:
            try:
                date_time_str = ann.get('DATE_TIME', '')
                if date_time_str:
                    ann_dt = self._parse_datetime(date_time_str)
                    if ann_dt and ann_dt > threshold_dt:
                        recent_announcements.append(ann)
            except Exception as e:
                self.logger.warning(f"时间戳解析失败: {e}")
        
        changes_detected = len(recent_announcements) > 0
        
        return DetectionResult(
            stock_code=stock_code,
            new_announcements=recent_announcements,
            total_announcements=current_count,
            detection_method='timestamp',
            detection_time=detection_time,
            changes_detected=changes_detected
        )
    
    def _parse_datetime(self, date_time_str: str) -> Optional[datetime]:
        """解析日期时间字符串"""
        # 重用HashDetectionStrategy的解析方法
        hash_strategy = HashDetectionStrategy([])
        return hash_strategy._parse_hkex_datetime(date_time_str)


class ChangeDetector:
    """公告变化检测器"""
    
    def __init__(self, monitor_config: Dict[str, Any]):
        """初始化变化检测器"""
        self.config = monitor_config
        self.logger = logging.getLogger('monitor.detector')
        
        # 获取检测配置
        detection_config = monitor_config.get('change_detection', {})
        self.algorithm = detection_config.get('algorithm', 'hash')
        
        # 初始化检测策略
        self._init_strategy(detection_config)
        
        # 统计信息
        self.stats = {
            'total_detections': 0,
            'changes_found': 0,
            'false_positives': 0,
            'detection_errors': 0
        }
        
        self.logger.info(f"变化检测器初始化完成，算法: {self.algorithm}")
    
    def _init_strategy(self, detection_config: Dict[str, Any]):
        """初始化检测策略"""
        if self.algorithm == 'hash':
            hash_fields = detection_config.get('hash_fields', [
                'stockCode', 'title', 'dateTime', 'longText'
            ])
            self.strategy = HashDetectionStrategy(hash_fields)
            
        elif self.algorithm == 'content':
            content_fields = detection_config.get('content_fields', [
                'TITLE', 'LONG_TEXT'
            ])
            self.strategy = ContentDetectionStrategy(content_fields)
            
        elif self.algorithm == 'timestamp':
            time_threshold = detection_config.get('time_threshold_minutes', 60)
            self.strategy = TimestampDetectionStrategy(time_threshold)
            
        else:
            raise ValueError(f"不支持的检测算法: {self.algorithm}")
    
    def generate_hash(self, announcement: Dict[str, Any]) -> str:
        """生成单个公告的hash（向后兼容）"""
        return self.strategy.generate_signature([announcement])
    
    async def detect_changes(self, stock_code: str, current_announcements: List[Any]) -> List[Any]:
        """检测公告变化"""
        try:
            self.stats['total_detections'] += 1
            
            # 这里应该从StockTracker获取之前的状态
            # 简化实现：返回所有当前公告作为新公告
            # 实际实现需要与StockTracker集成
            
            result = await self.strategy.detect_changes(
                stock_code, current_announcements, None
            )
            
            if result.changes_detected:
                self.stats['changes_found'] += 1
                self.logger.info(
                    f"股票 {stock_code} 检测到 {len(result.new_announcements)} 条新公告"
                )
            
            return result.new_announcements
            
        except Exception as e:
            self.stats['detection_errors'] += 1
            self.logger.error(f"检测股票 {stock_code} 变化失败: {e}")
            return []
    
    async def detect_changes_with_state(self, stock_code: str, 
                                       current_announcements: List[Any],
                                       previous_state: Optional[Dict[str, Any]]) -> DetectionResult:
        """使用状态检测变化（完整版本）"""
        try:
            self.stats['total_detections'] += 1
            
            result = await self.strategy.detect_changes(
                stock_code, current_announcements, previous_state
            )
            
            if result.changes_detected:
                self.stats['changes_found'] += 1
            
            return result
            
        except Exception as e:
            self.stats['detection_errors'] += 1
            self.logger.error(f"检测股票 {stock_code} 变化失败: {e}")
            
            # 返回空结果
            return DetectionResult(
                stock_code=stock_code,
                new_announcements=[],
                total_announcements=len(current_announcements),
                detection_method=self.algorithm,
                detection_time=time.time(),
                changes_detected=False
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取检测统计信息"""
        stats = self.stats.copy()
        stats['algorithm'] = self.algorithm
        
        if stats['total_detections'] > 0:
            stats['change_rate'] = stats['changes_found'] / stats['total_detections']
            stats['error_rate'] = stats['detection_errors'] / stats['total_detections']
        else:
            stats['change_rate'] = 0.0
            stats['error_rate'] = 0.0
        
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_detections': 0,
            'changes_found': 0,
            'false_positives': 0,
            'detection_errors': 0
        }
    
    async def cleanup(self):
        """清理资源"""
        try:
            # 输出最终统计
            stats = self.get_stats()
            self.logger.info(f"检测器统计信息: {stats}")
            
        except Exception as e:
            self.logger.error(f"检测器清理失败: {e}")


# 工厂函数
def create_detector(algorithm: str, config: Dict[str, Any]) -> DetectionStrategy:
    """创建检测策略工厂函数"""
    detection_config = config.get('change_detection', {})
    
    if algorithm == 'hash':
        hash_fields = detection_config.get('hash_fields', [
            'stockCode', 'title', 'dateTime', 'longText'
        ])
        return HashDetectionStrategy(hash_fields)
    
    elif algorithm == 'content':
        content_fields = detection_config.get('content_fields', [
            'TITLE', 'LONG_TEXT'
        ])
        return ContentDetectionStrategy(content_fields)
    
    elif algorithm == 'timestamp':
        time_threshold = detection_config.get('time_threshold_minutes', 60)
        return TimestampDetectionStrategy(time_threshold)
    
    else:
        raise ValueError(f"不支持的检测算法: {algorithm}")