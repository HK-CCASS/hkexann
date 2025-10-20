#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 股票状态管理系统
负责跟踪和管理股票公告的监听状态，支持Redis和文件存储
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, asdict
from pathlib import Path

try:
    import aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    aioredis = None


@dataclass
class StockState:
    """股票状态数据类"""
    stock_code: str
    last_check_time: float
    last_announcement_count: int
    last_announcement_hash: str
    error_count: int
    status: str  # 'active', 'paused', 'error'
    created_time: float
    updated_time: float
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StockState':
        """从字典创建对象"""
        return cls(**data)


class StockTracker:
    """股票状态跟踪器"""
    
    def __init__(self, monitor_config: Dict[str, Any]):
        """初始化状态跟踪器"""
        self.config = monitor_config
        self.logger = logging.getLogger('monitor.tracker')
        
        # 存储配置
        storage_config = monitor_config.get('change_detection', {}).get('storage', {})
        self.storage_type = storage_config.get('type', 'redis')
        
        # 初始化存储
        self._init_storage(storage_config)
        
        # 状态缓存
        self._state_cache: Dict[str, StockState] = {}
        self._cache_updated = False
        
        self.logger.info(f"股票状态跟踪器初始化完成，存储类型: {self.storage_type}")
    
    def _init_storage(self, storage_config: Dict[str, Any]):
        """初始化存储后端"""
        if self.storage_type == 'redis':
            self._init_redis_storage(storage_config.get('redis', {}))
        elif self.storage_type == 'file':
            self._init_file_storage(storage_config.get('file', {}))
        elif self.storage_type == 'memory':
            self._init_memory_storage()
        else:
            raise ValueError(f"不支持的存储类型: {self.storage_type}")
    
    def _init_redis_storage(self, redis_config: Dict[str, Any]):
        """初始化Redis存储"""
        if not REDIS_AVAILABLE:
            raise ImportError("Redis存储需要安装aioredis: pip install aioredis")
        
        self.redis_host = redis_config.get('host', 'localhost')
        self.redis_port = redis_config.get('port', 6379)
        self.redis_db = redis_config.get('db', 0)
        self.redis_key_prefix = redis_config.get('key_prefix', 'hkex:monitor:')
        self.redis_expire_days = redis_config.get('expire_days', 30)
        
        self._redis = None
        self.logger.info(f"Redis存储配置: {self.redis_host}:{self.redis_port}/{self.redis_db}")
    
    def _init_file_storage(self, file_config: Dict[str, Any]):
        """初始化文件存储"""
        self.file_path = Path(file_config.get('path', 'monitor_state.json'))
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"文件存储路径: {self.file_path}")
    
    def _init_memory_storage(self):
        """初始化内存存储"""
        self._memory_storage: Dict[str, StockState] = {}
        self.logger.info("使用内存存储")
    
    async def _get_redis(self):
        """获取Redis连接"""
        if self._redis is None:
            self._redis = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}",
                decode_responses=True
            )
        return self._redis
    
    async def get_stock_state(self, stock_code: str) -> Optional[StockState]:
        """获取股票状态"""
        try:
            # 先从缓存中查找
            if stock_code in self._state_cache:
                return self._state_cache[stock_code]
            
            # 从存储中加载
            if self.storage_type == 'redis':
                state = await self._get_stock_state_redis(stock_code)
            elif self.storage_type == 'file':
                state = await self._get_stock_state_file(stock_code)
            elif self.storage_type == 'memory':
                state = self._get_stock_state_memory(stock_code)
            else:
                state = None
            
            # 更新缓存
            if state:
                self._state_cache[stock_code] = state
            
            return state
            
        except Exception as e:
            self.logger.error(f"获取股票 {stock_code} 状态失败: {e}")
            return None
    
    async def _get_stock_state_redis(self, stock_code: str) -> Optional[StockState]:
        """从Redis获取股票状态"""
        redis = await self._get_redis()
        key = f"{self.redis_key_prefix}state:{stock_code}"
        
        data = await redis.get(key)
        if data:
            try:
                state_dict = json.loads(data)
                return StockState.from_dict(state_dict)
            except json.JSONDecodeError as e:
                self.logger.error(f"Redis数据解析失败: {e}")
        
        return None
    
    async def _get_stock_state_file(self, stock_code: str) -> Optional[StockState]:
        """从文件获取股票状态"""
        if not self.file_path.exists():
            return None
        
        def load_file():
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get(stock_code)
        
        try:
            state_data = await asyncio.get_event_loop().run_in_executor(
                None, load_file
            )
            if state_data:
                return StockState.from_dict(state_data)
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"文件读取失败: {e}")
        
        return None
    
    def _get_stock_state_memory(self, stock_code: str) -> Optional[StockState]:
        """从内存获取股票状态"""
        return self._memory_storage.get(stock_code)
    
    async def set_stock_state(self, stock_state: StockState) -> bool:
        """设置股票状态"""
        try:
            # 更新时间戳
            stock_state.updated_time = time.time()
            
            # 更新缓存
            self._state_cache[stock_state.stock_code] = stock_state
            self._cache_updated = True
            
            # 保存到存储
            if self.storage_type == 'redis':
                success = await self._set_stock_state_redis(stock_state)
            elif self.storage_type == 'file':
                success = await self._set_stock_state_file(stock_state)
            elif self.storage_type == 'memory':
                success = self._set_stock_state_memory(stock_state)
            else:
                success = False
            
            if success:
                self.logger.debug(f"更新股票 {stock_state.stock_code} 状态成功")
            
            return success
            
        except Exception as e:
            self.logger.error(f"设置股票 {stock_state.stock_code} 状态失败: {e}")
            return False
    
    async def _set_stock_state_redis(self, stock_state: StockState) -> bool:
        """保存股票状态到Redis"""
        try:
            redis = await self._get_redis()
            key = f"{self.redis_key_prefix}state:{stock_state.stock_code}"
            
            data = json.dumps(stock_state.to_dict())
            await redis.set(key, data)
            
            # 设置过期时间
            expire_seconds = self.redis_expire_days * 24 * 3600
            await redis.expire(key, expire_seconds)
            
            return True
        except Exception as e:
            self.logger.error(f"Redis保存失败: {e}")
            return False
    
    async def _set_stock_state_file(self, stock_state: StockState) -> bool:
        """保存股票状态到文件"""
        def save_file():
            # 读取现有数据
            data = {}
            if self.file_path.exists():
                try:
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    data = {}
            
            # 更新数据
            data[stock_state.stock_code] = stock_state.to_dict()
            
            # 写入文件
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return True
        
        try:
            return await asyncio.get_event_loop().run_in_executor(
                None, save_file
            )
        except Exception as e:
            self.logger.error(f"文件保存失败: {e}")
            return False
    
    def _set_stock_state_memory(self, stock_state: StockState) -> bool:
        """保存股票状态到内存"""
        self._memory_storage[stock_state.stock_code] = stock_state
        return True
    
    async def create_stock_state(self, stock_code: str) -> StockState:
        """创建新的股票状态"""
        current_time = time.time()
        stock_state = StockState(
            stock_code=stock_code,
            last_check_time=current_time,
            last_announcement_count=0,
            last_announcement_hash='',
            error_count=0,
            status='active',
            created_time=current_time,
            updated_time=current_time
        )
        
        await self.set_stock_state(stock_state)
        self.logger.info(f"创建股票 {stock_code} 状态")
        return stock_state
    
    async def update_check_time(self, stock_code: str, announcement_count: int = 0, 
                              announcement_hash: str = '') -> bool:
        """更新检查时间"""
        state = await self.get_stock_state(stock_code)
        if state is None:
            state = await self.create_stock_state(stock_code)
        
        state.last_check_time = time.time()
        state.last_announcement_count = announcement_count
        state.last_announcement_hash = announcement_hash
        state.status = 'active'
        
        return await self.set_stock_state(state)
    
    async def increment_error_count(self, stock_code: str) -> int:
        """增加错误计数"""
        state = await self.get_stock_state(stock_code)
        if state is None:
            state = await self.create_stock_state(stock_code)
        
        state.error_count += 1
        state.updated_time = time.time()
        
        # 根据错误次数更新状态
        if state.error_count >= 5:
            state.status = 'error'
        elif state.error_count >= 3:
            state.status = 'paused'
        
        await self.set_stock_state(state)
        return state.error_count
    
    async def reset_error_count(self, stock_code: str) -> bool:
        """重置错误计数"""
        state = await self.get_stock_state(stock_code)
        if state is None:
            return False
        
        state.error_count = 0
        state.status = 'active'
        state.updated_time = time.time()
        
        return await self.set_stock_state(state)
    
    async def set_stock_status(self, stock_code: str, status: str) -> bool:
        """设置股票状态"""
        state = await self.get_stock_state(stock_code)
        if state is None:
            state = await self.create_stock_state(stock_code)
        
        state.status = status
        state.updated_time = time.time()
        
        return await self.set_stock_state(state)
    
    async def get_active_stocks(self) -> List[str]:
        """获取活跃股票列表"""
        active_stocks = []
        
        # 从缓存获取
        for stock_code, state in self._state_cache.items():
            if state.status == 'active':
                active_stocks.append(stock_code)
        
        return active_stocks
    
    async def get_error_stocks(self) -> List[str]:
        """获取错误状态的股票列表"""
        error_stocks = []
        
        for stock_code, state in self._state_cache.items():
            if state.status == 'error':
                error_stocks.append(stock_code)
        
        return error_stocks
    
    async def cleanup_old_states(self, days: int = 30):
        """清理旧状态数据"""
        cutoff_time = time.time() - (days * 24 * 3600)
        cleaned_count = 0
        
        try:
            if self.storage_type == 'redis':
                # Redis自动过期，不需要手动清理
                pass
            elif self.storage_type == 'file':
                cleaned_count = await self._cleanup_file_states(cutoff_time)
            elif self.storage_type == 'memory':
                cleaned_count = self._cleanup_memory_states(cutoff_time)
            
            if cleaned_count > 0:
                self.logger.info(f"清理了 {cleaned_count} 条过期状态数据")
                
        except Exception as e:
            self.logger.error(f"清理过期状态失败: {e}")
    
    async def _cleanup_file_states(self, cutoff_time: float) -> int:
        """清理文件中的过期状态"""
        def cleanup_file():
            if not self.file_path.exists():
                return 0
            
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            original_count = len(data)
            cleaned_data = {
                k: v for k, v in data.items()
                if v.get('updated_time', 0) > cutoff_time
            }
            
            if len(cleaned_data) < original_count:
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
                
                return original_count - len(cleaned_data)
            
            return 0
        
        return await asyncio.get_event_loop().run_in_executor(
            None, cleanup_file
        )
    
    def _cleanup_memory_states(self, cutoff_time: float) -> int:
        """清理内存中的过期状态"""
        original_count = len(self._memory_storage)
        
        self._memory_storage = {
            k: v for k, v in self._memory_storage.items()
            if v.updated_time > cutoff_time
        }
        
        return original_count - len(self._memory_storage)
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            'total_stocks': len(self._state_cache),
            'active_stocks': 0,
            'paused_stocks': 0,
            'error_stocks': 0,
            'storage_type': self.storage_type
        }
        
        for state in self._state_cache.values():
            if state.status == 'active':
                stats['active_stocks'] += 1
            elif state.status == 'paused':
                stats['paused_stocks'] += 1
            elif state.status == 'error':
                stats['error_stocks'] += 1
        
        return stats
    
    async def cleanup(self):
        """清理资源"""
        try:
            # 保存缓存中的数据
            if self._cache_updated and self.storage_type == 'file':
                for state in self._state_cache.values():
                    await self._set_stock_state_file(state)
            
            # 关闭Redis连接
            if self._redis:
                await self._redis.close()
                
            self.logger.info("状态跟踪器资源清理完成")
            
        except Exception as e:
            self.logger.error(f"资源清理失败: {e}")