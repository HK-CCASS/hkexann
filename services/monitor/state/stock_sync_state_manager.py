"""
股票同步状态管理器
负责持久化股票同步状态，避免重启后的"假新增"问题
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Set, Optional, Any
import aiofiles

logger = logging.getLogger(__name__)


class StockSyncStateManager:
    """
    股票同步状态管理器
    
    负责管理和持久化股票同步的状态信息，包括：
    - 是否为首次同步
    - 上次同步的股票列表
    - 同步历史和统计信息
    - 防止重启后重复处理的机制
    
    特性：
    - 异步文件操作，避免阻塞主流程
    - 状态文件格式友好，便于调试和维护
    - 支持状态恢复和验证
    - 自动清理过期状态数据
    """
    
    def __init__(self, state_file_path: str = "hkexann/.stock_sync_state.json"):
        """
        初始化股票同步状态管理器
        
        Args:
            state_file_path: 状态文件路径
        """
        self.state_file = Path(state_file_path)
        self.state_file.parent.mkdir(exist_ok=True)
        
        # 状态数据
        self._is_first_sync = True
        self._previous_stocks: Optional[Set[str]] = None
        self._last_sync_time: Optional[datetime] = None
        self._sync_history: list = []
        self._total_syncs = 0
        
        # 配置
        self.max_history_records = 50  # 最多保留50条同步历史
        self.state_expire_days = 30    # 状态文件30天过期
        
        logger.info(f"初始化股票同步状态管理器，状态文件: {self.state_file}")
    
    async def initialize(self) -> bool:
        """
        初始化状态管理器，从文件恢复状态
        
        Returns:
            bool: 是否成功初始化
        """
        try:
            await self._load_state()
            logger.info("✅ 股票同步状态管理器初始化完成")
            logger.info(f"   首次同步: {self._is_first_sync}")
            logger.info(f"   历史股票数: {len(self._previous_stocks) if self._previous_stocks else 0}")
            logger.info(f"   上次同步: {self._last_sync_time or '从未同步'}")
            logger.info(f"   总同步次数: {self._total_syncs}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 股票同步状态管理器初始化失败: {e}")
            return False
    
    async def _load_state(self):
        """从文件加载状态"""
        try:
            if not self.state_file.exists():
                logger.info("🔍 状态文件不存在，这是首次运行")
                self._is_first_sync = True
                self._previous_stocks = None
                return
            
            async with aiofiles.open(self.state_file, 'r', encoding='utf-8') as f:
                content = await f.read()
                state_data = json.loads(content)
            
            # 检查状态文件是否过期
            last_sync_str = state_data.get('last_sync_time')
            if last_sync_str:
                last_sync_time = datetime.fromisoformat(last_sync_str)
                if datetime.now() - last_sync_time > timedelta(days=self.state_expire_days):
                    logger.warning(f"⚠️ 状态文件已过期（{self.state_expire_days}天），重置为首次同步")
                    self._is_first_sync = True
                    self._previous_stocks = None
                    return
            
            # 恢复状态
            self._is_first_sync = state_data.get('is_first_sync', True)
            
            # 恢复股票列表
            previous_stocks_list = state_data.get('previous_stocks', [])
            self._previous_stocks = set(previous_stocks_list) if previous_stocks_list else None
            
            # 恢复时间信息
            if last_sync_str:
                self._last_sync_time = datetime.fromisoformat(last_sync_str)
            
            # 恢复统计信息
            self._total_syncs = state_data.get('total_syncs', 0)
            self._sync_history = state_data.get('sync_history', [])
            
            logger.info(f"📂 已从状态文件恢复同步状态")
            
        except Exception as e:
            logger.warning(f"⚠️ 加载状态文件失败，使用默认状态: {e}")
            self._is_first_sync = True
            self._previous_stocks = None
    
    async def _save_state(self):
        """保存状态到文件"""
        try:
            state_data = {
                'is_first_sync': self._is_first_sync,
                'previous_stocks': list(self._previous_stocks) if self._previous_stocks else [],
                'last_sync_time': self._last_sync_time.isoformat() if self._last_sync_time else None,
                'total_syncs': self._total_syncs,
                'sync_history': self._sync_history[-self.max_history_records:],  # 只保留最近的记录
                'version': '1.0.0',
                'created_by': 'StockSyncStateManager',
                'last_updated': datetime.now().isoformat()
            }
            
            async with aiofiles.open(self.state_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(state_data, ensure_ascii=False, indent=2))
            
            logger.debug(f"💾 股票同步状态已保存")
            
        except Exception as e:
            logger.error(f"❌ 保存状态文件失败: {e}")
    
    def is_first_sync(self) -> bool:
        """
        检查是否为首次同步
        
        Returns:
            bool: 是否为首次同步
        """
        return self._is_first_sync
    
    def get_previous_stocks(self) -> Optional[Set[str]]:
        """
        获取上次同步的股票列表
        
        Returns:
            Optional[Set[str]]: 上次同步的股票集合，首次同步时返回None
        """
        return self._previous_stocks
    
    async def detect_stock_changes(self, current_stocks: Set[str]) -> Dict[str, Any]:
        """
        检测股票变化
        
        Args:
            current_stocks: 当前发现的股票集合
            
        Returns:
            Dict[str, Any]: 股票变化信息
        """
        start_time = time.time()
        
        changes = {
            'new_stocks': set(),
            'removed_stocks': set(),
            'unchanged_stocks': set(),
            'total_current': len(current_stocks),
            'total_previous': len(self._previous_stocks) if self._previous_stocks else 0,
            'has_changes': False,
            'is_first_sync': self._is_first_sync,
            'detection_time': time.time() - start_time
        }
        
        if self._is_first_sync:
            # 首次同步：所有股票都是新增
            logger.info("🆕 首次股票同步，将所有股票标记为新增")
            changes['new_stocks'] = current_stocks.copy()
            changes['has_changes'] = bool(current_stocks)
            
            # 记录首次同步
            await self._record_sync_event(current_stocks, changes)
            
            logger.info(f"✅ 首次同步发现 {len(current_stocks)} 只股票")
            
        elif self._previous_stocks is not None:
            # 后续同步：比较变化
            previous_stocks = self._previous_stocks
            
            changes['new_stocks'] = current_stocks - previous_stocks
            changes['removed_stocks'] = previous_stocks - current_stocks
            changes['unchanged_stocks'] = current_stocks & previous_stocks
            changes['has_changes'] = bool(changes['new_stocks'] or changes['removed_stocks'])
            
            if changes['has_changes']:
                logger.info(f"📈 检测到股票变化:")
                logger.info(f"   新增: {len(changes['new_stocks'])} 只")
                logger.info(f"   移除: {len(changes['removed_stocks'])} 只")
                logger.info(f"   不变: {len(changes['unchanged_stocks'])} 只")
                
                if changes['new_stocks'] and len(changes['new_stocks']) <= 20:
                    logger.info(f"   📝 新增股票: {sorted(list(changes['new_stocks']))}")
                if changes['removed_stocks'] and len(changes['removed_stocks']) <= 20:
                    logger.info(f"   📝 移除股票: {sorted(list(changes['removed_stocks']))}")
                    
                # 记录变化同步
                await self._record_sync_event(current_stocks, changes)
            else:
                logger.info("📊 股票列表无变化")
                
        else:
            # 异常情况：非首次但没有历史数据
            logger.warning("⚠️ 检测到异常状态：非首次同步但缺少历史基线，重置为首次同步")
            self._is_first_sync = True
            changes['new_stocks'] = current_stocks.copy()
            changes['has_changes'] = bool(current_stocks)
            changes['is_first_sync'] = True
            
            # 记录异常恢复
            await self._record_sync_event(current_stocks, changes, event_type='reset')
        
        return changes
    
    async def update_sync_state(self, current_stocks: Set[str], sync_result: Dict[str, Any] = None):
        """
        更新同步状态
        
        Args:
            current_stocks: 当前股票集合
            sync_result: 同步结果信息（可选）
        """
        try:
            # 更新基本状态
            self._is_first_sync = False
            self._previous_stocks = current_stocks.copy()
            self._last_sync_time = datetime.now()
            self._total_syncs += 1
            
            # 保存状态
            await self._save_state()
            
            logger.info(f"🔄 股票同步状态已更新，当前监控 {len(current_stocks)} 只股票")
            
        except Exception as e:
            logger.error(f"❌ 更新同步状态失败: {e}")
    
    async def _record_sync_event(self, current_stocks: Set[str], changes: Dict[str, Any], event_type: str = 'normal'):
        """
        记录同步事件到历史
        
        Args:
            current_stocks: 当前股票集合
            changes: 变化信息
            event_type: 事件类型 ('normal', 'reset', 'error')
        """
        try:
            event = {
                'timestamp': datetime.now().isoformat(),
                'event_type': event_type,
                'is_first_sync': self._is_first_sync,
                'total_stocks': len(current_stocks),
                'new_stocks_count': len(changes['new_stocks']),
                'removed_stocks_count': len(changes['removed_stocks']),
                'has_changes': changes['has_changes'],
                'detection_time_ms': int(changes.get('detection_time', 0) * 1000)
            }
            
            # 如果新增股票数量不多，记录具体股票代码
            if len(changes['new_stocks']) <= 10:
                event['new_stocks_sample'] = sorted(list(changes['new_stocks']))
            
            self._sync_history.append(event)
            
            # 限制历史记录数量
            if len(self._sync_history) > self.max_history_records:
                self._sync_history = self._sync_history[-self.max_history_records:]
                
        except Exception as e:
            logger.error(f"❌ 记录同步事件失败: {e}")
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """
        获取同步统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {
            'is_first_sync': self._is_first_sync,
            'total_syncs': self._total_syncs,
            'current_stocks_count': len(self._previous_stocks) if self._previous_stocks else 0,
            'last_sync_time': self._last_sync_time.isoformat() if self._last_sync_time else None,
            'history_records_count': len(self._sync_history),
            'state_file_path': str(self.state_file),
            'state_file_exists': self.state_file.exists()
        }
    
    async def reset_state(self, reason: str = "Manual reset"):
        """
        重置同步状态（慎用）
        
        Args:
            reason: 重置原因
        """
        logger.warning(f"⚠️ 重置股票同步状态: {reason}")
        
        # 备份当前状态
        backup_file = self.state_file.parent / f".stock_sync_state_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        if self.state_file.exists():
            import shutil
            shutil.copy2(self.state_file, backup_file)
            logger.info(f"📂 已备份当前状态到: {backup_file}")
        
        # 重置状态
        self._is_first_sync = True
        self._previous_stocks = None
        self._last_sync_time = None
        
        # 记录重置事件
        reset_event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': 'manual_reset',
            'reason': reason,
            'reset_by': 'StockSyncStateManager'
        }
        self._sync_history.append(reset_event)
        
        # 保存状态
        await self._save_state()
        
        logger.info("✅ 股票同步状态已重置")
    
    async def cleanup_old_backups(self, keep_days: int = 7):
        """
        清理旧的备份文件
        
        Args:
            keep_days: 保留天数
        """
        try:
            backup_pattern = ".stock_sync_state_backup_*.json"
            backup_files = list(self.state_file.parent.glob(backup_pattern))
            
            cutoff_time = datetime.now() - timedelta(days=keep_days)
            cleaned_count = 0
            
            for backup_file in backup_files:
                try:
                    # 从文件名提取时间戳
                    timestamp_str = backup_file.stem.split('_')[-2] + '_' + backup_file.stem.split('_')[-1]
                    file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    
                    if file_time < cutoff_time:
                        backup_file.unlink()
                        cleaned_count += 1
                        logger.debug(f"🗑️ 已清理过期备份: {backup_file.name}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ 清理备份文件失败 {backup_file.name}: {e}")
            
            if cleaned_count > 0:
                logger.info(f"🧹 已清理 {cleaned_count} 个过期备份文件")
                
        except Exception as e:
            logger.error(f"❌ 清理备份文件时出错: {e}")
    
    async def close(self):
        """关闭状态管理器，保存最终状态"""
        try:
            await self._save_state()
            await self.cleanup_old_backups()
            logger.info("✅ 股票同步状态管理器已关闭")
            
        except Exception as e:
            logger.error(f"❌ 关闭状态管理器时出错: {e}")


# 便捷函数
async def get_stock_sync_state_manager(state_file_path: str = None) -> StockSyncStateManager:
    """
    获取初始化后的股票同步状态管理器
    
    Args:
        state_file_path: 状态文件路径，None时使用默认路径
        
    Returns:
        StockSyncStateManager: 已初始化的状态管理器
    """
    manager = StockSyncStateManager(state_file_path) if state_file_path else StockSyncStateManager()
    await manager.initialize()
    return manager


# 测试代码
if __name__ == "__main__":
    async def test_stock_sync_state_manager():
        """测试股票同步状态管理器"""
        print("\n" + "="*70)
        print("🧪 测试股票同步状态管理器")
        print("="*70)
        
        # 创建测试用状态管理器
        test_state_file = "test_stock_sync_state.json"
        manager = StockSyncStateManager(test_state_file)
        
        try:
            # 初始化
            await manager.initialize()
            print(f"📊 初始化状态: 首次同步={manager.is_first_sync()}")
            
            # 模拟首次同步
            test_stocks_1 = {'00700', '00939', '01398', '02318'}
            changes_1 = await manager.detect_stock_changes(test_stocks_1)
            print(f"🔍 首次检测: 新增={len(changes_1['new_stocks'])}, 变化={changes_1['has_changes']}")
            
            # 更新状态
            await manager.update_sync_state(test_stocks_1)
            print(f"🔄 状态更新完成: 首次同步={manager.is_first_sync()}")
            
            # 模拟股票变化
            test_stocks_2 = {'00700', '00939', '01398', '02318', '09988', '03690'}  # 新增2只
            changes_2 = await manager.detect_stock_changes(test_stocks_2)
            print(f"🔍 第二次检测: 新增={len(changes_2['new_stocks'])}, 变化={changes_2['has_changes']}")
            
            # 更新状态
            await manager.update_sync_state(test_stocks_2)
            
            # 显示统计信息
            stats = manager.get_sync_statistics()
            print(f"📈 统计信息: 总同步={stats['total_syncs']}, 当前股票数={stats['current_stocks_count']}")
            
            print("\n✅ 测试完成！")
            
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await manager.close()
            
            # 清理测试文件
            import os
            if os.path.exists(test_state_file):
                os.remove(test_state_file)
                print(f"🗑️ 已清理测试文件: {test_state_file}")
    
    # 运行测试
    asyncio.run(test_stock_sync_state_manager())












