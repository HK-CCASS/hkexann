"""
优雅降级和维护模式管理器

这个模块实现了系统的优雅降级机制和维护模式管理，确保在服务异常或维护期间
系统能够继续提供基本功能，而不是完全停止服务。

主要功能：
- 多级服务降级策略
- 自动和手动维护模式切换
- 降级服务质量监控
- 服务恢复自动检测

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from datetime import datetime, timedelta
import json
from pathlib import Path

# 配置日志
# 配置日志（如果没有已配置的handler）
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ServiceLevel(IntEnum):
    """服务等级枚举（数值越小等级越高）"""
    FULL = 1        # 完整服务
    ENHANCED = 2    # 增强服务
    STANDARD = 3    # 标准服务
    BASIC = 4       # 基础服务
    MINIMAL = 5     # 最小服务
    EMERGENCY = 6   # 紧急服务


class DegradationTrigger(Enum):
    """降级触发器枚举"""
    ERROR_RATE = "error_rate"           # 错误率触发
    RESPONSE_TIME = "response_time"     # 响应时间触发
    RESOURCE_USAGE = "resource_usage"   # 资源使用率触发
    EXTERNAL_API = "external_api"      # 外部API故障触发
    MANUAL = "manual"                   # 手动触发
    MAINTENANCE = "maintenance"         # 维护模式触发


class MaintenanceType(Enum):
    """维护类型枚举"""
    SCHEDULED = "scheduled"         # 计划维护
    EMERGENCY = "emergency"         # 紧急维护
    UPGRADE = "upgrade"            # 升级维护
    HOTFIX = "hotfix"              # 热修复


@dataclass
class DegradationRule:
    """降级规则"""
    name: str
    trigger: DegradationTrigger
    threshold: float                    # 触发阈值
    target_level: ServiceLevel          # 目标服务等级
    duration_minutes: int = 0           # 持续时间（0表示直到手动恢复）
    auto_recovery: bool = True          # 是否自动恢复
    recovery_threshold: float = 0.0     # 恢复阈值
    description: str = ""


@dataclass
class ServiceRegistry:
    """服务注册表"""
    service_name: str
    current_level: ServiceLevel = ServiceLevel.FULL
    handlers: Dict[ServiceLevel, Callable] = field(default_factory=dict)
    fallback_data: Dict[str, Any] = field(default_factory=dict)
    last_health_check: Optional[datetime] = None
    health_status: bool = True


@dataclass
class MaintenanceWindow:
    """维护窗口"""
    start_time: datetime
    end_time: datetime
    maintenance_type: MaintenanceType
    affected_services: List[str]
    description: str = ""
    auto_start: bool = True
    auto_end: bool = True


class GracefulDegradationManager:
    """
    优雅降级管理器
    
    实现系统的优雅降级和维护模式管理：
    1. 多级服务降级策略
    2. 自动降级触发和恢复
    3. 维护模式调度
    4. 服务质量监控
    """
    
    def __init__(self, config_file: str = None):
        """
        初始化降级管理器
        
        Args:
            config_file: 配置文件路径
        """
        # 服务注册表
        self.services: Dict[str, ServiceRegistry] = {}
        
        # 降级规则
        self.degradation_rules: List[DegradationRule] = []
        
        # 维护窗口
        self.maintenance_windows: List[MaintenanceWindow] = []
        
        # 当前系统状态
        self.global_service_level = ServiceLevel.FULL
        self.maintenance_mode = False
        self.current_maintenance: Optional[MaintenanceWindow] = None
        
        # 监控指标
        self.metrics_history: Dict[str, List[Dict[str, Any]]] = {}
        
        # 降级历史
        self.degradation_history: List[Dict[str, Any]] = []
        
        # 配置文件
        self.config_file = config_file or "hkexann/degradation_config.json"
        
        # 加载配置和初始化
        self._load_configuration()
        self._initialize_default_rules()
        
        # 启动监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        
        logger.info("🛟 优雅降级管理器初始化完成")

    def _load_configuration(self):
        """加载配置"""
        try:
            config_path = Path(self.config_file)
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    
                # 加载维护窗口
                if 'maintenance_windows' in config:
                    for window_config in config['maintenance_windows']:
                        window = MaintenanceWindow(
                            start_time=datetime.fromisoformat(window_config['start_time']),
                            end_time=datetime.fromisoformat(window_config['end_time']),
                            maintenance_type=MaintenanceType(window_config['type']),
                            affected_services=window_config['affected_services'],
                            description=window_config.get('description', '')
                        )
                        self.maintenance_windows.append(window)
                
                logger.info(f"配置加载成功: {len(self.maintenance_windows)} 个维护窗口")
        except Exception as e:
            logger.warning(f"配置加载失败: {e}")

    def _initialize_default_rules(self):
        """初始化默认降级规则"""
        self.degradation_rules = [
            DegradationRule(
                name="高错误率降级",
                trigger=DegradationTrigger.ERROR_RATE,
                threshold=0.2,  # 20%错误率
                target_level=ServiceLevel.BASIC,
                duration_minutes=10,
                recovery_threshold=0.05,  # 5%错误率恢复
                description="当错误率超过20%时降级到基础服务"
            ),
            DegradationRule(
                name="响应时间过长降级", 
                trigger=DegradationTrigger.RESPONSE_TIME,
                threshold=10.0,  # 10秒响应时间
                target_level=ServiceLevel.STANDARD,
                duration_minutes=5,
                recovery_threshold=3.0,  # 3秒恢复
                description="当响应时间超过10秒时降级到标准服务"
            ),
            DegradationRule(
                name="资源不足降级",
                trigger=DegradationTrigger.RESOURCE_USAGE,
                threshold=0.9,  # 90%资源使用率
                target_level=ServiceLevel.MINIMAL,
                duration_minutes=15,
                recovery_threshold=0.7,  # 70%恢复
                description="当资源使用率超过90%时降级到最小服务"
            ),
            DegradationRule(
                name="外部API故障降级",
                trigger=DegradationTrigger.EXTERNAL_API,
                threshold=0.5,  # 50%外部API不可用
                target_level=ServiceLevel.BASIC,
                duration_minutes=20,
                recovery_threshold=0.1,  # 10%故障率恢复
                description="当外部API故障率超过50%时降级到基础服务"
            )
        ]

    def register_service(self, service_name: str, 
                        service_handlers: Dict[ServiceLevel, Callable] = None,
                        fallback_data: Dict[str, Any] = None):
        """
        注册服务
        
        Args:
            service_name: 服务名称
            service_handlers: 各服务等级的处理器
            fallback_data: 降级时使用的备用数据
        """
        registry = ServiceRegistry(
            service_name=service_name,
            handlers=service_handlers or {},
            fallback_data=fallback_data or {}
        )
        
        self.services[service_name] = registry
        logger.info(f"注册服务: {service_name}")

    def register_handler(self, service_name: str, 
                        service_level: ServiceLevel,
                        handler: Callable):
        """
        注册服务等级处理器
        
        Args:
            service_name: 服务名称
            service_level: 服务等级
            handler: 处理器函数
        """
        if service_name not in self.services:
            self.register_service(service_name)
        
        self.services[service_name].handlers[service_level] = handler
        logger.info(f"注册处理器: {service_name} - {service_level.name}")

    async def call_service(self, service_name: str, 
                          *args, **kwargs) -> Any:
        """
        调用服务（按当前服务等级）
        
        Args:
            service_name: 服务名称
            *args, **kwargs: 服务参数
            
        Returns:
            服务调用结果
        """
        if service_name not in self.services:
            raise ValueError(f"未注册的服务: {service_name}")
        
        registry = self.services[service_name]
        current_level = registry.current_level
        
        # 如果处于维护模式，强制使用最小服务等级
        if self.maintenance_mode:
            current_level = ServiceLevel.EMERGENCY
        
        # 查找可用的处理器
        handler = None
        for level in range(current_level, ServiceLevel.EMERGENCY + 1):
            service_level = ServiceLevel(level)
            if service_level in registry.handlers:
                handler = registry.handlers[service_level]
                break
        
        if handler is None:
            # 使用备用数据
            logger.warning(f"服务 {service_name} 无可用处理器，返回备用数据")
            return registry.fallback_data
        
        try:
            # 调用处理器
            if asyncio.iscoroutinefunction(handler):
                result = await handler(*args, **kwargs)
            else:
                result = handler(*args, **kwargs)
            
            # 更新健康状态
            registry.last_health_check = datetime.now()
            registry.health_status = True
            
            return result
            
        except Exception as e:
            logger.error(f"服务调用失败 {service_name}: {e}")
            
            # 更新健康状态
            registry.health_status = False
            
            # 尝试降级
            await self._try_service_degradation(service_name, e)
            
            # 返回备用数据
            return registry.fallback_data

    async def _try_service_degradation(self, service_name: str, error: Exception):
        """尝试服务降级"""
        if service_name not in self.services:
            return
        
        registry = self.services[service_name]
        current_level = registry.current_level
        
        # 如果已经是最低等级，无法再降级
        if current_level >= ServiceLevel.EMERGENCY:
            return
        
        # 降级一个等级
        new_level = ServiceLevel(current_level + 1)
        await self.degrade_service(service_name, new_level, 
                                  DegradationTrigger.ERROR_RATE,
                                  f"服务错误自动降级: {error}")

    async def degrade_service(self, service_name: str, 
                             target_level: ServiceLevel,
                             trigger: DegradationTrigger,
                             reason: str = ""):
        """
        降级服务
        
        Args:
            service_name: 服务名称
            target_level: 目标服务等级
            trigger: 降级触发器
            reason: 降级原因
        """
        if service_name not in self.services:
            logger.warning(f"尝试降级未注册的服务: {service_name}")
            return
        
        registry = self.services[service_name]
        old_level = registry.current_level
        
        if target_level >= old_level:
            registry.current_level = target_level
            
            # 记录降级事件
            degradation_event = {
                'timestamp': datetime.now().isoformat(),
                'service': service_name,
                'old_level': old_level.name,
                'new_level': target_level.name,
                'trigger': trigger.value,
                'reason': reason
            }
            
            self.degradation_history.append(degradation_event)
            
            # 更新全局服务等级
            self._update_global_service_level()
            
            logger.warning(f"服务降级: {service_name} {old_level.name} -> {target_level.name} ({reason})")

    async def restore_service(self, service_name: str, 
                             target_level: ServiceLevel = ServiceLevel.FULL):
        """
        恢复服务
        
        Args:
            service_name: 服务名称
            target_level: 目标服务等级
        """
        if service_name not in self.services:
            return
        
        registry = self.services[service_name]
        old_level = registry.current_level
        
        if target_level < old_level:
            registry.current_level = target_level
            
            # 记录恢复事件
            recovery_event = {
                'timestamp': datetime.now().isoformat(),
                'service': service_name,
                'old_level': old_level.name,
                'new_level': target_level.name,
                'action': 'restore'
            }
            
            self.degradation_history.append(recovery_event)
            
            # 更新全局服务等级
            self._update_global_service_level()
            
            logger.info(f"服务恢复: {service_name} {old_level.name} -> {target_level.name}")

    def _update_global_service_level(self):
        """更新全局服务等级"""
        if not self.services:
            return
        
        # 取所有服务的最低等级作为全局等级
        min_level = min(registry.current_level for registry in self.services.values())
        self.global_service_level = min_level

    async def enter_maintenance_mode(self, maintenance_type: MaintenanceType,
                                   affected_services: List[str] = None,
                                   description: str = ""):
        """
        进入维护模式
        
        Args:
            maintenance_type: 维护类型
            affected_services: 受影响的服务列表
            description: 维护描述
        """
        self.maintenance_mode = True
        
        # 创建维护窗口
        self.current_maintenance = MaintenanceWindow(
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(hours=1),  # 默认1小时
            maintenance_type=maintenance_type,
            affected_services=affected_services or list(self.services.keys()),
            description=description
        )
        
        # 降级受影响的服务
        for service_name in self.current_maintenance.affected_services:
            if service_name in self.services:
                await self.degrade_service(
                    service_name,
                    ServiceLevel.EMERGENCY,
                    DegradationTrigger.MAINTENANCE,
                    f"维护模式: {description}"
                )
        
        logger.warning(f"进入维护模式: {maintenance_type.value} - {description}")

    async def exit_maintenance_mode(self):
        """退出维护模式"""
        if not self.maintenance_mode:
            return
        
        self.maintenance_mode = False
        
        # 恢复受影响的服务
        if self.current_maintenance:
            for service_name in self.current_maintenance.affected_services:
                if service_name in self.services:
                    await self.restore_service(service_name, ServiceLevel.FULL)
        
        self.current_maintenance = None
        
        logger.info("退出维护模式，服务已恢复")

    def schedule_maintenance(self, window: MaintenanceWindow):
        """
        调度维护窗口
        
        Args:
            window: 维护窗口
        """
        self.maintenance_windows.append(window)
        logger.info(f"调度维护窗口: {window.start_time} - {window.end_time}")

    async def start_monitoring(self):
        """启动监控"""
        if self._monitoring_task is None:
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())

    async def stop_monitoring(self):
        """停止监控"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            self._monitoring_task = None

    async def _monitoring_loop(self):
        """监控循环"""
        while True:
            try:
                # 检查维护窗口
                await self._check_maintenance_windows()
                
                # 检查服务健康
                await self._check_service_health()
                
                # 检查降级规则
                await self._check_degradation_rules()
                
                await asyncio.sleep(30)  # 30秒检查一次
                
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(60)

    async def _check_maintenance_windows(self):
        """检查维护窗口"""
        current_time = datetime.now()
        
        for window in self.maintenance_windows[:]:  # 复制列表避免修改时出错
            # 检查是否需要开始维护
            if (not self.maintenance_mode and 
                window.auto_start and 
                current_time >= window.start_time and 
                current_time < window.end_time):
                
                await self.enter_maintenance_mode(
                    window.maintenance_type,
                    window.affected_services,
                    window.description
                )
                
            # 检查是否需要结束维护
            elif (self.maintenance_mode and 
                  window.auto_end and 
                  current_time >= window.end_time):
                
                await self.exit_maintenance_mode()
                self.maintenance_windows.remove(window)

    async def _check_service_health(self):
        """检查服务健康状态"""
        for service_name, registry in self.services.items():
            # 检查健康检查时间
            if registry.last_health_check:
                time_since_check = datetime.now() - registry.last_health_check
                if time_since_check > timedelta(minutes=5):  # 5分钟无响应
                    if registry.health_status:
                        registry.health_status = False
                        await self.degrade_service(
                            service_name,
                            ServiceLevel.BASIC,
                            DegradationTrigger.ERROR_RATE,
                            "服务无响应"
                        )

    async def _check_degradation_rules(self):
        """检查降级规则"""
        # 这里可以根据实际的指标数据检查降级规则
        # 由于没有实际的指标系统，这里只做示例
        pass

    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态
        
        Returns:
            Dict[str, Any]: 系统状态信息
        """
        # 服务状态统计
        service_levels = {}
        healthy_services = 0
        
        for service_name, registry in self.services.items():
            service_levels[service_name] = {
                'current_level': registry.current_level.name,
                'health_status': registry.health_status,
                'last_check': registry.last_health_check.isoformat() if registry.last_health_check else None
            }
            
            if registry.health_status:
                healthy_services += 1
        
        # 最近的降级事件
        recent_events = self.degradation_history[-10:] if self.degradation_history else []
        
        return {
            'global_service_level': self.global_service_level.name,
            'maintenance_mode': self.maintenance_mode,
            'current_maintenance': {
                'type': self.current_maintenance.maintenance_type.value,
                'description': self.current_maintenance.description,
                'affected_services': self.current_maintenance.affected_services
            } if self.current_maintenance else None,
            'service_summary': {
                'total_services': len(self.services),
                'healthy_services': healthy_services,
                'degraded_services': sum(1 for r in self.services.values() if r.current_level > ServiceLevel.FULL)
            },
            'services': service_levels,
            'scheduled_maintenance': len(self.maintenance_windows),
            'recent_events': recent_events
        }


if __name__ == "__main__":
    # 测试模块
    async def test_degradation_manager():
        """测试优雅降级管理器"""
        
        print("\n" + "="*70)
        print("🛟 优雅降级管理器测试")
        print("="*70)
        
        # 创建管理器
        manager = GracefulDegradationManager()
        
        # 模拟服务处理器
        def full_service_handler(*args, **kwargs):
            return {"level": "full", "data": "完整数据"}
        
        def basic_service_handler(*args, **kwargs):
            return {"level": "basic", "data": "基础数据"}
        
        def emergency_service_handler(*args, **kwargs):
            return {"level": "emergency", "data": "紧急数据"}
        
        # 注册测试服务
        print("\n📝 注册测试服务...")
        manager.register_service("api_service", fallback_data={"data": "备用数据"})
        manager.register_handler("api_service", ServiceLevel.FULL, full_service_handler)
        manager.register_handler("api_service", ServiceLevel.BASIC, basic_service_handler)
        manager.register_handler("api_service", ServiceLevel.EMERGENCY, emergency_service_handler)
        
        # 测试正常服务调用
        print(f"\n🔄 测试正常服务调用...")
        result = await manager.call_service("api_service")
        print(f"正常调用结果: {result}")
        
        # 测试服务降级
        print(f"\n📉 测试服务降级...")
        await manager.degrade_service("api_service", ServiceLevel.BASIC, 
                                    DegradationTrigger.ERROR_RATE, "测试降级")
        
        result = await manager.call_service("api_service")
        print(f"降级后调用结果: {result}")
        
        # 测试维护模式
        print(f"\n🔧 测试维护模式...")
        await manager.enter_maintenance_mode(MaintenanceType.SCHEDULED, 
                                           ["api_service"], "测试维护")
        
        result = await manager.call_service("api_service")
        print(f"维护模式调用结果: {result}")
        
        # 退出维护模式
        print(f"\n✅ 退出维护模式...")
        await manager.exit_maintenance_mode()
        
        result = await manager.call_service("api_service")
        print(f"恢复后调用结果: {result}")
        
        # 测试维护调度
        print(f"\n⏰ 测试维护调度...")
        future_time = datetime.now() + timedelta(seconds=5)
        maintenance_window = MaintenanceWindow(
            start_time=future_time,
            end_time=future_time + timedelta(seconds=10),
            maintenance_type=MaintenanceType.UPGRADE,
            affected_services=["api_service"],
            description="自动维护测试"
        )
        
        manager.schedule_maintenance(maintenance_window)
        
        # 启动监控（简短测试）
        await manager.start_monitoring()
        await asyncio.sleep(2)  # 等待2秒观察
        await manager.stop_monitoring()
        
        # 显示系统状态
        print(f"\n📊 系统状态:")
        status = manager.get_system_status()
        
        print(f"  全局服务等级: {status['global_service_level']}")
        print(f"  维护模式: {status['maintenance_mode']}")
        print(f"  服务总数: {status['service_summary']['total_services']}")
        print(f"  健康服务: {status['service_summary']['healthy_services']}")
        print(f"  降级服务: {status['service_summary']['degraded_services']}")
        
        if status['recent_events']:
            print(f"  最近事件:")
            for event in status['recent_events'][-3:]:
                print(f"    - {event['action'] if 'action' in event else 'degrade'}: "
                     f"{event['service']} ({event['new_level']})")
        
        print("\n" + "="*70)
    
    # 运行测试
    asyncio.run(test_degradation_manager())
