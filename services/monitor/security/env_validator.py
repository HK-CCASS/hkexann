"""
环境变量验证器

这个模块负责验证环境变量配置，确保系统在启动时有正确的环境变量设置。
提供环境变量的验证、警告和自动修复建议功能。

主要功能：
- 验证必需的环境变量
- 检查环境变量格式和有效性
- 生成环境变量配置建议
- 创建示例 .env 文件

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import os
import logging
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import re

# 配置日志
# 配置日志（如果没有已配置的handler）
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """验证级别枚举"""
    REQUIRED = "必需"
    RECOMMENDED = "推荐"
    OPTIONAL = "可选"


@dataclass
class EnvVarConfig:
    """环境变量配置数据类"""
    name: str
    description: str
    level: ValidationLevel
    default_value: Optional[str] = None
    validation_pattern: Optional[str] = None
    example_value: Optional[str] = None
    security_sensitive: bool = False


class EnvironmentValidator:
    """
    环境变量验证器
    
    负责验证系统所需的环境变量配置，确保系统能够安全运行。
    """
    
    def __init__(self):
        """初始化环境变量验证器"""
        self.env_configs = self._define_env_configs()
        self.validation_results = []
        
    def _define_env_configs(self) -> Dict[str, EnvVarConfig]:
        """
        定义所有环境变量配置
        
        Returns:
            Dict[str, EnvVarConfig]: 环境变量配置映射
        """
        configs = {}
        
        # SiliconFlow API配置
        configs['SILICONFLOW_API_KEY'] = EnvVarConfig(
            name='SILICONFLOW_API_KEY',
            description='SiliconFlow API访问密钥，用于LLM和嵌入模型服务',
            level=ValidationLevel.REQUIRED,
            validation_pattern=r'^sk-[a-zA-Z0-9]{32,}$',
            example_value='sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            security_sensitive=True
        )
        
        configs['SILICONFLOW_API_BASE'] = EnvVarConfig(
            name='SILICONFLOW_API_BASE',
            description='SiliconFlow API基础URL',
            level=ValidationLevel.OPTIONAL,
            default_value='https://api.siliconflow.cn/v1',
            validation_pattern=r'^https?://[a-zA-Z0-9.-]+(/.*)?$',
            example_value='https://api.siliconflow.cn/v1'
        )
        
        # 数据库配置
        configs['CLICKHOUSE_HOST'] = EnvVarConfig(
            name='CLICKHOUSE_HOST',
            description='ClickHouse数据库主机地址',
            level=ValidationLevel.RECOMMENDED,
            default_value='localhost',
            validation_pattern=r'^[a-zA-Z0-9.-]+$',
            example_value='localhost'
        )
        
        configs['CLICKHOUSE_PASSWORD'] = EnvVarConfig(
            name='CLICKHOUSE_PASSWORD',
            description='ClickHouse数据库密码',
            level=ValidationLevel.REQUIRED,
            validation_pattern=r'^.{6,}$',  # 至少6个字符
            example_value='your-secure-password',
            security_sensitive=True
        )
        
        configs['CCASS_HOST'] = EnvVarConfig(
            name='CCASS_HOST',
            description='CCASS数据库主机地址',
            level=ValidationLevel.RECOMMENDED,
            validation_pattern=r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$|^[a-zA-Z0-9.-]+$',
            example_value='192.168.6.207'
        )
        
        configs['CCASS_PASSWORD'] = EnvVarConfig(
            name='CCASS_PASSWORD',
            description='CCASS数据库密码',
            level=ValidationLevel.REQUIRED,
            validation_pattern=r'^.{1,}$',
            example_value='your-ccass-password',
            security_sensitive=True
        )
        
        configs['DB_PASSWORD'] = EnvVarConfig(
            name='DB_PASSWORD',
            description='MySQL数据库密码（用于config.yaml）',
            level=ValidationLevel.RECOMMENDED,
            validation_pattern=r'^.{1,}$',
            example_value='your-mysql-password',
            security_sensitive=True
        )
        
        # Milvus配置
        configs['MILVUS_HOST'] = EnvVarConfig(
            name='MILVUS_HOST',
            description='Milvus向量数据库主机地址',
            level=ValidationLevel.OPTIONAL,
            default_value='localhost',
            validation_pattern=r'^[a-zA-Z0-9.-]+$',
            example_value='localhost'
        )
        
        configs['MILVUS_PASSWORD'] = EnvVarConfig(
            name='MILVUS_PASSWORD',
            description='Milvus数据库密码（如果启用认证）',
            level=ValidationLevel.OPTIONAL,
            validation_pattern=r'^.{6,}$',
            example_value='milvus-password',
            security_sensitive=True
        )
        
        # Redis配置
        configs['REDIS_PASSWORD'] = EnvVarConfig(
            name='REDIS_PASSWORD',
            description='Redis数据库密码（如果启用认证）',
            level=ValidationLevel.OPTIONAL,
            validation_pattern=r'^.{6,}$',
            example_value='redis-password',
            security_sensitive=True
        )
        
        # 系统配置
        configs['LOG_LEVEL'] = EnvVarConfig(
            name='LOG_LEVEL',
            description='系统日志级别',
            level=ValidationLevel.OPTIONAL,
            default_value='INFO',
            validation_pattern=r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$',
            example_value='INFO'
        )
        
        configs['API_PORT'] = EnvVarConfig(
            name='API_PORT',
            description='API服务器端口',
            level=ValidationLevel.OPTIONAL,
            default_value='8168',
            validation_pattern=r'^[0-9]{1,5}$',
            example_value='8168'
        )
        
        return configs

    def validate_all(self) -> Dict[str, Any]:
        """
        验证所有环境变量
        
        Returns:
            Dict[str, Any]: 验证结果报告
        """
        logger.info("开始环境变量验证...")
        
        results = {
            'valid': True,
            'required_missing': [],
            'recommended_missing': [],
            'invalid_format': [],
            'security_warnings': [],
            'suggestions': []
        }
        
        for env_name, config in self.env_configs.items():
            validation_result = self._validate_single_env(env_name, config)
            
            if not validation_result['exists']:
                if config.level == ValidationLevel.REQUIRED:
                    results['required_missing'].append({
                        'name': env_name,
                        'description': config.description,
                        'example': config.example_value
                    })
                    results['valid'] = False
                elif config.level == ValidationLevel.RECOMMENDED:
                    results['recommended_missing'].append({
                        'name': env_name,
                        'description': config.description,
                        'default': config.default_value,
                        'example': config.example_value
                    })
            
            elif not validation_result['valid_format']:
                results['invalid_format'].append({
                    'name': env_name,
                    'description': config.description,
                    'current_value': validation_result['masked_value'],
                    'pattern': config.validation_pattern
                })
                if config.level == ValidationLevel.REQUIRED:
                    results['valid'] = False
            
            # 安全检查
            if config.security_sensitive and validation_result['exists']:
                security_check = self._check_security(env_name, validation_result['value'])
                if security_check:
                    results['security_warnings'].append(security_check)
        
        # 生成建议
        results['suggestions'] = self._generate_suggestions(results)
        
        logger.info(f"环境变量验证完成，发现 {len(results['required_missing'])} 个必需缺失项")
        return results

    def _validate_single_env(self, env_name: str, config: EnvVarConfig) -> Dict[str, Any]:
        """
        验证单个环境变量
        
        Args:
            env_name: 环境变量名
            config: 环境变量配置
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        value = os.environ.get(env_name)
        
        result = {
            'exists': value is not None,
            'value': value,
            'masked_value': self._mask_value(value, config.security_sensitive),
            'valid_format': True
        }
        
        if value is not None and config.validation_pattern:
            result['valid_format'] = bool(re.match(config.validation_pattern, value))
        
        return result

    def _check_security(self, env_name: str, value: str) -> Optional[Dict[str, Any]]:
        """
        检查敏感环境变量的安全性
        
        Args:
            env_name: 环境变量名
            value: 环境变量值
            
        Returns:
            Optional[Dict[str, Any]]: 安全警告信息
        """
        warnings = []
        
        # 检查弱密码
        if 'PASSWORD' in env_name:
            if len(value) < 8:
                warnings.append("密码长度少于8位，建议使用更强的密码")
            if value.lower() in ['123456', 'password', 'admin', 'root']:
                warnings.append("使用了常见弱密码，存在安全风险")
            if not re.search(r'[A-Z]', value) or not re.search(r'[a-z]', value) or not re.search(r'[0-9]', value):
                warnings.append("密码强度较弱，建议包含大小写字母和数字")
        
        # 检查API密钥
        if 'API_KEY' in env_name:
            if len(value) < 20:
                warnings.append("API密钥长度过短，可能无效")
        
        if warnings:
            return {
                'env_name': env_name,
                'warnings': warnings,
                'masked_value': self._mask_value(value, True)
            }
        
        return None

    def _mask_value(self, value: Optional[str], is_sensitive: bool) -> str:
        """
        掩码敏感值
        
        Args:
            value: 原始值
            is_sensitive: 是否为敏感信息
            
        Returns:
            str: 掩码后的值
        """
        if value is None:
            return "未设置"
        
        if not is_sensitive:
            return value
        
        if len(value) <= 8:
            return '*' * len(value)
        
        return value[:3] + '*' * (len(value) - 6) + value[-3:]

    def _generate_suggestions(self, results: Dict[str, Any]) -> List[str]:
        """
        生成配置建议
        
        Args:
            results: 验证结果
            
        Returns:
            List[str]: 建议列表
        """
        suggestions = []
        
        if results['required_missing']:
            suggestions.append("🚨 立即设置所有必需的环境变量，系统无法正常启动")
        
        if results['recommended_missing']:
            suggestions.append("⚠️ 设置推荐的环境变量以获得最佳性能")
        
        if results['invalid_format']:
            suggestions.append("🔧 修正格式错误的环境变量")
        
        if results['security_warnings']:
            suggestions.append("🔒 处理安全警告，使用更强的密码和密钥")
        
        suggestions.extend([
            "💡 创建 .env 文件统一管理环境变量",
            "📋 在生产环境中使用密钥管理服务",
            "🔄 定期轮换敏感凭据",
            "📝 为团队成员提供环境变量配置文档"
        ])
        
        return suggestions

    def create_env_template(self, output_path: Path) -> bool:
        """
        创建环境变量模板文件
        
        Args:
            output_path: 输出文件路径
            
        Returns:
            bool: 是否成功创建
        """
        try:
            template_content = self._generate_env_template()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(template_content)
            
            logger.info(f"环境变量模板已创建: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"创建环境变量模板失败: {e}")
            return False

    def _generate_env_template(self) -> str:
        """
        生成环境变量模板内容
        
        Returns:
            str: 模板内容
        """
        lines = [
            "# HKEX公告系统环境变量配置文件",
            "# 请复制此文件为 .env 并设置实际值",
            "# 注意：请不要将包含真实密钥的 .env 文件提交到版本控制系统",
            "",
            "# ============================================",
            "# SiliconFlow API 配置（必需）",
            "# ============================================",
            "",
        ]
        
        # 按类别组织环境变量
        categories = {
            'SiliconFlow API': ['SILICONFLOW_API_KEY', 'SILICONFLOW_API_BASE'],
            'ClickHouse 数据库': ['CLICKHOUSE_HOST', 'CLICKHOUSE_PASSWORD'],
            'CCASS 数据库': ['CCASS_HOST', 'CCASS_PASSWORD'],
            'MySQL 数据库': ['DB_PASSWORD'],
            'Milvus 向量数据库': ['MILVUS_HOST', 'MILVUS_PASSWORD'],
            'Redis 缓存': ['REDIS_PASSWORD'],
            '系统配置': ['LOG_LEVEL', 'API_PORT']
        }
        
        for category, env_names in categories.items():
            lines.extend([
                f"# {category}",
                "# " + "=" * (len(category) + 2),
                ""
            ])
            
            for env_name in env_names:
                if env_name in self.env_configs:
                    config = self.env_configs[env_name]
                    level_indicator = "（必需）" if config.level == ValidationLevel.REQUIRED else "（推荐）" if config.level == ValidationLevel.RECOMMENDED else "（可选）"
                    
                    lines.extend([
                        f"# {config.description} {level_indicator}",
                        f"# 示例值: {config.example_value or config.default_value or '请设置实际值'}",
                    ])
                    
                    if config.level == ValidationLevel.REQUIRED:
                        lines.append(f"{env_name}=")
                    else:
                        lines.append(f"# {env_name}={config.default_value or config.example_value or ''}")
                    
                    lines.append("")
            
            lines.append("")
        
        # 添加使用说明
        lines.extend([
            "# ============================================",
            "# 使用说明",
            "# ============================================",
            "#",
            "# 1. 将此文件复制为 .env：cp .env.template .env",
            "# 2. 编辑 .env 文件，设置实际的密钥和密码",
            "# 3. 确保 .env 文件在 .gitignore 中，避免泄露敏感信息",
            "# 4. 在生产环境中，建议使用密钥管理服务而非 .env 文件",
            "#",
            "# 安全提醒：",
            "# - 使用强密码（至少8位，包含大小写字母、数字和特殊字符）",
            "# - 定期轮换API密钥和数据库密码",
            "# - 不要在代码中硬编码敏感信息",
            ""
        ])
        
        return "\n".join(lines)

    def print_validation_report(self, results: Dict[str, Any]) -> None:
        """
        打印格式化的验证报告
        
        Args:
            results: 验证结果
        """
        print("\n" + "="*80)
        print("🔐 HKEX系统环境变量验证报告")
        print("="*80)
        
        # 总体状态
        status_emoji = "✅" if results['valid'] else "❌"
        print(f"\n{status_emoji} 系统状态: {'配置正确' if results['valid'] else '配置不完整'}")
        
        # 必需环境变量
        if results['required_missing']:
            print(f"\n🚨 缺少必需环境变量 ({len(results['required_missing'])} 个):")
            for item in results['required_missing']:
                print(f"  • {item['name']}")
                print(f"    描述: {item['description']}")
                print(f"    示例: {item['example']}")
                print()
        
        # 推荐环境变量
        if results['recommended_missing']:
            print(f"\n⚠️ 缺少推荐环境变量 ({len(results['recommended_missing'])} 个):")
            for item in results['recommended_missing']:
                print(f"  • {item['name']}")
                print(f"    描述: {item['description']}")
                if item['default']:
                    print(f"    默认值: {item['default']}")
                if item['example']:
                    print(f"    示例: {item['example']}")
                print()
        
        # 格式错误
        if results['invalid_format']:
            print(f"\n🔧 格式错误的环境变量 ({len(results['invalid_format'])} 个):")
            for item in results['invalid_format']:
                print(f"  • {item['name']}")
                print(f"    当前值: {item['current_value']}")
                print(f"    要求格式: {item['pattern']}")
                print()
        
        # 安全警告
        if results['security_warnings']:
            print(f"\n🔒 安全警告 ({len(results['security_warnings'])} 个):")
            for warning in results['security_warnings']:
                print(f"  • {warning['env_name']} ({warning['masked_value']})")
                for w in warning['warnings']:
                    print(f"    - {w}")
                print()
        
        # 建议
        print("💡 配置建议:")
        for suggestion in results['suggestions']:
            print(f"  {suggestion}")
        
        print("\n" + "="*80)


def main():
    """主函数 - 运行环境变量验证"""
    validator = EnvironmentValidator()
    
    # 执行验证
    results = validator.validate_all()
    
    # 打印报告
    validator.print_validation_report(results)
    
    # 创建环境变量模板
    project_root = Path('/Users/ericp/PycharmProjects/hkexann')
    template_path = project_root / '.env.template'
    
    if validator.create_env_template(template_path):
        print(f"\n📄 环境变量模板已创建: {template_path}")
        print("   请复制为 .env 文件并设置实际值")
    
    # 返回验证状态
    return results['valid']


if __name__ == "__main__":
    is_valid = main()
    exit(0 if is_valid else 1)  # 验证失败则退出码为1
