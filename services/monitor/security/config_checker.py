"""
安全配置检查器

这个模块负责检测系统配置中的安全风险，特别是硬编码的敏感信息。
用于在系统启动时进行安全扫描，确保生产环境的安全性。

主要功能：
- 检测硬编码的API密钥和密码
- 验证环境变量的使用
- 生成安全风险报告
- 提供安全建议

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import os
import re
import yaml
import logging
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import json

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SecurityRiskLevel(Enum):
    """安全风险级别枚举"""
    LOW = "低风险"
    MEDIUM = "中风险" 
    HIGH = "高风险"
    CRITICAL = "严重风险"


@dataclass
class SecurityIssue:
    """安全问题数据类"""
    level: SecurityRiskLevel
    category: str
    description: str
    location: str
    value: str
    recommendation: str
    is_env_var_available: bool = False


class SecurityConfigChecker:
    """
    安全配置检查器
    
    用于扫描系统配置文件，检测潜在的安全风险，
    特别是硬编码的敏感信息。
    """
    
    def __init__(self):
        """初始化安全检查器"""
        self.issues: List[SecurityIssue] = []
        
        # 敏感信息模式定义
        self.sensitive_patterns = {
            'api_key': {
                'patterns': [
                    r'sk-[a-zA-Z0-9]{32,}',  # SiliconFlow API密钥格式
                    r'api[_-]?key["\']?\s*[:=]\s*["\'][^"\']{10,}["\']',
                    r'access[_-]?token["\']?\s*[:=]\s*["\'][^"\']{10,}["\']'
                ],
                'description': 'API密钥',
                'risk_level': SecurityRiskLevel.CRITICAL
            },
            'password': {
                'patterns': [
                    r'password["\']?\s*[:=]\s*["\'][^"\']{1,}["\']',
                    r'passwd["\']?\s*[:=]\s*["\'][^"\']{1,}["\']',
                    r'pwd["\']?\s*[:=]\s*["\'][^"\']{1,}["\']'
                ],
                'description': '密码',
                'risk_level': SecurityRiskLevel.HIGH
            },
            'database_credentials': {
                'patterns': [
                    r'(host|server)["\']?\s*[:=]\s*["\'][0-9.]{7,15}["\']',
                    r'(user|username)["\']?\s*[:=]\s*["\'][^"\']{1,}["\']'
                ],
                'description': '数据库凭据',
                'risk_level': SecurityRiskLevel.MEDIUM
            },
            'private_key': {
                'patterns': [
                    r'-----BEGIN.*PRIVATE KEY-----',
                    r'private[_-]?key["\']?\s*[:=]\s*["\'][^"\']{10,}["\']'
                ],
                'description': '私钥',
                'risk_level': SecurityRiskLevel.CRITICAL
            }
        }
        
        # 环境变量模式
        self.env_var_patterns = [
            r'\$\{([A-Z_][A-Z0-9_]*)\}',  # ${VAR_NAME}
            r'\$([A-Z_][A-Z0-9_]*)',       # $VAR_NAME
            r'os\.environ\[.?([A-Z_][A-Z0-9_]*).?\]',  # os.environ["VAR"]
            r'os\.getenv\(.?([A-Z_][A-Z0-9_]*).?\)'    # os.getenv("VAR")
        ]

    def check_file(self, file_path: Path) -> List[SecurityIssue]:
        """
        检查单个文件的安全问题
        
        Args:
            file_path: 要检查的文件路径
            
        Returns:
            List[SecurityIssue]: 发现的安全问题列表
        """
        issues = []
        
        if not file_path.exists():
            logger.warning(f"文件不存在: {file_path}")
            return issues
            
        try:
            content = file_path.read_text(encoding='utf-8')
            
            # 检查敏感信息
            for category, config in self.sensitive_patterns.items():
                for pattern in config['patterns']:
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # 检查是否有对应的环境变量
                        env_var_available = self._check_env_var_for_value(
                            content, match.group(0)
                        )
                        
                        issue = SecurityIssue(
                            level=config['risk_level'],
                            category=category,
                            description=f"发现硬编码的{config['description']}",
                            location=f"{file_path}:行{self._get_line_number(content, match.start())}",
                            value=self._mask_sensitive_value(match.group(0)),
                            recommendation=self._get_recommendation(category, env_var_available),
                            is_env_var_available=env_var_available
                        )
                        issues.append(issue)
                        
        except Exception as e:
            logger.error(f"检查文件 {file_path} 时出错: {e}")
            
        return issues

    def check_settings_py(self, settings_path: Path) -> List[SecurityIssue]:
        """
        专门检查settings.py文件的安全问题
        
        Args:
            settings_path: settings.py文件路径
            
        Returns:
            List[SecurityIssue]: 发现的安全问题列表
        """
        issues = []
        
        if not settings_path.exists():
            return issues
            
        content = settings_path.read_text(encoding='utf-8')
        
        # 检查硬编码的SiliconFlow API密钥
        siliconflow_key_pattern = r'siliconflow_api_key:\s*str\s*=\s*["\']([^"\']+)["\']'
        matches = re.finditer(siliconflow_key_pattern, content)
        for match in matches:
            api_key = match.group(1)
            if api_key.startswith('sk-') and len(api_key) > 30:
                issue = SecurityIssue(
                    level=SecurityRiskLevel.CRITICAL,
                    category='siliconflow_api_key',
                    description='发现硬编码的SiliconFlow API密钥',
                    location=f"{settings_path}:行{self._get_line_number(content, match.start())}",
                    value=self._mask_api_key(api_key),
                    recommendation="将API密钥移至环境变量：export SILICONFLOW_API_KEY='your-key'",
                    is_env_var_available='SILICONFLOW_API_KEY' in os.environ
                )
                issues.append(issue)
        
        # 检查硬编码的数据库密码
        db_password_patterns = [
            r'clickhouse_password:\s*str\s*=\s*["\']([^"\']+)["\']',
            r'ccass_password:\s*str\s*=\s*["\']([^"\']+)["\']'
        ]
        
        for pattern in db_password_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                password = match.group(1)
                if password and password != "your-password":
                    field_name = pattern.split(':')[0].split('_')[0]
                    env_var_name = f"{field_name.upper()}_PASSWORD"
                    
                    issue = SecurityIssue(
                        level=SecurityRiskLevel.HIGH,
                        category='database_password',
                        description=f'发现硬编码的{field_name}数据库密码',
                        location=f"{settings_path}:行{self._get_line_number(content, match.start())}",
                        value=self._mask_password(password),
                        recommendation=f"将密码移至环境变量：export {env_var_name}='your-password'",
                        is_env_var_available=env_var_name in os.environ
                    )
                    issues.append(issue)
        
        return issues

    def check_config_yaml(self, config_path: Path) -> List[SecurityIssue]:
        """
        检查config.yaml文件的安全问题
        
        Args:
            config_path: config.yaml文件路径
            
        Returns:
            List[SecurityIssue]: 发现的安全问题列表
        """
        issues = []
        
        if not config_path.exists():
            return issues
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # 检查数据库密码配置
            if 'database' in config_data:
                db_config = config_data['database']
                password = db_config.get('password', '')
                
                # 检查是否使用了环境变量语法
                if password and not password.startswith('${'):
                    issue = SecurityIssue(
                        level=SecurityRiskLevel.HIGH,
                        category='database_password',
                        description='config.yaml中发现硬编码数据库密码',
                        location=f"{config_path}:database.password",
                        value=self._mask_password(str(password)),
                        recommendation="使用环境变量语法：password: \"${DB_PASSWORD}\"",
                        is_env_var_available='DB_PASSWORD' in os.environ
                    )
                    issues.append(issue)
                    
        except yaml.YAMLError as e:
            logger.error(f"解析YAML文件 {config_path} 时出错: {e}")
        except Exception as e:
            logger.error(f"检查config.yaml时出错: {e}")
            
        return issues

    def check_environment_variables(self) -> List[SecurityIssue]:
        """
        检查环境变量设置情况
        
        Returns:
            List[SecurityIssue]: 环境变量相关的安全问题
        """
        issues = []
        
        # 重要的环境变量
        critical_env_vars = {
            'SILICONFLOW_API_KEY': '用于SiliconFlow API访问',
            'DB_PASSWORD': '用于MySQL数据库连接',
            'CLICKHOUSE_PASSWORD': '用于ClickHouse数据库连接',
            'CCASS_PASSWORD': '用于CCASS数据库连接'
        }
        
        for env_var, description in critical_env_vars.items():
            if env_var not in os.environ:
                issue = SecurityIssue(
                    level=SecurityRiskLevel.MEDIUM,
                    category='missing_env_var',
                    description=f'缺少重要环境变量: {env_var}',
                    location='系统环境变量',
                    value=f'未设置 {env_var}',
                    recommendation=f"设置环境变量：export {env_var}='your-value' # {description}",
                    is_env_var_available=False
                )
                issues.append(issue)
                
        return issues

    def scan_project(self, project_root: Path) -> Dict[str, Any]:
        """
        扫描整个项目的安全配置
        
        Args:
            project_root: 项目根目录
            
        Returns:
            Dict[str, Any]: 完整的安全扫描报告
        """
        logger.info(f"开始安全扫描项目: {project_root}")
        
        all_issues = []
        
        # 检查关键配置文件
        config_files = [
            project_root / 'config' / 'settings.py',
            project_root / 'config.yaml',
            project_root / '.env',
            project_root / 'start_enhanced_monitor.py'
        ]
        
        for config_file in config_files:
            if config_file.exists():
                logger.info(f"检查文件: {config_file}")
                if config_file.name == 'settings.py':
                    file_issues = self.check_settings_py(config_file)
                elif config_file.name == 'config.yaml':
                    file_issues = self.check_config_yaml(config_file)
                else:
                    file_issues = self.check_file(config_file)
                all_issues.extend(file_issues)
        
        # 检查环境变量
        env_issues = self.check_environment_variables()
        all_issues.extend(env_issues)
        
        # 生成报告
        report = self._generate_security_report(all_issues)
        
        logger.info(f"安全扫描完成，发现 {len(all_issues)} 个问题")
        return report

    def _generate_security_report(self, issues: List[SecurityIssue]) -> Dict[str, Any]:
        """
        生成安全扫描报告
        
        Args:
            issues: 发现的安全问题列表
            
        Returns:
            Dict[str, Any]: 格式化的安全报告
        """
        # 按风险级别分类
        issues_by_level = {
            SecurityRiskLevel.CRITICAL: [],
            SecurityRiskLevel.HIGH: [],
            SecurityRiskLevel.MEDIUM: [],
            SecurityRiskLevel.LOW: []
        }
        
        for issue in issues:
            issues_by_level[issue.level].append(issue)
        
        # 计算安全分数 (满分100)
        security_score = self._calculate_security_score(issues_by_level)
        
        report = {
            'scan_time': str(Path.cwd()),
            'total_issues': len(issues),
            'security_score': security_score,
            'risk_summary': {
                '严重风险': len(issues_by_level[SecurityRiskLevel.CRITICAL]),
                '高风险': len(issues_by_level[SecurityRiskLevel.HIGH]),
                '中风险': len(issues_by_level[SecurityRiskLevel.MEDIUM]),
                '低风险': len(issues_by_level[SecurityRiskLevel.LOW])
            },
            'issues_by_level': {},
            'recommendations': self._generate_recommendations(issues_by_level),
            'quick_fixes': self._generate_quick_fixes(issues)
        }
        
        # 序列化问题详情
        for level, level_issues in issues_by_level.items():
            report['issues_by_level'][level.value] = [
                {
                    'category': issue.category,
                    'description': issue.description,
                    'location': issue.location,
                    'value': issue.value,
                    'recommendation': issue.recommendation,
                    'env_var_available': issue.is_env_var_available
                }
                for issue in level_issues
            ]
        
        return report

    def _calculate_security_score(self, issues_by_level: Dict[SecurityRiskLevel, List]) -> int:
        """计算安全分数"""
        score = 100
        score -= len(issues_by_level[SecurityRiskLevel.CRITICAL]) * 25
        score -= len(issues_by_level[SecurityRiskLevel.HIGH]) * 15
        score -= len(issues_by_level[SecurityRiskLevel.MEDIUM]) * 8
        score -= len(issues_by_level[SecurityRiskLevel.LOW]) * 3
        return max(0, score)

    def _generate_recommendations(self, issues_by_level: Dict[SecurityRiskLevel, List]) -> List[str]:
        """生成安全建议"""
        recommendations = []
        
        if issues_by_level[SecurityRiskLevel.CRITICAL]:
            recommendations.append("🚨 立即处理所有严重风险问题，特别是硬编码的API密钥")
            
        if issues_by_level[SecurityRiskLevel.HIGH]:
            recommendations.append("⚠️ 尽快修复高风险问题，将密码移至环境变量")
            
        recommendations.extend([
            "💡 创建 .env 文件存储敏感配置",
            "🔒 在生产环境中使用密钥管理服务",
            "📋 定期轮换API密钥和密码",
            "🔍 设置代码扫描工具防止敏感信息提交"
        ])
        
        return recommendations

    def _generate_quick_fixes(self, issues: List[SecurityIssue]) -> List[str]:
        """生成快速修复建议"""
        fixes = []
        
        # SiliconFlow API密钥修复
        if any(issue.category == 'siliconflow_api_key' for issue in issues):
            fixes.append("export SILICONFLOW_API_KEY='your-actual-api-key'")
            
        # 数据库密码修复  
        if any('password' in issue.category for issue in issues):
            fixes.extend([
                "export DB_PASSWORD='your-mysql-password'",
                "export CLICKHOUSE_PASSWORD='your-clickhouse-password'",
                "export CCASS_PASSWORD='your-ccass-password'"
            ])
            
        if fixes:
            fixes.insert(0, "# 将以下环境变量添加到 .env 文件或系统环境变量:")
            
        return fixes

    def _check_env_var_for_value(self, content: str, value: str) -> bool:
        """检查是否有对应的环境变量使用"""
        for pattern in self.env_var_patterns:
            if re.search(pattern, content):
                return True
        return False

    def _get_line_number(self, content: str, position: int) -> int:
        """获取位置对应的行号"""
        return content[:position].count('\n') + 1

    def _mask_sensitive_value(self, value: str) -> str:
        """掩码敏感值"""
        if len(value) <= 8:
            return '*' * len(value)
        return value[:4] + '*' * (len(value) - 8) + value[-4:]

    def _mask_api_key(self, api_key: str) -> str:
        """掩码API密钥"""
        if len(api_key) <= 10:
            return '*' * len(api_key)
        return api_key[:6] + '*' * (len(api_key) - 10) + api_key[-4:]

    def _mask_password(self, password: str) -> str:
        """掩码密码"""
        return '*' * len(password)

    def _get_recommendation(self, category: str, has_env_var: bool) -> str:
        """获取针对性建议"""
        if category == 'api_key':
            return "将API密钥移至环境变量" if not has_env_var else "确认环境变量配置正确"
        elif category == 'password':
            return "将密码移至环境变量" if not has_env_var else "确认环境变量配置正确"
        elif category == 'database_credentials':
            return "将数据库凭据移至环境变量"
        else:
            return "将敏感信息移至安全的配置管理系统"

    def print_security_report(self, report: Dict[str, Any]) -> None:
        """
        打印格式化的安全报告
        
        Args:
            report: 安全扫描报告
        """
        print("\n" + "="*80)
        print("🔐 HKEX系统安全配置检查报告")
        print("="*80)
        
        print(f"\n📊 安全评分: {report['security_score']}/100")
        print(f"🔍 总计问题: {report['total_issues']} 个")
        
        # 风险摘要
        print(f"\n📈 风险分布:")
        for risk, count in report['risk_summary'].items():
            emoji = "🚨" if "严重" in risk else "⚠️" if "高" in risk else "⚡" if "中" in risk else "ℹ️"
            print(f"  {emoji} {risk}: {count} 个")
        
        # 问题详情
        for level, issues in report['issues_by_level'].items():
            if issues:
                print(f"\n{level} ({len(issues)} 个):")
                for issue in issues:
                    env_status = "✅" if issue['env_var_available'] else "❌"
                    print(f"  • {issue['description']}")
                    print(f"    位置: {issue['location']}")
                    print(f"    值: {issue['value']}")
                    print(f"    环境变量: {env_status}")
                    print(f"    建议: {issue['recommendation']}")
                    print()
        
        # 建议
        print("💡 安全建议:")
        for rec in report['recommendations']:
            print(f"  {rec}")
        
        # 快速修复
        if report['quick_fixes']:
            print(f"\n🔧 快速修复:")
            for fix in report['quick_fixes']:
                print(f"  {fix}")
        
        print("\n" + "="*80)


def main():
    """主函数 - 运行安全检查"""
    checker = SecurityConfigChecker()
    project_root = Path('/Users/ericp/PycharmProjects/hkexann')
    
    # 执行安全扫描
    report = checker.scan_project(project_root)
    
    # 打印报告
    checker.print_security_report(report)
    
    # 保存报告到文件
    report_file = project_root / 'docs' / 'security_scan_report.json'
    report_file.parent.mkdir(exist_ok=True)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n📄 详细报告已保存至: {report_file}")
    
    # 返回安全分数用于自动化判断
    return report['security_score']


if __name__ == "__main__":
    score = main()
    exit(0 if score >= 80 else 1)  # 安全分数低于80则退出码为1
