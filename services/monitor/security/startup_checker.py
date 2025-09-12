"""
启动安全检查器

这个模块在系统启动时执行安全检查，确保系统配置安全。
集成了配置安全扫描和环境变量验证功能。

主要功能：
- 启动时安全检查
- 集成配置扫描和环境验证
- 生成安全启动报告
- 提供安全警告和建议

作者: HKEX分析团队
版本: 1.0.0
日期: 2025-01-17
"""

import os
import logging
import sys
import time
from typing import Dict, Any, Tuple
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from config_checker import SecurityConfigChecker
from env_validator import EnvironmentValidator

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SecurityStartupChecker:
    """
    启动安全检查器
    
    在系统启动时执行全面的安全检查，包括配置安全扫描和环境变量验证。
    """
    
    def __init__(self, project_root: Path):
        """
        初始化启动安全检查器
        
        Args:
            project_root: 项目根目录路径
        """
        self.project_root = project_root
        self.config_checker = SecurityConfigChecker()
        self.env_validator = EnvironmentValidator()
        
    def perform_startup_check(self, allow_warnings: bool = False) -> Tuple[bool, Dict[str, Any]]:
        """
        执行启动安全检查
        
        Args:
            allow_warnings: 是否允许在有警告的情况下继续启动
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (是否可以安全启动, 检查报告)
        """
        logger.info("🔐 开始系统启动安全检查...")
        start_time = time.time()
        
        # 执行配置安全扫描
        logger.info("📋 正在扫描配置文件安全...")
        config_report = self.config_checker.scan_project(self.project_root)
        
        # 执行环境变量验证
        logger.info("🔧 正在验证环境变量配置...")
        env_report = self.env_validator.validate_all()
        
        # 综合分析
        overall_report = self._generate_overall_report(config_report, env_report)
        overall_report['check_duration'] = round(time.time() - start_time, 2)
        
        # 判断是否可以安全启动
        can_start_safely = self._determine_safety(config_report, env_report, allow_warnings)
        
        # 打印报告
        self._print_startup_report(overall_report, can_start_safely)
        
        # 如果不安全，提供修复建议
        if not can_start_safely:
            self._print_fix_suggestions(overall_report)
        
        logger.info(f"🏁 安全检查完成，耗时 {overall_report['check_duration']} 秒")
        
        return can_start_safely, overall_report
    
    def _generate_overall_report(self, config_report: Dict[str, Any], env_report: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成综合安全报告
        
        Args:
            config_report: 配置安全扫描报告
            env_report: 环境变量验证报告
            
        Returns:
            Dict[str, Any]: 综合报告
        """
        # 计算综合安全分数
        config_score = config_report['security_score']
        env_score = 100 if env_report['valid'] else 0
        
        # 综合评分 (配置权重60%, 环境变量权重40%)
        overall_score = round(config_score * 0.6 + env_score * 0.4, 1)
        
        # 统计总问题数
        total_issues = (
            config_report['total_issues'] + 
            len(env_report['required_missing']) + 
            len(env_report['invalid_format'])
        )
        
        # 安全等级判定
        if overall_score >= 90:
            security_level = "优秀"
            security_emoji = "🟢"
        elif overall_score >= 75:
            security_level = "良好"
            security_emoji = "🟡"
        elif overall_score >= 50:
            security_level = "中等"
            security_emoji = "🟠"
        else:
            security_level = "较差"
            security_emoji = "🔴"
        
        return {
            'overall_score': overall_score,
            'security_level': security_level,
            'security_emoji': security_emoji,
            'total_issues': total_issues,
            'config_report': config_report,
            'env_report': env_report,
            'critical_issues': config_report['risk_summary']['严重风险'],
            'high_issues': config_report['risk_summary']['高风险'],
            'missing_required_env': len(env_report['required_missing']),
            'security_warnings': len(env_report['security_warnings']),
            'can_start_with_warnings': self._can_start_with_warnings(config_report, env_report),
            'startup_recommendations': self._get_startup_recommendations(config_report, env_report)
        }
    
    def _determine_safety(self, config_report: Dict[str, Any], env_report: Dict[str, Any], allow_warnings: bool) -> bool:
        """
        判断是否可以安全启动
        
        Args:
            config_report: 配置安全报告
            env_report: 环境变量报告
            allow_warnings: 是否允许警告
            
        Returns:
            bool: 是否可以安全启动
        """
        # 严重问题阻止启动
        has_critical_issues = config_report['risk_summary']['严重风险'] > 0
        has_missing_required_env = len(env_report['required_missing']) > 0
        
        if has_critical_issues or has_missing_required_env:
            return False
        
        # 如果允许警告，检查是否只有较低风险问题
        if allow_warnings:
            return True
        
        # 严格模式：任何高风险问题都阻止启动
        has_high_issues = config_report['risk_summary']['高风险'] > 0
        has_security_warnings = len(env_report['security_warnings']) > 0
        
        return not (has_high_issues or has_security_warnings)
    
    def _can_start_with_warnings(self, config_report: Dict[str, Any], env_report: Dict[str, Any]) -> bool:
        """检查是否可以在警告模式下启动"""
        has_critical = config_report['risk_summary']['严重风险'] > 0
        has_missing_required = len(env_report['required_missing']) > 0
        return not (has_critical or has_missing_required)
    
    def _get_startup_recommendations(self, config_report: Dict[str, Any], env_report: Dict[str, Any]) -> list:
        """获取启动相关的建议"""
        recommendations = []
        
        if config_report['risk_summary']['严重风险'] > 0:
            recommendations.append("🚨 立即修复严重安全风险，特别是硬编码的API密钥")
        
        if len(env_report['required_missing']) > 0:
            recommendations.append("🔧 设置所有必需的环境变量")
        
        if config_report['risk_summary']['高风险'] > 0:
            recommendations.append("⚠️ 尽快修复高风险问题")
        
        if len(env_report['security_warnings']) > 0:
            recommendations.append("🔒 处理密码安全警告")
        
        recommendations.extend([
            "💡 使用 --allow-warnings 参数可在警告情况下强制启动",
            "📋 查看 .env.template 文件了解环境变量配置",
            "🔄 定期运行安全检查确保系统安全"
        ])
        
        return recommendations
    
    def _print_startup_report(self, report: Dict[str, Any], can_start: bool) -> None:
        """
        打印启动安全报告
        
        Args:
            report: 综合安全报告
            can_start: 是否可以安全启动
        """
        print("\n" + "="*90)
        print("🔐 HKEX系统启动安全检查报告")
        print("="*90)
        
        # 总体状态
        status_emoji = "✅" if can_start else "❌"
        status_text = "可以安全启动" if can_start else "无法安全启动"
        print(f"\n{status_emoji} 启动状态: {status_text}")
        print(f"{report['security_emoji']} 安全等级: {report['security_level']} (评分: {report['overall_score']}/100)")
        print(f"🔍 检查耗时: {report['check_duration']} 秒")
        print(f"📊 发现问题: {report['total_issues']} 个")
        
        # 问题分布
        print(f"\n📈 问题分布:")
        print(f"  🚨 严重风险: {report['critical_issues']} 个")
        print(f"  ⚠️ 高风险: {report['high_issues']} 个")
        print(f"  🔧 缺少必需环境变量: {report['missing_required_env']} 个")
        print(f"  🔒 安全警告: {report['security_warnings']} 个")
        
        # 启动建议
        print(f"\n💡 启动建议:")
        for rec in report['startup_recommendations']:
            print(f"  {rec}")
        
        # 如果可以带警告启动
        if not can_start and report['can_start_with_warnings']:
            print(f"\n⚡ 可以使用警告模式启动: python start_enhanced_monitor.py --allow-warnings")
        
        print("\n" + "="*90)
    
    def _print_fix_suggestions(self, report: Dict[str, Any]) -> None:
        """
        打印修复建议
        
        Args:
            report: 综合安全报告
        """
        print("\n🔧 快速修复建议:")
        print("="*50)
        
        # 环境变量修复
        if report['env_report']['required_missing']:
            print("\n1. 创建环境变量配置:")
            print("   cp .env.template .env")
            print("   # 然后编辑 .env 文件，设置以下变量:")
            for missing in report['env_report']['required_missing']:
                print(f"   export {missing['name']}='{missing['example']}'")
        
        # 配置文件修复
        if report['config_report']['quick_fixes']:
            print("\n2. 修复配置文件:")
            for fix in report['config_report']['quick_fixes']:
                print(f"   {fix}")
        
        print("\n3. 验证修复结果:")
        print("   python3 services/monitor/security/startup_checker.py")
        
        print("\n" + "="*50)


def check_startup_security(project_root: Path = None, allow_warnings: bool = False) -> Tuple[bool, Dict[str, Any]]:
    """
    检查启动安全性的便捷函数
    
    Args:
        project_root: 项目根目录，默认为当前目录
        allow_warnings: 是否允许在有警告的情况下启动
        
    Returns:
        Tuple[bool, Dict[str, Any]]: (是否可以安全启动, 检查报告)
    """
    if project_root is None:
        project_root = Path.cwd()
    
    checker = SecurityStartupChecker(project_root)
    return checker.perform_startup_check(allow_warnings)


def main():
    """主函数 - 运行启动安全检查"""
    import argparse
    
    parser = argparse.ArgumentParser(description='HKEX系统启动安全检查')
    parser.add_argument('--allow-warnings', action='store_true', 
                       help='允许在有警告的情况下启动')
    parser.add_argument('--project-root', type=Path, default=Path.cwd(),
                       help='项目根目录路径')
    
    args = parser.parse_args()
    
    # 执行安全检查
    can_start, report = check_startup_security(args.project_root, args.allow_warnings)
    
    # 返回适当的退出码
    if can_start:
        print(f"\n🎉 系统可以安全启动！")
        exit(0)
    else:
        print(f"\n🛑 系统无法安全启动，请先修复安全问题。")
        exit(1)


if __name__ == "__main__":
    main()
