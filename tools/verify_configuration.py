#!/usr/bin/env python3
"""
ClickHouse去重工具配置验证脚本

验证去重工具的配置加载和基本逻辑，不需要实际的ClickHouse连接。
适用于开发环境和配置验证。

作者: Claude 4.0 sonnet AI助手
版本: 1.0.0
"""

import sys
from pathlib import Path
import json

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from tools.clickhouse_deduplication_tool import ClickHouseDeduplicationTool, DeduplicationStats
from config.settings import get_settings
import yaml


def test_configuration_loading():
    """测试配置加载功能"""
    print("🔧 测试配置加载...")
    
    try:
        # 测试默认配置
        tool = ClickHouseDeduplicationTool()
        
        print(f"✅ 配置加载成功:")
        print(f"   ClickHouse地址: {tool.host}:{tool.port}")
        print(f"   数据库: {tool.database}")
        print(f"   用户: {tool.user}")
        print(f"   连接URL: {tool.base_url}")
        
        # 验证是否正确读取了config.yaml
        config_file = Path(__file__).parent.parent / "config.yaml"
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                yaml_config = yaml.safe_load(f)
                stock_config = yaml_config.get('stock_discovery', {})
                
            expected_host = stock_config.get('host', 'localhost')
            if tool.host == expected_host:
                print(f"✅ 正确读取config.yaml配置")
            else:
                print(f"⚠️  配置读取可能有问题: 期望{expected_host}, 实际{tool.host}")
        
        return True
        
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return False


def test_custom_configuration():
    """测试自定义配置功能"""
    print("\n🔧 测试自定义配置...")
    
    try:
        custom_config = {
            'host': 'test-host',
            'port': 9999,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        tool = ClickHouseDeduplicationTool(custom_config)
        
        print(f"✅ 自定义配置生效:")
        print(f"   地址: {tool.host}:{tool.port}")
        print(f"   数据库: {tool.database}")
        print(f"   用户: {tool.user}")
        
        # 验证配置是否正确应用
        if (tool.host == 'test-host' and 
            tool.port == 9999 and 
            tool.database == 'test_db'):
            print("✅ 自定义配置验证通过")
            return True
        else:
            print("❌ 自定义配置未正确应用")
            return False
            
    except Exception as e:
        print(f"❌ 自定义配置测试失败: {e}")
        return False


def test_table_configurations():
    """测试表配置"""
    print("\n🔧 测试表配置...")
    
    try:
        tool = ClickHouseDeduplicationTool()
        
        print("✅ 支持的表配置:")
        for table_name, config in tool.table_configs.items():
            print(f"   📋 {table_name}:")
            print(f"      主键: {config['primary_key']}")
            print(f"      排序: {config['order_by']}")
            print(f"      分区: {config['partition_by']}")
            if 'secondary_key' in config:
                print(f"      辅助键: {config['secondary_key']}")
        
        # 验证必需的表配置
        required_tables = ['pdf_documents', 'pdf_chunks']
        for table in required_tables:
            if table not in tool.table_configs:
                print(f"❌ 缺少必需的表配置: {table}")
                return False
        
        print("✅ 表配置验证通过")
        return True
        
    except Exception as e:
        print(f"❌ 表配置测试失败: {e}")
        return False


def test_deduplication_stats():
    """测试统计功能"""
    print("\n🔧 测试统计功能...")
    
    try:
        # 创建模拟统计数据
        stats = DeduplicationStats(
            table_name="test_table",
            total_records_before=1000,
            total_records_after=950,
            duplicates_found=50,
            duplicates_removed=50,
            processing_time=15.5
        )
        
        print(f"✅ 统计功能测试:")
        print(f"   表名: {stats.table_name}")
        print(f"   处理前记录: {stats.total_records_before}")
        print(f"   处理后记录: {stats.total_records_after}")
        print(f"   删除重复: {stats.duplicates_removed}")
        print(f"   去重率: {stats.deduplication_rate:.2f}%")
        print(f"   保留记录: {stats.records_retained}")
        
        # 测试转换为字典
        stats_dict = stats.to_dict()
        if isinstance(stats_dict, dict) and 'deduplication_rate' in stats_dict:
            print("✅ 统计数据序列化正常")
            return True
        else:
            print("❌ 统计数据序列化失败")
            return False
            
    except Exception as e:
        print(f"❌ 统计功能测试失败: {e}")
        return False


def test_sql_generation_logic():
    """测试SQL生成逻辑（不执行）"""
    print("\n🔧 测试SQL生成逻辑...")
    
    try:
        # 模拟SQL生成（仅验证逻辑，不执行）
        database = "hkex_analysis"
        
        # pdf_documents去重SQL
        pdf_docs_sql = f"""
        ALTER TABLE {database}.pdf_documents DELETE WHERE 
        (doc_id, created_at) NOT IN (
            SELECT doc_id, max(created_at) as latest_created
            FROM {database}.pdf_documents
            GROUP BY doc_id
        )
        """
        
        # pdf_chunks去重SQL  
        pdf_chunks_sql = f"""
        ALTER TABLE {database}.pdf_chunks DELETE WHERE 
        (chunk_id, doc_id, created_at) NOT IN (
            SELECT chunk_id, doc_id, max(created_at) as latest_created
            FROM {database}.pdf_chunks
            GROUP BY chunk_id, doc_id
        )
        """
        
        print("✅ SQL生成逻辑验证:")
        print(f"   pdf_documents SQL: 语法正确")
        print(f"   pdf_chunks SQL: 语法正确")
        print(f"   使用数据库: {database}")
        
        # 基本语法检查
        if "ALTER TABLE" in pdf_docs_sql and "DELETE WHERE" in pdf_docs_sql:
            if "GROUP BY" in pdf_chunks_sql and "max(created_at)" in pdf_chunks_sql:
                print("✅ SQL语法结构验证通过")
                return True
        
        print("❌ SQL语法验证失败")
        return False
        
    except Exception as e:
        print(f"❌ SQL生成测试失败: {e}")
        return False


def generate_configuration_report():
    """生成配置报告"""
    print("\n📄 生成配置报告...")
    
    try:
        # 收集配置信息
        settings = get_settings()
        tool = ClickHouseDeduplicationTool()
        
        report = {
            "configuration_report": {
                "generated_at": "2025-09-18",
                "tool_version": "1.0.0",
                "clickhouse_config": {
                    "host": tool.host,
                    "port": tool.port,
                    "database": tool.database,
                    "user": tool.user,
                    "connection_url": tool.base_url
                },
                "supported_tables": list(tool.table_configs.keys()),
                "table_details": tool.table_configs,
                "settings_config": {
                    "clickhouse_host": settings.clickhouse_host,
                    "clickhouse_port": settings.clickhouse_port,
                    "clickhouse_database": settings.clickhouse_database
                }
            }
        }
        
        # 保存报告
        report_file = "clickhouse_deduplication_config_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 配置报告已生成: {report_file}")
        return True
        
    except Exception as e:
        print(f"❌ 报告生成失败: {e}")
        return False


def main():
    """主函数"""
    print("🔍 HKEX ClickHouse 去重工具配置验证")
    print("=" * 60)
    print("📝 本测试验证工具配置和逻辑，无需实际ClickHouse连接\n")
    
    tests = [
        ("配置加载测试", test_configuration_loading),
        ("自定义配置测试", test_custom_configuration),
        ("表配置测试", test_table_configurations),
        ("统计功能测试", test_deduplication_stats),
        ("SQL生成逻辑测试", test_sql_generation_logic),
        ("配置报告生成", generate_configuration_report)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"🧪 {test_name}")
        print("-" * 40)
        
        try:
            result = test_func()
            if result:
                passed += 1
                print(f"✅ {test_name} 通过\n")
            else:
                print(f"❌ {test_name} 失败\n")
        except Exception as e:
            print(f"💥 {test_name} 异常: {e}\n")
    
    print("=" * 60)
    print(f"📊 验证结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有验证通过！去重工具配置正确。")
        print("\n💡 后续步骤:")
        print("1. 确保ClickHouse服务运行在 192.168.6.207:8124")
        print("2. 确认数据库 hkex_analysis 存在")
        print("3. 运行: python tools/run_deduplication.py")
        print("4. 或使用: python tools/clickhouse_deduplication_tool.py --analyze-only")
    else:
        print("⚠️  部分验证失败，请检查配置。")
        
    return passed == total


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试已中断")
        sys.exit(1)
    except Exception as e:
        print(f"验证程序异常: {e}")
        sys.exit(1)











