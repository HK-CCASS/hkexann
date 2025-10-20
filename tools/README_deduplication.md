# ClickHouse 去重工具 - HKEX分析系统

🚀 **专业级数据去重解决方案** - 为 HKEX 多Agent分析系统定制开发

由 **Claude 4.0 sonnet** AI助手专业打造，提供安全、高效、智能的 ClickHouse 数据去重服务。

## 🎯 核心特性

- ✅ **智能去重**: 基于 `doc_id` 和 `chunk_id` 的精确去重
- 🛡️ **安全保护**: 自动备份、回滚机制、完整的操作日志  
- ⚡ **高性能**: 分区并行处理、批量操作优化
- 📊 **详细统计**: 去重前后对比、处理报告生成
- 🔧 **灵活配置**: 支持试运行、自定义策略
- 💫 **用户友好**: 交互式菜单、命令行接口

## 📋 支持的表

| 表名 | 主键字段 | 去重策略 | 备注 |
|------|---------|---------|------|
| `pdf_documents` | `doc_id` | 保留最新记录 | 基于 `created_at` 排序 |
| `pdf_chunks` | `chunk_id` + `doc_id` | 保留最新记录 | 复合主键去重 |

## 🚀 快速开始

### 方式一：交互式菜单（推荐）

```bash
# 启动交互式去重工具
python tools/run_deduplication.py
```

菜单选项：
1. **快速分析重复数据** - 无害检查，了解数据状况
2. **安全去重操作** - 完整去重流程（含备份）
3. **试运行测试** - 模拟去重效果
4. **从备份恢复** - 紧急数据恢复

### 方式二：命令行直接操作

```bash
# 分析所有表的重复数据
python tools/clickhouse_deduplication_tool.py --analyze-only

# 试运行去重（不修改数据）
python tools/clickhouse_deduplication_tool.py --dry-run

# 去重指定表（自动备份）
python tools/clickhouse_deduplication_tool.py --tables pdf_documents

# 去重所有表并生成报告
python tools/clickhouse_deduplication_tool.py --output report.json

# 危险操作：跳过备份去重（不推荐）
python tools/clickhouse_deduplication_tool.py --no-backup
```

## 📊 使用示例

### 示例1：日常重复数据检查

```bash
# 每日数据质量检查
python tools/clickhouse_deduplication_tool.py --analyze-only
```

**输出示例：**
```
📋 表 pdf_documents 重复数据分析:
   总记录数: 125,847
   重复组数: 1,245  
   重复记录: 2,891
   重复率: 2.30%

📋 表 pdf_chunks 重复数据分析:
   总记录数: 1,547,923
   重复组数: 156
   重复记录: 312
   重复率: 0.02%
```

### 示例2：安全去重操作

```bash
# 完整去重流程
python tools/clickhouse_deduplication_tool.py --tables all
```

**处理流程：**
1. 🔍 分析重复数据
2. 💾 自动创建备份表
3. 🔧 执行去重操作  
4. ⚡ 优化表性能
5. 📄 生成详细报告

### 示例3：试运行预览

```bash
# 查看去重效果，不实际修改
python tools/clickhouse_deduplication_tool.py --dry-run --tables pdf_documents
```

**输出示例：**
```
✅ 表 pdf_documents 去重完成:
   处理前记录: 125,847
   处理后记录: 122,956  
   移除重复: 2,891
   去重率: 2.30%
   处理时间: 15.3s
   [试运行模式 - 未实际修改数据]
```

## 🛡️ 安全机制

### 自动备份策略

```python
# 备份表命名规则
backup_table_name = f"{original_table}_backup_{timestamp}"
# 例如: pdf_documents_backup_20250918_143052
```

### 数据验证流程

1. **连接验证**: 确保 ClickHouse 服务可用
2. **表结构验证**: 检查目标表是否存在  
3. **权限验证**: 确认操作权限
4. **数据完整性**: 验证备份数据完整性

### 回滚操作

```bash
# 手动从备份恢复（紧急情况）
python tools/run_deduplication.py
# 选择菜单项 4：从备份恢复数据
```

## 📈 性能优化

### 处理策略

| 表大小 | 处理策略 | 预估时间 |
|--------|---------|---------|
| < 10万记录 | 单次处理 | < 10秒 |
| 10万-100万 | 分批处理 | 30秒-2分钟 |
| > 100万记录 | 分区并行 | 2-10分钟 |

### 最佳实践时间

- ⏰ **推荐时间**: 业务低峰期（凌晨 2-6 点）
- 📅 **频率建议**: 
  - 日常检查：每日一次
  - 深度清理：每周一次
  - 全面优化：每月一次

## 🔧 高级配置

### 自定义去重策略

```python
# 修改 table_configs 实现自定义策略
table_configs = {
    'pdf_documents': {
        'primary_key': 'doc_id',
        'order_by': 'updated_at DESC',  # 可改为其他排序字段
        'keep_strategy': 'latest'       # latest/oldest/custom
    }
}
```

### 数据库连接配置

```python
# 使用自定义连接
custom_config = {
    'host': 'your-clickhouse-host',
    'port': 8124,
    'user': 'your-user', 
    'password': 'your-password',
    'database': 'your-database'
}

async with ClickHouseDeduplicationTool(custom_config) as tool:
    # 执行操作
```

## 📊 报告格式

### JSON报告结构

```json
{
  "report_metadata": {
    "generated_at": "2025-09-18T14:30:52",
    "tool_version": "1.0.0",
    "database": "hkex_analysis",
    "total_tables_processed": 2
  },
  "summary": {
    "total_records_before": 1673770,
    "total_records_after": 1670567,
    "total_duplicates_removed": 3203,
    "overall_deduplication_rate": 0.19,
    "total_processing_time": 47.3
  },
  "table_details": [
    {
      "table_name": "pdf_documents", 
      "total_records_before": 125847,
      "duplicates_removed": 2891,
      "deduplication_rate": 2.30,
      "backup_table_name": "pdf_documents_backup_20250918_143052"
    }
  ],
  "recommendations": [
    "所有表的数据质量良好，重复率在正常范围内"
  ]
}
```

## 🚨 注意事项

### ⚠️ 重要警告

1. **生产环境**: 务必在业务低峰期执行
2. **备份策略**: 强烈建议保留自动备份（除非磁盘空间不足）
3. **权限要求**: 需要 ClickHouse 的 DELETE 和 CREATE TABLE 权限
4. **网络稳定**: 确保网络连接稳定，避免操作中断

### 🔍 故障排除

**问题1: 连接失败**
```bash
❌ ClickHouse连接测试失败
```
解决方案：
- 检查 ClickHouse 服务是否运行
- 验证配置文件中的连接参数
- 确认网络连接和防火墙设置

**问题2: 权限不足**
```bash
❌ 查询失败 (HTTP 403): Access denied
```
解决方案：
- 确认用户具有必要的数据库权限
- 检查用户名和密码是否正确

**问题3: 磁盘空间不足**
```bash
❌ 备份表创建失败: No space left on device
```
解决方案：
- 清理不必要的备份表
- 使用 `--no-backup` 选项（谨慎）
- 扩展磁盘空间

## 🎓 最佳实践

### 日常维护流程

```bash
# 1. 每日数据质量检查
python tools/clickhouse_deduplication_tool.py --analyze-only

# 2. 发现重复率 > 2% 时执行试运行
python tools/clickhouse_deduplication_tool.py --dry-run

# 3. 确认无误后执行实际去重
python tools/clickhouse_deduplication_tool.py --output daily_report.json

# 4. 检查报告并清理过期备份表
```

### 紧急数据恢复

```bash
# 紧急情况下快速恢复
python tools/run_deduplication.py
# 选择菜单 4 -> 输入备份表名 -> 确认恢复
```

## 🤝 技术支持

本工具由 **Claude 4.0 sonnet** 专业AI助手开发，具备：

- 🧠 **智能错误诊断**: 自动识别常见问题并提供解决方案
- 📞 **完整日志记录**: 便于问题追踪和性能分析  
- 🔄 **持续优化**: 基于使用反馈不断改进算法
- 💡 **最佳实践指导**: 提供数据库维护建议

---

> 💝 **感谢使用 HKEX ClickHouse 去重工具！**  
> 让数据更清洁，让分析更精准！🎯











