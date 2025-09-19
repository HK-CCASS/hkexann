# 数据去重工具使用指南

## 概述

这个工具专门用于清理 HKEX 分析系统中 Milvus 向量数据库和 ClickHouse 数据库的重复数据。基于 `doc_id` 和 `chunk_id` 的组合来识别和删除重复记录。

## 🎯 核心功能

- ✅ **智能检测**：自动扫描 ClickHouse `pdf_chunks`、`pdf_documents` 表和 Milvus 集合中的重复数据
- ✅ **安全去重**：按时间戳保留最新记录，删除旧重复项
- ✅ **数据备份**：提供完整的备份和恢复机制
- ✅ **详细报告**：生成完整的去重操作报告和统计信息
- ✅ **干运行模式**：支持预览模式，安全验证去重操作
- ✅ **回滚功能**：支持从备份文件恢复误删的数据

## 🚀 快速开始

### 1. 环境准备

确保系统已安装所有依赖：

```bash
# 安装 Python 依赖
pip install -r requirements.txt

# 确保数据库服务正常运行
# - ClickHouse (端口 8124)
# - Milvus (端口 19531)
```

### 2. 运行工具

```bash
# 交互式命令行模式
python tools/data_deduplication_tool.py

# 或者在 Python 代码中使用
from tools.data_deduplication_tool import DataDeduplicationTool

tool = DataDeduplicationTool()
await tool.initialize()
report = await tool.scan_duplicates()
```

### 3. 操作流程

#### 步骤 1：扫描重复数据
```python
# 扫描并分析重复项
report = await tool.scan_duplicates()
tool.print_report(report)
```

#### 步骤 2：预览去重操作（推荐）
```python
# 干运行模式，只预览不删除
report = await tool.remove_duplicates(DeduplicationMode.DRY_RUN)
tool.print_report(report)
```

#### 步骤 3：执行安全去重
```python
# 带备份的安全删除
report = await tool.remove_duplicates(DeduplicationMode.SAFE_REMOVE)
tool.print_report(report)
```

## 📊 去重策略

### 重复检测逻辑

1. **主键识别**：使用 `(doc_id, chunk_id)` 组合作为唯一标识符
2. **跨系统检测**：同时检测 ClickHouse 和 Milvus 中的重复项
3. **时间优先**：按 `created_at` 时间戳排序，保留最新记录
4. **数据一致性**：确保两个系统中的数据保持同步

### 保留规则

- ✅ **保留**：每组重复项中时间戳最新的记录
- ❌ **删除**：同组中的其他旧记录
- 🔒 **安全**：删除前自动创建完整备份

## 🛡️ 安全机制

### 备份策略

```json
{
  "session_id": "20250118_143022",
  "backup_time": "2025-01-18T14:30:22",
  "clickhouse_records": [
    {
      "doc_id": "DOC123",
      "chunk_id": "CHUNK456", 
      "table": "pdf_chunks",
      "raw_data": [...]
    }
  ],
  "milvus_records": [
    {
      "id": "VEC789",
      "doc_id": "DOC123",
      "chunk_id": "CHUNK456",
      "embedding": [...]
    }
  ]
}
```

### 回滚操作

```python
# 从备份恢复数据
success = await tool.rollback_from_backup("backups/dedup_backup_20250118_143022.json")
```

### 操作模式

| 模式 | 描述 | 备份 | 风险 |
|------|------|------|------|
| `DRY_RUN` | 只扫描预览，不删除 | ❌ | 🟢 无风险 |
| `SAFE_REMOVE` | 删除前自动备份 | ✅ | 🟡 低风险 |
| `FORCE_REMOVE` | 直接删除，无备份 | ❌ | 🔴 高风险 |

## 📈 报告解读

### 典型报告示例

```
📊 数据去重报告
=====================================
会话ID: 20250118_143022
开始时间: 2025-01-18 14:30:22
结束时间: 2025-01-18 14:32:15
操作模式: safe_remove

📈 统计信息:
  ClickHouse 总记录数: 15234
  Milvus 总记录数: 15198
  发现重复组数: 23
  待删除记录数: 31
  实际删除记录数: 31

💾 备份信息:
  备份文件: backups/dedup_backup_20250118_143022.json
  可回滚: 是

🔍 重复项详情 (显示前3组):
  组 1: doc_id=DOC_20250115_001, chunk_id=CHUNK_001
    1. clickhouse - 保留 - 2025-01-15 10:32:15
    2. clickhouse - 删除 - 2025-01-15 10:30:12
  
  组 2: doc_id=DOC_20250115_002, chunk_id=CHUNK_005
    1. milvus - 保留 - 2025-01-15 11:15:30
    2. milvus - 删除 - 2025-01-15 11:10:22
```

### 关键指标

- **重复组数**：发现的重复 `(doc_id, chunk_id)` 组合数量
- **删除率**：`删除记录数 / 总重复记录数`
- **成功率**：`实际删除数 / 计划删除数`

## ⚠️ 注意事项

### 操作前检查

1. **数据库状态**：确保 ClickHouse 和 Milvus 服务正常
2. **备份空间**：确保有足够的磁盘空间存储备份文件
3. **权限验证**：确保有删除数据库记录的权限
4. **系统负载**：避免在系统高负载时执行大批量操作

### 风险提醒

⚠️ **重要**：去重操作会永久删除数据，请务必：

1. 在生产环境使用前，先在测试环境验证
2. 使用 `DRY_RUN` 模式预览操作结果
3. 使用 `SAFE_REMOVE` 模式进行有备份的删除
4. 保留备份文件直到确认操作无误

### 最佳实践

```python
# ✅ 推荐的操作流程
async def recommended_deduplication():
    tool = DataDeduplicationTool()
    
    # 1. 初始化连接
    await tool.initialize()
    
    # 2. 扫描重复项
    scan_report = await tool.scan_duplicates()
    print(f"发现 {scan_report.duplicate_groups_found} 组重复数据")
    
    # 3. 干运行预览
    dry_report = await tool.remove_duplicates(DeduplicationMode.DRY_RUN)
    tool.print_report(dry_report)
    
    # 4. 用户确认后执行安全删除
    if input("确认执行删除? (yes/no): ") == "yes":
        safe_report = await tool.remove_duplicates(DeduplicationMode.SAFE_REMOVE)
        tool.print_report(safe_report)
    
    # 5. 清理资源
    await tool.cleanup()
```

## 🔧 故障排除

### 常见问题

#### 1. 连接失败
```
❌ ClickHouse 连接初始化失败
```
**解决方案**：
- 检查 `config/settings.py` 中的数据库配置
- 确认 ClickHouse 服务运行在正确端口
- 验证用户名和密码

#### 2. 权限不足
```
❌ 删除 ClickHouse 记录失败: Access denied
```
**解决方案**：
- 确认数据库用户有 DELETE 权限
- 检查表级别的访问权限

#### 3. 备份失败
```
❌ 创建数据备份失败: Disk space insufficient
```
**解决方案**：
- 清理 `backups/` 目录中的旧备份文件
- 增加磁盘空间

### 日志分析

工具会生成详细的操作日志：

```bash
# 查看最新日志
tail -f deduplication_*.log

# 搜索错误信息
grep "ERROR" deduplication_*.log
```

## 📁 文件结构

```
tools/
├── data_deduplication_tool.py    # 主工具文件
├── README.md                     # 使用文档
└── backups/                      # 备份文件目录
    ├── dedup_backup_*.json       # 数据备份文件
    └── dedup_report_*.json       # 操作报告文件
```

## 🤝 技术支持

如果遇到问题或需要新功能，请联系 HKEX 分析团队。

---

**免责声明**：此工具涉及数据删除操作，使用前请充分测试并做好备份。使用者需对数据安全负责。
