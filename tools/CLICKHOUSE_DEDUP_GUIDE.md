# ClickHouse PDF数据去重工具使用指南

## 📋 概述

本工具专门用于处理 HKEX 分析系统中 ClickHouse 数据库 `pdf_chunks` 和 `pdf_documents` 表的重复数据问题。提供高效、安全、易用的去重解决方案。

## 🎯 主要特点

- ✅ **专门优化**：专注于ClickHouse表，无需Milvus依赖
- ✅ **多种策略**：支持4种去重策略，灵活应对不同场景
- ✅ **安全机制**：自动备份，支持数据恢复
- ✅ **高效查询**：优化的SQL查询，快速检测重复
- ✅ **详细报告**：完整的统计信息和操作日志
- ✅ **简单易用**：提供交互式和命令行两种使用方式

## 🚀 快速开始

### 1. 环境检查

确保系统环境正常：

```bash
# 检查Python依赖
python -c "import asyncio; print('✅ asyncio可用')"

# 检查项目依赖
python -c "from config.settings import settings; print('✅ 项目配置正常')"

# 检查ClickHouse连接
python tools/dedup_health_check.py  # 可选，如果有的话
```

### 2. 选择使用方式

我们提供了两个主要工具：

#### 🔧 专业版工具 - `clickhouse_dedup_pro.py`
- 功能最完整，支持所有高级特性
- 适合需要精细控制的场景

#### 🚀 简易版工具 - `simple_dedup.py`  
- 界面友好，一键操作
- 适合日常维护使用

### 3. 简易使用（推荐新手）

```bash
# 交互式模式（最简单）
python tools/simple_dedup.py

# 快速扫描
python tools/simple_dedup.py --scan-only

# 预览删除
python tools/simple_dedup.py --dry-run

# 自动去重
python tools/simple_dedup.py --auto
```

### 4. 专业使用

```bash
# 扫描重复数据
python tools/clickhouse_dedup_pro.py scan

# 使用不同策略扫描
python tools/clickhouse_dedup_pro.py scan --strategy keep_oldest

# 执行去重
python tools/clickhouse_dedup_pro.py dedup

# 自动去重（无需确认）
python tools/clickhouse_dedup_pro.py dedup --auto
```

## 📊 去重策略详解

### 1. `keep_latest` (默认)
- **保留**：创建时间最新的记录
- **适用**：一般情况，确保数据时效性
- **逻辑**：`ORDER BY created_at DESC LIMIT 1`

### 2. `keep_oldest`
- **保留**：创建时间最早的记录  
- **适用**：重视数据原始性的场景
- **逻辑**：`ORDER BY created_at ASC LIMIT 1`

### 3. `keep_largest`
- **保留**：内容最丰富的记录
- **适用**：确保信息完整性
- **逻辑**：
  - `pdf_chunks`: 按 `text_length` 降序
  - `pdf_documents`: 按 `file_size` 降序

### 4. `keep_complete`
- **保留**：字段最完整的记录
- **适用**：数据质量优先
- **逻辑**：按非空字段数量排序

## 🔍 重复检测逻辑

### pdf_chunks 表
- **主键**：`(doc_id, chunk_id)` 组合
- **检测**：相同组合的多条记录
- **保留**：根据策略选择1条
- **删除**：同组其他记录

### pdf_documents 表  
- **主键**：`doc_id`
- **检测**：相同doc_id的多条记录
- **保留**：根据策略选择1条
- **删除**：同组其他记录

## 🛡️ 安全机制

### 备份策略

工具会自动创建JSON格式的备份文件：

```json
{
  "session_id": "20250118_143022",
  "backup_time": "2025-01-18T14:30:22",
  "strategy": "keep_latest",
  "tables": {
    "pdf_chunks": [
      {
        "doc_id": "DOC123",
        "chunk_id": "CHUNK456",
        "created_at": "2025-01-15T10:30:12",
        "... 其他字段": "..."
      }
    ],
    "pdf_documents": [...]
  }
}
```

### 恢复机制

```bash
# 从备份恢复（需要手动实现）
python tools/clickhouse_dedup_pro.py restore backup_file.json
```

### 操作模式

| 模式 | 安全性 | 说明 |
|------|--------|------|
| `SCAN_ONLY` | 🟢 最安全 | 只检测，不删除 |
| `PREVIEW` | 🟢 安全 | 预览删除计划 |
| `SAFE_DELETE` | 🟡 较安全 | 删除前备份 |
| `DIRECT_DELETE` | 🔴 风险 | 直接删除，无备份 |

## 📈 报告解读

### 典型报告示例

```
📊 ClickHouse PDF数据去重报告
=====================================
🆔 会话ID: 20250118_143022
⏱️  持续时间: 0:01:53
🎯 操作模式: safe_delete
🧠 去重策略: keep_latest

📈 统计摘要:
  🔍 扫描表数: 2
  📋 重复组数: 15
  🗑️  计划删除: 23 条
  ✅ 实际删除: 23 条
  📊 成功率: 100.0%

📋 分表统计:
  📊 pdf_chunks:
     总记录: 15,234
     重复组: 12
     计划删除: 18
     实际删除: 18
  
  📊 pdf_documents:
     总记录: 1,456
     重复组: 3
     计划删除: 5
     实际删除: 5

💾 备份信息:
  📁 备份文件: backups/clickhouse_dedup_backup_20250118_143022.json
  📦 文件大小: 15,678 bytes

💡 操作建议:
  ✅ 去重操作已完成
  🔄 如需恢复，请使用备份文件
```

### 关键指标说明

- **重复组数**：发现的重复键值组合数量
- **成功率**：`实际删除 / 计划删除 × 100%`
- **持续时间**：从扫描到完成的总耗时
- **备份大小**：备份文件的字节数

## 💡 使用场景

### 场景1：日常维护检查

```bash
# 每周运行一次，检查数据状态
python tools/simple_dedup.py --scan-only
```

### 场景2：发现重复后处理

```bash
# 1. 先预览
python tools/simple_dedup.py --dry-run

# 2. 确认无误后执行
python tools/simple_dedup.py --auto
```

### 场景3：大批量数据导入后

```bash
# 使用专业工具，选择合适策略
python tools/clickhouse_dedup_pro.py scan --strategy keep_complete
python tools/clickhouse_dedup_pro.py dedup --auto
```

### 场景4：数据质量审计

```bash
# 生成详细报告
python tools/clickhouse_dedup_pro.py scan
# 人工审查重复组
# 选择性执行去重
```

## ⚠️ 注意事项

### 操作前检查

1. **数据库状态**：确保ClickHouse服务正常
2. **磁盘空间**：确保有足够空间存储备份
3. **系统负载**：避免在高峰时段执行
4. **权限验证**：确保有ALTER TABLE权限

### 风险提醒

⚠️ **重要提醒**：

1. 去重操作会**永久删除数据**，请务必谨慎
2. 建议先在**测试环境**验证效果
3. 使用 `--dry-run` 模式预览操作
4. 保留备份文件直到确认无误
5. 重要数据请提前做好**额外备份**

### 最佳实践

```python
# ✅ 推荐的操作流程
async def best_practice_workflow():
    # 1. 健康检查（可选）
    # python tools/dedup_health_check.py
    
    # 2. 扫描评估
    # python tools/simple_dedup.py --scan-only
    
    # 3. 预览操作
    # python tools/simple_dedup.py --dry-run
    
    # 4. 执行去重
    # python tools/simple_dedup.py --auto
    
    # 5. 验证结果
    # python tools/simple_dedup.py --scan-only
```

## 🔧 故障排除

### 常见问题

#### 1. 连接失败
```
❌ ClickHouse连接建立失败
```
**解决方案**：
- 检查 `config/settings.py` 配置
- 确认ClickHouse服务状态
- 验证网络连接

#### 2. 权限不足  
```
❌ 删除记录失败: Access denied
```
**解决方案**：
- 确认数据库用户有ALTER权限
- 检查表级别访问权限

#### 3. 磁盘空间不足
```
❌ 创建备份失败: No space left
```
**解决方案**：
- 清理 `backups/` 目录
- 增加磁盘空间

#### 4. 导入模块失败
```
❌ 导入失败: No module named 'config.settings'
```
**解决方案**：
- 确保从项目根目录运行
- 检查Python路径配置

### 日志分析

```bash
# 查看最新日志
tail -f clickhouse_dedup_*.log

# 搜索错误
grep "ERROR\|FAIL" clickhouse_dedup_*.log

# 统计操作结果
grep "删除.*条记录" clickhouse_dedup_*.log
```

## 📊 性能参考

### 典型性能数据

| 记录规模 | 扫描时间 | 去重时间 | 内存占用 |
|----------|----------|----------|----------|
| 1万条 | ~5秒 | ~10秒 | ~50MB |
| 10万条 | ~30秒 | ~1分钟 | ~200MB |
| 100万条 | ~3分钟 | ~8分钟 | ~500MB |

### 优化建议

1. **分批处理**：大量数据建议分批执行
2. **非峰时段**：选择系统负载较低时执行
3. **磁盘IO**：确保ClickHouse有足够IO性能
4. **网络稳定**：避免网络不稳定时执行

## 🎓 进阶用法

### 自定义脚本集成

```python
from tools.clickhouse_dedup_pro import ClickHouseDedupPro, DedupStrategy

async def custom_dedup_workflow():
    tool = ClickHouseDedupPro()
    await tool.initialize()
    
    # 扫描
    report = await tool.scan_duplicates(strategy=DedupStrategy.KEEP_LATEST)
    
    # 自定义逻辑
    if report.get_summary()['total_duplicate_groups'] > 10:
        print("重复数据较多，建议人工审查")
        return
    
    # 执行去重
    await tool.execute_deduplication()
    await tool.cleanup()
```

### 定时任务集成

```bash
# crontab 示例 - 每周一凌晨2点检查
0 2 * * 1 /usr/bin/python3 /path/to/tools/simple_dedup.py --scan-only >> /var/log/dedup.log 2>&1
```

## 📞 技术支持

如遇到问题或需要新功能，请联系 HKEX 分析团队。

### 文件结构

```
tools/
├── clickhouse_dedup_pro.py      # 专业版工具
├── simple_dedup.py              # 简易版工具  
├── CLICKHOUSE_DEDUP_GUIDE.md    # 本使用指南
├── data_deduplication_tool.py   # 原有综合工具
├── quick_dedup.py               # 原有快速工具
└── backups/                     # 备份文件目录
    ├── clickhouse_dedup_backup_*.json
    └── dedup_report_*.json
```

---

**📝 版本信息**  
- 工具版本：v2.0.0
- 文档版本：v1.0.0  
- 更新时间：2025-01-18

**⚖️ 免责声明**  
此工具涉及数据删除操作，使用前请充分测试并做好备份。使用者需对数据安全负责。
