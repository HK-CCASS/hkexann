# HKEX统一下载器系统 🚀

## 📋 概述

HKEX统一下载器系统是一个企业级港股公告下载和管理解决方案，提供了统一的配置管理、文件管理、下载执行和向后兼容适配器。系统采用模块化设计，支持多种配置模板和部署场景。

### ✨ 核心特性

- **🏗️ 模块化架构**：统一配置管理、智能文件管理、可插拔下载器、向后兼容适配器
- **📊 8个预设场景模板**：覆盖从开发测试到生产部署的各种需求
- **🎯 多种部署模式**：支持批量下载、实时监控、数据归档、API集成等场景
- **🔄 向后兼容**：无缝对接现有 main.py 和监控系统，零侵入式升级
- **⚡ 企业级性能**：支持高并发、限流控制、错误重试、统计监控

### 🏆 系统优势

| 对比项 | 传统方案 | 统一下载器系统 |
|--------|----------|----------------|
| 配置管理 | 分散、不一致 | 统一模板，8种场景 |
| 文件管理 | 硬编码规则 | 4种命名策略，5种目录结构 |
| 扩展性 | 代码耦合 | 模块化设计，可插拔组件 |
| 兼容性 | 需要重构 | 100% 向后兼容 |
| 测试覆盖 | 无测试 | 100% 集成测试通过 |

## 🏗️ 系统架构

```
unified_downloader/
├── config/                  # 统一配置系统
│   └── unified_config.py    # 配置管理、验证和模板
├── file_manager/            # 统一文件管理系统
│   └── unified_file_manager.py  # 文件命名、目录组织、智能分类
├── core/                    # 下载器核心
│   └── downloader_abstract.py   # 下载器接口和策略模式
├── adapters/                # 向后兼容适配器
│   └── legacy_adapter.py    # 对现有系统的兼容支持
└── examples/                # 配置示例和模板
    └── config_templates.yaml # 不同场景的配置模板
```

## 🎯 核心特性

### 1. 统一配置系统
- **配置接口标准化**：统一的配置数据结构和访问接口
- **格式兼容**：支持YAML、JSON和字典格式
- **配置验证**：自动验证配置有效性
- **默认值管理**：智能默认值和配置继承
- **配置模板**：预定义的场景化配置模板

### 2. 统一文件管理系统
- **灵活的命名策略**：
  - 标准格式：`日期_股票代码_公司名称_标题.pdf`
  - 原始格式：保留原始文件名
  - 紧凑格式：`股票代码_日期_序号.pdf`
  - 自定义格式：使用模板变量自定义

- **多种目录结构**：
  - 平铺模式（FLAT）：所有文件在同一目录
  - 按股票分组（BY_STOCK）：`/股票代码/文件`
  - 按日期分组（BY_DATE）：`/年/月/日/文件`
  - 按分类分组（BY_CATEGORY）：`/分类/文件`
  - 层级模式（HIERARCHICAL）：`/股票代码/分类/年月/文件`

- **智能分类系统**：
  - 基于关键词的自动分类
  - 优先级权重系统
  - 可自定义分类规则

- **符号链接支持**：
  - 多维度文件组织
  - 按日期、分类、公司等创建符号链接

### 3. 下载器抽象层
- **统一接口**：标准化的下载器接口
- **策略模式**：可插拔的下载策略
  - 标准下载策略
  - 速率限制策略
  - 代理下载策略
- **并发控制**：可配置的并发数和速率限制
- **错误处理**：统一的重试和错误恢复机制
- **性能监控**：实时统计和性能指标

### 4. 向后兼容
- **无缝迁移**：支持现有配置文件的自动转换
- **API兼容**：保持与现有系统的API兼容性
- **渐进式迁移**：支持新旧系统并存和逐步迁移
- **适配器模式**：为不同系统提供专门的适配器

## 🚀 快速开始

### 基本使用

```python
from unified_downloader.config.unified_config import ConfigManager
from unified_downloader.file_manager.unified_file_manager import UnifiedFileManager
from unified_downloader.core.downloader_abstract import UnifiedDownloader

# 1. 加载配置
config_manager = ConfigManager('config.yaml')

# 2. 创建文件管理器
file_manager = UnifiedFileManager(
    config_manager.config.file_management,
    config_manager.config.filter.classification_rules
)

# 3. 创建下载器
downloader = UnifiedDownloader(config_manager.config.to_dict())

# 4. 下载公告
announcement = {
    'stock_code': '00700',
    'date': '20250113',
    'title': '公司公告',
    'url': 'https://...',
    'company_name': '腾讯控股'
}

# 异步下载
import asyncio

async def download():
    result = await downloader.download_single(announcement)
    if result.success:
        # 保存文件
        file_path = file_manager.save_file(content, announcement)
        print(f"文件已保存: {file_path}")

asyncio.run(download())
```

### 使用配置模板

```python
# 使用预定义的配置模板
config_manager = ConfigManager()

# 获取性能优化配置
performance_config = config_manager.get_profile('performance')

# 获取归档配置
archival_config = config_manager.get_profile('archival')

# 获取监控配置
monitoring_config = config_manager.get_profile('monitoring')
```

### 向后兼容使用

```python
from unified_downloader.adapters.legacy_adapter import UnifiedAPIAdapter

# 创建统一API适配器
adapter = UnifiedAPIAdapter('config.yaml')

# 使用main.py风格的接口
main_adapter = adapter.get_adapter('main')
save_path, count = await main_adapter.download_announcements_unified(task)

# 使用监控系统风格的接口
monitor_adapter = adapter.get_adapter('monitor')
file_path = await monitor_adapter.download_announcement(announcement)

# 使用异步下载器风格的接口
async_adapter = adapter.get_adapter('async')
path, downloaded, skipped = await async_adapter.run_download(task)
```

## 📁 配置模板说明

系统提供了8个预定义的配置模板，适用于不同场景：

### 1. **default** - 默认配置
- 适合大多数日常使用场景
- 平衡的性能和功能设置
- 标准的文件命名和层级目录结构

### 2. **minimal** - 最小配置
- 用于快速测试和验证
- 最简单的配置，关闭大部分高级功能
- 单线程下载，平铺目录结构

### 3. **performance** - 性能优化配置
- 适用于大批量下载任务
- 高并发设置（10个并发连接）
- 简化的目录结构和命名规则
- 关闭进度显示和智能分类以提升性能

### 4. **archival** - 归档配置
- 用于长期存储和完整组织
- 完整的层级目录结构
- 启用符号链接多维度组织
- 更多重试次数确保完整性
- 详细的智能分类规则

### 5. **monitoring** - 实时监控配置
- 配合监控系统使用
- 按日期组织便于查看最新公告
- 适度的并发和速率限制
- 定期休息避免被封禁
- 关键事件过滤

### 6. **development** - 开发测试配置
- 用于开发和调试
- 详细的日志输出
- 自定义命名模板便于追踪
- 小规模测试数据集

### 7. **analysis** - 数据分析配置
- 用于数据挖掘和分析
- 按类型分组便于批量分析
- 关注特定类型的公告
- 财务相关关键词过滤

### 8. **compliance** - 合规审计配置
- 满足监管和审计要求
- 完整的文件信息保留
- 低并发高可靠性设置
- 详细的审计日志
- 保持原始文件结构

## 🔧 高级功能

### 智能分类规则配置

```yaml
classification_rules:
  ipo:
    chinese: ["首次公開發售", "IPO", "新股上市"]
    english: ["Initial Public Offering", "IPO"]
    folder_name: "IPO"
    priority: 90  # 优先级（0-100）
    weight: 1.0   # 权重（0-1）

  financial_report:
    chinese: ["年報", "中期報告", "季度報告"]
    english: ["Annual Report", "Interim Report"]
    folder_name: "财务报告"
    priority: 70
    weight: 0.8
```

### 自定义文件命名模板

```yaml
file_management:
  naming_strategy: "custom"
  naming_template: "{year}/{month}/{stock_code}_{date}_{category}_{title}"
```

可用的模板变量：
- `{date}` - 公告日期（YYYYMMDD）
- `{year}`, `{month}`, `{day}` - 日期组件
- `{stock_code}` - 股票代码
- `{company_name}` - 公司名称
- `{title}` - 公告标题
- `{category}` - 分类名称
- `{doc_id}` - 文档ID
- `{timestamp}` - 当前时间戳

### 下载策略扩展

```python
from unified_downloader.core.downloader_abstract import DownloadStrategy

class CustomStrategy(DownloadStrategy):
    """自定义下载策略"""

    async def download(self, task):
        # 实现自定义下载逻辑
        pass

    async def validate(self, content):
        # 实现自定义验证逻辑
        pass

# 使用自定义策略
custom_strategy = CustomStrategy(config)
downloader = UnifiedDownloader(config, strategy=custom_strategy)
```

## 📊 统计和监控

```python
# 获取文件管理统计
stats = file_manager.get_statistics()
print(f"总文件数: {stats['total_files']}")
print(f"总大小: {stats['total_size_readable']}")
print(f"按股票分布: {stats['by_stock']}")
print(f"按分类分布: {stats['by_category']}")

# 获取下载器统计
download_stats = downloader.get_statistics()
print(f"下载成功: {download_stats['successful_downloads']}")
print(f"下载失败: {download_stats['failed_downloads']}")
print(f"平均速度: {download_stats['average_speed_readable']}")
```

## 🔄 迁移指南

### 从现有系统迁移

1. **备份现有配置和数据**
   ```bash
   cp config.yaml config.yaml.backup
   cp -r hkexann hkexann_backup
   ```

2. **转换配置文件**
   ```python
   from unified_downloader.config.unified_config import ConfigManager

   # 加载旧配置
   config_manager = ConfigManager('config.yaml')

   # 保存为新格式
   config_manager.save('unified_config.yaml')
   ```

3. **重新组织现有文件**
   ```python
   from unified_downloader.file_manager.unified_file_manager import UnifiedFileManager

   file_manager = UnifiedFileManager(config.file_management)

   # 重新组织现有文件
   results = file_manager.organize_existing_files('hkexann')
   print(f"移动文件: {len(results['moved'])}")
   print(f"创建符号链接: {len(results['symlinked'])}")
   ```

4. **更新代码引用**
   ```python
   # 旧代码
   from main import HKEXDownloader

   # 新代码
   from unified_downloader.adapters.legacy_adapter import MainPyAdapter
   adapter = MainPyAdapter('config.yaml')
   ```

## 🐛 故障排除

### 常见问题

1. **配置验证失败**
   ```python
   errors = config_manager.validate()
   if errors:
       print("配置错误:", errors)
   ```

2. **文件名过长**
   - 调整`max_filename_length`配置
   - 使用紧凑命名策略

3. **下载速度慢**
   - 增加`max_concurrent`值
   - 使用性能优化配置模板
   - 关闭不必要的功能（智能分类、进度条等）

4. **被服务器封禁**
   - 减少`requests_per_second`
   - 增加`min_delay`和`max_delay`
   - 启用休息功能

## 📝 最佳实践

### 1. 选择合适的配置模板
- 日常使用：`default`
- 大批量下载：`performance`
- 长期存储：`archival`
- 实时监控：`monitoring`

### 2. 文件组织策略
- 股票数量少：使用`BY_STOCK`结构
- 时间跨度大：使用`BY_DATE`结构
- 需要多维度查询：启用符号链接
- 存储空间有限：使用紧凑命名

### 3. 性能优化
- 合理设置并发数（建议3-10）
- 使用速率限制避免封禁
- 定期休息保持稳定性
- 关闭不必要的日志和进度显示

### 4. 错误处理
- 设置合理的重试次数（3-5次）
- 使用指数退避策略
- 记录详细错误日志
- 实现失败任务重新下载机制

## 🤝 贡献指南

欢迎贡献代码、报告问题或提出建议。请遵循以下步骤：

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 📄 许可证

本项目采用MIT许可证。详见LICENSE文件。

## 📧 联系方式

如有问题或建议，请通过以下方式联系：
- 提交Issue
- 发送邮件至项目维护者

---

*最后更新：2025年1月14日*