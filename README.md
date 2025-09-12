# HKEX 公告下载器与监听系统

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

一个功能强大的香港交易所（HKEX）上市公司公告下载与实时监听系统，结合了经典下载器和企业级微服务架构，支持异步高速下载、实时监听、向量化处理、多数据库存储等完整功能。

## 系统架构

本项目采用双系统架构设计：

1. **经典下载器** (`main.py`) - 2100+行独立工具，适合批量历史数据下载
2. **增强监听系统** (`services/` + `start_enhanced_monitor.py`) - 18000+行微服务架构，提供企业级实时监听能力

## 核心功能

### 经典下载器功能
- 📥 **批量下载**：支持单个或多个股票的公告批量下载
- 🚀 **异步高速下载**：使用异步技术大幅提升下载速度
- 🎯 **纯净LONG_TEXT分类**：直接使用港交所官方LONG_TEXT作为分类结果，无任何映射转换
- 📂 **智能目录结构**：自动创建1-2-3级目录层次，基于LONG_TEXT智能解析
- 🔍 **关键字搜索**：支持按关键字筛选特定类型的公告  
- 🗄️ **数据库支持**：可从数据库读取股票列表进行批量下载
- ⏰ **定时任务**：支持守护进程模式，定时自动下载最新公告
- 🌐 **繁简转换**：智能识别繁体/简体中文关键字
- 📊 **进度显示**：实时显示下载进度和统计信息
- 🔍 **文件验证**：自动检测和处理无效下载文件（如2KB错误页面）

### 增强监听系统功能
- 🔄 **实时监听**：每5分钟自动轮询港交所API，获取最新公告
- 🎯 **双重过滤**：股票过滤器 + 公告类型过滤器，精准筛选目标内容
- 💾 **多数据库支持**：支持ClickHouse时序数据库、MySQL关系数据库、Milvus向量数据库
- 📄 **文档处理**：自动PDF文本提取、向量化处理
- 🔍 **语义搜索**：基于4096维向量的语义相似度搜索
- 🏥 **健康监控**：完整的系统健康检查和故障恢复机制
- ⚡ **高性能架构**：基于asyncio的异步微服务架构
- 🔧 **服务管理**：连接池、重试机制、错误恢复等企业级特性

## 快速开始

### 安装

1. 克隆仓库：
```bash
git clone https://github.com/yourusername/hkexann.git
cd hkexann
```

2. 环境配置：
```bash
# 复制环境变量模板
cp .env.template .env

# 编辑环境变量，配置数据库连接等
vim .env
```

3. 安装依赖：
```bash
# 基础依赖
pip install -r requirements.txt

# 异步下载依赖（可选，大幅提升速度）
pip install aiohttp aiofiles tenacity tqdm

# 守护进程依赖（可选）
pip install schedule psutil

# 繁简转换依赖（可选）
pip install opencc-python-reimplemented

# 增强监听系统依赖（企业级功能）
pip install clickhouse-driver pymilvus sentence-transformers
```

### 基础用法

#### 经典下载器

1. **下载单个股票的公告**：
```bash
python main.py -s 00700
```

2. **指定日期范围**：
```bash
python main.py -s 00700 --start 2024-01-01 --end 2024-12-31
```

3. **搜索特定关键字**：
```bash
python main.py -s 00700 -k "財務報告" "年報"
```

4. **使用异步模式（推荐）**：
```bash
python main.py -s 00700 --async
```

#### 增强监听系统

1. **启动实时监听**：
```bash
python start_enhanced_monitor.py
```

2. **强制异步模式监听**：
```bash
HKEX_FORCE_ASYNC=true python main.py --config config.yaml
```

### 配置文件使用

1. 复制配置模板：
```bash
cp config_template.yaml config.yaml
```

2. 编辑 `config.yaml` 配置下载任务：
```yaml
download_tasks:
  - name: "腾讯公告下载"
    stock_code: "00700"
    start_date: "2024-01-01"
    end_date: "today"
    keywords: []  # 空表示下载所有公告
    enabled: true
```

3. 运行配置文件中的任务：
```bash
python main.py
```

## 高级功能

### 批量下载多个股票

在配置文件中设置股票列表：
```yaml
download_tasks:
  - name: "蓝筹股公告"
    stock_code: ["00001", "00002", "00003", "00005", "00700"]
    start_date: "2024-01-01"
    end_date: "today"
    enabled: true
```

### 从数据库获取股票列表

```yaml
download_tasks:
  - name: "数据库股票公告"
    from_database: true
    query: "SELECT stockCode FROM issue WHERE status = 'normal'"
    start_date: "2024-01-01"
    end_date: "today"
    enabled: true
```

配置数据库连接：
```yaml
database:
  enabled: true
  host: "localhost"
  port: 3306
  user: "root"
  password: "password"
  database: "ccass"
```

### 纯净LONG_TEXT分类功能

系统默认启用纯净LONG_TEXT分类，直接使用港交所官方分类：
```yaml
announcement_categories:
  enabled: true  # 启用纯净LONG_TEXT分类
```

**重要特性：**
- **零映射转换**：直接使用港交所官方LONG_TEXT作为分类结果
- **智能目录解析**：自动分析LONG_TEXT结构创建1-2-3级目录
- **官方权威性**：完全保持港交所原始分类标准

分类示例（基于港交所官方LONG_TEXT）：
```
HKEX/
└── 00700/
    ├── 通告及告示/
    │   ├── 股份發行/
    │   └── 其他-董事會決議案/
    ├── 財務報表/
    │   ├── 年度業績/
    │   └── 中期業績/
    ├── 企業管治/
    │   └── 股東大會通告/
    └── 月報表/
        └── 股份發行人的證券變動月報表/
```

### 守护进程模式

启用守护进程，实现定时自动下载：

1. 配置守护进程（在 config.yaml 中添加）：
```yaml
daemon:
  enabled: true
  schedule:
    times: ["09:00", "15:00"]  # 每天9点和15点执行
  tasks:
    incremental_mode: true      # 增量下载模式
    incremental_days: 7         # 下载最近7天的公告
```

2. 启动守护进程：
```bash
python main.py --daemon-start
```

3. 查看状态：
```bash
python main.py --daemon-status
```

4. 停止守护进程：
```bash
python main.py --daemon-stop
```

## 命令行参数

```bash
usage: main.py [-h] [-c CONFIG] [-s STOCK_CODE] [-k [KEYWORDS ...]] 
               [--start START] [--end END] [--save-path SAVE_PATH]
               [--async] [-v] [--check-config] [--list-tasks]
               [--run-task RUN_TASK] [--test-db] [--db-stocks]
               [--daemon-start] [--daemon-stop] [--daemon-status]

可选参数:
  -h, --help            显示帮助信息
  -c CONFIG             配置文件路径 (默认: config.yaml)
  -s STOCK_CODE         股票代码 (5位数字)
  -k KEYWORDS           搜索关键字
  --start START         开始日期 (YYYY-MM-DD)
  --end END             结束日期 (YYYY-MM-DD 或 today)
  --save-path SAVE_PATH 保存路径
  --async               使用异步模式下载（大幅提升速度）
  -v, --verbose         启用详细输出
  --check-config        检查配置文件格式
  --list-tasks          列出配置文件中的所有任务
  --run-task RUN_TASK   运行指定名称的任务
  --test-db             测试数据库连接
  --db-stocks           从数据库获取所有股票代码并下载
  --daemon-start        启动守护进程
  --daemon-stop         停止守护进程
  --daemon-status       查看守护进程状态
```

## 配置说明  

### 基本设置
- `save_path`: 下载文件保存路径
- `filename_length`: 文件名最大长度（建议220字符）
- `language`: 语言设置（zh/en）
- `max_results`: 每次搜索最大结果数
- `verbose_logging`: 是否启用详细日志

### 纯净LONG_TEXT分类配置
```yaml
announcement_categories:
  enabled: true  # 启用分类功能，直接使用港交所LONG_TEXT
```
**说明**：系统已简化为纯净分类模式，移除了复杂的映射规则，直接使用港交所官方LONG_TEXT作为分类结果。

### 高级设置
- `retry_attempts`: 下载失败重试次数
- `request_delay`: 请求间隔时间（秒）
- `timeout`: 请求超时时间（秒）
- `overwrite_existing`: 是否覆盖已存在文件

### 异步下载设置
```yaml
async:
  max_concurrent: 5           # 最大并发数（推荐5以避免限流）
  requests_per_second: 5      # 每秒请求数限制
  min_delay: 0.88            # 请求间最小延迟（秒）
  max_delay: 2.68            # 请求间最大延迟（秒）
  rest:
    enabled: true            # 启用休息机制防被封
    work_minutes: 30         # 工作时长（分钟）
    rest_minutes: 5          # 休息时长（分钟）
```

## 常见问题

### Q: 什么是纯净LONG_TEXT分类？
A: 纯净LONG_TEXT分类直接使用港交所官方的LONG_TEXT字段作为分类结果，不进行任何映射或转换，确保分类的权威性和准确性。系统会自动解析LONG_TEXT结构创建多级目录。

### Q: 如何提升下载速度？
A: 使用 `--async` 参数启用异步模式，可以大幅提升下载速度。推荐配置：
- `max_concurrent: 5`（避免过高导致限流）
- 启用休息机制防止被封禁

### Q: 下载的文件只有2KB是什么原因？
A: 这通常是下载失败或遇到错误页面。系统会自动检测小于5KB的文件并验证内容，如果是HTML错误页面会自动跳过。

### Q: 下载失败怎么办？
A: 程序会自动重试失败的下载。如果持续失败，可能是网络问题或被限流，建议：
- 减小 `max_concurrent` 并发数到3-5
- 增加 `min_delay` 和 `max_delay` 延迟时间
- 启用休息机制避免被封

### Q: 如何只下载特定类型的公告？
A: 使用 `-k` 参数指定关键字，如：
```bash
python main.py -s 00700 -k "年報" "財務報告"
```

### Q: 支持哪些股票代码格式？
A: 支持标准的5位数股票代码，如 00700（腾讯）、00001（长和）等。

## 系统依赖

### 基础依赖
- Python 3.8+
- requests
- pyyaml
- pymysql (数据库功能)

### 经典下载器依赖  
- aiohttp, aiofiles (异步下载)
- schedule, psutil (守护进程)
- opencc (繁简转换)

### 增强监听系统依赖
- clickhouse-driver (ClickHouse数据库)
- pymilvus (Milvus向量数据库) 
- sentence-transformers (文本向量化)
- asyncio (异步处理)
- tenacity (重试机制)

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 免责声明

本工具仅供学习和研究使用，请遵守相关法律法规和网站服务条款。下载的公告文件版权归香港交易所及相关上市公司所有。

## 作者

Victor Suen

## 更新日志

### v3.0 (2025-09) - 企业级架构升级
- **🏗️ 微服务架构**：新增18000+行企业级监听系统，基于services/目录的微服务架构
- **📡 实时监听**：增强监听系统支持每5分钟自动轮询港交所API
- **💾 多数据库支持**：集成ClickHouse时序数据库、Milvus向量数据库
- **📄 文档处理管道**：自动PDF文本提取与向量化处理
- **🔍 语义搜索**：基于4096维向量的语义相似度搜索
- **🎯 双重过滤系统**：股票过滤器 + 公告类型过滤器，精准筛选
- **🏥 健康监控**：完整的系统健康检查和故障恢复机制
- **⚡ 异步优化**：全面采用asyncio异步架构，大幅提升性能
- **🔧 企业级特性**：连接池、重试机制、错误恢复、服务管理

### v2.1 (2025-07) - 纯净分类系统
- **重大更新**：实现纯净LONG_TEXT分类系统
- **零映射转换**：直接使用港交所官方LONG_TEXT作为分类结果
- **智能目录解析**：自动创建1-2-3级目录层次结构
- **文件验证增强**：自动检测和处理无效下载文件
- **配置简化**：移除1335行复杂映射规则，配置文件减少90%
- **代码优化**：清理332行旧分类逻辑，提升系统性能
- **文件命名优化**：格式改为"时间_股票代码_公司名称_公告标题"

### v2.0 (2024-01) - 异步与自动化
- 新增异步下载模式，大幅提升下载速度
- 新增守护进程模式，支持定时自动下载  
- 新增公告自动分类功能
- 优化数据库支持，支持批量处理
- 改进错误处理和重试机制