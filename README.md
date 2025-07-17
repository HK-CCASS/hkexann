# HKEX 公告下载器 (HKEX Announcement Downloader)

一个功能强大的港交所公告自动下载工具，支持批量下载、智能分类、数据库集成等功能。

## 📖 目录

- [⚡ 快速开始](#-快速开始---完整部署方案)
- [🚀 功能特性](#-功能特性)
- [🛠️ 安装](#️-安装)
- [⚙️ 配置](#️-配置)
- [🎯 使用方法](#-使用方法)
  - [🤖 守护者进程模式](#-守护者进程模式-新功能)
  - [🚀 完整部署方案](#-完整部署方案从历史数据到每日增量)
- [📚 相关文档](#-相关文档)

## 🚀 功能特性

- **批量下载**: 支持单个或多个股票的公告批量下载
- **智能分类**: 基于港交所官方分类自动整理公告到不同文件夹
- **数据库集成**: 支持从MySQL数据库读取股票列表
- **关键字搜索**: 支持按关键字筛选特定类型的公告
- **日期范围**: 灵活的日期范围设置，支持相对日期
- **配置驱动**: 通过YAML配置文件管理所有设置
- **命令行界面**: 提供丰富的命令行参数
- **日志记录**: 详细的操作日志和错误追踪
- **繁简转换**: 支持繁简体中文关键字匹配
- **🆕 守护者进程**: 支持后台定时自动下载，无需手动干预
- **🔄 增量下载**: 智能增量模式，只下载最新公告避免重复
- **📊 完整方案**: 提供从历史数据到每日增量的完整部署方案

## ⚡ 快速开始 - 完整部署方案

**想要今天开始获取所有股票公告，往后每天只获取新公告？**

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置数据库连接（编辑 config.yaml）
# 3. 首次获取所有历史公告
python main.py --db-stocks

# 4. 启用每日增量下载
python main.py --daemon-start
```

详细步骤请参考：[🚀 完整部署方案](#-完整部署方案从历史数据到每日增量) 或 [📋 完整设置指南](COMPLETE_SETUP_GUIDE.md)

## 📋 系统要求

- Python 3.7+
- Windows/Linux/macOS
- 网络连接
- MySQL数据库（可选，用于自动获取股票列表）

## 🛠️ 安装

### 1. 克隆项目
```bash
git clone https://github.com/your-username/hkexann.git
cd hkexann
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置文件
复制配置模板并修改：
```bash
cp config_template.yaml config.yaml
```

## ⚙️ 配置

编辑 `config.yaml` 文件：

```yaml
# 基本设置
settings:
  save_path: "C:/Users/Administrator/Desktop"  # 下载保存路径
  filename_length: 220                         # 文件名最大长度
  language: "zh"                              # 语言设置 (zh/en)
  max_results: 500                            # 每次搜索最大结果数
  verbose_logging: true                       # 详细日志
  log_file: "hkex_downloader.log"            # 日志文件

# 下载任务配置
download_tasks:
  - name: "中华煤气公告下载"
    stock_code: "00003"
    start_date: "2024-01-01"
    end_date: "today"
    keywords: []  # 空数组表示下载所有公告
    enabled: true
```

## 🎯 使用方法

### 命令行使用

#### 基本用法
```bash
# 使用配置文件中的任务
python main.py

# 下载单个股票的所有公告
python main.py -s 00001

# 下载指定关键字的公告
python main.py -s 00001 -k "财务报告" "年报"

# 指定日期范围
python main.py -s 00001 --start 2024-01-01 --end 2024-12-31

# 使用自定义配置文件
python main.py --config my_config.yaml
```

#### 高级功能
```bash
# 检查配置文件
python main.py --check-config

# 列出所有任务
python main.py --list-tasks

# 运行指定任务
python main.py --run-task "中华煤气公告下载"

# 测试数据库连接
python main.py --test-db

# 从数据库获取股票列表并下载
python main.py --db-stocks
```

### 🤖 守护者进程模式 (新功能)

守护者进程模式允许程序在后台自动运行，按照预设时间定期下载公告。

#### 安装额外依赖
```bash
pip install schedule psutil
```

#### 基本使用
```bash
# 测试守护者进程配置
python main.py --daemon-test

# 启动守护者进程
python main.py --daemon-start

# 查看守护者进程状态
python main.py --daemon-status

# 停止守护者进程
python main.py --daemon-stop

# 重启守护者进程
python main.py --daemon-restart
```

#### 便捷脚本
```bash
# 使用Python控制脚本（推荐）
python daemon_control.py start
python daemon_control.py status
python daemon_control.py stop

# Windows用户
daemon.bat start
daemon.bat status

# Linux/macOS用户
./daemon.sh start
./daemon.sh status
```

#### 配置守护者进程
在 `config.yaml` 中添加：
```yaml
daemon:
  enabled: true                     # 启用守护者进程
  schedule:
    times: ["09:00", "18:00"]       # 每天9点和18点执行
  runtime:
    check_interval: 60              # 检查间隔(秒)
    max_retries: 3                  # 失败重试次数
  tasks:
    run_all_enabled: true           # 执行所有启用的任务
    run_on_startup: false           # 启动时立即执行
```

详细使用说明请参考：[守护者进程使用指南](DAEMON_USAGE.md)

#### 🗄️ 数据库任务（自动获取活跃股票）

守护者进程支持每天自动从数据库获取活跃股票列表并下载公告：

```yaml
download_tasks:
  - name: "数据库活跃股票任务"
    from_database: true              # 从数据库获取股票
    query: null                      # 使用默认查询获取活跃股票
    start_date: "2024-01-01"
    end_date: "today"
    enabled: true
    database_config:
      batch_size: 50                 # 每批处理50个股票
      delay_between_batches: 5       # 批次间延迟5秒
```

数据库任务详细说明：[数据库任务指南](DATABASE_TASKS.md)

#### 🚀 完整部署方案：从历史数据到每日增量

**场景**：今天开始获取所有股票的公告，往后每天只获取新的公告

##### 第一阶段：获取历史公告（一次性）

```bash
# 1. 安装依赖
pip install schedule psutil

# 2. 测试数据库连接
python main.py --test-db

# 3. 首次完整下载（使用数据库获取所有股票）
python main.py --db-stocks
```

##### 第二阶段：启用每日增量下载

修改 `config.yaml` 配置：

```yaml
daemon:
  enabled: true
  schedule:
    times: ["09:30", "18:30"]       # 每天两次
  tasks:
    run_all_enabled: true
    incremental_mode: true          # 启用增量模式
    incremental_days: 3             # 只获取最近3天

download_tasks:
  - name: "数据库活跃股票任务"
    from_database: true             # 自动获取所有活跃股票
    query: null                     # 默认查询获取活跃股票
    start_date: "2024-01-01"
    end_date: "today"
    enabled: true
    database_config:
      batch_size: 50                # 每批50个股票
      delay_between_batches: 5      # 批次间延迟5秒
      skip_on_error: true           # 跳过失败的股票
```

启动守护者进程：

```bash
# 测试配置
python main.py --daemon-test

# 启动守护者进程
python main.py --daemon-start

# 查看运行状态
python main.py --daemon-status
```

**结果**：系统将每天自动从数据库获取所有活跃股票，并只下载最近3天的公告，确保获取新公告的同时避免重复下载。

### 配置文件任务

支持多种任务类型：

#### 1. 单个股票任务
```yaml
- name: "腾讯控股公告"
  stock_code: "00700"
  start_date: "2024-01-01"
  end_date: "today"
  keywords: ["业绩", "财务报告"]
  enabled: true
```

#### 2. 多个股票任务
```yaml
- name: "蓝筹股公告"
  stock_codes: ["00001", "00002", "00003", "00005"]
  start_date: "2024-06-01"
  end_date: "today"
  keywords: ["业绩", "分派"]
  enabled: true
```

#### 3. 数据库任务
```yaml
- name: "数据库股票公告"
  from_database: true
  query: "SELECT stockCode FROM issue WHERE status = 'normal'"
  start_date: "2024-01-01"
  end_date: "today"
  keywords: []
  enabled: true
```

## 🗄️ 数据库集成

支持MySQL数据库集成，可以从数据库读取股票列表：

```yaml
database:
  enabled: true
  host: "localhost"
  port: 3306
  user: "root"
  password: "your_password"
  database: "ccass"
  default_table: "issue"
  fields:
    stock_code: "stockCode"
    stock_name: "stockName"
    status: "status"
    issue_id: "issueID"
  status_filter: ["normal"]
```

## 📁 智能分类

程序会根据公告标题自动分类到不同文件夹：

```
HKEX/
├── 00001/
│   ├── 01_业绩报告/
│   │   ├── 年报/
│   │   ├── 中期报告/
│   │   └── 季报/
│   ├── 02_交易公告/
│   │   ├── 须予披露交易/
│   │   └── 关连交易/
│   └── 03_公司管治/
│       ├── 股东大会/
│       └── 董事会决议/
```

## 📝 日志

程序会生成详细的日志文件 `hkex_downloader.log`：

```
2025-07-17 09:57:07,832 - INFO - 开始下载任务: 默认下载任务
2025-07-17 09:57:07,832 - INFO - 股票代码: 00081, 日期范围: 2024-01-01 至 2025-07-17
2025-07-17 09:57:08,055 - INFO - 找到 105 个符合条件的公告
```

## 🔧 故障排除

### 常见问题

1. **网络连接错误**
   - 检查网络连接
   - 确认港交所网站可访问

2. **配置文件错误**
   ```bash
   python main.py --check-config
   ```

3. **数据库连接失败**
   ```bash
   python main.py --test-db
   ```

4. **文件权限问题**
   - 确保保存路径有写入权限
   - 检查磁盘空间

## 📄 文件结构

```
hkexann/
├── main.py                 # 主程序文件
├── config.yaml            # 配置文件
├── config_template.yaml   # 配置模板
├── hkex_downloader.log    # 日志文件
├── README.md              # 说明文档
└── requirements.txt       # 依赖列表
```

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📚 API参考

### 命令行参数

| 参数 | 简写 | 说明 | 示例 |
|------|------|------|------|
| `--config` | `-c` | 配置文件路径 | `-c my_config.yaml` |
| `--stock-code` | `-s` | 股票代码(5位) | `-s 00001` |
| `--keywords` | `-k` | 搜索关键字 | `-k "年报" "财务"` |
| `--start` | | 开始日期 | `--start 2024-01-01` |
| `--end` | | 结束日期 | `--end today` |
| `--save-path` | | 保存路径 | `--save-path /path/to/save` |
| `--check-config` | | 检查配置文件 | `--check-config` |
| `--list-tasks` | | 列出所有任务 | `--list-tasks` |
| `--run-task` | | 运行指定任务 | `--run-task "任务名"` |
| `--test-db` | | 测试数据库连接 | `--test-db` |
| `--db-stocks` | | 从数据库下载 | `--db-stocks` |
| `--verbose` | `-v` | 详细输出 | `-v` |

### 配置文件结构

```yaml
settings:                    # 基本设置
  save_path: string         # 保存路径
  filename_length: int      # 文件名长度限制
  language: string          # 语言 (zh/en)
  max_results: int          # 最大结果数
  verbose_logging: bool     # 详细日志
  log_file: string          # 日志文件路径

date_range:                  # 默认日期范围
  start_date: string        # 开始日期
  end_date: string          # 结束日期

download_tasks:              # 下载任务列表
  - name: string            # 任务名称
    stock_code: string      # 股票代码
    stock_codes: list       # 多个股票代码
    start_date: string      # 开始日期
    end_date: string        # 结束日期
    keywords: list          # 关键字列表
    enabled: bool           # 是否启用
    from_database: bool     # 从数据库读取

database:                    # 数据库配置
  enabled: bool             # 是否启用
  host: string              # 主机地址
  port: int                 # 端口号
  user: string              # 用户名
  password: string          # 密码
  database: string          # 数据库名

advanced:                    # 高级设置
  retry_attempts: int       # 重试次数
  request_delay: float      # 请求间隔
  timeout: int              # 超时时间
  overwrite_existing: bool  # 覆盖已存在文件
```

## 💡 使用技巧

### 1. 批量下载多个股票
```yaml
download_tasks:
  - name: "港股通标的"
    stock_codes: ["00700", "00941", "01299", "02318"]
    start_date: "2024-01-01"
    end_date: "today"
    keywords: []
    enabled: true
```

### 2. 按类型筛选公告
```yaml
# 只下载财务报告
- name: "财务报告"
  stock_code: "00700"
  keywords: ["年报", "中期报告", "季报", "财务报告"]

# 只下载交易公告
- name: "交易公告"
  stock_code: "00700"
  keywords: ["收购", "合并", "须予披露", "关连交易"]
```

### 3. 定期任务设置
```bash
# 使用cron定期执行 (Linux/macOS)
0 9 * * 1-5 cd /path/to/hkexann && python main.py

# 使用任务计划程序 (Windows)
# 创建批处理文件 run_hkex.bat:
cd /d D:\py_pro\hkexann
python main.py
```

## 🔍 高级功能

### 自定义分类规则

可以在配置文件中自定义公告分类规则：

```yaml
announcement_categories:
  enabled: true
  "自定义分类":
    "子分类名":
      keywords: ["关键字1", "关键字2"]
      priority: 1  # 优先级，数字越小优先级越高
```

### 数据库查询示例

```yaml
# 查询特定行业股票
- name: "银行股"
  from_database: true
  query: "SELECT stockCode FROM issue WHERE stockName LIKE '%银行%' AND status = 'normal'"

# 查询市值前100的股票
- name: "大盘股"
  from_database: true
  query: "SELECT stockCode FROM issue ORDER BY marketCap DESC LIMIT 100"
```

## 📊 性能优化

### 1. 网络优化
```yaml
advanced:
  request_delay: 0.5      # 减少请求间隔 (注意不要过于频繁)
  timeout: 60             # 增加超时时间
  retry_attempts: 5       # 增加重试次数
```

### 2. 文件管理
```yaml
settings:
  filename_length: 200    # 适当的文件名长度

advanced:
  overwrite_existing: false  # 避免重复下载
```

## 👨‍💻 作者

Victor Suen

## 📜 许可证

MIT License

## 📚 相关文档

- [📋 完整设置指南](COMPLETE_SETUP_GUIDE.md) - **推荐** 从零开始的完整部署方案
- [守护者进程使用指南](DAEMON_USAGE.md) - 详细的守护者进程配置和使用方法
- [数据库任务指南](DATABASE_TASKS.md) - 数据库集成和自动获取股票的完整说明
- [守护者进程常见问题](DAEMON_FAQ.md) - 常见问题解答和故障排除
- [守护者进程详细说明](DAEMON_README.md) - 守护者进程的技术细节

## 🎯 典型使用场景

| 场景 | 配置方案 | 适用对象 |
|------|----------|----------|
| 个人研究 | 单股票 + 手动执行 | 个人投资者 |
| 小规模监控 | 多股票 + 定时执行 | 小型机构 |
| 大规模监控 | 数据库 + 守护者进程 | 大型机构 |
| 完整历史 + 增量 | 完整部署方案 | 专业用户 |

## 🙏 致谢

- 港交所提供的公开数据接口
- Python开源社区的优秀库

---

**免责声明**: 本工具仅供学习和研究使用，请遵守港交所网站的使用条款和相关法律法规。使用者需对使用本工具产生的任何后果承担责任。
