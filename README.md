# HKEX 公告下载器

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

一个功能强大的香港交易所（HKEX）上市公司公告批量下载工具，支持异步高速下载、自动分类、定时任务等功能。

## 功能特点

- 📥 **批量下载**：支持单个或多个股票的公告批量下载
- 🚀 **异步高速下载**：使用异步技术大幅提升下载速度
- 📂 **自动分类**：根据公告类型自动整理到不同文件夹
- 🔍 **关键字搜索**：支持按关键字筛选特定类型的公告
- 🗄️ **数据库支持**：可从数据库读取股票列表进行批量下载
- ⏰ **定时任务**：支持守护进程模式，定时自动下载最新公告
- 🌐 **繁简转换**：智能识别繁体/简体中文关键字
- 📊 **进度显示**：实时显示下载进度和统计信息

## 快速开始

### 安装

1. 克隆仓库：
```bash
git clone https://github.com/yourusername/hkexann.git
cd hkexann
```

2. 安装依赖：
```bash
# 基础依赖
pip install -r requirements.txt

# 异步下载依赖（可选，大幅提升速度）
pip install aiohttp aiofiles tenacity tqdm

# 守护进程依赖（可选）
pip install schedule psutil

# 繁简转换依赖（可选）
pip install opencc-python-reimplemented
```

### 基础用法

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

### 自动分类功能

启用公告自动分类，公告将按类型保存到不同文件夹：
```yaml
announcement_categories:
  enabled: true
```

分类示例：
```
HKEX/
└── 00700/
    ├── 01_业绩报告/
    │   ├── 年报/
    │   ├── 中期报告/
    │   └── 季报/
    ├── 02_交易公告/
    │   ├── 须予披露交易/
    │   └── 关连交易/
    └── 03_公司管治/
        └── 股东大会/
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
- `filename_length`: 文件名最大长度
- `language`: 语言设置（zh/en）
- `max_results`: 每次搜索最大结果数
- `verbose_logging`: 是否启用详细日志

### 高级设置
- `retry_attempts`: 下载失败重试次数
- `request_delay`: 请求间隔时间（秒）
- `timeout`: 请求超时时间（秒）
- `overwrite_existing`: 是否覆盖已存在文件

### 异步下载设置
```yaml
async:
  max_concurrent: 10          # 最大并发数
  requests_per_second: 2      # 每秒请求数限制
  rest:
    enabled: true            # 启用休息机制
    work_minutes: 30         # 工作时长（分钟）
    rest_minutes: 5          # 休息时长（分钟）
```

## 常见问题

### Q: 如何提升下载速度？
A: 使用 `--async` 参数启用异步模式，可以大幅提升下载速度。

### Q: 下载失败怎么办？
A: 程序会自动重试失败的下载。如果持续失败，可能是网络问题或被限流，建议：
- 减小 `max_concurrent` 并发数
- 增加 `request_delay` 请求延迟
- 启用休息机制避免被封

### Q: 如何只下载特定类型的公告？
A: 使用 `-k` 参数指定关键字，如：
```bash
python main.py -s 00700 -k "年報" "財務報告"
```

### Q: 支持哪些股票代码格式？
A: 支持标准的5位数股票代码，如 00700（腾讯）、00001（长和）等。

## 依赖项

- Python 3.8+
- requests
- pyyaml
- pymysql (可选，数据库功能)
- aiohttp, aiofiles (可选，异步下载)
- schedule, psutil (可选，守护进程)
- opencc (可选，繁简转换)

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 免责声明

本工具仅供学习和研究使用，请遵守相关法律法规和网站服务条款。下载的公告文件版权归香港交易所及相关上市公司所有。

## 作者

Victor Suen

## 更新日志

### v2.0 (2024-01)
- 新增异步下载模式，大幅提升下载速度
- 新增守护进程模式，支持定时自动下载
- 新增公告自动分类功能
- 优化数据库支持，支持批量处理
- 改进错误处理和重试机制