# 完整设置指南：从历史数据到每日增量

## 🎯 目标

今天开始获取所有股票的公告，往后每天只获取新的公告。

## 📋 前提条件

- ✅ Python 3.7+
- ✅ MySQL数据库（包含股票列表）
- ✅ 网络连接

## 🚀 第一阶段：获取历史公告（一次性）

### 1. 安装和配置

```bash
# 克隆项目
git clone https://github.com/your-username/hkexann.git
cd hkexann

# 安装依赖
pip install -r requirements.txt

# 安装守护者进程依赖
pip install schedule psutil
```

### 2. 配置数据库连接

编辑 `config.yaml`：

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
  status_filter: ["normal"]  # 只获取活跃股票
```

### 3. 测试数据库连接

```bash
python main.py --test-db
```

预期输出：
```
✓ 数据库连接测试成功
```

### 4. 首次完整下载

```bash
# 从数据库获取所有股票并下载历史公告
python main.py --db-stocks
```

这个过程可能需要几个小时，取决于股票数量和网络速度。

## 🔄 第二阶段：启用每日增量下载

### 1. 配置守护者进程

编辑 `config.yaml`，确保包含以下配置：

```yaml
# 守护者进程配置
daemon:
  enabled: true
  schedule:
    times: ["09:30", "18:30"]       # 每天9:30和18:30执行
  runtime:
    check_interval: 60
    max_retries: 3
    retry_delay: 300
  process:
    pid_file: "hkex_daemon.pid"
    log_file: "hkex_daemon.log"
  tasks:
    run_all_enabled: true
    incremental_mode: true          # 启用增量模式
    incremental_days: 3             # 只获取最近3天
    database_tasks:
      refresh_connection: true
      show_progress: true

# 下载任务配置
download_tasks:
  - name: "数据库活跃股票任务"
    from_database: true             # 从数据库获取股票
    query: null                     # 使用默认查询
    start_date: "2024-01-01"        # 会被增量模式覆盖
    end_date: "today"
    keywords: []
    enabled: true
    database_config:
      batch_size: 50                # 每批50个股票
      delay_between_batches: 5      # 批次间延迟5秒
      skip_on_error: true           # 跳过失败的股票
```

### 2. 测试配置

```bash
python main.py --daemon-test
```

预期输出：
```
测试守护者进程配置...
✓ 守护者进程已启用
✓ 调度时间: 09:30, 18:30
✓ 可执行任务: 1 个
  - 数据库任务: 1 个
✓ 数据库任务配置:
  - 数据库活跃股票任务: 默认查询 (获取活跃股票)
✓ 增量下载模式: 启用 (最近3天)
✓ PID文件路径可写: hkex_daemon.pid
✓ 日志文件路径可写: hkex_daemon.log

配置测试完成
```

### 3. 启动守护者进程

```bash
# 启动守护者进程
python main.py --daemon-start
```

### 4. 验证运行状态

```bash
# 查看状态
python main.py --daemon-status
```

预期输出：
```
守护者进程状态:
  启用状态: 是
  运行状态: 运行中
  进程ID: 12345
  启动时间: 2024-01-15 09:00:00
  调度时间: 09:30, 18:30
  下次执行: 2024-01-15 18:30:00
```

## 📊 工作原理

### 增量模式的工作方式

1. **每天09:30**：
   - 从数据库获取所有活跃股票
   - 只下载最近3天的公告
   - 自动跳过已存在的文件

2. **每天18:30**：
   - 重复上述过程
   - 确保获取当天发布的新公告

### 为什么选择3天？

- **覆盖周末**：确保周末发布的公告不会遗漏
- **网络容错**：如果某次执行失败，下次还能补上
- **效率平衡**：既保证完整性又不会下载太多重复内容

## 🔧 管理和监控

### 日常管理命令

```bash
# 查看状态
python main.py --daemon-status

# 停止守护者进程
python main.py --daemon-stop

# 重启守护者进程
python main.py --daemon-restart

# 查看日志
tail -f hkex_daemon.log
```

### 便捷脚本

```bash
# 使用Python控制脚本
python daemon_control.py status
python daemon_control.py stop
python daemon_control.py start

# Windows用户
daemon.bat status
daemon.bat stop
daemon.bat start
```

## 📈 预期结果

### 第一阶段完成后
- 获得所有股票的完整历史公告
- 文件按股票代码和分类整理
- 建立完整的公告数据库

### 第二阶段运行后
- 每天自动获取新公告
- 只下载增量数据，高效快速
- 无需人工干预，全自动运行

## 🔍 故障排除

### 常见问题

1. **数据库连接失败**
   ```bash
   python main.py --test-db
   ```

2. **守护者进程启动失败**
   ```bash
   python main.py --daemon-test
   ```

3. **下载速度慢**
   - 调整 `batch_size` 和 `delay_between_batches`
   - 检查网络连接

### 日志文件

- `hkex_downloader.log` - 主程序日志
- `hkex_daemon.log` - 守护者进程日志

## 💡 最佳实践

1. **首次运行**：建议在非交易时间进行
2. **监控日志**：定期检查日志文件
3. **磁盘空间**：确保有足够的存储空间
4. **备份配置**：保存配置文件的备份
5. **定期维护**：每月检查一次系统状态

## 🎯 成功标志

当您看到以下情况时，说明系统运行正常：

- ✅ 守护者进程状态显示"运行中"
- ✅ 日志显示定期执行任务
- ✅ 新公告文件按时下载
- ✅ 无重复下载现象
- ✅ 系统资源使用正常

恭喜！您已经成功部署了一个完全自动化的港交所公告下载系统！
