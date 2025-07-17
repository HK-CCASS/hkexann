# 守护者进程模式 - 快速使用指南

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install schedule psutil
```

### 2. 配置守护者进程
编辑 `config.yaml` 文件：
```yaml
daemon:
  enabled: true                     # 启用守护者进程
  schedule:
    times: ["09:00", "18:00"]       # 每天9点和18点执行
```

### 3. 测试配置
```bash
python main.py --daemon-test
```

### 4. 启动守护者进程
```bash
python main.py --daemon-start
```

### 5. 查看状态
```bash
python main.py --daemon-status
```

### 6. 停止守护者进程
```bash
python main.py --daemon-stop
```

## 📋 便捷脚本使用

### Python控制脚本（推荐）
```bash
# 测试配置
python daemon_control.py test

# 启动守护者进程
python daemon_control.py start

# 查看状态
python daemon_control.py status

# 停止守护者进程
python daemon_control.py stop

# 重启守护者进程
python daemon_control.py restart
```

### Windows批处理脚本
```cmd
daemon.bat test
daemon.bat start
daemon.bat status
daemon.bat stop
daemon.bat restart
```

### Linux/macOS Shell脚本
```bash
./daemon.sh test
./daemon.sh start
./daemon.sh status
./daemon.sh stop
./daemon.sh restart
```

## ⚙️ 配置选项

### 基本配置
```yaml
daemon:
  enabled: true                     # 是否启用守护者进程
  schedule:
    times: ["09:00", "18:00"]       # 执行时间列表
  
  runtime:
    check_interval: 60              # 检查间隔(秒)
    max_retries: 3                  # 最大重试次数
    retry_delay: 300                # 重试间隔(秒)
    
  tasks:
    run_all_enabled: true           # 执行所有启用的任务
    run_on_startup: false           # 启动时立即执行
    incremental_mode: true          # 增量下载模式
    incremental_days: 7             # 增量下载天数
```

### 高级配置
```yaml
daemon:
  process:
    pid_file: "hkex_daemon.pid"     # PID文件路径
    log_file: "hkex_daemon.log"     # 日志文件路径
    working_dir: "."                # 工作目录
```

## 📊 监控和日志

### 查看运行状态
```bash
python main.py --daemon-status
```

输出示例：
```
守护者进程状态:
  启用状态: 是
  运行状态: 运行中
  进程ID: 12345
  启动时间: 2024-01-15 09:00:00
  内存使用: 45.2 MB
  CPU使用: 0.1%
  调度时间: 09:00, 18:00
  下次执行: 2024-01-15 18:00:00
```

### 查看日志
```bash
# 查看主日志
tail -f hkex_downloader.log

# 查看守护者进程日志
tail -f hkex_daemon.log
```

## 🔧 故障排除

### 常见问题

1. **依赖包未安装**
   ```
   错误: 守护者进程模式需要安装额外依赖
   解决: pip install schedule psutil
   ```

2. **守护者进程未启用**
   ```
   错误: 守护者进程模式未启用
   解决: 在config.yaml中设置 daemon.enabled: true
   ```

3. **进程已在运行**
   ```
   错误: 守护者进程已在运行
   解决: 先停止现有进程或使用restart命令
   ```

### 调试步骤
1. 运行配置测试：`python main.py --daemon-test`
2. 检查日志文件：`hkex_daemon.log`
3. 查看进程状态：`python main.py --daemon-status`

## 💡 使用建议

1. **首次使用**：先运行配置测试确保一切正常
2. **调度时间**：避免在系统繁忙时段执行
3. **监控日志**：定期检查日志文件
4. **磁盘空间**：确保有足够的存储空间
5. **网络稳定**：确保网络连接稳定

## 📈 增量下载模式

### 什么是增量下载？
增量下载模式只获取最近几天的公告，而不是从任务配置的开始日期到今天的所有公告。这样可以：
- **提高效率**：避免重复下载已有的公告
- **节省时间**：只处理最新的数据
- **减少网络负载**：降低对港交所服务器的压力

### 配置增量下载
```yaml
daemon:
  tasks:
    incremental_mode: true          # 启用增量下载
    incremental_days: 7             # 获取最近7天的公告
```

### 工作原理
- **启用时**：忽略任务配置中的`start_date`，自动设置为"今天往前推N天"
- **禁用时**：使用任务配置中的完整日期范围

### 推荐设置
- **每日执行**：`incremental_days: 3` (获取最近3天，确保不遗漏)
- **每周执行**：`incremental_days: 10` (获取最近10天，覆盖周末)
- **首次运行**：建议先禁用增量模式，下载完整历史数据后再启用

## 🎯 典型使用场景

### 场景1：每日增量下载（推荐）
```yaml
daemon:
  enabled: true
  schedule:
    times: ["09:30"]  # 每天9:30执行
  tasks:
    run_all_enabled: true
    incremental_mode: true
    incremental_days: 3  # 获取最近3天
```

### 场景2：一天多次增量下载
```yaml
daemon:
  enabled: true
  schedule:
    times: ["09:00", "12:00", "18:00"]  # 一天三次
  tasks:
    incremental_mode: true
    incremental_days: 1  # 只获取今天的
```

### 场景3：完整历史下载
```yaml
daemon:
  enabled: true
  schedule:
    times: ["02:00"]  # 凌晨执行，避免影响交易时间
  tasks:
    incremental_mode: false  # 使用任务配置的完整日期范围
```

### 场景4：启动时立即执行
```yaml
daemon:
  enabled: true
  schedule:
    times: ["18:00"]
  tasks:
    run_on_startup: true  # 启动时立即执行一次
    incremental_mode: true
    incremental_days: 7
```
