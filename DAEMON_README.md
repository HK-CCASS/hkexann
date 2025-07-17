# HKEX 公告下载器 - 守护者进程模式

## 概述

守护者进程模式允许HKEX公告下载器在后台自动运行，按照预设的时间表定期下载公告，无需手动干预。

## 功能特性

- ✅ **定时调度**: 支持每天多个时间点自动执行下载任务
- ✅ **后台运行**: 守护者进程在后台持续运行，不影响其他工作
- ✅ **进程管理**: 支持启动、停止、重启、状态查看等操作
- ✅ **错误恢复**: 自动重试失败的任务，提高稳定性
- ✅ **日志记录**: 详细的运行日志，便于监控和调试
- ✅ **配置灵活**: 通过配置文件灵活设置调度时间和任务参数

## 安装依赖

守护者进程模式需要额外的Python包：

```bash
pip install schedule psutil
```

或者安装完整的依赖：

```bash
pip install -r requirements.txt
```

## 配置设置

在 `config.yaml` 文件中配置守护者进程：

```yaml
# 守护者进程配置
daemon:
  enabled: true                     # 启用守护者进程模式
  schedule:
    times: ["09:00", "18:00"]       # 每天执行时间 (24小时制)
  
  runtime:
    check_interval: 60              # 检查调度间隔 (秒)
    max_retries: 3                  # 任务失败最大重试次数
    retry_delay: 300                # 重试间隔 (秒)
    
  process:
    pid_file: "hkex_daemon.pid"     # PID文件路径
    log_file: "hkex_daemon.log"     # 守护者进程日志文件
    
  tasks:
    run_all_enabled: true           # 执行所有启用的下载任务
    run_on_startup: false           # 是否在启动时立即执行一次
```

## 使用方法

### 1. 命令行方式

```bash
# 测试配置
python main.py --daemon-test

# 启动守护者进程
python main.py --daemon-start

# 查看状态
python main.py --daemon-status

# 停止守护者进程
python main.py --daemon-stop

# 重启守护者进程
python main.py --daemon-restart
```

### 2. 便捷脚本方式

#### Windows用户：

```cmd
# 启动
daemon.bat start

# 查看状态
daemon.bat status

# 停止
daemon.bat stop

# 重启
daemon.bat restart

# 测试配置
daemon.bat test
```

#### Linux/macOS用户：

```bash
# 启动
./daemon.sh start

# 查看状态
./daemon.sh status

# 停止
./daemon.sh stop

# 重启
./daemon.sh restart

# 测试配置
./daemon.sh test
```

### 3. Python脚本方式

```bash
# 使用专用的控制脚本
python daemon_control.py start
python daemon_control.py status
python daemon_control.py stop
```

## 运行流程

1. **启动**: 守护者进程启动后会创建PID文件，设置调度任务
2. **调度**: 按照配置的时间点自动执行下载任务
3. **执行**: 运行所有启用的下载任务，支持失败重试
4. **日志**: 记录详细的运行日志到指定文件
5. **监控**: 可随时查看进程状态和运行情况

## 日志文件

- **主日志**: `hkex_downloader.log` - 包含所有操作的日志
- **守护者日志**: `hkex_daemon.log` - 守护者进程专用日志

## 故障排除

### 常见问题

1. **依赖包未安装**
   ```
   错误: 守护者进程模式需要安装额外依赖
   解决: pip install schedule psutil
   ```

2. **配置未启用**
   ```
   错误: 守护者进程模式未启用
   解决: 在config.yaml中设置 daemon.enabled: true
   ```

3. **权限问题**
   ```
   错误: PID文件路径不可写
   解决: 检查文件路径权限或更改为可写目录
   ```

4. **进程已运行**
   ```
   错误: 守护者进程已在运行
   解决: 先停止现有进程或使用restart命令
   ```

### 调试方法

1. **测试配置**: `python main.py --daemon-test`
2. **查看日志**: 检查 `hkex_daemon.log` 文件
3. **检查状态**: `python main.py --daemon-status`

## 最佳实践

1. **测试配置**: 启动前先运行配置测试
2. **合理调度**: 避免在系统繁忙时段执行大量下载
3. **监控日志**: 定期检查日志文件，及时发现问题
4. **备份配置**: 保存好配置文件的备份
5. **资源监控**: 注意磁盘空间和网络使用情况

## 注意事项

- 守护者进程会持续运行，请确保系统稳定
- 建议在服务器或长期运行的机器上使用
- 定期检查下载的文件和日志
- 如需修改配置，请重启守护者进程使配置生效
