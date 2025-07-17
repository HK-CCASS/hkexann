# 数据库任务功能 - 自动获取活跃股票

## 🎯 功能概述

数据库任务功能允许守护者进程每天自动从MySQL数据库获取活跃股票列表，然后为这些股票下载最新的公告。这样可以确保始终跟踪最新的活跃股票，无需手动维护股票列表。

## ✅ 已实现功能

- ✅ **自动获取活跃股票**：从数据库读取符合条件的股票列表
- ✅ **批量处理**：支持分批处理大量股票，避免系统负载过高
- ✅ **增量下载**：支持只下载最近几天的公告
- ✅ **错误处理**：单个股票失败不影响其他股票的处理
- ✅ **进度监控**：详细的处理进度和统计信息
- ✅ **守护者进程集成**：完全集成到定时调度系统中

## 🔧 配置方法

### 1. 数据库连接配置

在 `config.yaml` 中配置数据库连接：

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
  status_filter: ["normal"]  # 只获取状态为normal的股票
```

### 2. 数据库任务配置

在 `download_tasks` 中添加数据库任务：

```yaml
download_tasks:
  - name: "数据库活跃股票任务"
    from_database: true              # 标记为数据库任务
    query: null                      # 使用默认查询
    start_date: "2024-01-01"
    end_date: "today"
    keywords: []
    enabled: true
    database_config:
      batch_size: 50                 # 每批处理50个股票
      delay_between_batches: 5       # 批次间延迟5秒
      skip_on_error: true            # 单个股票失败时跳过
```

### 3. 守护者进程数据库配置

```yaml
daemon:
  tasks:
    database_tasks:
      refresh_connection: true       # 每次执行前刷新连接
      query_timeout: 30             # 查询超时时间
      show_progress: true           # 显示详细进度
```

## 🚀 使用方法

### 命令行测试

```bash
# 测试数据库连接
python main.py --test-db

# 手动执行数据库任务
python main.py --db-stocks

# 测试守护者进程配置
python main.py --daemon-test
```

### 守护者进程模式

```bash
# 启动守护者进程（会自动执行数据库任务）
python main.py --daemon-start

# 查看运行状态
python main.py --daemon-status
```

## 📊 工作流程

### 每日执行流程

1. **获取股票列表**：从数据库查询活跃股票
2. **分批处理**：将股票列表分成小批次
3. **逐个下载**：为每个股票下载公告
4. **进度监控**：记录成功/失败统计
5. **完成报告**：输出执行结果

### 示例执行日志

```
2024-01-15 09:00:00 - 开始执行定时下载任务...
2024-01-15 09:00:01 - 执行数据库增量任务: 数据库活跃股票任务 (最近7天)
2024-01-15 09:00:02 - 从数据库获取到 1250 个活跃股票代码
2024-01-15 09:00:02 - 开始处理第 1/25 批股票 (50 个)
2024-01-15 09:00:03 - [1/1250] 处理股票: 00001
2024-01-15 09:00:05 - ✓ 股票 00001 下载完成: 3 个文件
...
2024-01-15 09:45:30 - 数据库任务完成！
2024-01-15 09:45:30 - 总股票数: 1250
2024-01-15 09:45:30 - 成功处理: 1248
2024-01-15 09:45:30 - 失败数量: 2
2024-01-15 09:45:30 - 下载文件: 3567 个
```

## ⚙️ 高级配置

### 自定义SQL查询

如果需要特定的股票筛选条件，可以使用自定义查询：

```yaml
- name: "高市值股票任务"
  from_database: true
  query: "SELECT stockCode FROM issue WHERE status='normal' AND marketCap > 10000000000"
  # ... 其他配置
```

### 批量处理优化

```yaml
database_config:
  batch_size: 20                   # 减少批次大小，降低内存使用
  delay_between_batches: 10        # 增加延迟，减少服务器压力
  skip_on_error: false             # 遇到错误时停止，便于调试
```

### 性能调优

```yaml
daemon:
  tasks:
    database_tasks:
      refresh_connection: false    # 禁用连接刷新，提高性能
      show_progress: false         # 禁用详细进度，减少日志量
```

## 📈 监控和统计

### 查看执行统计

守护者进程会记录详细的执行统计：

- **总股票数**：从数据库获取的股票总数
- **成功处理**：成功下载公告的股票数
- **失败数量**：处理失败的股票数
- **下载文件**：总共下载的文件数

### 日志监控

```bash
# 查看守护者进程日志
tail -f hkex_daemon.log

# 查看主程序日志
tail -f hkex_downloader.log
```

## 🔍 故障排除

### 常见问题

1. **数据库连接失败**
   ```
   解决：检查数据库配置和网络连接
   测试：python main.py --test-db
   ```

2. **未获取到股票**
   ```
   解决：检查status_filter配置和数据库数据
   测试：python main.py --db-stocks
   ```

3. **处理速度慢**
   ```
   解决：调整batch_size和delay_between_batches
   ```

### 调试方法

1. **启用详细日志**：设置 `show_progress: true`
2. **减少批次大小**：设置较小的 `batch_size`
3. **测试单个股票**：使用命令行模式测试

## 💡 最佳实践

1. **首次运行**：
   - 先测试数据库连接
   - 使用小批次测试
   - 启用详细日志

2. **生产环境**：
   - 合理设置批次大小（推荐50-100）
   - 启用错误跳过模式
   - 定期监控日志

3. **性能优化**：
   - 避开交易时间执行
   - 使用增量下载模式
   - 合理设置延迟时间

## 🎯 典型配置示例

### 小规模部署（<500股票）
```yaml
database_config:
  batch_size: 100
  delay_between_batches: 2
  skip_on_error: true
```

### 大规模部署（>1000股票）
```yaml
database_config:
  batch_size: 50
  delay_between_batches: 5
  skip_on_error: true
```

### 调试模式
```yaml
database_config:
  batch_size: 10
  delay_between_batches: 1
  skip_on_error: false
```
