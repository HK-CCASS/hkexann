# 支持stock_code列表类型任务

## 任务描述
扩展配置文件中的stock_code字段支持列表类型，实现批量股票下载功能。

## 实施计划
1. 修改任务配置验证逻辑
2. 修改stock_code处理逻辑  
3. 增强下载统计和日志
4. 更新配置文件示例
5. 保持向后兼容性

## 技术方案
- 使用isinstance(stock_code, list)检查类型
- 对列表中每个股票代码调用_download_single_stock
- 累计统计所有股票的下载结果
- 错误处理：单个股票失败不影响其他股票

## 执行状态
- [x] 任务记录创建
- [x] 修改验证逻辑
- [x] 修改处理逻辑
- [x] 增强统计日志
- [x] 更新配置示例
- [x] 测试验证

## 实现细节
1. 在_download_task方法中添加类型检查：isinstance(stockcode, list)
2. 新增_download_multiple_stocks方法处理列表类型
3. 提供详细的统计信息和错误处理
4. 在config.yaml中添加批量下载任务示例
5. 保持向后兼容性，单个股票代码仍然有效 