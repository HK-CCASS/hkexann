# HKEX 公告系统 API 架构分析报告

生成日期：2025-09-27
分析工具：ZEN MCP Analyze (GPT-5 + Qwen3 Coder with UltraThink)

## 执行摘要

本报告全面分析了香港交易所（HKEX）公告下载与监控系统的API架构、输入输出参数及系统设计。系统采用混合架构，结合了传统单体应用和现代微服务模式，提供了公告搜索、下载和实时监控三大核心功能。

## 1. API 接口详细分析

### 1.1 公告搜索 API

**端点URL：** `https://www1.hkexnews.hk/search/titleSearchServlet.do`

**请求方法：** GET

**输入参数：**

| 参数名 | 类型 | 必需 | 说明 | 示例值 |
|--------|------|------|------|--------|
| stockId | string | 是 | 股票代码 | "00700" |
| fromDate | string | 是 | 开始日期 (YYYYMMDD) | "20240101" |
| toDate | string | 是 | 结束日期 (YYYYMMDD) | "20241231" |
| title | string | 否 | 搜索关键词 (URL编码) | "%E5%85%AC%E5%91%8A" |
| searchType | integer | 是 | 搜索类型 | 0 |
| documentType | integer | 是 | 文档类型过滤器 | -1 (所有) |
| category | integer | 是 | 类别过滤器 | 0 (所有) |
| market | string | 是 | 市场类型 | "SEHK" |
| sortDir | integer | 是 | 排序方向 | 0 (降序) |
| sortByOptions | string | 是 | 排序字段 | "DateTime" |
| rowRange | integer | 是 | 最大结果数 | 1000 |
| lang | string | 是 | 语言 | "zh"/"en" |
| t1code | integer | 是 | 一级分类代码 | -2 |
| t2Gcode | integer | 是 | 二级分组代码 | -2 |
| t2code | integer | 是 | 二级分类代码 | -2 |

**返回值结构（JSON）：**

```json
{
  "result": [
    {
      "STOCK_CODE": "00700",
      "STOCK_NAME": "腾讯控股",
      "DATE_TIME": "2024-03-15 18:45",
      "TITLE": "内幕消息及恢复买卖",
      "FILE_LINK": "/listedco/listconews/SEHK/2024/0315/2024031501234_c.pdf",
      "FILE_INFO": "PDF, 245KB",
      "NEWS_ID": "20240315-001234",
      "HKEX_TIER1_CODE": "1",
      "HKEX_TIER2_CODE": "11",
      "HKEX_TIER3_CODE": "111"
    }
  ],
  "recordCount": 156,
  "hasMore": false
}
```

### 1.2 公告下载功能

**下载URL模式：** `https://www1.hkexnews.hk{FILE_LINK}`

**下载特性：**
- 支持断点续传
- 自动重试机制（最多3次）
- 文件名清理（移除特殊字符）
- 支持批量并发下载

**下载参数控制：**
```yaml
async:
  max_concurrent: 5          # 最大并发下载数
  requests_per_second: 5     # 每秒请求限制
  timeout: 30               # 单个请求超时（秒）
  min_delay: 0.88           # 最小请求间隔
  max_delay: 2.68           # 最大请求间隔
  backoff_on_429: 60        # 429错误退避时间
```

### 1.3 实时监听 API

**端点URL：** `https://www1.hkexnews.hk/ncms/json/eds/lcisehk1relsdc_1.json`

**请求方法：** GET

**监控参数：**
- 轮询间隔：60秒
- 时间窗口：最近24小时
- 去重机制：基于NEWS_ID

**返回值结构（JSON）：**

```json
[
  {
    "s": "00700",              // 股票代码
    "sc": "腾讯控股",           // 股票名称
    "t": "2024-03-15 18:45:00", // 时间戳
    "ti": "内幕消息",           // 标题
    "u": "/listedco/...",      // 文件路径
    "id": "20240315-001234",   // 唯一标识
    "c1": "1",                 // HKEX一级分类
    "c2": "11",                // HKEX二级分类
    "c3": "111"                // HKEX三级分类
  }
]
```

---

*报告生成工具：ZEN MCP Analysis Framework*
*使用模型：GPT-5 + Qwen3-Coder-480B with MaxThinking*
*分析深度：架构层 + 实现层 + 业务层*