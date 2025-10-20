# HKEX公告下载系统数据流图

```mermaid
flowchart TD
    %% 数据源层
    subgraph "数据源层 (Data Sources)"
        HKEX[HKEX官方API<br/>港交所公告数据]
        STOCK_LIST[股票列表<br/>MySQL数据库]
        HISTORICAL[历史数据<br/>归档存储]
    end

    %% 数据采集层
    subgraph "数据采集层 (Data Collection)"
        API_POLL[API轮询监听<br/>60秒间隔]
        STOCK_SYNC[股票同步<br/>30分钟间隔]
        HIST_BACK[历史回填<br/>手动/自动]

        API_POLL --> HKEX
        STOCK_SYNC --> STOCK_LIST
        HIST_BACK --> HISTORICAL
    end

    %% 数据处理层
    subgraph "数据处理层 (Data Processing)"
        subgraph "第一阶段过滤"
            TIME_FILTER[时间过滤<br/>香港时区]
            ID_CACHE[ID缓存<br/>去重机制]
            BASIC_FILTER[基础过滤<br/>公告类型]
        end

        subgraph "第二阶段过滤"
            STOCK_FILTER[股票过滤<br/>监控股票列表]
            TYPE_FILTER[类型过滤<br/>排除无关类别]
            KEYWORD_FILTER[关键词过滤<br/>包含/排除规则]
        end

        subgraph "数据处理核心"
            MULTI_SPLIT[多股票拆分<br/>单公告多记录]
            CLASSIFY[智能分类<br/>HKEX官方分类]
            PRIORITY[优先级排序<br/>重要性评估]
        end

        API_POLL --> TIME_FILTER
        TIME_FILTER --> ID_CACHE
        ID_CACHE --> BASIC_FILTER
        BASIC_FILTER --> STOCK_FILTER
        STOCK_FILTER --> TYPE_FILTER
        TYPE_FILTER --> KEYWORD_FILTER
        KEYWORD_FILTER --> MULTI_SPLIT
        MULTI_SPLIT --> CLASSIFY
        CLASSIFY --> PRIORITY
    end

    %% 数据下载层
    subgraph "数据下载层 (Data Download)"
        QUEUE_DOWNLOAD[下载队列<br/>优先级排序]
        CONCURRENT[并发下载<br/>最多5个并发]
        RETRY[重试机制<br/>指数退避]
        VALIDATE[文件验证<br/>完整性检查]

        PRIORITY --> QUEUE_DOWNLOAD
        QUEUE_DOWNLOAD --> CONCURRENT
        CONCURRENT --> RETRY
        RETRY --> VALIDATE
    end

    %% 数据存储层
    subgraph "数据存储层 (Data Storage)"
        subgraph "文件存储"
            PDF_STORE[PDF文件存储<br/>按日期/股票分类]
            META_STORE[元数据存储<br/>JSON格式]
            BACKUP[备份存储<br/>压缩归档]
        end

        subgraph "数据库存储"
            CLICKHOUSE_INSERT[ClickHouse插入<br/>时序数据]
            MYSQL_UPDATE[MySQL更新<br/>股票状态]
            REDIS_CACHE[Redis缓存<br/>会话数据]
        end

        VALIDATE --> PDF_STORE
        VALIDATE --> META_STORE
        PDF_STORE --> BACKUP
        VALIDATE --> CLICKHOUSE_INSERT
        CLICKHOUSE_INSERT --> MYSQL_UPDATE
        CLICKHOUSE_INSERT --> REDIS_CACHE
    end

    %% 数据处理管道
    subgraph "数据处理管道 (Processing Pipeline)"
        subgraph "文档处理"
            PDF_PARSE[PDF解析<br/>文本提取]
            TEXT_CLEAN[文本清理<br/>格式标准化]
            CHUNK_SPLIT[文本分块<br/>智能分段]
        end

        subgraph "向量化处理"
            EMBEDDING[文本嵌入<br/>4096维向量]
            BATCH_PROCESS[批量处理<br/>15个一批]
            VECTOR_STORE[向量存储<br/>Milvus数据库]
        end

        subgraph "索引处理"
            SEMANTIC_INDEX[语义索引<br/>向量搜索]
            FULLTEXT_INDEX[全文索引<br/>关键词搜索]
            METADATA_INDEX[元数据索引<br/>结构化查询]
        end

        PDF_STORE --> PDF_PARSE
        PDF_PARSE --> TEXT_CLEAN
        TEXT_CLEAN --> CHUNK_SPLIT
        CHUNK_SPLIT --> EMBEDDING
        EMBEDDING --> BATCH_PROCESS
        BATCH_PROCESS --> VECTOR_STORE
        VECTOR_STORE --> SEMANTIC_INDEX
        META_STORE --> FULLTEXT_INDEX
        CLICKHOUSE_INSERT --> METADATA_INDEX
    end

    %% 数据服务层
    subgraph "数据服务层 (Data Services)"
        subgraph "查询服务"
            REALTIME_QUERY[实时查询<br/>最新公告]
            HISTORICAL_QUERY[历史查询<br/>时间范围]
            SEMANTIC_SEARCH[语义搜索<br/>相似内容]
        end

        subgraph "分析服务"
            TREND_ANALYSIS[趋势分析<br/>统计指标]
            CLASSIFY_STATS[分类统计<br/>公告类型]
            PERFORMANCE[性能监控<br/>系统指标]
        end

        subgraph "API服务"
            REST_API[REST API<br/>外部接口]
            GraphQL[GraphQL<br/>灵活查询]
            WEBHOOK[Webhook<br/>事件通知]
        end

        SEMANTIC_INDEX --> SEMANTIC_SEARCH
        FULLTEXT_INDEX --> REALTIME_QUERY
        METADATA_INDEX --> HISTORICAL_QUERY
        CLICKHOUSE_INSERT --> TREND_ANALYSIS
        CLASSIFY --> CLASSIFY_STATS
        CONCURRENT --> PERFORMANCE
        REALTIME_QUERY --> REST_API
        SEMANTIC_SEARCH --> GraphQL
        PERFORMANCE --> WEBHOOK
    end

    %% 监控和反馈
    subgraph "监控反馈 (Monitoring & Feedback)"
        HEALTH_CHECK[健康检查<br/>系统状态]
        ERROR_LOG[错误日志<br/>异常追踪]
        PERFORMANCE_METRICS[性能指标<br/>吞吐量统计]
        ALERT_SYSTEM[告警系统<br/>异常通知]

        RETRY --> ERROR_LOG
        CONCURRENT --> PERFORMANCE_METRICS
        CLICKHOUSE_INSERT --> HEALTH_CHECK
        ERROR_LOG --> ALERT_SYSTEM
        PERFORMANCE_METRICS --> ALERT_SYSTEM
    end

    %% 样式设置
    classDef source fill:#ffcccc,stroke:#333,stroke-width:2px
    classDef collection fill:#ccffcc,stroke:#333,stroke-width:2px
    classDef processing fill:#ccccff,stroke:#333,stroke-width:2px
    classDef download fill:#ffffcc,stroke:#333,stroke-width:2px
    classDef storage fill:#ffccff,stroke:#333,stroke-width:2px
    classDef pipeline fill:#ccffff,stroke:#333,stroke-width:2px
    classDef service fill:#ffcc99,stroke:#333,stroke-width:2px
    classDef monitor fill:#cccccc,stroke:#333,stroke-width:2px

    class HKEX,STOCK_LIST,HISTORICAL source
    class API_POLL,STOCK_SYNC,HIST_BACK collection
    class TIME_FILTER,ID_CACHE,BASIC_FILTER,STOCK_FILTER,TYPE_FILTER,KEYWORD_FILTER,MULTI_SPLIT,CLASSIFY,PRIORITY processing
    class QUEUE_DOWNLOAD,CONCURRENT,RETRY,VALIDATE download
    class PDF_STORE,META_STORE,BACKUP,CLICKHOUSE_INSERT,MYSQL_UPDATE,REDIS_CACHE storage
    class PDF_PARSE,TEXT_CLEAN,CHUNK_SPLIT,EMBEDDING,BATCH_PROCESS,VECTOR_STORE,SEMANTIC_INDEX,FULLTEXT_INDEX,METADATA_INDEX pipeline
    class REALTIME_QUERY,HISTORICAL_QUERY,SEMANTIC_SEARCH,TREND_ANALYSIS,CLASSIFY_STATS,PERFORMANCE,REST_API,GraphQL,WEBHOOK service
    class HEALTH_CHECK,ERROR_LOG,PERFORMANCE_METRICS,ALERT_SYSTEM monitor
```

## 数据流说明

### 主要数据流向

1. **数据采集阶段**
   - API轮询监听每60秒获取最新公告
   - 股票同步每30分钟更新监控股票列表
   - 历史回填支持手动和自动两种模式

2. **双重过滤阶段**
   - 第一阶段：时间过滤、ID去重、基础类型过滤
   - 第二阶段：股票过滤、类型过滤、关键词过滤

3. **并发下载阶段**
   - 优先级排序的下载队列
   - 最多5个并发下载任务
   - 指数退避重试机制

4. **向量化处理阶段**
   - PDF解析和文本提取
   - 4096维向量嵌入
   - 批量处理（15个一批）

5. **存储和索引阶段**
   - ClickHouse存储时序数据
   - Milvus存储向量数据
   - Redis缓存会话数据

### 性能优化点

- **并发处理**: 最多5个并发下载任务
- **批量向量化**: 15个文档一批处理
- **智能缓存**: ID缓存避免重复处理
- **索引优化**: 多维度索引支持快速查询

### 监控指标

- 实时处理吞吐量
- 下载成功率
- 向量化处理速度
- 系统健康状态

---

*生成时间: 2025-10-20*
*数据流版本: v2.1*