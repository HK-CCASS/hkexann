# HKEX公告下载系统架构图

```mermaid
graph TB
    %% 用户接口层
    subgraph "用户接口层"
        CLI[CLI命令行工具]
        API[REST API接口]
        MONITOR[监控面板]
    end

    %% 应用服务层 - 三个架构层次
    subgraph "应用服务层"
        subgraph "传统架构 (Legacy)"
            MAIN[main.py<br/>2,100+行<br/>单体架构]
            CONFIG1[ConfigManager<br/>配置管理]
            DB1[DatabaseManager<br/>数据库管理]
            CLASSIFIER[AnnouncementClassifier<br/>公告分类器]
        end

        subgraph "微服务架构 (Microservices)"
            API_MON[api_monitor.py<br/>API监听器]
            PROC[enhanced_processor.py<br/>增强处理器]
            FILTER[dual_filter.py<br/>双重过滤器]
            STOCK[stock_discovery.py<br/>股票发现]
            DOC[document_processor<br/>文档处理]
            EMBED[embeddings<br/>向量化服务]
        end

        subgraph "现代统一架构 (Modern)"
            UNIFIED[unified_downloader<br/>企业级架构]
            UNIFIED_CONFIG[unified_config.py<br/>统一配置]
            FILE_MGR[file_manager<br/>文件管理]
            STRATEGY[downloader_strategy<br/>下载策略]
            ADAPTER[legacy_adapter<br/>向后兼容]
        end
    end

    %% 数据访问层
    subgraph "数据访问层"
        subgraph "数据库系统"
            MYSQL[(MySQL<br/>股票元数据)]
            CLICKHOUSE[(ClickHouse<br/>时序数据)]
            MILVUS[(Milvus<br/>向量数据库)]
            REDIS[(Redis<br/>缓存)]
        end

        subgraph "外部API"
            HKEX_API[HKEX官方API]
            SILICONFLOW[SiliconFlow API<br/>AI服务]
        end
    end

    %% 基础设施层
    subgraph "基础设施层"
        subgraph "存储系统"
            PDF_STORE[PDF文件存储]
            CACHE_DIR[缓存目录]
            LOG_DIR[日志系统]
        end

        subgraph "监控系统"
            HEALTH[健康检查]
            METRICS[性能指标]
            ALERT[告警系统]
        end
    end

    %% 连接关系 - 传统架构
    CLI --> MAIN
    MAIN --> CONFIG1
    MAIN --> DB1
    MAIN --> CLASSIFIER
    CONFIG1 --> MYSQL
    DB1 --> MYSQL
    DB1 --> CLICKHOUSE

    %% 连接关系 - 微服务架构
    API --> API_MON
    API_MON --> FILTER
    FILTER --> PROC
    PROC --> DOC
    DOC --> EMBED
    STOCK --> MYSQL
    API_MON --> HKEX_API
    EMBED --> SILICONFLOW
    DOC --> PDF_STORE
    EMBED --> MILVUS

    %% 连接关系 - 现代架构
    CLI --> UNIFIED
    UNIFIED --> UNIFIED_CONFIG
    UNIFIED --> FILE_MGR
    UNIFIED --> STRATEGY
    UNIFIED --> ADAPTER
    ADAPTER --> MAIN

    %% 数据库连接
    API_MON --> CLICKHOUSE
    STOCK --> CLICKHOUSE
    PROC --> REDIS
    EMBED --> MILVUS

    %% 监控连接
    MONITOR --> HEALTH
    HEALTH --> METRICS
    METRICS --> ALERT

    %% 样式设置
    classDef legacy fill:#ff9999,stroke:#333,stroke-width:2px
    classDef microservices fill:#99ccff,stroke:#333,stroke-width:2px
    classDef modern fill:#99ff99,stroke:#333,stroke-width:2px
    classDef database fill:#ffcc99,stroke:#333,stroke-width:2px
    classDef external fill:#cc99ff,stroke:#333,stroke-width:2px
    classDef infra fill:#cccccc,stroke:#333,stroke-width:2px

    class MAIN,CONFIG1,DB1,CLASSIFIER legacy
    class API_MON,PROC,FILTER,STOCK,DOC,EMBED microservices
    class UNIFIED,UNIFIED_CONFIG,FILE_MGR,STRATEGY,ADAPTER modern
    class MYSQL,CLICKHOUSE,MILVUS,REDIS database
    class HKEX_API,SILICONFLOW external
    class PDF_STORE,CACHE_DIR,LOG_DIR,HEALTH,METRICS,ALERT infra
```

## 架构说明

### 三层架构设计

1. **传统架构** (红色)
   - 基于main.py的单体架构
   - 代码量2,100+行
   - 适用于简单任务和向后兼容

2. **微服务架构** (蓝色)
   - 分布式微服务设计
   - 代码量29,000+行
   - 高内聚低耦合，易于扩展

3. **现代统一架构** (绿色)
   - 企业级模块化设计
   - 支持策略模式和依赖注入
   - 100%向后兼容

### 数据库系统集成

- **MySQL**: 存储股票元数据和公司信息
- **ClickHouse**: 时序数据存储，支持高性能查询
- **Milvus**: 向量数据库，支持语义搜索
- **Redis**: 缓存和会话管理

### 外部服务集成

- **HKEX API**: 港交所官方公告数据源
- **SiliconFlow API**: AI服务，提供文本嵌入和LLM服务

---

*生成时间: 2025-10-20*
*系统版本: v2.1*