# HKEX 增强公告监听系统架构图

## 系统总览

```mermaid
graph TB
    subgraph "启动层 (start_enhanced_monitor.py)"
        CLI[命令行接口]
        CONFIG[配置加载器]
        LOGGER[日志系统]
        MAIN[主控制器]
    end
    
    subgraph "核心处理层 (EnhancedAnnouncementProcessor)"
        EAP[增强公告处理器]
        STATS[处理统计]
        SCHEDULER[调度器]
    end
    
    subgraph "数据发现层"
        SD[股票发现管理器]
        SDC[ClickHouse集成]
        SDE[增强发现引擎]
    end
    
    subgraph "监听层"
        API[HKEX API监听器]
        FILTER[双重过滤器]
    end
    
    subgraph "下载层"
        DLW[实时下载包装器]
        DL[异步下载器]
        FM[文件管理器]
    end
    
    subgraph "处理层"
        VEC[实时向量处理器]
        PDF[PDF解析器]
        EMB[嵌入生成器]
    end
    
    subgraph "存储层"
        CH[(ClickHouse)]
        MILVUS[(Milvus向量数据库)]
        FS[文件系统]
    end
    
    subgraph "历史处理"
        HP[历史批量处理器]
        CHP[修正历史处理器]
    end
    
    subgraph "监控与管理"
        HEALTH[健康监控]
        ERROR[错误处理]
        METRICS[指标收集]
        SECURITY[安全检查]
    end
    
    %% 主要数据流
    CLI --> CONFIG
    CONFIG --> MAIN
    MAIN --> EAP
    
    EAP --> SD
    EAP --> API
    EAP --> DLW
    EAP --> VEC
    EAP --> HP
    
    SD --> SDC
    SD --> SDE
    SDC --> CH
    
    API --> FILTER
    FILTER --> DLW
    
    DLW --> DL
    DL --> FM
    FM --> FS
    
    DLW --> VEC
    VEC --> PDF
    VEC --> EMB
    VEC --> CH
    VEC --> MILVUS
    
    HP --> CHP
    CHP --> CH
    
    EAP --> HEALTH
    EAP --> ERROR
    EAP --> METRICS
    EAP --> SECURITY
    
    %% 样式定义
    classDef coreComponent fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef dataLayer fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef storage fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef monitoring fill:#fff3e0,stroke:#e65100,stroke-width:2px
    
    class EAP,MAIN coreComponent
    class SD,API,FILTER,DLW,VEC dataLayer
    class CH,MILVUS,FS storage
    class HEALTH,ERROR,METRICS,SECURITY monitoring
```

## 详细组件架构

### 1. 启动控制流程

```mermaid
sequenceDiagram
    participant CLI as 命令行接口
    participant CONFIG as 配置加载器
    participant MAIN as 主控制器
    participant EAP as 增强公告处理器
    
    CLI->>CONFIG: 加载config.yaml
    CONFIG->>CONFIG: 环境变量替换
    CONFIG->>CONFIG: 设置日志系统
    CONFIG->>MAIN: 返回完整配置
    MAIN->>EAP: 创建处理器实例
    EAP->>EAP: 初始化所有组件
    EAP->>MAIN: 开始持续监听
```

### 2. 核心处理器组件详图

```mermaid
graph TB
    subgraph "EnhancedAnnouncementProcessor"
        subgraph "配置管理"
            SC[调度配置]
            EC[错误配置]
            FC[过滤配置]
        end
        
        subgraph "状态管理"
            STATS[处理统计]
            STATE[运行状态]
            ERROR_COUNT[错误计数]
        end
        
        subgraph "核心组件"
            SD[股票发现管理器]
            AM[API监听器]
            DF[双重过滤器]
            DW[下载包装器]
            VP[向量处理器]
            HP[历史处理器]
        end
        
        subgraph "调度功能"
            STOCK_SYNC[股票同步调度]
            API_POLL[API轮询调度]
            CONTINUOUS[持续监听]
        end
    end
    
    SC --> STOCK_SYNC
    SC --> API_POLL
    EC --> ERROR_COUNT
    FC --> DF
    
    STOCK_SYNC --> SD
    API_POLL --> AM
    AM --> DF
    DF --> DW
    DW --> VP
```

### 3. 数据流处理管道

```mermaid
graph LR
    subgraph "输入层"
        HKEX_API[HKEX公告API]
        CH_STOCKS[ClickHouse股票库]
    end
    
    subgraph "处理管道"
        A1[获取股票列表]
        A2[轮询API获取公告]
        A3[股票过滤]
        A4[类型过滤]
        A5[下载PDF]
        A6[解析PDF]
        A7[生成向量]
        A8[存储数据]
    end
    
    subgraph "输出层"
        PDF_FILES[PDF文件]
        CH_DATA[ClickHouse数据]
        MILVUS_VEC[Milvus向量]
    end
    
    CH_STOCKS --> A1
    HKEX_API --> A2
    A1 --> A3
    A2 --> A3
    A3 --> A4
    A4 --> A5
    A5 --> A6
    A6 --> A7
    A7 --> A8
    
    A5 --> PDF_FILES
    A8 --> CH_DATA
    A8 --> MILVUS_VEC
```

### 4. 监控与错误处理系统

```mermaid
graph TB
    subgraph "监控系统"
        HEALTH[健康监控]
        METRICS[指标收集器]
        RESOURCE[资源监控]
        SYSTEM[系统监控]
    end
    
    subgraph "错误处理"
        ERROR_CLASS[错误分类器]
        ERROR_HANDLER[统一错误处理器]
        GRACEFUL[优雅降级]
        RECOVERY[错误恢复]
    end
    
    subgraph "安全模块"
        CONFIG_CHECK[配置检查]
        ENV_VALID[环境验证]
        STARTUP_CHECK[启动检查]
    end
    
    HEALTH --> METRICS
    METRICS --> RESOURCE
    RESOURCE --> SYSTEM
    
    ERROR_CLASS --> ERROR_HANDLER
    ERROR_HANDLER --> GRACEFUL
    GRACEFUL --> RECOVERY
    
    CONFIG_CHECK --> ENV_VALID
    ENV_VALID --> STARTUP_CHECK
```

## 核心特性

### 🎯 主要功能
- **实时监听**: 轮询HKEX公告API，实时获取最新公告
- **智能过滤**: 双重过滤机制（股票过滤 + 类型过滤）
- **自动下载**: 异步批量下载PDF文件
- **向量化处理**: 自动解析PDF并生成向量嵌入
- **数据存储**: 结构化数据存储到ClickHouse，向量存储到Milvus

### 🔧 系统能力
- **并发处理**: 支持高并发下载和处理
- **错误恢复**: 完善的错误处理和重试机制
- **配置驱动**: 完全基于YAML配置文件管理
- **模块化设计**: 松耦合的组件架构
- **监控完善**: 详细的系统监控和统计

### 📊 数据流向
1. **发现阶段**: 从ClickHouse发现监控股票列表
2. **监听阶段**: 实时轮询HKEX API获取公告
3. **过滤阶段**: 双重过滤保留感兴趣的公告
4. **下载阶段**: 异步下载PDF文件到本地
5. **处理阶段**: 解析PDF并生成向量嵌入
6. **存储阶段**: 数据存储到数据库和向量库

### 🚀 启动模式
- **正常模式**: `python start_enhanced_monitor.py`
- **测试模式**: `python start_enhanced_monitor.py -t`
- **自定义配置**: `python start_enhanced_monitor.py -c custom_config.yaml`

## 技术栈

- **Python**: 主要编程语言
- **AsyncIO**: 异步编程框架
- **ClickHouse**: 时序数据存储
- **Milvus**: 向量数据库
- **YAML**: 配置文件格式
- **aiohttp**: 异步HTTP客户端
- **PyMuPDF**: PDF处理库

---

*该系统架构图反映了 HKEX 增强公告监听系统的完整技术架构和数据流程*
