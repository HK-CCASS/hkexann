# Milvus 与 ClickHouse 数据库字段映射关系分析报告

生成日期：2025-09-27
系统版本：HKEX公告监控系统 v3.0

## 执行摘要

本报告详细分析了 HKEX 公告系统中 Milvus 向量数据库与 ClickHouse 时序数据库之间的字段映射关系。系统采用双数据库架构：ClickHouse 存储结构化元数据和文本内容，Milvus 存储向量嵌入和支持语义搜索。两个数据库通过 `doc_id` 和 `chunk_id` 建立关联。

## 1. 数据库架构概览

### 1.1 数据流架构

```
PDF文档 → 文本提取 → 分块处理 → 双向存储
                            ↓
                    ┌─────────────────────────────┐
                    │                             │
                    ↓                             ↓
            ClickHouse (元数据)            Milvus (向量)
            - pdf_documents表              - pdf_embeddings_v3集合
            - pdf_chunks表                 - 4096维向量
                    │                             │
                    └──────── doc_id ────────────┘
                              chunk_id
```

### 1.2 数据库角色定位

| 数据库 | 角色 | 主要用途 | 数据类型 |
|--------|------|----------|----------|
| **ClickHouse** | 时序存储 | 元数据管理、统计分析、历史查询 | 结构化数据 |
| **Milvus** | 向量引擎 | 语义搜索、相似度匹配、智能检索 | 向量嵌入 |

## 2. ClickHouse 数据库表结构

### 2.1 pdf_documents 表（文档元数据）

| 字段名 | 类型 | 说明 | 索引类型 |
|--------|------|------|----------|
| **doc_id** | String | 文档唯一标识（主键） | PRIMARY KEY |
| file_path | String | 文件完整路径 | - |
| file_name | String | 文件名 | - |
| file_size | UInt64 | 文件大小（字节） | - |
| file_hash | String | 文件SHA256哈希 | - |
| **stock_code** | String | 股票代码 | bloom_filter |
| company_name | String | 公司名称 | - |
| document_type | String | 文档类型 | bloom_filter |
| document_category | String | 文档分类 | bloom_filter |
| document_title | String | 文档标题 | tokenbf_v1 |
| publish_date | Date | 发布日期 | PARTITION KEY |
| **hkex_t1_code** | String | HKEX 1级分类代码 | bloom_filter |
| **hkex_t2_code** | String | HKEX 2/3级分类代码 | bloom_filter |
| hkex_category_name | String | HKEX分类名称 | tokenbf_v1 |
| page_count | UInt16 | 总页数 | - |
| processing_status | Enum8 | 处理状态 | bloom_filter |
| chunks_count | UInt32 | 分块数量 | - |
| vectors_count | UInt32 | 向量数量 | - |
| error_message | Nullable(String) | 错误信息 | - |
| created_at | DateTime | 创建时间 | - |
| updated_at | DateTime | 更新时间 | - |
| processed_at | Nullable(DateTime) | 处理完成时间 | - |

### 2.2 pdf_chunks 表（文档分块）

| 字段名 | 类型 | 说明 | 索引类型 |
|--------|------|------|----------|
| **chunk_id** | String | 分块唯一标识 | PRIMARY KEY |
| **doc_id** | String | 关联文档ID | PRIMARY KEY |
| chunk_index | UInt32 | 分块索引序号 | ORDER BY |
| page_number | UInt16 | 所在页码 | - |
| text | String | 文本内容 | tokenbf_v1 |
| text_length | UInt32 | 文本字符长度 | - |
| text_hash | String | 文本SHA256哈希 | - |
| bbox | Array(Float32) | 边界框坐标[x1,y1,x2,y2] | - |
| chunk_type | Enum8 | 分块类型 | bloom_filter |
| table_id | Nullable(String) | 表格ID（如果是表格） | - |
| table_data | Nullable(String) | 表格JSON数据 | - |
| vector_status | Enum8 | 向量化状态 | bloom_filter |
| **vector_id** | Nullable(String) | Milvus向量ID | - |
| created_at | DateTime | 创建时间 | - |
| updated_at | DateTime | 更新时间 | - |

### 2.3 ClickHouse 表分区策略

- **pdf_documents**: 按 `toYYYYMM(publish_date)` 和 `stock_code` 分区
- **pdf_chunks**: 按 `substring(doc_id, 1, 6)` 分区
- **索引粒度**: 8192行

## 3. Milvus 向量数据库结构

### 3.1 pdf_embeddings_v3 集合

| 字段名 | 类型 | 维度/长度 | 说明 |
|--------|------|-----------|------|
| **id** | VARCHAR | 64 | 主键，UUID格式 |
| **embedding** | FLOAT_VECTOR | 4096 | 向量嵌入（核心） |
| **doc_id** | VARCHAR | 64 | 文档ID（关联键） |
| **chunk_id** | VARCHAR | 64 | 分块ID（关联键） |
| stock_code | VARCHAR | 10 | 股票代码 |
| company_name | VARCHAR | 500 | 公司名称 |
| document_type | VARCHAR | 100 | 文档类型 |
| document_category | VARCHAR | 200 | 文档分类 |
| document_title | VARCHAR | 500 | 文档标题 |
| **hkex_level1_code** | VARCHAR | 20 | HKEX 1级分类代码 |
| **hkex_level1_name** | VARCHAR | 500 | HKEX 1级分类名称 |
| **hkex_level2_code** | VARCHAR | 20 | HKEX 2级分类代码 |
| **hkex_level2_name** | VARCHAR | 500 | HKEX 2级分类名称 |
| **hkex_level3_code** | VARCHAR | 20 | HKEX 3级分类代码 |
| **hkex_level3_name** | VARCHAR | 500 | HKEX 3级分类名称 |
| hkex_classification_confidence | FLOAT | - | 分类置信度 |
| hkex_full_path | VARCHAR | 1000 | 完整分类路径 |
| hkex_classification_method | VARCHAR | 50 | 分类方法 |
| chunk_type | VARCHAR | 50 | 分块类型 |
| **text_content** | VARCHAR | 50000 | 文本内容（扩容） |
| publish_date | INT64 | - | 发布日期时间戳 |
| page_number | INT32 | - | 页码 |
| file_path | VARCHAR | 500 | 文件路径 |
| chunk_length | INT32 | - | 文本长度 |
| importance_score | FLOAT | - | 重要性评分(0-1) |
| metadata | JSON | - | 业务元数据 |
| created_at | INT64 | - | 创建时间戳 |

### 3.2 Milvus 索引配置

```yaml
索引类型: IVF_PQ
度量类型: COSINE
nlist: 2048
m: 8
nbits: 8
分片数: 2
副本数: 1
一致性级别: Session
```

## 4. 字段映射关系矩阵

### 4.1 核心关联字段

| 业务含义 | ClickHouse (pdf_documents) | ClickHouse (pdf_chunks) | Milvus (pdf_embeddings_v3) | 映射类型 |
|---------|---------------------------|------------------------|---------------------------|----------|
| **文档ID** | doc_id (主键) | doc_id (外键) | doc_id | 1:N:N |
| **分块ID** | - | chunk_id (主键) | chunk_id | 1:1 |
| **向量ID** | - | vector_id | id (主键) | 1:1 |
| **股票代码** | stock_code | - | stock_code | 复制 |
| **公司名称** | company_name | - | company_name | 复制 |

### 4.2 HKEX分类字段映射

| 分类级别 | ClickHouse字段 | Milvus字段 | 数据流向 |
|----------|---------------|------------|----------|
| **1级分类代码** | hkex_t1_code | hkex_level1_code | CH → Milvus |
| **1级分类名称** | - | hkex_level1_name | 仅Milvus |
| **2级分类代码** | hkex_t2_code | hkex_level2_code | CH → Milvus |
| **2级分类名称** | - | hkex_level2_name | 仅Milvus |
| **3级分类代码** | hkex_t2_code | hkex_level3_code | CH → Milvus |
| **3级分类名称** | hkex_category_name | hkex_level3_name | CH → Milvus |
| **分类置信度** | - | hkex_classification_confidence | 仅Milvus |
| **完整路径** | - | hkex_full_path | 仅Milvus |
| **分类方法** | - | hkex_classification_method | 仅Milvus |

### 4.3 内容字段映射

| 内容类型 | ClickHouse (pdf_chunks) | Milvus | 说明 |
|----------|------------------------|--------|------|
| **文本内容** | text (String) | text_content (VARCHAR 50000) | Milvus扩容至50K |
| **文本长度** | text_length (UInt32) | chunk_length (INT32) | 直接映射 |
| **分块类型** | chunk_type (Enum8) | chunk_type (VARCHAR) | 枚举转字符串 |
| **页码** | page_number (UInt16) | page_number (INT32) | 类型扩展 |
| **向量嵌入** | - | embedding (FLOAT_VECTOR) | 仅Milvus，4096维 |

### 4.4 时间戳字段映射

| 时间类型 | ClickHouse | Milvus | 转换方式 |
|----------|-----------|--------|----------|
| **发布日期** | publish_date (Date) | publish_date (INT64) | Date → Unix时间戳 |
| **创建时间** | created_at (DateTime) | created_at (INT64) | DateTime → Unix时间戳 |
| **更新时间** | updated_at (DateTime) | - | 仅ClickHouse |
| **处理时间** | processed_at (DateTime) | - | 仅ClickHouse |

## 5. 数据同步流程

### 5.1 写入流程

```python
# 1. 文档元数据写入ClickHouse
INSERT INTO pdf_documents (doc_id, stock_code, ...) VALUES (...)

# 2. 文档分块写入ClickHouse
INSERT INTO pdf_chunks (chunk_id, doc_id, text, ...) VALUES (...)

# 3. 向量化处理
embedding = generate_embedding(text)

# 4. 向量写入Milvus
collection.insert({
    "id": vector_id,
    "embedding": embedding,
    "doc_id": doc_id,
    "chunk_id": chunk_id,
    ...
})

# 5. 更新ClickHouse中的vector_id
UPDATE pdf_chunks SET vector_id = ? WHERE chunk_id = ?
```

### 5.2 查询流程

```python
# 语义搜索流程
1. 用户查询 → 生成查询向量
2. Milvus向量搜索 → 获取相似chunk_id列表
3. ClickHouse查询 → 根据chunk_id获取完整文本和元数据
4. 结果聚合 → 合并Milvus分数和ClickHouse数据
5. 返回结果 → 排序后的文档列表
```

## 6. 关键设计决策

### 6.1 数据冗余策略

| 字段类别 | 策略 | 原因 |
|----------|------|------|
| **关联键** | 双向存储 | 支持双向查询 |
| **业务字段** | Milvus冗余 | 减少跨库查询 |
| **文本内容** | 双向存储 | CH用于展示，Milvus用于搜索 |
| **处理状态** | 仅ClickHouse | 状态管理集中化 |
| **向量数据** | 仅Milvus | 专用向量存储 |

### 6.2 性能优化

1. **ClickHouse优化**
   - 使用MergeTree引擎，支持快速写入和查询
   - 按月份和股票代码分区，提升查询效率
   - Bloom Filter索引加速点查询
   - Token索引支持全文搜索

2. **Milvus优化**
   - IVF_PQ索引平衡精度和速度
   - COSINE相似度适合文本向量
   - 2分片提升并发能力
   - 50KB文本容量支持长文档

### 6.3 数据一致性保证

| 机制 | 实现方式 | 保证级别 |
|------|---------|----------|
| **事务一致性** | 应用层两阶段提交 | 最终一致性 |
| **ID关联** | UUID生成器 | 强一致性 |
| **状态同步** | vector_status字段 | 状态可追踪 |
| **错误处理** | 重试机制+错误记录 | 容错性 |

## 7. 常见查询模式

### 7.1 基于向量的语义搜索

```sql
-- Milvus查询
search_params = {
    "metric_type": "COSINE",
    "params": {"nprobe": 16}
}
results = collection.search(
    data=[query_vector],
    anns_field="embedding",
    param=search_params,
    limit=20,
    expr=f'stock_code == "{stock_code}" and hkex_level1_code == "1"'
)
```

### 7.2 基于元数据的结构化查询

```sql
-- ClickHouse查询
SELECT
    doc_id,
    document_title,
    stock_code,
    company_name,
    hkex_category_name,
    publish_date
FROM pdf_documents
WHERE
    stock_code = '00700'
    AND hkex_t1_code = '1'
    AND publish_date >= '2024-01-01'
ORDER BY publish_date DESC
LIMIT 100
```

### 7.3 联合查询（向量+元数据）

```python
# 1. Milvus获取相似文档
vector_results = milvus_search(query_embedding, top_k=50)
chunk_ids = [r.id for r in vector_results]

# 2. ClickHouse获取详细信息
query = f"""
    SELECT
        c.chunk_id,
        c.text,
        d.document_title,
        d.stock_code,
        d.hkex_category_name
    FROM pdf_chunks c
    JOIN pdf_documents d ON c.doc_id = d.doc_id
    WHERE c.chunk_id IN ({','.join(chunk_ids)})
"""

# 3. 合并结果并排序
combined_results = merge_results(vector_results, clickhouse_results)
```

## 8. 数据统计与监控

### 8.1 数据量统计

```sql
-- ClickHouse统计
SELECT
    COUNT(DISTINCT doc_id) as total_documents,
    COUNT(*) as total_chunks,
    SUM(vectors_count) as total_vectors,
    AVG(chunks_count) as avg_chunks_per_doc
FROM pdf_documents;

-- Milvus统计
collection.num_entities  # 总向量数
```

### 8.2 处理状态监控

```sql
-- 处理状态分布
SELECT
    processing_status,
    COUNT(*) as count,
    AVG(chunks_count) as avg_chunks
FROM pdf_documents
GROUP BY processing_status;

-- 向量化状态
SELECT
    vector_status,
    COUNT(*) as count
FROM pdf_chunks
GROUP BY vector_status;
```

## 9. 问题与优化建议

### 9.1 当前问题

| 问题 | 影响 | 严重度 |
|------|------|--------|
| HKEX分类字段不一致 | CH使用t1/t2_code，Milvus使用level1/2/3 | 中 |
| 文本内容重复存储 | 存储空间增加约30% | 低 |
| 缺少分布式事务 | 可能出现数据不一致 | 中 |
| 向量维度固定4096 | 灵活性受限 | 低 |

### 9.2 优化建议

1. **统一命名规范**
   - 将ClickHouse的hkex_t1_code改为hkex_level1_code
   - 统一使用level1/2/3命名体系

2. **优化存储策略**
   - 考虑将长文本仅存储在ClickHouse
   - Milvus只保留摘要或前N字符

3. **增强一致性保证**
   - 实现分布式事务框架
   - 添加数据校验机制

4. **性能优化**
   - 实施查询结果缓存
   - 优化跨库JOIN查询

## 10. 总结

HKEX公告系统通过ClickHouse和Milvus的双数据库架构，实现了结构化数据管理与向量语义搜索的完美结合。关键设计要点：

1. **清晰的职责分离**：ClickHouse负责元数据和状态管理，Milvus专注向量搜索
2. **灵活的关联机制**：通过doc_id和chunk_id实现数据关联
3. **冗余策略平衡**：关键字段冗余存储，提升查询效率
4. **HKEX分类体系**：完整支持三级分类系统，但存在命名不一致问题
5. **扩展性设计**：支持大文本（50KB）和高维向量（4096维）

通过持续优化字段映射和数据同步机制，系统能够更好地支持复杂的业务查询需求。

---

*报告生成工具：HKEX系统分析框架*
*数据源：源代码静态分析 + SQL Schema分析*
*分析深度：字段级映射 + 数据流分析 + 性能优化*