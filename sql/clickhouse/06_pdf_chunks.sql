-- PDF文档块表
-- 存储PDF文档的分块文本内容

USE hkex_analysis;

CREATE TABLE IF NOT EXISTS hkex_analysis.pdf_chunks (
    -- 分块标识
    chunk_id String COMMENT '分块ID',
    doc_id String COMMENT '文档ID',
    
    -- 分块信息
    chunk_index UInt32 COMMENT '分块索引',
    page_number UInt16 COMMENT '页码',
    
    -- 文本内容
    text String COMMENT '文本内容',
    text_length UInt32 COMMENT '文本长度',
    text_hash String COMMENT '文本哈希值',
    
    -- 位置信息
    bbox Array(Float32) COMMENT '边界框坐标 [x1,y1,x2,y2]',
    
    -- 分块类型
    chunk_type Enum8('paragraph' = 1, 'table' = 2, 'title' = 3, 'list' = 4, 'other' = 5) COMMENT '分块类型',
    
    -- 表格信息（如果是表格）
    table_id Nullable(String) COMMENT '表格ID',
    table_data Nullable(String) COMMENT '表格数据（JSON格式）',
    
    -- 向量化状态
    vector_status Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4) COMMENT '向量化状态',
    vector_id Nullable(String) COMMENT '向量ID',
    
    -- 系统字段
    created_at DateTime DEFAULT now() COMMENT '创建时间',
    updated_at DateTime DEFAULT now() COMMENT '更新时间'
    
) ENGINE = MergeTree()
PARTITION BY substring(doc_id, 1, 6)
ORDER BY (doc_id, chunk_id)
PRIMARY KEY (doc_id, chunk_id)
SETTINGS index_granularity = 8192
COMMENT 'PDF文档分块表';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_chunks_text ON hkex_analysis.pdf_chunks (text) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_chunks_type ON hkex_analysis.pdf_chunks (chunk_type) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_chunks_vector_status ON hkex_analysis.pdf_chunks (vector_status) TYPE bloom_filter GRANULARITY 1;
