-- PDF文档表
-- 存储 hkexannpdf 目录下的PDF文档元数据

USE hkex_analysis;

CREATE TABLE IF NOT EXISTS hkex_analysis.pdf_documents (
    -- 文档标识
    doc_id String COMMENT '文档ID',
    file_path String COMMENT '文件路径',
    file_name String COMMENT '文件名',
    file_size UInt64 COMMENT '文件大小（字节）',
    file_hash String COMMENT '文件哈希值',
    
    -- 股票信息
    stock_code String COMMENT '股票代码',
    company_name String COMMENT '公司名称',
    
    -- 文档信息
    document_type String COMMENT '文档类型',
    document_category String COMMENT '文档分类',
    document_title String COMMENT '文档标题',
    publish_date Date COMMENT '发布日期',

    -- HKEX官方分类代码 (新增)
    hkex_t1_code String COMMENT 'HKEX 1级分类代码',
    hkex_t2_code String COMMENT 'HKEX 2级/3级分类代码',
    hkex_category_name String COMMENT 'HKEX分类名称',
    
    -- 页面信息
    page_count UInt16 COMMENT '页数',
    
    -- 处理状态
    processing_status Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4) COMMENT '处理状态',
    chunks_count UInt32 DEFAULT 0 COMMENT '分块数量',
    vectors_count UInt32 DEFAULT 0 COMMENT '向量数量',
    
    -- 错误信息
    error_message Nullable(String) COMMENT '错误信息',
    
    -- 系统字段
    created_at DateTime DEFAULT now() COMMENT '创建时间',
    updated_at DateTime DEFAULT now() COMMENT '更新时间',
    processed_at Nullable(DateTime) COMMENT '处理完成时间'
    
) ENGINE = MergeTree()
PARTITION BY (toYYYYMM(publish_date), stock_code)
ORDER BY (doc_id, publish_date)
PRIMARY KEY doc_id
SETTINGS index_granularity = 8192
COMMENT 'PDF文档元数据表';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pdf_stock_code ON hkex_analysis.pdf_documents (stock_code) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_pdf_document_type ON hkex_analysis.pdf_documents (document_type) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_pdf_category ON hkex_analysis.pdf_documents (document_category) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_pdf_title ON hkex_analysis.pdf_documents (document_title) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_pdf_processing_status ON hkex_analysis.pdf_documents (processing_status) TYPE bloom_filter GRANULARITY 1;

-- HKEX分类代码索引 (新增)
CREATE INDEX IF NOT EXISTS idx_pdf_hkex_t1_code ON hkex_analysis.pdf_documents (hkex_t1_code) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_pdf_hkex_t2_code ON hkex_analysis.pdf_documents (hkex_t2_code) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_pdf_hkex_category_name ON hkex_analysis.pdf_documents (hkex_category_name) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1;
