-- ClickHouse Schema Extension for HKEX Classification System
-- 创建时间: 2025-09-26
-- 目的: 修复schema-code不匹配bug，添加HKEX 3级分类支持
-- 影响表: pdf_documents
-- 兼容性: 增量扩展，保持100%向后兼容

-- =============================================================================
-- 1. HKEX官方3级分类字段扩展
-- =============================================================================

-- 一级分类字段
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_level1_code String DEFAULT '';
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_level1_name String DEFAULT '';

-- 二级分类字段
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_level2_code String DEFAULT '';
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_level2_name String DEFAULT '';

-- 三级分类字段
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_level3_code String DEFAULT '';
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_level3_name String DEFAULT '';

-- =============================================================================
-- 2. 分类元数据字段
-- =============================================================================

-- 分类置信度
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_classification_confidence Float32 DEFAULT 0.0;

-- 完整分类路径
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_full_path String DEFAULT '';

-- 分类方法标识 (keyword/hkex_official/hybrid)
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_classification_method String DEFAULT 'keyword';

-- 公告数量统计
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_announcement_count Int32 DEFAULT 0;

-- =============================================================================
-- 3. 兼容性字段 (与现有keyword分类系统并存)
-- =============================================================================

-- 关键字分类结果
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS keyword_category String DEFAULT '';

-- 关键字优先级
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS keyword_priority Int32 DEFAULT 0;

-- 关键字置信度
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS keyword_confidence Float32 DEFAULT 0.0;

-- =============================================================================
-- 4. 时间分区优化字段
-- =============================================================================

-- 分类执行日期
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS classification_date Date DEFAULT today();

-- 分类执行时间戳
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS classification_timestamp DateTime DEFAULT now();

-- =============================================================================
-- 5. 索引优化 (支持6种搜索类型)
-- =============================================================================

-- 分类层级索引 (支持范围搜索 Range Search)
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_hkex_level1 hkex_level1_code TYPE set(0) GRANULARITY 1;
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_hkex_level2 hkex_level2_code TYPE set(0) GRANULARITY 1;
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_hkex_level3 hkex_level3_code TYPE set(0) GRANULARITY 1;

-- 分类名称全文索引 (支持文本搜索 Text Search)
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_hkex_names (hkex_level1_name, hkex_level2_name, hkex_level3_name) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1;

-- 时间分区索引 (支持聚合搜索 Aggregation Search)
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_classification_time classification_date TYPE minmax GRANULARITY 1;

-- 置信度索引 (支持多向量搜索 Multi-Vector Search)
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_confidence hkex_classification_confidence TYPE minmax GRANULARITY 1;

-- 兼容性索引
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_keyword_category keyword_category TYPE set(0) GRANULARITY 1;
ALTER TABLE pdf_documents ADD INDEX IF NOT EXISTS idx_keyword_priority keyword_priority TYPE minmax GRANULARITY 1;

-- =============================================================================
-- 6. 验证脚本
-- =============================================================================

-- 检查新字段是否已成功添加
SELECT
    name,
    type,
    default_expression
FROM system.columns
WHERE table = 'pdf_documents'
    AND database = currentDatabase()
    AND name LIKE '%hkex%' OR name LIKE '%keyword%' OR name LIKE '%classification%'
ORDER BY name;

-- 统计表结构变更
SELECT
    'Schema extension completed' as status,
    count(*) as total_fields_count,
    countIf(name LIKE '%hkex%') as hkex_fields_count,
    countIf(name LIKE '%keyword%') as keyword_fields_count
FROM system.columns
WHERE table = 'pdf_documents' AND database = currentDatabase();