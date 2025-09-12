-- ClickHouse 财技事件数据表结构定义
-- 用于存储港交所财技事件数据的高性能列式存储表

-- 供股事件表
CREATE TABLE IF NOT EXISTS rights_issue (
    stock_code String COMMENT '股票代码',
    company_name String COMMENT '公司名称',
    announcement_date Date COMMENT '公告日期',
    status String COMMENT '状态',
    rights_price Nullable(Float64) COMMENT '供股价',
    rights_price_premium Nullable(String) COMMENT '供股价溢价',
    rights_ratio Nullable(String) COMMENT '供股比例',
    current_price Nullable(Float64) COMMENT '现价',
    current_price_premium Nullable(String) COMMENT '现价溢价',
    stock_adjustment Nullable(String) COMMENT '股份调整',
    underwriter Nullable(String) COMMENT '包销商',
    ex_rights_date Nullable(Date) COMMENT '除权日',
    rights_trading_start Nullable(Date) COMMENT '供股交易开始日',
    rights_trading_end Nullable(Date) COMMENT '供股交易结束日',
    final_payment_date Nullable(Date) COMMENT '最后缴款日',
    result_announcement_date Nullable(Date) COMMENT '结果公布日',
    allotment_date Nullable(Date) COMMENT '配发日',
    created_at DateTime DEFAULT now() COMMENT '记录创建时间',
    updated_at DateTime DEFAULT now() COMMENT '记录更新时间'
) ENGINE = MergeTree()
ORDER BY (stock_code, announcement_date)
PARTITION BY toYYYYMM(announcement_date)
COMMENT '供股事件数据表';

-- 配股事件表
CREATE TABLE IF NOT EXISTS placement (
    stock_code String COMMENT '股票代码',
    company_name String COMMENT '公司名称',
    announcement_date Date COMMENT '公告日期',
    status String COMMENT '状态',
    placement_price Nullable(Float64) COMMENT '配股价',
    placement_price_premium Nullable(String) COMMENT '配股价溢价',
    new_shares_ratio Nullable(String) COMMENT '新股比例',
    current_price Nullable(Float64) COMMENT '现价',
    current_price_premium Nullable(String) COMMENT '现价溢价',
    placement_agent Nullable(String) COMMENT '配股代理',
    authorization_method Nullable(String) COMMENT '授权方式',
    placement_method Nullable(String) COMMENT '配股方式',
    completion_date Nullable(Date) COMMENT '完成日期',
    created_at DateTime DEFAULT now() COMMENT '记录创建时间',
    updated_at DateTime DEFAULT now() COMMENT '记录更新时间'
) ENGINE = MergeTree()
ORDER BY (stock_code, announcement_date)
PARTITION BY toYYYYMM(announcement_date)
COMMENT '配股事件数据表';

-- 合股事件表
CREATE TABLE IF NOT EXISTS consolidation (
    stock_code String COMMENT '股票代码',
    company_name String COMMENT '公司名称',
    announcement_date Date COMMENT '公告日期',
    status String COMMENT '状态',
    temporary_counter Nullable(String) COMMENT '临时代号',
    consolidation_ratio Nullable(String) COMMENT '合股比例',
    effective_date Nullable(Date) COMMENT '生效日期',
    other_corporate_actions Nullable(String) COMMENT '其他公司行动',
    created_at DateTime DEFAULT now() COMMENT '记录创建时间',
    updated_at DateTime DEFAULT now() COMMENT '记录更新时间'
) ENGINE = MergeTree()
ORDER BY (stock_code, announcement_date)
PARTITION BY toYYYYMM(announcement_date)
COMMENT '合股事件数据表';

-- 拆股事件表
CREATE TABLE IF NOT EXISTS stock_split (
    stock_code String COMMENT '股票代码',
    company_name String COMMENT '公司名称',
    announcement_date Date COMMENT '公告日期',
    status String COMMENT '状态',
    temporary_counter Nullable(String) COMMENT '临时代号',
    split_ratio Nullable(String) COMMENT '拆股比例',
    effective_date Nullable(Date) COMMENT '生效日期',
    other_corporate_actions Nullable(String) COMMENT '其他公司行动',
    created_at DateTime DEFAULT now() COMMENT '记录创建时间',
    updated_at DateTime DEFAULT now() COMMENT '记录更新时间'
) ENGINE = MergeTree()
ORDER BY (stock_code, announcement_date)
PARTITION BY toYYYYMM(announcement_date)
COMMENT '拆股事件数据表';

-- 创建索引以优化查询性能
-- 股票代码索引
CREATE INDEX IF NOT EXISTS idx_rights_issue_stock_code ON rights_issue (stock_code) TYPE set(0) GRANULARITY 8192;
CREATE INDEX IF NOT EXISTS idx_placement_stock_code ON placement (stock_code) TYPE set(0) GRANULARITY 8192;
CREATE INDEX IF NOT EXISTS idx_consolidation_stock_code ON consolidation (stock_code) TYPE set(0) GRANULARITY 8192;
CREATE INDEX IF NOT EXISTS idx_stock_split_stock_code ON stock_split (stock_code) TYPE set(0) GRANULARITY 8192;

-- 状态索引
CREATE INDEX IF NOT EXISTS idx_rights_issue_status ON rights_issue (status) TYPE set(0) GRANULARITY 8192;
CREATE INDEX IF NOT EXISTS idx_placement_status ON placement (status) TYPE set(0) GRANULARITY 8192;
CREATE INDEX IF NOT EXISTS idx_consolidation_status ON consolidation (status) TYPE set(0) GRANULARITY 8192;
CREATE INDEX IF NOT EXISTS idx_stock_split_status ON stock_split (status) TYPE set(0) GRANULARITY 8192;

-- 创建物化视图用于统计和分析
-- 每月事件统计视图
CREATE MATERIALIZED VIEW IF NOT EXISTS events_monthly_stats
ENGINE = SummingMergeTree()
ORDER BY (event_type, year_month, stock_code)
AS SELECT
    'rights_issue' as event_type,
    toYYYYMM(announcement_date) as year_month,
    stock_code,
    count() as event_count
FROM rights_issue
GROUP BY event_type, year_month, stock_code
UNION ALL
SELECT
    'placement' as event_type,
    toYYYYMM(announcement_date) as year_month,
    stock_code,
    count() as event_count
FROM placement
GROUP BY event_type, year_month, stock_code
UNION ALL
SELECT
    'consolidation' as event_type,
    toYYYYMM(announcement_date) as year_month,
    stock_code,
    count() as event_count
FROM consolidation
GROUP BY event_type, year_month, stock_code
UNION ALL
SELECT
    'stock_split' as event_type,
    toYYYYMM(announcement_date) as year_month,
    stock_code,
    count() as event_count
FROM stock_split
GROUP BY event_type, year_month, stock_code;