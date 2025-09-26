-- HKEX分类字段迁移脚本
-- 为现有的pdf_documents表添加HKEX分类代码字段

USE hkex_analysis;

-- 检查并添加HKEX分类字段
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_t1_code String;
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_t2_code String;
ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS hkex_category_name String;

-- 显示表结构确认
DESCRIBE TABLE pdf_documents;
