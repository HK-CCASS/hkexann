#!/usr/bin/env python3
"""
HKEX公告PDF文件智能重组脚本
基于common_keywords配置自动重新整理现有的PDF文件
"""

import os
import shutil
import re
import yaml
from pathlib import Path
from typing import Dict, List, Tuple, Any
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pdf_reorganization.log')
    ]
)
logger = logging.getLogger(__name__)


class PDFReorganizer:
    """PDF文件智能重组器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化重组器"""
        self.config = self.load_config(config_path)
        self.keyword_config = self.config.get('common_keywords', {})
        self.dry_run = True  # 默认模拟运行
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'categorized_files': 0,
            'uncategorized_files': 0,
            'errors': 0,
            'categories': {}
        }
        
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info("✅ 配置文件加载成功")
            return config
        except Exception as e:
            logger.error(f"❌ 配置文件加载失败: {e}")
            return {}
    
    def extract_info_from_filename(self, filename: str) -> Tuple[str, str, str, str]:
        """
        从文件名提取信息
        
        Args:
            filename: 文件名，格式如 "2025-09-12_ALCO_HOLDINGS_00328_供股建議公告.pdf"
            
        Returns:
            Tuple[date, company_name, stock_code, title]
        """
        try:
            # 移除.pdf扩展名
            name_without_ext = filename.replace('.pdf', '')
            
            # 尝试匹配标准格式：日期_公司名_股票代码_标题
            pattern = r'^(\d{4}-\d{2}-\d{2})_(.+?)_(\d{5}|\d{5}\.HK)_(.+)$'
            match = re.match(pattern, name_without_ext)
            
            if match:
                date, company_name, stock_code, title = match.groups()
                # 清理股票代码（移除.HK后缀）
                stock_code = stock_code.replace('.HK', '')
                return date, company_name, stock_code, title
            
            # 尝试匹配另一种格式：日期_公司名_股票代码_其他信息
            pattern2 = r'^(\d{4}-\d{2}-\d{2})_(.+?)_(\d{5}|\d{5}\.HK)(.*)$'
            match2 = re.match(pattern2, name_without_ext)
            
            if match2:
                date, company_name, stock_code, rest = match2.groups()
                stock_code = stock_code.replace('.HK', '')
                title = rest.strip('_') if rest else "其他公告"
                return date, company_name, stock_code, title
                
            # 如果无法解析，返回默认值
            return "unknown", "unknown_company", "00000", filename
            
        except Exception as e:
            logger.warning(f"文件名解析失败 {filename}: {e}")
            return "unknown", "unknown_company", "00000", filename
    
    def classify_by_keywords(self, title: str) -> Tuple[str, int, float]:
        """
        基于关键字分类标题
        
        Args:
            title: 公告标题
            
        Returns:
            Tuple[category_name, priority, weight]
        """
        best_match = None
        best_priority = 0
        best_weight = 0.0
        
        title_lower = title.lower()
        
        for category_key, category_config in self.keyword_config.items():
            if not isinstance(category_config, dict):
                continue
                
            folder_name = category_config.get('folder_name', category_key)
            priority = category_config.get('priority', 50)
            weight = category_config.get('weight', 0.5)
            
            # 检查中文关键字
            chinese_keywords = category_config.get('chinese', [])
            for keyword in chinese_keywords:
                if keyword.lower() in title_lower:
                    if priority > best_priority or (priority == best_priority and weight > best_weight):
                        best_match = folder_name
                        best_priority = priority
                        best_weight = weight
                        break
            
            # 检查英文关键字
            english_keywords = category_config.get('english', [])
            for keyword in english_keywords:
                if keyword.lower() in title_lower:
                    if priority > best_priority or (priority == best_priority and weight > best_weight):
                        best_match = folder_name
                        best_priority = priority
                        best_weight = weight
                        break
        
        return best_match or "其他", best_priority, best_weight
    
    def get_priority_level(self, priority: int) -> str:
        """根据优先级返回等级标识"""
        if priority >= 85:
            return "🚨特高优先级"
        elif priority >= 70:
            return "🔴高优先级"
        elif priority >= 55:
            return "🟡中优先级"
        else:
            return "🟢低优先级"
    
    def generate_new_path(self, base_dir: str, date: str, company_name: str, 
                         stock_code: str, title: str, category: str, 
                         priority: int) -> str:
        """
        生成新的文件路径
        
        优化后的目录结构：
        hkexann/
        ├── 🚨特高优先级/
        │   ├── IPO/
        │   │   └── {stock_code}_{company_name}/
        │   │       └── {date}/
        │   └── 私有化/
        ├── 🔴高优先级/
        │   ├── 合股/
        │   └── 拆股/
        ├── 🟡中优先级/
        │   ├── 供股/
        │   └── 配股/
        ├── 🟢低优先级/
        │   └── 回购/
        └── 📅按日期归档/
            └── 2025-09-12/
        """
        priority_level = self.get_priority_level(priority)
        
        # 清理路径名称
        clean_company = re.sub(r'[<>:"/\\|?*]', '-', company_name)
        clean_category = re.sub(r'[<>:"/\\|?*]', '-', category)
        
        # 构建新路径
        new_path = os.path.join(
            base_dir,
            priority_level,
            clean_category,
            f"{stock_code}_{clean_company}",
            date
        )
        
        return new_path
    
    def reorganize_directory(self, source_dir: str, target_dir: str = None) -> Dict[str, Any]:
        """
        重组整个目录的PDF文件
        
        Args:
            source_dir: 源目录路径
            target_dir: 目标目录路径（如果为None，则在源目录旁创建新目录）
        """
        if target_dir is None:
            target_dir = source_dir + "_reorganized"
        
        logger.info(f"🚀 开始重组目录: {source_dir}")
        logger.info(f"📁 目标目录: {target_dir}")
        logger.info(f"🔄 模式: {'模拟运行' if self.dry_run else '实际执行'}")
        
        # 确保目标目录存在
        if not self.dry_run:
            os.makedirs(target_dir, exist_ok=True)
        
        # 遍历所有PDF文件
        for root, dirs, files in os.walk(source_dir):
            pdf_files = [f for f in files if f.lower().endswith('.pdf')]
            self.stats['total_files'] += len(pdf_files)
            
            for filename in pdf_files:
                try:
                    self.process_single_file(root, filename, target_dir)
                    self.stats['processed_files'] += 1
                except Exception as e:
                    logger.error(f"❌ 处理文件失败 {filename}: {e}")
                    self.stats['errors'] += 1
        
        self.print_statistics()
        return self.stats
    
    def process_single_file(self, source_dir: str, filename: str, target_base: str):
        """处理单个PDF文件"""
        source_path = os.path.join(source_dir, filename)
        
        # 提取文件信息
        date, company_name, stock_code, title = self.extract_info_from_filename(filename)
        
        # 智能分类
        category, priority, weight = self.classify_by_keywords(title)
        
        # 更新统计
        if category != "其他":
            self.stats['categorized_files'] += 1
        else:
            self.stats['uncategorized_files'] += 1
        
        if category not in self.stats['categories']:
            self.stats['categories'][category] = 0
        self.stats['categories'][category] += 1
        
        # 生成新路径
        new_dir = self.generate_new_path(
            target_base, date, company_name, stock_code, title, category, priority
        )
        new_path = os.path.join(new_dir, filename)
        
        # 日志输出
        priority_level = self.get_priority_level(priority)
        logger.info(f"📄 {filename}")
        logger.info(f"   📊 分类: {category} | 优先级: {priority} ({priority_level}) | 权重: {weight}")
        logger.info(f"   🏢 公司: {company_name} ({stock_code}) | 📅 日期: {date}")
        logger.info(f"   📁 新路径: {new_path}")
        
        # 执行文件操作
        if not self.dry_run:
            os.makedirs(new_dir, exist_ok=True)
            shutil.copy2(source_path, new_path)
            logger.info(f"   ✅ 文件已复制")
        else:
            logger.info(f"   🔍 模拟运行 - 未实际移动文件")
        
        logger.info("")  # 空行分隔
    
    def print_statistics(self):
        """打印统计信息"""
        logger.info("=" * 60)
        logger.info("📊 重组统计报告")
        logger.info("=" * 60)
        logger.info(f"📄 总文件数: {self.stats['total_files']}")
        logger.info(f"✅ 已处理: {self.stats['processed_files']}")
        logger.info(f"🎯 已分类: {self.stats['categorized_files']}")
        logger.info(f"❓ 未分类: {self.stats['uncategorized_files']}")
        logger.info(f"❌ 错误数: {self.stats['errors']}")
        logger.info("")
        
        logger.info("📋 分类分布:")
        for category, count in sorted(self.stats['categories'].items(), 
                                    key=lambda x: x[1], reverse=True):
            logger.info(f"   {category}: {count} 个文件")
        
        logger.info("=" * 60)
    
    def set_dry_run(self, dry_run: bool):
        """设置是否为模拟运行"""
        self.dry_run = dry_run
        logger.info(f"🔄 设置运行模式: {'模拟运行' if dry_run else '实际执行'}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="HKEX PDF文件智能重组工具")
    parser.add_argument('source_dir', help='源目录路径')
    parser.add_argument('--target-dir', help='目标目录路径（可选）')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径')
    parser.add_argument('--execute', action='store_true', help='实际执行（默认为模拟运行）')
    
    args = parser.parse_args()
    
    # 创建重组器
    reorganizer = PDFReorganizer(args.config)
    reorganizer.set_dry_run(not args.execute)
    
    # 执行重组
    stats = reorganizer.reorganize_directory(args.source_dir, args.target_dir)
    
    if not args.execute:
        print("\n" + "="*60)
        print("🔍 这是模拟运行！")
        print("📝 如果结果满意，请添加 --execute 参数实际执行")
        print("💡 示例: python reorganize_pdfs.py /path/to/hkexann --execute")
        print("="*60)


if __name__ == "__main__":
    main()
