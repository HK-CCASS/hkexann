"""
统一文件管理系统
处理文件命名、目录结构、智能分类和符号链接
"""

import os
import re
import shutil
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging

from ..config.unified_config import (
    FileManagementConfig,
    FileNamingStrategy,
    DirectoryStructure
)

logger = logging.getLogger(__name__)


class FileNamingManager:
    """文件命名管理器"""

    def __init__(self, config: FileManagementConfig):
        """
        初始化文件命名管理器

        Args:
            config: 文件管理配置
        """
        self.config = config
        self.naming_cache = {}  # 缓存已生成的文件名，避免重复

    def generate_filename(self, announcement: Dict[str, Any]) -> str:
        """
        根据配置生成文件名

        Args:
            announcement: 公告信息字典

        Returns:
            生成的文件名
        """
        strategy = self.config.naming_strategy

        if strategy == FileNamingStrategy.STANDARD:
            filename = self._generate_standard_name(announcement)
        elif strategy == FileNamingStrategy.ORIGINAL:
            filename = self._generate_original_name(announcement)
        elif strategy == FileNamingStrategy.COMPACT:
            filename = self._generate_compact_name(announcement)
        elif strategy == FileNamingStrategy.CUSTOM:
            filename = self._generate_custom_name(announcement)
        else:
            filename = self._generate_standard_name(announcement)

        # 清理和截断文件名
        if self.config.sanitize_filename:
            filename = self._sanitize_filename(filename)

        # 确保文件名长度不超过限制
        filename = self._truncate_filename(filename, self.config.max_filename_length)

        # 确保文件名唯一
        filename = self._ensure_unique_filename(filename, announcement)

        return filename

    def _generate_standard_name(self, announcement: Dict[str, Any]) -> str:
        """生成标准格式文件名：日期_股票代码_公司名称_标题"""
        date = announcement.get('date', datetime.now().strftime('%Y%m%d'))
        stock_code = announcement.get('stock_code', 'unknown')
        company_name = announcement.get('company_name', '').replace(' ', '_')[:30]
        title = announcement.get('title', 'untitled')

        # 清理特殊字符
        company_name = re.sub(r'[^\w\u4e00-\u9fa5-]', '', company_name)
        title = re.sub(r'[^\w\u4e00-\u9fa5-]', '_', title)

        return f"{date}_{stock_code}_{company_name}_{title}.pdf"

    def _generate_original_name(self, announcement: Dict[str, Any]) -> str:
        """保留原始文件名"""
        original_name = announcement.get('original_filename', '')
        if not original_name:
            # 如果没有原始文件名，使用标准格式
            return self._generate_standard_name(announcement)

        # 确保有.pdf扩展名
        if not original_name.endswith('.pdf'):
            original_name += '.pdf'

        return original_name

    def _generate_compact_name(self, announcement: Dict[str, Any]) -> str:
        """生成紧凑格式文件名：股票代码_日期_序号"""
        stock_code = announcement.get('stock_code', 'unknown')
        date = announcement.get('date', datetime.now().strftime('%Y%m%d'))

        # 生成唯一序号
        base_key = f"{stock_code}_{date}"
        if base_key not in self.naming_cache:
            self.naming_cache[base_key] = 0
        self.naming_cache[base_key] += 1

        seq = self.naming_cache[base_key]
        return f"{stock_code}_{date}_{seq:04d}.pdf"

    def _generate_custom_name(self, announcement: Dict[str, Any]) -> str:
        """根据自定义模板生成文件名"""
        template = self.config.naming_template

        # 可用变量
        variables = {
            'date': announcement.get('date', datetime.now().strftime('%Y%m%d')),
            'year': announcement.get('date', datetime.now().strftime('%Y'))[:4],
            'month': announcement.get('date', datetime.now().strftime('%m'))[4:6] if len(announcement.get('date', '')) >= 6 else datetime.now().strftime('%m'),
            'day': announcement.get('date', datetime.now().strftime('%d'))[6:8] if len(announcement.get('date', '')) >= 8 else datetime.now().strftime('%d'),
            'stock_code': announcement.get('stock_code', 'unknown'),
            'company_name': announcement.get('company_name', ''),
            'title': announcement.get('title', 'untitled'),
            'category': announcement.get('category', 'general'),
            'doc_id': announcement.get('doc_id', ''),
            'timestamp': datetime.now().strftime('%Y%m%d_%H%M%S')
        }

        # 替换变量
        filename = template
        for key, value in variables.items():
            placeholder = f"{{{key}}}"
            if placeholder in filename:
                # 清理值中的特殊字符
                clean_value = re.sub(r'[^\w\u4e00-\u9fa5-]', '_', str(value))
                filename = filename.replace(placeholder, clean_value)

        # 确保有.pdf扩展名
        if not filename.endswith('.pdf'):
            filename += '.pdf'

        return filename

    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名中的非法字符"""
        # Windows文件名非法字符
        illegal_chars = r'[<>:"/\\|?*]'
        filename = re.sub(illegal_chars, '_', filename)

        # 移除控制字符
        filename = ''.join(char for char in filename if ord(char) >= 32)

        # 移除前后空格和点
        filename = filename.strip('. ')

        # 确保文件名不为空
        if not filename or filename == '.pdf':
            filename = 'unnamed.pdf'

        return filename

    def _truncate_filename(self, filename: str, max_length: int) -> str:
        """截断文件名到指定长度"""
        if len(filename) <= max_length:
            return filename

        # 保留扩展名
        name, ext = os.path.splitext(filename)

        # 计算可用长度
        available_length = max_length - len(ext) - 3  # 预留3个字符给"..."

        if available_length <= 0:
            # 如果文件名太短，使用哈希值
            hash_value = hashlib.md5(filename.encode()).hexdigest()[:8]
            return f"{hash_value}{ext}"

        # 截断并添加省略号
        truncated = name[:available_length] + "..."
        return truncated + ext

    def _ensure_unique_filename(self, filename: str, announcement: Dict[str, Any]) -> str:
        """确保文件名唯一性"""
        # 如果公告有唯一ID，可以在冲突时添加ID
        if 'doc_id' in announcement:
            name, ext = os.path.splitext(filename)
            doc_id = announcement['doc_id']
            # 检查是否已经包含ID
            if doc_id not in name:
                # 在文件名末尾添加ID的后8位
                unique_suffix = doc_id[-8:] if len(doc_id) > 8 else doc_id
                filename = f"{name}_{unique_suffix}{ext}"

        return filename


class DirectoryManager:
    """目录管理器"""

    def __init__(self, config: FileManagementConfig):
        """
        初始化目录管理器

        Args:
            config: 文件管理配置
        """
        self.config = config
        self.base_path = Path(config.base_path).resolve()

    def get_save_path(self, announcement: Dict[str, Any]) -> Path:
        """
        根据配置获取文件保存路径

        Args:
            announcement: 公告信息

        Returns:
            保存路径
        """
        structure = self.config.directory_structure

        if structure == DirectoryStructure.FLAT:
            path = self._get_flat_path()
        elif structure == DirectoryStructure.BY_STOCK:
            path = self._get_by_stock_path(announcement)
        elif structure == DirectoryStructure.BY_DATE:
            path = self._get_by_date_path(announcement)
        elif structure == DirectoryStructure.BY_CATEGORY:
            path = self._get_by_category_path(announcement)
        elif structure == DirectoryStructure.HIERARCHICAL:
            path = self._get_hierarchical_path(announcement)
        else:
            path = self._get_hierarchical_path(announcement)

        # 创建目录
        path.mkdir(parents=True, exist_ok=True)

        return path

    def _get_flat_path(self) -> Path:
        """获取平铺模式路径"""
        return self.base_path / "HKEX"

    def _get_by_stock_path(self, announcement: Dict[str, Any]) -> Path:
        """获取按股票分组的路径"""
        stock_code = announcement.get('stock_code', 'unknown')
        path = self.base_path / "HKEX" / stock_code

        # 可选：添加分类子目录
        if self.config.create_category_subdirs:
            category = announcement.get('category', '其他')
            path = path / category

        return path

    def _get_by_date_path(self, announcement: Dict[str, Any]) -> Path:
        """获取按日期分组的路径"""
        date_str = announcement.get('date', datetime.now().strftime('%Y%m%d'))

        # 解析日期
        try:
            if len(date_str) == 8:  # YYYYMMDD格式
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
            else:
                # 其他格式，尝试解析
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                year = date_obj.strftime('%Y')
                month = date_obj.strftime('%m')
                day = date_obj.strftime('%d')
        except:
            # 解析失败，使用当前日期
            now = datetime.now()
            year = now.strftime('%Y')
            month = now.strftime('%m')
            day = now.strftime('%d')

        path = self.base_path / "HKEX" / year / month

        if self.config.create_date_subdirs:
            path = path / day

        return path

    def _get_by_category_path(self, announcement: Dict[str, Any]) -> Path:
        """获取按分类分组的路径"""
        category = announcement.get('category', '其他')
        path = self.base_path / "HKEX" / category

        # 可选：添加股票子目录
        stock_code = announcement.get('stock_code')
        if stock_code:
            path = path / stock_code

        return path

    def _get_hierarchical_path(self, announcement: Dict[str, Any]) -> Path:
        """获取层级模式路径：/股票代码/分类/年月/"""
        stock_code = announcement.get('stock_code', 'unknown')
        category = announcement.get('category', '其他')
        date_str = announcement.get('date', datetime.now().strftime('%Y%m%d'))

        # 构建基础路径
        path = self.base_path / "HKEX" / stock_code

        # 添加分类目录
        if self.config.create_category_subdirs:
            path = path / category

        # 添加日期目录
        if self.config.create_date_subdirs:
            try:
                if len(date_str) >= 6:
                    year_month = date_str[:6]  # YYYYMM
                else:
                    year_month = datetime.now().strftime('%Y%m')
            except:
                year_month = datetime.now().strftime('%Y%m')

            path = path / year_month

        return path

    def create_symlinks(self, file_path: Path, announcement: Dict[str, Any]) -> List[Path]:
        """
        创建符号链接用于多维度组织

        Args:
            file_path: 原始文件路径
            announcement: 公告信息

        Returns:
            创建的符号链接路径列表
        """
        if not self.config.enable_symlinks:
            return []

        symlinks = []

        for strategy in self.config.symlink_strategies:
            if strategy == "by_date":
                link_path = self._create_date_symlink(file_path, announcement)
            elif strategy == "by_category":
                link_path = self._create_category_symlink(file_path, announcement)
            elif strategy == "by_company":
                link_path = self._create_company_symlink(file_path, announcement)
            else:
                continue

            if link_path:
                symlinks.append(link_path)

        return symlinks

    def _create_date_symlink(self, file_path: Path, announcement: Dict[str, Any]) -> Optional[Path]:
        """创建按日期组织的符号链接"""
        date_str = announcement.get('date', datetime.now().strftime('%Y%m%d'))

        try:
            year = date_str[:4]
            month = date_str[4:6] if len(date_str) >= 6 else '01'

            link_dir = self.base_path / "by_date" / year / month
            link_dir.mkdir(parents=True, exist_ok=True)

            link_path = link_dir / file_path.name

            if not link_path.exists():
                link_path.symlink_to(file_path)
                logger.debug(f"创建日期符号链接: {link_path} -> {file_path}")
                return link_path
        except Exception as e:
            logger.warning(f"创建日期符号链接失败: {e}")

        return None

    def _create_category_symlink(self, file_path: Path, announcement: Dict[str, Any]) -> Optional[Path]:
        """创建按分类组织的符号链接"""
        category = announcement.get('category', '其他')

        try:
            link_dir = self.base_path / "by_category" / category
            link_dir.mkdir(parents=True, exist_ok=True)

            link_path = link_dir / file_path.name

            if not link_path.exists():
                link_path.symlink_to(file_path)
                logger.debug(f"创建分类符号链接: {link_path} -> {file_path}")
                return link_path
        except Exception as e:
            logger.warning(f"创建分类符号链接失败: {e}")

        return None

    def _create_company_symlink(self, file_path: Path, announcement: Dict[str, Any]) -> Optional[Path]:
        """创建按公司组织的符号链接"""
        company_name = announcement.get('company_name', '')

        if not company_name:
            return None

        try:
            # 清理公司名称
            clean_name = re.sub(r'[^\w\u4e00-\u9fa5]', '_', company_name)[:50]

            link_dir = self.base_path / "by_company" / clean_name
            link_dir.mkdir(parents=True, exist_ok=True)

            link_path = link_dir / file_path.name

            if not link_path.exists():
                link_path.symlink_to(file_path)
                logger.debug(f"创建公司符号链接: {link_path} -> {file_path}")
                return link_path
        except Exception as e:
            logger.warning(f"创建公司符号链接失败: {e}")

        return None


class SmartClassifier:
    """智能分类器"""

    def __init__(self, classification_rules: Dict[str, Any]):
        """
        初始化智能分类器

        Args:
            classification_rules: 分类规则配置
        """
        self.rules = classification_rules
        self.priority_map = self._build_priority_map()

    def _build_priority_map(self) -> List[Tuple[str, Dict[str, Any]]]:
        """构建优先级映射"""
        priority_list = []

        for category, rule in self.rules.items():
            priority = rule.get('priority', 0)
            priority_list.append((category, rule))

        # 按优先级排序
        priority_list.sort(key=lambda x: x[1].get('priority', 0), reverse=True)

        return priority_list

    def classify(self, announcement: Dict[str, Any]) -> str:
        """
        对公告进行分类

        Args:
            announcement: 公告信息

        Returns:
            分类名称
        """
        title = announcement.get('title', '')
        content = announcement.get('content', '')
        long_text = announcement.get('long_text', '')

        # 合并所有文本用于匹配
        full_text = f"{title} {content} {long_text}".lower()

        # 按优先级检查规则
        for category, rule in self.priority_map:
            # 检查中文关键词
            chinese_keywords = rule.get('chinese', [])
            for keyword in chinese_keywords:
                if keyword.lower() in full_text:
                    return rule.get('folder_name', category)

            # 检查英文关键词
            english_keywords = rule.get('english', [])
            for keyword in english_keywords:
                if keyword.lower() in full_text:
                    return rule.get('folder_name', category)

        # 默认分类
        return '其他'

    def get_category_info(self, category: str) -> Dict[str, Any]:
        """
        获取分类详细信息

        Args:
            category: 分类名称

        Returns:
            分类信息字典
        """
        for cat_key, rule in self.rules.items():
            if rule.get('folder_name') == category or cat_key == category:
                return rule

        return {'folder_name': category, 'priority': 0, 'weight': 0}


class UnifiedFileManager:
    """统一文件管理器 - 整合所有文件管理功能"""

    def __init__(self, config: FileManagementConfig, classification_rules: Optional[Dict[str, Any]] = None):
        """
        初始化统一文件管理器

        Args:
            config: 文件管理配置
            classification_rules: 分类规则（可选）
        """
        self.config = config
        self.naming_manager = FileNamingManager(config)
        self.directory_manager = DirectoryManager(config)
        self.classifier = SmartClassifier(classification_rules) if classification_rules else None

    def prepare_file_path(self, announcement: Dict[str, Any]) -> Tuple[Path, str]:
        """
        准备文件保存路径和文件名

        Args:
            announcement: 公告信息

        Returns:
            (保存目录路径, 文件名)
        """
        # 智能分类
        if self.classifier:
            category = self.classifier.classify(announcement)
            announcement['category'] = category

        # 生成文件名
        filename = self.naming_manager.generate_filename(announcement)

        # 获取保存路径
        save_dir = self.directory_manager.get_save_path(announcement)

        return save_dir, filename

    def save_file(self, content: bytes, announcement: Dict[str, Any]) -> Path:
        """
        保存文件到磁盘

        Args:
            content: 文件内容
            announcement: 公告信息

        Returns:
            保存的文件路径
        """
        # 准备路径
        save_dir, filename = self.prepare_file_path(announcement)
        file_path = save_dir / filename

        # 检查文件是否已存在
        if file_path.exists():
            # 如果文件已存在，检查是否需要覆盖
            if not self.config.preserve_original_structure:
                logger.info(f"文件已存在，跳过: {file_path}")
                return file_path

            # 生成新的文件名
            counter = 1
            name, ext = os.path.splitext(filename)
            while file_path.exists():
                new_filename = f"{name}_{counter}{ext}"
                file_path = save_dir / new_filename
                counter += 1

        # 保存文件
        try:
            file_path.write_bytes(content)
            logger.info(f"文件已保存: {file_path}")

            # 创建符号链接
            if self.config.enable_symlinks:
                symlinks = self.directory_manager.create_symlinks(file_path, announcement)
                if symlinks:
                    logger.info(f"创建了 {len(symlinks)} 个符号链接")

            return file_path
        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            raise

    def organize_existing_files(self, source_dir: str) -> Dict[str, List[Path]]:
        """
        重新组织现有文件

        Args:
            source_dir: 源目录

        Returns:
            组织结果字典
        """
        source_path = Path(source_dir)
        if not source_path.exists():
            raise ValueError(f"源目录不存在: {source_dir}")

        results = {
            'moved': [],
            'symlinked': [],
            'failed': []
        }

        # 遍历所有PDF文件
        for pdf_file in source_path.rglob('*.pdf'):
            try:
                # 从文件名提取信息
                announcement = self._extract_info_from_filename(pdf_file.name)

                # 获取新路径
                new_dir, new_filename = self.prepare_file_path(announcement)
                new_path = new_dir / new_filename

                # 移动或复制文件
                if new_path != pdf_file:
                    if new_path.exists():
                        logger.warning(f"目标文件已存在: {new_path}")
                        results['failed'].append(pdf_file)
                    else:
                        shutil.move(str(pdf_file), str(new_path))
                        results['moved'].append(new_path)

                        # 创建符号链接
                        if self.config.enable_symlinks:
                            symlinks = self.directory_manager.create_symlinks(new_path, announcement)
                            results['symlinked'].extend(symlinks)
            except Exception as e:
                logger.error(f"处理文件失败 {pdf_file}: {e}")
                results['failed'].append(pdf_file)

        return results

    def _extract_info_from_filename(self, filename: str) -> Dict[str, Any]:
        """
        从文件名提取公告信息

        Args:
            filename: 文件名

        Returns:
            公告信息字典
        """
        # 尝试解析标准格式：日期_股票代码_公司名称_标题.pdf
        pattern = r'(\d{8})_(\d{5})_([^_]+)_(.+)\.pdf'
        match = re.match(pattern, filename)

        if match:
            return {
                'date': match.group(1),
                'stock_code': match.group(2),
                'company_name': match.group(3),
                'title': match.group(4),
                'original_filename': filename
            }

        # 尝试其他格式
        # 格式2：股票代码_日期_序号.pdf
        pattern2 = r'(\d{5})_(\d{8})_(\d+)\.pdf'
        match2 = re.match(pattern2, filename)

        if match2:
            return {
                'stock_code': match2.group(1),
                'date': match2.group(2),
                'title': f"公告_{match2.group(3)}",
                'original_filename': filename
            }

        # 无法解析，返回基本信息
        return {
            'title': filename.replace('.pdf', ''),
            'original_filename': filename,
            'date': datetime.now().strftime('%Y%m%d')
        }

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取文件管理统计信息

        Returns:
            统计信息字典
        """
        base_path = Path(self.config.base_path)

        if not base_path.exists():
            return {
                'total_files': 0,
                'total_size': 0,
                'by_stock': {},
                'by_category': {},
                'by_date': {}
            }

        stats = {
            'total_files': 0,
            'total_size': 0,
            'by_stock': {},
            'by_category': {},
            'by_date': {}
        }

        # 统计所有PDF文件
        for pdf_file in base_path.rglob('*.pdf'):
            stats['total_files'] += 1
            stats['total_size'] += pdf_file.stat().st_size

            # 按股票统计
            parts = pdf_file.parts
            for i, part in enumerate(parts):
                if re.match(r'\d{5}', part):  # 股票代码格式
                    stats['by_stock'][part] = stats['by_stock'].get(part, 0) + 1
                    break

            # 按分类统计
            if self.classifier:
                info = self._extract_info_from_filename(pdf_file.name)
                category = self.classifier.classify(info)
                stats['by_category'][category] = stats['by_category'].get(category, 0) + 1

            # 按日期统计
            info = self._extract_info_from_filename(pdf_file.name)
            date = info.get('date', '')
            if date and len(date) >= 6:
                year_month = date[:6]
                stats['by_date'][year_month] = stats['by_date'].get(year_month, 0) + 1

        # 转换大小为可读格式
        stats['total_size_readable'] = self._format_size(stats['total_size'])

        return stats

    def _format_size(self, size: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"