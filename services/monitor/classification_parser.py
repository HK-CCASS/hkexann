"""
HKEX公告分类解析器
解析HKEX分类信息文件，建立分类代码到过滤规则的映射
"""

import csv
import logging
from typing import Dict, List, Set, Optional, Tuple, Any
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class HKEXCategory:
    """HKEX分类信息数据类"""
    level1_code: str
    level1_name: str
    level1_count: int
    level2_code: str
    level2_name: str
    level3_code: str
    level3_name: str
    count: int
    full_path: str

    @property
    def level2_combined_code(self) -> str:
        """二级组合代码 (level1-level2)"""
        return f"{self.level1_code}-{self.level2_code}"

    @property
    def unique_id(self) -> str:
        """唯一标识符"""
        return self.level3_code


class HKEXClassificationParser:
    """
    HKEX分类文件解析器

    解析HKEX--分类信息-*.csv文件，建立完整的分类体系映射
    支持基于分类代码的精确过滤
    """

    def __init__(self, classification_file_path: Optional[str] = None):
        """
        初始化解析器

        Args:
            classification_file_path: 分类文件路径，如果为None则自动查找
        """
        self.classification_file_path = classification_file_path or self._find_classification_file()
        self.categories: Dict[str, HKEXCategory] = {}  # level3_code -> category
        self.level1_categories: Dict[str, str] = {}  # level1_code -> level1_name
        self.level2_categories: Dict[str, str] = {}  # level1-level2 -> level2_name
        self.name_to_code: Dict[str, str] = {}  # category_name -> level3_code
        self.loaded = False

    def _find_classification_file(self) -> str:
        """自动查找分类文件"""
        # 优先查找当前目录
        current_dir = Path.cwd()
        pattern = "HKEX--分类信息-*.csv"

        for file_path in current_dir.glob(pattern):
            return str(file_path)

        # 如果没找到，尝试其他可能的位置
        possible_paths = [
            "HKEX--分类信息-20250926.csv",  # 具体文件名
            "./HKEX--分类信息-20250926.csv",
            "../HKEX--分类信息-20250926.csv"
        ]

        for path_str in possible_paths:
            path = Path(path_str)
            if path.exists():
                return str(path)

        raise FileNotFoundError(f"未找到HKEX分类文件，搜索模式: {pattern}")

    def load_classifications(self) -> bool:
        """
        加载并解析分类文件

        Returns:
            bool: 是否成功加载
        """
        try:
            if not Path(self.classification_file_path).exists():
                logger.error(f"分类文件不存在: {self.classification_file_path}")
                return False

            logger.info(f"正在加载HKEX分类文件: {self.classification_file_path}")

            with open(self.classification_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)  # 读取表头

                # 验证表头格式
                expected_header = ['1级分类代码', '1级分类名称', '1级分类公告数量',
                                 '2级分组代码', '2级分组名称', '3级分类代码',
                                 '3级分类名称', '公告数量', '完整路径']
                if header != expected_header:
                    logger.warning(f"表头格式不匹配，期望: {expected_header}")
                    logger.warning(f"实际: {header}")

                # 解析数据
                for row_num, row in enumerate(reader, start=2):
                    try:
                        if len(row) != 9:
                            logger.warning(f"第{row_num}行数据格式错误，跳过: {row}")
                            continue

                        level1_code, level1_name, level1_count, level2_code, level2_name, \
                        level3_code, level3_name, count, full_path = row

                        # 创建分类对象
                        category = HKEXCategory(
                            level1_code=level1_code,
                            level1_name=level1_name,
                            level1_count=int(level1_count),
                            level2_code=level2_code,
                            level2_name=level2_name,
                            level3_code=level3_code,
                            level3_name=level3_name,
                            count=int(count),
                            full_path=full_path
                        )

                        # 建立映射
                        self.categories[level3_code] = category
                        self.level1_categories[level1_code] = level1_name
                        self.level2_categories[category.level2_combined_code] = level2_name

                        # 名称到代码的反向映射（用于兼容性）
                        self.name_to_code[level3_name] = level3_code

                        # 也支持部分名称匹配
                        self.name_to_code[level3_name.lower()] = level3_code

                    except Exception as e:
                        logger.error(f"解析第{row_num}行数据失败: {e}, 数据: {row}")
                        continue

            self.loaded = True
            logger.info(f"✅ 成功加载 {len(self.categories)} 个3级分类")
            logger.info(f"   1级分类: {len(self.level1_categories)} 个")
            logger.info(f"   2级分类: {len(self.level2_categories)} 个")

            return True

        except Exception as e:
            logger.error(f"加载分类文件失败: {e}")
            return False

    def get_category_by_code(self, level3_code: str) -> Optional[HKEXCategory]:
        """
        根据3级分类代码获取分类信息

        Args:
            level3_code: 3级分类代码

        Returns:
            HKEXCategory或None
        """
        return self.categories.get(level3_code)

    def get_category_by_name(self, name: str) -> Optional[HKEXCategory]:
        """
        根据分类名称获取分类信息

        Args:
            name: 分类名称

        Returns:
            HKEXCategory或None
        """
        code = self.name_to_code.get(name) or self.name_to_code.get(name.lower())
        if code:
            return self.get_category_by_code(code)
        return None

    def find_categories_by_keywords(self, keywords: List[str]) -> List[HKEXCategory]:
        """
        根据关键字查找匹配的分类

        Args:
            keywords: 关键字列表

        Returns:
            匹配的分类列表
        """
        if not keywords:
            return []

        matches = []
        for category in self.categories.values():
            category_text = f"{category.level1_name} {category.level2_name} {category.level3_name}".lower()
            if any(keyword.lower() in category_text for keyword in keywords):
                matches.append(category)

        return matches

    def get_excluded_codes_by_names(self, excluded_names: List[str]) -> Set[str]:
        """
        根据排除的分类名称获取对应的分类代码

        Args:
            excluded_names: 排除的分类名称列表

        Returns:
            排除的分类代码集合
        """
        excluded_codes = set()

        for name in excluded_names:
            # 直接名称匹配
            category = self.get_category_by_name(name)
            if category:
                excluded_codes.add(category.level3_code)
                continue

            # 关键字匹配
            matches = self.find_categories_by_keywords([name])
            for match in matches:
                excluded_codes.add(match.level3_code)

        return excluded_codes

    def get_level1_categories(self) -> Dict[str, str]:
        """获取所有1级分类"""
        return self.level1_categories.copy()

    def get_level2_categories(self, level1_code: Optional[str] = None) -> Dict[str, str]:
        """
        获取2级分类

        Args:
            level1_code: 指定1级分类代码，如果为None则返回所有

        Returns:
            2级分类字典
        """
        if level1_code:
            return {k: v for k, v in self.level2_categories.items()
                   if k.startswith(f"{level1_code}-")}
        return self.level2_categories.copy()

    def get_statistics(self) -> Dict[str, Any]:
        """获取分类统计信息"""
        if not self.loaded:
            return {"error": "分类文件未加载"}

        total_announcements = sum(cat.count for cat in self.categories.values())

        # 按1级分类统计
        level1_stats = {}
        for cat in self.categories.values():
            if cat.level1_code not in level1_stats:
                level1_stats[cat.level1_code] = {
                    "name": cat.level1_name,
                    "count": 0,
                    "subcategories": 0
                }
            level1_stats[cat.level1_code]["count"] += cat.count
            level1_stats[cat.level1_code]["subcategories"] += 1

        return {
            "total_categories": len(self.categories),
            "total_announcements": total_announcements,
            "level1_categories": len(self.level1_categories),
            "level2_categories": len(self.level2_categories),
            "level1_stats": level1_stats,
            "loaded": self.loaded
        }

    def is_excluded_category(self, level3_code: str, excluded_codes: Set[str]) -> bool:
        """
        检查分类代码是否在排除列表中

        Args:
            level3_code: 3级分类代码
            excluded_codes: 排除的分类代码集合

        Returns:
            bool: 是否排除
        """
        return level3_code in excluded_codes

    def get_category_hierarchy(self, level3_code: str) -> Optional[Dict[str, str]]:
        """
        获取分类的完整层级信息

        Args:
            level3_code: 3级分类代码

        Returns:
            包含完整层级信息的字典
        """
        category = self.get_category_by_code(level3_code)
        if not category:
            return None

        return {
            "level1_code": category.level1_code,
            "level1_name": category.level1_name,
            "level2_code": category.level2_code,
            "level2_name": category.level2_name,
            "level3_code": category.level3_code,
            "level3_name": category.level3_name,
            "full_path": category.full_path,
            "count": category.count
        }


# 全局单例实例
_default_parser = None

def get_classification_parser(classification_file_path: Optional[str] = None) -> HKEXClassificationParser:
    """
    获取全局HKEX分类解析器实例

    Args:
        classification_file_path: 分类文件路径

    Returns:
        HKEXClassificationParser实例
    """
    global _default_parser

    if _default_parser is None or (classification_file_path and
                                   _default_parser.classification_file_path != classification_file_path):
        _default_parser = HKEXClassificationParser(classification_file_path)

    return _default_parser
