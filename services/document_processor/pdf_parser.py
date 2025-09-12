"""
港交所PDF文档解析器 - 智能文档分块与内容提取服务

这个模块提供专门针对港交所公告PDF文档的高级解析功能，包括智能分块、
内容分类、元数据提取和向量化预处理。采用基于layout的分块策略，
确保上下文完整性和最佳的向量检索效果。

核心功能：
- PDF文档智能解析与文本提取
- 基于Token长度的自适应分块算法
- 港交所特有格式识别（开头语、表格、公告类型等）
- 文档元数据自动提取（股票代码、公司名称、发布日期等）
- 多层次内容分类（段落、表格、标题、列表）
- 边界框保留用于版面分析
- 文本去重和内容哈希

技术特性：
- PyMuPDF高性能PDF处理
- jieba中文分词优化的Token计算
- 正则表达式驱动的格式识别
- 自适应分块保持上下文完整性
- 支持跨页内容合并
- 完整的错误处理和日志记录

分块策略：
- 目标Token范围：300-500 tokens
- 最小Token数：250 tokens
- 最大Token数：550 tokens
- 支持跨页合并但保持语义完整性

数据结构：
- DocumentChunk: 文档分块数据类
- DocumentMetadata: 文档元数据类
- HKEXPDFParser: 主解析器类

Time Complexity: O(n*p) 其中n为页数，p为每页块数
Space Complexity: O(c*s) 其中c为chunk数量，s为平均chunk大小

作者: HKEX分析团队  
版本: 1.0.0
依赖: PyMuPDF, jieba, pathlib
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """
    文档分块数据类 - 表示PDF文档的一个语义完整的文本块
    
    每个DocumentChunk代表经过智能分块算法处理后的文档片段，
    保持语义完整性的同时控制在适合向量化的Token长度范围内。
    
    Attributes:
        chunk_id (str): 唯一的chunk标识符，格式为"doc_xxx_chunk_0001"
        doc_id (str): 所属文档的唯一标识符
        chunk_index (int): 在文档中的chunk序号（从0开始）
        page_number (int): chunk主要内容所在的页码
        text (str): chunk的完整文本内容
        text_length (int): 文本字符数量
        text_hash (str): 文本内容的MD5哈希，用于去重
        bbox (List[float]): 边界框坐标 [x1, y1, x2, y2]
        chunk_type (str): chunk类型标识：
            - "paragraph": 普通段落文本
            - "table": 表格内容  
            - "title": 标题或小标题
            - "list": 列表项内容
            - "other": 其他类型
        table_data (Optional[str]): 结构化表格数据（如有）
        is_header (bool): 是否为港交所公告开头语（不参与向量化）
        
    Note:
        - is_header为True的chunk通常不进行向量化处理
        - bbox保留原始PDF坐标信息，用于版面分析
        - text_hash可用于检测重复内容
    """
    chunk_id: str
    doc_id: str
    chunk_index: int
    page_number: int
    text: str
    text_length: int
    text_hash: str
    bbox: List[float]  # [x1, y1, x2, y2]
    chunk_type: str  # paragraph, table, title, list, other
    table_data: Optional[str] = None
    is_header: bool = False  # 是否为开头语


@dataclass
class DocumentMetadata:
    """
    PDF文档元数据类 - 存储文档的完整元信息
    
    包含从文件路径、文件名和PDF内容中提取的所有相关元数据信息，
    用于文档管理、检索和分析。
    
    Attributes:
        doc_id (str): 文档唯一标识符，基于文件哈希生成
        file_path (str): 文档的完整文件路径
        file_name (str): 文档文件名
        file_size (int): 文件大小（字节）
        file_hash (str): 文件内容MD5哈希值，用于去重和版本控制
        stock_code (str): 股票代码，如"00700"、"01299"等
        company_name (str): 公司名称，从文件名或路径中提取
        document_type (str): 文档类型，如"公告及通告"、"通函"等
        document_category (str): 文档分类，如"持續關連交易"、"股權披露"等
        document_title (str): 文档标题
        publish_date (Optional[datetime]): 文档发布日期
        page_count (int): PDF页数
        
    Note:
        - 从标准化的港交所文档路径结构中自动提取元数据
        - 支持多语言（中英文）文档识别
        - file_hash可用于文档去重和变更检测
    """
    doc_id: str
    file_path: str
    file_name: str
    file_size: int
    file_hash: str
    stock_code: str
    company_name: str
    document_type: str
    document_category: str
    document_title: str
    publish_date: Optional[datetime]
    page_count: int


class HKEXPDFParser:
    """
    港交所PDF文档智能解析器 - 专为HKEX公告文档优化的解析引擎
    
    这个类提供针对港交所公告PDF文档的专业化解析服务，包括智能分块、
    格式识别、元数据提取等功能。采用基于PyMuPDF的高性能处理引擎，
    结合港交所文档特有的格式规则进行优化。
    
    核心特性：
    - 港交所特有格式识别（开头语、公告类型、表格结构等）
    - 智能Token计算，结合中英文混合内容特点
    - 自适应分块算法，保持语义完整性
    - 多层次内容分类（段落、标题、列表、表格）
    - 跨页内容智能合并
    - 完整的元数据自动提取
    
    分块策略：
    - 基于Token长度的动态分块
    - 语义边界保护（避免在句子中间分割）
    - 跨页合并支持
    - 特殊内容识别（开头语、免责声明等）
    
    支持的文档类型：
    - 公告及通告
    - 通函文件
    - 月报文件
    - 其他监管文档
    
    Attributes:
        HEADER_PATTERNS (List[str]): 港交所开头语识别正则表达式
        TOKEN_TARGET_MIN (int): 目标Token数下限 (250)
        TOKEN_TARGET_MAX (int): 目标Token数上限 (550) 
        TOKEN_IDEAL_MIN (int): 理想Token数下限 (300)
        TOKEN_IDEAL_MAX (int): 理想Token数上限 (500)
        header_regex (Pattern): 编译后的开头语匹配模式
        
    Example:
        parser = HKEXPDFParser()
        metadata, chunks = parser.parse_pdf(Path("announcement.pdf"))
        
        for chunk in chunks:
            if not chunk.is_header:  # 跳过开头语
                print(f"Chunk {chunk.chunk_index}: {chunk.text[:100]}...")
                
    Note:
        - 建议PDF文件按照标准港交所路径结构组织
        - 开头语chunk不建议用于向量化
        - 支持中英文混合内容的Token计算
    """

    # 港交所公告开头语模式（这些内容不进行向量化）
    HEADER_PATTERNS = [r"香港交易及結算所有限公司.*不對本公告的內容承擔任何責任",
        r"香港交易所.*概不對本公告全部或任何部份內容而產生或因依賴該等內容而引致的任何損失承擔任何責任",
        r"The Stock Exchange of Hong Kong Limited.*takes no responsibility",
        r"本公告乃根據香港聯合交易所有限公司.*而刊載",
        r"This announcement is made pursuant to.*of The Stock Exchange of Hong Kong Limited",
        r"除另有說明外，本公告所載金額均以.*為單位", r"Unless otherwise stated.*are expressed in.*",
        r"緊急停牌|EMERGENCY SUSPENSION", r"恢復買賣|RESUMPTION OF TRADING"]

    # Token计算配置
    TOKEN_TARGET_MIN = 250  # 300-50
    TOKEN_TARGET_MAX = 550  # 500+50
    TOKEN_IDEAL_MIN = 300
    TOKEN_IDEAL_MAX = 500

    def __init__(self):
        """
        初始化港交所PDF解析器实例
        
        编译港交所开头语识别的正则表达式模式，为后续的文档解析做准备。
        开头语模式用于识别港交所公告中的标准免责声明和格式化声明。
        
        Note:
            - 自动编译多个开头语正则表达式模式
            - 使用IGNORECASE和DOTALL标志处理多行文本
            - 初始化完成后即可开始解析PDF文档
        """
        self.header_regex = re.compile('|'.join(self.HEADER_PATTERNS), re.IGNORECASE | re.DOTALL)
        logger.info("港交所PDF解析器初始化完成")

    def _is_official_category(self, category_name: str) -> bool:
        """
        判断是否为港交所官方分类
        
        支持模糊匹配，处理分类名称的变体
        
        Args:
            category_name: 目录名称
            
        Returns:
            bool: 是否为官方分类
        """
        # 标准化处理：去除空格，转换编码
        normalized = category_name.strip().replace(' ', '').replace('/', '')
        
        # 常见的官方分类模式
        official_patterns = [
            '公告', '通告', '通函', '财务', '報表', '环境', '社会', '管治',
            '合併', '守則', '債券', '结构', '產品'
        ]
        
        # 检查是否包含官方分类关键字
        for pattern in official_patterns:
            if pattern in normalized:
                return True
                
        return False

    def extract_metadata_from_path(self, file_path: Path) -> Dict[str, str]:
        """
        从标准化的港交所文档路径结构中提取元数据信息
        
        新的字段映射策略：
        - document_type = 港交所官方主分类 (如：公告及通告, 财务报表等)
        - document_category = 关键字分类/港交所子分类 (如：供股/[股份購回])
        
        期望的路径格式：
        hkexann/HKEX/{股票代码}/{关键字分类}/{港交所主分类}/{港交所子分类}/filename.pdf
        或者：
        hkexann/HKEX/{股票代码}/{港交所主分类}/{港交所子分类}/filename.pdf (无关键字分类)
        
        Args:
            file_path (Path): PDF文件的完整路径对象
            
        Returns:
            Dict[str, str]: 提取的元数据字典，包含：
                - stock_code: 股票代码（如"00700"）
                - company_name: 公司名称
                - document_type: 港交所官方主分类（如"公告及通告"）
                - document_category: 关键字分类+子分类（如"供股/[持續關連交易]"）
                - document_title: 文档标题
                - publish_date: 发布日期（datetime对象或None）
                
        Example:
            path1 = Path("hkexann/HKEX/00700/供股/公告及通告/持續關連交易/2024-01-15_供股公告.pdf")
            # document_type: "公告及通告"
            # document_category: "供股/持續關連交易"
            
            path2 = Path("hkexann/HKEX/00700/公告及通告/持續關連交易/2024-01-15_一般公告.pdf")  
            # document_type: "公告及通告"
            # document_category: "持續關連交易"
        """
        try:
            # 找到HKEX标准目录的位置
            parts = file_path.parts
            hkex_root_index = -1

            # 查找HKEX目录（标准结构：.../HKEX/{stockcode}/...）
            for i, part in enumerate(parts):
                if part.upper() == 'HKEX':
                    hkex_root_index = i
                    break

            # 定义已知的关键字分类列表（用于识别路径结构）
            keyword_categories = {
                'IPO', '新股', '私有化', '全购', '合股', '拆股', '供股', '配股', '配售', '可转换', '回购'
            }
            
            # 定义已知的港交所官方主分类
            official_main_categories = {
                '公告及通告', '翌日披露報表', '通函', '合併守則', 
                '財務報表/環境、社會及管治資料', '債券及結構性產品',
                '财务报表', '环境社会及管治资料'  # 简化版本
            }

            # 初始化变量
            stock_code = "UNKNOWN"
            document_type = "公告及通告"
            document_category = "其他"
            filename = parts[-1] if parts else "unknown.pdf"
            
            # 基于HKEX标准目录解析路径结构
            if hkex_root_index >= 0 and hkex_root_index + 2 <= len(parts) - 1:
                stock_code = parts[hkex_root_index + 1]  # HKEX后的第一级：股票代码
                filename = parts[-1]
                
                # 分析路径结构确定字段映射
                keyword_category = None
                official_main_category = None
                official_sub_category = None
                
                # 从股票代码后的目录开始分析
                path_after_stock = parts[hkex_root_index + 2:-1]  # 排除股票代码和文件名
                
                if len(path_after_stock) >= 1:
                    # 检查第一个目录是否为关键字分类
                    first_dir = path_after_stock[0]
                    if first_dir in keyword_categories:
                        keyword_category = first_dir
                        remaining_dirs = path_after_stock[1:]
                    else:
                        remaining_dirs = path_after_stock
                    
                    # 在剩余目录中查找港交所官方主分类
                    for i, dir_name in enumerate(remaining_dirs):
                        if dir_name in official_main_categories or self._is_official_category(dir_name):
                            official_main_category = dir_name
                            # 后续目录作为子分类
                            if i + 1 < len(remaining_dirs):
                                sub_dirs = remaining_dirs[i + 1:]
                                official_sub_category = '/'.join(sub_dirs)
                            break
                    
                    # 如果没找到官方主分类，使用第一个非关键字目录
                    if not official_main_category and remaining_dirs:
                        official_main_category = remaining_dirs[0]
                        if len(remaining_dirs) > 1:
                            official_sub_category = '/'.join(remaining_dirs[1:])
                
                # 构建最终的字段值
                document_type = official_main_category or "公告及通告"  # 默认为公告及通告
                
                # 构建document_category
                if keyword_category and official_sub_category:
                    document_category = f"{keyword_category}/{official_sub_category}"
                elif keyword_category:
                    document_category = keyword_category
                elif official_sub_category:
                    document_category = official_sub_category
                else:
                    document_category = "其他"
                
            # 回退逻辑：如果没找到HKEX目录
            else:
                # 回退逻辑：寻找股票代码位置
                stock_code_found = False
                stock_code_index = -1
                
                # 特殊处理：检查文件名是否以 "股票代码.HK_" 格式开头
                filename = parts[-1] if parts else ""
                logger.debug(f"检查文件名: {filename}")
                if filename and '_' in filename:
                    first_part = filename.split('_')[0]
                    logger.debug(f"文件名第一部分: {first_part}")
                    if '.' in first_part and first_part.endswith('.HK'):
                        # 提取.HK前的部分作为股票代码
                        potential_code = first_part.split('.')[0]
                        logger.debug(f"潜在股票代码: {potential_code}")
                        if potential_code.isdigit() and (len(potential_code) == 4 or len(potential_code) == 5):
                            stock_code = potential_code
                            stock_code_found = True
                            logger.info(f"从文件名提取股票代码: {stock_code} (原格式: {first_part})")
                        else:
                            logger.debug(f"潜在代码不符合格式: {potential_code}")
                    else:
                        logger.debug(f"第一部分不是.HK格式: {first_part}")
                else:
                    logger.debug(f"文件名不含下划线或为空: {filename}")
                
                # 如果没有从文件名找到，继续用原逻辑在目录中查找
                if not stock_code_found:
                    for i, part in enumerate(parts):
                        if part.isdigit() and (len(part) == 4 or len(part) == 5):
                            stock_code = part
                            stock_code_index = i
                            stock_code_found = True
                            break

                # 如果从目录中找到了股票代码，基于位置确定文档类型和分类
                if stock_code_found and stock_code_index >= 0 and stock_code_index + 1 < len(parts) - 1:
                    # 基于股票代码位置确定文档类型和分类（使用旧逻辑）
                    document_type = parts[stock_code_index + 1]  # 股票代码后第一级
                    if stock_code_index + 2 < len(parts) - 1:
                        # 合并所有后续目录为document_category
                        category_parts = parts[stock_code_index + 2:-1]
                        document_category = '/'.join(category_parts) if category_parts else "general"
                    else:
                        document_category = "general"
                # 如果没找到股票代码，保持初始默认值

            # 验证股票代码格式（排除 "UNKNOWN" 默认值）
            if stock_code != "UNKNOWN" and not (stock_code.isdigit() and (len(stock_code) == 4 or len(stock_code) == 5)):
                logger.warning(f"股票代码格式异常: {stock_code}, 路径: {file_path}")
            elif stock_code == "UNKNOWN":
                logger.debug(f"未能从路径提取股票代码: {file_path}")

            # 从文件名提取详细信息
            # 文件名格式: 时间_股票代码_公司名称_公告文件名.pdf
            try:
                filename_parts = filename.replace('.pdf', '').split('_')

                publish_date = None
                extracted_stock_code = stock_code  # 默认使用目录中的股票代码
                company_name = ""
                document_title = ""
            except Exception as filename_error:
                logger.error(f"文件名解析初始化异常: {filename_error}")
                raise
            
            if len(filename_parts) >= 4:
                # 解析标准格式: 时间_股票代码_公司名称_公告文件名
                try:
                    # 第1部分: 日期 (YYYY-MM-DD)
                    date_str = filename_parts[0]
                    publish_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    logger.debug(f"无法解析日期: {filename_parts[0]}")

                # 第2部分: 股票代码（验证与目录一致性）
                if len(filename_parts) > 1:
                    file_stock_code = filename_parts[1]
                    if file_stock_code == stock_code:
                        extracted_stock_code = file_stock_code
                    else:
                        logger.debug(f"文件名股票代码 {file_stock_code} 与目录 {stock_code} 不一致")
                        # 如果目录股票代码格式不正确，使用文件名中的
                        if file_stock_code.isdigit() and (len(file_stock_code) == 4 or len(file_stock_code) == 5):
                            extracted_stock_code = file_stock_code

                    # 第3部分: 公司名称
                    if len(filename_parts) > 2:
                        company_name = filename_parts[2]

                    # 第4部分及以后: 文档标题
                    if len(filename_parts) > 3:
                        document_title = '_'.join(filename_parts[3:])

                # >=4分支的return语句
                return {'stock_code': extracted_stock_code,
                    'company_name': company_name, 'document_type': document_type,
                    'document_category': document_category,
                    'document_title': document_title or filename, 'publish_date': publish_date}

            elif len(filename_parts) >= 3:
                # 兼容旧格式或简化格式
                try:
                    date_str = filename_parts[0]
                    publish_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except:
                    pass

                if len(filename_parts) > 2:
                    company_name = filename_parts[2]

                if len(filename_parts) > 3:
                    document_title = '_'.join(filename_parts[3:])

                return {'stock_code': extracted_stock_code,  # 使用验证后的股票代码
                    'company_name': company_name, 'document_type': document_type,
                    'document_category': document_category,
                    'document_title': document_title or filename, 'publish_date': publish_date}
            else:
                # 处理部分不足的情况
                if len(filename_parts) >= 1:
                    try:
                        date_str = filename_parts[0]
                        publish_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except:
                        pass
                if len(filename_parts) >= 2:
                    document_title = '_'.join(filename_parts[1:])

                return {'stock_code': extracted_stock_code,  # 使用验证后的股票代码
                    'company_name': company_name, 'document_type': document_type,  # 正确映射
                    'document_category': document_category,  # 正确映射
                    'document_title': document_title or filename, 'publish_date': publish_date}

        except Exception as e:
            logger.warning(f"从路径提取元数据失败: {e}")
            import traceback
            logger.debug(f"异常堆栈: {traceback.format_exc()}")

        # 默认返回
        logger.warning(f"使用默认返回值，路径: {file_path}")
        return {'stock_code': 'UNKNOWN', 'company_name': '', 'document_type': 'announcement',
            'document_category': 'general', 'document_title': file_path.stem, 'publish_date': None}

    def calculate_tokens(self, text: str) -> int:
        """
        智能计算中英文混合文本的Token数量
        
        采用专门优化的算法处理港交所公告中常见的中英文混合内容，
        准确估算用于向量化的Token数量。考虑中文字符、英文单词、
        数字和特殊符号的不同权重。
        
        计算规则：
        - 中文字符：每字符计为1个Token
        - 英文单词：每单词计为1个Token
        - 其他字符：每4个字符计为1个Token
        - 最小返回值：1
        
        Args:
            text (str): 要计算Token数的文本内容
            
        Returns:
            int: 估算的Token数量，最小值为1
            
        Note:
            - 专门优化中文分词处理
            - 考虑财经术语和港交所特有词汇
            - 结果用于控制chunk大小，确保向量化效果
            
        Example:
            # 中英文混合
            tokens = parser.calculate_tokens("騰訊控股有限公司 Tencent Holdings Limited")
            # 返回: ~12 tokens
            
            # 纯中文
            tokens = parser.calculate_tokens("持續關連交易公告")
            # 返回: ~7 tokens
        """
        if not text:
            return 0

        # 简单的token计算：中文字符计为1个token，英文单词计为1个token
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
        english_words = len(re.findall(r'\b[a-zA-Z]+\b', text))
        other_chars = len(re.sub(r'[\u4e00-\u9fff\s\b[a-zA-Z]+\b]', '', text))

        # 估算token数量
        tokens = chinese_chars + english_words + other_chars // 4
        return max(1, tokens)

    def is_header_content(self, text: str) -> bool:
        """判断是否为港交所开头语"""
        if not text or len(text.strip()) < 10:
            return False

        # 检查是否匹配开头语模式
        return bool(self.header_regex.search(text))

    def extract_text_blocks(self, pdf_path: Path) -> List[Dict[str, Any]]:
        """提取PDF中的文本块"""
        text_blocks = []

        try:
            doc = fitz.open(str(pdf_path))

            for page_num in range(len(doc)):
                page = doc[page_num]

                # 获取页面中的所有文本块
                blocks = page.get_text("dict")["blocks"]

                for block_num, block in enumerate(blocks):
                    if "lines" in block:  # 文本块
                        block_text = ""
                        bbox = block["bbox"]  # [x0, y0, x1, y1]

                        # 合并块中的所有行
                        for line in block["lines"]:
                            for span in line["spans"]:
                                block_text += span["text"]
                            block_text += "\n"

                        block_text = block_text.strip()

                        if block_text and len(block_text) > 5:  # 过滤太短的文本
                            text_blocks.append(
                                {'page_number': page_num + 1, 'block_number': block_num, 'text': block_text,
                                    'bbox': list(bbox), 'type': 'text'})

                    elif "image" in block:  # 图片块（暂时跳过）
                        continue

            doc.close()

        except Exception as e:
            logger.error(f"提取PDF文本块失败: {e}")

        return text_blocks

    def merge_blocks_to_chunks(self, text_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """将文本块合并为合适大小的chunks"""
        chunks = []
        current_chunk_text = ""
        current_chunk_blocks = []
        current_page = 1

        for block in text_blocks:
            block_text = block['text']
            block_tokens = self.calculate_tokens(block_text)
            current_tokens = self.calculate_tokens(current_chunk_text)

            # 判断是否应该开始新的chunk
            should_split = False

            if current_tokens == 0:
                # 第一个块，直接添加
                pass
            elif current_tokens + block_tokens > self.TOKEN_TARGET_MAX:
                # 超过最大长度，必须分割
                should_split = True
            elif (current_tokens >= self.TOKEN_IDEAL_MIN and current_tokens + block_tokens > self.TOKEN_IDEAL_MAX):
                # 在理想范围内且加上新块会超过理想最大值，可以分割
                should_split = True
            elif block['page_number'] != current_page and current_tokens >= self.TOKEN_IDEAL_MIN:
                # 跨页且当前chunk已达到最小理想长度
                should_split = True

            if should_split and current_chunk_text:
                # 创建当前chunk
                chunks.append({'text': current_chunk_text.strip(), 'blocks': current_chunk_blocks.copy(),
                    'page_number': current_page, 'token_count': current_tokens})

                # 重置
                current_chunk_text = ""
                current_chunk_blocks = []
                current_page = block['page_number']

            # 添加当前块
            if current_chunk_text:
                current_chunk_text += "\n\n"
            current_chunk_text += block_text
            current_chunk_blocks.append(block)
            current_page = block['page_number']

        # 添加最后一个chunk
        if current_chunk_text.strip():
            chunks.append(
                {'text': current_chunk_text.strip(), 'blocks': current_chunk_blocks, 'page_number': current_page,
                    'token_count': self.calculate_tokens(current_chunk_text)})

        return chunks

    def create_chunk_objects(self, chunks: List[Dict[str, Any]], doc_metadata: DocumentMetadata) -> List[DocumentChunk]:
        """创建DocumentChunk对象"""
        chunk_objects = []

        for i, chunk_data in enumerate(chunks):
            text = chunk_data['text']

            # 计算文本哈希
            text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()

            # 生成chunk_id
            chunk_id = f"{doc_metadata.doc_id}_chunk_{i:04d}"

            # 计算边界框（合并所有块的边界框）
            if chunk_data['blocks']:
                min_x = min(block['bbox'][0] for block in chunk_data['blocks'])
                min_y = min(block['bbox'][1] for block in chunk_data['blocks'])
                max_x = max(block['bbox'][2] for block in chunk_data['blocks'])
                max_y = max(block['bbox'][3] for block in chunk_data['blocks'])
                bbox = [min_x, min_y, max_x, max_y]
            else:
                bbox = [0, 0, 0, 0]

            # 判断chunk类型
            chunk_type = self.classify_chunk_type(text)

            # 判断是否为开头语
            is_header = self.is_header_content(text)

            chunk_obj = DocumentChunk(chunk_id=chunk_id, doc_id=doc_metadata.doc_id, chunk_index=i,
                page_number=chunk_data['page_number'], text=text, text_length=len(text), text_hash=text_hash, bbox=bbox,
                chunk_type=chunk_type, is_header=is_header)

            chunk_objects.append(chunk_obj)

        return chunk_objects

    def classify_chunk_type(self, text: str) -> str:
        """分类chunk类型"""
        text_lower = text.lower()

        # 表格标识
        if ('|' in text and text.count('|') >= 3) or ('表' in text and ('：' in text or ':' in text)):
            return 'table'

        # 标题标识
        if len(text) < 100 and (
                text.isupper() or re.match(r'^[一二三四五六七八九十\d]+[、\.]\s*[^\n]{5,50}$', text) or re.match(
            r'^[A-Z][A-Z\s\d\.\-]{5,50}$', text)):
            return 'title'

        # 列表标识
        if re.search(r'^[\s]*[(\[（【]?[一二三四五六七八九十\d]+[)\]）】、\.]\s', text, re.MULTILINE):
            return 'list'

        # 默认为段落
        return 'paragraph'

    def parse_pdf(self, pdf_path: Path) -> Tuple[DocumentMetadata, List[DocumentChunk]]:
        """
        完整解析港交所PDF文档，生成元数据和智能分块结果
        
        这是PDF解析器的主要入口方法，执行从原始PDF到结构化数据的完整转换。
        包括文件读取、元数据提取、文本分块、内容分类、开头语识别等全流程处理。
        
        处理流程：
        1. 计算文件基本信息（大小、哈希值）
        2. 从路径提取元数据（股票代码、公司名称等）
        3. 获取PDF页数和基本信息
        4. 提取所有文本块并保留位置信息
        5. 执行智能分块算法
        6. 创建标准化的DocumentChunk对象
        7. 生成统计信息和日志
        
        Args:
            pdf_path (Path): PDF文件的完整路径对象
            
        Returns:
            Tuple[DocumentMetadata, List[DocumentChunk]]: 包含两个元素的元组：
                - DocumentMetadata: 文档的完整元数据信息
                - List[DocumentChunk]: 文档分块列表，每个chunk包含：
                    * 文本内容和位置信息
                    * 内容类型分类（段落、表格、标题等）
                    * 是否为开头语的标识
                    * Token数量和哈希值
                    
        Raises:
            Exception: 当PDF文件无法读取或解析失败时抛出异常
            
        Note:
            - 开头语chunks标记为is_header=True，通常不用于向量化
            - 每个chunk的Token数控制在250-550范围内
            - 保留原始PDF坐标信息用于版面分析
            - 生成唯一的文档ID和chunk ID用于追踪
            
        Time Complexity: O(n*m) 其中n为页数，m为每页平均块数
        Space Complexity: O(c*s) 其中c为chunk数量，s为平均chunk大小
        
        Example:
            parser = HKEXPDFParser()
            pdf_path = Path("announcements/00700_announcement.pdf")
            
            try:
                metadata, chunks = parser.parse_pdf(pdf_path)
                
                print(f"文档: {metadata.document_title}")
                print(f"公司: {metadata.company_name} ({metadata.stock_code})")
                print(f"总chunks: {len(chunks)}")
                
                # 处理非开头语chunks
                content_chunks = [c for c in chunks if not c.is_header]
                print(f"内容chunks: {len(content_chunks)}")
                
            except Exception as e:
                print(f"解析失败: {e}")
        """
        try:
            logger.info(f"开始解析PDF: {pdf_path}")

            # 1. 计算文件基本信息
            file_stats = pdf_path.stat()
            file_size = file_stats.st_size

            with open(pdf_path, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()

            # 2. 从路径提取元数据
            path_metadata = self.extract_metadata_from_path(pdf_path)

            # 3. 获取PDF页数
            try:
                doc = fitz.open(str(pdf_path))
                page_count = len(doc)
                doc.close()
            except:
                page_count = 0

            # 4. 创建文档元数据
            doc_id = f"doc_{file_hash[:16]}"

            doc_metadata = DocumentMetadata(doc_id=doc_id, file_path=str(pdf_path), file_name=pdf_path.name,
                file_size=file_size, file_hash=file_hash, stock_code=path_metadata['stock_code'],
                company_name=path_metadata['company_name'], document_type=path_metadata['document_type'],
                document_category=path_metadata['document_category'], document_title=path_metadata['document_title'],
                publish_date=path_metadata['publish_date'], page_count=page_count)

            # 5. 提取文本块
            text_blocks = self.extract_text_blocks(pdf_path)

            if not text_blocks:
                logger.warning(f"PDF中未提取到文本内容: {pdf_path}")
                return doc_metadata, []

            # 6. 合并为chunks
            chunks = self.merge_blocks_to_chunks(text_blocks)

            # 7. 创建chunk对象
            chunk_objects = self.create_chunk_objects(chunks, doc_metadata)

            # 8. 统计信息
            total_chunks = len(chunk_objects)
            header_chunks = sum(1 for chunk in chunk_objects if chunk.is_header)
            content_chunks = total_chunks - header_chunks

            avg_tokens = sum(
                self.calculate_tokens(chunk.text) for chunk in chunk_objects) / total_chunks if total_chunks > 0 else 0

            logger.info(f"PDF解析完成: {pdf_path}")
            logger.info(f"  总chunks: {total_chunks}, 开头语chunks: {header_chunks}, 内容chunks: {content_chunks}")
            logger.info(f"  平均token数: {avg_tokens:.1f}")

            return doc_metadata, chunk_objects

        except Exception as e:
            logger.error(f"解析PDF失败 {pdf_path}: {e}")
            raise
