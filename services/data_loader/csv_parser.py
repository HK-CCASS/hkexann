"""
CSV事件数据解析器
解析港交所事件CSV数据（供股、配股、合股等）
"""

import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class EventRecord:
    """事件记录基类"""
    stock_code: str
    company_name: str
    announcement_date: date
    status: str
    raw_data: Dict[str, Any]  # 原始数据备份


@dataclass
class RightsIssueRecord(EventRecord):
    """供股记录"""
    rights_price: Optional[float]
    rights_price_premium_text: str
    rights_price_premium: Optional[float]
    rights_ratio: str
    current_price: Optional[float]
    current_price_premium_text: str
    current_price_premium: Optional[float]
    stock_adjustment: str
    underwriter: str
    ex_rights_date: Optional[date]
    rights_trading_start: Optional[date]
    rights_trading_end: Optional[date]
    final_payment_date: Optional[date]
    result_announcement_date: Optional[date]
    allotment_date: Optional[date]


@dataclass
class PlacementRecord(EventRecord):
    """配股记录"""
    placement_price: Optional[float]
    placement_price_premium_text: str
    placement_price_premium: Optional[float]
    new_shares_ratio: Optional[float]
    current_price: Optional[float]
    current_price_premium_text: str
    current_price_premium: Optional[float]
    placement_agent: str
    authorization_method: str
    placement_method: str
    completion_date: Optional[date]


@dataclass
class ConsolidationRecord(EventRecord):
    """合股记录"""
    temporary_counter: str
    consolidation_ratio: str
    effective_date: Optional[date]
    other_corporate_actions: str


@dataclass
class StockSplitRecord(EventRecord):
    """拆股记录"""
    temporary_counter: str
    split_ratio: str
    effective_date: Optional[date]
    other_corporate_actions: str


class CSVEventParser:
    """CSV事件数据解析器"""

    def __init__(self):
        """初始化解析器"""
        logger.info("CSV事件数据解析器初始化完成")

    def parse_date(self, date_str: str) -> Optional[date]:
        """
        解析日期字符串
        支持多种日期格式，特别是港交所CSV中的DD/MM/YY格式
        """
        if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
            return None

        date_str = str(date_str).strip()

        # 处理特殊值
        if date_str in ['未公佈', '未公布', '-', '']:
            return None

        # DD/MM/YY格式（如 22/07/25）
        dd_mm_yy_pattern = r'(\d{1,2})/(\d{1,2})/(\d{2})'
        match = re.search(dd_mm_yy_pattern, date_str)
        if match:
            try:
                day, month, year = match.groups()
                # 假设两位年份：00-29表示20XX年，30-99表示19XX年  
                year = int(year)
                if year <= 29:
                    year = 2000 + year
                else:
                    year = 1900 + year
                return date(year, int(month), int(day))
            except ValueError:
                pass

        # 其他常见日期格式
        date_patterns = [r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
            r'(\d{4})/(\d{1,2})/(\d{1,2})',  # YYYY/MM/DD
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY
            r'(\d{4})(\d{2})(\d{2})',  # YYYYMMDD
        ]

        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    parts = match.groups()
                    if len(parts[0]) == 4:  # Year first
                        year, month, day = parts
                    elif len(parts[2]) == 4:  # Year last
                        day, month, year = parts
                    else:
                        continue

                    return date(int(year), int(month), int(day))
                except ValueError:
                    continue

        # 如果所有格式都失败，记录警告
        logger.warning(f"无法解析日期: {date_str}")
        return None

    def parse_price(self, price_str: str) -> Optional[float]:
        """
        解析价格字符串
        """
        if not price_str or pd.isna(price_str):
            return None

        price_str = str(price_str).strip()

        # 移除货币符号和特殊字符
        price_str = re.sub(r'[港币$,，\s]', '', price_str)

        # 提取数字
        match = re.search(r'(\d+\.?\d*)', price_str)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass

        return None

    def parse_percentage(self, pct_str: str) -> Optional[float]:
        """
        解析百分比字符串
        """
        if not pct_str or pd.isna(pct_str):
            return None

        pct_str = str(pct_str).strip()

        # 移除百分号和其他字符
        pct_str = re.sub(r'[%％\s]', '', pct_str)

        # 查找数字（包括负数）
        match = re.search(r'(-?\d+\.?\d*)', pct_str)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass

        return None

    def parse_ratio(self, ratio_str: str) -> str:
        """
        解析比例字符串（如"1:10"）
        """
        if not ratio_str or pd.isna(ratio_str):
            return ""

        return str(ratio_str).strip()

    def clean_text(self, text: str) -> str:
        """
        清理文本字段
        """
        if not text or pd.isna(text):
            return ""

        return str(text).strip()

    def parse_rights_issue_csv(self, csv_path: Path) -> List[RightsIssueRecord]:
        """
        解析供股CSV文件
        
        预期字段：代號,名稱,公佈日,供股價 / 溢價(折讓),比例,現價 / 溢價(折讓),除權日,供股權開始買賣,
                供股權結束買賣,最後付款,公佈结果,合/拆/紅股,包銷商,派送日,狀態
        """
        try:
            logger.info(f"解析供股CSV: {csv_path}")

            # 读取CSV文件
            df = pd.read_csv(csv_path, encoding='utf-8')

            if df.empty:
                logger.warning(f"CSV文件为空: {csv_path}")
                return []

            logger.info(f"CSV字段: {list(df.columns)}")
            logger.info(f"数据行数: {len(df)}")

            # 清理字段名（去除空格）
            df.columns = df.columns.str.strip()
            logger.info(f"清理后的CSV字段: {list(df.columns)}")

            records = []

            for index, row in df.iterrows():
                try:
                    # 基础字段
                    stock_code = self.clean_text(row.get('代號', ''))
                    company_name = self.clean_text(row.get('名稱', ''))
                    announcement_date = self.parse_date(row.get('公佈日', ''))
                    status = self.clean_text(row.get('狀態', ''))

                    # 供股特定字段
                    rights_price_text = self.clean_text(row.get('供股價 / 溢價(折讓)', ''))
                    rights_price = self.parse_price(rights_price_text)
                    rights_price_premium = self.parse_percentage(rights_price_text)

                    rights_ratio = self.parse_ratio(row.get('比例', ''))

                    current_price_text = self.clean_text(row.get('現價 / 溢價(折讓)', ''))
                    current_price = self.parse_price(current_price_text)
                    current_price_premium = self.parse_percentage(current_price_text)

                    stock_adjustment = self.clean_text(row.get('合/拆/紅股', ''))
                    underwriter = self.clean_text(row.get('包銷商', ''))

                    # 日期字段
                    ex_rights_date = self.parse_date(row.get('除權日', ''))

                    # 处理供股权买卖字段（可能包含换行符）
                    rights_trading_start_field = None
                    rights_trading_end_field = None
                    result_announcement_field = None

                    # 查找包含换行符的字段名称
                    for col in row.index:
                        col_clean = str(col).strip().replace('\n', '').replace('\r', '')
                        if '供股權開始買賣' in col_clean:
                            rights_trading_start_field = col
                        elif '供股權結束買賣' in col_clean:
                            rights_trading_end_field = col
                        elif '公佈結果' in col_clean or '公佈结果' in col_clean:
                            result_announcement_field = col

                    rights_trading_start = self.parse_date(
                        row.get(rights_trading_start_field, '') if rights_trading_start_field else '')
                    rights_trading_end = self.parse_date(
                        row.get(rights_trading_end_field, '') if rights_trading_end_field else '')
                    final_payment_date = self.parse_date(row.get('最後付款', ''))
                    result_announcement_date = self.parse_date(
                        row.get(result_announcement_field, '') if result_announcement_field else '')
                    allotment_date = self.parse_date(row.get('派送日', ''))

                    # 创建记录
                    record = RightsIssueRecord(stock_code=stock_code, company_name=company_name,
                        announcement_date=announcement_date, status=status, raw_data=row.to_dict(),
                        rights_price=rights_price, rights_price_premium_text=rights_price_text,
                        rights_price_premium=rights_price_premium, rights_ratio=rights_ratio,
                        current_price=current_price, current_price_premium_text=current_price_text,
                        current_price_premium=current_price_premium, stock_adjustment=stock_adjustment,
                        underwriter=underwriter, ex_rights_date=ex_rights_date,
                        rights_trading_start=rights_trading_start, rights_trading_end=rights_trading_end,
                        final_payment_date=final_payment_date, result_announcement_date=result_announcement_date,
                        allotment_date=allotment_date)

                    records.append(record)

                except Exception as e:
                    logger.error(f"解析供股记录失败 行{index}: {e}")
                    continue

            logger.info(f"成功解析供股记录: {len(records)}/{len(df)}")
            return records

        except Exception as e:
            logger.error(f"解析供股CSV失败: {e}")
            return []

    def parse_placement_csv(self, csv_path: Path) -> List[PlacementRecord]:
        """
        解析配股CSV文件
        
        预期字段：代號,名稱,公佈日,配股價 / 溢價(折讓),新股比例,現價 / 溢價(折讓),配售代理,授權方式,配售方式,完成配售日期,狀態
        """
        try:
            logger.info(f"解析配股CSV: {csv_path}")

            df = pd.read_csv(csv_path, encoding='utf-8')

            if df.empty:
                logger.warning(f"CSV文件为空: {csv_path}")
                return []

            logger.info(f"CSV字段: {list(df.columns)}")
            logger.info(f"数据行数: {len(df)}")

            # 清理字段名（去除空格）
            df.columns = df.columns.str.strip()
            logger.info(f"清理后的CSV字段: {list(df.columns)}")

            records = []

            for index, row in df.iterrows():
                try:
                    # 基础字段
                    stock_code = self.clean_text(row.get('代號', ''))
                    company_name = self.clean_text(row.get('名稱', ''))
                    announcement_date = self.parse_date(row.get('公佈日', ''))
                    status = self.clean_text(row.get('狀態', ''))

                    # 配股特定字段
                    placement_price_text = self.clean_text(row.get('配股價 / 溢價(折讓)', ''))
                    placement_price = self.parse_price(placement_price_text)
                    placement_price_premium = self.parse_percentage(placement_price_text)

                    new_shares_ratio = self.parse_percentage(row.get('新股比例', ''))

                    current_price_text = self.clean_text(row.get('現價 / 溢價(折讓)', ''))
                    current_price = self.parse_price(current_price_text)
                    current_price_premium = self.parse_percentage(current_price_text)

                    placement_agent = self.clean_text(row.get('配售代理', ''))
                    authorization_method = self.clean_text(row.get('授權方式', ''))
                    placement_method = self.clean_text(row.get('配售方式', ''))
                    completion_date = self.parse_date(row.get('完成配售日期', ''))

                    # 创建记录
                    record = PlacementRecord(stock_code=stock_code, company_name=company_name,
                        announcement_date=announcement_date, status=status, raw_data=row.to_dict(),
                        placement_price=placement_price, placement_price_premium_text=placement_price_text,
                        placement_price_premium=placement_price_premium, new_shares_ratio=new_shares_ratio,
                        current_price=current_price, current_price_premium_text=current_price_text,
                        current_price_premium=current_price_premium, placement_agent=placement_agent,
                        authorization_method=authorization_method, placement_method=placement_method,
                        completion_date=completion_date)

                    records.append(record)

                except Exception as e:
                    logger.error(f"解析配股记录失败 行{index}: {e}")
                    continue

            logger.info(f"成功解析配股记录: {len(records)}/{len(df)}")
            return records

        except Exception as e:
            logger.error(f"解析配股CSV失败: {e}")
            return []

    def parse_consolidation_csv(self, csv_path: Path) -> List[ConsolidationRecord]:
        """
        解析合股CSV文件
        
        预期字段：代號,名稱,公佈日,临时櫃位,比例,生效日期,其他股本活動,狀態
        """
        try:
            logger.info(f"解析合股CSV: {csv_path}")

            df = pd.read_csv(csv_path, encoding='utf-8')

            if df.empty:
                logger.warning(f"CSV文件为空: {csv_path}")
                return []

            logger.info(f"CSV字段: {list(df.columns)}")
            logger.info(f"数据行数: {len(df)}")

            # 清理字段名（去除空格）
            df.columns = df.columns.str.strip()
            logger.info(f"清理后的CSV字段: {list(df.columns)}")

            records = []

            for index, row in df.iterrows():
                try:
                    # 基础字段
                    stock_code = self.clean_text(row.get('代號', ''))
                    company_name = self.clean_text(row.get('名稱', ''))
                    announcement_date = self.parse_date(row.get('公佈日 ', ''))  # 注意：合股CSV中有空格
                    status = self.clean_text(row.get('狀態', ''))

                    # 合股特定字段
                    temporary_counter = self.clean_text(row.get('臨時櫃位', ''))
                    consolidation_ratio = self.parse_ratio(row.get('比例', ''))
                    effective_date = self.parse_date(row.get('生效日期', ''))
                    other_corporate_actions = self.clean_text(row.get('其他股本活動', ''))

                    # 创建记录
                    record = ConsolidationRecord(stock_code=stock_code, company_name=company_name,
                        announcement_date=announcement_date, status=status, raw_data=row.to_dict(),
                        temporary_counter=temporary_counter, consolidation_ratio=consolidation_ratio,
                        effective_date=effective_date, other_corporate_actions=other_corporate_actions)

                    records.append(record)

                except Exception as e:
                    logger.error(f"解析合股记录失败 行{index}: {e}")
                    continue

            logger.info(f"成功解析合股记录: {len(records)}/{len(df)}")
            return records

        except Exception as e:
            logger.error(f"解析合股CSV失败: {e}")
            return []

    def parse_stock_split_csv(self, csv_path: Path) -> List[StockSplitRecord]:
        """
        解析拆股CSV文件
        
        预期字段：代號,名稱,公佈日,临时櫃位,比例,生效日期,其他股本活動,狀態
        """
        try:
            logger.info(f"解析拆股CSV: {csv_path}")

            df = pd.read_csv(csv_path, encoding='utf-8')

            if df.empty:
                logger.warning(f"CSV文件为空: {csv_path}")
                return []

            logger.info(f"CSV字段: {list(df.columns)}")
            logger.info(f"数据行数: {len(df)}")

            # 清理字段名（去除空格）
            df.columns = df.columns.str.strip()
            logger.info(f"清理后的CSV字段: {list(df.columns)}")

            records = []

            for index, row in df.iterrows():
                try:
                    # 基础字段
                    stock_code = self.clean_text(row.get('代號', ''))
                    company_name = self.clean_text(row.get('名稱', ''))
                    announcement_date = self.parse_date(row.get('公佈日 ', ''))  # 注意：拆股CSV中有空格
                    status = self.clean_text(row.get('狀態', ''))

                    # 拆股特定字段
                    temporary_counter = self.clean_text(row.get('臨時櫃位', ''))
                    split_ratio = self.parse_ratio(row.get('比例', ''))
                    effective_date = self.parse_date(row.get('生效日期', ''))
                    other_corporate_actions = self.clean_text(row.get('其他股本活動', ''))

                    # 创建记录
                    record = StockSplitRecord(
                        stock_code=stock_code,
                        company_name=company_name,
                        announcement_date=announcement_date,
                        status=status,
                        raw_data=row.to_dict(),
                        temporary_counter=temporary_counter,
                        split_ratio=split_ratio,
                        effective_date=effective_date,
                        other_corporate_actions=other_corporate_actions
                    )

                    records.append(record)

                except Exception as e:
                    logger.error(f"解析拆股记录失败 行{index}: {e}")
                    continue

            logger.info(f"成功解析拆股记录: {len(records)}/{len(df)}")
            return records

        except Exception as e:
            logger.error(f"解析拆股CSV失败: {e}")
            return []

    def parse_all_event_csvs(self, csv_directory: Path) -> Dict[str, List]:
        """
        解析所有事件CSV文件
        
        Args:
            csv_directory: CSV文件目录
            
        Returns:
            包含所有事件记录的字典
        """
        results = {'rights_issue': [], 'placement': [], 'consolidation': [], 'stock_split': []}

        try:
            if not csv_directory.exists():
                logger.error(f"CSV目录不存在: {csv_directory}")
                return results

            # 查找CSV文件
            csv_files = {
                'rights_issue': csv_directory / '供股.csv', 
                'placement': csv_directory / '配股.csv',
                'consolidation': csv_directory / '合股.csv',
                'stock_split': csv_directory / '拆股.csv'
            }

            # 解析各类事件文件
            for event_type, csv_path in csv_files.items():
                if csv_path.exists():
                    if event_type == 'rights_issue':
                        results[event_type] = self.parse_rights_issue_csv(csv_path)
                    elif event_type == 'placement':
                        results[event_type] = self.parse_placement_csv(csv_path)
                    elif event_type == 'consolidation':
                        results[event_type] = self.parse_consolidation_csv(csv_path)
                    elif event_type == 'stock_split':
                        results[event_type] = self.parse_stock_split_csv(csv_path)
                else:
                    logger.warning(f"CSV文件不存在: {csv_path}")

            # 统计结果
            total_records = sum(len(records) for records in results.values())
            logger.info(f"CSV解析完成:")
            logger.info(f"  供股记录: {len(results['rights_issue'])}")
            logger.info(f"  配股记录: {len(results['placement'])}")
            logger.info(f"  合股记录: {len(results['consolidation'])}")
            logger.info(f"  拆股记录: {len(results['stock_split'])}")
            logger.info(f"  总记录数: {total_records}")

            return results

        except Exception as e:
            logger.error(f"解析CSV文件失败: {e}")
            return results
