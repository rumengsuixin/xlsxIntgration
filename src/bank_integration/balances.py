"""Balance extraction and balance-sheet row helpers."""

import calendar
import logging
import re
from datetime import date, datetime
from typing import List, Optional, Tuple

from openpyxl.utils import get_column_letter
from openpyxl.utils.datetime import from_excel as excel_to_date

from .config import BALANCE_BANK_ORDER, BANK_BALANCE_COL, BANK_DATE_COL


def get_last_balance(df, bank_name: str):
    """
    Find the latest valid balance by transaction date.

    If multiple rows share the same date, keep the later row in source order.
    Returns (date_str "YYYY-MM-DD", balance float), or (None, None).
    """
    balance_col = BANK_BALANCE_COL.get(bank_name, "")
    date_col = BANK_DATE_COL.get(bank_name, "")

    if balance_col not in df.columns:
        logging.warning(f"  余额列 '{balance_col}' 不存在，跳过余额更新")
        return None, None
    if date_col not in df.columns:
        logging.warning(f"  日期列 '{date_col}' 不存在，跳过余额更新")
        return None, None

    latest_date = None
    latest_balance = None

    for _, row in df.iterrows():
        bal_str = str(row.get(balance_col, "")).strip().replace(",", "").replace("+", "")
        if not bal_str:
            continue
        try:
            balance = float(bal_str)
        except ValueError:
            continue
        if balance == 0.0:
            continue

        date_raw = str(row.get(date_col, "")).strip()
        m = re.search(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", date_raw)
        if m:
            try:
                row_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
            if latest_date is None or row_date >= latest_date:
                latest_date = row_date
                latest_balance = balance

    if latest_date is not None:
        return latest_date.isoformat(), latest_balance

    return None, None


def parse_cell_date(val) -> Optional[date]:
    """Parse an openpyxl cell value as a Python date."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, (int, float)) and val > 0:
        try:
            return excel_to_date(int(val)).date()
        except Exception:
            pass
    if isinstance(val, str):
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", val.strip())
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def detect_balance_sheet_year(ws) -> Optional[int]:
    """Scan column A and return the largest year found in date cells."""
    max_year = None
    for row_idx in range(1, ws.max_row + 1):
        d = parse_cell_date(ws.cell(row=row_idx, column=1).value)
        if d is not None and (max_year is None or d.year > max_year):
            max_year = d.year
    return max_year


def collect_balance_month_blocks(ws) -> List[Tuple[int, date]]:
    """Return all balance month blocks as (start_row, month_end)."""
    blocks = []
    for row_idx in range(1, ws.max_row + 1):
        d = parse_cell_date(ws.cell(row=row_idx, column=1).value)
        if d is not None:
            blocks.append((row_idx, d))
    return blocks


def build_target_dates(current_year: int) -> List[date]:
    """Build target month-end dates: previous December plus current January-December."""
    targets = [date(current_year - 1, 12, 31)]
    for month in range(1, 13):
        last_day = calendar.monthrange(current_year, month)[1]
        targets.append(date(current_year, month, last_day))
    return targets


def refresh_balance_sheet_dates(ws, current_year: int) -> None:
    """Refresh balance-sheet dates for the current year and clear company balances."""
    target_dates = build_target_dates(current_year)
    existing_blocks = collect_balance_month_blocks(ws)
    block_size = len(BALANCE_BANK_ORDER)

    for i, target_date in enumerate(target_dates):
        if i < len(existing_blocks):
            block_start, _ = existing_blocks[i]
            ws.cell(row=block_start, column=1).value = target_date
            for row in range(block_start, block_start + block_size + 1):
                for col in range(5, 27):
                    ws.cell(row=row, column=col).value = None
        else:
            append_balance_month_block(ws, target_date, start_row=ws.max_row + 2)

    logging.info(f"银行余额表日期已更新至 {current_year} 年度（上一年12月末+当前年1-12月末）")


def append_balance_month_block(ws, month_end: date, start_row: Optional[int] = None) -> int:
    """Append a month-end balance block and return its start row."""
    if start_row is None:
        start_row = ws.max_row + 1
    company_codes = [chr(c) for c in range(ord("A"), ord("V") + 1)]
    first_company_col = 5
    last_company_col = 4 + len(company_codes)

    for i, bk in enumerate(BALANCE_BANK_ORDER):
        row = start_row + i
        if i == 0:
            ws.cell(row=row, column=1).value = month_end
            ws.cell(row=row, column=2).value = "银行存款"
        ws.cell(row=row, column=3).value = bk
        ws.cell(row=row, column=4).value = (
            f"=SUM({get_column_letter(first_company_col)}{row}:"
            f"{get_column_letter(last_company_col)}{row})"
        )

    summary_row = start_row + len(BALANCE_BANK_ORDER)
    ws.cell(row=summary_row, column=2).value = "货币资金合计"
    bank_start = start_row
    bank_end = start_row + len(BALANCE_BANK_ORDER) - 1
    for col_idx in range(4, last_company_col + 1):
        col_letter = get_column_letter(col_idx)
        ws.cell(row=summary_row, column=col_idx).value = (
            f"=SUM({col_letter}{bank_start}:{col_letter}{bank_end})"
        )

    return start_row


def find_or_create_date_block(ws, year: int, month: int) -> int:
    """Find a target month block, or append one if missing."""
    for row_idx in range(1, ws.max_row + 1):
        a_val = ws.cell(row=row_idx, column=1).value
        if a_val is not None:
            cell_date = parse_cell_date(a_val)
            if cell_date is not None and cell_date.year == year and cell_date.month == month:
                return row_idx

    start_row = ws.max_row + 2
    last_day = calendar.monthrange(year, month)[1]
    month_end = date(year, month, last_day)
    append_balance_month_block(ws, month_end, start_row=start_row)
    summary_row = start_row + len(BALANCE_BANK_ORDER)

    logging.info(f"  银行余额表新增 {year}年{month}月 数据块（行 {start_row}-{summary_row}）")
    return start_row


def update_balance_sheet(ws, bank_name: str, company_code: str, date_str: str, balance: float) -> None:
    """Update a company balance cell in the bank balance sheet."""
    m = re.match(r"(\d{4})-(\d{2})", date_str)
    if not m:
        logging.warning(f"  日期格式异常 '{date_str}'，跳过余额更新")
        return

    year, month = int(m.group(1)), int(m.group(2))
    col_idx = ord(company_code) - ord("A") + 5
    block_start = find_or_create_date_block(ws, year, month)

    target_row = None
    for row_idx in range(block_start, block_start + len(BALANCE_BANK_ORDER)):
        c_val = str(ws.cell(row=row_idx, column=3).value or "").strip()
        if c_val == bank_name:
            target_row = row_idx
            break

    if target_row is None:
        logging.warning(f"  银行余额块中未找到银行行 [{bank_name}]，跳过")
        return

    ws.cell(row=target_row, column=col_idx).value = balance
    logging.info(f"  银行余额已更新: {year}年{month}月 / {bank_name} / 公司{company_code} = {balance}")

