"""Workbook copy preparation and summary writing."""

import logging
import shutil
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Union

import openpyxl

from .balances import detect_balance_sheet_year, refresh_balance_sheet_dates, update_balance_sheet
from .config import BALANCE_SHEET, OUTPUT_PATH, PROTECTED_SHEETS, SUMMARY_FILE, TEMPLATE_PATH


def prepare_work_copy(output_dir: Optional[Union[str, Path]] = None) -> Optional[str]:
    """
    Prepare data/output/国内银行汇总.xlsx from the template when needed.

    If the existing output workbook already matches the current year, it is reused.
    """
    output_path = Path(output_dir) / SUMMARY_FILE if output_dir is not None else OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    current_year = date.today().year

    def copy_from_template() -> bool:
        if not TEMPLATE_PATH.exists():
            logging.error(f"缺少模板文件: {TEMPLATE_PATH}。请先将 {SUMMARY_FILE} 放入 template/ 子目录。")
            return False
        try:
            shutil.copy2(TEMPLATE_PATH, output_path)
            logging.info(f"已从模板复制工作副本: {output_path}")
        except PermissionError:
            logging.error(f"无法覆盖工作副本，请先关闭 Excel 中的 {SUMMARY_FILE}")
            return False
        return True

    def check_year(path: Path) -> Optional[int]:
        try:
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        except PermissionError:
            logging.error(f"无法读取 {SUMMARY_FILE}，请先关闭 Excel 中的该文件")
            return -1
        try:
            if BALANCE_SHEET in wb.sheetnames:
                return detect_balance_sheet_year(wb[BALANCE_SHEET])
            return None
        finally:
            wb.close()

    needs_copy = False
    if not output_path.exists():
        needs_copy = True
    else:
        year = check_year(output_path)
        if year == -1:
            return None
        if year != current_year:
            logging.info(f"工作副本年份为 {year}，当前年份为 {current_year}，重新从模板复制")
            needs_copy = True

    if needs_copy:
        if not copy_from_template():
            return None
        try:
            wb = openpyxl.load_workbook(output_path)
        except PermissionError:
            logging.error(f"无法打开刚复制的工作副本 {SUMMARY_FILE}")
            return None
        if BALANCE_SHEET in wb.sheetnames:
            year = detect_balance_sheet_year(wb[BALANCE_SHEET])
            if year != current_year:
                refresh_balance_sheet_dates(wb[BALANCE_SHEET], current_year)
                wb.save(output_path)
                logging.info(f"工作副本日期已更新至 {current_year} 年度")
        wb.close()

    return str(output_path)


def write_all_to_summary(results: List[Dict], summary_path: str) -> None:
    """Write all detail sheets and balance updates to the summary workbook."""
    try:
        wb = openpyxl.load_workbook(summary_path)
    except PermissionError:
        logging.error(f"无法打开汇总文件，请先关闭 Excel 中的 {SUMMARY_FILE}")
        raise

    for item in results:
        sheet_name = item["sheet_name"]
        df = item["df"]

        if sheet_name in PROTECTED_SHEETS:
            logging.warning(f"  [{sheet_name}] 是受保护表，跳过写入")
            continue

        if sheet_name in wb.sheetnames:
            del wb[sheet_name]
        ws = wb.create_sheet(sheet_name)

        ws.append(list(df.columns))
        for row_tuple in df.itertuples(index=False, name=None):
            ws.append([v if v != "" else None for v in row_tuple])

        logging.info(f"  [{sheet_name}] 写入 {len(df)} 行数据")

    if BALANCE_SHEET in wb.sheetnames:
        ws_bal = wb[BALANCE_SHEET]
        for item in results:
            if item.get("balance") is not None and item.get("balance_date"):
                update_balance_sheet(
                    ws_bal,
                    item["bank_name"],
                    item["company_code"],
                    item["balance_date"],
                    item["balance"],
                )
    else:
        logging.warning(f"汇总文件中未找到 '{BALANCE_SHEET}' 工作表，跳过余额更新")

    try:
        wb.save(summary_path)
        logging.info(f"汇总文件保存完成: {summary_path}")
    except PermissionError:
        logging.error(f"无法保存汇总文件，请先关闭 Excel 中的 {SUMMARY_FILE}")
        raise
