"""Application entry point."""

import logging
import os

from .balances import get_last_balance
from .config import BANK_ABBR, INPUT_DIR, OUTPUT_DIR
from .readers import read_bank_file
from .scanner import scan_source_files
from .workbook import prepare_work_copy, write_all_to_summary


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    summary_path = prepare_work_copy(OUTPUT_DIR)
    if summary_path is None:
        return

    sources = scan_source_files(INPUT_DIR)
    if not sources:
        logging.warning(
            "未找到符合命名规范的源文件。"
            "请将文件重命名为 {公司代号}-{银行全称}.{扩展名}，例如：B-建设银行.xls，"
            f"并放入 {INPUT_DIR}"
        )
        return

    logging.info(f"共找到 {len(sources)} 个源文件待处理")
    results = []

    for item in sources:
        company = item["company"]
        bank_name = item["bank_name"]
        filepath = item["filepath"]
        sheet_name = f"{company}-{BANK_ABBR[bank_name]}"

        logging.info(f"处理: {os.path.basename(filepath)} → [{sheet_name}]")
        try:
            df = read_bank_file(filepath, bank_name)

            if df.empty:
                logging.warning(f"  [{sheet_name}] 无有效数据行，仅写入列头")
            else:
                logging.info(f"  [{sheet_name}] 共 {len(df)} 条记录")

            balance_date, balance = get_last_balance(df, bank_name)
            if balance is not None:
                logging.info(f"  期末余额: {balance}（日期: {balance_date}）")
            else:
                logging.info("  未提取到期末余额，跳过余额更新")

            results.append(
                {
                    "sheet_name": sheet_name,
                    "df": df,
                    "bank_name": bank_name,
                    "company_code": company,
                    "balance": balance,
                    "balance_date": balance_date,
                }
            )
        except Exception as exc:
            logging.error(f"  处理失败: {exc}", exc_info=True)
            continue

    if not results:
        logging.warning("没有可写入的数据，退出")
        return

    write_all_to_summary(results, summary_path)
    logging.info("全部处理完成")

