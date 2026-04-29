"""代号2（海外银行）应用入口。"""

import logging
import os
from collections import Counter

from .balances import get_monthly_balances
from .config2 import (
    BANK_ABBR_2,
    BANK_BALANCE_COL_2,
    BANK_DATE_COL_2,
    BANK_READ_CONFIG_2,
    INPUT_DIR_2,
    OUTPUT_PATH_2,
    TEMPLATE_PATH_2,
)
from .readers import read_bank_file
from .scanner import scan_source_files_2
from .workbook import align_workbook_year_2, prepare_work_copy, write_all_to_summary_2


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    summary_path = prepare_work_copy(
        template_path=TEMPLATE_PATH_2,
        output_path=OUTPUT_PATH_2,
        summary_file=OUTPUT_PATH_2.name,
    )
    if summary_path is None:
        return 1

    sources = scan_source_files_2(INPUT_DIR_2)
    if not sources:
        logging.warning(
            "未找到可处理的海外银行流水文件。请把文件放入 data/input/2，并按规则命名，"
            "例如：A-东亚银行-HKD.csv、B-大华银行（UOB)-SGD.xlsx。"
        )
        logging.warning(f"当前检查的文件夹: {INPUT_DIR_2}")
        return 1

    logging.info(f"共找到 {len(sources)} 个源文件待处理")
    results = []
    failures = []

    for item in sources:
        company = item["company"]
        bank_name = item["bank_name"]
        currency = item["currency"]
        filepath = item["filepath"]
        sheet_name = f"{company}-{BANK_ABBR_2[bank_name]}-{currency}"

        logging.info(f"处理: {os.path.basename(filepath)} → [{sheet_name}]")
        try:
            df = read_bank_file(
                filepath,
                bank_name,
                bank_read_config=BANK_READ_CONFIG_2,
                bank_date_col=BANK_DATE_COL_2,
            )

            if df.empty:
                logging.warning(f"  [{sheet_name}] 无有效数据行，仅写入列头")
            else:
                logging.info(f"  [{sheet_name}] 共 {len(df)} 条记录")

            monthly_balances = get_monthly_balances(
                df,
                bank_name,
                balance_col_map=BANK_BALANCE_COL_2,
                date_col_map=BANK_DATE_COL_2,
            )
            if monthly_balances:
                for date_str, bal in monthly_balances:
                    logging.info(f"  期末余额: {bal}（日期: {date_str}）")
            else:
                logging.info("  未提取到期末余额，跳过余额更新")

            results.append(
                {
                    "sheet_name": sheet_name,
                    "df": df,
                    "bank_name": bank_name,
                    "company_code": company,
                    "currency": currency,
                    "monthly_balances": monthly_balances,
                }
            )
        except Exception as exc:
            logging.error(f"  处理失败: {exc}", exc_info=True)
            failures.append(os.path.basename(filepath))
            continue

    if not results:
        logging.warning("没有可写入的数据，请检查源文件格式和命名是否正确。")
        if failures:
            logging.warning("处理失败的文件: " + "、".join(failures))
        return 1

    years = [
        int(date_str[:4])
        for item in results
        for date_str, _ in item.get("monthly_balances", [])
    ]
    if years:
        source_year = Counter(years).most_common(1)[0][0]
        align_workbook_year_2(summary_path, source_year)

    try:
        write_all_to_summary_2(results, summary_path)
    except Exception:
        return 1

    if failures:
        logging.warning("部分文件处理失败，已跳过: " + "、".join(failures))
        logging.warning(f"已成功生成结果文件: {summary_path}")
        return 1

    logging.info("全部处理完成")
    logging.info(f"结果文件: {summary_path}")
    return 0
