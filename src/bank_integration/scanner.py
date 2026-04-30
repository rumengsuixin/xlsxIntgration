"""Source file discovery."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Union

from .config import BANK_ABBR, SUMMARY_FILE
from .config2 import BANK_ABBR_2, SUMMARY_FILE_2


def scan_source_files(input_dir: Union[str, Path]) -> List[Dict]:
    """
    Scan for source files named {company}-{bank_name}.{xls|xlsx|csv}.

    Only the provided input directory is scanned; subdirectories such as raw/
    are intentionally ignored.
    """
    input_path = Path(input_dir)
    pattern = re.compile(r"^([A-Z])-(.+)\.(xls|xlsx|csv)$")
    results = []

    if not input_path.exists():
        return results

    for path in sorted(input_path.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        fname = path.name
        if fname.startswith("~$") or fname == SUMMARY_FILE:
            continue
        m = pattern.match(fname)
        if not m:
            continue
        company = m.group(1)
        bank_name = m.group(2)
        if bank_name not in BANK_ABBR:
            logging.warning(f"未知银行名称 '{bank_name}'，跳过文件: {fname}")
            continue
        results.append(
            {
                "company": company,
                "bank_name": bank_name,
                "filepath": str(path),
            }
        )

    return results


def scan_source_files_2(input_dir: Union[str, Path]) -> List[Dict]:
    """
    扫描代号2源文件，命名格式：{公司}-{银行}-{币种}.{xls|xlsx|csv|pdf}

    只扫描指定目录，不递归子目录。
    返回列表每项含 company、bank_name、currency、filepath。
    """
    input_path = Path(input_dir)
    # 贪婪匹配银行名（允许含括号、空格），货币为末尾2-4个大写字母
    pattern = re.compile(r"^([A-Z])-(.+)-([A-Z]{2,4})\.(?i:xls|xlsx|csv|pdf)$")
    results = []

    if not input_path.exists():
        return results

    for path in sorted(input_path.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        fname = path.name
        if fname.startswith("~$") or fname == SUMMARY_FILE_2:
            continue
        m = pattern.match(fname)
        if not m:
            continue
        company = m.group(1)
        bank_name = m.group(2)
        currency = m.group(3)
        ext = path.suffix.lower().lstrip(".")
        if bank_name not in BANK_ABBR_2:
            logging.warning(f"未知银行名称 '{bank_name}'，跳过文件: {fname}")
            continue
        if ext == "pdf" and bank_name != "华美银行":
            logging.warning(f"仅华美银行支持 PDF 源文件，跳过文件: {fname}")
            continue
        results.append(
            {
                "company": company,
                "bank_name": bank_name,
                "currency": currency,
                "filepath": str(path),
            }
        )

    return results
