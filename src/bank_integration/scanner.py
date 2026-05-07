"""Source file discovery."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Union

from .config import BANK_ABBR, SUMMARY_FILE
from .config2 import BANK_ABBR_2, SUMMARY_FILE_2


def scan_source_files(input_dir: Union[str, Path]) -> List[Dict]:
    """
    Scan for source files named {company_prefix}-{bank_name}.{xls|xlsx|csv}.

    Only the provided input directory is scanned; subdirectories such as raw/
    are intentionally ignored.
    """
    input_path = Path(input_dir)
    allowed_exts = {".xls", ".xlsx", ".csv"}
    results = []

    if not input_path.exists():
        return results

    for path in sorted(input_path.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        fname = path.name
        if fname.startswith("~$") or fname == SUMMARY_FILE:
            continue
        if path.suffix.lower() not in allowed_exts:
            continue

        company = None
        bank_name = None
        stem = path.stem
        for candidate in sorted(BANK_ABBR, key=len, reverse=True):
            suffix = f"-{candidate}"
            if stem.endswith(suffix):
                prefix = stem[: -len(suffix)]
                if prefix:
                    company = prefix
                    bank_name = candidate
                break

        if company is None or bank_name is None:
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
    allowed_exts = {".xls", ".xlsx", ".csv", ".pdf"}
    currency_pattern = re.compile(r"^(.+)-([A-Z]{2,4})$")
    results = []

    if not input_path.exists():
        return results

    for path in sorted(input_path.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        fname = path.name
        if fname.startswith("~$") or fname == SUMMARY_FILE_2:
            continue
        ext = path.suffix.lower().lstrip(".")
        if f".{ext}" not in allowed_exts:
            continue

        m = currency_pattern.match(path.stem)
        if not m:
            continue

        company = None
        bank_name = None
        name_without_currency = m.group(1)
        currency = m.group(2)
        for candidate in sorted(BANK_ABBR_2, key=len, reverse=True):
            suffix = f"-{candidate}"
            if name_without_currency.endswith(suffix):
                prefix = name_without_currency[: -len(suffix)]
                if prefix:
                    company = prefix
                    bank_name = candidate
                break

        if company is None or bank_name is None:
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
