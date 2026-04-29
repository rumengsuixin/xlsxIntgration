"""Source file discovery."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Union

from .config import BANK_ABBR, SUMMARY_FILE


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
