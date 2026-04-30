# -*- coding: utf-8 -*-
"""Daily balance extraction for text-based bank statement PDFs."""

import re
import warnings
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def parse_statement_end(text: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.search(r"ENDING DATE:\s+([A-Za-z]+)\s+\d{1,2},\s+(\d{4})", text, re.I)
    if not m:
        return None, None
    return int(m.group(2)), MONTHS.get(m.group(1).lower())


def parse_amount(value: str) -> float:
    text = value.strip().replace("$", "").replace(",", "")
    is_negative = text.startswith("-") or (text.startswith("(") and text.endswith(")"))
    text = text.strip("-()")
    amount = float(text)
    return -amount if is_negative else amount


def infer_balance_date(mm_dd: str, end_year: Optional[int], end_month: Optional[int]) -> str:
    month, day = [int(part) for part in mm_dd.split("-")]
    if end_year is None:
        return mm_dd
    year = end_year - 1 if end_month is not None and month > end_month else end_year
    return f"{year:04d}-{month:02d}-{day:02d}"


def extract_daily_balance_rows(text: str, statement_month_only: bool = False) -> List[Tuple[str, float]]:
    """Extract DAILY BALANCES as (YYYY-MM-DD, amount) rows."""
    if "DAILY BALANCES" not in text:
        return []

    end_year, end_month = parse_statement_end(text)
    lines = [line.strip() for line in text.splitlines()]
    rows: List[Tuple[str, float]] = []
    in_section = False

    for line in lines:
        upper = line.upper()
        if upper == "DAILY BALANCES":
            in_section = True
            continue
        if not in_section:
            continue
        if not line or upper.startswith("DATE AMOUNT"):
            continue
        if upper in {"OVERDRAFT/RETURN ITEM FEES"} or upper.startswith("OVERDRAFT/"):
            break

        pairs = re.findall(r"(\d{2}-\d{2})\s+(\$?\(?-?[\d,]+\.\d{2}\)?)", line)
        for mm_dd, amount in pairs:
            date_str = infer_balance_date(mm_dd, end_year, end_month)
            if statement_month_only and end_year is not None and end_month is not None:
                if not date_str.startswith(f"{end_year:04d}-{end_month:02d}-"):
                    continue
            rows.append((date_str, parse_amount(amount)))

    return rows


def extract_statement_month_last_daily_balance(text: str) -> List[Tuple[str, float]]:
    rows = extract_daily_balance_rows(text, statement_month_only=True)
    return rows[-1:] if rows else []


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Python 3.8 is no longer supported.*")
            import pdfplumber
    except ImportError as exc:
        raise RuntimeError(
            "缺少依赖 pdfplumber。请先运行: venv\\Scripts\\pip.exe install -r requirements.txt"
        ) from exc

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        warnings.filterwarnings("ignore", message="Python 3.8 is no longer supported.*")
        pdf = pdfplumber.open(pdf_path)
    with pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def read_huamei_daily_balance_pdf(filepath: str) -> pd.DataFrame:
    rows = extract_statement_month_last_daily_balance(extract_pdf_text(Path(filepath)))
    return pd.DataFrame(rows, columns=["Date", "Amount"])
