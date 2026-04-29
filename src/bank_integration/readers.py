"""Readers for bank source files."""

import re

import pandas as pd

from .config import BANK_DATE_COL, BANK_READ_CONFIG


def read_bank_file(filepath: str, bank_name: str) -> pd.DataFrame:
    """Read a bank source file and return a cleaned DataFrame with string values."""
    cfg = BANK_READ_CONFIG[bank_name]
    header = cfg["header"]

    if cfg["is_csv"]:
        df = None
        for enc in ("utf-8-sig", "gbk", "utf-8"):
            try:
                df = pd.read_csv(filepath, header=header, dtype=str, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        if df is None:
            raise ValueError(f"无法解码 CSV 文件（尝试 utf-8-sig/gbk/utf-8）: {filepath}")
        df.columns = [c.split("[")[0].strip() for c in df.columns]
    else:
        df = pd.read_excel(
            filepath,
            header=header,
            dtype=str,
            engine=cfg["engine"],
        )

    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.fillna("")

    if bank_name == "农业银行":
        date_col = BANK_DATE_COL[bank_name]
        if date_col in df.columns:
            df = df[df[date_col].str.match(r"\d{4}-\d{2}-\d{2}")]

    return df

