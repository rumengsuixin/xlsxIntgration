"""Readers for bank source files."""

import re
from typing import Dict, Optional

import pandas as pd

from .config import BANK_DATE_COL, BANK_READ_CONFIG


def read_bank_file(
    filepath: str,
    bank_name: str,
    bank_read_config: Optional[Dict] = None,
    bank_date_col: Optional[Dict] = None,
) -> pd.DataFrame:
    """
    读取银行流水文件，返回清洗后的 DataFrame（所有列保留为字符串）。

    bank_read_config / bank_date_col 默认使用代号1配置；
    传入代号2配置字典时，自动应用额外的读取选项。

    额外选项（代号2用，写在 bank_read_config[bank_name] 中）：
      encoding         强制编码（覆盖默认的三次编码尝试）
      col_map          读取后重命名列 {旧名: 新名}
      strip_col_suffix_char  列名含该字符时截断至该字符之前（去空白）
      row_filter_col   只保留该列以 row_filter_prefix 开头的行
      row_filter_prefix 行过滤前缀
      row_filter_val   只保留该列去空白后等于此值的行
    """
    cfg = (bank_read_config or BANK_READ_CONFIG)[bank_name]
    date_col_map = bank_date_col or BANK_DATE_COL
    header = cfg["header"]

    if cfg["is_csv"]:
        df = _read_csv(filepath, header, cfg)
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

    # --- 代号2额外处理 ---
    suffix_char = cfg.get("strip_col_suffix_char")
    if suffix_char:
        df.columns = [
            c.split(suffix_char)[0].strip() if suffix_char in c else c
            for c in df.columns
        ]

    col_map = cfg.get("col_map")
    if col_map:
        df = df.rename(columns=col_map)

    row_filter_col = cfg.get("row_filter_col")
    if row_filter_col and row_filter_col in df.columns:
        prefix = cfg.get("row_filter_prefix")
        val = cfg.get("row_filter_val")
        if prefix is not None:
            df = df[df[row_filter_col].str.startswith(prefix, na=False)]
        elif val is not None:
            df = df[df[row_filter_col].str.strip() == val]

    # --- 代号1特殊处理：农业银行行过滤 ---
    if bank_read_config is None and bank_name == "农业银行":
        date_col = date_col_map.get(bank_name, "")
        if date_col in df.columns:
            df = df[df[date_col].str.match(r"\d{4}-\d{2}-\d{2}")]

    return df


def _read_csv(filepath: str, header, cfg: Dict) -> pd.DataFrame:
    forced_enc = cfg.get("encoding")
    encodings = [forced_enc] if forced_enc else ("utf-8-sig", "gbk", "utf-8")
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, header=header, dtype=str, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        raise ValueError(f"无法解码 CSV 文件（尝试编码: {encodings}）: {filepath}")
    # 去除列名中 [英文] 后缀（中国银行格式，其他银行无影响）
    df.columns = [c.split("[")[0].strip() for c in df.columns]
    return df
