"""代号5（代付订单对账）应用逻辑。

数据流：
    data/input/raw/5/  →  scan_source_files_5()
                               │
        read_admin_5 / read_ibfpay_5 / read_superpay_5 / read_wangguypay_5
                               │
    build_ibfpay_lookup_5 / build_superpay_lookup_5 / build_wangguypay_lookup_5
                               │
                      enrich_admin_5()   ← left-join 三个查找表
                               │
          data/output/代付对账结果_{YYYYMMDD}.xlsx

输出新增列（追加在 admin 原始列末尾，共 7 列）：
    是否匹配      - 是 / 否 / 平台多余
    平台流水号    - 各平台主键
    平台代付金额  - 平台记录的代付金额
    平台状态      - 平台记录的交易状态
    手续费        - 平台收取的手续费
    到账金额      - 扣除手续费后实际到账
    交易日期      - 格式化后的交易时间
"""

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional, Set

import pandas as pd

from .config import OUTPUT_DIR
from .config5 import (
    INPUT_DIR_5,
    OUTPUT_FILE_TEMPLATE_5,
    OUTPUT_SHEET_5,
    OUTPUT_FAILED_SHEET_5,
    OUTPUT_SUMMARY_SHEET_5,
    ADMIN_SHEET_5,
    ADMIN_JOIN_COL_5,
    ADMIN_DATE_COL_5,
    ADMIN_AMOUNT_COL_5,
    ADMIN_STATUS_COL_5,
    ADMIN_ORG_COL_5,
    IBFYPAY_SHEET_5,
    IBFYPAY_HEADER_5,
    IBFYPAY_JOIN_COL_5,
    IBFYPAY_ADMIN_JOIN_COL_5,
    IBFYPAY_TYPE_COL_5,
    IBFYPAY_TYPE_PAYOUT_5,
    IBFYPAY_TYPE_FEE_5,
    IBFYPAY_AMOUNT_COL_5,
    IBFYPAY_TIME_COL_5,
    IBFYPAY_ACCOUNT_COL_5,
    IBFYPAY_REMARK_COL_5,
    SUPERPAY_SHEET_5,
    SUPERPAY_HEADER_5,
    SUPERPAY_JOIN_COL_5,
    SUPERPAY_PLATFORM_NO_COL_5,
    SUPERPAY_AMOUNT_COL_5,
    SUPERPAY_FEE_TOTAL_COL_5,
    SUPERPAY_ACTUAL_COL_5,
    SUPERPAY_STATUS_COL_5,
    SUPERPAY_CREATE_TIME_COL_5,
    SUPERPAY_FINISH_TIME_COL_5,
    WANGGUYPAY_SHEET_5,
    WANGGUYPAY_HEADER_5,
    WANGGUYPAY_JOIN_COL_5,
    WANGGUYPAY_PLATFORM_NO_COL_5,
    WANGGUYPAY_AMOUNT_COL_5,
    WANGGUYPAY_FEE_COL_5,
    WANGGUYPAY_ARRIVE_COL_5,
    WANGGUYPAY_STATUS_COL_5,
    WANGGUYPAY_CREATE_TIME_COL_5,
    WANGGUYPAY_FINISH_TIME_COL_5,
    MATCH_STATUS_COL_5,
    PLATFORM_ORDER_NO_COL_5,
    PLATFORM_AMOUNT_COL_5,
    PLATFORM_STATUS_COL_5,
    FEE_COL_5,
    ARRIVE_AMOUNT_COL_5,
    TRANSACTION_DATE_COL_5,
    OUTPUT_NEW_COLS_5,
    PLATFORM_PREFIXES_5,
)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _normalize_columns_5(columns) -> Set[str]:
    """去除列名首尾空格，返回规范化集合。"""
    return {str(col).strip() for col in columns}


def _format_date_5(val) -> str:
    """将任意日期值格式化为 YYYY-MM-DD，失败返回空串。"""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    try:
        s = str(val).strip()
        if not s or s.lower() in ("nan", "none", "nat"):
            return ""
        parsed = pd.to_datetime(s, errors="coerce")
        if not pd.isna(parsed):
            return parsed.strftime("%Y-%m-%d")
        if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
            return s[:10]
        return ""
    except Exception:
        return ""


def _to_float_5(val) -> Optional[float]:
    """将字符串金额转为 float，支持千分位逗号，失败返回 None。"""
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _dedup_lookup_5(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    """过滤空 key 行，对 key_col 去重（保留首行），发现重复时打 warning。"""
    df = df[df[key_col].astype(str).str.strip() != ""].copy()
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    dupes = before - len(df)
    if dupes > 0:
        logging.warning("【%s】去重时发现 %d 条重复流水号，已保留首行", label, dupes)
    return df


# ---------------------------------------------------------------------------
# 文件扫描
# ---------------------------------------------------------------------------

def scan_source_files_5(input_dir: Path) -> dict:
    """扫描输入目录，按平台识别文件，返回 {"admin": [...], "ibfpay": [...], ...}。

    识别规则（文件名 stem 小写前缀匹配，来自 PLATFORM_PREFIXES_5）：
        admin-          → admin 后台主表（XLS，engine=xlrd）
        ibfpay-/ibf平台 → IBFYPAY 平台（XLSX）
        superpay-       → SUPERPAY 平台（XLSX）
        wangupay- 或 wangguypay- → WANGGUYPAY 平台（XLSX）

    跳过临时文件（"~$" 前缀）和非 xls/xlsx 格式。
    同平台多文件时全部收录，由调用方合并。
    """
    result: dict = {key: [] for key in PLATFORM_PREFIXES_5}

    if not input_dir.exists():
        logging.warning("输入目录不存在: %s", input_dir)
        return result

    for f in sorted(input_dir.iterdir()):
        if not f.is_file() or f.name.startswith("~$"):
            continue
        if f.suffix.lower() not in {".xls", ".xlsx"}:
            continue
        stem = f.stem.lower()
        matched = False
        for platform_key, prefixes in PLATFORM_PREFIXES_5.items():
            if any(stem.startswith(p) for p in prefixes):
                result[platform_key].append(f)
                matched = True
                break
        if not matched:
            logging.warning("未识别文件，已跳过: %s", f.name)

    for key, files in result.items():
        if len(files) > 1:
            logging.info("%s 平台发现 %d 个文件，将合并: %s", key, len(files), [f.name for f in files])

    return result


# ---------------------------------------------------------------------------
# 内部辅助：sheet 选择
# ---------------------------------------------------------------------------

def _select_sheet_by_columns_5(
    filepath: Path,
    *,
    default_sheet,
    header: int,
    required_columns: List[str],
    label: str,
) -> str:
    """返回首个列名满足 required_columns 的 sheet 名称。

    注意：preview 读取时必须传入正确的 header，否则 WANGGUYPAY（header=1）
    会把第1行无用标题当作列头，导致 required_columns 匹配失败。

    Args:
        filepath: Excel 文件路径。
        default_sheet: 优先尝试的 sheet 名或整数下标。
        header: 读取表头的行偏移（0-indexed），WANGGUYPAY 须传 1。
        required_columns: 必须存在的列名列表。
        label: 日志中使用的平台名称。

    Returns:
        可用的 sheet 名称字符串。

    Raises:
        ValueError: 无任何 sheet 含有 required_columns 时抛出。
    """
    with pd.ExcelFile(filepath) as xls:
        sheet_names = list(xls.sheet_names)
        if isinstance(default_sheet, int):
            default_name = sheet_names[default_sheet] if -len(sheet_names) <= default_sheet < len(sheet_names) else None
        else:
            default_name = default_sheet if default_sheet in sheet_names else None

        ordered_sheets = []
        if default_name is not None:
            ordered_sheets.append(default_name)
        ordered_sheets.extend(s for s in sheet_names if s != default_name)

        for sheet_name in ordered_sheets:
            try:
                preview = pd.read_excel(xls, sheet_name=sheet_name, header=header, nrows=0, dtype=str)
            except Exception:
                logging.warning("无法检查 %s 文件 sheet '%s' 的表头", label, sheet_name)
                continue
            cols = _normalize_columns_5(preview.columns)
            if all(c in cols for c in required_columns):
                if sheet_name != default_name:
                    logging.warning(
                        "【%s】默认 sheet '%s' 在 %s 中不可用；使用包含所需列 %s 的 sheet '%s'。可用 sheets: %s",
                        label, default_sheet, filepath.name, required_columns, sheet_name, sheet_names,
                    )
                return sheet_name

    raise ValueError(
        f"【{label}】{filepath.name} 中找不到含必要列 {required_columns!r} 的 sheet，可用 sheets: {sheet_names}"
    )


def _find_sheet_with_col_5(xls: pd.ExcelFile, col: str) -> str:
    """在 ExcelFile 的所有 sheet 中找到第一个含指定列的 sheet 名称。

    专用于 admin XLS 的 sheet 回退查找（engine=xlrd）。

    Raises:
        ValueError: 未找到时抛出，提示可用 sheet 列表。
    """
    for s in xls.sheet_names:
        try:
            preview = pd.read_excel(xls, sheet_name=s, nrows=0, engine="xlrd")
            if col in _normalize_columns_5(preview.columns):
                return s
        except Exception:
            continue
    raise ValueError(f"所有 sheet 均不含列 '{col}'，可用 sheet: {xls.sheet_names}")


# ---------------------------------------------------------------------------
# 平台文件读取
# ---------------------------------------------------------------------------

def read_admin_5(filepath: Path) -> pd.DataFrame:
    """读取 admin 主表（XLS，sheet="Simple"），全列字符串。

    关键点：
      - 文件为 .xls 格式，必须 engine="xlrd"，openpyxl 无法处理
      - 若 sheet "Simple" 不存在则通过 _find_sheet_with_col_5 回退查找
      - 返回 dropna(how="all").fillna("") 的 DataFrame

    Args:
        filepath: admin XLS 文件路径。

    Returns:
        包含 admin 全部原始列的 DataFrame。
    """
    with pd.ExcelFile(filepath, engine="xlrd") as xls:
        sheet_names = xls.sheet_names
        if ADMIN_SHEET_5 in sheet_names:
            target = ADMIN_SHEET_5
        else:
            logging.warning(
                "admin 文件未找到 sheet '%s'，自动查找含 '%s' 列的 sheet",
                ADMIN_SHEET_5, ADMIN_JOIN_COL_5,
            )
            target = _find_sheet_with_col_5(xls, ADMIN_JOIN_COL_5)
    df = pd.read_excel(filepath, sheet_name=target, dtype=str, engine="xlrd")
    return df.dropna(how="all").fillna("")


def read_ibfpay_5(filepath: Path) -> pd.DataFrame:
    """读取 IBFYPAY 资金流水账文件（XLSX，sheet="Sheet"，header=0），返回按订单合并的 DataFrame。

    文件格式：每笔代付对应两行，通过 IBFYPAY_JOIN_COL_5（系统流水号）匹配：
      - IBFYPAY_TYPE_PAYOUT_5（代付扣款）    → 代付金额（变动金额取绝对值）
      - IBFYPAY_TYPE_FEE_5（代付扣除手续费）  → 手续费（变动金额取绝对值）

    Args:
        filepath: IBFYPAY 资金流水账 XLSX 文件路径。

    Returns:
        每个系统流水号一行的 DataFrame，含：系统流水号、代付金额、手续费、变动时间。
    """
    df = pd.read_excel(filepath, sheet_name=IBFYPAY_SHEET_5, header=IBFYPAY_HEADER_5, dtype=str)
    df = df.dropna(how="all").fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    if IBFYPAY_JOIN_COL_5 not in df.columns:
        logging.warning("【IBFYPAY】%s 中缺少关联列 '%s'，跳过", filepath.name, IBFYPAY_JOIN_COL_5)
        return pd.DataFrame(columns=[IBFYPAY_JOIN_COL_5, "代付金额", "手续费", IBFYPAY_TIME_COL_5])

    if IBFYPAY_TYPE_COL_5 in df.columns:
        # 资金流水账格式：两行合并（代付扣款 + 代付扣除手续费）
        payout_df = df[df[IBFYPAY_TYPE_COL_5].str.strip() == IBFYPAY_TYPE_PAYOUT_5].copy()
        fee_df    = df[df[IBFYPAY_TYPE_COL_5].str.strip() == IBFYPAY_TYPE_FEE_5].copy()

        payout_df = payout_df[[IBFYPAY_JOIN_COL_5, IBFYPAY_AMOUNT_COL_5, IBFYPAY_TIME_COL_5]].rename(
            columns={IBFYPAY_AMOUNT_COL_5: "代付金额"}
        )
        fee_df = fee_df[[IBFYPAY_JOIN_COL_5, IBFYPAY_AMOUNT_COL_5]].rename(
            columns={IBFYPAY_AMOUNT_COL_5: "手续费"}
        )
        merged = payout_df.merge(fee_df, on=IBFYPAY_JOIN_COL_5, how="left")
    else:
        # 订单明细格式：一行一笔，无独立手续费行
        logging.info("【IBFYPAY】%s 为订单明细格式（无「%s」列），手续费置0", filepath.name, IBFYPAY_TYPE_COL_5)
        amount_col = next((c for c in ("金额", IBFYPAY_AMOUNT_COL_5) if c in df.columns), None)
        if not amount_col:
            logging.warning("【IBFYPAY】%s 中缺少金额列，跳过", filepath.name)
            return pd.DataFrame(columns=[IBFYPAY_JOIN_COL_5, "代付金额", "手续费", IBFYPAY_TIME_COL_5])
        time_col = next((c for c in (IBFYPAY_TIME_COL_5, "创建时间", "完成时间") if c in df.columns), None)
        keep = [IBFYPAY_JOIN_COL_5, amount_col] + ([time_col] if time_col else [])
        rename_map = {amount_col: "代付金额"}
        if time_col and time_col != IBFYPAY_TIME_COL_5:
            rename_map[time_col] = IBFYPAY_TIME_COL_5
        merged = df[keep].rename(columns=rename_map)
        merged["手续费"] = "0"
        if IBFYPAY_TIME_COL_5 not in merged.columns:
            merged[IBFYPAY_TIME_COL_5] = ""

    for col in ("代付金额", "手续费"):
        merged[col] = pd.to_numeric(
            merged[col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        ).fillna(0.0).abs()

    return merged


def read_superpay_5(filepath: Path) -> pd.DataFrame:
    """读取 SUPERPAY 平台文件（XLSX，sheet="sheet1"，header=0），全列字符串。

    Args:
        filepath: SUPERPAY XLSX 文件路径。

    Returns:
        包含 SUPERPAY 全部列的 DataFrame。
    """
    sheet = _select_sheet_by_columns_5(
        filepath,
        default_sheet=SUPERPAY_SHEET_5,
        header=SUPERPAY_HEADER_5,
        required_columns=[SUPERPAY_JOIN_COL_5, SUPERPAY_AMOUNT_COL_5, SUPERPAY_STATUS_COL_5],
        label="SUPERPAY",
    )
    df = pd.read_excel(filepath, sheet_name=sheet, header=SUPERPAY_HEADER_5, dtype=str)
    return df.dropna(how="all").fillna("")


def read_wangguypay_5(filepath: Path) -> pd.DataFrame:
    """读取 WANGGUYPAY 平台文件（XLSX，sheet="付款订单"，header=1），全列字符串。

    关键点：
      - 表头在第2行（header=1），第1行为无用大标题行
      - _select_sheet_by_columns_5 的 header 参数也必须传 1，否则预览到错误列头

    Args:
        filepath: WANGGUYPAY XLSX 文件路径。

    Returns:
        包含 WANGGUYPAY 全部列的 DataFrame（已跳过第1行冗余标题）。
    """
    sheet = _select_sheet_by_columns_5(
        filepath,
        default_sheet=WANGGUYPAY_SHEET_5,
        header=WANGGUYPAY_HEADER_5,
        required_columns=[WANGGUYPAY_JOIN_COL_5, WANGGUYPAY_AMOUNT_COL_5, WANGGUYPAY_STATUS_COL_5],
        label="WANGGUYPAY",
    )
    df = pd.read_excel(filepath, sheet_name=sheet, header=WANGGUYPAY_HEADER_5, dtype=str)
    return df.dropna(how="all").fillna("")


# ---------------------------------------------------------------------------
# 查找表构建
# ---------------------------------------------------------------------------

def build_ibfpay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 IBFYPAY_JOIN_COL_5（系统流水号）为索引的 IBFYPAY 查找表。

    输入为 read_ibfpay_5 返回的已合并 DataFrame（每个流水号一行）。
    查找表供 enrich_admin_5 通过 admin.IBFYPAY_ADMIN_JOIN_COL_5（第三方订单号）查询。

    Args:
        df: read_ibfpay_5 返回的合并后 DataFrame（系统流水号、代付金额、手续费、变动时间）。

    Returns:
        以 IBFYPAY_JOIN_COL_5（系统流水号）为索引的 lookup DataFrame。
    """
    keep_cols = [c for c in [
        IBFYPAY_JOIN_COL_5,
        "代付金额",
        "手续费",
        IBFYPAY_TIME_COL_5,
    ] if c in df.columns]
    result = _dedup_lookup_5(df[keep_cols], IBFYPAY_JOIN_COL_5, "IBFYPAY")
    return result.set_index(IBFYPAY_JOIN_COL_5)


def build_superpay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 SUPERPAY_JOIN_COL_5（商户订单号）为索引的 SUPERPAY 查找表。

    Args:
        df: read_superpay_5 返回的原始 DataFrame。

    Returns:
        以 SUPERPAY_JOIN_COL_5 为索引的 lookup DataFrame。
    """
    keep_cols = [c for c in [
        SUPERPAY_JOIN_COL_5,
        SUPERPAY_PLATFORM_NO_COL_5,
        SUPERPAY_AMOUNT_COL_5,
        SUPERPAY_FEE_TOTAL_COL_5,
        SUPERPAY_ACTUAL_COL_5,
        SUPERPAY_STATUS_COL_5,
        SUPERPAY_CREATE_TIME_COL_5,
        SUPERPAY_FINISH_TIME_COL_5,
    ] if c in df.columns]
    result = _dedup_lookup_5(df[keep_cols], SUPERPAY_JOIN_COL_5, "SUPERPAY")
    return result.set_index(SUPERPAY_JOIN_COL_5)


def build_wangguypay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 WANGGUYPAY_JOIN_COL_5（商户订单号）为索引的 WANGGUYPAY 查找表。

    Args:
        df: read_wangguypay_5 返回的原始 DataFrame。

    Returns:
        以 WANGGUYPAY_JOIN_COL_5 为索引的 lookup DataFrame。
    """
    keep_cols = [c for c in [
        WANGGUYPAY_JOIN_COL_5,
        WANGGUYPAY_PLATFORM_NO_COL_5,
        WANGGUYPAY_AMOUNT_COL_5,
        WANGGUYPAY_FEE_COL_5,
        WANGGUYPAY_ARRIVE_COL_5,
        WANGGUYPAY_STATUS_COL_5,
        WANGGUYPAY_CREATE_TIME_COL_5,
        WANGGUYPAY_FINISH_TIME_COL_5,
    ] if c in df.columns]
    result = _dedup_lookup_5(df[keep_cols], WANGGUYPAY_JOIN_COL_5, "WANGGUYPAY")
    return result.set_index(WANGGUYPAY_JOIN_COL_5)


# ---------------------------------------------------------------------------
# 主匹配逻辑
# ---------------------------------------------------------------------------

def _build_platform_only_rows_5(
    result_cols: list,
    admin_ibfpay_keys: Set[str],
    admin_order_keys: Set[str],
    ibfpay_lk: Optional[pd.DataFrame],
    superpay_lk: Optional[pd.DataFrame],
    wangguypay_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """构建平台有、admin 无的多余行（MATCH_STATUS_COL_5 = "平台多余"）。

    admin_ibfpay_keys: admin.第三方订单号 集合，与 ibfpay_lk.index（系统流水号）比对
    admin_order_keys:  admin.订单号 集合，与 superpay/wangguypay_lk.index（商户订单号）比对
    """
    extra = []

    # ── IBFYPAY 多余行 ────────────────────────────────────────
    if ibfpay_lk is not None:
        for key in ibfpay_lk.index:
            k = str(key).strip()
            if not k or k in admin_ibfpay_keys:
                continue
            row: dict = {c: "" for c in result_cols}
            row[MATCH_STATUS_COL_5]      = "平台多余"
            row[ADMIN_ORG_COL_5]         = "IBFYPAY"
            row[PLATFORM_ORDER_NO_COL_5] = k
            amt = str(ibfpay_lk.at[key, "代付金额"]).strip() if "代付金额" in ibfpay_lk.columns else ""
            fee = str(ibfpay_lk.at[key, "手续费"]).strip()   if "手续费"   in ibfpay_lk.columns else ""
            amt_f = _to_float_5(amt) or 0.0
            fee_f = _to_float_5(fee) or 0.0
            row[PLATFORM_AMOUNT_COL_5] = amt
            row[PLATFORM_STATUS_COL_5] = ""
            row[FEE_COL_5]             = fee
            row[ARRIVE_AMOUNT_COL_5]   = str(round(amt_f - fee_f, 2))
            if IBFYPAY_TIME_COL_5 in ibfpay_lk.columns:
                row[TRANSACTION_DATE_COL_5] = _format_date_5(ibfpay_lk.at[key, IBFYPAY_TIME_COL_5])
            extra.append(row)

    # ── SUPERPAY 多余行 ───────────────────────────────────────
    if superpay_lk is not None:
        for key in superpay_lk.index:
            k = str(key).strip()
            if not k or k in admin_order_keys:
                continue
            row = {c: "" for c in result_cols}
            row[ADMIN_JOIN_COL_5]        = k
            row[MATCH_STATUS_COL_5]      = "平台多余"
            row[ADMIN_ORG_COL_5]         = "SUPERPAY"
            sp_no     = str(superpay_lk.at[key, SUPERPAY_PLATFORM_NO_COL_5]).strip()  if SUPERPAY_PLATFORM_NO_COL_5  in superpay_lk.columns else ""
            sp_amt    = str(superpay_lk.at[key, SUPERPAY_AMOUNT_COL_5]).strip()       if SUPERPAY_AMOUNT_COL_5       in superpay_lk.columns else ""
            sp_fee    = str(superpay_lk.at[key, SUPERPAY_FEE_TOTAL_COL_5]).strip()    if SUPERPAY_FEE_TOTAL_COL_5    in superpay_lk.columns else ""
            sp_actual = str(superpay_lk.at[key, SUPERPAY_ACTUAL_COL_5]).strip()       if SUPERPAY_ACTUAL_COL_5       in superpay_lk.columns else ""
            sp_status = str(superpay_lk.at[key, SUPERPAY_STATUS_COL_5]).strip()       if SUPERPAY_STATUS_COL_5       in superpay_lk.columns else ""
            sp_time   = str(superpay_lk.at[key, SUPERPAY_FINISH_TIME_COL_5]).strip()  if SUPERPAY_FINISH_TIME_COL_5  in superpay_lk.columns else ""
            sp_amt_f    = _to_float_5(sp_amt) or 0.0
            sp_actual_f = _to_float_5(sp_actual) or 0.0
            sp_calc_fee = str(round(abs(sp_amt_f - sp_actual_f), 2)) if (sp_amt or sp_actual) else ""
            row[PLATFORM_ORDER_NO_COL_5] = sp_no
            row[PLATFORM_AMOUNT_COL_5]   = sp_amt
            row[PLATFORM_STATUS_COL_5]   = sp_status
            row[FEE_COL_5]               = sp_calc_fee
            row[ARRIVE_AMOUNT_COL_5]     = sp_actual
            row[TRANSACTION_DATE_COL_5]  = _format_date_5(sp_time)
            extra.append(row)

    # ── WANGGUYPAY 多余行 ─────────────────────────────────────
    if wangguypay_lk is not None:
        for key in wangguypay_lk.index:
            k = str(key).strip()
            if not k or k in admin_order_keys:
                continue
            row = {c: "" for c in result_cols}
            row[ADMIN_JOIN_COL_5]        = k
            row[MATCH_STATUS_COL_5]      = "平台多余"
            row[ADMIN_ORG_COL_5]         = "WANGGUYPAY"
            wg_no     = str(wangguypay_lk.at[key, WANGGUYPAY_PLATFORM_NO_COL_5]).strip() if WANGGUYPAY_PLATFORM_NO_COL_5  in wangguypay_lk.columns else ""
            wg_amt    = str(wangguypay_lk.at[key, WANGGUYPAY_AMOUNT_COL_5]).strip()      if WANGGUYPAY_AMOUNT_COL_5       in wangguypay_lk.columns else ""
            wg_fee    = str(wangguypay_lk.at[key, WANGGUYPAY_FEE_COL_5]).strip()         if WANGGUYPAY_FEE_COL_5          in wangguypay_lk.columns else ""
            wg_arrive = str(wangguypay_lk.at[key, WANGGUYPAY_ARRIVE_COL_5]).strip()      if WANGGUYPAY_ARRIVE_COL_5       in wangguypay_lk.columns else ""
            wg_status = str(wangguypay_lk.at[key, WANGGUYPAY_STATUS_COL_5]).strip()      if WANGGUYPAY_STATUS_COL_5       in wangguypay_lk.columns else ""
            wg_time   = str(wangguypay_lk.at[key, WANGGUYPAY_FINISH_TIME_COL_5]).strip() if WANGGUYPAY_FINISH_TIME_COL_5  in wangguypay_lk.columns else ""
            row[PLATFORM_ORDER_NO_COL_5] = wg_no
            row[PLATFORM_AMOUNT_COL_5]   = wg_amt
            row[PLATFORM_STATUS_COL_5]   = wg_status
            row[FEE_COL_5]               = wg_fee
            row[ARRIVE_AMOUNT_COL_5]     = wg_arrive
            row[TRANSACTION_DATE_COL_5]  = _format_date_5(wg_time)
            extra.append(row)

    if not extra:
        return pd.DataFrame(columns=result_cols)
    return pd.DataFrame(extra, columns=result_cols)


def enrich_admin_5(
    admin_df: pd.DataFrame,
    ibfpay_lk: Optional[pd.DataFrame],
    superpay_lk: Optional[pd.DataFrame],
    wangguypay_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """以 admin 为主表，通过订单号与三平台查找表 left-join，追加新增列。

    匹配逻辑（优先级：IBFYPAY > SUPERPAY > WANGGUYPAY）：
      - IBFYPAY：以 admin.IBFYPAY_ADMIN_JOIN_COL_5（第三方订单号）↔ ibfpay_lk.index（系统流水号）关联
      - SUPERPAY / WANGGUYPAY：以 admin.ADMIN_JOIN_COL_5（订单号）↔ 平台 lookup.index（商户订单号）关联
      - 任一平台命中 → 是否匹配=是，填充该平台的流水号/代付金额/手续费/到账金额/交易日期
      - 均未命中 → 是否匹配=否
      - 追加平台多余行（_build_platform_only_rows_5）

    注意：
      - 理论上一笔订单只在一个平台出现，命中多平台时打 warning
      - IBFYPAY 到账金额 = 代付金额 - 手续费（由此函数计算填入 ARRIVE_AMOUNT_COL_5）

    Args:
        admin_df: read_admin_5 返回的 admin 主表。
        ibfpay_lk: build_ibfpay_lookup_5 返回值，可为 None（文件不存在时）。
        superpay_lk: build_superpay_lookup_5 返回值，可为 None。
        wangguypay_lk: build_wangguypay_lookup_5 返回值，可为 None。

    Returns:
        admin 原始列 + OUTPUT_NEW_COLS_5 的完整 DataFrame。
    """
    result = admin_df.copy()
    expected_rows = len(admin_df)

    def _safe_merge(base, lookup, left_on, prefix, label):
        if left_on not in base.columns:
            logging.warning("【%s】admin 文件中缺少关联列 '%s'，跳过该平台匹配", label, left_on)
            return base
        merged = base.merge(
            lookup.add_prefix(prefix),
            left_on=left_on, right_index=True, how="left",
        )
        if len(merged) != expected_rows:
            raise ValueError(
                f"【{label}】merge 后行数从 {expected_rows} 变为 {len(merged)}，"
                f"请检查 build_{label.lower()}_lookup 去重逻辑"
            )
        for c in lookup.columns:
            merged[f"{prefix}{c}"] = merged[f"{prefix}{c}"].fillna("")
        return merged

    ibfpay_avail     = ibfpay_lk is not None    and not ibfpay_lk.empty
    superpay_avail   = superpay_lk is not None  and not superpay_lk.empty
    wangguypay_avail = wangguypay_lk is not None and not wangguypay_lk.empty

    if ibfpay_avail:
        result = _safe_merge(result, ibfpay_lk, IBFYPAY_ADMIN_JOIN_COL_5, "_i_", "IBFYPAY")
    if superpay_avail:
        result = _safe_merge(result, superpay_lk, ADMIN_JOIN_COL_5, "_s_", "SUPERPAY")
    if wangguypay_avail:
        result = _safe_merge(result, wangguypay_lk, ADMIN_JOIN_COL_5, "_w_", "WANGGUYPAY")

    match_status_list      = []
    platform_order_no_list = []
    platform_amount_list   = []
    platform_status_list   = []
    fee_list               = []
    arrive_amount_list     = []
    transaction_date_list  = []

    for _, row in result.iterrows():
        # IBFYPAY 命中判断（join 键：admin.第三方订单号 ↔ ibfpay.系统流水号）
        ibf_amt  = str(row.get("_i_代付金额",               "")).strip() if ibfpay_avail    else ""
        ibf_fee  = str(row.get("_i_手续费",                 "")).strip() if ibfpay_avail    else ""
        ibf_time = str(row.get(f"_i_{IBFYPAY_TIME_COL_5}",  "")).strip() if ibfpay_avail    else ""
        ibf_tp   = str(row.get(IBFYPAY_ADMIN_JOIN_COL_5,    "")).strip()
        ibf_hit  = ibf_amt != ""

        # SUPERPAY 命中判断（join 键：admin.订单号 ↔ superpay.商户订单号）
        sp_amt    = str(row.get(f"_s_{SUPERPAY_AMOUNT_COL_5}",     "")).strip() if superpay_avail else ""
        sp_no     = str(row.get(f"_s_{SUPERPAY_PLATFORM_NO_COL_5}","")).strip() if superpay_avail else ""
        sp_fee    = str(row.get(f"_s_{SUPERPAY_FEE_TOTAL_COL_5}",  "")).strip() if superpay_avail else ""
        sp_actual = str(row.get(f"_s_{SUPERPAY_ACTUAL_COL_5}",     "")).strip() if superpay_avail else ""
        sp_status = str(row.get(f"_s_{SUPERPAY_STATUS_COL_5}",     "")).strip() if superpay_avail else ""
        sp_time   = str(row.get(f"_s_{SUPERPAY_FINISH_TIME_COL_5}","")).strip() if superpay_avail else ""
        sp_ctime  = str(row.get(f"_s_{SUPERPAY_CREATE_TIME_COL_5}","")).strip() if superpay_avail else ""
        sp_hit    = sp_amt != ""

        # WANGGUYPAY 命中判断（join 键：admin.订单号 ↔ wangguypay.商户订单号）
        wg_amt    = str(row.get(f"_w_{WANGGUYPAY_AMOUNT_COL_5}",    "")).strip() if wangguypay_avail else ""
        wg_no     = str(row.get(f"_w_{WANGGUYPAY_PLATFORM_NO_COL_5}","")).strip() if wangguypay_avail else ""
        wg_fee    = str(row.get(f"_w_{WANGGUYPAY_FEE_COL_5}",        "")).strip() if wangguypay_avail else ""
        wg_arrive = str(row.get(f"_w_{WANGGUYPAY_ARRIVE_COL_5}",     "")).strip() if wangguypay_avail else ""
        wg_status = str(row.get(f"_w_{WANGGUYPAY_STATUS_COL_5}",     "")).strip() if wangguypay_avail else ""
        wg_time   = str(row.get(f"_w_{WANGGUYPAY_FINISH_TIME_COL_5}","")).strip() if wangguypay_avail else ""
        wg_ctime  = str(row.get(f"_w_{WANGGUYPAY_CREATE_TIME_COL_5}","")).strip() if wangguypay_avail else ""
        wg_hit    = wg_amt != ""

        if ibf_hit and (sp_hit or wg_hit):
            logging.warning(
                "订单 %s 同时命中多个平台，取 IBFYPAY",
                str(row.get(ADMIN_JOIN_COL_5, "")).strip(),
            )

        if ibf_hit:
            ibf_amt_f = _to_float_5(ibf_amt) or 0.0
            ibf_fee_f = _to_float_5(ibf_fee) or 0.0
            arrive    = round(ibf_amt_f - ibf_fee_f, 2)
            match_status_list.append("是")
            platform_order_no_list.append(ibf_tp)
            platform_amount_list.append(ibf_amt)
            platform_status_list.append("")
            fee_list.append(ibf_fee)
            arrive_amount_list.append(str(arrive))
            transaction_date_list.append(_format_date_5(ibf_time))
        elif sp_hit:
            sp_amt_f    = _to_float_5(sp_amt) or 0.0
            sp_actual_f = _to_float_5(sp_actual) or 0.0
            sp_calc_fee = str(round(abs(sp_amt_f - sp_actual_f), 2))
            match_status_list.append("是")
            platform_order_no_list.append(sp_no)
            platform_amount_list.append(sp_amt)
            platform_status_list.append(sp_status)
            fee_list.append(sp_calc_fee)
            arrive_amount_list.append(sp_actual)
            transaction_date_list.append(_format_date_5(sp_time) or _format_date_5(sp_ctime))
        elif wg_hit:
            match_status_list.append("是")
            platform_order_no_list.append(wg_no)
            platform_amount_list.append(wg_amt)
            platform_status_list.append(wg_status)
            fee_list.append(wg_fee)
            arrive_amount_list.append(wg_arrive)
            transaction_date_list.append(_format_date_5(wg_time) or _format_date_5(wg_ctime))
        else:
            match_status_list.append("否")
            platform_order_no_list.append("")
            platform_amount_list.append("")
            platform_status_list.append("")
            fee_list.append("")
            arrive_amount_list.append("")
            transaction_date_list.append("")

    # 还原为仅 admin 原始列，再追加 7 个新增列
    admin_cols = list(admin_df.columns)
    result = result[admin_cols].copy()

    result[MATCH_STATUS_COL_5]      = match_status_list
    result[PLATFORM_ORDER_NO_COL_5] = platform_order_no_list
    result[PLATFORM_AMOUNT_COL_5]   = platform_amount_list
    result[PLATFORM_STATUS_COL_5]   = platform_status_list
    result[FEE_COL_5]               = fee_list
    result[ARRIVE_AMOUNT_COL_5]     = arrive_amount_list
    result[TRANSACTION_DATE_COL_5]  = transaction_date_list

    # 计算 admin key 集合，用于判断平台多余行
    admin_ibfpay_keys: Set[str] = (
        {str(v).strip() for v in admin_df[IBFYPAY_ADMIN_JOIN_COL_5] if str(v).strip()}
        if IBFYPAY_ADMIN_JOIN_COL_5 in admin_df.columns else set()
    )
    admin_order_keys: Set[str] = (
        {str(v).strip() for v in admin_df[ADMIN_JOIN_COL_5] if str(v).strip()}
        if ADMIN_JOIN_COL_5 in admin_df.columns else set()
    )

    result_cols = list(result.columns)
    extra_df = _build_platform_only_rows_5(
        result_cols,
        admin_ibfpay_keys,
        admin_order_keys,
        ibfpay_lk if ibfpay_avail else None,
        superpay_lk if superpay_avail else None,
        wangguypay_lk if wangguypay_avail else None,
    )

    if not extra_df.empty:
        result = pd.concat([result, extra_df], ignore_index=True)

    return result


def log_match_stats_5(result_df: pd.DataFrame) -> None:
    """按匹配状态打印统计摘要。未匹配数 > 0 时打 warning。"""
    total     = len(result_df)
    matched   = (result_df[MATCH_STATUS_COL_5] == "是").sum()
    unmatched = (result_df[MATCH_STATUS_COL_5] == "否").sum()
    extra     = (result_df[MATCH_STATUS_COL_5] == "平台多余").sum()
    logging.info("共 %d 条 — 匹配 %d / 未匹配 %d / 平台多余 %d", total, matched, unmatched, extra)
    if unmatched > 0:
        logging.warning("存在 %d 条未匹配记录，请查看【匹配失败订单】sheet", unmatched)


def build_summary_sheet_5(result_df: pd.DataFrame) -> pd.DataFrame:
    """按平台汇总代付金额、手续费、到账金额（仅统计匹配成功的行）。

    分组键：机构列（ADMIN_ORG_COL_5）。
    汇总列：成功笔数、代付金额合计、手续费合计、到账金额合计。

    Args:
        result_df: enrich_admin_5 返回的完整结果 DataFrame。

    Returns:
        汇总 DataFrame，供写入 OUTPUT_SUMMARY_SHEET_5。
    """
    summary_cols = ["机构", "笔数", "代付金额合计", "手续费合计", "到账金额合计"]
    matched = result_df[result_df[MATCH_STATUS_COL_5].isin(["是", "平台多余"])].copy()
    if matched.empty or ADMIN_ORG_COL_5 not in matched.columns:
        return pd.DataFrame(columns=summary_cols)

    matched["_amt"]    = matched[PLATFORM_AMOUNT_COL_5].apply(lambda v: _to_float_5(v) or 0.0)
    matched["_fee"]    = matched[FEE_COL_5].apply(lambda v: _to_float_5(v) or 0.0)
    matched["_arrive"] = matched[ARRIVE_AMOUNT_COL_5].apply(lambda v: _to_float_5(v) or 0.0)

    grp = matched.groupby(ADMIN_ORG_COL_5, as_index=False).agg(
        笔数=("_amt", "count"),
        代付金额合计=("_amt", "sum"),
        手续费合计=("_fee", "sum"),
        到账金额合计=("_arrive", "sum"),
    )
    grp = grp.rename(columns={ADMIN_ORG_COL_5: "机构"})
    for col in ["代付金额合计", "手续费合计", "到账金额合计"]:
        grp[col] = grp[col].round(2)
    return grp[summary_cols]


# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------

def write_output_5(
    result_df: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """将结果写入 data/output/代付对账结果_{YYYYMMDD}.xlsx。

    输出结构（3 个 sheet）：
      - OUTPUT_SHEET_5（"代付对账结果"）：全量结果，冻结首行，启用自动筛选
      - OUTPUT_FAILED_SHEET_5（"匹配失败订单"）：是否匹配=否 的行
      - OUTPUT_SUMMARY_SHEET_5（"平台汇总"）：build_summary_sheet_5 结果

    Args:
        result_df: enrich_admin_5 返回的完整 DataFrame。
        output_dir: 输出目录（自动 mkdir）。

    Returns:
        写入的输出文件 Path。
    """
    today = date.today().strftime("%Y%m%d")
    filename = OUTPUT_FILE_TEMPLATE_5.format(date=today)
    output_path = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)

    failed_df  = result_df[result_df[MATCH_STATUS_COL_5] == "否"].copy()
    summary_df = build_summary_sheet_5(result_df)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name=OUTPUT_SHEET_5, index=False)
        failed_df.to_excel(writer, sheet_name=OUTPUT_FAILED_SHEET_5, index=False)
        summary_df.to_excel(writer, sheet_name=OUTPUT_SUMMARY_SHEET_5, index=False)
        for sname in (OUTPUT_SHEET_5, OUTPUT_FAILED_SHEET_5):
            ws = writer.sheets[sname]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions

    logging.info("结果文件已写入: %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> int:
    """代号5 主流程：扫描 → 读取 → 构建查找表 → 匹配 → 输出。

    Returns:
        0 表示成功，1 表示失败。
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── 扫描 ──────────────────────────────────────────────────
    files = scan_source_files_5(INPUT_DIR_5)

    # ── admin 必须存在 ────────────────────────────────────────
    if not files["admin"]:
        logging.error(
            "未找到 admin 文件。请将文件放入 %s，"
            "文件名须以 'admin-'（不区分大小写）开头，"
            "例如：admin-Okey兑换202604-V3.xls",
            INPUT_DIR_5,
        )
        return 1

    # ── 读取 admin（支持多文件合并）──────────────────────────
    admin_frames = [read_admin_5(fp) for fp in files["admin"]]
    admin_df = pd.concat(admin_frames, ignore_index=True) if len(admin_frames) > 1 else admin_frames[0]
    logging.info("admin 共 %d 条记录", len(admin_df))

    # ── IBFYPAY ───────────────────────────────────────────────
    ibfpay_lk: Optional[pd.DataFrame] = None
    if files["ibfpay"]:
        frames = [read_ibfpay_5(fp) for fp in files["ibfpay"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        ibfpay_lk = build_ibfpay_lookup_5(raw)
        logging.info("IBFYPAY 查找表共 %d 条", len(ibfpay_lk))

    # ── SUPERPAY ──────────────────────────────────────────────
    superpay_lk: Optional[pd.DataFrame] = None
    if files["superpay"]:
        frames = [read_superpay_5(fp) for fp in files["superpay"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        superpay_lk = build_superpay_lookup_5(raw)
        logging.info("SUPERPAY 查找表共 %d 条", len(superpay_lk))

    # ── WANGGUYPAY ────────────────────────────────────────────
    wangguypay_lk: Optional[pd.DataFrame] = None
    if files["wangguypay"]:
        frames = [read_wangguypay_5(fp) for fp in files["wangguypay"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        wangguypay_lk = build_wangguypay_lookup_5(raw)
        logging.info("WANGGUYPAY 查找表共 %d 条", len(wangguypay_lk))

    # ── 匹配 ──────────────────────────────────────────────────
    result_df = enrich_admin_5(admin_df, ibfpay_lk, superpay_lk, wangguypay_lk)
    log_match_stats_5(result_df)

    # ── 输出 ──────────────────────────────────────────────────
    try:
        output_path = write_output_5(result_df, OUTPUT_DIR)
        logging.info("完成。输出文件: %s", output_path)
    except PermissionError:
        logging.error("无法写入输出文件，请确认文件未在 Excel 中打开后重试。")
        return 1
    except Exception:
        logging.error("写入输出文件失败", exc_info=True)
        return 1

    return 0
