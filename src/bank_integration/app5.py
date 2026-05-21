"""代号5（代付订单对账）应用逻辑。

数据流：
    data/input/raw/5/  →  scan_source_files_5()
                               │
        read_admin_5 / read_ibfpay_5 / read_superpay_5 / read_wangguypay_5 / read_phonecard_5
                               │
    build_ibfpay_lookup_5 / build_superpay_lookup_5 / build_wangguypay_lookup_5 / build_phonecard_lookup_5
                               │
                      enrich_admin_5()   ← left-join 三个查找表
                               │
          data/output/代付对账结果_{YYYYMMDD}.xlsx

输出新增列（追加在 admin 原始列末尾）：
    是否匹配      - 是 / 否 / 平台多余
    平台流水号    - 各平台主键
    平台代付金额  - 平台记录的代付金额
    币种          - 平台金额币种
    平台状态      - 平台记录的交易状态
    手续费        - 平台收取的手续费
    到账金额      - 扣除手续费后实际到账
    交易日期      - 格式化后的交易时间
"""

import logging
import re
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
    ADMIN_TP_ORDER_COL_5,
    IBFYPAY_SHEET_5,
    IBFYPAY_HEADER_5,
    IBFYPAY_JOIN_COL_5,
    IBFYPAY_ADMIN_JOIN_COL_5,
    IBFYPAY_TYPE_COL_5,
    IBFYPAY_TYPE_SYSTEM_5,
    IBFYPAY_TYPE_PAYOUT_5,
    IBFYPAY_TYPE_FEE_5,
    IBFYPAY_TYPE_REJECT_5,
    IBFYPAY_AMOUNT_COL_5,
    IBFYPAY_BEGIN_AMOUNT_COL_5,
    IBFYPAY_END_AMOUNT_COL_5,
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
    SUPERPAY_CURRENCY_COL_5,
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
    WANGGUYPAY_FUND_TYPE_COL_5,
    WANGGUYPAY_BEGIN_AMOUNT_COL_5,
    WANGGUYPAY_FUND_AMOUNT_COL_5,
    WANGGUYPAY_END_AMOUNT_COL_5,
    WANGGUYPAY_FUND_TYPE_PAYOUT_5,
    WANGGUYPAY_FUND_TYPE_FEE_5,
    WANGGUYPAY_FUND_STATUS_5,
    WANGGUYPAY_FUND_FILE_PREFIXES_5,
    PHONECARD_PLATFORM_NAME_5,
    PHONECARD_JOIN_COL_5,
    PHONECARD_AMOUNT_COL_5,
    PHONECARD_STATUS_COL_5,
    PHONECARD_DATE_COL_5,
    PHONECARD_PLATFORM_NO_COL_5,
    PHONECARD_ORDER_TYPE_COL_5,
    PHONECARD_PRIZE_COL_5,
    PHONECARD_PREFERRED_SHEET_KEY_5,
    EPIN_PLATFORM_NAME_5,
    EPIN_SIPARISLER_ORDER_ID_COL_5,
    EPIN_SIPARISLER_STATUS_COL_5,
    EPIN_SIPARISLER_ORDER_NO_COL_5,
    EPIN_SIPARISLER_PRODUCT_COL_5,
    EPIN_SIPARISLER_UNIT_PRICE_COL_5,
    EPIN_SIPARISLER_AMOUNT_COL_5,
    EPIN_SIPARISLER_QTY_COL_5,
    EPIN_SIPARISLER_CONFIRM_TIME_COL_5,
    EPIN_PINLER_ORDER_ID_COL_5,
    EPIN_PINLER_ORDER_NO_COL_5,
    EPIN_PINLER_PIN_ID_COL_5,
    EPIN_PINLER_PIN_CODE_COL_5,
    EPIN_ORG_PATTERN_5,
    EPIN_ADMIN_JOIN_COL_5,
    MATCH_STATUS_COL_5,
    PLATFORM_ORDER_NO_COL_5,
    PLATFORM_AMOUNT_COL_5,
    PLATFORM_CURRENCY_COL_5,
    PLATFORM_STATUS_COL_5,
    FEE_COL_5,
    ARRIVE_AMOUNT_COL_5,
    TRANSACTION_DATE_COL_5,
    OUTPUT_NEW_COLS_5,
    PLATFORM_PREFIXES_5,
    SUMMARY_BEGIN_BALANCE_COL_5,
    SUMMARY_RECHARGE_COL_5,
    SUMMARY_WITHDRAWAL_COL_5,
    SUMMARY_CALC_END_BALANCE_COL_5,
    SUMMARY_PLATFORM_END_BALANCE_COL_5,
    SUMMARY_BALANCE_COLS_5,
    IBFYPAY_DEFAULT_CURRENCY_5,
    WANGGUYPAY_DEFAULT_CURRENCY_5,
    EPIN_DEFAULT_CURRENCY_5,
    BALANCE_RECHARGE_KEYWORDS_5,
    BALANCE_RECHARGE_EXCLUDE_KEYWORDS_5,
    BALANCE_WITHDRAWAL_KEYWORDS_5,
)


IBFYPAY_SOURCE_PRIORITY_COL_5 = "_ibfpay_source_priority"
WANGGUYPAY_FORMAT_COL_5 = "_wangguypay_format"
WANGGUYPAY_FORMAT_FUND_5 = "fund"
WANGGUYPAY_FORMAT_ORDER_5 = "order"
PLATFORM_STATUS_MAP_5 = {
    "SUPERPAY": {
        "代付成功": "成功",
        "代付失败": "失败",
        "代付关闭": "关闭",
    },
    "WANGGUYPAY": {
        "付款成功": "成功",
        "付款失败": "失败",
        "处理中": "处理中",
        WANGGUYPAY_FUND_STATUS_5: "成功",
    },
    "PHONECARD": {
        "已完成": "成功",
        "成功": "成功",
        "失败": "失败",
        "处理中": "处理中",
        "关闭": "关闭",
    },
    "EPIN": {
        "Başarılı":  "成功",    # 土耳其语"成功"
        "Başarısız": "失败",    # 土耳其语"失败"
        "Beklemede": "处理中",  # 土耳其语"等待中"
        "İptal":     "关闭",    # 土耳其语"取消"
    },
}


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


def _to_datetime_5(val):
    """将任意时间值转为 pandas Timestamp，失败返回 NaT。"""
    try:
        if pd.isna(val):
            return pd.NaT
    except (TypeError, ValueError):
        pass
    return pd.to_datetime(str(val).strip(), errors="coerce")


def _is_recharge_type_5(raw_type: str) -> bool:
    """判断资金流水类型是否为明确的充值/入金类。"""
    text = str(raw_type).strip()
    if not text:
        return False
    if any(k in text for k in BALANCE_RECHARGE_EXCLUDE_KEYWORDS_5):
        return False
    return any(k in text for k in BALANCE_RECHARGE_KEYWORDS_5)


def _is_withdrawal_type_5(raw_type: str) -> bool:
    """判断资金流水类型是否为提现/出金类。"""
    text = str(raw_type).strip()
    if not text:
        return False
    return any(k in text for k in BALANCE_WITHDRAWAL_KEYWORDS_5)


def _first_valid_float_5(values, *, reverse: bool = False) -> Optional[float]:
    """从序列中取第一个有效数字。"""
    items = list(values)
    if reverse:
        items = list(reversed(items))
    for value in items:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass
        return float(value)
    return None


def _pick_chain_begin_5(grp: pd.DataFrame) -> Optional[float]:
    """同一时间多条流水时，取不被其他行期末承接的期初余额。"""
    first_time = grp["_balance_time"].min()
    rows = grp[grp["_balance_time"] == first_time]
    end_values = {round(float(v), 6) for v in rows["_end"].tolist() if not pd.isna(v)}
    for value in rows["_begin"].tolist():
        if value is None or pd.isna(value):
            continue
        if round(float(value), 6) not in end_values:
            return float(value)
    return _first_valid_float_5(rows["_begin"].tolist())


def _pick_chain_end_5(grp: pd.DataFrame) -> Optional[float]:
    """同一时间多条流水时，取不再作为其他行期初的期末余额。"""
    last_time = grp["_balance_time"].max()
    rows = grp[grp["_balance_time"] == last_time]
    begin_values = {round(float(v), 6) for v in rows["_begin"].tolist() if not pd.isna(v)}
    for value in reversed(rows["_end"].tolist()):
        if value is None or pd.isna(value):
            continue
        if round(float(value), 6) not in begin_values:
            return float(value)
    return _first_valid_float_5(rows["_end"].tolist(), reverse=True)


def _normalize_platform_status_5(platform: str, raw_status: str = "", *, rejected: bool = False) -> str:
    """将各平台原始状态统一为 成功/失败/处理中/关闭/驳回。"""
    platform_key = str(platform).strip().upper()
    if platform_key == "IBFYPAY":
        return "驳回" if rejected else "成功"

    status = str(raw_status).strip()
    if not status:
        return ""

    mapping = PLATFORM_STATUS_MAP_5.get(platform_key, {})
    normalized = mapping.get(status)
    if normalized is not None:
        return normalized

    logging.warning("【%s】发现未识别平台状态 '%s'，保留原值", platform_key or platform, status)
    return status


def _normalize_currency_5(value: object) -> str:
    """清洗平台币种显示值。"""
    return str(value or "").strip().upper()


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
        wangupay-/wangguypay- 或 Wangupay资金记录 → WANGGUYPAY 平台（XLS/XLSX）

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

    wangguypay_files = result.get("wangguypay", [])
    fund_files = [
        f for f in wangguypay_files
        if any(f.stem.lower().startswith(p) for p in WANGGUYPAY_FUND_FILE_PREFIXES_5)
    ]
    if fund_files:
        ignored = [f.name for f in wangguypay_files if f not in fund_files]
        if ignored:
            logging.info("WANGGUYPAY 已发现资金记录文件，忽略旧付款订单文件: %s", ignored)
        result["wangguypay"] = fund_files

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
                if default_name is not None and sheet_name != default_name:
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
        merged[IBFYPAY_SOURCE_PRIORITY_COL_5] = 2
        reject_keys = set(
            df.loc[df[IBFYPAY_TYPE_COL_5].str.strip() == IBFYPAY_TYPE_REJECT_5, IBFYPAY_JOIN_COL_5]
            .astype(str).str.strip()
        )
        merged["_ibfpay_rejected"] = merged[IBFYPAY_JOIN_COL_5].isin(reject_keys)
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
        merged[IBFYPAY_SOURCE_PRIORITY_COL_5] = 1
        merged["_ibfpay_rejected"] = False
        if IBFYPAY_TIME_COL_5 not in merged.columns:
            merged[IBFYPAY_TIME_COL_5] = ""

    for col in ("代付金额", "手续费"):
        merged[col] = pd.to_numeric(
            merged[col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        ).fillna(0.0).abs()

    return merged


def read_ibfpay_balance_source_5(filepath: Path) -> pd.DataFrame:
    """读取 IBFYPAY 原始资金流水列，用于平台汇总余额计算。"""
    df = pd.read_excel(filepath, sheet_name=IBFYPAY_SHEET_5, header=IBFYPAY_HEADER_5, dtype=str)
    df = df.dropna(how="all").fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    required = [
        IBFYPAY_TYPE_COL_5,
        IBFYPAY_BEGIN_AMOUNT_COL_5,
        IBFYPAY_AMOUNT_COL_5,
        IBFYPAY_END_AMOUNT_COL_5,
        IBFYPAY_TIME_COL_5,
    ]
    if not all(c in df.columns for c in required):
        logging.info("【IBFYPAY】%s 无完整余额流水列，跳过平台余额提取", filepath.name)
        return pd.DataFrame(columns=required)
    return df[required].copy()


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
    """读取 WANGGUYPAY 文件，自动兼容资金记录和旧付款订单格式。

    资金记录格式不依赖固定 sheet 名；遍历全部 sheet，使用 header=1 预览，
    找到同时包含 平台订单号、交易类型、变动金额(try) 的 sheet 后读取。
    旧付款订单格式继续按付款订单列读取。
    """
    fund_required = [
        WANGGUYPAY_PLATFORM_NO_COL_5,
        WANGGUYPAY_FUND_TYPE_COL_5,
        WANGGUYPAY_FUND_AMOUNT_COL_5,
    ]
    try:
        sheet = _select_sheet_by_columns_5(
            filepath,
            default_sheet=None,
            header=WANGGUYPAY_HEADER_5,
            required_columns=fund_required,
            label="WANGGUYPAY资金记录",
        )
        df = pd.read_excel(filepath, sheet_name=sheet, header=WANGGUYPAY_HEADER_5, dtype=str)
        df = df.dropna(how="all").fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        df[WANGGUYPAY_FORMAT_COL_5] = WANGGUYPAY_FORMAT_FUND_5
        return df
    except ValueError:
        pass

    sheet = _select_sheet_by_columns_5(
        filepath,
        default_sheet=WANGGUYPAY_SHEET_5,
        header=WANGGUYPAY_HEADER_5,
        required_columns=[WANGGUYPAY_JOIN_COL_5, WANGGUYPAY_AMOUNT_COL_5, WANGGUYPAY_STATUS_COL_5],
        label="WANGGUYPAY付款订单",
    )
    df = pd.read_excel(filepath, sheet_name=sheet, header=WANGGUYPAY_HEADER_5, dtype=str)
    df = df.dropna(how="all").fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    df[WANGGUYPAY_FORMAT_COL_5] = WANGGUYPAY_FORMAT_ORDER_5
    return df


def _select_phonecard_sheet_5(filepath: Path) -> str:
    """选择话费卡订单明细 sheet，优先使用名称含“汇总”的 sheet。"""
    required = [
        PHONECARD_DATE_COL_5,
        PHONECARD_JOIN_COL_5,
        PHONECARD_PRIZE_COL_5,
        PHONECARD_AMOUNT_COL_5,
        PHONECARD_STATUS_COL_5,
        PHONECARD_PLATFORM_NO_COL_5,
        PHONECARD_ORDER_TYPE_COL_5,
    ]
    with pd.ExcelFile(filepath) as xls:
        candidates = []
        for sheet_name in xls.sheet_names:
            try:
                preview = pd.read_excel(xls, sheet_name=sheet_name, nrows=0, dtype=str)
            except Exception:
                logging.warning("无法检查话费卡文件 sheet '%s' 的表头", sheet_name)
                continue
            cols = _normalize_columns_5(preview.columns)
            if all(c in cols for c in required):
                candidates.append(sheet_name)

    if not candidates:
        raise ValueError(f"【话费卡】{filepath.name} 中找不到含必要列 {required!r} 的 sheet")

    preferred = [
        s for s in candidates
        if PHONECARD_PREFERRED_SHEET_KEY_5 in str(s)
    ]
    return preferred[0] if preferred else candidates[0]


def read_phonecard_5(filepath: Path) -> pd.DataFrame:
    """读取 Okey 话费卡结算文件的订单明细列。

    样例文件的“0201-0430汇总”左侧是逐单明细，右侧是结算汇总。此处只保留
    逐单对账所需列，避免把右侧汇总字段误当成逐单手续费或结算金额。
    """
    sheet = _select_phonecard_sheet_5(filepath)
    df = pd.read_excel(filepath, sheet_name=sheet, dtype=str)
    df = df.dropna(how="all").fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    keep_cols = [
        PHONECARD_DATE_COL_5,
        PHONECARD_JOIN_COL_5,
        PHONECARD_PRIZE_COL_5,
        PHONECARD_AMOUNT_COL_5,
        PHONECARD_STATUS_COL_5,
        PHONECARD_PLATFORM_NO_COL_5,
        PHONECARD_ORDER_TYPE_COL_5,
    ]
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        logging.warning("【话费卡】%s 缺少列 %s，跳过", filepath.name, missing)
        return pd.DataFrame(columns=keep_cols)
    return df[keep_cols].copy()


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
    work = df.copy()
    if "手续费" in work.columns:
        work["_ibfpay_fee_nonzero"] = (
            pd.to_numeric(work["手续费"], errors="coerce").fillna(0.0).abs() > 0
        ).astype(int)
    else:
        work["_ibfpay_fee_nonzero"] = 0
    if IBFYPAY_SOURCE_PRIORITY_COL_5 not in work.columns:
        work[IBFYPAY_SOURCE_PRIORITY_COL_5] = 0

    work = work.sort_values(
        by=["_ibfpay_fee_nonzero", IBFYPAY_SOURCE_PRIORITY_COL_5],
        ascending=[False, False],
        kind="mergesort",
    )

    keep_cols = [c for c in [
        IBFYPAY_JOIN_COL_5,
        "代付金额",
        "手续费",
        IBFYPAY_TIME_COL_5,
        "_ibfpay_rejected",
    ] if c in work.columns]
    result = _dedup_lookup_5(work[keep_cols], IBFYPAY_JOIN_COL_5, "IBFYPAY")
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
        SUPERPAY_CURRENCY_COL_5,
        SUPERPAY_FEE_TOTAL_COL_5,
        SUPERPAY_ACTUAL_COL_5,
        SUPERPAY_STATUS_COL_5,
        SUPERPAY_CREATE_TIME_COL_5,
        SUPERPAY_FINISH_TIME_COL_5,
    ] if c in df.columns]
    result = _dedup_lookup_5(df[keep_cols], SUPERPAY_JOIN_COL_5, "SUPERPAY")
    return result.set_index(SUPERPAY_JOIN_COL_5)


def build_wangguypay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 WANGGUYPAY 平台订单号为索引的查找表。

    新资金记录格式按 平台订单号 聚合：付款结算为代付金额，扣除代付结算手续费为手续费。
    旧付款订单格式也统一改用平台订单号作为索引，以便与 admin.第三方订单号 匹配。
    """
    is_fund = (
        WANGGUYPAY_FORMAT_COL_5 in df.columns
        and (df[WANGGUYPAY_FORMAT_COL_5].astype(str).str.strip() == WANGGUYPAY_FORMAT_FUND_5).any()
    ) or all(c in df.columns for c in [
        WANGGUYPAY_PLATFORM_NO_COL_5,
        WANGGUYPAY_FUND_TYPE_COL_5,
        WANGGUYPAY_FUND_AMOUNT_COL_5,
    ])

    if is_fund:
        required = [
            WANGGUYPAY_PLATFORM_NO_COL_5,
            WANGGUYPAY_FUND_TYPE_COL_5,
            WANGGUYPAY_FUND_AMOUNT_COL_5,
        ]
        if not all(c in df.columns for c in required):
            logging.warning("【WANGGUYPAY】资金记录缺少必要列 %s，跳过", required)
            return pd.DataFrame(columns=[
                WANGGUYPAY_AMOUNT_COL_5,
                WANGGUYPAY_FEE_COL_5,
                WANGGUYPAY_ARRIVE_COL_5,
                WANGGUYPAY_STATUS_COL_5,
                WANGGUYPAY_CREATE_TIME_COL_5,
                WANGGUYPAY_FINISH_TIME_COL_5,
            ])

        work = df.copy()
        work[WANGGUYPAY_PLATFORM_NO_COL_5] = work[WANGGUYPAY_PLATFORM_NO_COL_5].astype(str).str.strip()
        work[WANGGUYPAY_FUND_TYPE_COL_5] = work[WANGGUYPAY_FUND_TYPE_COL_5].astype(str).str.strip()
        work["_fund_amount"] = pd.to_numeric(
            work[WANGGUYPAY_FUND_AMOUNT_COL_5].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        ).fillna(0.0).abs()

        payout = work[work[WANGGUYPAY_FUND_TYPE_COL_5] == WANGGUYPAY_FUND_TYPE_PAYOUT_5].copy()
        fee = work[work[WANGGUYPAY_FUND_TYPE_COL_5] == WANGGUYPAY_FUND_TYPE_FEE_5].copy()

        payout_cols = [WANGGUYPAY_PLATFORM_NO_COL_5, "_fund_amount"]
        for c in (WANGGUYPAY_CREATE_TIME_COL_5, WANGGUYPAY_FINISH_TIME_COL_5):
            if c in payout.columns:
                payout_cols.append(c)
        payout = payout[payout_cols].rename(columns={"_fund_amount": WANGGUYPAY_AMOUNT_COL_5})
        fee = fee[[WANGGUYPAY_PLATFORM_NO_COL_5, "_fund_amount"]].rename(
            columns={"_fund_amount": WANGGUYPAY_FEE_COL_5}
        )
        merged = payout.merge(fee, on=WANGGUYPAY_PLATFORM_NO_COL_5, how="left")
        merged[WANGGUYPAY_FEE_COL_5] = pd.to_numeric(
            merged[WANGGUYPAY_FEE_COL_5], errors="coerce"
        ).fillna(0.0)
        merged[WANGGUYPAY_ARRIVE_COL_5] = (
            pd.to_numeric(merged[WANGGUYPAY_AMOUNT_COL_5], errors="coerce").fillna(0.0)
            - merged[WANGGUYPAY_FEE_COL_5]
        ).round(2)
        merged[WANGGUYPAY_STATUS_COL_5] = WANGGUYPAY_FUND_STATUS_5

        keep_cols = [c for c in [
            WANGGUYPAY_PLATFORM_NO_COL_5,
            WANGGUYPAY_AMOUNT_COL_5,
            WANGGUYPAY_FEE_COL_5,
            WANGGUYPAY_ARRIVE_COL_5,
            WANGGUYPAY_STATUS_COL_5,
            WANGGUYPAY_CREATE_TIME_COL_5,
            WANGGUYPAY_FINISH_TIME_COL_5,
        ] if c in merged.columns]
        result = _dedup_lookup_5(merged[keep_cols], WANGGUYPAY_PLATFORM_NO_COL_5, "WANGGUYPAY资金记录")
        return result.set_index(WANGGUYPAY_PLATFORM_NO_COL_5, drop=False)

    keep_cols = [c for c in [
        WANGGUYPAY_PLATFORM_NO_COL_5,
        WANGGUYPAY_AMOUNT_COL_5,
        WANGGUYPAY_FEE_COL_5,
        WANGGUYPAY_ARRIVE_COL_5,
        WANGGUYPAY_STATUS_COL_5,
        WANGGUYPAY_CREATE_TIME_COL_5,
        WANGGUYPAY_FINISH_TIME_COL_5,
    ] if c in df.columns]
    result = _dedup_lookup_5(df[keep_cols], WANGGUYPAY_PLATFORM_NO_COL_5, "WANGGUYPAY")
    return result.set_index(WANGGUYPAY_PLATFORM_NO_COL_5, drop=False)


def build_phonecard_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以话费卡订单号为索引的查找表。"""
    keep_cols = [c for c in [
        PHONECARD_JOIN_COL_5,
        PHONECARD_PLATFORM_NO_COL_5,
        PHONECARD_AMOUNT_COL_5,
        PHONECARD_STATUS_COL_5,
        PHONECARD_DATE_COL_5,
    ] if c in df.columns]
    if PHONECARD_JOIN_COL_5 not in keep_cols:
        logging.warning("【话费卡】缺少关联列 '%s'，跳过", PHONECARD_JOIN_COL_5)
        return pd.DataFrame(columns=keep_cols)
    result = _dedup_lookup_5(df[keep_cols], PHONECARD_JOIN_COL_5, "话费卡")
    return result.set_index(PHONECARD_JOIN_COL_5)


def read_epin_siparisler_5(filepath: Path) -> pd.DataFrame:
    """读取 epin 订单列表文件（epin_siparisler_*）。"""
    df = pd.read_excel(filepath, engine="openpyxl", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").fillna("")
    logging.info("【EPIN siparisler】%s 共 %d 行", filepath.name, len(df))
    return df


def read_epin_pinler_5(filepath: Path) -> pd.DataFrame:
    """读取 epin pin 码列表文件（epin_pinler_*）。"""
    df = pd.read_excel(filepath, engine="openpyxl", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").fillna("")
    logging.info("【EPIN pinler】%s 共 %d 行", filepath.name, len(df))
    return df


def build_epin_lookup_5(
    siparisler_df: pd.DataFrame,
    pinler_df: pd.DataFrame,
) -> Optional[pd.DataFrame]:
    """构建以 Pin码 为索引的 epin 查找表。

    匹配路径：
      admin.第三方订单号（pin码）
        → epin_pinler.Pin码  →  epin_pinler.[Pin ID, 订单ID, 订单号]
        → epin_siparisler.(订单ID, 订单号)  →  [单价(USD), 产品, 确认时间, 订单状态]

    JOIN 键同时使用「订单ID + 订单号」：单独用订单ID可能因平台数据问题命中多行，
    双键联合可精确定位唯一行，作为平台层面的兜底容错。

    注：pandas 默认读取时 Pin码 列为明文；
    用 openpyxl data_only=True 时会显示为 ****，属正常差异。
    """
    # ── 1. 清洗 pinler：去掉脱敏行（全星号）和空值，保留明文 Pin码 ──────────
    p = pinler_df.copy()
    p[EPIN_PINLER_PIN_CODE_COL_5] = p[EPIN_PINLER_PIN_CODE_COL_5].str.strip()
    p = p[~p[EPIN_PINLER_PIN_CODE_COL_5].str.match(r"^\*+$", na=False)]
    p = p[p[EPIN_PINLER_PIN_CODE_COL_5] != ""]

    if p.empty:
        logging.warning("【EPIN】epin_pinler 无有效明文 Pin码，跳过 epin 匹配")
        return None

    # ── 2. 去重：同一 Pin码 保留首行 ─────────────────────────────────────────
    before = len(p)
    p = p.drop_duplicates(subset=[EPIN_PINLER_PIN_CODE_COL_5])
    if len(p) < before:
        logging.warning("【EPIN】epin_pinler 存在 %d 个重复 Pin码，已保留首行", before - len(p))

    # ── 3. 清洗 siparisler，按「订单ID + 订单号」双键去重 ─────────────────────
    join_keys_sip = [EPIN_SIPARISLER_ORDER_ID_COL_5, EPIN_SIPARISLER_ORDER_NO_COL_5]
    s = siparisler_df.copy()
    for col in join_keys_sip:
        if col in s.columns:
            s[col] = s[col].str.strip()
    before_s = len(s)
    s = s.drop_duplicates(subset=[k for k in join_keys_sip if k in s.columns])
    if len(s) < before_s:
        logging.warning("【EPIN】epin_siparisler 存在 %d 行重复(订单ID+订单号)，已保留首行",
                        before_s - len(s))

    # ── 4. pinler LEFT JOIN siparisler（双键：订单ID + 订单号）──────────────
    # 两份文件的列名相同（"订单ID"、"订单号"），直接 on= 即可
    pin_cols = [c for c in [
        EPIN_PINLER_PIN_CODE_COL_5,
        EPIN_PINLER_PIN_ID_COL_5,
        EPIN_PINLER_ORDER_ID_COL_5,
        EPIN_PINLER_ORDER_NO_COL_5,
    ] if c in p.columns]

    sip_cols = [c for c in [
        EPIN_SIPARISLER_ORDER_ID_COL_5,
        EPIN_SIPARISLER_ORDER_NO_COL_5,
        EPIN_SIPARISLER_UNIT_PRICE_COL_5,
        EPIN_SIPARISLER_PRODUCT_COL_5,
        EPIN_SIPARISLER_CONFIRM_TIME_COL_5,
        EPIN_SIPARISLER_STATUS_COL_5,
    ] if c in s.columns]

    # 两侧 JOIN 列名相同，用 on= 避免产生 _x/_y 后缀
    on_cols = [c for c in [EPIN_PINLER_ORDER_ID_COL_5, EPIN_PINLER_ORDER_NO_COL_5]
               if c in pin_cols and c in sip_cols]

    merged = p[pin_cols].merge(s[sip_cols], on=on_cols, how="left")

    # ── 5. merge 后若仍有同一 Pin码 多行，打 warning 并保留首行 ───────────────
    dup_mask = merged.duplicated(subset=[EPIN_PINLER_PIN_CODE_COL_5], keep=False)
    if dup_mask.any():
        n_dup = dup_mask.sum()
        logging.warning("【EPIN】双键 JOIN 后仍有 %d 行重复 Pin码，保留首行", n_dup)
        merged = merged.drop_duplicates(subset=[EPIN_PINLER_PIN_CODE_COL_5])

    merged = merged.fillna("")
    lookup = merged.set_index(EPIN_PINLER_PIN_CODE_COL_5)
    logging.info("【EPIN】查找表共 %d 条（有效 Pin码数）", len(lookup))
    return lookup


# ---------------------------------------------------------------------------
# 主匹配逻辑
# ---------------------------------------------------------------------------

def _build_platform_only_rows_5(
    result_cols: list,
    admin_ibfpay_keys: Set[str],
    admin_order_keys: Set[str],
    admin_wangguypay_keys: Set[str],
    ibfpay_lk: Optional[pd.DataFrame],
    superpay_lk: Optional[pd.DataFrame],
    wangguypay_lk: Optional[pd.DataFrame],
    phonecard_lk: Optional[pd.DataFrame],
    epin_lk: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """构建平台有、admin 无的多余行（MATCH_STATUS_COL_5 = "平台多余"）。

    admin_ibfpay_keys: admin.第三方订单号 集合，与 ibfpay_lk.index（系统流水号）比对
    admin_order_keys:  admin.订单号 集合，与 superpay_lk.index（商户订单号）比对
    admin_wangguypay_keys: admin.第三方订单号 集合，与 wangguypay_lk.index（平台订单号）比对
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
            is_rejected = (
                bool(ibfpay_lk.at[key, "_ibfpay_rejected"])
                if "_ibfpay_rejected" in ibfpay_lk.columns else False
            )
            row[PLATFORM_AMOUNT_COL_5] = amt
            row[PLATFORM_CURRENCY_COL_5] = IBFYPAY_DEFAULT_CURRENCY_5
            row[PLATFORM_STATUS_COL_5] = _normalize_platform_status_5("IBFYPAY", rejected=is_rejected)
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
            sp_currency = _normalize_currency_5(superpay_lk.at[key, SUPERPAY_CURRENCY_COL_5]) if SUPERPAY_CURRENCY_COL_5 in superpay_lk.columns else ""
            sp_status = str(superpay_lk.at[key, SUPERPAY_STATUS_COL_5]).strip()       if SUPERPAY_STATUS_COL_5       in superpay_lk.columns else ""
            sp_time   = str(superpay_lk.at[key, SUPERPAY_FINISH_TIME_COL_5]).strip()  if SUPERPAY_FINISH_TIME_COL_5  in superpay_lk.columns else ""
            sp_amt_f    = _to_float_5(sp_amt) or 0.0
            sp_actual_f = _to_float_5(sp_actual) or 0.0
            sp_calc_fee = str(round(abs(sp_amt_f - sp_actual_f), 2)) if (sp_amt or sp_actual) else ""
            row[PLATFORM_ORDER_NO_COL_5] = sp_no
            row[PLATFORM_AMOUNT_COL_5]   = sp_amt
            row[PLATFORM_CURRENCY_COL_5] = sp_currency
            row[PLATFORM_STATUS_COL_5]   = _normalize_platform_status_5("SUPERPAY", sp_status)
            row[FEE_COL_5]               = sp_calc_fee
            row[ARRIVE_AMOUNT_COL_5]     = sp_actual
            row[TRANSACTION_DATE_COL_5]  = _format_date_5(sp_time)
            extra.append(row)

    # ── WANGGUYPAY 多余行 ─────────────────────────────────────
    if wangguypay_lk is not None:
        for key in wangguypay_lk.index:
            k = str(key).strip()
            if not k or k in admin_wangguypay_keys:
                continue
            row = {c: "" for c in result_cols}
            row[ADMIN_TP_ORDER_COL_5]    = k
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
            row[PLATFORM_CURRENCY_COL_5] = WANGGUYPAY_DEFAULT_CURRENCY_5
            row[PLATFORM_STATUS_COL_5]   = _normalize_platform_status_5("WANGGUYPAY", wg_status)
            row[FEE_COL_5]               = wg_fee
            row[ARRIVE_AMOUNT_COL_5]     = wg_arrive
            row[TRANSACTION_DATE_COL_5]  = _format_date_5(wg_time)
            extra.append(row)

    # ── 话费卡多余行 ─────────────────────────────────────────
    if phonecard_lk is not None:
        for key in phonecard_lk.index:
            k = str(key).strip()
            if not k or k in admin_order_keys:
                continue
            row = {c: "" for c in result_cols}
            row[ADMIN_JOIN_COL_5]        = k
            row[MATCH_STATUS_COL_5]      = "平台多余"
            row[ADMIN_ORG_COL_5]         = PHONECARD_PLATFORM_NAME_5
            pc_no     = str(phonecard_lk.at[key, PHONECARD_PLATFORM_NO_COL_5]).strip() if PHONECARD_PLATFORM_NO_COL_5 in phonecard_lk.columns else ""
            pc_amt    = str(phonecard_lk.at[key, PHONECARD_AMOUNT_COL_5]).strip()      if PHONECARD_AMOUNT_COL_5      in phonecard_lk.columns else ""
            pc_status = str(phonecard_lk.at[key, PHONECARD_STATUS_COL_5]).strip()      if PHONECARD_STATUS_COL_5      in phonecard_lk.columns else ""
            pc_time   = str(phonecard_lk.at[key, PHONECARD_DATE_COL_5]).strip()        if PHONECARD_DATE_COL_5        in phonecard_lk.columns else ""
            row[PLATFORM_ORDER_NO_COL_5] = pc_no
            row[PLATFORM_AMOUNT_COL_5]   = pc_amt
            row[PLATFORM_CURRENCY_COL_5] = ""
            row[PLATFORM_STATUS_COL_5]   = _normalize_platform_status_5("PHONECARD", pc_status)
            row[FEE_COL_5]               = ""
            row[ARRIVE_AMOUNT_COL_5]     = pc_amt
            row[TRANSACTION_DATE_COL_5]  = _format_date_5(pc_time)
            extra.append(row)

    # ── epin 多余行（pin码在 epin 平台有、admin 无）────────────────────────────
    # 关联键与 IBFYPAY 相同（admin.第三方订单号），复用 admin_ibfpay_keys 作比对集合
    if epin_lk is not None:
        for key in epin_lk.index:
            k = str(key).strip()
            if not k or k in admin_ibfpay_keys:
                continue
            row: dict = {c: "" for c in result_cols}
            row[ADMIN_TP_ORDER_COL_5]    = k   # 第三方订单号 = pin码
            row[MATCH_STATUS_COL_5]      = "平台多余"
            row[ADMIN_ORG_COL_5]         = EPIN_PLATFORM_NAME_5
            ep_pin_id = str(epin_lk.at[key, EPIN_PINLER_PIN_ID_COL_5]).strip()  if EPIN_PINLER_PIN_ID_COL_5          in epin_lk.columns else ""
            ep_amt    = str(epin_lk.at[key, EPIN_SIPARISLER_UNIT_PRICE_COL_5]).strip() if EPIN_SIPARISLER_UNIT_PRICE_COL_5  in epin_lk.columns else ""
            ep_status = str(epin_lk.at[key, EPIN_SIPARISLER_STATUS_COL_5]).strip()     if EPIN_SIPARISLER_STATUS_COL_5      in epin_lk.columns else ""
            ep_time   = str(epin_lk.at[key, EPIN_SIPARISLER_CONFIRM_TIME_COL_5]).strip() if EPIN_SIPARISLER_CONFIRM_TIME_COL_5 in epin_lk.columns else ""
            row[PLATFORM_ORDER_NO_COL_5] = ep_pin_id
            row[PLATFORM_AMOUNT_COL_5]   = ep_amt
            row[PLATFORM_CURRENCY_COL_5] = EPIN_DEFAULT_CURRENCY_5
            row[PLATFORM_STATUS_COL_5]   = _normalize_platform_status_5(EPIN_PLATFORM_NAME_5, ep_status)
            row[FEE_COL_5]               = ""
            row[ARRIVE_AMOUNT_COL_5]     = ep_amt
            row[TRANSACTION_DATE_COL_5]  = _format_date_5(ep_time)
            extra.append(row)

    if not extra:
        return pd.DataFrame(columns=result_cols)
    return pd.DataFrame(extra, columns=result_cols)


def enrich_admin_5(
    admin_df: pd.DataFrame,
    ibfpay_lk: Optional[pd.DataFrame],
    superpay_lk: Optional[pd.DataFrame],
    wangguypay_lk: Optional[pd.DataFrame],
    phonecard_lk: Optional[pd.DataFrame] = None,
    epin_lk: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """以 admin 为主表，通过订单号与各平台查找表 left-join，追加新增列。

    匹配逻辑（优先级：IBFYPAY > SUPERPAY > WANGGUYPAY > 话费卡 > EPIN）：
      - IBFYPAY：以 admin.第三方订单号 ↔ ibfpay_lk.index（系统流水号）关联
      - SUPERPAY：以 admin.订单号 ↔ superpay_lk.index（商户订单号）关联
      - WANGGUYPAY：以 admin.第三方订单号 ↔ wangguypay_lk.index（平台订单号）关联
      - 话费卡：以 admin.订单号 ↔ phonecard_lk.index（订单号）关联
      - EPIN（TODO）：仅对机构为纯数字的行，以 admin.第三方订单号（pin码）
        ↔ epin_lk.index 关联；build_epin_lookup_5 实现后生效
      - 任一平台命中 → 是否匹配=是，均未命中 → 是否匹配=否
      - 追加平台多余行（_build_platform_only_rows_5）
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
    phonecard_avail  = phonecard_lk is not None and not phonecard_lk.empty
    epin_avail       = epin_lk is not None       and not epin_lk.empty

    if ibfpay_avail:
        result = _safe_merge(result, ibfpay_lk, IBFYPAY_ADMIN_JOIN_COL_5, "_i_", "IBFYPAY")
    if superpay_avail:
        result = _safe_merge(result, superpay_lk, ADMIN_JOIN_COL_5, "_s_", "SUPERPAY")
    if wangguypay_avail:
        result = _safe_merge(result, wangguypay_lk, ADMIN_TP_ORDER_COL_5, "_w_", "WANGGUYPAY")
    if phonecard_avail:
        result = _safe_merge(result, phonecard_lk, ADMIN_JOIN_COL_5, "_p_", "PHONECARD")
    if epin_avail:
        result = _safe_merge(result, epin_lk, EPIN_ADMIN_JOIN_COL_5, "_e_", "EPIN")

    match_status_list      = []
    platform_order_no_list = []
    platform_amount_list   = []
    platform_currency_list = []
    platform_status_list   = []
    fee_list               = []
    arrive_amount_list     = []
    transaction_date_list  = []
    org_output_list        = []

    for _, row in result.iterrows():
        # IBFYPAY 命中判断（join 键：admin.第三方订单号 ↔ ibfpay.系统流水号）
        ibf_amt  = str(row.get("_i_代付金额",               "")).strip() if ibfpay_avail    else ""
        ibf_fee  = str(row.get("_i_手续费",                 "")).strip() if ibfpay_avail    else ""
        ibf_time = str(row.get(f"_i_{IBFYPAY_TIME_COL_5}",  "")).strip() if ibfpay_avail    else ""
        ibf_tp   = str(row.get(IBFYPAY_ADMIN_JOIN_COL_5,    "")).strip()
        ibf_hit  = ibf_amt != ""
        ibf_rejected = (
            str(row.get("_i__ibfpay_rejected", "")).strip().lower() == "true"
            if ibfpay_avail else False
        )

        # SUPERPAY 命中判断（join 键：admin.订单号 ↔ superpay.商户订单号）
        sp_amt    = str(row.get(f"_s_{SUPERPAY_AMOUNT_COL_5}",     "")).strip() if superpay_avail else ""
        sp_no     = str(row.get(f"_s_{SUPERPAY_PLATFORM_NO_COL_5}","")).strip() if superpay_avail else ""
        sp_fee    = str(row.get(f"_s_{SUPERPAY_FEE_TOTAL_COL_5}",  "")).strip() if superpay_avail else ""
        sp_actual = str(row.get(f"_s_{SUPERPAY_ACTUAL_COL_5}",     "")).strip() if superpay_avail else ""
        sp_currency = _normalize_currency_5(row.get(f"_s_{SUPERPAY_CURRENCY_COL_5}", "")) if superpay_avail else ""
        sp_status = str(row.get(f"_s_{SUPERPAY_STATUS_COL_5}",     "")).strip() if superpay_avail else ""
        sp_time   = str(row.get(f"_s_{SUPERPAY_FINISH_TIME_COL_5}","")).strip() if superpay_avail else ""
        sp_ctime  = str(row.get(f"_s_{SUPERPAY_CREATE_TIME_COL_5}","")).strip() if superpay_avail else ""
        sp_hit    = sp_amt != ""

        # WANGGUYPAY 命中判断（join 键：admin.第三方订单号 ↔ wangguypay.平台订单号）
        wg_amt    = str(row.get(f"_w_{WANGGUYPAY_AMOUNT_COL_5}",    "")).strip() if wangguypay_avail else ""
        wg_no     = str(row.get(f"_w_{WANGGUYPAY_PLATFORM_NO_COL_5}","")).strip() if wangguypay_avail else ""
        wg_fee    = str(row.get(f"_w_{WANGGUYPAY_FEE_COL_5}",        "")).strip() if wangguypay_avail else ""
        wg_arrive = str(row.get(f"_w_{WANGGUYPAY_ARRIVE_COL_5}",     "")).strip() if wangguypay_avail else ""
        wg_status = str(row.get(f"_w_{WANGGUYPAY_STATUS_COL_5}",     "")).strip() if wangguypay_avail else ""
        wg_time   = str(row.get(f"_w_{WANGGUYPAY_FINISH_TIME_COL_5}","")).strip() if wangguypay_avail else ""
        wg_ctime  = str(row.get(f"_w_{WANGGUYPAY_CREATE_TIME_COL_5}","")).strip() if wangguypay_avail else ""
        wg_hit    = wg_amt != ""

        # 话费卡命中判断（join 键：admin.订单号 ↔ 话费卡.订单号）
        pc_amt    = str(row.get(f"_p_{PHONECARD_AMOUNT_COL_5}",      "")).strip() if phonecard_avail else ""
        pc_no     = str(row.get(f"_p_{PHONECARD_PLATFORM_NO_COL_5}", "")).strip() if phonecard_avail else ""
        pc_status = str(row.get(f"_p_{PHONECARD_STATUS_COL_5}",      "")).strip() if phonecard_avail else ""
        pc_time   = str(row.get(f"_p_{PHONECARD_DATE_COL_5}",        "")).strip() if phonecard_avail else ""
        pc_hit    = pc_amt != ""

        # epin 命中判断（仅对机构为纯数字的行；join 键待 build_epin_lookup_5 实现后生效）
        org_val  = str(row.get(ADMIN_ORG_COL_5, "")).strip()
        is_epin_candidate = bool(re.match(EPIN_ORG_PATTERN_5, org_val)) if org_val else False
        # TODO: epin_lk 目前恒为 None，ep_hit 恒为 False；build_epin_lookup_5 实现后此处自动生效
        ep_amt   = str(row.get(f"_e_{EPIN_SIPARISLER_UNIT_PRICE_COL_5}", "")).strip() if (epin_avail and is_epin_candidate) else ""
        ep_hit   = ep_amt != ""

        if ibf_hit and (sp_hit or wg_hit or pc_hit):
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
            platform_currency_list.append(IBFYPAY_DEFAULT_CURRENCY_5)
            platform_status_list.append(_normalize_platform_status_5("IBFYPAY", rejected=ibf_rejected))
            fee_list.append(ibf_fee)
            arrive_amount_list.append(str(arrive))
            transaction_date_list.append(_format_date_5(ibf_time))
            org_output_list.append(row.get(ADMIN_ORG_COL_5, ""))
        elif sp_hit:
            sp_amt_f    = _to_float_5(sp_amt) or 0.0
            sp_actual_f = _to_float_5(sp_actual) or 0.0
            sp_calc_fee = str(round(abs(sp_amt_f - sp_actual_f), 2))
            match_status_list.append("是")
            platform_order_no_list.append(sp_no)
            platform_amount_list.append(sp_amt)
            platform_currency_list.append(sp_currency)
            platform_status_list.append(_normalize_platform_status_5("SUPERPAY", sp_status))
            fee_list.append(sp_calc_fee)
            arrive_amount_list.append(sp_actual)
            transaction_date_list.append(_format_date_5(sp_time) or _format_date_5(sp_ctime))
            org_output_list.append(row.get(ADMIN_ORG_COL_5, ""))
        elif wg_hit:
            match_status_list.append("是")
            platform_order_no_list.append(wg_no)
            platform_amount_list.append(wg_amt)
            platform_currency_list.append(WANGGUYPAY_DEFAULT_CURRENCY_5)
            platform_status_list.append(_normalize_platform_status_5("WANGGUYPAY", wg_status))
            fee_list.append(wg_fee)
            arrive_amount_list.append(wg_arrive)
            transaction_date_list.append(_format_date_5(wg_time) or _format_date_5(wg_ctime))
            org_output_list.append(row.get(ADMIN_ORG_COL_5, ""))
        elif pc_hit:
            match_status_list.append("是")
            platform_order_no_list.append(pc_no)
            platform_amount_list.append(pc_amt)
            platform_currency_list.append("")
            platform_status_list.append(_normalize_platform_status_5("PHONECARD", pc_status))
            fee_list.append("")
            arrive_amount_list.append(pc_amt)
            transaction_date_list.append(_format_date_5(pc_time))
            org_output_list.append(PHONECARD_PLATFORM_NAME_5)
        elif ep_hit:
            ep_pin_id  = str(row.get(f"_e_{EPIN_PINLER_PIN_ID_COL_5}",          "")).strip() if epin_avail else ""
            ep_status  = str(row.get(f"_e_{EPIN_SIPARISLER_STATUS_COL_5}",       "")).strip() if epin_avail else ""
            ep_time    = str(row.get(f"_e_{EPIN_SIPARISLER_CONFIRM_TIME_COL_5}", "")).strip() if epin_avail else ""
            match_status_list.append("是")
            platform_order_no_list.append(ep_pin_id)
            platform_amount_list.append(ep_amt)
            platform_currency_list.append(EPIN_DEFAULT_CURRENCY_5)
            platform_status_list.append(_normalize_platform_status_5(EPIN_PLATFORM_NAME_5, ep_status))
            fee_list.append("")
            arrive_amount_list.append(ep_amt)
            transaction_date_list.append(_format_date_5(ep_time))
            org_output_list.append(EPIN_PLATFORM_NAME_5)
        else:
            match_status_list.append("否")
            platform_order_no_list.append("")
            platform_amount_list.append("")
            platform_currency_list.append("")
            platform_status_list.append("")
            fee_list.append("")
            arrive_amount_list.append("")
            transaction_date_list.append("")
            org_output_list.append(row.get(ADMIN_ORG_COL_5, ""))

    # 还原为仅 admin 原始列，再追加新增列
    admin_cols = list(admin_df.columns)
    result = result[admin_cols].copy()
    if ADMIN_ORG_COL_5 in result.columns:
        result[ADMIN_ORG_COL_5] = org_output_list

    result[MATCH_STATUS_COL_5]      = match_status_list
    result[PLATFORM_ORDER_NO_COL_5] = platform_order_no_list
    result[PLATFORM_AMOUNT_COL_5]   = platform_amount_list
    result[PLATFORM_CURRENCY_COL_5] = platform_currency_list
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
    admin_wangguypay_keys: Set[str] = (
        {str(v).strip() for v in admin_df[ADMIN_TP_ORDER_COL_5] if str(v).strip()}
        if ADMIN_TP_ORDER_COL_5 in admin_df.columns else set()
    )

    result_cols = list(result.columns)
    extra_df = _build_platform_only_rows_5(
        result_cols,
        admin_ibfpay_keys,
        admin_order_keys,
        admin_wangguypay_keys,
        ibfpay_lk if ibfpay_avail else None,
        superpay_lk if superpay_avail else None,
        wangguypay_lk if wangguypay_avail else None,
        phonecard_lk if phonecard_avail else None,
        epin_lk if epin_avail else None,
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


def _build_one_platform_balance_summary_5(
    df: Optional[pd.DataFrame],
    *,
    platform: str,
    currency: str,
    type_col: str,
    begin_col: str,
    change_col: str,
    end_col: str,
    time_cols: List[str],
) -> pd.DataFrame:
    """按月提取单个平台资金流水的期初、充值和平台期末余额。"""
    output_cols = ["交易月份", "机构", PLATFORM_CURRENCY_COL_5] + SUMMARY_BALANCE_COLS_5
    if df is None or df.empty:
        return pd.DataFrame(columns=output_cols)

    required = [type_col, begin_col, change_col, end_col]
    if not all(c in df.columns for c in required):
        return pd.DataFrame(columns=output_cols)

    work = df.copy()
    existing_time_cols = [c for c in time_cols if c in work.columns]
    if not existing_time_cols:
        return pd.DataFrame(columns=output_cols)

    work["_balance_time"] = pd.NaT
    for c in existing_time_cols:
        parsed = work[c].apply(_to_datetime_5)
        work["_balance_time"] = work["_balance_time"].fillna(parsed)
    work = work[~work["_balance_time"].isna()].copy()
    if work.empty:
        return pd.DataFrame(columns=output_cols)

    work["_month"] = work["_balance_time"].dt.strftime("%Y-%m")
    work["_begin"] = work[begin_col].apply(_to_float_5)
    work["_change"] = work[change_col].apply(_to_float_5)
    work["_end"] = work[end_col].apply(_to_float_5)
    work["_is_recharge"] = work[type_col].apply(_is_recharge_type_5)
    work["_is_withdrawal"] = work[type_col].apply(_is_withdrawal_type_5)
    if platform == "IBFYPAY":
        ibfpay_system = work[type_col].astype(str).str.strip().eq(IBFYPAY_TYPE_SYSTEM_5)
        work["_is_recharge"] = work["_is_recharge"] | (
            ibfpay_system & (work["_change"].fillna(0.0) > 0)
        )
        work["_is_withdrawal"] = work["_is_withdrawal"] | (
            ibfpay_system & (work["_change"].fillna(0.0) < 0)
        )
    work = work.sort_values("_balance_time", kind="mergesort")

    rows = []
    for month, grp in work.groupby("_month", sort=True):
        begin = _pick_chain_begin_5(grp)
        end = _pick_chain_end_5(grp)
        recharge = grp.loc[
            grp["_is_recharge"] & (grp["_change"].fillna(0.0) > 0),
            "_change",
        ].sum()
        withdrawal = abs(grp.loc[
            grp["_is_withdrawal"],
            "_change",
        ].fillna(0.0).sum())
        rows.append({
            "交易月份": month,
            "机构": platform,
            PLATFORM_CURRENCY_COL_5: currency,
            SUMMARY_BEGIN_BALANCE_COL_5: round(begin, 2) if begin is not None else "",
            SUMMARY_RECHARGE_COL_5: round(float(recharge), 2),
            SUMMARY_WITHDRAWAL_COL_5: round(float(withdrawal), 2),
            SUMMARY_CALC_END_BALANCE_COL_5: "",
            SUMMARY_PLATFORM_END_BALANCE_COL_5: round(end, 2) if end is not None else "",
        })
    return pd.DataFrame(rows, columns=output_cols)


def build_platform_balance_summary_5(
    ibfpay_raw: Optional[pd.DataFrame] = None,
    wangguypay_raw: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """构建平台汇总可合并的余额数据。SUPERPAY 当前无余额源列，故不产生余额行。"""
    frames = [
        _build_one_platform_balance_summary_5(
            ibfpay_raw,
            platform="IBFYPAY",
            currency=IBFYPAY_DEFAULT_CURRENCY_5,
            type_col=IBFYPAY_TYPE_COL_5,
            begin_col=IBFYPAY_BEGIN_AMOUNT_COL_5,
            change_col=IBFYPAY_AMOUNT_COL_5,
            end_col=IBFYPAY_END_AMOUNT_COL_5,
            time_cols=[IBFYPAY_TIME_COL_5],
        ),
        _build_one_platform_balance_summary_5(
            wangguypay_raw,
            platform="WANGGUYPAY",
            currency=WANGGUYPAY_DEFAULT_CURRENCY_5,
            type_col=WANGGUYPAY_FUND_TYPE_COL_5,
            begin_col=WANGGUYPAY_BEGIN_AMOUNT_COL_5,
            change_col=WANGGUYPAY_FUND_AMOUNT_COL_5,
            end_col=WANGGUYPAY_END_AMOUNT_COL_5,
            time_cols=[WANGGUYPAY_FINISH_TIME_COL_5, WANGGUYPAY_CREATE_TIME_COL_5],
        ),
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    cols = ["交易月份", "机构", PLATFORM_CURRENCY_COL_5] + SUMMARY_BALANCE_COLS_5
    if not frames:
        return pd.DataFrame(columns=cols)
    return pd.concat(frames, ignore_index=True)[cols]


def build_summary_sheet_5(
    result_df: pd.DataFrame,
    platform_balance_summary: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """按交易月份、平台和币种汇总代付金额、手续费、到账金额。

    分组键：交易日期归属月份（YYYY-MM） + 机构列（ADMIN_ORG_COL_5）+ 币种。
    汇总列：成功笔数、代付金额合计、手续费合计、到账金额合计。
    仅统计平台状态为 "成功" 的行；交易日期为空或无法解析时，交易月份保留为空字符串。

    Args:
        result_df: enrich_admin_5 返回的完整结果 DataFrame。

    Returns:
        汇总 DataFrame，供写入 OUTPUT_SUMMARY_SHEET_5。
    """
    month_col = "交易月份"
    summary_cols = [
        month_col,
        "机构",
        PLATFORM_CURRENCY_COL_5,
        "笔数",
        SUMMARY_BEGIN_BALANCE_COL_5,
        "代付金额合计",
        "手续费合计",
        "到账金额合计",
        SUMMARY_RECHARGE_COL_5,
        SUMMARY_WITHDRAWAL_COL_5,
        SUMMARY_CALC_END_BALANCE_COL_5,
        SUMMARY_PLATFORM_END_BALANCE_COL_5,
    ]
    if PLATFORM_STATUS_COL_5 not in result_df.columns:
        return pd.DataFrame(columns=summary_cols)
    matched = result_df[result_df[PLATFORM_STATUS_COL_5].astype(str).str.strip() == "成功"].copy()
    if matched.empty or ADMIN_ORG_COL_5 not in matched.columns:
        return pd.DataFrame(columns=summary_cols)

    if TRANSACTION_DATE_COL_5 in matched.columns:
        matched[month_col] = matched[TRANSACTION_DATE_COL_5].apply(
            lambda v: (_format_date_5(v)[:7] if _format_date_5(v) else "")
        )
    else:
        matched[month_col] = ""

    matched["_amt"]    = matched[PLATFORM_AMOUNT_COL_5].apply(lambda v: _to_float_5(v) or 0.0)
    matched["_fee"]    = matched[FEE_COL_5].apply(lambda v: _to_float_5(v) or 0.0)
    matched["_arrive"] = matched[ARRIVE_AMOUNT_COL_5].apply(lambda v: _to_float_5(v) or 0.0)
    if PLATFORM_CURRENCY_COL_5 in matched.columns:
        matched[PLATFORM_CURRENCY_COL_5] = matched[PLATFORM_CURRENCY_COL_5].apply(_normalize_currency_5)
    else:
        matched[PLATFORM_CURRENCY_COL_5] = ""

    grp = matched.groupby([month_col, ADMIN_ORG_COL_5, PLATFORM_CURRENCY_COL_5], as_index=False).agg(
        笔数=("_amt", "count"),
        代付金额合计=("_amt", "sum"),
        手续费合计=("_fee", "sum"),
        到账金额合计=("_arrive", "sum"),
    )
    grp = grp.rename(columns={ADMIN_ORG_COL_5: "机构"})
    for col in ["代付金额合计", "手续费合计", "到账金额合计"]:
        grp[col] = grp[col].round(2)

    for col in SUMMARY_BALANCE_COLS_5:
        grp[col] = ""
    if platform_balance_summary is not None and not platform_balance_summary.empty:
        balance_cols = [month_col, "机构", PLATFORM_CURRENCY_COL_5] + SUMMARY_BALANCE_COLS_5
        balance_df = platform_balance_summary.copy()
        if PLATFORM_CURRENCY_COL_5 in balance_df.columns:
            balance_df[PLATFORM_CURRENCY_COL_5] = balance_df[PLATFORM_CURRENCY_COL_5].apply(_normalize_currency_5)
        else:
            balance_df[PLATFORM_CURRENCY_COL_5] = ""
        balance_df = balance_df[[c for c in balance_cols if c in balance_df.columns]]
        grp = grp.drop(columns=SUMMARY_BALANCE_COLS_5).merge(
            balance_df,
            on=[month_col, "机构", PLATFORM_CURRENCY_COL_5],
            how="left",
        )
        for col in SUMMARY_BALANCE_COLS_5:
            if col not in grp.columns:
                grp[col] = ""
            else:
                grp[col] = grp[col].fillna("")

    calc_mask = grp[SUMMARY_BEGIN_BALANCE_COL_5].astype(str).str.strip() != ""
    grp.loc[calc_mask, SUMMARY_CALC_END_BALANCE_COL_5] = (
        grp.loc[calc_mask, SUMMARY_BEGIN_BALANCE_COL_5].astype(float)
        + grp.loc[calc_mask, SUMMARY_RECHARGE_COL_5].replace("", 0).astype(float)
        - grp.loc[calc_mask, SUMMARY_WITHDRAWAL_COL_5].replace("", 0).astype(float)
        - grp.loc[calc_mask, "代付金额合计"].astype(float)
        - grp.loc[calc_mask, "手续费合计"].astype(float)
    ).round(2)
    return grp[summary_cols]


# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------

def write_output_5(
    result_df: pd.DataFrame,
    output_dir: Path,
    platform_balance_summary: Optional[pd.DataFrame] = None,
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
    summary_df = build_summary_sheet_5(result_df, platform_balance_summary)

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
    ibfpay_balance_raw: Optional[pd.DataFrame] = None
    if files["ibfpay"]:
        frames = [read_ibfpay_5(fp) for fp in files["ibfpay"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        ibfpay_lk = build_ibfpay_lookup_5(raw)
        balance_frames = [read_ibfpay_balance_source_5(fp) for fp in files["ibfpay"]]
        balance_frames = [f for f in balance_frames if not f.empty]
        if balance_frames:
            ibfpay_balance_raw = pd.concat(balance_frames, ignore_index=True)
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
    wangguypay_raw: Optional[pd.DataFrame] = None
    if files["wangguypay"]:
        frames = [read_wangguypay_5(fp) for fp in files["wangguypay"]]
        wangguypay_raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        wangguypay_lk = build_wangguypay_lookup_5(wangguypay_raw)
        logging.info("WANGGUYPAY 查找表共 %d 条", len(wangguypay_lk))

    # ── 话费卡 ────────────────────────────────────────────────
    phonecard_lk: Optional[pd.DataFrame] = None
    if files["phonecard"]:
        frames = [read_phonecard_5(fp) for fp in files["phonecard"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        phonecard_lk = build_phonecard_lookup_5(raw)
        logging.info("话费卡 查找表共 %d 条", len(phonecard_lk))

    # ── epin ──────────────────────────────────────────────────
    epin_siparisler_df: Optional[pd.DataFrame] = None
    epin_pinler_df: Optional[pd.DataFrame] = None
    epin_lk: Optional[pd.DataFrame] = None
    if files.get("epin_siparisler"):
        frames = [read_epin_siparisler_5(fp) for fp in files["epin_siparisler"]]
        epin_siparisler_df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        logging.info("EPIN 订单列表共 %d 行", len(epin_siparisler_df))
    if files.get("epin_pinler"):
        frames = [read_epin_pinler_5(fp) for fp in files["epin_pinler"]]
        epin_pinler_df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        logging.info("EPIN pin 码列表共 %d 行", len(epin_pinler_df))
    if epin_siparisler_df is not None and epin_pinler_df is not None:
        epin_lk = build_epin_lookup_5(epin_siparisler_df, epin_pinler_df)

    platform_balance_summary = build_platform_balance_summary_5(ibfpay_balance_raw, wangguypay_raw)

    # ── 匹配 ──────────────────────────────────────────────────
    result_df = enrich_admin_5(admin_df, ibfpay_lk, superpay_lk, wangguypay_lk, phonecard_lk, epin_lk)
    log_match_stats_5(result_df)

    # ── 输出 ──────────────────────────────────────────────────
    try:
        output_path = write_output_5(result_df, OUTPUT_DIR, platform_balance_summary)
        logging.info("完成。输出文件: %s", output_path)
    except PermissionError:
        logging.error("无法写入输出文件，请确认文件未在 Excel 中打开后重试。")
        return 1
    except Exception:
        logging.error("写入输出文件失败", exc_info=True)
        return 1

    return 0
