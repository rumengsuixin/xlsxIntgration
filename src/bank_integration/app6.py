"""代号6（代收代付对账）应用逻辑。

数据流：
    data/input/6/
        Admin收款订单明细*.xlsx   → 代收主表
        Admin兑换订单明细*.xlsx   → 代付主表
        betcat-payment_*.csv      → Betcat 代收（多文件合并）
        betcat-payout_*.csv       → Betcat 代付（多文件合并）
        Cashnewpay收款明细*.xlsx  → Cashnewpay 代收
        Cashnewpay兑换明细*.xlsx  → Cashnewpay 代付
                  ↓
        scan_source_files_6()
                  ↓
        read_admin_collection_6 / read_admin_payout_6
        read_betcat_csv_6 / read_cashnewpay_xlsx_6
                  ↓
        build_betcat_lookup_6 / build_cashnewpay_lookup_6
                  ↓
        enrich_admin_6()  ← left-join 查找表（代收和代付各调用一次）
                  ↓
        data/output/代收代付对账结果_{YYYYMMDD}.xlsx（5 个 sheet）

对账逻辑：
    代收：Admin收款.订单号 ↔ Betcat.MerOrderNo / Cashnewpay.商户订单号
    代付：Admin兑换.订单号 ↔ Betcat.MerOrderNo / Cashnewpay.商户订单号
    匹配优先级：Betcat > Cashnewpay
"""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from .config import OUTPUT_DIR
from .config6 import (
    CASHNEWPAY_STATUS_PREFIX_MAP_6,
    ADMIN_COLLECTION_DATE_COL_6,
    ADMIN_COLLECTION_JOIN_COL_6,
    ADMIN_COLLECTION_SHEET_6,
    ADMIN_PAYOUT_DATE_COL_6,
    ADMIN_PAYOUT_JOIN_COL_6,
    ADMIN_PAYOUT_SHEET_6,
    BETCAT_AMOUNT_COL_6,
    BETCAT_CREATE_TIME_COL_6,
    BETCAT_FEE_COL_6,
    BETCAT_JOIN_COL_6,
    BETCAT_PAY_TIME_COL_6,
    BETCAT_PLATFORM_NO_COL_6,
    BETCAT_STATUS_COL_6,
    CASHNEWPAY_AMOUNT_COL_6,
    CASHNEWPAY_CREATE_TIME_COL_6,
    CASHNEWPAY_FEE_COL_6,
    CASHNEWPAY_FINISH_TIME_COL_6,
    CASHNEWPAY_JOIN_COL_6,
    CASHNEWPAY_PLATFORM_NO_COL_6,
    CASHNEWPAY_SHEET_6,
    CASHNEWPAY_STATE_DESC_COL_6,
    CASHNEWPAY_STATUS_COL_6,
    FEE_COL_6,
    INPUT_DIR_6,
    MATCH_STATUS_COL_6,
    OUTPUT_AMOUNT_DIFF_SHEET_6,
    OUTPUT_COLLECTION_FAILED_SHEET_6,
    OUTPUT_COLLECTION_SHEET_6,
    OUTPUT_FILE_TEMPLATE_6,
    OUTPUT_NEW_COLS_6,
    OUTPUT_PAYOUT_FAILED_SHEET_6,
    OUTPUT_PAYOUT_SHEET_6,
    OUTPUT_SUMMARY_SHEET_6,
    PLATFORM_ORDER_NO_COL_6,
    PLATFORM_PREFIXES_6,
    PLATFORM_SOURCE_COL_6,
    PLATFORM_STATUS_COL_6,
    PLATFORM_AMOUNT_COL_6,
    PLATFORM_STATUS_MAP_6,
    SUMMARY_AMOUNT_COL_6,
    SUMMARY_ARRIVE_COL_6,
    SUMMARY_COUNT_COL_6,
    SUMMARY_FEE_COL_6,
    SUMMARY_MONTH_COL_6,
    SUMMARY_PLATFORM_COL_6,
    SUMMARY_TYPE_COL_6,
    TRANSACTION_DATE_COL_6,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_columns_6(columns) -> Set[str]:
    """去除列名首尾空格，返回规范化集合。"""
    return {str(c).strip() for c in columns}


def _format_date_6(val) -> str:
    """将任意日期值格式化为 YYYY-MM-DD，失败返回空串。

    支持：
    - pandas Timestamp / datetime
    - ISO 8601 带时区（如 2026-04-01T00:07:49-03:00）
    - 含毫秒字符串（如 2026-04-28 08:30:03.313）
    - YYYY-MM-DD / YYYY/MM/DD（含时间变体）
    """
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    try:
        s = str(val).strip()
        if not s or s.lower() in ("nan", "none", "nat"):
            return ""
        parsed = pd.to_datetime(s, errors="coerce", utc=True)
        if not pd.isna(parsed):
            return parsed.strftime("%Y-%m-%d")
        # 兜底：直接取前10字符（已是 YYYY-MM-DD 格式时）
        if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
            return s[:10]
        return ""
    except Exception:
        return ""


def _to_float_6(val) -> Optional[float]:
    """将字符串金额转为 float，支持千分位逗号，失败返回 None。"""
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _normalize_platform_status_6(platform: str, raw_status: str = "") -> str:
    """将各平台原始状态统一为：成功 / 失败 / 处理中 / 关闭。

    使用 PLATFORM_STATUS_MAP_6 字典进行精确映射；
    Cashnewpay 再额外尝试 CASHNEWPAY_STATUS_PREFIX_MAP_6 前缀匹配（如 USER_REFUND-...）；
    均未命中时打 warning 并保留原文。
    """
    platform_key = str(platform).strip().upper()
    status = str(raw_status).strip()
    if not status:
        return ""
    mapping = PLATFORM_STATUS_MAP_6.get(platform_key, {})
    normalized = mapping.get(status)
    if normalized is not None:
        return normalized
    # Cashnewpay 前缀匹配（处理含动态后缀的状态，如 USER_REFUND-<单号>）
    if platform_key == "CASHNEWPAY":
        for prefix, mapped in CASHNEWPAY_STATUS_PREFIX_MAP_6.items():
            if status.startswith(prefix):
                return mapped
    logger.warning("【%s】发现未识别平台状态 '%s'，保留原值", platform_key, status)
    return status


# ─────────────────────────────────────────────────────────────────────────────
# 文件扫描
# ─────────────────────────────────────────────────────────────────────────────

def scan_source_files_6(input_dir: Path) -> Dict[str, List[Path]]:
    """扫描输入目录，按 PLATFORM_PREFIXES_6 识别文件。

    返回结构：
    {
        "admin_collection":    [Path, ...],
        "admin_payout":        [Path, ...],
        "betcat_payment":      [Path, ...],   # 可能多个 CSV
        "betcat_payout":       [Path, ...],   # 可能多个 CSV
        "cashnewpay_collection": [Path, ...],
        "cashnewpay_exchange": [Path, ...],
    }

    规则：
    - 跳过 ~$ 开头的临时文件
    - 扩展名：.xlsx / .xls / .csv
    - stem 小写后 startswith 任意前缀即命中
    """
    result: Dict[str, List[Path]] = {key: [] for key in PLATFORM_PREFIXES_6}

    if not input_dir.exists():
        logger.warning("输入目录不存在: %s", input_dir)
        return result

    for f in sorted(input_dir.iterdir()):
        if not f.is_file() or f.name.startswith("~$"):
            continue
        if f.suffix.lower() not in {".xls", ".xlsx", ".csv"}:
            continue
        stem = f.stem.lower()
        matched = False
        for platform_key, prefixes in PLATFORM_PREFIXES_6.items():
            if any(stem.startswith(p) for p in prefixes):
                result[platform_key].append(f)
                matched = True
                break
        if not matched:
            logger.warning("未识别文件，已跳过: %s", f.name)

    for key, files in result.items():
        if len(files) > 1:
            logger.info("%s 发现 %d 个文件，将合并: %s", key, len(files), [f.name for f in files])

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 平台文件读取
# ─────────────────────────────────────────────────────────────────────────────

def read_admin_collection_6(filepath: Path) -> pd.DataFrame:
    """读取 Admin 收款订单主表。

    格式：xlsx，engine=openpyxl，sheet=ADMIN_COLLECTION_SHEET_6（"已完成订单"）
    """
    with pd.ExcelFile(filepath, engine="openpyxl") as xls:
        sheet_names = xls.sheet_names
        if ADMIN_COLLECTION_SHEET_6 in sheet_names:
            target = ADMIN_COLLECTION_SHEET_6
        else:
            # 回退：查找含 ADMIN_COLLECTION_JOIN_COL_6 列的 sheet
            target = None
            for s in sheet_names:
                try:
                    preview = pd.read_excel(xls, sheet_name=s, nrows=0, dtype=str)
                    if ADMIN_COLLECTION_JOIN_COL_6 in _normalize_columns_6(preview.columns):
                        target = s
                        break
                except Exception:
                    continue
            if target is None:
                raise ValueError(
                    f"admin 收款文件 {filepath.name} 中找不到 sheet '{ADMIN_COLLECTION_SHEET_6}' "
                    f"或含 '{ADMIN_COLLECTION_JOIN_COL_6}' 列的 sheet，可用 sheet: {sheet_names}"
                )
            logger.warning(
                "admin 收款文件未找到 sheet '%s'，回退使用 sheet '%s'",
                ADMIN_COLLECTION_SHEET_6, target,
            )
    df = pd.read_excel(filepath, sheet_name=target, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").fillna("")
    # Excel 文本前缀单引号（防科学计数法）：openpyxl 会原样保留 '，需清除
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.lstrip("'")
    return df


def read_admin_payout_6(filepath: Path) -> pd.DataFrame:
    """读取 Admin 兑换订单主表。

    格式：xlsx，engine=openpyxl，sheet=ADMIN_PAYOUT_SHEET_6（"Sheet1"）
    """
    with pd.ExcelFile(filepath, engine="openpyxl") as xls:
        sheet_names = xls.sheet_names
        if ADMIN_PAYOUT_SHEET_6 in sheet_names:
            target = ADMIN_PAYOUT_SHEET_6
        else:
            target = None
            for s in sheet_names:
                try:
                    preview = pd.read_excel(xls, sheet_name=s, nrows=0, dtype=str)
                    if ADMIN_PAYOUT_JOIN_COL_6 in _normalize_columns_6(preview.columns):
                        target = s
                        break
                except Exception:
                    continue
            if target is None:
                raise ValueError(
                    f"admin 兑换文件 {filepath.name} 中找不到 sheet '{ADMIN_PAYOUT_SHEET_6}' "
                    f"或含 '{ADMIN_PAYOUT_JOIN_COL_6}' 列的 sheet，可用 sheet: {sheet_names}"
                )
            logger.warning(
                "admin 兑换文件未找到 sheet '%s'，回退使用 sheet '%s'",
                ADMIN_PAYOUT_SHEET_6, target,
            )
    df = pd.read_excel(filepath, sheet_name=target, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").fillna("")
    # Excel 文本前缀单引号（防科学计数法）：openpyxl 会原样保留 '，需清除
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.lstrip("'")
    return df


def read_betcat_csv_6(filepath: Path) -> pd.DataFrame:
    """读取 Betcat 平台 CSV 文件（payment 和 payout 格式相同）。

    列含：CreateTime / OrderNo / MerOrderNo / ChannelOrderNo / ChannelTradeNo /
         Amount / Currency / Status / TradeCharge / PayTime
    时间：ISO 8601 带时区（如 2026-04-01T00:07:49-03:00），保留为字符串
    编码：平台导出编码不稳定（混有 UTF-8/GBK），依次尝试 utf-8-sig / gbk / gb18030 / utf-8
    """
    df = None
    for enc in ("utf-8-sig", "gbk", "gb18030", "utf-8"):
        try:
            df = pd.read_csv(filepath, dtype=str, keep_default_na=False, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        raise UnicodeError(
            f"无法解码 Betcat CSV 文件: {filepath.name}（已尝试 utf-8-sig/gbk/gb18030/utf-8）"
        )
    df.columns = [str(c).strip() for c in df.columns]
    return df.dropna(how="all").fillna("")


def read_cashnewpay_xlsx_6(filepath: Path) -> pd.DataFrame:
    """读取 Cashnewpay 平台 xlsx 文件（收款明细和兑换明细格式相同）。

    格式：xlsx，engine=openpyxl，sheet=CASHNEWPAY_SHEET_6（"Sheet1"）
    列含：18 列，含商户订单号 / 订单金额 / 手续费 / 订单状态 / 创建时间 / 完成时间 等
    """
    with pd.ExcelFile(filepath, engine="openpyxl") as xls:
        sheet_names = xls.sheet_names
        if CASHNEWPAY_SHEET_6 in sheet_names:
            target = CASHNEWPAY_SHEET_6
        else:
            target = sheet_names[0]
            logger.warning(
                "Cashnewpay 文件未找到 sheet '%s'，使用首个 sheet '%s'",
                CASHNEWPAY_SHEET_6, target,
            )
    df = pd.read_excel(filepath, sheet_name=target, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    return df.dropna(how="all").fillna("")


# ─────────────────────────────────────────────────────────────────────────────
# 去重工具
# ─────────────────────────────────────────────────────────────────────────────

def _dedup_lookup_6(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    """过滤空 key 行，对 key_col 去重（保留首行），发现重复时打 warning。"""
    df = df[df[key_col].astype(str).str.strip() != ""].copy()
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    dupes = before - len(df)
    if dupes > 0:
        logger.warning("【%s】去重时发现 %d 条重复订单号，已保留首行", label, dupes)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 查找表构建
# ─────────────────────────────────────────────────────────────────────────────

def build_betcat_lookup_6(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 BETCAT_JOIN_COL_6（MerOrderNo）为索引的 Betcat 查找表。

    保留：OrderNo / Amount / Status / TradeCharge / PayTime / CreateTime
    """
    keep_cols = [c for c in [
        BETCAT_JOIN_COL_6,
        BETCAT_PLATFORM_NO_COL_6,
        BETCAT_AMOUNT_COL_6,
        BETCAT_STATUS_COL_6,
        BETCAT_FEE_COL_6,
        BETCAT_PAY_TIME_COL_6,
        BETCAT_CREATE_TIME_COL_6,
    ] if c in df.columns]

    if BETCAT_JOIN_COL_6 not in keep_cols:
        logger.warning("【BETCAT】缺少关联列 '%s'，返回空查找表", BETCAT_JOIN_COL_6)
        return pd.DataFrame()

    result = _dedup_lookup_6(df[keep_cols], BETCAT_JOIN_COL_6, "BETCAT")
    return result.set_index(BETCAT_JOIN_COL_6)


def build_cashnewpay_lookup_6(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 CASHNEWPAY_JOIN_COL_6（商户订单号）为索引的 Cashnewpay 查找表。

    保留：订单号(平台内部) / 订单金额 / 手续费 / 订单状态 / 完成时间 / 创建时间 / 状态描述
    """
    keep_cols = [c for c in [
        CASHNEWPAY_JOIN_COL_6,
        CASHNEWPAY_PLATFORM_NO_COL_6,
        CASHNEWPAY_AMOUNT_COL_6,
        CASHNEWPAY_FEE_COL_6,
        CASHNEWPAY_STATUS_COL_6,
        CASHNEWPAY_FINISH_TIME_COL_6,
        CASHNEWPAY_CREATE_TIME_COL_6,
        CASHNEWPAY_STATE_DESC_COL_6,
    ] if c in df.columns]

    if CASHNEWPAY_JOIN_COL_6 not in keep_cols:
        logger.warning("【CASHNEWPAY】缺少关联列 '%s'，返回空查找表", CASHNEWPAY_JOIN_COL_6)
        return pd.DataFrame()

    result = _dedup_lookup_6(df[keep_cols], CASHNEWPAY_JOIN_COL_6, "CASHNEWPAY")
    return result.set_index(CASHNEWPAY_JOIN_COL_6)


# ─────────────────────────────────────────────────────────────────────────────
# 主匹配逻辑
# ─────────────────────────────────────────────────────────────────────────────

def enrich_admin_6(
    admin_df: pd.DataFrame,
    betcat_lk: Optional[pd.DataFrame],
    cashnewpay_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """以 admin 为主表，left-join 两个查找表，追加 OUTPUT_NEW_COLS_6。

    代收和代付均调用此函数（join key 相同：Admin.订单号 ↔ 平台.MerOrderNo/商户订单号）

    匹配优先级：Betcat > Cashnewpay
    """
    result = admin_df.copy()
    expected_rows = len(admin_df)

    def _safe_merge(base, lookup, left_on, prefix, label):
        if left_on not in base.columns:
            logger.warning("【%s】admin 文件中缺少关联列 '%s'，跳过该平台匹配", label, left_on)
            return base
        merged = base.merge(
            lookup.add_prefix(prefix),
            left_on=left_on, right_index=True, how="left",
        )
        if len(merged) != expected_rows:
            raise ValueError(
                f"【{label}】merge 后行数从 {expected_rows} 变为 {len(merged)}，"
                f"请检查 build_{label.lower()}_lookup_6 去重逻辑"
            )
        for c in lookup.columns:
            merged[f"{prefix}{c}"] = merged[f"{prefix}{c}"].fillna("")
        return merged

    betcat_avail     = betcat_lk is not None     and not betcat_lk.empty
    cashnewpay_avail = cashnewpay_lk is not None and not cashnewpay_lk.empty

    # 两个平台使用相同的 admin 侧关联键
    admin_join_col = (
        ADMIN_COLLECTION_JOIN_COL_6
        if ADMIN_COLLECTION_JOIN_COL_6 in admin_df.columns
        else ADMIN_PAYOUT_JOIN_COL_6
    )

    if betcat_avail:
        result = _safe_merge(result, betcat_lk, admin_join_col, "_b_", "BETCAT")
    if cashnewpay_avail:
        result = _safe_merge(result, cashnewpay_lk, admin_join_col, "_c_", "CASHNEWPAY")

    match_status_list      = []
    platform_source_list   = []
    platform_order_no_list = []
    platform_amount_list   = []
    platform_status_list   = []
    fee_list               = []
    transaction_date_list  = []

    for _, row in result.iterrows():
        # ── Betcat 命中判断 ───────────────────────────────────────────────────
        b_amt    = str(row.get(f"_b_{BETCAT_AMOUNT_COL_6}",     "")).strip() if betcat_avail else ""
        b_no     = str(row.get(f"_b_{BETCAT_PLATFORM_NO_COL_6}","")).strip() if betcat_avail else ""
        b_status = str(row.get(f"_b_{BETCAT_STATUS_COL_6}",     "")).strip() if betcat_avail else ""
        b_fee    = str(row.get(f"_b_{BETCAT_FEE_COL_6}",        "")).strip() if betcat_avail else ""
        b_time   = str(row.get(f"_b_{BETCAT_PAY_TIME_COL_6}",   "")).strip() if betcat_avail else ""
        b_ctime  = str(row.get(f"_b_{BETCAT_CREATE_TIME_COL_6}","")).strip() if betcat_avail else ""
        b_hit    = b_amt != ""

        # ── Cashnewpay 命中判断 ───────────────────────────────────────────────
        c_amt    = str(row.get(f"_c_{CASHNEWPAY_AMOUNT_COL_6}",      "")).strip() if cashnewpay_avail else ""
        c_no     = str(row.get(f"_c_{CASHNEWPAY_PLATFORM_NO_COL_6}", "")).strip() if cashnewpay_avail else ""
        c_status = str(row.get(f"_c_{CASHNEWPAY_STATUS_COL_6}",      "")).strip() if cashnewpay_avail else ""
        c_desc   = str(row.get(f"_c_{CASHNEWPAY_STATE_DESC_COL_6}",  "")).strip() if cashnewpay_avail else ""
        c_fee    = str(row.get(f"_c_{CASHNEWPAY_FEE_COL_6}",         "")).strip() if cashnewpay_avail else ""
        c_time   = str(row.get(f"_c_{CASHNEWPAY_FINISH_TIME_COL_6}", "")).strip() if cashnewpay_avail else ""
        c_ctime  = str(row.get(f"_c_{CASHNEWPAY_CREATE_TIME_COL_6}", "")).strip() if cashnewpay_avail else ""
        c_hit    = c_amt != ""

        if b_hit and c_hit:
            logger.warning(
                "订单 %s 同时命中 BETCAT 和 CASHNEWPAY，取 BETCAT",
                str(row.get(admin_join_col, "")).strip(),
            )

        if b_hit:
            # Cashnewpay 的状态描述（英文）优先于订单状态字段做映射
            raw_status = b_status
            match_status_list.append("是")
            platform_source_list.append("BETCAT")
            platform_order_no_list.append(b_no)
            platform_amount_list.append(b_amt)
            platform_status_list.append(_normalize_platform_status_6("BETCAT", raw_status))
            fee_list.append(b_fee)
            transaction_date_list.append(_format_date_6(b_time) or _format_date_6(b_ctime))
        elif c_hit:
            # 优先用英文状态描述映射，fallback 到中文订单状态
            raw_status = c_desc if c_desc else c_status
            match_status_list.append("是")
            platform_source_list.append("CASHNEWPAY")
            platform_order_no_list.append(c_no)
            platform_amount_list.append(c_amt)
            platform_status_list.append(_normalize_platform_status_6("CASHNEWPAY", raw_status))
            fee_list.append(c_fee)
            transaction_date_list.append(_format_date_6(c_time) or _format_date_6(c_ctime))
        else:
            match_status_list.append("否")
            platform_source_list.append("")
            platform_order_no_list.append("")
            platform_amount_list.append("")
            platform_status_list.append("")
            fee_list.append("")
            transaction_date_list.append("")

    # 还原为仅 admin 原始列，再追加 7 个新增列
    admin_cols = list(admin_df.columns)
    result = result[admin_cols].copy()

    result[MATCH_STATUS_COL_6]      = match_status_list
    result[PLATFORM_SOURCE_COL_6]   = platform_source_list
    result[PLATFORM_ORDER_NO_COL_6] = platform_order_no_list
    result[PLATFORM_AMOUNT_COL_6]   = platform_amount_list
    result[PLATFORM_STATUS_COL_6]   = platform_status_list
    result[FEE_COL_6]               = fee_list
    result[TRANSACTION_DATE_COL_6]  = transaction_date_list

    # 追加平台多余行
    admin_order_keys: Set[str] = (
        {str(v).strip() for v in admin_df[admin_join_col] if str(v).strip()}
        if admin_join_col in admin_df.columns else set()
    )
    result_cols = list(result.columns)
    extra_df = _build_platform_only_rows_6(
        result_cols,
        admin_order_keys,
        betcat_lk if betcat_avail else None,
        cashnewpay_lk if cashnewpay_avail else None,
    )
    if not extra_df.empty:
        result = pd.concat([result, extra_df], ignore_index=True)

    return result


def _build_platform_only_rows_6(
    result_cols: List[str],
    admin_order_keys: Set[str],
    betcat_lk: Optional[pd.DataFrame],
    cashnewpay_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """构建平台有、admin 无的多余行（MATCH_STATUS_COL_6 = "平台多余"）。"""
    extra = []

    # ── Betcat 多余行 ─────────────────────────────────────────
    if betcat_lk is not None:
        for key in betcat_lk.index:
            k = str(key).strip()
            if not k or k in admin_order_keys:
                continue
            row: dict = {c: "" for c in result_cols}
            row[MATCH_STATUS_COL_6]      = "平台多余"
            row[PLATFORM_SOURCE_COL_6]   = "BETCAT"
            b_no     = str(betcat_lk.at[key, BETCAT_PLATFORM_NO_COL_6]).strip() if BETCAT_PLATFORM_NO_COL_6 in betcat_lk.columns else ""
            b_amt    = str(betcat_lk.at[key, BETCAT_AMOUNT_COL_6]).strip()      if BETCAT_AMOUNT_COL_6      in betcat_lk.columns else ""
            b_status = str(betcat_lk.at[key, BETCAT_STATUS_COL_6]).strip()      if BETCAT_STATUS_COL_6      in betcat_lk.columns else ""
            b_fee    = str(betcat_lk.at[key, BETCAT_FEE_COL_6]).strip()         if BETCAT_FEE_COL_6         in betcat_lk.columns else ""
            b_time   = str(betcat_lk.at[key, BETCAT_PAY_TIME_COL_6]).strip()    if BETCAT_PAY_TIME_COL_6    in betcat_lk.columns else ""
            row[PLATFORM_ORDER_NO_COL_6] = b_no
            row[PLATFORM_AMOUNT_COL_6]   = b_amt
            row[PLATFORM_STATUS_COL_6]   = _normalize_platform_status_6("BETCAT", b_status)
            row[FEE_COL_6]               = b_fee
            row[TRANSACTION_DATE_COL_6]  = _format_date_6(b_time)
            extra.append(row)

    # ── Cashnewpay 多余行 ────────────────────────────────────
    if cashnewpay_lk is not None:
        for key in cashnewpay_lk.index:
            k = str(key).strip()
            if not k or k in admin_order_keys:
                continue
            row = {c: "" for c in result_cols}
            row[MATCH_STATUS_COL_6]      = "平台多余"
            row[PLATFORM_SOURCE_COL_6]   = "CASHNEWPAY"
            c_no     = str(cashnewpay_lk.at[key, CASHNEWPAY_PLATFORM_NO_COL_6]).strip()  if CASHNEWPAY_PLATFORM_NO_COL_6  in cashnewpay_lk.columns else ""
            c_amt    = str(cashnewpay_lk.at[key, CASHNEWPAY_AMOUNT_COL_6]).strip()       if CASHNEWPAY_AMOUNT_COL_6       in cashnewpay_lk.columns else ""
            c_status = str(cashnewpay_lk.at[key, CASHNEWPAY_STATUS_COL_6]).strip()       if CASHNEWPAY_STATUS_COL_6       in cashnewpay_lk.columns else ""
            c_desc   = str(cashnewpay_lk.at[key, CASHNEWPAY_STATE_DESC_COL_6]).strip()   if CASHNEWPAY_STATE_DESC_COL_6   in cashnewpay_lk.columns else ""
            c_fee    = str(cashnewpay_lk.at[key, CASHNEWPAY_FEE_COL_6]).strip()          if CASHNEWPAY_FEE_COL_6          in cashnewpay_lk.columns else ""
            c_time   = str(cashnewpay_lk.at[key, CASHNEWPAY_FINISH_TIME_COL_6]).strip()  if CASHNEWPAY_FINISH_TIME_COL_6  in cashnewpay_lk.columns else ""
            raw_status = c_desc if c_desc else c_status
            row[PLATFORM_ORDER_NO_COL_6] = c_no
            row[PLATFORM_AMOUNT_COL_6]   = c_amt
            row[PLATFORM_STATUS_COL_6]   = _normalize_platform_status_6("CASHNEWPAY", raw_status)
            row[FEE_COL_6]               = c_fee
            row[TRANSACTION_DATE_COL_6]  = _format_date_6(c_time)
            extra.append(row)

    if not extra:
        return pd.DataFrame(columns=result_cols)
    return pd.DataFrame(extra, columns=result_cols)


# ─────────────────────────────────────────────────────────────────────────────
# 统计 & 汇总
# ─────────────────────────────────────────────────────────────────────────────

def _extract_amount_diff_rows_6(df: pd.DataFrame) -> pd.DataFrame:
    """提取已匹配（是否匹配=是）但 admin金额 ≠ 平台金额的行（差值 > 0.01）。"""
    if MATCH_STATUS_COL_6 not in df.columns or PLATFORM_AMOUNT_COL_6 not in df.columns:
        return df.iloc[0:0]
    matched = df[df[MATCH_STATUS_COL_6] == "是"].copy()
    admin_amt_col = "金额"
    if admin_amt_col not in matched.columns:
        return df.iloc[0:0]
    matched["_a"] = matched[admin_amt_col].apply(_to_float_6)
    matched["_p"] = matched[PLATFORM_AMOUNT_COL_6].apply(_to_float_6)
    diff = matched[
        matched["_a"].notna() &
        matched["_p"].notna() &
        (abs(matched["_a"] - matched["_p"]) > 0.01)
    ].drop(columns=["_a", "_p"])
    return diff


def log_match_stats_6(df: pd.DataFrame, label: str) -> None:
    """按匹配状态打印统计摘要，未匹配数 > 0 时打 warning。"""
    total     = len(df)
    matched   = (df[MATCH_STATUS_COL_6] == "是").sum()
    unmatched = (df[MATCH_STATUS_COL_6] == "否").sum()
    extra     = (df[MATCH_STATUS_COL_6] == "平台多余").sum()
    logger.info(
        "【%s】共 %d 条 — 匹配 %d / 未匹配 %d / 平台多余 %d",
        label, total, matched, unmatched, extra,
    )
    if unmatched > 0:
        logger.warning("【%s】存在 %d 条未匹配记录，请查看匹配失败 sheet", label, unmatched)


def build_summary_sheet_6(
    collection_df: pd.DataFrame,
    payout_df: pd.DataFrame,
) -> pd.DataFrame:
    """按类型（代收/代付）× 来源平台 × 交易月份汇总笔数/金额/手续费。

    只统计 MATCH_STATUS_COL_6 == "是" 且 PLATFORM_STATUS_COL_6 == "成功" 的行。
    """
    summary_cols = [
        SUMMARY_MONTH_COL_6,
        SUMMARY_TYPE_COL_6,
        SUMMARY_PLATFORM_COL_6,
        SUMMARY_COUNT_COL_6,
        SUMMARY_AMOUNT_COL_6,
        SUMMARY_FEE_COL_6,
        SUMMARY_ARRIVE_COL_6,
    ]

    frames = []
    for df, type_label in ((collection_df, "代收"), (payout_df, "代付")):
        if df is None or df.empty or PLATFORM_STATUS_COL_6 not in df.columns:
            continue
        matched = df[df[PLATFORM_STATUS_COL_6].astype(str).str.strip() == "成功"].copy()
        if matched.empty:
            continue

        matched[SUMMARY_MONTH_COL_6] = matched[TRANSACTION_DATE_COL_6].apply(
            lambda v: (_format_date_6(v)[:7] if _format_date_6(v) else "")
        )
        matched["_amt"] = matched[PLATFORM_AMOUNT_COL_6].apply(lambda v: _to_float_6(v) or 0.0)
        matched["_fee"] = matched[FEE_COL_6].apply(lambda v: _to_float_6(v) or 0.0)

        grp = matched.groupby(
            [PLATFORM_SOURCE_COL_6, SUMMARY_MONTH_COL_6], as_index=False
        ).agg(
            **{SUMMARY_COUNT_COL_6:  ("_amt", "count")},
            **{SUMMARY_AMOUNT_COL_6: ("_amt", "sum")},
            **{SUMMARY_FEE_COL_6:    ("_fee", "sum")},
        )
        grp[SUMMARY_TYPE_COL_6] = type_label
        grp = grp.rename(columns={PLATFORM_SOURCE_COL_6: SUMMARY_PLATFORM_COL_6})
        for col in (SUMMARY_AMOUNT_COL_6, SUMMARY_FEE_COL_6):
            grp[col] = grp[col].round(2)
        grp[SUMMARY_ARRIVE_COL_6] = (grp[SUMMARY_AMOUNT_COL_6] - grp[SUMMARY_FEE_COL_6]).round(2)
        frames.append(grp[summary_cols])

    if not frames:
        return pd.DataFrame(columns=summary_cols)
    return pd.concat(frames, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# 输出
# ─────────────────────────────────────────────────────────────────────────────

def write_output_6(
    collection_df: pd.DataFrame,
    payout_df: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """将代收/代付对账结果写入 data/output/代收代付对账结果_{YYYYMMDD}.xlsx。

    输出 5 个 sheet：
        OUTPUT_COLLECTION_SHEET_6        → 代收全量结果
        OUTPUT_COLLECTION_FAILED_SHEET_6 → 代收匹配失败（是否匹配 == "否"）
        OUTPUT_PAYOUT_SHEET_6            → 代付全量结果
        OUTPUT_PAYOUT_FAILED_SHEET_6     → 代付匹配失败（是否匹配 == "否"）
        OUTPUT_SUMMARY_SHEET_6           → 平台汇总
    """
    today = date.today().strftime("%Y%m%d")
    filename = OUTPUT_FILE_TEMPLATE_6.format(date=today)
    output_path = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)

    collection_failed = (
        collection_df[collection_df[MATCH_STATUS_COL_6] == "否"].copy()
        if MATCH_STATUS_COL_6 in collection_df.columns else collection_df.iloc[0:0]
    )
    payout_failed = (
        payout_df[payout_df[MATCH_STATUS_COL_6] == "否"].copy()
        if MATCH_STATUS_COL_6 in payout_df.columns else payout_df.iloc[0:0]
    )
    summary_df = build_summary_sheet_6(collection_df, payout_df)
    amount_diff_df = pd.concat(
        [_extract_amount_diff_rows_6(collection_df),
         _extract_amount_diff_rows_6(payout_df)],
        ignore_index=True,
    ).fillna("")
    logger.info("金额差异订单共 %d 条", len(amount_diff_df))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        collection_df.to_excel(writer, sheet_name=OUTPUT_COLLECTION_SHEET_6, index=False)
        collection_failed.to_excel(writer, sheet_name=OUTPUT_COLLECTION_FAILED_SHEET_6, index=False)
        payout_df.to_excel(writer, sheet_name=OUTPUT_PAYOUT_SHEET_6, index=False)
        payout_failed.to_excel(writer, sheet_name=OUTPUT_PAYOUT_FAILED_SHEET_6, index=False)
        summary_df.to_excel(writer, sheet_name=OUTPUT_SUMMARY_SHEET_6, index=False)
        amount_diff_df.to_excel(writer, sheet_name=OUTPUT_AMOUNT_DIFF_SHEET_6, index=False)

        for sname in (
            OUTPUT_COLLECTION_SHEET_6,
            OUTPUT_COLLECTION_FAILED_SHEET_6,
            OUTPUT_PAYOUT_SHEET_6,
            OUTPUT_PAYOUT_FAILED_SHEET_6,
            OUTPUT_SUMMARY_SHEET_6,
            OUTPUT_AMOUNT_DIFF_SHEET_6,
        ):
            ws = writer.sheets[sname]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions

    logger.info("结果文件已写入: %s", output_path)
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    """代号6 主流程：扫描 → 读取 → 构建查找表 → 匹配 → 输出。

    Returns:
        0 成功 / 1 失败
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── 扫描 ──────────────────────────────────────────────────────────────────
    files = scan_source_files_6(INPUT_DIR_6)

    # ── Admin 主表必须存在 ─────────────────────────────────────────────────────
    if not files["admin_collection"] and not files["admin_payout"]:
        logger.error(
            "未找到任何 admin 文件。请将文件放入 %s，"
            "文件名须以 'admin收款' 或 'admin兑换' 开头。",
            INPUT_DIR_6,
        )
        return 1

    # ── 读取 Admin 收款（代收主表）────────────────────────────────────────────
    admin_collection: Optional[pd.DataFrame] = None
    if files["admin_collection"]:
        frames = [read_admin_collection_6(fp) for fp in files["admin_collection"]]
        admin_collection = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        logger.info("admin 收款订单共 %d 条", len(admin_collection))
    else:
        logger.warning("未找到 admin 收款文件，跳过代收对账。")

    # ── 读取 Admin 兑换（代付主表）────────────────────────────────────────────
    admin_payout: Optional[pd.DataFrame] = None
    if files["admin_payout"]:
        frames = [read_admin_payout_6(fp) for fp in files["admin_payout"]]
        admin_payout = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        logger.info("admin 兑换订单共 %d 条", len(admin_payout))
    else:
        logger.warning("未找到 admin 兑换文件，跳过代付对账。")

    # ── 读取 Betcat Payment（代收）────────────────────────────────────────────
    betcat_payment_lk: Optional[pd.DataFrame] = None
    if files["betcat_payment"]:
        frames = [read_betcat_csv_6(fp) for fp in files["betcat_payment"]]
        raw = pd.concat(frames, ignore_index=True)
        betcat_payment_lk = build_betcat_lookup_6(raw)
        logger.info("Betcat payment 查找表共 %d 条（来自 %d 个文件）",
                    len(betcat_payment_lk), len(files["betcat_payment"]))

    # ── 读取 Betcat Payout（代付）─────────────────────────────────────────────
    betcat_payout_lk: Optional[pd.DataFrame] = None
    if files["betcat_payout"]:
        frames = [read_betcat_csv_6(fp) for fp in files["betcat_payout"]]
        raw = pd.concat(frames, ignore_index=True)
        betcat_payout_lk = build_betcat_lookup_6(raw)
        logger.info("Betcat payout 查找表共 %d 条（来自 %d 个文件）",
                    len(betcat_payout_lk), len(files["betcat_payout"]))

    # ── 读取 Cashnewpay 收款（代收）───────────────────────────────────────────
    cashnewpay_collection_lk: Optional[pd.DataFrame] = None
    if files["cashnewpay_collection"]:
        frames = [read_cashnewpay_xlsx_6(fp) for fp in files["cashnewpay_collection"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        cashnewpay_collection_lk = build_cashnewpay_lookup_6(raw)
        logger.info("Cashnewpay 收款查找表共 %d 条", len(cashnewpay_collection_lk))

    # ── 读取 Cashnewpay 兑换（代付）───────────────────────────────────────────
    cashnewpay_exchange_lk: Optional[pd.DataFrame] = None
    if files["cashnewpay_exchange"]:
        frames = [read_cashnewpay_xlsx_6(fp) for fp in files["cashnewpay_exchange"]]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        cashnewpay_exchange_lk = build_cashnewpay_lookup_6(raw)
        logger.info("Cashnewpay 兑换查找表共 %d 条", len(cashnewpay_exchange_lk))

    # ── 代收对账 ──────────────────────────────────────────────────────────────
    if admin_collection is not None:
        collection_result = enrich_admin_6(admin_collection, betcat_payment_lk, cashnewpay_collection_lk)
        log_match_stats_6(collection_result, "代收")
    else:
        collection_result = pd.DataFrame(columns=OUTPUT_NEW_COLS_6)

    # ── 代付对账 ──────────────────────────────────────────────────────────────
    if admin_payout is not None:
        payout_result = enrich_admin_6(admin_payout, betcat_payout_lk, cashnewpay_exchange_lk)
        log_match_stats_6(payout_result, "代付")
    else:
        payout_result = pd.DataFrame(columns=OUTPUT_NEW_COLS_6)

    # ── 输出 ──────────────────────────────────────────────────────────────────
    try:
        output_path = write_output_6(collection_result, payout_result, OUTPUT_DIR)
        logger.info("完成。输出文件：%s", output_path)
    except PermissionError:
        logger.error("无法写入输出文件，请确认文件未在 Excel 中打开后重试。")
        return 1
    except Exception:
        logger.error("写入输出文件失败", exc_info=True)
        return 1

    return 0
