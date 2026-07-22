"""通用对账引擎：声明式平台的读取 / 查找表构建 / 匹配。

与具体代号无关。消费 platform_spec.PlatformSpec + OutputSchema，
把原本每平台一套的 read_* / build_*_lookup / enrich 硬编码逻辑，
收敛成一套按注册表遍历的通用流程。各代号只需提供 BUILTIN_SPECS 与
（必要时）自定义 handler，新增结构类似的平台即可纯声明接入、无需改代码。

低层读取原语（read_source_table / dedup_lookup / format_date / to_float 等）
也集中在此，供各代号 app 模块复用（app 侧以别名回引，保持既有引用不变）。
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from .platform_spec import (
    CANON,
    DIRECTIONS,  # noqa: F401  （对外复用）
    OutputSchema,
    PlatformSpec,
    get_handler,
    register_handler,
)

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# 低层读取 / 清洗原语（代号无关）
# ═════════════════════════════════════════════════════════════════════════════

CSV_ENCODINGS = ("utf-8-sig", "gbk", "gb18030", "utf-8")


def normalize_columns(columns) -> Set[str]:
    """去除列名首尾空格，返回规范化集合。"""
    return {str(c).strip() for c in columns}


def format_date(val) -> str:
    """将任意日期值格式化为 YYYY-MM-DD，失败返回空串。

    支持：pandas Timestamp / datetime、ISO 8601 带时区、含毫秒字符串、
    YYYY-MM-DD / YYYY/MM/DD（含时间变体）。
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


def to_float(val) -> Optional[float]:
    """将字符串金额转为 float，支持千分位逗号，失败返回 None。"""
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def excel_engine(filepath: Path) -> str:
    """按扩展名选择 Excel 引擎：.xls → xlrd（仅 xlrd 2.x 支持），其余 → openpyxl。"""
    return "xlrd" if filepath.suffix.lower() == ".xls" else "openpyxl"


def read_csv_multi_encoding(filepath: Path) -> pd.DataFrame:
    """多编码尝试读取 CSV（平台导出编码不稳定，混有 UTF-8/GBK）。

    依次尝试 utf-8-sig / gbk / gb18030 / utf-8，只捕获 UnicodeDecodeError 重试，
    全部失败抛清晰中文 UnicodeError；保留 dtype=str 与 keep_default_na=False。
    """
    for enc in CSV_ENCODINGS:
        try:
            return pd.read_csv(filepath, dtype=str, keep_default_na=False, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeError(
        f"无法解码 CSV 文件: {filepath.name}（已尝试 {'/'.join(CSV_ENCODINGS)}）"
    )


def select_sheet(
    xls: pd.ExcelFile,
    preferred_sheet: Optional[str],
    *,
    fallback_join_col: Optional[str] = None,
    use_first_sheet: bool = False,
    label: str = "",
    filename: str = "",
) -> str:
    """在 Excel 多 sheet 中选择目标 sheet。

    - 命中 preferred_sheet 直接返回；
    - 否则传了 fallback_join_col → 遍历找含该列的 sheet（找不到抛 ValueError）；
    - 否则 use_first_sheet=True → 用首个 sheet 并打 warning；
    - 均不适用（如 preferred_sheet 为 None）→ 兜底用首个 sheet。
    """
    sheet_names = xls.sheet_names
    if preferred_sheet and preferred_sheet in sheet_names:
        return preferred_sheet

    if fallback_join_col is not None:
        for s in sheet_names:
            try:
                preview = pd.read_excel(xls, sheet_name=s, nrows=0, dtype=str)
                if fallback_join_col in normalize_columns(preview.columns):
                    logger.warning(
                        "%s文件未找到 sheet '%s'，回退使用含 '%s' 列的 sheet '%s'",
                        label, preferred_sheet, fallback_join_col, s,
                    )
                    return s
            except Exception:
                continue
        raise ValueError(
            f"{label}文件 {filename} 中找不到 sheet '{preferred_sheet}' "
            f"或含 '{fallback_join_col}' 列的 sheet，可用 sheet: {sheet_names}"
        )

    if use_first_sheet:
        target = sheet_names[0]
        logger.warning(
            "%s文件未找到 sheet '%s'，使用首个 sheet '%s'",
            label, preferred_sheet, target,
        )
        return target

    return sheet_names[0]


def read_source_table(
    filepath: Path,
    *,
    preferred_sheet: Optional[str] = None,
    fallback_join_col: Optional[str] = None,
    use_first_sheet: bool = False,
    label: str = "",
) -> pd.DataFrame:
    """按扩展名自适应读取源文件为 DataFrame，并做通用清洗。

    - .csv          → 多编码读取（preferred_sheet 等 Excel 参数忽略）
    - .xls / .xlsx  → 按扩展名选引擎；命中/回退 sheet 后读取
    统一收尾：列名 strip + dropna(how="all") + fillna("")（与既有行为一致）。
    """
    ext = filepath.suffix.lower()
    if ext == ".csv":
        df = read_csv_multi_encoding(filepath)
    else:
        engine = excel_engine(filepath)
        with pd.ExcelFile(filepath, engine=engine) as xls:
            target = select_sheet(
                xls,
                preferred_sheet,
                fallback_join_col=fallback_join_col,
                use_first_sheet=use_first_sheet,
                label=label,
                filename=filepath.name,
            )
        df = pd.read_excel(filepath, sheet_name=target, dtype=str, engine=engine)
    df.columns = [str(c).strip() for c in df.columns]
    return df.dropna(how="all").fillna("")


def dedup_lookup(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    """过滤空 key 行，对 key_col 去重（保留首行），发现重复时打 warning。"""
    df = df[df[key_col].astype(str).str.strip() != ""].copy()
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    dupes = before - len(df)
    if dupes > 0:
        logger.warning("【%s】去重时发现 %d 条重复订单号，已保留首行", label, dupes)
    return df


# ═════════════════════════════════════════════════════════════════════════════
# 通用状态标准化
# ═════════════════════════════════════════════════════════════════════════════

def normalize_status(spec: PlatformSpec, raw_status: str = "") -> str:
    """将平台原始状态统一为：成功 / 失败 / 处理中 / 关闭。

    先按 spec.status_map 精确映射，再按 spec.status_prefix_map 前缀匹配
    （如 USER_REFUND-<单号>），均未命中打 warning 并保留原文。
    """
    status = str(raw_status).strip()
    if not status:
        return ""
    normalized = spec.status_map.get(status)
    if normalized is not None:
        return normalized
    for prefix, mapped in spec.status_prefix_map.items():
        if status.startswith(prefix):
            return mapped
    logger.warning("【%s】发现未识别平台状态 '%s'，保留原值", spec.key, status)
    return status


# ═════════════════════════════════════════════════════════════════════════════
# 通用读取 & 查找表构建
# ═════════════════════════════════════════════════════════════════════════════

def read_platform_source(spec: PlatformSpec, direction: str, filepath: Path) -> pd.DataFrame:
    """按 spec/direction 读取单个平台源文件（格式自适应）。"""
    return read_source_table(
        filepath,
        preferred_sheet=spec.sheet_for(direction),
        use_first_sheet=spec.use_first_sheet,
        label=spec.key,
    )


def build_lookup_from_columns(
    df: pd.DataFrame,
    join_col: str,
    columns: Dict[str, str],
    key: str,
) -> pd.DataFrame:
    """把源表按 columns（规范字段→源列名）归一化为查找表。

    - 将命中的源列 rename 成 CANON 内部列名；
    - 以 join_col 去重并设为索引；
    - 缺关联列时打 warning 返回空表（与各平台原 build_*_lookup 行为一致）。
    """
    rename = {
        src: CANON[field]
        for field, src in columns.items()
        if field in CANON and src in df.columns
    }
    if join_col not in df.columns:
        logger.warning("【%s】缺少关联列 '%s'，返回空查找表", key, join_col)
        return pd.DataFrame()
    keep = [join_col] + list(rename.keys())
    sub = df[keep].rename(columns=rename)
    sub = dedup_lookup(sub, join_col, key)
    return sub.set_index(join_col)


def generic_build_lookup(spec: PlatformSpec, direction: str, df: pd.DataFrame) -> pd.DataFrame:
    """通用查找表构建：用 spec.cols_for(direction) 归一化列名。"""
    return build_lookup_from_columns(df, spec.join_col, spec.cols_for(direction), spec.key)


# ═════════════════════════════════════════════════════════════════════════════
# 通用匹配（enrich）
# ═════════════════════════════════════════════════════════════════════════════

def _prefix(key: str) -> str:
    """merge 时的平台列前缀（唯一、不与业务列冲突，enrich 结尾丢弃）。"""
    return f"__lk_{key}__"


def _safe_merge(base, lookup, left_on, prefix, label, expected_rows):
    """left-join 单个平台查找表，校验行数不膨胀，缺失值填空串。"""
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
            f"请检查该平台去重逻辑"
        )
    for c in lookup.columns:
        merged[f"{prefix}{c}"] = merged[f"{prefix}{c}"].fillna("")
    return merged


def enrich_admin_generic(
    admin_df: pd.DataFrame,
    lookups: Dict[str, Optional[pd.DataFrame]],
    specs: List[PlatformSpec],
    schema: OutputSchema,
) -> pd.DataFrame:
    """以 admin 为主表，按优先级 left-join 各平台查找表，追加 7 个输出列。

    - specs 已按 priority 升序；lookups 以 spec.key 索引，None/空表跳过。
    - 逐行按优先级取第一个命中的平台（该平台金额非空即命中）。
    - 结尾丢弃全部内部列，仅保留 admin 原列 + schema.output_cols()，
      再追加"平台多余"行（平台有、admin 无）。
    """
    result = admin_df.copy()
    expected_rows = len(admin_df)

    admin_join_col = next(
        (c for c in schema.admin_join_candidates if c in admin_df.columns),
        schema.admin_join_candidates[0],
    )

    active = [
        s for s in specs
        if lookups.get(s.key) is not None and not lookups[s.key].empty
    ]

    for s in active:
        result = _safe_merge(
            result, lookups[s.key], admin_join_col, _prefix(s.key), s.key, expected_rows,
        )

    match_status_list: List[str] = []
    platform_source_list: List[str] = []
    platform_order_no_list: List[str] = []
    platform_amount_list: List[str] = []
    platform_status_list: List[str] = []
    fee_list: List[str] = []
    transaction_date_list: List[str] = []

    for _, row in result.iterrows():
        # 各平台命中判断（金额非空即命中），按优先级顺序收集命中平台
        hit_keys = []
        amounts: Dict[str, str] = {}
        for s in active:
            amt = str(row.get(f"{_prefix(s.key)}{CANON['amount']}", "")).strip()
            amounts[s.key] = amt
            if amt != "":
                hit_keys.append(s.key)

        if len(hit_keys) > 1:
            logger.warning(
                "订单 %s 同时命中 %s，按优先级取 %s",
                str(row.get(admin_join_col, "")).strip(), "/".join(hit_keys), hit_keys[0],
            )

        chosen = None
        for s in active:
            if amounts[s.key] != "":
                chosen = s
                break

        if chosen is not None:
            p = _prefix(chosen.key)
            desc = str(row.get(f"{p}{CANON['status_desc']}", "")).strip()
            st = str(row.get(f"{p}{CANON['status']}", "")).strip()
            raw_status = desc if desc else st       # 英文状态描述优先，回退中文状态
            ftime = str(row.get(f"{p}{CANON['finish_time']}", "")).strip()
            ctime = str(row.get(f"{p}{CANON['create_time']}", "")).strip()
            match_status_list.append(schema.match_yes)
            platform_source_list.append(chosen.key)
            platform_order_no_list.append(str(row.get(f"{p}{CANON['platform_no']}", "")).strip())
            platform_amount_list.append(amounts[chosen.key])
            platform_status_list.append(normalize_status(chosen, raw_status))
            fee_list.append(str(row.get(f"{p}{CANON['fee']}", "")).strip())
            transaction_date_list.append(format_date(ftime) or format_date(ctime))
        else:
            match_status_list.append(schema.match_no)
            platform_source_list.append("")
            platform_order_no_list.append("")
            platform_amount_list.append("")
            platform_status_list.append("")
            fee_list.append("")
            transaction_date_list.append("")

    # 还原为仅 admin 原始列，再追加 7 个新增列（丢弃全部内部 __lk_* 列）
    admin_cols = list(admin_df.columns)
    result = result[admin_cols].copy()

    result[schema.match_status_col]      = match_status_list
    result[schema.platform_source_col]   = platform_source_list
    result[schema.platform_order_no_col] = platform_order_no_list
    result[schema.platform_amount_col]   = platform_amount_list
    result[schema.platform_status_col]   = platform_status_list
    result[schema.fee_col]               = fee_list
    result[schema.transaction_date_col]  = transaction_date_list

    # 追加平台多余行
    admin_order_keys: Set[str] = (
        {str(v).strip() for v in admin_df[admin_join_col] if str(v).strip()}
        if admin_join_col in admin_df.columns else set()
    )
    result_cols = list(result.columns)
    extra_df = build_platform_only_rows(result_cols, admin_order_keys, lookups, active, schema)
    if not extra_df.empty:
        result = pd.concat([result, extra_df], ignore_index=True)

    return result


def build_platform_only_rows(
    result_cols: List[str],
    admin_order_keys: Set[str],
    lookups: Dict[str, Optional[pd.DataFrame]],
    active_specs: List[PlatformSpec],
    schema: OutputSchema,
) -> pd.DataFrame:
    """构建平台有、admin 无的多余行（match_status = 平台多余）。

    按 active_specs（优先级）顺序遍历，交易日期只取完成时间（无创建时间回退），
    与原逐平台实现一致。
    """
    extra: List[dict] = []

    for s in active_specs:
        lk = lookups.get(s.key)
        if lk is None or lk.empty:
            continue
        for key in lk.index:
            k = str(key).strip()
            if not k or k in admin_order_keys:
                continue
            no = str(lk.at[key, CANON["platform_no"]]).strip() if CANON["platform_no"] in lk.columns else ""
            amt = str(lk.at[key, CANON["amount"]]).strip() if CANON["amount"] in lk.columns else ""
            st = str(lk.at[key, CANON["status"]]).strip() if CANON["status"] in lk.columns else ""
            desc = str(lk.at[key, CANON["status_desc"]]).strip() if CANON["status_desc"] in lk.columns else ""
            fee = str(lk.at[key, CANON["fee"]]).strip() if CANON["fee"] in lk.columns else ""
            ftime = str(lk.at[key, CANON["finish_time"]]).strip() if CANON["finish_time"] in lk.columns else ""
            raw_status = desc if desc else st

            row = {c: "" for c in result_cols}
            row[schema.match_status_col]      = schema.match_extra
            row[schema.platform_source_col]   = s.key
            row[schema.platform_order_no_col] = no
            row[schema.platform_amount_col]   = amt
            row[schema.platform_status_col]   = normalize_status(s, raw_status)
            row[schema.fee_col]               = fee
            row[schema.transaction_date_col]  = format_date(ftime)
            extra.append(row)

    if not extra:
        return pd.DataFrame(columns=result_cols)
    return pd.DataFrame(extra, columns=result_cols)


# ═════════════════════════════════════════════════════════════════════════════
# 内置 generic handler
# ═════════════════════════════════════════════════════════════════════════════

class GenericHandler:
    """声明式平台的默认 handler：读取 + 归一化查找表全部由 spec 驱动。"""

    def read(self, spec: PlatformSpec, direction: str, filepath: Path) -> pd.DataFrame:
        return read_platform_source(spec, direction, filepath)

    def build_lookup(self, spec: PlatformSpec, direction: str, df: pd.DataFrame) -> pd.DataFrame:
        return generic_build_lookup(spec, direction, df)


# 注册内置 handler；插件可 register_handler 覆盖或新增自定义 handler
register_handler("generic", GenericHandler())


def resolve_handler(spec: PlatformSpec):
    """取 spec.handler 对应的 handler，缺失回退 generic。"""
    return get_handler(spec.handler) or get_handler("generic")
