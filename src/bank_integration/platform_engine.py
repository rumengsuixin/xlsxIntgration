"""通用对账引擎：声明式平台的读取 / 查找表构建 / 匹配。

与具体代号无关。消费 platform_spec.PlatformSpec + OutputSchema，
把原本每平台一套的 read_* / build_*_lookup / enrich 硬编码逻辑，
收敛成一套按注册表遍历的通用流程。各代号只需提供 BUILTIN_SPECS 与
（必要时）自定义 handler，新增结构类似的平台即可纯声明接入、无需改代码。

低层读取原语（read_source_table / dedup_lookup / format_date / to_float 等）
也集中在此，供各代号 app 模块复用（app 侧以别名回引，保持既有引用不变）。
"""

import logging
import re
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

    时区处理：保留原始时区的**本地墙钟日期**，不转 UTC。带时区的平台时间
    （如 Betcat 的 `2026-06-30T23:53:20-03:00`）若转 UTC 会把 6/30 深夜交易
    推到 7/1，导致月份归属错误、且与 admin 本地时间（无时区）对不上。与
    app5._format_date_5 的口径一致。
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
        parsed = pd.to_datetime(s, errors="coerce")
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
    required_columns: Optional[List[str]] = None,
    use_first_sheet: bool = False,
    label: str = "",
    filename: str = "",
) -> str:
    """在 Excel 多 sheet 中选择目标 sheet。

    - 命中 preferred_sheet 直接返回；
    - 否则传了 required_columns → 遍历找含全部所需列的 sheet（找不到抛 ValueError）；
    - 否则传了 fallback_join_col → 遍历找含该列的 sheet（找不到抛 ValueError）；
    - 否则 use_first_sheet=True → 用首个 sheet 并打 warning；
    - 均不适用（如 preferred_sheet 为 None）→ 兜底用首个 sheet。
    """
    sheet_names = xls.sheet_names
    if preferred_sheet and preferred_sheet in sheet_names:
        return preferred_sheet

    if required_columns:
        for s in sheet_names:
            try:
                preview = pd.read_excel(xls, sheet_name=s, nrows=0, dtype=str)
                cols = normalize_columns(preview.columns)
                if all(rc in cols for rc in required_columns):
                    if s != preferred_sheet:
                        logger.warning(
                            "%s文件未命中 sheet '%s'，回退使用含所需列的 sheet '%s'",
                            label, preferred_sheet, s,
                        )
                    return s
            except Exception:
                continue
        raise ValueError(
            f"{label}文件 {filename} 中找不到 sheet '{preferred_sheet}' "
            f"或含所需列 {required_columns} 的 sheet，可用 sheet: {sheet_names}"
        )

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
    required_columns: Optional[List[str]] = None,
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
                required_columns=required_columns,
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


# ═════════════════════════════════════════════════════════════════════════════
# 列式 enrich（代号5：多 admin 关联键 / 币种 / 到账 / 机构 / 逐平台 handler 取值）
# ═════════════════════════════════════════════════════════════════════════════
#
# 与代号6 的 enrich_admin_generic 物理隔离——代号6 代码路径零触碰。
# 控制流（merge / 命中 / 优先级 / 平台多余行 / 组装）由引擎掌控，
# 逐平台取值委派给 handler 的 is_hit / match_values / extra_values；
# 声明式平台用 platform_handlers_5.GenericPayout5Handler 的通用取值。
# 输出列由 schema.column_plan（List[OutputColumn]）声明，source 为逻辑字段名。

def normalize_currency(value: object) -> str:
    """清洗平台币种显示值（strip + 大写）。"""
    return str(value or "").strip().upper()


def _admin_join_of(spec: PlatformSpec, schema: OutputSchema, admin_df: pd.DataFrame) -> str:
    """该平台在 admin 侧的关联列：spec.admin_join_col 优先，否则用 schema 候选。"""
    if spec.admin_join_col:
        return spec.admin_join_col
    return next(
        (c for c in schema.admin_join_candidates if c in admin_df.columns),
        schema.admin_join_candidates[0],
    )


def enrich_admin_columnar(admin_df, lookups, specs, schema) -> pd.DataFrame:
    """以 admin 为主表，按各平台自身 admin 关联键 left-join，追加 column_plan 声明的列。

    - specs 按 priority 升序;lookups 以 spec.key 索引(原生列查找表),None/空跳过。
    - 逐行按优先级取第一个命中的平台(handler.is_hit),命中值由 handler.match_values 提供。
    - schema.org_col 存在时,命中行用 handler 返回的 org 覆盖 admin 机构列;未命中保留 admin 原值。
    - 结尾丢弃全部内部前缀列,追加 column_plan 列 + 平台多余行。
    """
    result = admin_df.copy()
    expected_rows = len(admin_df)
    plan = schema.column_plan or []

    active = [
        s for s in specs
        if lookups.get(s.key) is not None and not lookups[s.key].empty
    ]

    for s in active:
        result = _safe_merge(
            result, lookups[s.key], _admin_join_of(s, schema, admin_df),
            _prefix(s.key), s.key, expected_rows,
        )

    col_lists: Dict[str, List[str]] = {c.name: [] for c in plan}
    org_list: List[str] = []

    for _, row in result.iterrows():
        chosen = None
        hit_keys = []
        for s in active:
            h = resolve_handler(s)
            if h.is_hit(s, row, _prefix(s.key)):
                hit_keys.append(s.key)
                if chosen is None:
                    chosen = s
        if len(hit_keys) > 1:
            logger.warning(
                "订单 %s 同时命中 %s，按优先级取 %s",
                str(row.get(_admin_join_of(chosen, schema, admin_df), "")).strip(),
                "/".join(hit_keys), hit_keys[0],
            )

        admin_org = str(row.get(schema.org_col, "")) if schema.org_col else ""
        if chosen is not None:
            vals = resolve_handler(chosen).match_values(chosen, row, _prefix(chosen.key), admin_org)
        else:
            vals = {"match_status": schema.match_no}

        for c in plan:
            col_lists[c.name].append(vals.get(c.source, ""))
        if schema.org_col:
            org_list.append(vals.get("org", admin_org) if chosen is not None else admin_org)

    # 还原为仅 admin 原始列(丢弃全部内部前缀列),覆盖机构列,追加输出列
    admin_cols = list(admin_df.columns)
    result = result[admin_cols].copy()
    if schema.org_col and schema.org_col in result.columns:
        result[schema.org_col] = org_list
    for c in plan:
        result[c.name] = col_lists[c.name]

    result_cols = list(result.columns)
    extra_df = _build_platform_only_rows_columnar(result_cols, admin_df, lookups, active, schema)
    if not extra_df.empty:
        result = pd.concat([result, extra_df], ignore_index=True)

    return result


def _build_platform_only_rows_columnar(result_cols, admin_df, lookups, active_specs, schema) -> pd.DataFrame:
    """平台有、admin 无的多余行(match_status=平台多余),取值委派 handler.extra_values。"""
    extra: List[dict] = []
    plan = schema.column_plan or []

    for s in active_specs:
        lk = lookups.get(s.key)
        if lk is None or lk.empty:
            continue
        handler = resolve_handler(s)
        ajc = _admin_join_of(s, schema, admin_df)
        admin_keys = (
            {str(v).strip() for v in admin_df[ajc] if str(v).strip()}
            if ajc in admin_df.columns else set()
        )
        backfill = (
            s.admin_join_col if s.extra_backfill_admin_col == "__default__"
            else s.extra_backfill_admin_col
        )
        for key in lk.index:
            k = str(key).strip()
            if not k or k in admin_keys:
                continue
            vals = handler.extra_values(s, lk.loc[key], k)
            row = {c: "" for c in result_cols}
            row[schema.match_status_col] = schema.match_extra
            if schema.org_col:
                row[schema.org_col] = vals.get("org", s.org_name or s.key)
            if backfill:
                row[backfill] = k
            for c in plan:
                if not c.in_extra or c.source == "match_status":
                    continue
                row[c.name] = vals.get(c.source, "")
            extra.append(row)

    if not extra:
        return pd.DataFrame(columns=result_cols)
    return pd.DataFrame(extra, columns=result_cols)


# ═════════════════════════════════════════════════════════════════════════════
# 通用聚合对账原语（代号无关：多对多，按 键+日期 汇总后 full-outer 比对）
# ═════════════════════════════════════════════════════════════════════════════
#
# 与逐行 1:1 的 enrich_admin_columnar 正交：平台无订单号、一个收款ID 对多笔，
# 无法逐行追加，须两侧各自按 (ID, 日期) groupby 求和/计数后做 full-outer 比对，
# 并产出独立 sheet。以下三个原语纯声明驱动，无任何平台专属分支：
#   derive_series      —— 按列取值(可去前缀 / 正则取组)
#   aggregate_by_keys  —— 按 (id, date) 聚合金额合计与笔数
#   reconcile_aggregate—— full-outer + 日期口径(exact/period/t1_window) + 状态判定


def derive_series(df: pd.DataFrame, col: Optional[str], *,
                  regex: Optional[str] = None,
                  strip_prefix: Optional[str] = None) -> pd.Series:
    """按列取值，返回字符串 Series。

    - col 缺失或不在表中 → 返回等长空串 Series（缺列不报错，交由上层判定）；
    - strip_prefix：去掉值开头的固定前缀（如 admin `其他` 去 `BIN-`）；
    - regex：在（去前缀后的）值上应用正则，取第 1 组（如 `USDT\\s*([\\d.]+)`）。
    先去前缀再取正则，二者可单独或组合使用。
    """
    if not col or col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=object)
    s = df[col].astype(str).str.strip()
    if strip_prefix:
        s = s.str.replace(rf"^{re.escape(strip_prefix)}", "", regex=True)
    if regex:
        s = s.str.extract(regex, expand=False, flags=re.IGNORECASE)
    return s.fillna("")


def aggregate_by_keys(ids, dates, amounts) -> pd.DataFrame:
    """按 (id, date) 聚合金额合计(_sum)与笔数(_cnt)。

    仅计入 id 非空且金额可解析为 float 的行；返回列 [_id, _date, _sum, _cnt]。
    """
    amt = [to_float(a) for a in amounts]
    df = pd.DataFrame({
        "_id": [str(i).strip() for i in ids],
        "_date": [str(d).strip() for d in dates],
        "_amt": amt,
    })
    df = df[(df["_id"] != "") & df["_amt"].notna()]
    if df.empty:
        return pd.DataFrame(columns=["_id", "_date", "_sum", "_cnt"])
    g = df.groupby(["_id", "_date"], as_index=False).agg(
        _sum=("_amt", "sum"), _cnt=("_amt", "count"))
    return g


def _shift_day(date_str: str, days: int) -> str:
    """将 YYYY-MM-DD 平移 days 天并重新格式化；解析失败原样返回。"""
    ts = pd.to_datetime(str(date_str), errors="coerce")
    if pd.isna(ts):
        return str(date_str)
    return (ts + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


# reconcile_aggregate 输出列的固定语义顺序（JSON 的 output_columns 仅提供显示名）
_RECON_FIELDS = ("date", "id", "admin_amt", "admin_cnt", "plat_amt", "plat_cnt", "diff", "status")

_DEFAULT_RECON_LABELS = {
    "consistent": "一致",
    "amount_diff": "金额不符",
    "platform_missing": "平台缺失",
    "platform_extra": "平台多余",
}


def reconcile_aggregate(admin_agg: pd.DataFrame, platform_agg: pd.DataFrame, *,
                        date_match_mode: str = "exact",
                        tolerance: float = 0.0,
                        labels: Optional[dict] = None,
                        columns: Optional[List[str]] = None) -> pd.DataFrame:
    """两侧聚合结果 full-outer 比对，产出对账表。

    date_match_mode：
      - exact     ：严格按 (id, 平台日) 对齐；
      - period    ：忽略日期，仅按 id 汇总比对（整期）；
      - t1_window ：平台打款日 pd 归属到一个 admin 业务日——pd 当天有 admin 归 pd，
                    否则前一日 pd-1 有 admin 归 pd-1(T+1)，都没有则留在 pd(平台多余)。
      - t1_shift  ：平台日恒定前移 1 天归属到 admin 的 pd-1（不试同日）——用于平台日期
                    是「上传日 T、内容为 T-1 天数据」的场景（如 Binance 每日打款导出）。
    状态：两侧都有→|差额|≤容差 一致 / 否则 金额不符；仅 admin→平台缺失；仅平台→平台多余。
    columns：8 个显示列名，按 _RECON_FIELDS 顺序对应；缺省用内部字段名。
    """
    lab = {**_DEFAULT_RECON_LABELS, **(labels or {})}
    cols = list(columns) if columns and len(columns) == len(_RECON_FIELDS) else list(_RECON_FIELDS)

    # admin 侧：(id, date) → (sum, cnt)
    admin: Dict[tuple, tuple] = {}
    for _, r in admin_agg.iterrows():
        admin[(str(r["_id"]).strip(), format_date(r["_date"]))] = (float(r["_sum"]), int(r["_cnt"]))

    if date_match_mode == "period":
        collapsed: Dict[tuple, tuple] = {}
        for (i, _d), (s, c) in admin.items():
            s0, c0 = collapsed.get((i, ""), (0.0, 0))
            collapsed[(i, "")] = (s0 + s, c0 + c)
        admin = collapsed

    # 平台侧：按日期口径归属业务日后累加
    plat: Dict[tuple, tuple] = {}
    for _, r in platform_agg.iterrows():
        i, pday = str(r["_id"]).strip(), format_date(r["_date"])
        if date_match_mode == "period":
            bd = ""
        elif date_match_mode == "t1_window":
            if (i, pday) in admin:
                bd = pday
            elif (i, _shift_day(pday, -1)) in admin:
                bd = _shift_day(pday, -1)
            else:
                bd = pday
        elif date_match_mode == "t1_shift":
            bd = _shift_day(pday, -1)      # 平台上传日 T → 业务日 T-1（恒定，不试同日）
        else:  # exact
            bd = pday
        s0, c0 = plat.get((i, bd), (0.0, 0))
        plat[(i, bd)] = (s0 + float(r["_sum"]), c0 + int(r["_cnt"]))

    rows: List[dict] = []
    for (i, d) in sorted(set(admin) | set(plat)):
        a = admin.get((i, d))
        p = plat.get((i, d))
        a_amt = round(a[0], 8) if a else 0.0
        p_amt = round(p[0], 8) if p else 0.0
        diff = round(p_amt - a_amt, 8)
        if a and p:
            status = lab["consistent"] if abs(diff) <= tolerance else lab["amount_diff"]
        elif a:
            status = lab["platform_missing"]
        else:
            status = lab["platform_extra"]
        values = [
            d, i,
            a_amt if a else "", a[1] if a else "",
            p_amt if p else "", p[1] if p else "",
            diff, status,
        ]
        rows.append(dict(zip(cols, values)))

    return pd.DataFrame(rows, columns=cols)
