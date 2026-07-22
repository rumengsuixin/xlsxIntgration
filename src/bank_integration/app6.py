"""代号6（代收代付对账）应用逻辑。

平台接入已外置化（两层）：
    platforms/6/*.json    声明式平台（结构类似的平台改配置即接入，免重打包）
    platforms/plugins/*.py 疑难平台插件（自定义 handler，放文件即生效，免重打包）
内置默认平台声明见 config6.BUILTIN_SPECS_6；注册表加载见 platform_loader；
通用读取 / 查找表 / 匹配见 platform_engine。本模块只保留代号6 特有的
admin 主表读取、输出/汇总，以及把上述通用能力接线成主流程的薄壳。

数据流：
    data/input/6/（源文件均支持 .csv/.xls/.xlsx，按扩展名自适应读取）
        Admin收款订单明细*   → 代收主表
        Admin兑换订单明细*   → 代付主表
        <平台>收款/付文件     → 由注册表（内置+外置）按前缀识别、方向归类
                  ↓
        load_platform_registry("6")  → scan_source_files_6
                  ↓
        read_admin_* / handler.read → handler.build_lookup（归一化到 CANON 内部列）
                  ↓
        enrich_admin_generic()  ← 按优先级 left-join（代收/代付各一次）
                  ↓
        data/output/代收代付对账结果_{YYYYMMDD}.xlsx（6 个 sheet）

对账逻辑：
    代收：Admin收款.订单号 ↔ 各平台关联键（见各平台 spec.join_col）
    代付：Admin兑换.订单号 ↔ 各平台关联键
    匹配优先级：由各平台 spec.priority 决定（内置 Betcat=10 < Cashnewpay=20 < Goldenpay=30）
"""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .config import OUTPUT_DIR
from .config6 import (
    ADMIN_COLLECTION_JOIN_COL_6,
    ADMIN_COLLECTION_PREFIXES_6,
    ADMIN_COLLECTION_SHEET_6,
    ADMIN_PAYOUT_JOIN_COL_6,
    ADMIN_PAYOUT_PREFIXES_6,
    ADMIN_PAYOUT_SHEET_6,
    BUILTIN_SPECS_6,
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
    PLATFORM_AMOUNT_COL_6,
    PLATFORM_ORDER_NO_COL_6,
    PLATFORM_SOURCE_COL_6,
    PLATFORM_STATUS_COL_6,
    SUMMARY_AMOUNT_COL_6,
    SUMMARY_ARRIVE_COL_6,
    SUMMARY_COUNT_COL_6,
    SUMMARY_FEE_COL_6,
    SUMMARY_MONTH_COL_6,
    SUMMARY_PLATFORM_COL_6,
    SUMMARY_TYPE_COL_6,
    TRANSACTION_DATE_COL_6,
)
from .platform_spec import DIRECTIONS, OutputSchema, PlatformSpec
from .platform_engine import (
    build_lookup_from_columns,
    dedup_lookup as _dedup_lookup_6,          # noqa: F401  （兼容别名）
    enrich_admin_generic,
    format_date as _format_date_6,
    generic_build_lookup,
    normalize_columns as _normalize_columns_6,  # noqa: F401  （兼容别名）
    read_source_table as _read_source_table_6,
    resolve_handler,
    to_float as _to_float_6,
)
from .platform_loader import load_platform_registry

logger = logging.getLogger(__name__)


# ── 代号6 输出 schema（供通用 enrich 使用）─────────────────────────────────────
_SCHEMA_6 = OutputSchema(
    match_status_col=MATCH_STATUS_COL_6,
    platform_source_col=PLATFORM_SOURCE_COL_6,
    platform_order_no_col=PLATFORM_ORDER_NO_COL_6,
    platform_amount_col=PLATFORM_AMOUNT_COL_6,
    platform_status_col=PLATFORM_STATUS_COL_6,
    fee_col=FEE_COL_6,
    transaction_date_col=TRANSACTION_DATE_COL_6,
    admin_join_candidates=[ADMIN_COLLECTION_JOIN_COL_6, ADMIN_PAYOUT_JOIN_COL_6],
)


def _builtin_spec_6(key: str) -> PlatformSpec:
    """按 key 从内置声明构造 PlatformSpec（供薄壳 build_*_lookup 使用，不读外置）。"""
    for data in BUILTIN_SPECS_6:
        if data["key"] == key:
            return PlatformSpec.from_dict(data)
    raise KeyError(f"未知内置平台: {key}")


# ─────────────────────────────────────────────────────────────────────────────
# 文件扫描（注册表驱动）
# ─────────────────────────────────────────────────────────────────────────────

def scan_source_files_6(
    input_dir: Path,
    specs: Optional[List[PlatformSpec]] = None,
) -> Dict[str, object]:
    """扫描输入目录，按 admin 前缀 + 各平台方向前缀识别文件。

    返回结构：
    {
        "admin_collection": [Path, ...],
        "admin_payout":     [Path, ...],
        "platforms": { "<KEY>": {"collection": [Path...], "payout": [Path...]}, ... },
    }

    规则：跳过 ~$ 临时文件；扩展名 .xlsx/.xls/.csv；stem 小写后 startswith 前缀即命中。
    识别顺序：admin 优先，其后按 specs（优先级）顺序，各平台 collection→payout。
    """
    if specs is None:
        specs = load_platform_registry("6")

    result: Dict[str, object] = {
        "admin_collection": [],
        "admin_payout": [],
        "platforms": {s.key: {"collection": [], "payout": []} for s in specs},
    }

    if not input_dir.exists():
        logger.warning("输入目录不存在: %s", input_dir)
        return result

    # (前缀列表, 目标描述) —— 目标为 ("admin_collection",) / ("admin_payout",) / ("platform", key, direction)
    matchers: List[tuple] = [
        (ADMIN_COLLECTION_PREFIXES_6, ("admin_collection",)),
        (ADMIN_PAYOUT_PREFIXES_6, ("admin_payout",)),
    ]
    for s in specs:
        for direction in DIRECTIONS:
            d = s.directions.get(direction)
            if d and d.prefixes:
                matchers.append((d.prefixes, ("platform", s.key, direction)))

    for f in sorted(input_dir.iterdir()):
        if not f.is_file() or f.name.startswith("~$"):
            continue
        if f.suffix.lower() not in {".xls", ".xlsx", ".csv"}:
            continue
        stem = f.stem.lower()
        matched = False
        for prefixes, target in matchers:
            if any(stem.startswith(p) for p in prefixes):
                if target[0] == "platform":
                    result["platforms"][target[1]][target[2]].append(f)
                else:
                    result[target[0]].append(f)
                matched = True
                break
        if not matched:
            logger.warning("未识别文件，已跳过: %s", f.name)

    # 多文件合并日志
    for admin_key in ("admin_collection", "admin_payout"):
        if len(result[admin_key]) > 1:
            logger.info("%s 发现 %d 个文件，将合并", admin_key, len(result[admin_key]))
    for s in specs:
        for direction in DIRECTIONS:
            fl = result["platforms"][s.key][direction]
            if len(fl) > 1:
                logger.info(
                    "%s %s 发现 %d 个文件，将合并: %s",
                    s.key, direction, len(fl), [x.name for x in fl],
                )

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Admin 主表读取（代号6 特有）
# ─────────────────────────────────────────────────────────────────────────────

def read_admin_collection_6(filepath: Path) -> pd.DataFrame:
    """读取 Admin 收款订单主表（格式自适应：.csv/.xls/.xlsx）。

    Excel 优先 sheet=ADMIN_COLLECTION_SHEET_6（"已完成订单"），
    未命中则回退到含 ADMIN_COLLECTION_JOIN_COL_6 列的 sheet。
    """
    df = _read_source_table_6(
        filepath,
        preferred_sheet=ADMIN_COLLECTION_SHEET_6,
        fallback_join_col=ADMIN_COLLECTION_JOIN_COL_6,
        label="admin 收款",
    )
    # Excel 文本前缀单引号（防科学计数法）：openpyxl 会原样保留 '，需清除
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.lstrip("'")
    return df


def read_admin_payout_6(filepath: Path) -> pd.DataFrame:
    """读取 Admin 兑换订单主表（格式自适应：.csv/.xls/.xlsx）。

    Excel 优先 sheet=ADMIN_PAYOUT_SHEET_6（"Sheet1"），
    未命中则回退到含 ADMIN_PAYOUT_JOIN_COL_6 列的 sheet。
    """
    df = _read_source_table_6(
        filepath,
        preferred_sheet=ADMIN_PAYOUT_SHEET_6,
        fallback_join_col=ADMIN_PAYOUT_JOIN_COL_6,
        label="admin 兑换",
    )
    # Excel 文本前缀单引号（防科学计数法）：openpyxl 会原样保留 '，需清除
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.lstrip("'")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 查找表构建（薄壳，委派通用引擎；保留签名供既有测试/调用）
# ─────────────────────────────────────────────────────────────────────────────

def build_betcat_lookup_6(df: pd.DataFrame) -> pd.DataFrame:
    """构建 Betcat 查找表（内部列归一化为 CANON）。收/付列结构相同，方向不影响。"""
    return generic_build_lookup(_builtin_spec_6("BETCAT"), "collection", df)


def build_cashnewpay_lookup_6(df: pd.DataFrame) -> pd.DataFrame:
    """构建 Cashnewpay 查找表（内部列归一化为 CANON）。收/付列结构相同，方向不影响。"""
    return generic_build_lookup(_builtin_spec_6("CASHNEWPAY"), "collection", df)


def build_goldenpay_lookup_6(
    df: pd.DataFrame,
    platform_no_src: str,
    amount_src: str,
) -> pd.DataFrame:
    """构建 Goldenpay 查找表。收/付平台单号列与金额列名不同，由调用方传入源列名。

    与内置顶层 columns（手续费/状态/时间）合并后，交给通用归一化构建。
    """
    spec = _builtin_spec_6("GOLDENPAY")
    columns = dict(spec.columns)
    columns["platform_no"] = platform_no_src
    columns["amount"] = amount_src
    return build_lookup_from_columns(df, spec.join_col, columns, spec.key)


# ─────────────────────────────────────────────────────────────────────────────
# 主匹配逻辑（薄壳，委派通用引擎；保留签名供既有测试/调用）
# ─────────────────────────────────────────────────────────────────────────────

def enrich_admin_6(
    admin_df: pd.DataFrame,
    betcat_lk: Optional[pd.DataFrame],
    cashnewpay_lk: Optional[pd.DataFrame],
    goldenpay_lk: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """以 admin 为主表，left-join 三个内置平台查找表，追加 OUTPUT_NEW_COLS_6。

    保留原三平台位置参数签名（供既有测试直接调用）；内部转为按注册表优先级匹配。
    匹配优先级由各平台 spec.priority 决定（内置 Betcat < Cashnewpay < Goldenpay）。
    """
    specs = load_platform_registry("6")
    lookups: Dict[str, Optional[pd.DataFrame]] = {
        "BETCAT": betcat_lk,
        "CASHNEWPAY": cashnewpay_lk,
        "GOLDENPAY": goldenpay_lk,
    }
    return enrich_admin_generic(admin_df, lookups, specs, _SCHEMA_6)


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

    输出 6 个 sheet：代收结果 / 代收匹配失败 / 代付结果 / 代付匹配失败 / 平台汇总 / 金额差异订单。
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

def _read_admin_frames(filepaths, reader, label: str) -> Optional[pd.DataFrame]:
    """读取一个或多个 admin 文件并合并；无文件返回 None。"""
    if not filepaths:
        return None
    frames = [reader(fp) for fp in filepaths]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    logger.info("%s共 %d 条", label, len(df))
    return df


def _build_direction_lookups(
    files: Dict[str, object],
    specs: List[PlatformSpec],
    direction: str,
) -> Dict[str, pd.DataFrame]:
    """为某方向（collection/payout）构建各平台查找表：读取→合并→handler.build_lookup。"""
    lookups: Dict[str, pd.DataFrame] = {}
    platforms = files["platforms"]
    for s in specs:
        fps = platforms.get(s.key, {}).get(direction, [])
        if not fps:
            continue
        handler = resolve_handler(s)
        frames = [handler.read(s, direction, fp) for fp in fps]
        raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        lookups[s.key] = handler.build_lookup(s, direction, raw)
        logger.info("%s %s 查找表共 %d 条", s.key, direction, len(lookups[s.key]))
    return lookups


def main() -> int:
    """代号6 主流程：加载注册表 → 扫描 → 读取 → 构建查找表 → 匹配 → 输出。

    Returns:
        0 成功 / 1 失败
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── 加载平台注册表（内置 + 外置 JSON/插件）─────────────────────────────────
    specs = load_platform_registry("6")
    logger.info("已加载 %d 个平台: %s", len(specs), ", ".join(s.key for s in specs))

    # ── 扫描 ──────────────────────────────────────────────────────────────────
    files = scan_source_files_6(INPUT_DIR_6, specs)

    # ── Admin 主表必须存在 ─────────────────────────────────────────────────────
    if not files["admin_collection"] and not files["admin_payout"]:
        logger.error(
            "未找到任何 admin 文件。请将文件放入 %s，"
            "文件名须以 'admin收款' 或 'admin兑换' 开头。",
            INPUT_DIR_6,
        )
        return 1

    # ── 读取 Admin 主表 ────────────────────────────────────────────────────────
    admin_collection = _read_admin_frames(
        files["admin_collection"], read_admin_collection_6, "admin 收款订单")
    if admin_collection is None:
        logger.warning("未找到 admin 收款文件，跳过代收对账。")
    admin_payout = _read_admin_frames(
        files["admin_payout"], read_admin_payout_6, "admin 兑换订单")
    if admin_payout is None:
        logger.warning("未找到 admin 兑换文件，跳过代付对账。")

    # ── 构建各平台查找表（代收 / 代付两个方向）─────────────────────────────────
    collection_lookups = _build_direction_lookups(files, specs, "collection")
    payout_lookups = _build_direction_lookups(files, specs, "payout")

    # ── 代收对账 ──────────────────────────────────────────────────────────────
    if admin_collection is not None:
        collection_result = enrich_admin_generic(
            admin_collection, collection_lookups, specs, _SCHEMA_6)
        log_match_stats_6(collection_result, "代收")
    else:
        collection_result = pd.DataFrame(columns=OUTPUT_NEW_COLS_6)

    # ── 代付对账 ──────────────────────────────────────────────────────────────
    if admin_payout is not None:
        payout_result = enrich_admin_generic(
            admin_payout, payout_lookups, specs, _SCHEMA_6)
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
