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
    手续费        - 平台收取的手续费（IBFYPAY 无此字段，留空串）
    到账金额      - 扣除手续费后实际到账（IBFYPAY 留空串）
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
    IBFYPAY_PLATFORM_NO_COL_5,
    IBFYPAY_AMOUNT_COL_5,
    IBFYPAY_STATUS_COL_5,
    IBFYPAY_CREATE_TIME_COL_5,
    IBFYPAY_FINISH_TIME_COL_5,
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
    # TODO: 参考 app3._format_date，支持 datetime/date 对象、字符串多种格式
    raise NotImplementedError("TODO: _format_date_5")


def _to_float_5(val) -> Optional[float]:
    """将字符串金额转为 float，支持千分位逗号，失败返回 None。"""
    # TODO: 参考 app3._to_float，去除逗号后 float()
    raise NotImplementedError("TODO: _to_float_5")


def _dedup_lookup_5(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    """过滤空 key 行，对 key_col 去重（保留首行），发现重复时打 warning。"""
    # TODO: 参考 app3._dedup_lookup
    raise NotImplementedError("TODO: _dedup_lookup_5")


# ---------------------------------------------------------------------------
# 文件扫描
# ---------------------------------------------------------------------------

def scan_source_files_5(input_dir: Path) -> dict:
    """扫描输入目录，按平台识别文件，返回 {"admin": [...], "ibfpay": [...], ...}。

    识别规则（文件名 stem 小写前缀匹配，来自 PLATFORM_PREFIXES_5）：
        admin-          → admin 后台主表（XLS，engine=xlrd）
        ibfpay-         → IBFYPAY 平台（XLSX）
        superpay-       → SUPERPAY 平台（XLSX）
        wangupay- 或 wangguypay- → WANGGUYPAY 平台（XLSX）

    跳过临时文件（"~$" 前缀）和非 xls/xlsx 格式。
    同平台多文件时全部收录，由调用方合并。
    """
    result: dict = {key: [] for key in PLATFORM_PREFIXES_5}

    # TODO:
    # for f in sorted(input_dir.iterdir()):
    #     if not f.is_file() or f.name.startswith("~$"):
    #         continue
    #     if f.suffix.lower() not in {".xls", ".xlsx"}:
    #         continue
    #     stem = f.stem.lower()
    #     matched = False
    #     for platform_key, prefixes in PLATFORM_PREFIXES_5.items():
    #         if any(stem.startswith(p) for p in prefixes):
    #             result[platform_key].append(f)
    #             matched = True
    #             break
    #     if not matched:
    #         logging.warning("未识别文件，已跳过: %s", f.name)
    # for key, files in result.items():
    #     if len(files) > 1:
    #         logging.info("%s 平台发现 %d 个文件，将合并: %s", key, len(files), [f.name for f in files])

    raise NotImplementedError("TODO: scan_source_files_5")


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
    # TODO: 参考 app3._select_sheet_by_columns 完整实现
    raise NotImplementedError("TODO: _select_sheet_by_columns_5")


def _find_sheet_with_col_5(xls: pd.ExcelFile, col: str) -> str:
    """在 ExcelFile 的所有 sheet 中找到第一个含指定列的 sheet 名称。

    专用于 admin XLS 的 sheet 回退查找（engine=xlrd）。

    Raises:
        ValueError: 未找到时抛出，提示可用 sheet 列表。
    """
    # TODO:
    # for s in xls.sheet_names:
    #     preview = pd.read_excel(xls, sheet_name=s, nrows=0, engine="xlrd")
    #     if col in _normalize_columns_5(preview.columns):
    #         return s
    # raise ValueError(f"所有 sheet 均不含列 '{col}'，可用 sheet: {xls.sheet_names}")
    raise NotImplementedError("TODO: _find_sheet_with_col_5")


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
    # TODO:
    # with pd.ExcelFile(filepath, engine="xlrd") as xls:
    #     sheet_names = xls.sheet_names
    #     if ADMIN_SHEET_5 in sheet_names:
    #         target = ADMIN_SHEET_5
    #     else:
    #         logging.warning("admin 文件未找到 sheet '%s'，自动查找含 '%s' 列的 sheet",
    #                         ADMIN_SHEET_5, ADMIN_JOIN_COL_5)
    #         target = _find_sheet_with_col_5(xls, ADMIN_JOIN_COL_5)
    # df = pd.read_excel(filepath, sheet_name=target, dtype=str, engine="xlrd")
    # return df.dropna(how="all").fillna("")
    raise NotImplementedError("TODO: read_admin_5")


def read_ibfpay_5(filepath: Path) -> pd.DataFrame:
    """读取 IBFYPAY 平台文件（XLSX，sheet="Sheet"，header=0），全列字符串。

    Args:
        filepath: IBFYPAY XLSX 文件路径。

    Returns:
        包含 IBFYPAY 全部列的 DataFrame。
    """
    # TODO:
    # sheet = _select_sheet_by_columns_5(
    #     filepath,
    #     default_sheet=IBFYPAY_SHEET_5,
    #     header=IBFYPAY_HEADER_5,
    #     required_columns=[IBFYPAY_JOIN_COL_5, IBFYPAY_AMOUNT_COL_5, IBFYPAY_STATUS_COL_5],
    #     label="IBFYPAY",
    # )
    # df = pd.read_excel(filepath, sheet_name=sheet, header=IBFYPAY_HEADER_5, dtype=str)
    # return df.dropna(how="all").fillna("")
    raise NotImplementedError("TODO: read_ibfpay_5")


def read_superpay_5(filepath: Path) -> pd.DataFrame:
    """读取 SUPERPAY 平台文件（XLSX，sheet="sheet1"，header=0），全列字符串。

    Args:
        filepath: SUPERPAY XLSX 文件路径。

    Returns:
        包含 SUPERPAY 全部列的 DataFrame。
    """
    # TODO:
    # sheet = _select_sheet_by_columns_5(
    #     filepath,
    #     default_sheet=SUPERPAY_SHEET_5,
    #     header=SUPERPAY_HEADER_5,
    #     required_columns=[SUPERPAY_JOIN_COL_5, SUPERPAY_AMOUNT_COL_5, SUPERPAY_STATUS_COL_5],
    #     label="SUPERPAY",
    # )
    # df = pd.read_excel(filepath, sheet_name=sheet, header=SUPERPAY_HEADER_5, dtype=str)
    # return df.dropna(how="all").fillna("")
    raise NotImplementedError("TODO: read_superpay_5")


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
    # TODO:
    # sheet = _select_sheet_by_columns_5(
    #     filepath,
    #     default_sheet=WANGGUYPAY_SHEET_5,
    #     header=WANGGUYPAY_HEADER_5,   # 必须是 1
    #     required_columns=[WANGGUYPAY_JOIN_COL_5, WANGGUYPAY_AMOUNT_COL_5, WANGGUYPAY_STATUS_COL_5],
    #     label="WANGGUYPAY",
    # )
    # df = pd.read_excel(filepath, sheet_name=sheet, header=WANGGUYPAY_HEADER_5, dtype=str)
    # return df.dropna(how="all").fillna("")
    raise NotImplementedError("TODO: read_wangguypay_5")


# ---------------------------------------------------------------------------
# 查找表构建
# ---------------------------------------------------------------------------

def build_ibfpay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 IBFYPAY_JOIN_COL_5（订单号）为索引的 IBFYPAY 查找表。

    注意：IBFYPAY 无手续费列和到账金额列，enrich_admin_5 对这类行
    填充 FEE_COL_5="" 和 ARRIVE_AMOUNT_COL_5=""，以区别于真实的零值。

    Args:
        df: read_ibfpay_5 返回的原始 DataFrame。

    Returns:
        以 IBFYPAY_JOIN_COL_5 为索引的 lookup DataFrame。
    """
    # TODO:
    # keep_cols = [c for c in [
    #     IBFYPAY_JOIN_COL_5,
    #     IBFYPAY_PLATFORM_NO_COL_5,
    #     IBFYPAY_AMOUNT_COL_5,
    #     IBFYPAY_STATUS_COL_5,
    #     IBFYPAY_CREATE_TIME_COL_5,
    #     IBFYPAY_FINISH_TIME_COL_5,
    # ] if c in df.columns]
    # result = _dedup_lookup_5(df[keep_cols], IBFYPAY_JOIN_COL_5, "IBFYPAY")
    # return result.set_index(IBFYPAY_JOIN_COL_5)
    raise NotImplementedError("TODO: build_ibfpay_lookup_5")


def build_superpay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 SUPERPAY_JOIN_COL_5（商户订单号）为索引的 SUPERPAY 查找表。

    手续费字段：SUPERPAY_FEE_TOTAL_COL_5 → FEE_COL_5
    到账金额字段：SUPERPAY_ACTUAL_COL_5 → ARRIVE_AMOUNT_COL_5

    Args:
        df: read_superpay_5 返回的原始 DataFrame。

    Returns:
        以 SUPERPAY_JOIN_COL_5 为索引的 lookup DataFrame。
    """
    # TODO:
    # keep_cols = [c for c in [
    #     SUPERPAY_JOIN_COL_5,
    #     SUPERPAY_PLATFORM_NO_COL_5,
    #     SUPERPAY_AMOUNT_COL_5,
    #     SUPERPAY_FEE_TOTAL_COL_5,
    #     SUPERPAY_ACTUAL_COL_5,
    #     SUPERPAY_STATUS_COL_5,
    #     SUPERPAY_CREATE_TIME_COL_5,
    #     SUPERPAY_FINISH_TIME_COL_5,
    # ] if c in df.columns]
    # result = _dedup_lookup_5(df[keep_cols], SUPERPAY_JOIN_COL_5, "SUPERPAY")
    # return result.set_index(SUPERPAY_JOIN_COL_5)
    raise NotImplementedError("TODO: build_superpay_lookup_5")


def build_wangguypay_lookup_5(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 WANGGUYPAY_JOIN_COL_5（商户订单号）为索引的 WANGGUYPAY 查找表。

    手续费字段：WANGGUYPAY_FEE_COL_5 → FEE_COL_5
    到账金额字段：WANGGUYPAY_ARRIVE_COL_5 → ARRIVE_AMOUNT_COL_5

    Args:
        df: read_wangguypay_5 返回的原始 DataFrame。

    Returns:
        以 WANGGUYPAY_JOIN_COL_5 为索引的 lookup DataFrame。
    """
    # TODO: 结构与 build_superpay_lookup_5 完全对称
    # keep_cols = [c for c in [
    #     WANGGUYPAY_JOIN_COL_5,
    #     WANGGUYPAY_PLATFORM_NO_COL_5,
    #     WANGGUYPAY_AMOUNT_COL_5,
    #     WANGGUYPAY_FEE_COL_5,
    #     WANGGUYPAY_ARRIVE_COL_5,
    #     WANGGUYPAY_STATUS_COL_5,
    #     WANGGUYPAY_CREATE_TIME_COL_5,
    #     WANGGUYPAY_FINISH_TIME_COL_5,
    # ] if c in df.columns]
    # result = _dedup_lookup_5(df[keep_cols], WANGGUYPAY_JOIN_COL_5, "WANGGUYPAY")
    # return result.set_index(WANGGUYPAY_JOIN_COL_5)
    raise NotImplementedError("TODO: build_wangguypay_lookup_5")


# ---------------------------------------------------------------------------
# 主匹配逻辑
# ---------------------------------------------------------------------------

def _build_platform_only_rows_5(
    result_cols: list,
    admin_keys: Set[str],
    ibfpay_lk: Optional[pd.DataFrame],
    superpay_lk: Optional[pd.DataFrame],
    wangguypay_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """构建平台有、admin 无的多余行（MATCH_STATUS_COL_5 = "平台多余"）。

    对三个平台各自遍历 index，凡不在 admin_keys 中的键均生成一行，
    admin 原始列留空，填充可从平台数据推断的新增列。

    IBFYPAY 段：FEE_COL_5 / ARRIVE_AMOUNT_COL_5 留空（平台无此数据）
    SUPERPAY 段：FEE_COL_5 = 手续费，ARRIVE_AMOUNT_COL_5 = 实收
    WANGGUYPAY 段：FEE_COL_5 = 手续费(try)，ARRIVE_AMOUNT_COL_5 = 到账金额(try)
    """
    # TODO: 参考 app3._build_platform_only_rows 的三段式结构
    raise NotImplementedError("TODO: _build_platform_only_rows_5")


def enrich_admin_5(
    admin_df: pd.DataFrame,
    ibfpay_lk: Optional[pd.DataFrame],
    superpay_lk: Optional[pd.DataFrame],
    wangguypay_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """以 admin 为主表，通过订单号与三平台查找表 left-join，追加新增列。

    匹配逻辑（优先级：IBFYPAY > SUPERPAY > WANGGUYPAY）：
      - 三平台均以 ADMIN_JOIN_COL_5（订单号）关联
      - 任一平台命中 → 是否匹配=是，填充该平台的流水号/金额/状态/手续费/到账金额
      - 均未命中 → 是否匹配=否
      - 追加平台多余行（_build_platform_only_rows_5）

    注意：
      - 理论上一笔订单只在一个平台出现，命中多平台时打 warning
      - merge 后行数须与 admin 原始行数相等（_dedup_lookup_5 保证无重复键）
      - IBFYPAY 匹配行：FEE_COL_5="" / ARRIVE_AMOUNT_COL_5=""（区别于真实零值）

    Args:
        admin_df: read_admin_5 返回的 admin 主表。
        ibfpay_lk: build_ibfpay_lookup_5 返回值，可为 None（文件不存在时）。
        superpay_lk: build_superpay_lookup_5 返回值，可为 None。
        wangguypay_lk: build_wangguypay_lookup_5 返回值，可为 None。

    Returns:
        admin 原始列 + OUTPUT_NEW_COLS_5 的完整 DataFrame。
    """
    # TODO: 参考 app3.enrich_admin 的 _safe_merge + 逐行遍历 + 插入新列 + concat 多余行 模式
    raise NotImplementedError("TODO: enrich_admin_5")


def log_match_stats_5(result_df: pd.DataFrame) -> None:
    """按匹配状态打印统计摘要。未匹配数 > 0 时打 warning。"""
    # TODO:
    # total = len(result_df)
    # matched   = (result_df[MATCH_STATUS_COL_5] == "是").sum()
    # unmatched = (result_df[MATCH_STATUS_COL_5] == "否").sum()
    # extra     = (result_df[MATCH_STATUS_COL_5] == "平台多余").sum()
    # logging.info("共 %d 条 — 匹配 %d / 未匹配 %d / 平台多余 %d", total, matched, unmatched, extra)
    # if unmatched > 0:
    #     logging.warning("存在 %d 条未匹配记录，请查看【匹配失败订单】sheet", unmatched)
    raise NotImplementedError("TODO: log_match_stats_5")


def build_summary_sheet_5(result_df: pd.DataFrame) -> pd.DataFrame:
    """按平台汇总代付金额、手续费、到账金额（仅统计匹配成功的行）。

    分组键：机构列（ADMIN_ORG_COL_5）。
    汇总列：成功笔数、代付金额合计、手续费合计、到账金额合计。

    Args:
        result_df: enrich_admin_5 返回的完整结果 DataFrame。

    Returns:
        汇总 DataFrame，供写入 OUTPUT_SUMMARY_SHEET_5。
    """
    # TODO
    raise NotImplementedError("TODO: build_summary_sheet_5")


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
    # TODO:
    # today = date.today().strftime("%Y%m%d")
    # filename = OUTPUT_FILE_TEMPLATE_5.format(date=today)
    # output_path = output_dir / filename
    # output_dir.mkdir(parents=True, exist_ok=True)
    #
    # failed_df  = result_df[result_df[MATCH_STATUS_COL_5] == "否"].copy()
    # summary_df = build_summary_sheet_5(result_df)
    #
    # with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    #     result_df.to_excel(writer, sheet_name=OUTPUT_SHEET_5, index=False)
    #     failed_df.to_excel(writer, sheet_name=OUTPUT_FAILED_SHEET_5, index=False)
    #     summary_df.to_excel(writer, sheet_name=OUTPUT_SUMMARY_SHEET_5, index=False)
    #     for sname in (OUTPUT_SHEET_5, OUTPUT_FAILED_SHEET_5):
    #         ws = writer.sheets[sname]
    #         ws.freeze_panes = "A2"
    #         ws.auto_filter.ref = ws.dimensions
    # logging.info("结果文件已写入: %s", output_path)
    # return output_path
    raise NotImplementedError("TODO: write_output_5")


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
    # TODO: files = scan_source_files_5(INPUT_DIR_5)
    files: dict = {key: [] for key in PLATFORM_PREFIXES_5}

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
    # TODO:
    # admin_frames = [read_admin_5(fp) for fp in files["admin"]]
    # admin_df = pd.concat(admin_frames, ignore_index=True) if len(admin_frames) > 1 else admin_frames[0]
    # logging.info("admin 共 %d 条记录", len(admin_df))

    # ── IBFYPAY ───────────────────────────────────────────────
    ibfpay_lk: Optional[pd.DataFrame] = None
    # TODO:
    # if files["ibfpay"]:
    #     frames = [read_ibfpay_5(fp) for fp in files["ibfpay"]]
    #     raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    #     ibfpay_lk = build_ibfpay_lookup_5(raw)
    #     logging.info("IBFYPAY 查找表共 %d 条", len(ibfpay_lk))

    # ── SUPERPAY ──────────────────────────────────────────────
    superpay_lk: Optional[pd.DataFrame] = None
    # TODO:
    # if files["superpay"]:
    #     frames = [read_superpay_5(fp) for fp in files["superpay"]]
    #     raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    #     superpay_lk = build_superpay_lookup_5(raw)
    #     logging.info("SUPERPAY 查找表共 %d 条", len(superpay_lk))

    # ── WANGGUYPAY ────────────────────────────────────────────
    wangguypay_lk: Optional[pd.DataFrame] = None
    # TODO:
    # if files["wangguypay"]:
    #     frames = [read_wangguypay_5(fp) for fp in files["wangguypay"]]
    #     raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    #     wangguypay_lk = build_wangguypay_lookup_5(raw)
    #     logging.info("WANGGUYPAY 查找表共 %d 条", len(wangguypay_lk))

    # ── 匹配 ──────────────────────────────────────────────────
    # TODO:
    # result_df = enrich_admin_5(admin_df, ibfpay_lk, superpay_lk, wangguypay_lk)
    # log_match_stats_5(result_df)

    # ── 输出 ──────────────────────────────────────────────────
    # TODO:
    # try:
    #     output_path = write_output_5(result_df, OUTPUT_DIR)
    #     logging.info("完成。输出文件: %s", output_path)
    # except PermissionError:
    #     logging.error("无法写入输出文件，请确认文件未在 Excel 中打开后重试。")
    #     return 1
    # except Exception:
    #     logging.error("写入输出文件失败", exc_info=True)
    #     return 1

    logging.info("代号5框架初始化完成，具体匹配逻辑待实现（TODO）。")
    return 0
