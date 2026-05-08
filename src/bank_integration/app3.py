"""代号3（游戏订单匹配）应用逻辑。

数据流：
    data/input/3/  →  scan_source_files_3()
                           │
                      read_admin / read_adyen / read_huawei / read_google
                           │
                  build_adyen_lookup / build_huawei_lookup / build_google_lookup
                           │
                      enrich_admin()   ← left-join 三个查找表
                           │
                  data/output/订单匹配结果_{YYYYMMDD}.xlsx

输出新增列（追加在 admin 原始列末尾，共 7 列）：
    平台订单金额  - 各平台匹配到的订单交易金额
    平台币种      - 对应货币
    状态          - 成功 / 失败 / 退款
    结算金额      - 扣除平台手续费后实际到账金额
                    Adyen: Payable(SC) [USD/HKD]
                    Google: Charge(TRY) - |fee(TRY)|；退款: |refund| - |fee_refund|
                    华为: 空（平台报表无手续费数据）
    手续费        - 平台收取的手续费
                    Adyen: Markup+Scheme Fees+Interchange [USD/HKD]
                    Google: |Google fee Amount(Buyer Currency)| [TRY]
                    华为: 空
    国家税费      - Google 专属，= admin.金额 - Charge(TRY)（反映 Google 代扣当地税额）
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import OUTPUT_DIR
from .config3 import (
    ADMIN_AMOUNT_COL,
    ADMIN_DATE_COL,
    ADMIN_JOIN_COL,
    ADMIN_PAYMENT_COL,
    ADMIN_REFUND_COL,
    ADMIN_SHEET,
    ADYEN_AMOUNT_COL,
    ADYEN_CURRENCY_COL,
    ADYEN_DATE_COL,
    ADYEN_INTERCHANGE_COL,
    ADYEN_JOIN_COL,
    ADYEN_MARKUP_COL,
    ADYEN_PAYABLE_COL,
    ADYEN_RECORD_TYPE_COL,
    ADYEN_RECORD_TYPE_PRIORITY,
    ADYEN_SCHEME_FEES_COL,
    ADYEN_SHEET,
    COUNTRY_TAX_COL,
    FEE_COL,
    GOOGLE_BUYER_AMOUNT_COL,
    GOOGLE_BUYER_CURRENCY_COL,
    GOOGLE_CHARGE_TYPE,
    GOOGLE_DATE_COL,
    GOOGLE_FEE_REFUND_TYPE,
    GOOGLE_FEE_TYPE,
    GOOGLE_JOIN_COL,
    GOOGLE_REFUND_TYPE,
    GOOGLE_TRANSACTION_TYPE_COL,
    HUAWEI_AMOUNT_COL,
    HUAWEI_CURRENCY_COL,
    HUAWEI_DATE_COL,
    HUAWEI_JOIN_COL,
    HUAWEI_SHEET,
    INPUT_DIR_3,
    OUTPUT_FILE_TEMPLATE,
    OUTPUT_SHEET_3,
    PLATFORM_AMOUNT_COL,
    PLATFORM_CURRENCY_COL,
    SETTLEMENT_AMOUNT_COL,
    STATUS_COL,
    TRANSACTION_DATE_COL,
)


def scan_source_files_3(input_dir: Path) -> dict:
    """扫描输入目录，按平台识别文件。

    识别规则（文件名 stem 小写前缀匹配）：
        admin   → admin 文件
        adyen-  → Adyen 文件（含连字符以避免误匹配）
        华为    → 华为文件
        googol- 或 google-  → Google Play 文件

    返回 {"admin": Path|None, "adyen": Path|None, "huawei": Path|None, "google": Path|None}
    """
    result = {"admin": None, "adyen": None, "huawei": None, "google": None}
    buckets = {k: [] for k in result}  # type: dict

    for f in sorted(input_dir.glob("*.xlsx")):
        if f.name.startswith("~$"):
            continue
        stem = f.stem.lower()
        if stem.startswith("admin"):
            buckets["admin"].append(f)
        elif stem.startswith("adyen-"):
            buckets["adyen"].append(f)
        elif stem.startswith("华为"):
            buckets["huawei"].append(f)
        elif stem.startswith("googol-") or stem.startswith("google-"):
            buckets["google"].append(f)

    for key, files in buckets.items():
        if not files:
            continue
        if len(files) > 1:
            logging.warning(
                "在 %s 发现多个 %s 文件，将使用第一个：%s",
                input_dir,
                key,
                files[0].name,
            )
        result[key] = files[0]

    return result


def read_admin(filepath: Path) -> pd.DataFrame:
    """读取 admin 订单表（工作表：汇总），全列字符串。"""
    df = pd.read_excel(filepath, sheet_name=ADMIN_SHEET, dtype=str)
    return df.dropna(how="all").fillna("")


def read_adyen(filepath: Path) -> pd.DataFrame:
    """读取 Adyen 报告（工作表：Data），全列字符串。"""
    return pd.read_excel(filepath, sheet_name=ADYEN_SHEET, dtype=str).fillna("")


def read_huawei(filepath: Path) -> pd.DataFrame:
    """读取华为平台文件（工作表：Sheet0），全列字符串。"""
    df = pd.read_excel(filepath, sheet_name=HUAWEI_SHEET, dtype=str)
    return df.dropna(how="all").fillna("")


def read_google(filepath: Path) -> pd.DataFrame:
    """读取 Google Play 报告（第一个工作表），返回全部行。"""
    df = pd.read_excel(filepath, sheet_name=0, dtype=str)
    return df.fillna("")


def _dedup_lookup(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    """对 lookup 表按 key_col 去重，空 key 行过滤掉，发现重复时打 warning。"""
    df = df[df[key_col].str.strip() != ""].copy()
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    dupes = before - len(df)
    if dupes > 0:
        logging.warning("【%s】去重时发现 %d 条重复流水号，已保留首行", label, dupes)
    return df


def build_adyen_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 Psp Reference 为索引的 Adyen 查找表（每个 PSP 一行）。

    同一 Psp Reference 有多行（Received/Authorised/SentForSettle 等），
    按 ADYEN_RECORD_TYPE_PRIORITY 优先取最准确的行，其余丢弃。
    """
    priority_map = {rt: i for i, rt in enumerate(ADYEN_RECORD_TYPE_PRIORITY)}
    fallback = len(ADYEN_RECORD_TYPE_PRIORITY)

    df2 = df[df[ADYEN_JOIN_COL].str.strip() != ""].copy()
    df2["_p"] = df2[ADYEN_RECORD_TYPE_COL].map(priority_map).fillna(fallback).astype(int)
    df2 = df2.sort_values([ADYEN_JOIN_COL, "_p"])

    before = len(df2[ADYEN_JOIN_COL].unique())
    result = df2.drop_duplicates(subset=[ADYEN_JOIN_COL], keep="first")
    dupes = len(df2) - len(result)
    if dupes > 0:
        logging.warning("【Adyen】同一 PSP Reference 存在多行记录，共去重 %d 行（优先取 SentForSettle/Authorised）", dupes)

    keep_cols = [c for c in [
        ADYEN_JOIN_COL, ADYEN_AMOUNT_COL, ADYEN_CURRENCY_COL,
        ADYEN_PAYABLE_COL, ADYEN_MARKUP_COL, ADYEN_SCHEME_FEES_COL, ADYEN_INTERCHANGE_COL,
        ADYEN_DATE_COL,
    ] if c in result.columns]
    return result[keep_cols].fillna("").set_index(ADYEN_JOIN_COL)


def build_huawei_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 华为订单号 为索引的华为查找表。

    退款行在华为数据中为独立新行（华为订单号不同），正常不存在重复键；
    若出现重复则保留首行（原始成功记录在前）。
    """
    keep_cols = [c for c in [HUAWEI_JOIN_COL, HUAWEI_AMOUNT_COL, HUAWEI_CURRENCY_COL, HUAWEI_DATE_COL]
                 if c in df.columns]
    result = _dedup_lookup(df[keep_cols], HUAWEI_JOIN_COL, "华为")
    return result.fillna("").set_index(HUAWEI_JOIN_COL)


def _format_date(val) -> str:
    try:
        s = str(val).strip()
        return s[:10] if len(s) >= 10 else ""
    except Exception:
        return ""


def _to_float(val) -> Optional[float]:
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def build_google_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """构建以 Description 为索引的 Google Play 查找表，合并同一流水号的 4 种行类型。

    每笔订单在 Google 数据中最多出现 4 行：
      Charge          → 原始收款（正数）
      Google fee      → 平台手续费（负数）
      Charge refund   → 退款（负数）
      Google fee refund → 手续费退回（正数）

    对应 admin.是否退款：
      正常   → 平台金额 = Charge + Google fee（商户净收入，两者求和）
      已退款 → 平台金额 = |Charge refund + Google fee refund|（净退款金额）
    """
    type_col = GOOGLE_TRANSACTION_TYPE_COL
    join = GOOGLE_JOIN_COL
    amt = GOOGLE_BUYER_AMOUNT_COL
    ccy = GOOGLE_BUYER_CURRENCY_COL

    def _pick(type_val, out_amt_col, include_ccy=False, ccy_out_col="ccy",
              include_date=False, date_out_col="transaction_date"):
        sub = df[df[type_col].str.strip() == type_val].copy()
        sub = sub[sub[join].str.strip() != ""]
        cols = (
            [join, amt]
            + ([ccy] if include_ccy else [])
            + ([GOOGLE_DATE_COL] if include_date and GOOGLE_DATE_COL in sub.columns else [])
        )
        sub = sub[[c for c in cols if c in sub.columns]]
        before = len(sub)
        sub = sub.drop_duplicates(subset=[join], keep="first")
        if len(sub) < before:
            logging.warning("【Google-%s】发现 %d 条重复流水号，已保留首行", type_val, before - len(sub))
        rename = {amt: out_amt_col}
        if include_ccy:
            rename[ccy] = ccy_out_col
        if include_date and GOOGLE_DATE_COL in sub.columns:
            rename[GOOGLE_DATE_COL] = date_out_col
        return sub.rename(columns=rename)

    charge = _pick(GOOGLE_CHARGE_TYPE, "charge_amt", include_ccy=True, include_date=True)
    fee = _pick(GOOGLE_FEE_TYPE, "fee_amt")
    refund = _pick(GOOGLE_REFUND_TYPE, "refund_amt",
                   include_ccy=True, ccy_out_col="refund_ccy",
                   include_date=True, date_out_col="refund_date")
    fee_refund = _pick(GOOGLE_FEE_REFUND_TYPE, "fee_refund_amt")

    result = charge
    for part in (fee, refund, fee_refund):
        if not part.empty:
            result = result.merge(part, on=join, how="outer")

    # 退款订单（无原始 Charge 行）用 refund_date / refund_ccy 补充缺失字段
    if "refund_date" in result.columns:
        if "transaction_date" not in result.columns:
            result["transaction_date"] = None
        mask = result["transaction_date"].isna() | (result["transaction_date"] == "")
        result.loc[mask, "transaction_date"] = result.loc[mask, "refund_date"]
        result = result.drop(columns=["refund_date"])

    if "refund_ccy" in result.columns:
        if "ccy" not in result.columns:
            result["ccy"] = None
        mask = result["ccy"].isna() | (result["ccy"] == "")
        result.loc[mask, "ccy"] = result.loc[mask, "refund_ccy"]
        result = result.drop(columns=["refund_ccy"])

    return result.fillna("").drop_duplicates(subset=[join]).set_index(join)


def _build_platform_only_rows(
    result_cols: list,
    admin_keys: set,
    adyen_lk: Optional[pd.DataFrame],
    huawei_lk: Optional[pd.DataFrame],
    google_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """构建平台有、admin 无对应记录的多余行（回调丢失/网络延迟场景）。

    所有 admin 原始列留空，填充可从平台数据计算的 7 个新增列，状态统一标"失败"。
    """
    extra = []

    # ── Adyen 多余行 ────────────────────────────────────────
    if adyen_lk is not None:
        for key in adyen_lk.index:
            if key in admin_keys:
                continue
            row: dict = {c: "" for c in result_cols}
            row[ADMIN_JOIN_COL]     = key
            row[ADMIN_PAYMENT_COL]  = "Adyen"
            row[PLATFORM_AMOUNT_COL]   = str(adyen_lk.at[key, ADYEN_AMOUNT_COL]).strip()
            row[PLATFORM_CURRENCY_COL] = str(adyen_lk.at[key, ADYEN_CURRENCY_COL]).strip()
            row[STATUS_COL]            = "成功"
            row[SETTLEMENT_AMOUNT_COL] = str(adyen_lk.at[key, ADYEN_PAYABLE_COL]).strip()
            m   = _to_float(adyen_lk.at[key, ADYEN_MARKUP_COL])
            sch = _to_float(adyen_lk.at[key, ADYEN_SCHEME_FEES_COL])
            itc = _to_float(adyen_lk.at[key, ADYEN_INTERCHANGE_COL])
            if any(x is not None for x in [m, sch, itc]):
                row[FEE_COL] = str(round((m or 0.0) + (sch or 0.0) + (itc or 0.0), 4))
            if ADYEN_DATE_COL in adyen_lk.columns:
                row[TRANSACTION_DATE_COL] = _format_date(adyen_lk.at[key, ADYEN_DATE_COL])
            extra.append(row)

    # ── 华为多余行 ────────────────────────────────────────
    if huawei_lk is not None:
        for key in huawei_lk.index:
            if key in admin_keys:
                continue
            row = {c: "" for c in result_cols}
            row[ADMIN_JOIN_COL]     = key
            row[ADMIN_PAYMENT_COL]  = "华为支付"
            amt = str(huawei_lk.at[key, HUAWEI_AMOUNT_COL]).strip()
            row[PLATFORM_AMOUNT_COL]   = amt
            row[PLATFORM_CURRENCY_COL] = str(huawei_lk.at[key, HUAWEI_CURRENCY_COL]).strip()
            row[SETTLEMENT_AMOUNT_COL] = amt  # 暂无手续费数据
            row[STATUS_COL]            = "成功"
            if HUAWEI_DATE_COL in huawei_lk.columns:
                row[TRANSACTION_DATE_COL] = _format_date(huawei_lk.at[key, HUAWEI_DATE_COL])
            extra.append(row)

    # ── Google 多余行 ─────────────────────────────────────
    if google_lk is not None:
        for key in google_lk.index:
            if key in admin_keys:
                continue
            row = {c: "" for c in result_cols}
            row[ADMIN_JOIN_COL]    = key
            row[ADMIN_PAYMENT_COL] = "Google支付"
            r  = _to_float(google_lk.at[key, "refund_amt"])
            c_ = _to_float(google_lk.at[key, "charge_amt"])
            f  = _to_float(google_lk.at[key, "fee_amt"])
            fr = _to_float(google_lk.at[key, "fee_refund_amt"])
            row[PLATFORM_CURRENCY_COL] = str(google_lk.at[key, "ccy"]).strip()
            if r is not None:  # 有退款记录 → 退款
                row[PLATFORM_AMOUNT_COL]   = str(round(abs(r or 0.0) + abs(fr or 0.0), 2))
                row[SETTLEMENT_AMOUNT_COL] = str(round(abs(r or 0.0) - abs(fr or 0.0), 2))
                if fr is not None:
                    row[FEE_COL] = str(round(abs(fr), 2))
                row[STATUS_COL] = "退款"
            else:              # 无退款记录 → 成功
                if c_ is not None:
                    row[PLATFORM_AMOUNT_COL]   = str(round(abs(c_ or 0.0) + abs(f or 0.0), 2))
                    row[SETTLEMENT_AMOUNT_COL] = str(round((c_ or 0.0) - abs(f or 0.0), 2))
                    # 国家税费：倒推 admin.金额 = Charge × 1.2，国家税费 = admin.金额 - Charge = Charge × 0.2
                    row[COUNTRY_TAX_COL] = str(round(abs(c_) * 0.2, 2))
                if f is not None:
                    row[FEE_COL] = str(round(abs(f), 2))
                row[STATUS_COL] = "成功"
            if "transaction_date" in google_lk.columns:
                row[TRANSACTION_DATE_COL] = _format_date(google_lk.at[key, "transaction_date"])
            extra.append(row)

    if not extra:
        return pd.DataFrame(columns=result_cols)
    return pd.DataFrame(extra, columns=result_cols)


def enrich_admin(
    admin_df: pd.DataFrame,
    adyen_lk: Optional[pd.DataFrame],
    huawei_lk: Optional[pd.DataFrame],
    google_lk: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """以 admin 为主表，通过流水号与三个平台查找表 left-join，追加 3 列。"""
    result = admin_df.copy()
    admin_col_count = len(admin_df.columns)

    expected_rows = len(admin_df)

    def _safe_merge(base, lookup, prefix, platform_name):
        merged = base.merge(
            lookup.add_prefix(prefix),
            left_on=ADMIN_JOIN_COL, right_index=True, how="left",
        )
        if len(merged) != expected_rows:
            raise ValueError(
                f"【{platform_name}】merge 后行数从 {expected_rows} 变为 {len(merged)}，"
                f"平台数据仍存在重复流水号，请检查 build_{platform_name.lower()}_lookup 去重逻辑"
            )
        for c in lookup.columns:
            merged[f"{prefix}{c}"] = merged[f"{prefix}{c}"].fillna("")
        return merged

    # ── Adyen join ───────────────────────────────────────────
    adyen_avail = adyen_lk is not None and not adyen_lk.empty
    if adyen_avail:
        result = _safe_merge(result, adyen_lk, "_a_", "Adyen")

    # ── 华为 join ─────────────────────────────────────────────
    huawei_avail = huawei_lk is not None and not huawei_lk.empty
    if huawei_avail:
        result = _safe_merge(result, huawei_lk, "_h_", "华为")

    # ── Google join ───────────────────────────────────────────
    google_avail = google_lk is not None and not google_lk.empty
    if google_avail:
        result = _safe_merge(result, google_lk, "_g_", "Google")

    # ── 构建 7 个新列 ─────────────────────────────────────────
    platform_amt_list     = []
    platform_ccy_list     = []
    status_list           = []
    settlement_list       = []
    fee_list              = []
    country_tax_list      = []
    transaction_date_list = []

    for _, row in result.iterrows():
        src = str(row.get(ADMIN_PAYMENT_COL, "")).strip()
        refund_flag = str(row.get(ADMIN_REFUND_COL, "")).strip()

        settle_amt       = ""
        fee_amt          = ""
        country_tax      = ""
        transaction_date = ""

        if src == "Adyen" and adyen_avail:
            amt = str(row.get(f"_a_{ADYEN_AMOUNT_COL}", "")).strip()
            ccy = str(row.get(f"_a_{ADYEN_CURRENCY_COL}", "")).strip()
            matched = amt != ""
            # 结算金额：Payable (SC)
            settle_amt = str(row.get(f"_a_{ADYEN_PAYABLE_COL}", "")).strip()
            # 手续费：Markup + Scheme Fees + Interchange（均在 SentForSettle 行，SC 货币）
            m   = _to_float(row.get(f"_a_{ADYEN_MARKUP_COL}", ""))
            sch = _to_float(row.get(f"_a_{ADYEN_SCHEME_FEES_COL}", ""))
            itc = _to_float(row.get(f"_a_{ADYEN_INTERCHANGE_COL}", ""))
            if any(x is not None for x in [m, sch, itc]):
                fee_amt = str(round((m or 0.0) + (sch or 0.0) + (itc or 0.0), 4))
            transaction_date = _format_date(row.get(ADMIN_DATE_COL, ""))

        elif src == "华为支付" and huawei_avail:
            amt = str(row.get(f"_h_{HUAWEI_AMOUNT_COL}", "")).strip()
            ccy = str(row.get(f"_h_{HUAWEI_CURRENCY_COL}", "")).strip()
            matched = amt != ""
            settle_amt = amt  # 暂无手续费数据，结算金额 = 平台订单金额
            transaction_date = _format_date(row.get(ADMIN_DATE_COL, ""))

        elif "Google" in src and google_avail:
            ccy = str(row.get("_g_ccy", "")).strip()
            if refund_flag == "已退款":
                # 平台订单金额：|Charge refund| + |fee refund|
                r  = _to_float(row.get("_g_refund_amt", ""))
                fr = _to_float(row.get("_g_fee_refund_amt", ""))
                amt = str(round(abs(r or 0.0) + abs(fr or 0.0), 2)) if (r is not None or fr is not None) else ""
                # 结算金额：|refund| - |fee_refund|
                if r is not None:
                    settle_amt = str(round(abs(r or 0.0) - abs(fr or 0.0), 2))
                # 手续费：|fee_refund|
                if fr is not None:
                    fee_amt = str(round(abs(fr), 2))
            else:
                # 平台订单金额：|Charge| + |Google fee|
                c = _to_float(row.get("_g_charge_amt", ""))
                f = _to_float(row.get("_g_fee_amt", ""))
                amt = str(round(abs(c or 0.0) + abs(f or 0.0), 2)) if c is not None else ""
                # 结算金额：Charge(TRY) - |fee(TRY)|
                if c is not None:
                    settle_amt = str(round((c or 0.0) - abs(f or 0.0), 2))
                # 手续费：|Google fee|
                if f is not None:
                    fee_amt = str(round(abs(f), 2))
                # 国家税费：admin.金额 - Charge(TRY)
                admin_amt = _to_float(row.get(ADMIN_AMOUNT_COL, ""))
                if admin_amt is not None and c is not None:
                    country_tax = str(round(admin_amt - c, 2))
            matched = amt != ""
            transaction_date = _format_date(row.get(ADMIN_DATE_COL, ""))

        else:
            amt = ccy = ""
            matched = False

        platform_amt_list.append(amt)
        platform_ccy_list.append(ccy)
        settlement_list.append(settle_amt)
        fee_list.append(fee_amt)
        country_tax_list.append(country_tax)
        transaction_date_list.append(transaction_date)

        if refund_flag == "已退款":
            status_list.append("退款")
        elif matched:
            status_list.append("成功")
        else:
            status_list.append("失败")

    # ── 清理临时列，插入 6 个新列 ─────────────────────────────
    tmp_cols = [c for c in result.columns if c.startswith(("_a_", "_h_", "_g_"))]
    result = result.drop(columns=tmp_cols)

    result.insert(admin_col_count + 0, PLATFORM_AMOUNT_COL,   platform_amt_list)
    result.insert(admin_col_count + 1, PLATFORM_CURRENCY_COL, platform_ccy_list)
    result.insert(admin_col_count + 2, STATUS_COL,            status_list)
    result.insert(admin_col_count + 3, SETTLEMENT_AMOUNT_COL, settlement_list)
    result.insert(admin_col_count + 4, FEE_COL,               fee_list)
    result.insert(admin_col_count + 5, COUNTRY_TAX_COL,       country_tax_list)
    result.insert(admin_col_count + 6, TRANSACTION_DATE_COL,  transaction_date_list)

    # ── 追加平台多余行（平台有、admin 无对应记录）────────────
    admin_keys = set(admin_df[ADMIN_JOIN_COL].str.strip())
    extra_df = _build_platform_only_rows(
        list(result.columns),
        admin_keys,
        adyen_lk if adyen_avail else None,
        huawei_lk if huawei_avail else None,
        google_lk if google_avail else None,
    )
    if not extra_df.empty:
        logging.info(
            "追加平台多余行：共 %d 行（在平台报表中存在但 admin 无对应记录，回调丢失或网络延迟）",
            len(extra_df),
        )
        result = pd.concat([result, extra_df], ignore_index=True)

    return result.fillna("")


def log_match_stats(result_df: pd.DataFrame) -> None:
    """按平台打印匹配统计（成功 / 失败 / 退款）。"""
    if ADMIN_PAYMENT_COL not in result_df.columns:
        return

    platform_map = [("Adyen", "Adyen"), ("华为支付", "华为支付"), ("Google支付", "Google支付")]
    for label, pay_val in platform_map:
        rows = result_df[result_df[ADMIN_PAYMENT_COL].str.strip() == pay_val]
        if rows.empty:
            continue
        success = (rows[STATUS_COL] == "成功").sum()
        failed = (rows[STATUS_COL] == "失败").sum()
        refund = (rows[STATUS_COL] == "退款").sum()
        logging.info("【%s】共 %d 条 — 成功 %d，失败 %d，退款 %d", label, len(rows), success, failed, refund)
        if failed > 0:
            ids = rows.loc[rows[STATUS_COL] == "失败", ADMIN_JOIN_COL].tolist()
            logging.warning("【%s】未匹配流水号: %s", label, ids)


def write_output(result_df: pd.DataFrame, output_dir: Path) -> Path:
    """将结果写入 data/output/订单匹配结果_{YYYYMMDD}.xlsx，单工作表。"""
    today = date.today().strftime("%Y%m%d")
    filename = OUTPUT_FILE_TEMPLATE.format(date=today)
    output_path = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name=OUTPUT_SHEET_3, index=False)

    return output_path


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    files = scan_source_files_3(INPUT_DIR_3)
    if files["admin"] is None:
        logging.error(
            "未找到 admin 订单文件。请将文件放入 %s，文件名须以 'admin'（不区分大小写）开头，"
            "例如：admin收入订单明细-xxx.xlsx",
            INPUT_DIR_3,
        )
        return 1

    logging.info("读取 admin 文件: %s", files["admin"].name)
    try:
        admin_df = read_admin(files["admin"])
    except Exception:
        logging.error("读取 admin 文件失败", exc_info=True)
        return 1
    logging.info("  admin 共 %d 条记录", len(admin_df))

    adyen_lk = huawei_lk = google_lk = None

    if files["adyen"]:
        logging.info("读取 Adyen 文件: %s", files["adyen"].name)
        try:
            adyen_lk = build_adyen_lookup(read_adyen(files["adyen"]))
            logging.info("  Adyen 去重后共 %d 个唯一 PSP Reference", len(adyen_lk))
        except Exception:
            logging.error("读取 Adyen 文件失败", exc_info=True)
    else:
        logging.warning("未找到 Adyen 文件，跳过 Adyen 匹配")

    if files["huawei"]:
        logging.info("读取华为文件: %s", files["huawei"].name)
        try:
            huawei_lk = build_huawei_lookup(read_huawei(files["huawei"]))
            logging.info("  华为共 %d 条记录", len(huawei_lk))
        except Exception:
            logging.error("读取华为文件失败", exc_info=True)
    else:
        logging.warning("未找到华为文件，跳过华为匹配")

    if files["google"]:
        logging.info("读取 Google Play 文件: %s", files["google"].name)
        try:
            google_lk = build_google_lookup(read_google(files["google"]))
            logging.info("  Google Play 共 %d 个唯一订单", len(google_lk))
        except Exception:
            logging.error("读取 Google Play 文件失败", exc_info=True)
    else:
        logging.warning("未找到 Google Play 文件，跳过 Google 匹配")

    result_df = enrich_admin(admin_df, adyen_lk, huawei_lk, google_lk)
    log_match_stats(result_df)

    try:
        output_path = write_output(result_df, OUTPUT_DIR)
        logging.info("结果文件已写入: %s", output_path)
    except PermissionError:
        logging.error("无法写入输出文件，请确认文件未在 Excel 中打开后重试。")
        return 1
    except Exception:
        logging.error("写入输出文件失败", exc_info=True)
        return 1

    return 0
