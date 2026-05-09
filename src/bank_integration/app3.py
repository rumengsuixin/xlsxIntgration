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
    ADYEN_SETTLEMENT_CURRENCY_COL,
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
    GOOGLE_MERCHANT_AMOUNT_COL,
    GOOGLE_MERCHANT_CURRENCY_COL,
    GOOGLE_REFUND_TYPE,
    GOOGLE_TRANSACTION_TYPE_COL,
    HUAWEI_AMOUNT_COL,
    HUAWEI_CURRENCY_COL,
    HUAWEI_DATE_COL,
    HUAWEI_JOIN_COL,
    HUAWEI_SHEET,
    INPUT_DIR_3,
    OUTPUT_APPLE_SHEET_3,
    OUTPUT_FILE_TEMPLATE,
    OUTPUT_SHEET_3,
    OUTPUT_SUMMARY_SHEET_3,
    PLATFORM_AMOUNT_COL,
    PLATFORM_CURRENCY_COL,
    SETTLEMENT_CURRENCY_COL,
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
        苹果    → 苹果文件

    返回 {"admin": [Path, ...], ...}，同平台多文件时全部收录，由调用方合并。
    """
    result: dict = {"admin": [], "adyen": [], "huawei": [], "google": [], "apple": []}

    for f in sorted(input_dir.glob("*.xlsx")):
        if f.name.startswith("~$"):
            continue
        stem = f.stem.lower()
        if stem.startswith("admin"):
            result["admin"].append(f)
        elif stem.startswith("adyen-"):
            result["adyen"].append(f)
        elif stem.startswith("华为"):
            result["huawei"].append(f)
        elif stem.startswith("googol-") or stem.startswith("google-"):
            result["google"].append(f)
        elif stem.startswith("苹果"):
            result["apple"].append(f)

    for key, file_list in result.items():
        if len(file_list) > 1:
            logging.info(
                "发现 %d 个 %s 文件，将合并读取：%s",
                len(file_list), key, [f.name for f in file_list],
            )

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


def read_apple(filepath: Path) -> pd.DataFrame:
    """读取 App Store Connect 报表（header 在第 4 行，即 header=3），全列字符串。"""
    df = pd.read_excel(filepath, header=3, dtype=str)
    return df.dropna(how="all").fillna("")


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
    只要任一行是 Refused，则该 PSP 不计入成功匹配；其余 PSP 按
    ADYEN_RECORD_TYPE_PRIORITY 优先取最准确的成功行。
    """
    priority_map = {rt: i for i, rt in enumerate(ADYEN_RECORD_TYPE_PRIORITY)}

    df2 = df[df[ADYEN_JOIN_COL].str.strip() != ""].copy()
    record_types = df2[ADYEN_RECORD_TYPE_COL].str.strip()

    refused_keys = set(df2.loc[record_types == "Refused", ADYEN_JOIN_COL].str.strip())
    if refused_keys:
        logging.warning("【Adyen】%d 个 PSP Reference 存在 Refused，已按失败处理并排除成功匹配", len(refused_keys))
        df2 = df2[~df2[ADYEN_JOIN_COL].str.strip().isin(refused_keys)].copy()
        record_types = df2[ADYEN_RECORD_TYPE_COL].str.strip()

    success_mask = record_types.isin(ADYEN_RECORD_TYPE_PRIORITY)
    filtered_count = len(df2) - int(success_mask.sum())
    if filtered_count > 0:
        logging.info("【Adyen】过滤 %d 条非成功状态记录（Received/Cancelled 等），不计入匹配", filtered_count)
    df2 = df2[success_mask].copy()

    df2["_p"] = df2[ADYEN_RECORD_TYPE_COL].str.strip().map(priority_map).astype(int)
    df2 = df2.sort_values([ADYEN_JOIN_COL, "_p"])

    result = df2.drop_duplicates(subset=[ADYEN_JOIN_COL], keep="first")
    dupes = len(df2) - len(result)
    if dupes > 0:
        logging.warning("【Adyen】同一 PSP Reference 存在多行记录，共去重 %d 行（优先取 SentForSettle/Authorised）", dupes)

    keep_cols = [c for c in [
        ADYEN_JOIN_COL, ADYEN_AMOUNT_COL, ADYEN_CURRENCY_COL,
        ADYEN_SETTLEMENT_CURRENCY_COL,
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
    merchant_amt = GOOGLE_MERCHANT_AMOUNT_COL
    ccy = GOOGLE_BUYER_CURRENCY_COL
    merchant_ccy = GOOGLE_MERCHANT_CURRENCY_COL

    def _pick(type_val, out_amt_col, include_merchant_amt=False, merchant_amt_out_col="merchant_amt",
              include_ccy=False, ccy_out_col="ccy",
              include_merchant_ccy=False, merchant_ccy_out_col="merchant_ccy",
              include_date=False, date_out_col="transaction_date"):
        sub = df[df[type_col].str.strip() == type_val].copy()
        sub = sub[sub[join].str.strip() != ""]
        cols = (
            [join, amt]
            + ([merchant_amt] if include_merchant_amt and merchant_amt in sub.columns else [])
            + ([ccy] if include_ccy else [])
            + ([merchant_ccy] if include_merchant_ccy and merchant_ccy in sub.columns else [])
            + ([GOOGLE_DATE_COL] if include_date and GOOGLE_DATE_COL in sub.columns else [])
        )
        sub = sub[[c for c in cols if c in sub.columns]]
        before = len(sub)
        sub = sub.drop_duplicates(subset=[join], keep="first")
        if len(sub) < before:
            logging.warning("【Google-%s】发现 %d 条重复流水号，已保留首行", type_val, before - len(sub))
        rename = {amt: out_amt_col}
        if include_merchant_amt and merchant_amt in sub.columns:
            rename[merchant_amt] = merchant_amt_out_col
        if include_ccy:
            rename[ccy] = ccy_out_col
        if include_merchant_ccy and merchant_ccy in sub.columns:
            rename[merchant_ccy] = merchant_ccy_out_col
        if include_date and GOOGLE_DATE_COL in sub.columns:
            rename[GOOGLE_DATE_COL] = date_out_col
        return sub.rename(columns=rename)

    charge = _pick(
        GOOGLE_CHARGE_TYPE,
        "charge_amt",
        include_merchant_amt=True,
        merchant_amt_out_col="merchant_charge_amt",
        include_ccy=True,
        include_merchant_ccy=True,
        include_date=True,
    )
    fee = _pick(
        GOOGLE_FEE_TYPE,
        "fee_amt",
        include_merchant_amt=True,
        merchant_amt_out_col="merchant_fee_amt",
    )
    refund = _pick(GOOGLE_REFUND_TYPE, "refund_amt",
                   include_merchant_amt=True, merchant_amt_out_col="merchant_refund_amt",
                   include_ccy=True, ccy_out_col="refund_ccy",
                   include_merchant_ccy=True, merchant_ccy_out_col="refund_merchant_ccy",
                   include_date=True, date_out_col="refund_date")
    fee_refund = _pick(
        GOOGLE_FEE_REFUND_TYPE,
        "fee_refund_amt",
        include_merchant_amt=True,
        merchant_amt_out_col="merchant_fee_refund_amt",
    )

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

    if "refund_merchant_ccy" in result.columns:
        if "merchant_ccy" not in result.columns:
            result["merchant_ccy"] = None
        mask = result["merchant_ccy"].isna() | (result["merchant_ccy"] == "")
        result.loc[mask, "merchant_ccy"] = result.loc[mask, "refund_merchant_ccy"]
        result = result.drop(columns=["refund_merchant_ccy"])

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
            row[SETTLEMENT_CURRENCY_COL] = str(adyen_lk.at[key, ADYEN_SETTLEMENT_CURRENCY_COL]).strip()
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
            row[SETTLEMENT_CURRENCY_COL] = row[PLATFORM_CURRENCY_COL]
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
            mr  = _to_float(google_lk.at[key, "merchant_refund_amt"])
            mc  = _to_float(google_lk.at[key, "merchant_charge_amt"])
            mf  = _to_float(google_lk.at[key, "merchant_fee_amt"])
            mfr = _to_float(google_lk.at[key, "merchant_fee_refund_amt"])
            row[PLATFORM_CURRENCY_COL] = str(google_lk.at[key, "ccy"]).strip()
            row[SETTLEMENT_CURRENCY_COL] = str(google_lk.at[key, "merchant_ccy"]).strip()
            if r is not None:  # 有退款记录 → 退款
                row[PLATFORM_AMOUNT_COL]   = str(round(abs(r or 0.0) + abs(fr or 0.0), 2))
                if mr is not None:
                    row[SETTLEMENT_AMOUNT_COL] = str(round(abs(mr or 0.0) - abs(mfr or 0.0), 2))
                if mfr is not None:
                    row[FEE_COL] = str(round(abs(mfr), 2))
                row[STATUS_COL] = "退款"
            else:              # 无退款记录 → 成功
                if c_ is not None:
                    row[PLATFORM_AMOUNT_COL]   = str(round(abs(c_ or 0.0) + abs(f or 0.0), 2))
                if mc is not None:
                    row[SETTLEMENT_AMOUNT_COL] = str(round((mc or 0.0) - abs(mf or 0.0), 2))
                if mc is not None:
                    # 国家税费：按 Merchant Currency 的 Charge 金额倒推。
                    merchant_charge = abs(mc or 0.0)
                    row[COUNTRY_TAX_COL] = str(round((merchant_charge * 1.2) - merchant_charge, 2))
                if mf is not None:
                    row[FEE_COL] = str(round(abs(mf), 2))
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
    settlement_ccy_list   = []
    status_list           = []
    settlement_list       = []
    fee_list              = []
    country_tax_list      = []
    transaction_date_list = []

    for _, row in result.iterrows():
        src = str(row.get(ADMIN_PAYMENT_COL, "")).strip()
        refund_flag = str(row.get(ADMIN_REFUND_COL, "")).strip()

        settle_amt       = ""
        settle_ccy       = ""
        fee_amt          = ""
        country_tax      = ""
        transaction_date = ""

        if src == "Adyen" and adyen_avail:
            amt = str(row.get(f"_a_{ADYEN_AMOUNT_COL}", "")).strip()
            ccy = str(row.get(f"_a_{ADYEN_CURRENCY_COL}", "")).strip()
            settle_ccy = str(row.get(f"_a_{ADYEN_SETTLEMENT_CURRENCY_COL}", "")).strip()
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
            settle_ccy = ccy
            matched = amt != ""
            settle_amt = amt  # 暂无手续费数据，结算金额 = 平台订单金额
            transaction_date = _format_date(row.get(ADMIN_DATE_COL, ""))

        elif "Google" in src and google_avail:
            ccy = str(row.get("_g_ccy", "")).strip()
            settle_ccy = str(row.get("_g_merchant_ccy", "")).strip()
            if refund_flag == "已退款":
                # 平台订单金额：|Charge refund| + |fee refund|
                r  = _to_float(row.get("_g_refund_amt", ""))
                fr = _to_float(row.get("_g_fee_refund_amt", ""))
                mr  = _to_float(row.get("_g_merchant_refund_amt", ""))
                mfr = _to_float(row.get("_g_merchant_fee_refund_amt", ""))
                amt = str(round(abs(r or 0.0) + abs(fr or 0.0), 2)) if (r is not None or fr is not None) else ""
                # 结算金额：|merchant refund| - |merchant fee_refund|
                if mr is not None:
                    settle_amt = str(round(abs(mr or 0.0) - abs(mfr or 0.0), 2))
                # 手续费：|merchant fee_refund|
                if mfr is not None:
                    fee_amt = str(round(abs(mfr), 2))
            else:
                # 平台订单金额：|Charge| + |Google fee|
                c = _to_float(row.get("_g_charge_amt", ""))
                f = _to_float(row.get("_g_fee_amt", ""))
                mc = _to_float(row.get("_g_merchant_charge_amt", ""))
                mf = _to_float(row.get("_g_merchant_fee_amt", ""))
                amt = str(round(abs(c or 0.0) + abs(f or 0.0), 2)) if c is not None else ""
                # 结算金额：merchant charge - |merchant fee|
                if mc is not None:
                    settle_amt = str(round((mc or 0.0) - abs(mf or 0.0), 2))
                # 手续费：|merchant Google fee|
                if mf is not None:
                    fee_amt = str(round(abs(mf), 2))
                # 国家税费：按 Merchant Currency 的 Charge 金额倒推。
                if mc is not None:
                    merchant_charge = abs(mc or 0.0)
                    country_tax = str(round((merchant_charge * 1.2) - merchant_charge, 2))
            matched = amt != ""
            transaction_date = _format_date(row.get(ADMIN_DATE_COL, ""))

        elif "苹果支付" in src:
            amt        = str(row.get(ADMIN_AMOUNT_COL, "")).strip()
            ccy        = ""   # Admin 无币种列，留空
            settle_ccy = ""
            matched    = True  # Admin 中已确认付款，无需平台文件验证
            settle_amt = amt   # 暂无苹果手续费数据，结算金额取 Admin 金额
            transaction_date = _format_date(row.get(ADMIN_DATE_COL, ""))

        else:
            amt = ccy = ""
            settle_ccy = ""
            matched = False

        platform_amt_list.append(amt)
        platform_ccy_list.append(ccy)
        settlement_ccy_list.append(settle_ccy)
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
    result.insert(admin_col_count + 2, SETTLEMENT_CURRENCY_COL, settlement_ccy_list)
    result.insert(admin_col_count + 3, STATUS_COL,            status_list)
    result.insert(admin_col_count + 4, SETTLEMENT_AMOUNT_COL, settlement_list)
    result.insert(admin_col_count + 5, FEE_COL,               fee_list)
    result.insert(admin_col_count + 6, COUNTRY_TAX_COL,       country_tax_list)
    result.insert(admin_col_count + 7, TRANSACTION_DATE_COL,  transaction_date_list)

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

    platform_map = [("Adyen", "Adyen"), ("华为支付", "华为支付"), ("Google支付", "Google支付"), ("苹果支付Lua", "苹果支付Lua")]
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


def build_summary_sheet(result_df: pd.DataFrame) -> pd.DataFrame:
    """按交易日期、平台、结算币种汇总结算金额。"""
    summary_cols = [
        TRANSACTION_DATE_COL,
        ADMIN_PAYMENT_COL,
        SETTLEMENT_CURRENCY_COL,
        "成功笔数",
        "成功金额",
        "退款笔数",
        "退款金额",
        "净交易金额",
    ]

    if result_df.empty:
        return pd.DataFrame(columns=summary_cols)

    df = result_df.copy()
    df[STATUS_COL] = df[STATUS_COL].astype(str).str.strip()
    df = df[df[STATUS_COL].isin(["成功", "退款"])].copy()
    if df.empty:
        return pd.DataFrame(columns=summary_cols)

    group_cols = [TRANSACTION_DATE_COL, ADMIN_PAYMENT_COL, SETTLEMENT_CURRENCY_COL]
    for col in group_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    amount = df[SETTLEMENT_AMOUNT_COL].fillna("").astype(str).str.strip().str.replace(",", "", regex=False)
    df["_amount"] = pd.to_numeric(amount, errors="coerce").fillna(0.0)
    df["_success_count"] = (df[STATUS_COL] == "成功").astype(int)
    df["_refund_count"] = (df[STATUS_COL] == "退款").astype(int)
    df["_success_amount"] = df["_amount"].where(df[STATUS_COL] == "成功", 0.0)
    df["_refund_amount"] = df["_amount"].where(df[STATUS_COL] == "退款", 0.0)

    summary = (
        df.groupby(group_cols, as_index=False)
        .agg(
            成功笔数=("_success_count", "sum"),
            成功金额=("_success_amount", "sum"),
            退款笔数=("_refund_count", "sum"),
            退款金额=("_refund_amount", "sum"),
        )
        .sort_values(group_cols, kind="stable")
        .reset_index(drop=True)
    )
    summary["净交易金额"] = summary["成功金额"] - summary["退款金额"]
    return summary[summary_cols]


def build_apple_platform_summary(apple_raw_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """从苹果平台报表数据构建交易金额汇总行。

    以 Settlement Date × 结算货币 分组，按 Quantity 正负区分成功/退款。
    """
    summary_cols = [
        TRANSACTION_DATE_COL, ADMIN_PAYMENT_COL, SETTLEMENT_CURRENCY_COL,
        "成功笔数", "成功金额", "退款笔数", "退款金额", "净交易金额",
    ]
    if apple_raw_df is None or apple_raw_df.empty:
        return pd.DataFrame(columns=summary_cols)

    df = apple_raw_df.copy()

    date_col = next(
        (c for c in ["Settlement Date", "Transaction Date"] if c in df.columns), None
    )
    if date_col is None:
        logging.warning("苹果平台报表未找到日期列，跳过苹果汇总")
        return pd.DataFrame(columns=summary_cols)
    df["_date"] = (
        pd.to_datetime(df[date_col].astype(str).str.strip(), errors="coerce")
        .dt.strftime("%Y-%m-%d").fillna("")
    )
    df = df[df["_date"] != ""]

    currency_col = next(
        (c for c in ["Currency of Proceeds", "Partner Share Currency"] if c in df.columns), None
    )
    df["_currency"] = df[currency_col].astype(str).str.strip() if currency_col else ""

    if "Extended Partner Share" not in df.columns:
        logging.warning("苹果平台报表未找到 Extended Partner Share 列，跳过苹果汇总")
        return pd.DataFrame(columns=summary_cols)
    df["_eps"] = pd.to_numeric(
        df["Extended Partner Share"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0.0)

    if "Quantity" in df.columns:
        df["_qty"] = pd.to_numeric(
            df["Quantity"].astype(str).str.replace(",", "", regex=False), errors="coerce"
        ).fillna(0.0)
    else:
        df["_qty"] = df["_eps"].apply(lambda x: 1.0 if x >= 0 else -1.0)

    df["_is_success"] = df["_qty"] > 0
    df["_is_refund"]  = df["_qty"] < 0

    rows = []
    for (dt, cur), grp in df.groupby(["_date", "_currency"], sort=True):
        rows.append({
            TRANSACTION_DATE_COL:    dt,
            ADMIN_PAYMENT_COL:       "苹果支付Lua",
            SETTLEMENT_CURRENCY_COL: cur,
            "成功笔数": int(grp.loc[grp["_is_success"], "_qty"].sum()),
            "成功金额": round(grp.loc[grp["_is_success"], "_eps"].sum(), 2),
            "退款笔数": int(grp.loc[grp["_is_refund"], "_qty"].abs().sum()),
            "退款金额": round(grp.loc[grp["_is_refund"], "_eps"].abs().sum(), 2),
        })

    if not rows:
        return pd.DataFrame(columns=summary_cols)

    result = pd.DataFrame(rows)
    result["净交易金额"] = result["成功金额"] - result["退款金额"]
    return result[summary_cols]


def _write_apple_sheet(
    writer: pd.ExcelWriter,
    apple_admin_df: pd.DataFrame,
    apple_raw_df: Optional[pd.DataFrame],
) -> None:
    """在 ExcelWriter 中写入苹果支付 Sheet。

    布局（从上到下）：
      合并日期汇总：admin 与平台同结算日期数据并排，一行一个日期
      Admin 苹果订单详情 + 统计
      苹果平台报表详情 + 合计
    """
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    ws = writer.book.create_sheet(OUTPUT_APPLE_SHEET_3)
    writer.sheets[OUTPUT_APPLE_SHEET_3] = ws

    row = 1

    def _title(r, text, size=12):
        ws.cell(r, 1).value = text
        ws.cell(r, 1).font = Font(bold=True, size=size)

    def _header(r, headers):
        for ci, h in enumerate(headers, 1):
            c = ws.cell(r, ci)
            c.value = h
            c.font = Font(bold=True)

    def _coerce(val):
        """将字符串型数字转为 float，非数字保持字符串，供公式正常计算。"""
        s = str(val).strip()
        if s == "":
            return ""
        try:
            return float(s.replace(",", ""))
        except ValueError:
            return s

    def _sum_col(r, ci, r_start, r_end):
        if r_end >= r_start:
            cl = get_column_letter(ci)
            ws.cell(r, ci).value = f"=SUM({cl}{r_start}:{cl}{r_end})"
        else:
            ws.cell(r, ci).value = 0

    # ══════════════════════════════════════════════════════════════
    # 合并日期汇总（admin 与平台同行并排）
    # ══════════════════════════════════════════════════════════════
    _title(row, "苹果订单日期汇总")
    row += 1

    merged_headers = [
        "日期",
        "笔数", "正常笔数", "退款笔数", "正常金额", "退款金额", "合计金额",
        "Quantity合计", "Extended Partner Share合计", "客户支付合计", "手续费合计",
    ]
    _header(row, merged_headers)
    row += 1

    # -- Admin 按支付日期聚合 --
    admin_summary: dict = {}
    if not apple_admin_df.empty:
        adf = apple_admin_df.copy()
        adf["_date"] = (
            pd.to_datetime(adf[ADMIN_DATE_COL].astype(str).str.strip(), errors="coerce")
            .dt.strftime("%Y-%m-%d").fillna("")
            if ADMIN_DATE_COL in adf.columns else ""
        )
        adf["_amount"] = (
            pd.to_numeric(
                adf[ADMIN_AMOUNT_COL].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            ).fillna(0.0)
            if ADMIN_AMOUNT_COL in adf.columns else 0.0
        )
        flag = (
            adf[ADMIN_REFUND_COL].astype(str).str.strip()
            if ADMIN_REFUND_COL in adf.columns
            else pd.Series("正常", index=adf.index)
        )
        adf["_normal"] = flag == "正常"
        adf["_refund"] = flag == "已退款"
        for dt, grp in adf.groupby("_date", sort=True):
            n_amt = round(grp.loc[grp["_normal"], "_amount"].sum(), 2)
            r_amt = round(grp.loc[grp["_refund"], "_amount"].sum(), 2)
            admin_summary[dt] = [
                len(grp), int(grp["_normal"].sum()), int(grp["_refund"].sum()),
                n_amt, r_amt, round(n_amt - r_amt, 2),
            ]

    # -- 平台 按结算日期聚合 --
    platform_summary: dict = {}
    if apple_raw_df is not None and not apple_raw_df.empty:
        plat_date_col = next(
            (c for c in ["Settlement Date", "Transaction Date", "Begin Date", "End Date", "Report Date", "Date", "日期"]
             if c in apple_raw_df.columns),
            None,
        )
        if plat_date_col:
            pdf = apple_raw_df.copy()
            pdf["_date"] = (
                pd.to_datetime(pdf[plat_date_col].astype(str).str.strip(), errors="coerce")
                .dt.strftime("%Y-%m-%d").fillna("")
            )
            pdf = pdf[pdf["_date"] != ""]

            def _num(col):
                if col not in pdf.columns:
                    return pd.Series(0.0, index=pdf.index)
                return pd.to_numeric(
                    pdf[col].astype(str).str.replace(",", "", regex=False), errors="coerce"
                ).fillna(0.0)

            pdf["_qty"] = _num("Quantity")
            pdf["_eps"] = _num("Extended Partner Share")
            pdf["_tcp"] = pdf["_qty"] * _num("Customer Price")
            pdf["_fee"] = pdf["_tcp"] - pdf["_eps"]

            for dt, grp in pdf.groupby("_date", sort=True):
                platform_summary[dt] = [
                    int(grp["_qty"].sum()),
                    round(grp["_eps"].sum(), 2),
                    round(grp["_tcp"].sum(), 2),
                    round(grp["_fee"].sum(), 2),
                ]

    # -- 合并写出：admin 与平台 outer join 按日期排序 --
    all_dates = sorted(set(admin_summary) | set(platform_summary))
    summary_start = row
    for dt in all_dates:
        a = admin_summary.get(dt, ["", "", "", "", "", ""])
        p = platform_summary.get(dt, ["", "", "", ""])
        vals = [dt] + a + p
        for ci, v in enumerate(vals, 1):
            ws.cell(row, ci).value = v
        row += 1
    summary_end = row - 1

    ws.cell(row, 1).value = "合计"
    ws.cell(row, 1).font = Font(bold=True)
    for ci in range(2, len(merged_headers) + 1):
        _sum_col(row, ci, summary_start, summary_end)
    row += 2

    # ══════════════════════════════════════════════════════════════
    # Admin 苹果订单详情
    # ══════════════════════════════════════════════════════════════
    _title(row, "Admin 苹果订单详情")
    row += 1

    admin_cols = list(apple_admin_df.columns)
    _header(row, admin_cols)
    row += 1

    admin_data_start = row
    for _, dr in apple_admin_df.iterrows():
        for ci, val in enumerate(dr, 1):
            ws.cell(row, ci).value = _coerce(val)
        row += 1
    admin_data_end = row - 1
    row += 1

    ws.cell(row, 1).value = "统计"
    ws.cell(row, 1).font = Font(bold=True)
    row += 1
    _header(row, ["", "笔数", "合计金额"])
    row += 1

    ridx = (admin_cols.index(ADMIN_REFUND_COL) + 1) if ADMIN_REFUND_COL in admin_cols else None
    aidx = (admin_cols.index(ADMIN_AMOUNT_COL) + 1) if ADMIN_AMOUNT_COL in admin_cols else None

    if admin_data_end >= admin_data_start and ridx and aidx:
        rc = get_column_letter(ridx)
        ac = get_column_letter(aidx)
        rng_r = f"{rc}{admin_data_start}:{rc}{admin_data_end}"
        rng_a = f"{ac}{admin_data_start}:{ac}{admin_data_end}"

        normal_stat_row = row
        ws.cell(row, 1).value = "正常订单"
        ws.cell(row, 2).value = f'=COUNTIF({rng_r},"正常")'
        ws.cell(row, 3).value = f'=SUMIF({rng_r},"正常",{rng_a})'
        row += 1
        refund_stat_row = row
        ws.cell(row, 1).value = "退款订单"
        ws.cell(row, 2).value = f'=COUNTIF({rng_r},"已退款")'
        ws.cell(row, 3).value = f'=SUMIF({rng_r},"已退款",{rng_a})'
        row += 1
        ws.cell(row, 1).value = "合计"
        ws.cell(row, 1).font = Font(bold=True)
        ws.cell(row, 2).value = f"=SUM(B{normal_stat_row}:B{refund_stat_row})"
        ws.cell(row, 3).value = f"=SUM(C{normal_stat_row}:C{refund_stat_row})"
        row += 2
    else:
        row += 1

    # ══════════════════════════════════════════════════════════════
    # 苹果平台报表详情
    # ══════════════════════════════════════════════════════════════
    _title(row, "苹果平台报表详情")
    row += 1

    if apple_raw_df is None or apple_raw_df.empty:
        ws.cell(row, 1).value = "（未找到苹果平台文件）"
        return

    # 追加手续费计算列（= Customer Price × Quantity - Extended Partner Share）
    def _to_num_s(series):
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False), errors="coerce"
        ).fillna(0.0)

    plat_df = apple_raw_df.copy()
    if all(c in plat_df.columns for c in ["Quantity", "Customer Price", "Extended Partner Share"]):
        plat_df["手续费"] = (
            _to_num_s(plat_df["Quantity"]) * _to_num_s(plat_df["Customer Price"])
            - _to_num_s(plat_df["Extended Partner Share"])
        ).round(4)
    else:
        plat_df["手续费"] = ""

    pcols = list(plat_df.columns)
    _header(row, pcols)
    row += 1

    plat_data_start = row
    for _, dr in plat_df.iterrows():
        for ci, val in enumerate(dr, 1):
            ws.cell(row, ci).value = _coerce(val)
        row += 1
    plat_data_end = row - 1
    row += 1

    # 平台数据合计行
    ps_idx  = (pcols.index("Partner Share") + 1) if "Partner Share" in pcols else None
    ext_idx = (pcols.index("Extended Partner Share") + 1) if "Extended Partner Share" in pcols else None
    cp_idx  = (pcols.index("Customer Price") + 1) if "Customer Price" in pcols else None
    fee_idx = (pcols.index("手续费") + 1) if "手续费" in pcols else None

    ws.cell(row, 2).value = "Partner Share（结算单价）"
    ws.cell(row, 3).value = "Extended Partner Share（合计结算）"
    ws.cell(row, 4).value = "Customer Price（客户单价）"
    ws.cell(row, 5).value = "手续费合计（客户支付-结算）"
    for ci in (2, 3, 4, 5):
        ws.cell(row, ci).font = Font(bold=True)
    row += 1

    ws.cell(row, 1).value = "合计"
    ws.cell(row, 1).font = Font(bold=True)
    for out_ci, src_idx in ((2, ps_idx), (3, ext_idx), (4, cp_idx)):
        if src_idx and plat_data_end >= plat_data_start:
            data_col = get_column_letter(src_idx)
            ws.cell(row, out_ci).value = f"=SUM({data_col}{plat_data_start}:{data_col}{plat_data_end})"
    if fee_idx and plat_data_end >= plat_data_start:
        fee_col = get_column_letter(fee_idx)
        ws.cell(row, 5).value = f"=SUM({fee_col}{plat_data_start}:{fee_col}{plat_data_end})"


def write_output(
    result_df: pd.DataFrame,
    output_dir: Path,
    apple_raw_df: Optional[pd.DataFrame] = None,
) -> Path:
    """将结果写入 data/output/订单匹配结果_{YYYYMMDD}.xlsx。"""
    today = date.today().strftime("%Y%m%d")
    filename = OUTPUT_FILE_TEMPLATE.format(date=today)
    output_path = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)

    # 苹果行单独写入苹果支付 Sheet，从主表剔除
    apple_mask = result_df[ADMIN_PAYMENT_COL].str.strip().str.contains("苹果支付", na=False)
    main_df = result_df[~apple_mask].reset_index(drop=True)

    _extra_cols = [
        PLATFORM_AMOUNT_COL, PLATFORM_CURRENCY_COL, SETTLEMENT_CURRENCY_COL,
        STATUS_COL, SETTLEMENT_AMOUNT_COL, FEE_COL, COUNTRY_TAX_COL, TRANSACTION_DATE_COL,
    ]
    admin_orig_cols = [c for c in result_df.columns if c not in _extra_cols]
    apple_admin_df = result_df[apple_mask][admin_orig_cols].reset_index(drop=True)

    # 非苹果行用 result_df 汇总；苹果行用平台数据汇总（admin 无法匹配，结算金额为空）
    non_apple_summary = build_summary_sheet(result_df[~apple_mask].reset_index(drop=True))
    apple_summary     = build_apple_platform_summary(apple_raw_df)
    summary_df = (
        pd.concat([non_apple_summary, apple_summary], ignore_index=True)
        .sort_values(
            [TRANSACTION_DATE_COL, ADMIN_PAYMENT_COL, SETTLEMENT_CURRENCY_COL],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        main_df.to_excel(writer, sheet_name=OUTPUT_SHEET_3, index=False)
        summary_df.to_excel(writer, sheet_name=OUTPUT_SUMMARY_SHEET_3, index=False)
        _write_apple_sheet(writer, apple_admin_df, apple_raw_df)

    return output_path


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    files = scan_source_files_3(INPUT_DIR_3)
    if not files["admin"]:
        logging.error(
            "未找到 admin 订单文件。请将文件放入 %s，文件名须以 'admin'（不区分大小写）开头，"
            "例如：admin收入订单明细-xxx.xlsx",
            INPUT_DIR_3,
        )
        return 1

    admin_frames = []
    for fp in files["admin"]:
        logging.info("读取 admin 文件: %s", fp.name)
        try:
            admin_frames.append(read_admin(fp))
        except Exception:
            logging.error("读取 admin 文件失败: %s", fp.name, exc_info=True)
            return 1
    admin_df = pd.concat(admin_frames, ignore_index=True) if len(admin_frames) > 1 else admin_frames[0]
    logging.info("  admin 共 %d 条记录", len(admin_df))

    adyen_lk = huawei_lk = google_lk = None

    if files["adyen"]:
        adyen_frames = []
        for fp in files["adyen"]:
            logging.info("读取 Adyen 文件: %s", fp.name)
            try:
                adyen_frames.append(read_adyen(fp))
            except Exception:
                logging.error("读取 Adyen 文件失败: %s", fp.name, exc_info=True)
        if adyen_frames:
            raw = pd.concat(adyen_frames, ignore_index=True) if len(adyen_frames) > 1 else adyen_frames[0]
            adyen_lk = build_adyen_lookup(raw)
            logging.info("  Adyen 去重后共 %d 个唯一 PSP Reference", len(adyen_lk))
    else:
        logging.warning("未找到 Adyen 文件，跳过 Adyen 匹配")

    if files["huawei"]:
        huawei_frames = []
        for fp in files["huawei"]:
            logging.info("读取华为文件: %s", fp.name)
            try:
                huawei_frames.append(read_huawei(fp))
            except Exception:
                logging.error("读取华为文件失败: %s", fp.name, exc_info=True)
        if huawei_frames:
            raw = pd.concat(huawei_frames, ignore_index=True) if len(huawei_frames) > 1 else huawei_frames[0]
            huawei_lk = build_huawei_lookup(raw)
            logging.info("  华为共 %d 条记录", len(huawei_lk))
    else:
        logging.warning("未找到华为文件，跳过华为匹配")

    if files["google"]:
        google_frames = []
        for fp in files["google"]:
            logging.info("读取 Google Play 文件: %s", fp.name)
            try:
                google_frames.append(read_google(fp))
            except Exception:
                logging.error("读取 Google Play 文件失败: %s", fp.name, exc_info=True)
        if google_frames:
            raw = pd.concat(google_frames, ignore_index=True) if len(google_frames) > 1 else google_frames[0]
            google_lk = build_google_lookup(raw)
            logging.info("  Google Play 共 %d 个唯一订单", len(google_lk))
    else:
        logging.warning("未找到 Google Play 文件，跳过 Google 匹配")

    apple_raw_df = None
    if files["apple"]:
        apple_frames = []
        for fp in files["apple"]:
            logging.info("读取苹果文件: %s", fp.name)
            try:
                apple_frames.append(read_apple(fp))
            except Exception:
                logging.error("读取苹果文件失败: %s", fp.name, exc_info=True)
        if apple_frames:
            apple_raw_df = pd.concat(apple_frames, ignore_index=True) if len(apple_frames) > 1 else apple_frames[0]
            logging.info("  苹果平台报表共 %d 行", len(apple_raw_df))
    else:
        logging.info("未找到苹果文件（苹果 Sheet 的平台数据部分将为空）")

    result_df = enrich_admin(admin_df, adyen_lk, huawei_lk, google_lk)
    log_match_stats(result_df)

    try:
        output_path = write_output(result_df, OUTPUT_DIR, apple_raw_df)
        logging.info("结果文件已写入: %s", output_path)
    except PermissionError:
        logging.error("无法写入输出文件，请确认文件未在 Excel 中打开后重试。")
        return 1
    except Exception:
        logging.error("写入输出文件失败", exc_info=True)
        return 1

    return 0
