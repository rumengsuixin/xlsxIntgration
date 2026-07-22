"""代号5(代付订单对账)平台 handler 与输出 schema。

配合 platform_engine.enrich_admin_columnar 使用:
- GenericPayout5Handler:声明式平台(SUPERPAY)的通用取值,由 spec 字段驱动
  (fee_mode / arrive_mode / currency_col|default / org_source 等)。
- IbfypayHandler / EpinHandler:覆盖 match_values(平台流水号/状态特殊、EPIN 倒推汇率)。
- WangguypayHandler / PhonecardHandler:匹配取值走通用逻辑,仅读取(read/build_lookup)特殊
  (阶段3 平移;当前 enrich 由旧 main 建好的原生列查找表喂入)。

查找表为"平台原生列名"(与既有单测契约一致):match_values/extra_values 按 spec.columns
声明的原生列名从(带前缀的)合并行 / 查找表行取值。所有输出列由 SCHEMA_5.column_plan 声明。
"""

import re

from . import config5 as c5
from .platform_engine import format_date, normalize_currency, normalize_status, to_float
from .platform_spec import OutputColumn, OutputSchema, register_handler


# ═════════════════════════════════════════════════════════════════════════════
# 声明式基类:通用取值(SUPERPAY 直接用;WANGGUYPAY/PHONECARD 继承取值、仅换读取)
# ═════════════════════════════════════════════════════════════════════════════

class GenericPayout5Handler:
    """代号5 声明式平台默认 handler。取值全部由 spec 字段驱动。"""

    # ── 读取 / 查找表(阶段3 由 app5 对应函数平移进来)────────────────────────
    def read(self, spec, files):
        raise NotImplementedError("read 将于阶段3 迁入;当前由 app5 旧 read_*_5 负责")

    def build_lookup(self, spec, df):
        raise NotImplementedError("build_lookup 将于阶段3 迁入;当前由 app5 旧 build_*_lookup_5 负责")

    # ── 命中判定 ──────────────────────────────────────────────────────────────
    def is_hit(self, spec, row, prefix):
        """金额列非空即命中;有 admin_match_filter 时叠加 admin 侧门槛(EPIN 机构纯数字)。"""
        amt_col = spec.columns.get("amount")
        amt = str(row.get(f"{prefix}{amt_col}", "")).strip() if amt_col else ""
        if amt == "":
            return False
        flt = spec.admin_match_filter
        if flt:
            val = str(row.get(flt["col"], "")).strip()
            if not val or not re.match(flt["pattern"], val):
                return False
        return True

    # ── 取值助手 ──────────────────────────────────────────────────────────────
    @staticmethod
    def _rcol(row, prefix, col):
        return str(row.get(f"{prefix}{col}", "")).strip() if col else ""

    def _currency(self, spec, get):
        """get(col) 返回某原生列值;命中/多余复用。"""
        if spec.currency_col:
            return normalize_currency(get(spec.currency_col))
        return spec.currency_default

    # ── 命中行取值(fee 无 guard,交易日期 finish 回退 create)──────────────────
    def match_values(self, spec, row, prefix, admin_org):
        cols = spec.columns
        get = lambda col: self._rcol(row, prefix, col)
        amt = get(cols.get("amount"))
        amt_f = to_float(amt) or 0.0
        arrive = self._arrive(spec, get, amt, amt_f)
        fee = self._fee(spec, get, amt, amt_f, arrive, guard=False)
        date = format_date(get(cols.get("finish_time"))) or format_date(get(cols.get("create_time")))
        org = admin_org if spec.org_source == "admin" else (spec.org_name or spec.key)
        return {
            "match_status": "是",
            "platform_no": get(cols.get("platform_no")),
            "amount": amt,
            "currency": self._currency(spec, get),
            "status": normalize_status(spec, get(cols.get("status"))),
            "fee": fee,
            "arrive": arrive,
            "transaction_date": date,
            "implied_rate": "",
            "calc_amount": "",
            "org": org,
        }

    # ── 平台多余行取值(fee 带 guard,交易日期仅 finish)────────────────────────
    def extra_values(self, spec, lk_row, key):
        cols = spec.columns
        get = lambda col: str(lk_row.get(col, "")).strip() if col else ""
        amt = get(cols.get("amount"))
        amt_f = to_float(amt) or 0.0
        arrive = self._arrive(spec, get, amt, amt_f)
        fee = self._fee(spec, get, amt, amt_f, arrive, guard=True)
        return {
            "platform_no": get(cols.get("platform_no")),
            "amount": amt,
            "currency": self._currency(spec, get),
            "status": normalize_status(spec, get(cols.get("status"))),
            "fee": fee,
            "arrive": arrive,
            "transaction_date": format_date(get(cols.get("finish_time"))),
            "implied_rate": "",
            "calc_amount": "",
            "org": spec.org_name or spec.key,
        }

    # ── fee / arrive 模式解析 ─────────────────────────────────────────────────
    def _arrive(self, spec, get, amt, amt_f):
        m = spec.arrive_mode
        if m == "column":
            return get(spec.columns.get("arrive_amount"))
        if m == "equals_amount":
            return amt
        if m == "amount_minus_fee":
            fee_f = to_float(get(spec.columns.get("fee"))) or 0.0
            return str(round(amt_f - fee_f, 2))
        return ""

    def _fee(self, spec, get, amt, amt_f, arrive, guard):
        m = spec.fee_mode
        if m == "column":
            return get(spec.columns.get("fee"))
        if m == "amount_minus_arrive":
            if guard and not (amt or arrive):
                return ""
            arrive_f = to_float(arrive) or 0.0
            return str(round(abs(amt_f - arrive_f), 2))
        return ""


# ═════════════════════════════════════════════════════════════════════════════
# IBFYPAY:平台流水号=admin 第三方订单号,状态=驳回/成功(非状态映射)
# ═════════════════════════════════════════════════════════════════════════════

class IbfypayHandler(GenericPayout5Handler):

    def match_values(self, spec, row, prefix, admin_org):
        cols = spec.columns
        get = lambda col: self._rcol(row, prefix, col)
        amt = get(cols.get("amount"))
        fee = get(cols.get("fee"))
        amt_f = to_float(amt) or 0.0
        fee_f = to_float(fee) or 0.0
        rejected = str(row.get(f"{prefix}{c5.IBFYPAY_REJECTED_COL_5}", "")).strip().lower() == "true"
        return {
            "match_status": "是",
            "platform_no": str(row.get(spec.admin_join_col, "")).strip(),  # admin 第三方订单号
            "amount": amt,
            "currency": spec.currency_default,
            "status": "驳回" if rejected else "成功",
            "fee": fee,
            "arrive": str(round(amt_f - fee_f, 2)),
            "transaction_date": format_date(get(cols.get("finish_time"))),
            "implied_rate": "",
            "calc_amount": "",
            "org": admin_org,
        }

    def extra_values(self, spec, lk_row, key):
        cols = spec.columns
        amt = str(lk_row.get(cols.get("amount"), "")).strip()
        fee = str(lk_row.get(cols.get("fee"), "")).strip()
        amt_f = to_float(amt) or 0.0
        fee_f = to_float(fee) or 0.0
        rejected = bool(lk_row.get(c5.IBFYPAY_REJECTED_COL_5, False))
        ftime = str(lk_row.get(cols.get("finish_time"), "")).strip() if cols.get("finish_time") else ""
        return {
            "platform_no": key,   # 系统流水号
            "amount": amt,
            "currency": spec.currency_default,
            "status": "驳回" if rejected else "成功",
            "fee": fee,
            "arrive": str(round(amt_f - fee_f, 2)),
            "transaction_date": format_date(ftime),
            "implied_rate": "",
            "calc_amount": "",
            "org": "IBFYPAY",
        }


# ═════════════════════════════════════════════════════════════════════════════
# EPIN:通用取值 + 倒推汇率/计算金额(从产品串推导)
# ═════════════════════════════════════════════════════════════════════════════

class EpinHandler(GenericPayout5Handler):

    def match_values(self, spec, row, prefix, admin_org):
        vals = super().match_values(spec, row, prefix, admin_org)
        amt = vals["amount"]
        product = self._rcol(row, prefix, c5.EPIN_SIPARISLER_PRODUCT_COL_5)
        try:
            m = re.search(r'([\d,]+(?:\.\d+)?)\s*TL', product, re.IGNORECASE)
            if not m:
                m = re.search(r'([\d,]+(?:\.\d+)?)', product)
            product_amt = float(m.group(1).replace(",", "")) if m else None
            ep_price = float(amt.replace(",", ""))
            if product_amt is not None and ep_price != 0:
                rate = round(product_amt / ep_price, 4)
                vals["implied_rate"] = rate
                vals["calc_amount"] = int(round(rate * ep_price))
        except (ValueError, ZeroDivisionError):
            pass
        return vals


# ═════════════════════════════════════════════════════════════════════════════
# WANGGUYPAY / PHONECARD:匹配取值走通用逻辑,仅读取特殊(阶段3 迁入)
# ═════════════════════════════════════════════════════════════════════════════

class WangguypayHandler(GenericPayout5Handler):
    pass


class PhonecardHandler(GenericPayout5Handler):
    pass


# ═════════════════════════════════════════════════════════════════════════════
# 代号5 输出 schema（10 个追加列 + 机构覆盖）
# ═════════════════════════════════════════════════════════════════════════════

SCHEMA_5 = OutputSchema(
    match_status_col=c5.MATCH_STATUS_COL_5,
    platform_source_col=c5.ADMIN_ORG_COL_5,   # 列式 enrich 不使用(占位)
    platform_order_no_col=c5.PLATFORM_ORDER_NO_COL_5,
    platform_amount_col=c5.PLATFORM_AMOUNT_COL_5,
    platform_status_col=c5.PLATFORM_STATUS_COL_5,
    fee_col=c5.FEE_COL_5,
    transaction_date_col=c5.TRANSACTION_DATE_COL_5,
    admin_join_candidates=[c5.ADMIN_JOIN_COL_5, c5.ADMIN_TP_ORDER_COL_5],
    column_plan=[
        OutputColumn(c5.MATCH_STATUS_COL_5, "match_status"),
        OutputColumn(c5.PLATFORM_ORDER_NO_COL_5, "platform_no"),
        OutputColumn(c5.PLATFORM_AMOUNT_COL_5, "amount"),
        OutputColumn(c5.PLATFORM_CURRENCY_COL_5, "currency"),
        OutputColumn(c5.PLATFORM_STATUS_COL_5, "status"),
        OutputColumn(c5.FEE_COL_5, "fee"),
        OutputColumn(c5.ARRIVE_AMOUNT_COL_5, "arrive"),
        OutputColumn(c5.TRANSACTION_DATE_COL_5, "transaction_date"),
        OutputColumn(c5.IMPLIED_RATE_COL_5, "implied_rate", in_extra=False),
        OutputColumn(c5.CALC_AMOUNT_COL_5, "calc_amount", in_extra=False),
    ],
    org_col=c5.ADMIN_ORG_COL_5,
)


# ── 注册 handler ─────────────────────────────────────────────────────────────
register_handler("generic5", GenericPayout5Handler())
register_handler("ibfpay", IbfypayHandler())
register_handler("wangguypay", WangguypayHandler())
register_handler("phonecard", PhonecardHandler())
register_handler("epin", EpinHandler())
