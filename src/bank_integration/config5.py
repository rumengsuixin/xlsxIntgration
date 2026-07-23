"""代号5（代付订单对账）路径常量与列名配置。"""

import os
from pathlib import Path

from .config import DATA_DIR, OUTPUT_DIR  # noqa: F401

# ── 路径常量 ──────────────────────────────────────────────────────────────────
# 输入目录支持 BANK_INPUT_DIR 环境变量覆盖（供 macro 桥接控制运行时目录）；
# 未设置时回退仓库默认 data/input/5，现有启动器/命令行行为完全不变。
INPUT_DIR_5 = Path(os.environ.get("BANK_INPUT_DIR", DATA_DIR / "input" / "5"))
OUTPUT_FILE_TEMPLATE_5 = "代付对账结果_{date}.xlsx"

# ── 输出工作表名称 ────────────────────────────────────────────────────────────
OUTPUT_SHEET_5         = "代付对账结果"
OUTPUT_FAILED_SHEET_5  = "匹配失败订单"
OUTPUT_SUMMARY_SHEET_5 = "平台汇总"

# ── Admin 主表（XLS，engine=xlrd，sheet="Simple"）────────────────────────────
ADMIN_SHEET_5          = "Simple"
ADMIN_JOIN_COL_5       = "订单号"          # 与各平台「商户订单号」对应
ADMIN_DATE_COL_5       = "日期"
ADMIN_AMOUNT_COL_5     = "金额"
ADMIN_STATUS_COL_5     = "状态"
ADMIN_TP_ORDER_COL_5   = "第三方订单号"    # 部分平台回写，可用于二次核对
ADMIN_ORDER_TYPE_COL_5 = "订单类型"
ADMIN_ORG_COL_5        = "机构"
ADMIN_PRIZE_COL_5      = "奖品名称"

# ── IBFYPAY 平台（资金流水账格式；xlsx，sheet="Sheet"，header=0）──────────────
# 文件命名前缀：ibf平台（如 IBF平台兑换资金流水明细202604.xlsx）
# 每笔代付对应两行，通过 系统流水号 匹配：
#   类型="代付扣款"        → 代付金额（变动金额 取绝对值）
#   类型="代付扣除手续费"   → 手续费（变动金额 取绝对值）
# admin 侧关联键：ADMIN_TP_ORDER_COL_5（第三方订单号），非通用 ADMIN_JOIN_COL_5（订单号）
IBFYPAY_SHEET_5           = "Sheet"
IBFYPAY_HEADER_5          = 0
IBFYPAY_JOIN_COL_5        = "系统流水号"        # 平台侧关联键
IBFYPAY_ADMIN_JOIN_COL_5  = "第三方订单号"      # admin 侧关联键（不同于其他平台用 订单号）
IBFYPAY_TYPE_COL_5        = "类型"
IBFYPAY_TYPE_SYSTEM_5     = "系统操作"
IBFYPAY_TYPE_PAYOUT_5     = "代付扣款"          # 过滤代付扣款行
IBFYPAY_TYPE_FEE_5        = "代付扣除手续费"     # 过滤手续费行
IBFYPAY_TYPE_REJECT_5     = "代付驳回"            # 驳回行（代付被拒后原路退款）
IBFYPAY_AMOUNT_COL_5      = "变动金额"          # 金额列（负数，取绝对值）
IBFYPAY_BEGIN_AMOUNT_COL_5 = "原金额"
IBFYPAY_END_AMOUNT_COL_5   = "变动后金额"
IBFYPAY_TIME_COL_5        = "变动时间"          # 交易时间
IBFYPAY_ACCOUNT_COL_5     = "账户"
IBFYPAY_REMARK_COL_5      = "备注"

# ── SUPERPAY 平台（XLSX，sheet="sheet1"，header=0）───────────────────────────
SUPERPAY_SHEET_5             = "sheet1"
SUPERPAY_HEADER_5            = 0
SUPERPAY_JOIN_COL_5          = "商户订单号"    # 对应 admin.订单号
SUPERPAY_PLATFORM_NO_COL_5   = "代付订单Id"
SUPERPAY_AMOUNT_COL_5        = "代付金额"
SUPERPAY_FEE_RATE_COL_5      = "费率"
SUPERPAY_FEE_SINGLE_COL_5    = "单笔手续费"
SUPERPAY_FEE_TOTAL_COL_5     = "手续费"
SUPERPAY_ACTUAL_COL_5        = "实收"          # 到账金额（代付金额 - 手续费）
SUPERPAY_CURRENCY_COL_5      = "币种"
SUPERPAY_CHANNEL_NO_COL_5    = "渠道订单号"
SUPERPAY_ACCOUNT_COL_5       = "收款账号"
SUPERPAY_PAYEE_COL_5         = "收款人"
SUPERPAY_DESC_COL_5          = "转账描述"
SUPERPAY_STATUS_COL_5        = "支付状态"
SUPERPAY_CREATE_TIME_COL_5   = "创建时间"
SUPERPAY_FINISH_TIME_COL_5   = "订单支付成功时间"

# ── WANGGUYPAY 平台（旧付款订单 + 新资金记录，表头均在第2行）────────────────
WANGGUYPAY_SHEET_5             = "付款订单"
WANGGUYPAY_HEADER_5            = 1              # 关键：实际文件第1行是无用大标题，列头在第2行
WANGGUYPAY_JOIN_COL_5          = "商户订单号"   # 旧付款订单列；新资金记录改用 平台订单号 ↔ admin.第三方订单号
WANGGUYPAY_PLATFORM_NO_COL_5   = "平台订单号"
WANGGUYPAY_AMOUNT_COL_5        = "交易金额(try)"
WANGGUYPAY_FEE_RATE_COL_5      = "费率"
WANGGUYPAY_FEE_COL_5           = "手续费(try)"
WANGGUYPAY_ARRIVE_COL_5        = "到账金额(try)"
WANGGUYPAY_PAYEE_NAME_COL_5    = "收款人名称"
WANGGUYPAY_ACCOUNT_COL_5       = "收款人银行账号"
WANGGUYPAY_ACCOUNT_TYPE_COL_5  = "收款人账号类型"
WANGGUYPAY_IFSC_COL_5          = "收款人IFSC/UPI"
WANGGUYPAY_CREATE_TIME_COL_5   = "创建时间"
WANGGUYPAY_FINISH_TIME_COL_5   = "完成时间"
WANGGUYPAY_STATUS_COL_5        = "交易状态"
WANGGUYPAY_CALLBACK_COL_5      = "回调状态"
WANGGUYPAY_FAIL_INFO_COL_5     = "失败信息"
WANGGUYPAY_FUND_TYPE_COL_5     = "交易类型"
WANGGUYPAY_BEGIN_AMOUNT_COL_5   = "期初金额(try)"
WANGGUYPAY_FUND_AMOUNT_COL_5   = "变动金额(try)"
WANGGUYPAY_END_AMOUNT_COL_5     = "期末金额(try)"
WANGGUYPAY_FUND_TYPE_PAYOUT_5  = "付款结算"
WANGGUYPAY_FUND_TYPE_FEE_5     = "扣除代付结算手续费"
WANGGUYPAY_FUND_STATUS_5       = "成功"
WANGGUYPAY_FUND_FILE_PREFIXES_5 = ["wangupay资金记录", "wangguypay资金记录"]

# ── 话费卡结算（XLSX，订单明细 sheet 自动识别）──────────────────────────────
PHONECARD_PLATFORM_NAME_5     = "话费卡"
PHONECARD_JOIN_COL_5          = "订单号"
PHONECARD_AMOUNT_COL_5        = "金额"
PHONECARD_STATUS_COL_5        = "状态"
PHONECARD_DATE_COL_5          = "日期"
PHONECARD_PLATFORM_NO_COL_5   = "第三方订单号"
PHONECARD_ORDER_TYPE_COL_5    = "订单类型"
PHONECARD_PRIZE_COL_5         = "奖品名称"
PHONECARD_PREFERRED_SHEET_KEY_5 = "汇总"

# ── Binance USDT 批量代付（聚合型:平台无订单号,按 收款ID+日期 汇总对账）──────────
# 平台文件为原始 Binance 模板 USDT奖品发放信息YYYY-MM-DD.xls:
#   sheet=Binance Pay Payout Template,表头在第2行(header=1),日期取自文件名;
#   收款ID 在 `Recipient's Account information (Required)`,USDT 面额在 `Amount (Required)`。
# admin 侧新格式多出 `其他` 列(BIN-<收款ID>)标识 USDT 兑换单,USDT 面额在 `奖品名称`
# (如 "USDT 0.5",`金额`列是 TRY)。二者按 收款ID 去前缀关联,按 T+1 窗口对齐日期。
BINANCE_PLATFORM_NAME_5    = "Binance"
BINANCE_SHEET_5            = "Binance Pay Payout Template"
BINANCE_HEADER_5           = 1                 # 第1行是大标题,列头在第2行
BINANCE_ID_COL_5           = "Recipient's Account information (Required)"
BINANCE_AMOUNT_COL_5       = "Amount (Required)"
BINANCE_DATE_FROM_NAME_5   = r"(\d{4}-\d{2}-\d{2})"   # 从文件名提取打款日
BINANCE_FILE_PREFIXES_5    = ["usdt奖品发放信息", "binance-", "merged-"]
BINANCE_OUTPUT_SHEET_5     = "Binance-USDT对账"

ADMIN_OTHER_COL_5          = "其他"           # 新格式 admin 多出的列(BIN-<收款ID>)
BINANCE_ADMIN_PREFIX_5     = "BIN-"
BINANCE_ADMIN_USDT_REGEX_5 = r"USDT\s*([\d.]+)"      # 从 奖品名称 提取 USDT 面额
ADMIN_STATUS_DONE_5        = "已完成"

BINANCE_RECON_LABELS_5 = {
    "consistent":       "一致",
    "amount_diff":      "金额不符",
    "platform_missing": "平台缺失",
    "platform_extra":   "平台多余",
}
BINANCE_RECON_COLUMNS_5 = [
    "日期", "收款ID", "admin应付USDT", "admin笔数",
    "平台实付USDT", "平台笔数", "差额", "对账状态",
]

# ── 文件识别前缀映射（stem.lower() startswith 任意一个前缀即命中）─────────────
# wangupay-（实际文件名少一个"g"）和 wangguypay- 均支持
PLATFORM_PREFIXES_5: dict = {
    "admin":           ["admin-"],
    "ibfpay":          ["ibfpay-", "ibf平台"],    # ibf平台 识别资金流水账格式文件
    "superpay":        ["superpay-"],
    "wangguypay":      ["wangupay-", "wangguypay-", "wangupay资金记录", "wangguypay资金记录"],
    "phonecard":       ["okey话费卡结算"],
    "epin_siparisler": ["epin_siparisler_"],
    "epin_pinler":     ["epin_pinler_"],
    "epin_odemeler":   ["epin_odemeler_"],
}

# ── 输出新增列（追加在 admin 原始列末尾）────────────────────────────────────
MATCH_STATUS_COL_5       = "是否匹配"       # 是 / 否 / 平台多余
PLATFORM_ORDER_NO_COL_5  = "平台流水号"     # 系统流水号 / 代付订单Id / 平台订单号
PLATFORM_AMOUNT_COL_5    = "平台代付金额"   # 平台记录的代付金额
PLATFORM_CURRENCY_COL_5  = "币种"           # 平台金额币种
PLATFORM_STATUS_COL_5    = "平台状态"       # 统一为：成功 / 失败 / 处理中 / 关闭
FEE_COL_5                = "手续费"         # 平台收取的手续费（IBFYPAY 无此字段，留空串）
ARRIVE_AMOUNT_COL_5      = "到账金额"       # 扣除手续费后实际到账（IBFYPAY 留空串）
TRANSACTION_DATE_COL_5   = "交易日期"       # 格式化为 YYYY-MM-DD 的交易时间
IMPLIED_RATE_COL_5       = "倒推汇率"       # 产品TL面值 / 单价(USD)，仅 EPIN 行有值
CALC_AMOUNT_COL_5        = "计算金额"       # 倒推汇率 * 平台代付金额（取整），仅 EPIN 行
OUTPUT_AMOUNT_DIFF_SHEET_5 = "匹配金额差异"  # 计算金额与 admin.金额 不符的 EPIN 行

IBFYPAY_DEFAULT_CURRENCY_5 = "TRY"
WANGGUYPAY_DEFAULT_CURRENCY_5 = "TRY"
EPIN_DEFAULT_CURRENCY_5 = "USD"

OUTPUT_NEW_COLS_5 = [
    MATCH_STATUS_COL_5,
    PLATFORM_ORDER_NO_COL_5,
    PLATFORM_AMOUNT_COL_5,
    PLATFORM_CURRENCY_COL_5,
    PLATFORM_STATUS_COL_5,
    FEE_COL_5,
    ARRIVE_AMOUNT_COL_5,
    TRANSACTION_DATE_COL_5,
    IMPLIED_RATE_COL_5,
    CALC_AMOUNT_COL_5,
]

# ── 平台汇总余额列 ──────────────────────────────────────────────────────────
SUMMARY_BEGIN_BALANCE_COL_5        = "期初余额"
SUMMARY_RECHARGE_COL_5             = "充值"
SUMMARY_WITHDRAWAL_COL_5           = "提现"
SUMMARY_CALC_END_BALANCE_COL_5     = "期末余额（计算）"
SUMMARY_PLATFORM_END_BALANCE_COL_5 = "期末余额（平台余额）"
SUMMARY_BALANCE_COLS_5 = [
    SUMMARY_BEGIN_BALANCE_COL_5,
    SUMMARY_RECHARGE_COL_5,
    SUMMARY_WITHDRAWAL_COL_5,
    SUMMARY_CALC_END_BALANCE_COL_5,
    SUMMARY_PLATFORM_END_BALANCE_COL_5,
]

BALANCE_RECHARGE_KEYWORDS_5 = ("充值", "入金", "加款", "手动增加")
BALANCE_RECHARGE_EXCLUDE_KEYWORDS_5 = ("退", "驳回", "退款", "退还", "返还", "冲正")
BALANCE_WITHDRAWAL_KEYWORDS_5 = ("提现", "出金", "提款")

# ── epin 平台（订单列表 epin_siparisler + pin码列表 epin_pinler）──────────────
EPIN_PLATFORM_NAME_5 = "EPIN"

# epin 订单列表（siparisler）列名
EPIN_SIPARISLER_ORDER_ID_COL_5     = "订单ID"
EPIN_SIPARISLER_STATUS_COL_5       = "订单状态"
EPIN_SIPARISLER_ORDER_NO_COL_5     = "订单号"
EPIN_SIPARISLER_PRODUCT_COL_5      = "产品"
EPIN_SIPARISLER_UNIT_PRICE_COL_5   = "单价(USD)"
EPIN_SIPARISLER_AMOUNT_COL_5       = "金额(USD)"
EPIN_SIPARISLER_QTY_COL_5          = "数量"
EPIN_SIPARISLER_CONFIRM_TIME_COL_5 = "确认时间"

# epin pin码列表（pinler）列名
EPIN_PINLER_ORDER_ID_COL_5  = "订单ID"
EPIN_PINLER_ORDER_NO_COL_5  = "订单号"
EPIN_PINLER_PIN_ID_COL_5    = "Pin ID"
EPIN_PINLER_PIN_CODE_COL_5  = "Pin码"

# admin 中 epin 订单识别规则：机构字段为纯数字
EPIN_ORG_PATTERN_5    = r"^\d+$"
# admin 侧关联键（第三方订单号存 pin 码值）
EPIN_ADMIN_JOIN_COL_5 = "第三方订单号"

# epin 付款列表（odemeler）列名
EPIN_ODEMELER_PAYMENT_ID_COL_5    = "付款ID"
EPIN_ODEMELER_STATUS_COL_5        = "付款状态"
EPIN_ODEMELER_CREATE_TIME_COL_5   = "创建时间"
EPIN_ODEMELER_CONFIRM_TIME_COL_5  = "确认时间"
EPIN_ODEMELER_TYPE_COL_5          = "付款类型"
EPIN_ODEMELER_AMOUNT_COL_5        = "付款金额(USD)"
EPIN_ODEMELER_USER_COL_5          = "用户"
EPIN_ODEMELER_BEGIN_BALANCE_COL_5 = "付款前余额(USD)"
EPIN_ODEMELER_END_BALANCE_COL_5   = "付款后余额(USD)"

# ── 平台状态映射（原始→标准，IBFYPAY 状态由 handler 特判不入表）──────────────
# 由 app5._normalize_platform_status_5 与外置化引擎共用（单一事实来源）。
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

# ── 内置平台声明（外置化真值来源）──────────────────────────────────────────────
# 代号5 用列式 enrich（platform_engine.enrich_admin_columnar），按平台原生列名取值。
# 优先级 IBFYPAY(10) > SUPERPAY(20) > WANGGUYPAY(30) > PHONECARD(40) > EPIN(50)，
# 与旧硬编码链一致。SUPERPAY 纯声明(handler=generic5)；其余各挂自定义 handler。
IBFYPAY_REJECTED_COL_5 = "_ibfpay_rejected"   # build_ibfpay_lookup 产出的驳回标记内部列
IBFYPAY_AMOUNT_LOOKUP_COL_5 = "代付金额"       # build_ibfpay_lookup 归一后的代付金额列(非源"变动金额")
IBFYPAY_FEE_LOOKUP_COL_5    = "手续费"          # build_ibfpay_lookup 归一后的手续费列

# 内置(疑难)平台 key:其 read/build 保留在 app5 由 main 分派;其余(外部 JSON/插件)走通用 handler
BUILTIN_PLATFORM_KEYS_5 = {"IBFYPAY", "SUPERPAY", "WANGGUYPAY", "PHONECARD", "EPIN"}

BUILTIN_SPECS_5 = [
    {
        "key": "IBFYPAY",
        "priority": 10,
        "handler": "ibfpay",
        "join_col": IBFYPAY_JOIN_COL_5,
        "admin_join_col": IBFYPAY_ADMIN_JOIN_COL_5,
        "sheet": IBFYPAY_SHEET_5,
        "currency_default": IBFYPAY_DEFAULT_CURRENCY_5,
        "fee_mode": "column",
        "arrive_mode": "amount_minus_fee",
        "org_source": "admin",
        "org_name": "IBFYPAY",
        "extra_backfill_admin_col": None,      # IBFYPAY 多余行不回填 admin 列
        "balance_handler": True,
        "columns": {
            "amount":      IBFYPAY_AMOUNT_LOOKUP_COL_5,   # 代付金额(build 归一)
            "fee":         IBFYPAY_FEE_LOOKUP_COL_5,       # 手续费(build 归一)
            "finish_time": IBFYPAY_TIME_COL_5,
        },
        "directions": {"payout": {"prefixes": PLATFORM_PREFIXES_5["ibfpay"]}},
    },
    {
        "key": "SUPERPAY",
        "priority": 20,
        "handler": "generic5",
        "join_col": SUPERPAY_JOIN_COL_5,
        "admin_join_col": ADMIN_JOIN_COL_5,
        "sheet": SUPERPAY_SHEET_5,
        "required_columns": [SUPERPAY_JOIN_COL_5, SUPERPAY_AMOUNT_COL_5, SUPERPAY_STATUS_COL_5],
        "currency_col": SUPERPAY_CURRENCY_COL_5,
        "fee_mode": "amount_minus_arrive",
        "arrive_mode": "column",
        "org_source": "admin",
        "status_map": PLATFORM_STATUS_MAP_5["SUPERPAY"],
        "columns": {
            "platform_no": SUPERPAY_PLATFORM_NO_COL_5,
            "amount":      SUPERPAY_AMOUNT_COL_5,
            "arrive_amount": SUPERPAY_ACTUAL_COL_5,
            "fee":         SUPERPAY_FEE_TOTAL_COL_5,
            "status":      SUPERPAY_STATUS_COL_5,
            "finish_time": SUPERPAY_FINISH_TIME_COL_5,
            "create_time": SUPERPAY_CREATE_TIME_COL_5,
        },
        "directions": {"payout": {"prefixes": PLATFORM_PREFIXES_5["superpay"]}},
    },
    {
        "key": "WANGGUYPAY",
        "priority": 30,
        "handler": "wangguypay",
        "join_col": WANGGUYPAY_PLATFORM_NO_COL_5,
        "admin_join_col": ADMIN_TP_ORDER_COL_5,
        "currency_default": WANGGUYPAY_DEFAULT_CURRENCY_5,
        "fee_mode": "column",
        "arrive_mode": "column",
        "org_source": "admin",
        "org_name": "WANGGUYPAY",
        "balance_handler": True,
        "status_map": PLATFORM_STATUS_MAP_5["WANGGUYPAY"],
        "columns": {
            "platform_no": WANGGUYPAY_PLATFORM_NO_COL_5,
            "amount":      WANGGUYPAY_AMOUNT_COL_5,
            "arrive_amount": WANGGUYPAY_ARRIVE_COL_5,
            "fee":         WANGGUYPAY_FEE_COL_5,
            "status":      WANGGUYPAY_STATUS_COL_5,
            "finish_time": WANGGUYPAY_FINISH_TIME_COL_5,
            "create_time": WANGGUYPAY_CREATE_TIME_COL_5,
        },
        "directions": {"payout": {"prefixes": PLATFORM_PREFIXES_5["wangguypay"]}},
    },
    {
        "key": "PHONECARD",
        "priority": 40,
        "handler": "phonecard",
        "join_col": PHONECARD_JOIN_COL_5,
        "admin_join_col": ADMIN_JOIN_COL_5,
        "currency_default": "",
        "fee_mode": "none",
        "arrive_mode": "equals_amount",
        "org_source": "platform",
        "org_name": PHONECARD_PLATFORM_NAME_5,
        "status_map": PLATFORM_STATUS_MAP_5["PHONECARD"],
        "columns": {
            "platform_no": PHONECARD_PLATFORM_NO_COL_5,
            "amount":      PHONECARD_AMOUNT_COL_5,
            "status":      PHONECARD_STATUS_COL_5,
            "finish_time": PHONECARD_DATE_COL_5,
        },
        "directions": {"payout": {"prefixes": PLATFORM_PREFIXES_5["phonecard"]}},
    },
    {
        "key": "EPIN",
        "priority": 50,
        "handler": "epin",
        "join_col": EPIN_PINLER_PIN_CODE_COL_5,
        "admin_join_col": EPIN_ADMIN_JOIN_COL_5,
        "currency_default": EPIN_DEFAULT_CURRENCY_5,
        "fee_mode": "none",
        "arrive_mode": "equals_amount",
        "org_source": "platform",
        "org_name": EPIN_PLATFORM_NAME_5,
        "admin_match_filter": {"col": ADMIN_ORG_COL_5, "pattern": EPIN_ORG_PATTERN_5},
        "emits_amount_diff": True,
        "balance_handler": True,
        "status_map": PLATFORM_STATUS_MAP_5["EPIN"],
        "columns": {
            "platform_no": EPIN_SIPARISLER_ORDER_ID_COL_5,
            "amount":      EPIN_SIPARISLER_UNIT_PRICE_COL_5,
            "status":      EPIN_SIPARISLER_STATUS_COL_5,
            "finish_time": EPIN_SIPARISLER_CONFIRM_TIME_COL_5,
        },
        "handler_params": {
            "pinler_prefixes": PLATFORM_PREFIXES_5["epin_pinler"],
            "odemeler_prefixes": PLATFORM_PREFIXES_5["epin_odemeler"],
        },
        "directions": {"payout": {"prefixes": PLATFORM_PREFIXES_5["epin_siparisler"]}},
    },
    {
        # 聚合型平台:handler=aggregate_recon,不走列式 enrich/查找表,产出独立 sheet。
        # join_col 仅为占位以满足 from_dict 必填(聚合引擎不建查找表、不使用它)。
        "key": "BINANCE",
        "priority": 60,
        "handler": "aggregate_recon",
        "join_col": BINANCE_ID_COL_5,
        "recon_mode": "aggregate",
        "recon": {
            "date_match_mode": "t1_window",     # 平台常在 admin 次日打款,按 T+1 窗口对齐
            "amount_tolerance": 0,
            "output_sheet": BINANCE_OUTPUT_SHEET_5,
            "platform": {
                "sheet": BINANCE_SHEET_5,
                "header_row": BINANCE_HEADER_5,
                "id_col": BINANCE_ID_COL_5,
                "amount_col": BINANCE_AMOUNT_COL_5,
                "date_from_filename": BINANCE_DATE_FROM_NAME_5,
            },
            "admin": {
                "filter_col": ADMIN_OTHER_COL_5,
                "filter_prefix": BINANCE_ADMIN_PREFIX_5,
                "status_col": ADMIN_STATUS_COL_5,
                "status_include": [ADMIN_STATUS_DONE_5],
                "id_col": ADMIN_OTHER_COL_5,
                "id_strip_prefix": BINANCE_ADMIN_PREFIX_5,
                "amount_col": ADMIN_PRIZE_COL_5,
                "amount_regex": BINANCE_ADMIN_USDT_REGEX_5,
                "date_col": ADMIN_DATE_COL_5,
            },
            "labels": BINANCE_RECON_LABELS_5,
            "output_columns": BINANCE_RECON_COLUMNS_5,
        },
        "directions": {"payout": {"prefixes": BINANCE_FILE_PREFIXES_5}},
    },
]
