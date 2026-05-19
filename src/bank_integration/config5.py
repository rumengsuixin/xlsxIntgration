"""代号5（代付订单对账）路径常量与列名配置。"""

from .config import DATA_DIR, OUTPUT_DIR  # noqa: F401

# ── 路径常量 ──────────────────────────────────────────────────────────────────
INPUT_DIR_5 = DATA_DIR / "input" / "raw" / "5"
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

# ── 文件识别前缀映射（stem.lower() startswith 任意一个前缀即命中）─────────────
# wangupay-（实际文件名少一个"g"）和 wangguypay- 均支持
PLATFORM_PREFIXES_5: dict = {
    "admin":      ["admin-"],
    "ibfpay":     ["ibfpay-", "ibf平台"],    # ibf平台 识别资金流水账格式文件
    "superpay":   ["superpay-"],
    "wangguypay": ["wangupay-", "wangguypay-", "wangupay资金记录", "wangguypay资金记录"],
    "phonecard":  ["okey话费卡结算"],
}

# ── 输出新增列（追加在 admin 原始列末尾，共 7 列）──────────────────────────
MATCH_STATUS_COL_5       = "是否匹配"       # 是 / 否 / 平台多余
PLATFORM_ORDER_NO_COL_5  = "平台流水号"     # 系统流水号 / 代付订单Id / 平台订单号
PLATFORM_AMOUNT_COL_5    = "平台代付金额"   # 平台记录的代付金额
PLATFORM_STATUS_COL_5    = "平台状态"       # 统一为：成功 / 失败 / 处理中 / 关闭
FEE_COL_5                = "手续费"         # 平台收取的手续费（IBFYPAY 无此字段，留空串）
ARRIVE_AMOUNT_COL_5      = "到账金额"       # 扣除手续费后实际到账（IBFYPAY 留空串）
TRANSACTION_DATE_COL_5   = "交易日期"       # 格式化为 YYYY-MM-DD 的交易时间

OUTPUT_NEW_COLS_5 = [
    MATCH_STATUS_COL_5,
    PLATFORM_ORDER_NO_COL_5,
    PLATFORM_AMOUNT_COL_5,
    PLATFORM_STATUS_COL_5,
    FEE_COL_5,
    ARRIVE_AMOUNT_COL_5,
    TRANSACTION_DATE_COL_5,
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
