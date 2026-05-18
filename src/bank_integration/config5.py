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

# ── IBFYPAY 平台（XLSX，sheet="Sheet"，header=0）─────────────────────────────
# 注意：IBFYPAY 无手续费列和到账金额列，匹配后 FEE_COL_5 / ARRIVE_AMOUNT_COL_5 留空串
IBFYPAY_SHEET_5            = "Sheet"
IBFYPAY_HEADER_5           = 0
IBFYPAY_JOIN_COL_5         = "订单号"       # 直接对应 admin.订单号
IBFYPAY_PLATFORM_NO_COL_5  = "系统流水号"
IBFYPAY_PRODUCT_COL_5      = "支付产品"
IBFYPAY_AMOUNT_COL_5       = "金额"
IBFYPAY_STATUS_COL_5       = "状态"
IBFYPAY_REMARK_COL_5       = "备注"
IBFYPAY_CALLBACK_COL_5     = "回调状态"
IBFYPAY_CREATE_TIME_COL_5  = "创建时间"
IBFYPAY_FINISH_TIME_COL_5  = "完成时间"
IBFYPAY_PAY_INFO_COL_5     = "付款信息"

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

# ── WANGGUYPAY 平台（XLSX，sheet="付款订单"，header=1，表头在第2行！）─────────
WANGGUYPAY_SHEET_5             = "付款订单"
WANGGUYPAY_HEADER_5            = 1              # 关键：实际文件第1行是无用大标题，列头在第2行
WANGGUYPAY_JOIN_COL_5          = "商户订单号"   # 对应 admin.订单号
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

# ── 文件识别前缀映射（stem.lower() startswith 任意一个前缀即命中）─────────────
# wangupay-（实际文件名少一个"g"）和 wangguypay- 均支持
PLATFORM_PREFIXES_5: dict = {
    "admin":      ["admin-"],
    "ibfpay":     ["ibfpay-"],
    "superpay":   ["superpay-"],
    "wangguypay": ["wangupay-", "wangguypay-"],
}

# ── 输出新增列（追加在 admin 原始列末尾，共 7 列）──────────────────────────
MATCH_STATUS_COL_5       = "是否匹配"       # 是 / 否 / 平台多余
PLATFORM_ORDER_NO_COL_5  = "平台流水号"     # 系统流水号 / 代付订单Id / 平台订单号
PLATFORM_AMOUNT_COL_5    = "平台代付金额"   # 平台记录的代付金额
PLATFORM_STATUS_COL_5    = "平台状态"       # 平台记录的交易状态
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
