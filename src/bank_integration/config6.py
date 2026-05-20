"""代号6（代收代付对账）路径常量与列名配置。

数据来源（data/input/6/）：
    Admin收款订单明细*.xlsx   → 已完成订单 sheet，代收主表
    Admin兑换订单明细*.xlsx   → Sheet1，代付主表
    betcat-payment_*.csv     → 代收平台文件（多文件合并）
    betcat-payout_*.csv      → 代付平台文件（多文件合并）
    Cashnewpay收款明细*.xlsx  → Sheet1，代收平台文件
    Cashnewpay兑换明细*.xlsx  → Sheet1，代付平台文件
"""

from .config import DATA_DIR, OUTPUT_DIR  # noqa: F401

# ── 路径常量 ──────────────────────────────────────────────────────────────────
INPUT_DIR_6 = DATA_DIR / "input" / "6"
OUTPUT_FILE_TEMPLATE_6 = "代收代付对账结果_{date}.xlsx"

# ── 输出工作表名称（5 个 sheet）──────────────────────────────────────────────
OUTPUT_COLLECTION_SHEET_6       = "代收对账结果"
OUTPUT_COLLECTION_FAILED_SHEET_6 = "代收匹配失败"
OUTPUT_PAYOUT_SHEET_6           = "代付对账结果"
OUTPUT_PAYOUT_FAILED_SHEET_6    = "代付匹配失败"
OUTPUT_SUMMARY_SHEET_6          = "平台汇总"
OUTPUT_AMOUNT_DIFF_SHEET_6      = "金额差异订单"

# ── 文件识别前缀映射（stem 小写 startswith 匹配）─────────────────────────────
# 键名与 scan_source_files_6 返回 dict 的键一一对应
PLATFORM_PREFIXES_6: dict = {
    "admin_collection":    ["admin收款"],
    "admin_payout":        ["admin兑换"],
    "betcat_payment":      ["betcat-payment"],
    "betcat_payout":       ["betcat-payout"],
    "cashnewpay_collection": ["cashnewpay收款"],
    "cashnewpay_exchange": ["cashnewpay兑换"],
}

# ── Admin 收款主表（xlsx，openpyxl，sheet="已完成订单"）──────────────────────
# 文件：Admin收款订单明细*.xlsx，14 列，27,247 行（2026-04 样例）
ADMIN_COLLECTION_SHEET_6           = "已完成订单"
ADMIN_COLLECTION_JOIN_COL_6        = "订单号"       # 关联平台 MerOrderNo / 商户订单号
ADMIN_COLLECTION_AMOUNT_COL_6      = "金额"
ADMIN_COLLECTION_DATE_COL_6        = "支付时间"     # 实际支付时间
ADMIN_COLLECTION_CREATE_TIME_COL_6 = "创建时间"
ADMIN_COLLECTION_CONFIRM_TIME_COL_6 = "确认时间"
ADMIN_COLLECTION_GAME_COL_6        = "充值游戏"
ADMIN_COLLECTION_PLAYER_ID_COL_6   = "玩家ID"
ADMIN_COLLECTION_PAY_METHOD_COL_6  = "支付方式"     # BetcatPay / CashnewPay 等

# ── Admin 兑换主表（xlsx，openpyxl，sheet="Sheet1"）──────────────────────────
# 文件：Admin兑换订单明细*.xlsx，8 列，11,685 行（2026-04 样例）
ADMIN_PAYOUT_SHEET_6          = "Sheet1"
ADMIN_PAYOUT_JOIN_COL_6       = "订单号"            # 关联平台 MerOrderNo / 商户订单号
ADMIN_PAYOUT_AMOUNT_COL_6     = "金额"
ADMIN_PAYOUT_DATE_COL_6       = "日期"
ADMIN_PAYOUT_PHONE_TYPE_COL_6 = "话费种类"          # 如 R$30, R$200
ADMIN_PAYOUT_PLAYER_NO_COL_6  = "玩家号"
ADMIN_PAYOUT_PHONE_NO_COL_6   = "充值手机号"

# ── Betcat 平台（CSV，代收/代付列结构相同，header=0）────────────────────────
# 时间格式：ISO 8601 带时区，如 2026-04-01T00:07:49-03:00
# 货币：BRL（巴西雷亚尔）
BETCAT_HEADER_6           = 0
BETCAT_JOIN_COL_6         = "MerOrderNo"        # 商户订单号，关联 Admin.订单号
BETCAT_PLATFORM_NO_COL_6  = "OrderNo"           # Betcat 系统内部流水号
BETCAT_CHANNEL_ORDER_COL_6 = "ChannelOrderNo"
BETCAT_CHANNEL_TRADE_COL_6 = "ChannelTradeNo"
BETCAT_AMOUNT_COL_6       = "Amount"
BETCAT_CURRENCY_COL_6     = "Currency"
BETCAT_STATUS_COL_6       = "Status"
BETCAT_FEE_COL_6          = "TradeCharge"       # 代收有值；代付通常为 0
BETCAT_CREATE_TIME_COL_6  = "CreateTime"
BETCAT_PAY_TIME_COL_6     = "PayTime"

# ── Cashnewpay 平台（xlsx，openpyxl，sheet="Sheet1"，代收/代付列结构相同）───
# 时间格式：字符串含毫秒，如 "2026-04-28 08:30:03.313"
CASHNEWPAY_SHEET_6             = "Sheet1"
CASHNEWPAY_JOIN_COL_6          = "商户订单号"   # 关联 Admin.订单号
CASHNEWPAY_PLATFORM_NO_COL_6   = "订单号"       # Cashnewpay 内部订单号
CASHNEWPAY_E_ORDER_COL_6       = "E单号"
CASHNEWPAY_AMOUNT_COL_6        = "订单金额"
CASHNEWPAY_FEE_COL_6           = "手续费"       # 代收有值；兑换通常为 0
CASHNEWPAY_STATUS_COL_6        = "订单状态"     # 如"成功"
CASHNEWPAY_CALLBACK_COL_6      = "回调状态"
CASHNEWPAY_SETTLE_COL_6        = "结算状态"
CASHNEWPAY_CREATE_TIME_COL_6   = "创建时间"
CASHNEWPAY_FINISH_TIME_COL_6   = "完成时间"
CASHNEWPAY_CUSTOMER_COL_6      = "客户姓名"
CASHNEWPAY_CPF_COL_6           = "CPF"
CASHNEWPAY_PIX_TYPE_COL_6      = "Pix类型"
CASHNEWPAY_PIX_ACCOUNT_COL_6   = "Pix账号"
CASHNEWPAY_STATE_DESC_COL_6    = "状态描述"     # 英文原始状态，如 PAID
CASHNEWPAY_CHANNEL_COL_6       = "支付渠道"

# ── 输出新增列（代收/代付共用，追加在 admin 原始列之后）─────────────────────
MATCH_STATUS_COL_6      = "是否匹配"    # 是 / 否 / 平台多余
PLATFORM_SOURCE_COL_6   = "来源平台"    # BETCAT / CASHNEWPAY
PLATFORM_ORDER_NO_COL_6 = "平台流水号"  # Betcat.OrderNo 或 Cashnewpay.订单号
PLATFORM_AMOUNT_COL_6   = "平台金额"
PLATFORM_STATUS_COL_6   = "平台状态"   # 标准化：成功 / 失败 / 处理中 / 关闭
FEE_COL_6               = "手续费"
TRANSACTION_DATE_COL_6  = "交易日期"   # YYYY-MM-DD

OUTPUT_NEW_COLS_6 = [
    MATCH_STATUS_COL_6,
    PLATFORM_SOURCE_COL_6,
    PLATFORM_ORDER_NO_COL_6,
    PLATFORM_AMOUNT_COL_6,
    PLATFORM_STATUS_COL_6,
    FEE_COL_6,
    TRANSACTION_DATE_COL_6,
]

# ── 平台状态映射（原始 → 标准化）────────────────────────────────────────────
# Betcat Status 已知值（来自 2026-04 样例）；如遇未知值将保留原文并打 warning
PLATFORM_STATUS_MAP_6: dict = {
    "BETCAT": {
        "支付已通知": "成功",
        "支付成功":   "成功",
        "支付完成":   "成功",
        "支付失败":   "失败",
        "支付关闭":   "关闭",
        "待支付":     "处理中",
        "处理中":     "处理中",
    },
    "CASHNEWPAY": {
        "成功":   "成功",
        "失败":   "失败",
        "处理中": "处理中",
        "退款":   "关闭",
        # 状态描述（英文）—— 精确匹配
        "PAID":            "成功",
        "FAILED":          "失败",
        "failed":          "失败",
        "REJECTED":        "失败",
        "Payment failed":  "失败",
        "PENDING":         "处理中",
        "CREATED":         "处理中",
        "PROCESSING":      "处理中",
    },
}

# Cashnewpay 状态描述前缀映射（状态值含动态后缀时使用，如 USER_REFUND-<单号>）
CASHNEWPAY_STATUS_PREFIX_MAP_6: dict = {
    "USER_REFUND": "关闭",
    "REFUND":      "关闭",
}

# ── 平台汇总输出列 ────────────────────────────────────────────────────────────
SUMMARY_TYPE_COL_6    = "类型"          # 代收 / 代付
SUMMARY_PLATFORM_COL_6 = "来源平台"
SUMMARY_MONTH_COL_6   = "交易月份"      # YYYY-MM
SUMMARY_COUNT_COL_6   = "笔数"
SUMMARY_AMOUNT_COL_6  = "金额合计"
SUMMARY_FEE_COL_6     = "手续费合计"
SUMMARY_ARRIVE_COL_6  = "到账金额合计"  # 金额合计 - 手续费合计
