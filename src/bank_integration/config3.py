"""代号3（游戏订单匹配）路径常量与列名配置。"""

from .config import DATA_DIR, OUTPUT_DIR  # noqa: F401

INPUT_DIR_3 = DATA_DIR / "input" / "3"
OUTPUT_FILE_TEMPLATE = "订单匹配结果_{date}.xlsx"
OUTPUT_SHEET_3 = "订单匹配结果"
OUTPUT_SUMMARY_SHEET_3 = "交易金额汇总"
OUTPUT_APPLE_SHEET_3 = "苹果支付"

# ── Admin 主表 ────────────────────────────────────────────
ADMIN_SHEET = "汇总"
ADMIN_JOIN_COL = "流水号"
ADMIN_AMOUNT_COL = "金额"        # 用于 Google 国家税费计算
ADMIN_PAYMENT_COL = "支付方式"   # 值为 Adyen / 华为支付 / Google支付 / 苹果支付Lua（苹果不做平台关联）
ADMIN_REFUND_COL = "是否退款"    # 值为 正常 / 已退款

# ── Adyen 平台 ────────────────────────────────────────────
ADYEN_SHEET = "Data"
ADYEN_JOIN_COL = "Psp Reference"
ADYEN_RECORD_TYPE_COL = "Record Type"
ADYEN_AMOUNT_COL = "Main Amount"
ADYEN_CURRENCY_COL = "Main Currency"
ADYEN_SETTLEMENT_CURRENCY_COL = "Settlement Currency"
# 去重优先级（取交易金额最准确的行）
ADYEN_RECORD_TYPE_PRIORITY = ["SentForSettle", "Authorised"]
# 结算与手续费（均来自 SentForSettle 行，Settlement Currency）
ADYEN_PAYABLE_COL     = "Payable (SC)"
ADYEN_MARKUP_COL      = "Markup (SC)"
ADYEN_SCHEME_FEES_COL = "Scheme Fees (SC)"
ADYEN_INTERCHANGE_COL = "Interchange (SC)"

# ── 华为平台 ──────────────────────────────────────────────
HUAWEI_SHEET = "Sheet0"
HUAWEI_JOIN_COL = "华为订单号"
HUAWEI_AMOUNT_COL = "支付金额"
HUAWEI_CURRENCY_COL = "交易货币"

# ── Google Play 平台 ──────────────────────────────────────
GOOGLE_JOIN_COL = "Description"            # GPA.xxx 格式，对应 admin 流水号
GOOGLE_TRANSACTION_TYPE_COL = "Transaction Type"
GOOGLE_CHARGE_TYPE = "Charge"
GOOGLE_FEE_TYPE = "Google fee"
GOOGLE_REFUND_TYPE = "Charge refund"
GOOGLE_FEE_REFUND_TYPE = "Google fee refund"
GOOGLE_BUYER_AMOUNT_COL = "Amount (Buyer Currency)"
GOOGLE_BUYER_CURRENCY_COL = "Buyer Currency"
GOOGLE_MERCHANT_AMOUNT_COL = "Amount (Merchant Currency)"
GOOGLE_MERCHANT_CURRENCY_COL = "Merchant Currency"

# ── 输出新增列（共 7 列）────────────────────────────────────
PLATFORM_AMOUNT_COL   = "平台订单金额"
PLATFORM_CURRENCY_COL = "平台币种"
SETTLEMENT_CURRENCY_COL = "结算币种"
STATUS_COL            = "状态"               # 成功 / 失败 / 退款
SETTLEMENT_AMOUNT_COL = "结算金额"           # 扣除手续费后到账金额
FEE_COL               = "手续费"             # 平台手续费
COUNTRY_TAX_COL       = "国家税费"           # Google 专属：admin.金额 - Charge(TRY)

# ── 日期列与第7输出列 ─────────────────────────────────────
ADMIN_DATE_COL       = "支付时间"
ADYEN_DATE_COL       = "Booking Date"
HUAWEI_DATE_COL      = "支付时间 (基于UTC+8)"
GOOGLE_DATE_COL      = "Transaction Date"
TRANSACTION_DATE_COL = "交易日期"

# ── 华为结算文件（月度聚合，header=1读取英文列名）────────────
# 文件格式：第1行=中文列名，第2行=英文列名（作为列头），第3行起=数据
HUAWEI_SETTLE_DATE_COL      = "End Date"                                          # YYYYMM格式
HUAWEI_SETTLE_AMOUNT_COL    = "Settlement Amount (Settlement currency)"            # 结算金额（HKD）
HUAWEI_SETTLE_CURRENCY_COL  = "Settlement Currency"                               # 结算货币（HKD）
HUAWEI_SETTLE_TOTAL_TRX_COL = "Total Transaction Amount (Transaction Currency)"   # 总交易额（交易货币）
HUAWEI_SETTLE_VAT_COL       = "Output VAT (Transaction Currency)"                 # 销项税额（交易货币）
HUAWEI_SETTLE_RATE_COL      = "Exchange Rate for Settlement"                      # 结算汇率
