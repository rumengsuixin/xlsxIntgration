"""代号6（代收代付对账）路径常量与列名配置。

数据来源（data/input/6/，源文件均支持 .csv/.xls/.xlsx，按扩展名自适应读取）：
    Admin收款订单明细*   → Excel 取"已完成订单"sheet，代收主表
    Admin兑换订单明细*   → Excel 取 Sheet1，代付主表
    betcat-payment_*     → 代收平台文件（多文件合并）
    betcat-payout_*      → 代付平台文件（多文件合并）
    Cashnewpay收款明细*  → Excel 取 Sheet1，代收平台文件
    Cashnewpay兑换明细*  → Excel 取 Sheet1，代付平台文件
"""

import os
from pathlib import Path

from .config import DATA_DIR, OUTPUT_DIR  # noqa: F401

# ── 路径常量 ──────────────────────────────────────────────────────────────────
# 输入目录支持 BANK_INPUT_DIR 环境变量覆盖（供 macro 桥接控制运行时目录）；
# 未设置时回退仓库默认 data/input/6，现有启动器/命令行行为完全不变。
INPUT_DIR_6 = Path(os.environ.get("BANK_INPUT_DIR", DATA_DIR / "input" / "6"))
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
    "goldenpay_collection": ["goldenpay收款"],
    "goldenpay_exchange":   ["goldenpay兑换"],
}

# ── Admin 收款主表（格式自适应，Excel sheet="已完成订单"）──────────────────────
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

# ── Admin 兑换主表（格式自适应，Excel sheet="Sheet1"）──────────────────────────
# 文件：Admin兑换订单明细*.xlsx，8 列，11,685 行（2026-04 样例）
ADMIN_PAYOUT_SHEET_6          = "Sheet1"
ADMIN_PAYOUT_JOIN_COL_6       = "订单号"            # 关联平台 MerOrderNo / 商户订单号
ADMIN_PAYOUT_AMOUNT_COL_6     = "金额"
ADMIN_PAYOUT_DATE_COL_6       = "日期"
ADMIN_PAYOUT_PHONE_TYPE_COL_6 = "话费种类"          # 如 R$30, R$200
ADMIN_PAYOUT_PLAYER_NO_COL_6  = "玩家号"
ADMIN_PAYOUT_PHONE_NO_COL_6   = "充值手机号"

# ── Betcat 平台（格式自适应，代收/代付列结构相同，header=0）────────────────────────
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

# ── Cashnewpay 平台（格式自适应，Excel sheet="Sheet1"，代收/代付列结构相同）───
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

# ── Goldenpay 平台（格式自适应，Excel）─────────────────────────────────────────
# 与 Betcat/Cashnewpay 不同：收款与兑换列结构不一致（收款 22 列 / 兑换 17 列），
# 金额列与平台单号列名收/付各异，故 build_goldenpay_lookup_6 读取时归一化到规范列名。
# 时间列为真正的 Excel datetime 对象；货币 BRL。
GOLDENPAY_COLLECTION_SHEET_6 = "代收订单导出"
GOLDENPAY_PAYOUT_SHEET_6     = "代付订单导出"
GOLDENPAY_JOIN_COL_6         = "商户单号"      # 收/付相同，关联 Admin.订单号
# 收/付相同
GOLDENPAY_FEE_COL_6          = "手续费"
GOLDENPAY_STATUS_COL_6       = "订单状态"       # 收款值="支付成功"，兑换值="成功"
GOLDENPAY_CREATE_TIME_COL_6  = "创建时间"
GOLDENPAY_FINISH_TIME_COL_6  = "完成时间"
# 收/付各异（作为 build 时的源列名，归一化到下方规范名）
GOLDENPAY_COLLECTION_PLATFORM_NO_SRC_6 = "订单号"    # 收款：平台单号源列（P 开头）
GOLDENPAY_COLLECTION_AMOUNT_SRC_6      = "订单金额"  # 收款：金额源列（非"实际金额"）
GOLDENPAY_PAYOUT_PLATFORM_NO_SRC_6     = "订单编号"  # 兑换：平台单号源列（T 开头）
GOLDENPAY_PAYOUT_AMOUNT_SRC_6          = "金额"      # 兑换：金额源列
# 归一化后的规范列名（enrich 统一引用；查找表内部列，merge 时加 _g_ 前缀，不与输出列冲突）
GOLDENPAY_PLATFORM_NO_COL_6  = "平台单号"
GOLDENPAY_AMOUNT_COL_6       = "金额"

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
    "GOLDENPAY": {
        "支付成功": "成功",
        "成功":     "成功",
        "支付失败": "失败",
        "失败":     "失败",
        "待支付":   "处理中",
        "处理中":   "处理中",
        "关闭":     "关闭",
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

# ── Admin 文件识别前缀（供 scan 独立识别主表，不属于"平台"注册表）──────────────
ADMIN_COLLECTION_PREFIXES_6 = PLATFORM_PREFIXES_6["admin_collection"]
ADMIN_PAYOUT_PREFIXES_6     = PLATFORM_PREFIXES_6["admin_payout"]

# ── 内置平台声明（外置化的"真值来源"）──────────────────────────────────────────
# 用上方现有常量拼装，避免重复硬编码字符串导致漂移。platform_loader 读取它作为
# 默认注册表，再叠加 platforms/6/*.json 与 platforms/plugins/*.py（外部可覆盖/新增）。
# 规范字段名（columns / directions.<dir>.columns 的键）取自 platform_spec.CANON：
#   platform_no / amount / fee / status / status_desc / finish_time / create_time
# Goldenpay 收/付平台单号列与金额列名不同，用 directions.<dir>.columns 覆盖，
# 通用引擎据此归一化，无需再写命令式 rename。
BUILTIN_SPECS_6: list = [
    {
        "key": "BETCAT",
        "priority": 10,
        "join_col": BETCAT_JOIN_COL_6,
        "handler": "generic",
        "use_first_sheet": False,          # 无 sheet 约定，取首个 sheet（不打 warning）
        "columns": {
            "platform_no":  BETCAT_PLATFORM_NO_COL_6,
            "amount":       BETCAT_AMOUNT_COL_6,
            "fee":          BETCAT_FEE_COL_6,
            "status":       BETCAT_STATUS_COL_6,
            "finish_time":  BETCAT_PAY_TIME_COL_6,
            "create_time":  BETCAT_CREATE_TIME_COL_6,
        },
        "status_map": PLATFORM_STATUS_MAP_6["BETCAT"],
        "directions": {
            "collection": {"prefixes": PLATFORM_PREFIXES_6["betcat_payment"]},
            "payout":     {"prefixes": PLATFORM_PREFIXES_6["betcat_payout"]},
        },
    },
    {
        "key": "CASHNEWPAY",
        "priority": 20,
        "join_col": CASHNEWPAY_JOIN_COL_6,
        "handler": "generic",
        "sheet": CASHNEWPAY_SHEET_6,
        "use_first_sheet": True,
        "columns": {
            "platform_no":  CASHNEWPAY_PLATFORM_NO_COL_6,
            "amount":       CASHNEWPAY_AMOUNT_COL_6,
            "fee":          CASHNEWPAY_FEE_COL_6,
            "status":       CASHNEWPAY_STATUS_COL_6,
            "status_desc":  CASHNEWPAY_STATE_DESC_COL_6,
            "finish_time":  CASHNEWPAY_FINISH_TIME_COL_6,
            "create_time":  CASHNEWPAY_CREATE_TIME_COL_6,
        },
        "status_map": PLATFORM_STATUS_MAP_6["CASHNEWPAY"],
        "status_prefix_map": CASHNEWPAY_STATUS_PREFIX_MAP_6,
        "directions": {
            "collection": {"prefixes": PLATFORM_PREFIXES_6["cashnewpay_collection"]},
            "payout":     {"prefixes": PLATFORM_PREFIXES_6["cashnewpay_exchange"]},
        },
    },
    {
        "key": "GOLDENPAY",
        "priority": 30,
        "join_col": GOLDENPAY_JOIN_COL_6,
        "handler": "generic",
        "use_first_sheet": True,
        "columns": {
            "fee":          GOLDENPAY_FEE_COL_6,
            "status":       GOLDENPAY_STATUS_COL_6,
            "finish_time":  GOLDENPAY_FINISH_TIME_COL_6,
            "create_time":  GOLDENPAY_CREATE_TIME_COL_6,
        },
        "status_map": PLATFORM_STATUS_MAP_6["GOLDENPAY"],
        "directions": {
            "collection": {
                "prefixes": PLATFORM_PREFIXES_6["goldenpay_collection"],
                "sheet": GOLDENPAY_COLLECTION_SHEET_6,
                "columns": {
                    "platform_no": GOLDENPAY_COLLECTION_PLATFORM_NO_SRC_6,
                    "amount":      GOLDENPAY_COLLECTION_AMOUNT_SRC_6,
                },
            },
            "payout": {
                "prefixes": PLATFORM_PREFIXES_6["goldenpay_exchange"],
                "sheet": GOLDENPAY_PAYOUT_SHEET_6,
                "columns": {
                    "platform_no": GOLDENPAY_PAYOUT_PLATFORM_NO_SRC_6,
                    "amount":      GOLDENPAY_PAYOUT_AMOUNT_SRC_6,
                },
            },
        },
    },
]
