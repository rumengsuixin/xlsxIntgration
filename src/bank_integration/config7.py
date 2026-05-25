"""代号7（汇率抓取与报表）路径常量与默认配置。"""

from .config import DATA_DIR, OUTPUT_DIR  # noqa: F401

# ── 路径常量 ──────────────────────────────────────────────────────────────────
RATE_CACHE_FILE = DATA_DIR / "exchange_rates_cache.json"
OUTPUT_FILE_TEMPLATE_7 = "汇率_{start}_{end}.xlsx"
OUTPUT_SHEET_7 = "汇率数据"

# ── XE.com 汇率数据源 ─────────────────────────────────────────────────────────
# from=USD 时表格含 "USD per unit" 列，即 1 单位指定货币 = X 美元
XE_URL_TEMPLATE_7 = "https://www.xe.com/currencytables/?from=USD&date={date}"

# ── 默认抓取货币列表（可通过 --currencies 参数覆盖） ───────────────────────────
DEFAULT_CURRENCIES_7 = ["USD", "TRY", "EUR", "HKD", "SGD", "THB"]

# ── 抓取参数 ──────────────────────────────────────────────────────────────────
# 周末/假日 XE.com 可能无数据，向前最多回退 N 天
MAX_FALLBACK_DAYS_7 = 3
