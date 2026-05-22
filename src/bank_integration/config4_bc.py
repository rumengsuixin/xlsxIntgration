"""代号4 子功能：BC 平台（betcatpay）浏览器自动化配置。"""
import os

from .config import DATA_DIR, OUTPUT_DIR
from .config4 import CHROME_DEBUG_PORT_4

BC_REPORT_URL_TEMPLATE = (
    "https://ajv23m50.m.betcatpay.com/report/daily-payment"
    "?start_time={start_time}&end_time={end_time}"
    "&pageIndex={page}&pageSize=10&isPaging=1"
)

# BC 平台使用独立 Chrome profile，与 aim1/epin 完全隔离
BC_CHROME_PROFILE_DIR = DATA_DIR / "browser_profile" / "4_bc"
CHROME_DEBUG_PORT_BC = CHROME_DEBUG_PORT_4

BC_OUTPUT_DIR = OUTPUT_DIR / "4_bc"
BC_EXTRACT_DIR = OUTPUT_DIR / "4_bc" / "extracted"

# 下载文件名前缀：payment_YYYYMMDD_*.zip
BC_ZIP_FILENAME_PREFIX = "payment_"

# 行点击之间的随机等待范围（秒），可通过 .env 覆盖
BC_CLICK_INTERVAL_MIN_SECONDS: float = float(os.getenv("BC_CLICK_INTERVAL_MIN_SECONDS", "1.0"))
BC_CLICK_INTERVAL_MAX_SECONDS: float = float(os.getenv("BC_CLICK_INTERVAL_MAX_SECONDS", "3.0"))
