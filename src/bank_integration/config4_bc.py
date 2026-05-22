"""代号4 子功能：BC 平台（betcatpay）浏览器自动化配置。"""
from .config import OUTPUT_DIR
from .config4 import CHROME_PROFILE_DIR_4, CHROME_DEBUG_PORT_4

BC_REPORT_URL_TEMPLATE = (
    "https://ajv23m50.m.betcatpay.com/report/daily-payment"
    "?start_time={start_time}&end_time={end_time}"
    "&pageIndex={page}&pageSize=10&isPaging=1"
)

# 与 aim1 共用同一 Chrome 实例（同 profile 目录 + 同调试端口）
BC_CHROME_PROFILE_DIR = CHROME_PROFILE_DIR_4
CHROME_DEBUG_PORT_BC = CHROME_DEBUG_PORT_4

BC_OUTPUT_DIR = OUTPUT_DIR / "4_bc"
BC_EXTRACT_DIR = OUTPUT_DIR / "4_bc" / "extracted"

# 下载文件名前缀：payment_YYYYMMDD_*.zip
BC_ZIP_FILENAME_PREFIX = "payment_"
