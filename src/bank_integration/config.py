"""Project paths and bank-specific configuration."""

from pathlib import Path
import sys


def get_project_root() -> Path:
    """Return the folder that contains template/ and data/."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


PROJECT_ROOT = get_project_root()
TEMPLATE_DIR = PROJECT_ROOT / "template"
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input" / "1"
RAW_INPUT_DIR = DATA_DIR / "input" / "raw"
OUTPUT_DIR = DATA_DIR / "output"

SUMMARY_FILE = "国内银行汇总.xlsx"
TEMPLATE_PATH = TEMPLATE_DIR / "1" / SUMMARY_FILE
OUTPUT_PATH = OUTPUT_DIR / SUMMARY_FILE

BANK_ABBR = {
    "招商银行": "招行",
    "建设银行": "建行",
    "工商银行": "工行",
    "中信银行": "中信",
    "浦发银行": "浦发",
    "农业银行": "农行",
    "中国银行": "中行",
}

BANK_READ_CONFIG = {
    "工商银行": {"header": 1, "engine": "openpyxl", "is_csv": False},
    "中信银行": {"header": 15, "engine": "openpyxl", "is_csv": False},
    "招商银行": {"header": 12, "engine": "openpyxl", "is_csv": False},
    "农业银行": {"header": 2, "engine": "xlrd", "is_csv": False},
    "建设银行": {"header": 9, "engine": "xlrd", "is_csv": False},
    "浦发银行": {"header": 4, "engine": "xlrd", "is_csv": False},
    "中国银行": {"header": 7, "engine": None, "is_csv": True},
}

BANK_BALANCE_COL = {
    "工商银行": "余额",
    "中信银行": "账户余额",
    "招商银行": "余额",
    "农业银行": "账户余额",
    "建设银行": "余额",
    "浦发银行": "余额",
    "中国银行": "交易后余额",
}

BANK_DATE_COL = {
    "工商银行": "交易时间",
    "中信银行": "交易日期",
    "招商银行": "交易日",
    "农业银行": "交易时间",
    "建设银行": "交易时间",
    "浦发银行": "交易日期",
    "中国银行": "交易日期",
}

BALANCE_SHEET = "银行余额"
PROTECTED_SHEETS = {"Sheet9", BALANCE_SHEET}

BALANCE_BANK_ORDER = [
    "招商银行",
    "浦发银行",
    "中国银行",
    "工商银行",
    "建设银行",
    "农业银行",
    "兴业银行",
    "中信银行",
]
