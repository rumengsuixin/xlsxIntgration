"""代号7：汇率抓取与报表主流程。

用法：
  venv/Scripts/python.exe 整合7.py
  venv/Scripts/python.exe 整合7.py --date-range 2026-05-01 2026-05-31
  venv/Scripts/python.exe 整合7.py --date-range 2026-05-01 2026-05-31 --currencies USD TRY EUR
"""

import logging
import re
import sys
from datetime import date, timedelta
from typing import List, Optional, Tuple

import pandas as pd

from .config7 import DEFAULT_CURRENCIES_7, OUTPUT_DIR, OUTPUT_FILE_TEMPLATE_7, OUTPUT_SHEET_7
from .exchange_rate import batch_fetch_rates

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
USAGE = (
    "用法: 整合7.py [--date-range YYYY-MM-DD YYYY-MM-DD] [--currencies USD TRY ...]"
)


# ── 参数解析 ──────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> date:
    if not DATE_RE.match(s):
        raise ValueError(f"日期格式错误（应为 YYYY-MM-DD）：{s!r}")
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise ValueError(f"日期不存在：{s!r}")


def _get_previous_month_range() -> Tuple[date, date]:
    today = date.today()
    first_this_month = today.replace(day=1)
    last_prev = first_this_month - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


def parse_args(argv: Optional[List[str]] = None) -> Tuple[date, date, List[str]]:
    args = list(argv if argv is not None else sys.argv[1:])
    start: Optional[date] = None
    end: Optional[date] = None
    currencies: Optional[List[str]] = None

    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--date-range":
            if index + 2 >= len(args):
                raise ValueError(f"--date-range 需要两个参数。{USAGE}")
            start = _parse_date(args[index + 1])
            end = _parse_date(args[index + 2])
            if start > end:
                raise ValueError("开始日期不能晚于结束日期")
            index += 3
        elif arg == "--currencies":
            # 收集 --currencies 后面所有非 "--" 参数
            index += 1
            collected: List[str] = []
            while index < len(args) and not args[index].startswith("--"):
                collected.append(args[index].strip().upper())
                index += 1
            if not collected:
                raise ValueError(f"--currencies 至少需要一个货币代码。{USAGE}")
            currencies = collected
        else:
            raise ValueError(f"未知参数 {arg!r}。{USAGE}")

    if start is None or end is None:
        start, end = _get_previous_month_range()

    if currencies is None:
        currencies = list(DEFAULT_CURRENCIES_7)

    return start, end, currencies


# ── 日期范围生成 ──────────────────────────────────────────────────────────────

def _date_range(start: date, end: date) -> List[date]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


# ── Excel 输出 ────────────────────────────────────────────────────────────────

def write_output(df: pd.DataFrame, start: date, end: date) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filename = OUTPUT_FILE_TEMPLATE_7.format(
        start=start.isoformat(),
        end=end.isoformat(),
    )
    out_path = OUTPUT_DIR / filename

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=OUTPUT_SHEET_7, index=False)

    logger.info("已输出汇率报表：%s（共 %d 行）", out_path, len(df))


# ── 主入口 ────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> None:
    try:
        start, end, currencies = parse_args(argv)
    except ValueError as e:
        print(f"错误：{e}", file=sys.stderr)
        sys.exit(1)

    logger.info("代号7 汇率抓取开始：%s ~ %s，货币：%s", start, end, currencies)

    dates = _date_range(start, end)
    df = batch_fetch_rates(currencies, dates)

    # 列名排列：日期在最前，货币列按传入顺序
    cols = ["日期"] + [c for c in currencies if c in df.columns]
    df = df[cols]

    write_output(df, start, end)
    logger.info("代号7 完成")
