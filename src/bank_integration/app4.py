"""Mode 4 recharge order browser export."""

import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple, TypeVar

from .config4 import (
    CHROME_PROFILE_DIR_4,
    EXPORT_BATCH_SIZE_4,
    EXPORT_BATCH_WAIT_SECONDS_4,
    EXPORT_COMPLETED_SUFFIXES_4,
    EXPORT_DOWNLOAD_DIR_4,
    EXPORT_RETRY_LIMIT_4,
    EXPORT_URL_TEMPLATE,
)


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PROFILE_NAME = "Default"
T = TypeVar("T")
DATE_ARGS_USAGE = "用法: 整合4.py [--date-range YYYY-MM-DD YYYY-MM-DD] [--wait-seconds N]"


def parse_date(value: str) -> date:
    """Parse a strict YYYY-MM-DD date."""
    text = value.strip()
    if not DATE_RE.fullmatch(text):
        raise ValueError("日期格式必须是 YYYY-MM-DD，例如 2025-05-12")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"日期不合法: {text}") from exc
    if parsed.strftime("%Y-%m-%d") != text:
        raise ValueError(f"日期不合法: {text}")
    return parsed


def iter_dates(start: date, end: date) -> Iterable[date]:
    """Yield each natural day in [start, end]."""
    if start > end:
        raise ValueError("开始日期不能晚于结束日期")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_export_url(day: date) -> str:
    """Build one export URL. The [PAGE] placeholder is intentionally preserved."""
    date_text = day.strftime("%Y-%m-%d")
    return EXPORT_URL_TEMPLATE.format(date=date_text)


def build_export_urls(start: date, end: date) -> List[str]:
    return [build_export_url(day) for day in iter_dates(start, end)]


def get_previous_month_range(today: date) -> Tuple[date, date]:
    first_day_this_month = today.replace(day=1)
    last_day_previous_month = first_day_this_month - timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)
    return first_day_previous_month, last_day_previous_month


def parse_positive_int(value: str, option_name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{option_name} 必须是正整数") from exc
    if parsed <= 0:
        raise ValueError(f"{option_name} 必须是正整数")
    return parsed


def parse_date_args(argv: Optional[Sequence[str]] = None, today: Optional[date] = None) -> Tuple[date, date, int]:
    args = list(sys.argv[1:] if argv is None else argv)
    start: Optional[date] = None
    end: Optional[date] = None
    wait_seconds = EXPORT_BATCH_WAIT_SECONDS_4

    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--date-range":
            if index + 2 >= len(args):
                raise ValueError(f"参数错误。{DATE_ARGS_USAGE}")
            if start is not None:
                raise ValueError(f"参数错误。{DATE_ARGS_USAGE}")
            start = parse_date(args[index + 1])
            end = parse_date(args[index + 2])
            index += 3
        elif arg == "--wait-seconds":
            if index + 1 >= len(args):
                raise ValueError(f"参数错误。{DATE_ARGS_USAGE}")
            wait_seconds = parse_positive_int(args[index + 1], "--wait-seconds")
            index += 2
        else:
            raise ValueError(f"参数错误。{DATE_ARGS_USAGE}")

    if start is None or end is None:
        start, end = get_previous_month_range(today or date.today())
    if start > end:
        raise ValueError("开始日期不能晚于结束日期")
    return start, end, wait_seconds


def chunk_list(values: Sequence[T], chunk_size: int) -> List[List[T]]:
    """Split values into non-empty fixed-size chunks."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    return [list(values[index : index + chunk_size]) for index in range(0, len(values), chunk_size)]


def find_chrome_executable() -> Optional[str]:
    """Return a Chrome executable path or command name."""
    system = platform.system().lower()

    candidates: List[str] = []
    if system == "windows":
        candidates.extend(["chrome", "chrome.exe"])
        for env_name in ("PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"):
            base = os.environ.get(env_name)
            if base:
                candidates.append(str(Path(base) / "Google" / "Chrome" / "Application" / "chrome.exe"))
    elif system == "darwin":
        candidates.append("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
        candidates.extend(["google-chrome", "chrome"])
    else:
        candidates.extend(["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"])

    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def configure_chrome_downloads(profile_dir: Path, download_dir: Path) -> Path:
    """Create/update Chrome Preferences so downloads go to download_dir."""
    download_dir.mkdir(parents=True, exist_ok=True)
    default_dir = profile_dir / PROFILE_NAME
    default_dir.mkdir(parents=True, exist_ok=True)
    prefs_path = default_dir / "Preferences"

    prefs = {}
    if prefs_path.exists():
        try:
            with prefs_path.open("r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            if isinstance(loaded, dict):
                prefs = loaded
        except (json.JSONDecodeError, OSError):
            logging.warning("Chrome Preferences 无法读取，将重新写入下载配置: %s", prefs_path)

    download_prefs = prefs.setdefault("download", {})
    download_prefs["default_directory"] = str(download_dir.resolve())
    download_prefs["prompt_for_download"] = False
    download_prefs["directory_upgrade"] = True
    prefs.setdefault("safebrowsing", {})["enabled"] = True

    with prefs_path.open("w", encoding="utf-8") as fh:
        json.dump(prefs, fh, ensure_ascii=False, indent=2)
    return prefs_path


def get_primary_cookie_path(profile_dir: Path) -> Path:
    return profile_dir / PROFILE_NAME / "Cookies"


def get_network_cookie_path(profile_dir: Path) -> Path:
    return profile_dir / PROFILE_NAME / "Network" / "Cookies"


def _is_nonempty_file(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def has_chrome_cookie_store(profile_dir: Path) -> bool:
    return _is_nonempty_file(get_primary_cookie_path(profile_dir))


def log_cookie_store_status(profile_dir: Path) -> None:
    primary_cookie_path = get_primary_cookie_path(profile_dir)
    network_cookie_path = get_network_cookie_path(profile_dir)

    if _is_nonempty_file(primary_cookie_path):
        logging.info("检测到有效 Chrome Cookie 数据，profile 已写入可复用登录态:")
        logging.info("  %s", primary_cookie_path)
    else:
        logging.warning("尚未检测到有效 Cookie 文件: %s", primary_cookie_path)
        logging.warning("若已登录，请确认是在本程序打开的独立 Chrome 窗口中完成登录。")

    if _is_nonempty_file(network_cookie_path):
        logging.info("检测到 Network Cookie 数据，但不作为有效登录态判定依据:")
        logging.info("  %s", network_cookie_path)


def build_chrome_args(chrome_path: str, profile_dir: Path, urls: List[str]) -> List[str]:
    args = [
        chrome_path,
        f"--user-data-dir={profile_dir.resolve()}",
        f"--profile-directory={PROFILE_NAME}",
        "--no-first-run",
        "--no-default-browser-check",
        "--remote-debugging-port=0",
        "--class=bank-integration-export",
    ]
    args.extend(urls)
    return args


def launch_chrome(chrome_path: str, profile_dir: Path, urls: List[str]) -> subprocess.Popen:
    args = build_chrome_args(chrome_path, profile_dir, urls)
    return subprocess.Popen(args)


def expected_export_stem(day: date) -> str:
    date_text = day.strftime("%Y-%m-%d")
    return f"{date_text},{date_text}"


def has_completed_export_file(download_dir: Path, day: date, suffixes: Sequence[str]) -> bool:
    """Return whether the expected completed export file exists for day."""
    if not download_dir.exists():
        return False
    expected_stem = expected_export_stem(day)
    normalized_suffixes = tuple(suffix.lower() for suffix in suffixes)
    for path in download_dir.iterdir():
        if not path.is_file():
            continue
        if path.stem == expected_stem and path.suffix.lower() in normalized_suffixes:
            return True
    return False


def missing_export_dates(download_dir: Path, days: Sequence[date], suffixes: Sequence[str]) -> List[date]:
    return [day for day in days if not has_completed_export_file(download_dir, day, suffixes)]


def wait_for_export_files(
    download_dir: Path,
    days: Sequence[date],
    timeout_seconds: int,
    suffixes: Sequence[str],
    poll_seconds: float = 1.0,
) -> List[date]:
    """Wait until all expected date-named exports exist, then return missing dates."""
    deadline = time.monotonic() + timeout_seconds
    latest_missing = missing_export_dates(download_dir, days, suffixes)
    while latest_missing and time.monotonic() < deadline:
        time.sleep(poll_seconds)
        latest_missing = missing_export_dates(download_dir, days, suffixes)
    return latest_missing


def export_batches_with_retries(
    chrome_path: str,
    profile_dir: Path,
    download_dir: Path,
    dated_urls: Sequence[Tuple[date, str]],
    batch_size: int = EXPORT_BATCH_SIZE_4,
    wait_seconds: int = EXPORT_BATCH_WAIT_SECONDS_4,
    retry_limit: int = EXPORT_RETRY_LIMIT_4,
    completed_suffixes: Sequence[str] = EXPORT_COMPLETED_SUFFIXES_4,
    launcher: Callable[[str, Path, List[str]], subprocess.Popen] = launch_chrome,
    waiter: Callable[[Path, Sequence[date], int, Sequence[str]], List[date]] = wait_for_export_files,
) -> List[date]:
    """Open export URLs in batches. Return dates that still lack expected export files."""
    failures: List[date] = []
    batches = chunk_list(dated_urls, batch_size)

    for batch_index, batch in enumerate(batches, start=1):
        pending = list(batch)
        batch_dates = [day for day, _url in pending]

        for attempt in range(1, retry_limit + 2):
            pending_dates = [day for day, _url in pending]
            pending_urls = [url for _day, url in pending]
            logging.info(
                "打开第 %d/%d 批导出链接（第 %d 次尝试）: %s 至 %s，共 %d 个",
                batch_index,
                len(batches),
                attempt,
                pending_dates[0].strftime("%Y-%m-%d"),
                pending_dates[-1].strftime("%Y-%m-%d"),
                len(pending_urls),
            )
            launcher(chrome_path, profile_dir, pending_urls)
            missing_dates = waiter(download_dir, pending_dates, wait_seconds, completed_suffixes)
            if not missing_dates:
                logging.info("第 %d 批下载完成：已检测到 %d 个日期文件", batch_index, len(batch_dates))
                break

            if attempt <= retry_limit:
                missing_text = ", ".join(day.strftime("%Y-%m-%d") for day in missing_dates)
                logging.warning(
                    "第 %d 批下载不完整，缺失 %d 个日期：%s，准备只重试缺失项",
                    batch_index,
                    len(missing_dates),
                    missing_text,
                )
                missing_set = set(missing_dates)
                pending = [(day, url) for day, url in pending if day in missing_set]
        else:
            failures.extend(missing_dates)
            missing_text = ", ".join(
                f"{expected_export_stem(day)}.{{xls,xlsx,csv}}" for day in missing_dates
            )
            logging.error(
                "第 %d 批超过重试次数仍不完整：%s 至 %s，缺失文件：%s",
                batch_index,
                batch_dates[0].strftime("%Y-%m-%d"),
                batch_dates[-1].strftime("%Y-%m-%d"),
                missing_text,
            )

    return failures


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        start, end, wait_seconds = parse_date_args(argv)
        dated_urls = [(day, build_export_url(day)) for day in iter_dates(start, end)]
        urls = [url for _day, url in dated_urls]
    except ValueError as exc:
        logging.error("%s", exc)
        return 1

    logging.info("导出日期范围: %s 至 %s", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    logging.info("每批下载等待时间: %d 秒", wait_seconds)

    chrome_path = find_chrome_executable()
    if not chrome_path:
        logging.error("未找到 Google Chrome。请先安装 Chrome，或确认 chrome 命令可在终端中运行。")
        return 1

    prefs_path = configure_chrome_downloads(CHROME_PROFILE_DIR_4, EXPORT_DOWNLOAD_DIR_4)
    has_cookie_before_launch = has_chrome_cookie_store(CHROME_PROFILE_DIR_4)

    logging.info("Chrome: %s", chrome_path)
    logging.info("下载目录: %s", EXPORT_DOWNLOAD_DIR_4.resolve())
    logging.info("Chrome user-data-dir: %s", CHROME_PROFILE_DIR_4.resolve())
    logging.info("Chrome profile: %s", (CHROME_PROFILE_DIR_4 / PROFILE_NAME).resolve())
    logging.info("下载配置已写入: %s", prefs_path)
    logging.info("请用“重新运行代号4”验证登录态；普通 Chrome 手动打开不会共享这个独立登录环境。")
    log_cookie_store_status(CHROME_PROFILE_DIR_4)

    if not has_cookie_before_launch:
        logging.info("当前独立 Chrome profile 还没有 Cookie 数据，将先打开第一个导出链接用于登录。")
        logging.info("请在打开的 Chrome 窗口中完成登录，不要马上关闭 Chrome；登录后回到此终端按回车继续打开导出链接。")
        try:
            launch_chrome(chrome_path, CHROME_PROFILE_DIR_4, urls[:1])
            input("登录完成后按回车继续打开导出链接：")
            log_cookie_store_status(CHROME_PROFILE_DIR_4)
        except Exception:
            logging.error("启动 Chrome 失败", exc_info=True)
            return 1

    for day in iter_dates(start, end):
        logging.info("导出日期 %s: %s", day.strftime("%Y-%m-%d"), build_export_url(day))

    try:
        failures = export_batches_with_retries(
            chrome_path,
            CHROME_PROFILE_DIR_4,
            EXPORT_DOWNLOAD_DIR_4,
            dated_urls,
            wait_seconds=wait_seconds,
        )
    except Exception:
        logging.error("启动 Chrome 或等待下载失败", exc_info=True)
        return 1

    if failures:
        logging.error("导出完成，但有 %d 个日期未检测到完整下载。", len(failures))
        for missing_day in failures:
            logging.error(
                "缺失日期: %s，期望文件名: %s.{xls,xlsx,csv}",
                missing_day.strftime("%Y-%m-%d"),
                expected_export_stem(missing_day),
            )
        return 1

    logging.info("导出完成，已检测到 %d 个日期文件。", len(urls))
    return 0
