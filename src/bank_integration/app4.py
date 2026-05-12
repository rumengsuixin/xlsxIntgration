"""Mode 4 recharge order browser export."""

import json
import logging
import os
import platform
import re
import shutil
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .config4 import CHROME_PROFILE_DIR_4, EXPORT_DOWNLOAD_DIR_4, EXPORT_URL_TEMPLATE


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
    default_dir = profile_dir / "Default"
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


def launch_chrome(chrome_path: str, profile_dir: Path, urls: List[str]) -> subprocess.Popen:
    args = [
        chrome_path,
        f"--user-data-dir={profile_dir.resolve()}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    args.extend(urls)
    return subprocess.Popen(args)


def prompt_dates() -> Tuple[date, date]:
    start_text = input("请输入支付开始日期 pay_sdate（YYYY-MM-DD）：")
    end_text = input("请输入支付结束日期 pay_edate（YYYY-MM-DD）：")
    start = parse_date(start_text)
    end = parse_date(end_text)
    if start > end:
        raise ValueError("开始日期不能晚于结束日期")
    return start, end


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        start, end = prompt_dates()
        urls = build_export_urls(start, end)
    except ValueError as exc:
        logging.error("%s", exc)
        return 1

    chrome_path = find_chrome_executable()
    if not chrome_path:
        logging.error("未找到 Google Chrome。请先安装 Chrome，或确认 chrome 命令可在终端中运行。")
        return 1

    first_run = not (CHROME_PROFILE_DIR_4 / "Default" / "Preferences").exists()
    prefs_path = configure_chrome_downloads(CHROME_PROFILE_DIR_4, EXPORT_DOWNLOAD_DIR_4)

    logging.info("Chrome: %s", chrome_path)
    logging.info("下载目录: %s", EXPORT_DOWNLOAD_DIR_4.resolve())
    logging.info("Chrome 独立配置目录: %s", CHROME_PROFILE_DIR_4.resolve())
    logging.info("下载配置已写入: %s", prefs_path)
    if first_run:
        logging.info("首次运行如跳转登录页，请在打开的 Chrome 窗口中登录；登录态会保存在独立配置目录中。")
        logging.info("如果登录后没有自动下载，请保持 Chrome 登录状态并重新运行代号4。")

    for day in iter_dates(start, end):
        logging.info("导出日期 %s: %s", day.strftime("%Y-%m-%d"), build_export_url(day))

    try:
        launch_chrome(chrome_path, CHROME_PROFILE_DIR_4, urls)
    except Exception:
        logging.error("启动 Chrome 失败", exc_info=True)
        return 1

    logging.info("已在 Chrome 中打开 %d 个导出链接。请到下载目录查看 Excel 文件。", len(urls))
    return 0
