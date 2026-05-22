"""代号4 子功能 main() - BC 平台（betcatpay）代收订单抓取。"""
import json
import logging
import subprocess
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List

from .app4 import (
    find_chrome_executable,
    build_chrome_args,
    has_chrome_cookie_store,
    log_cookie_store_status,
    parse_date_args,
    PROFILE_NAME,
)
from .browser_operator import (
    ChromeOperator,
    is_chrome_running,
    get_chrome_pages,
    open_new_tab,
)
from .config4_bc import (
    BC_REPORT_URL_TEMPLATE,
    BC_CHROME_PROFILE_DIR,
    BC_OUTPUT_DIR,
    BC_EXTRACT_DIR,
    BC_ZIP_FILENAME_PREFIX,
    CHROME_DEBUG_PORT_BC,
)

logger = logging.getLogger(__name__)

_BC_ORIGIN = "https://ajv23m50.m.betcatpay.com/"
_CST = timezone(timedelta(hours=8))


# ---------------------------------------------------------------------------
# URL 构建
# ---------------------------------------------------------------------------

def build_bc_report_url(start: date, end: date, page: int = 1) -> str:
    """日期范围 + 页码 → BC 平台 URL（start/end 均转 CST 零点毫秒时间戳）。"""
    start_dt = datetime(start.year, start.month, start.day, tzinfo=_CST)
    # end_time 取 end 当天结束（次日零点），区间左闭右开
    end_dt = datetime(end.year, end.month, end.day, tzinfo=_CST) + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    return BC_REPORT_URL_TEMPLATE.format(
        start_time=start_ms,
        end_time=end_ms,
        page=page,
    )


# ---------------------------------------------------------------------------
# Chrome profile 配置
# ---------------------------------------------------------------------------

def _configure_bc_profile(profile_dir: Path, output_dir: Path) -> None:
    """在 Chrome 启动前更新 Preferences，将 BC 域名写入弹窗和自动下载白名单。

    window.open() 触发的文件下载在未授权域名下会被 Chrome 拦截；
    将域名加入 content_settings.exceptions.popups / automatic_downloads 后，
    即使是 JS 不可信事件触发的下载也不再弹出拦截提示。
    """
    prefs_path = profile_dir / PROFILE_NAME / "Preferences"
    prefs_path.parent.mkdir(parents=True, exist_ok=True)

    prefs: dict = {}
    if prefs_path.exists():
        try:
            with prefs_path.open("r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            if isinstance(loaded, dict):
                prefs = loaded
        except (json.JSONDecodeError, OSError):
            logger.warning("Chrome Preferences 读取失败，将重新写入: %s", prefs_path)

    # 下载目录
    dl = prefs.setdefault("download", {})
    dl["default_directory"] = str(output_dir.resolve())
    dl["prompt_for_download"] = False
    dl["directory_upgrade"] = True

    # BC 域名弹窗 + 自动下载白名单（setting=1 表示 ALLOW）
    # Chrome content settings key 格式："{scheme}://{host}:{port},*"
    bc_key = f"{_BC_ORIGIN.rstrip('/')}:443,*"
    exceptions = (
        prefs
        .setdefault("profile", {})
        .setdefault("content_settings", {})
        .setdefault("exceptions", {})
    )
    exceptions.setdefault("popups", {})[bc_key] = {"setting": 1}
    exceptions.setdefault("automatic_downloads", {})[bc_key] = {"setting": 1}

    with prefs_path.open("w", encoding="utf-8") as fh:
        json.dump(prefs, fh, ensure_ascii=False, indent=2)
    logger.info("Chrome Preferences 已更新：BC 域名弹窗/自动下载白名单 → %s", bc_key)


# ---------------------------------------------------------------------------
# 登录保障
# ---------------------------------------------------------------------------

def ensure_bc_login(chrome_path: Path, start: date, end: date) -> None:
    """打开第1页报表 URL（站内未登录时自动跳转登录页），等待用户登录后按回车继续。"""
    first_url = build_bc_report_url(start, end, page=1)
    has_cookie = has_chrome_cookie_store(BC_CHROME_PROFILE_DIR)

    chrome_ready = False
    if is_chrome_running(CHROME_DEBUG_PORT_BC):
        logger.info("检测到 BC 平台 Chrome 已在端口 %d 运行", CHROME_DEBUG_PORT_BC)
        try:
            pages = get_chrome_pages(CHROME_DEBUG_PORT_BC)
        except Exception:
            pages = []
        target_pages = [p for p in pages if p.get("url", "").startswith(_BC_ORIGIN)]
        if target_pages:
            logger.info("找到目标标签页，直接复用: %s", target_pages[0].get("url"))
            chrome_ready = True
        else:
            try:
                open_new_tab(CHROME_DEBUG_PORT_BC, first_url)
                chrome_ready = True
            except Exception:
                logger.warning("CDP 打开新标签页失败，将重新启动 Chrome", exc_info=True)

    if not chrome_ready:
        logger.info("正在启动 Chrome，目标: %s", first_url)
        try:
            subprocess.Popen(
                build_chrome_args(
                    chrome_path, BC_CHROME_PROFILE_DIR, [first_url], CHROME_DEBUG_PORT_BC
                )
            )
        except Exception:
            logger.error("启动 Chrome 失败", exc_info=True)
            raise

    if not has_cookie:
        logger.info("当前 BC 平台独立 Chrome profile 还没有 Cookie 数据。")
        logger.info("请在打开的 Chrome 窗口中完成登录，登录后回到此终端按回车继续。")
        input("登录完成后按回车继续：")
        log_cookie_store_status(BC_CHROME_PROFILE_DIR)


# ---------------------------------------------------------------------------
# 分页采集
# ---------------------------------------------------------------------------

def _load_page_dates(operator: ChromeOperator, url: str) -> List[date]:
    """导航至 url，等待表格渲染后返回本页所有行的日期列表。空列表表示无数据。"""
    operator.navigate(url)
    try:
        operator.wait_for_condition(
            "document.querySelectorAll('.el-table__body-wrapper tr.el-table__row').length > 0",
            timeout=15.0,
        )
    except TimeoutError:
        logger.info("页面无表格行，视为无数据: %s", url)
        return []

    time.sleep(0.5)

    dates_json = operator.evaluate(
        "(function(){"
        "var rows=document.querySelectorAll('.el-table__body-wrapper tr.el-table__row');"
        "var dates=[];"
        "for(var i=0;i<rows.length;i++){"
        "var cell=rows[i].querySelector('td.el-table_2_column_6 .cell');"
        "dates.push(cell?cell.textContent.trim():'');"
        "}"
        "return JSON.stringify(dates);"
        "})()"
    )
    date_strs: List[str] = json.loads(dates_json) if dates_json else []
    result: List[date] = []
    for s in date_strs:
        if not s:
            continue
        try:
            result.append(datetime.strptime(s, "%Y-%m-%d").date())
        except ValueError:
            logger.warning("无法解析日期字符串: %r，跳过", s)
    return result


def _click_row_button(operator: ChromeOperator, row_index: int) -> None:
    """点击第 row_index 行的「创建时间」导出按钮。

    依赖 Chrome profile 已将 BC 域名加入弹窗白名单，
    使 JS .click() 触发的 window.open 下载不被拦截。
    """
    clicked = operator.evaluate(
        f"(function(){{"
        f"var rows=document.querySelectorAll('.el-table__body-wrapper tr.el-table__row');"
        f"var row=rows[{row_index}];"
        f"if(!row)return false;"
        f"var btns=row.querySelectorAll('.cell .el-button');"
        f"for(var i=0;i<btns.length;i++){{"
        f"if(btns[i].textContent.trim()==='创建时间'){{btns[i].click();return true;}}"
        f"}}"
        f"return false;"
        f"}})()"
    )
    if not clicked:
        logger.warning("第 %d 行未找到「创建时间」导出按钮，跳过", row_index)


def _iter_completed_zips(
    download_dir: Path,
    expected_dates: List[date],
    timeout: float = 120.0,
):
    """逐一 yield 已完成下载的 ZIP 路径，直到所有日期就绪或超时。"""
    pending = list(expected_dates)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        still_pending = []
        for d in pending:
            zips = list(download_dir.glob(f"{BC_ZIP_FILENAME_PREFIX}{d:%Y%m%d}_*.zip"))
            in_progress = list(download_dir.glob(f"{BC_ZIP_FILENAME_PREFIX}{d:%Y%m%d}_*.crdownload"))
            if zips and not in_progress:
                for z in zips:
                    logger.info("ZIP 下载完成: %s", z.name)
                    yield z
            else:
                still_pending.append(d)
        pending = still_pending
        if pending:
            time.sleep(0.5)
    if pending:
        raise TimeoutError(
            f"等待 ZIP 超时（{timeout}s），未完成日期数：{len(pending)}"
        )


def extract_page_zips(zip_paths: List[Path], extract_dir: Path) -> List[Path]:
    """将 ZIP 列表解压到 extract_dir，返回解压出的文件路径列表。"""
    extracted: List[Path] = []
    for zip_path in zip_paths:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            zf.extractall(extract_dir)
            extracted.extend(extract_dir / name for name in names)
        logger.info("已解压 %s → %d 个文件", zip_path.name, len(names))
    return extracted


def collect_all_pages(
    chrome_path: Path,
    start: date,
    end: date,
    download_dir: Path,
    extract_dir: Path,
) -> List[Path]:
    """分页遍历：逐行串行执行 点击→等待ZIP→解压，返回所有解压文件路径。"""
    logger.info("正在连接 Chrome CDP（路径: %s，端口 %d）...", chrome_path, CHROME_DEBUG_PORT_BC)
    operator = ChromeOperator(CHROME_DEBUG_PORT_BC).connect(tab_url=_BC_ORIGIN)

    all_extracted: List[Path] = []
    page = 1

    try:
        while True:
            url = build_bc_report_url(start, end, page)
            logger.info("第 %d 页: %s", page, url)

            dates = _load_page_dates(operator, url)
            if not dates:
                logger.info("第 %d 页无数据，分页结束", page)
                break

            logger.info("第 %d 页找到 %d 条记录，开始串行点击下载...", page, len(dates))
            for i, d in enumerate(dates):
                logger.info("点击第 %d 行（%s）导出按钮", i, d)
                _click_row_button(operator, i)
                for zip_path in _iter_completed_zips(download_dir, [d]):
                    all_extracted.extend(extract_page_zips([zip_path], extract_dir))

            page += 1
    finally:
        operator.disconnect()

    return all_extracted


# ---------------------------------------------------------------------------
# 结果合并（骨架，细节 TODO）
# ---------------------------------------------------------------------------

def merge_extracted_files(extracted_files: List[Path], output_path: Path) -> None:
    """合并解压后的所有文件为单个 xlsx。

    TODO:
      1. 确认解压出的文件格式（CSV / XLSX）和列结构
      2. pd.read_csv / pd.read_excel 逐文件读取
      3. pd.concat 后写入 output_path
    """
    logger.info("待合并 %d 个文件 → %s", len(extracted_files), output_path)
    raise NotImplementedError("merge_extracted_files 尚未实现，需确认解压文件的列结构")


# ---------------------------------------------------------------------------
# 输出路径
# ---------------------------------------------------------------------------

def _build_output_path(start: date, end: date) -> Path:
    return BC_OUTPUT_DIR / f"bc_代收订单_{start:%Y%m%d}_{end:%Y%m%d}.xlsx"


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # 1. 解析日期参数（默认上月全月）
    try:
        start, end, _ = parse_date_args(argv)
    except SystemExit:
        return 1

    logger.info("BC 平台代收订单抓取，日期范围：%s ~ %s", start, end)

    # 2. 准备目录并配置 Chrome profile（须在启动 Chrome 前完成）
    BC_CHROME_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    BC_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    BC_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    _configure_bc_profile(BC_CHROME_PROFILE_DIR, BC_OUTPUT_DIR)

    # 3. 查找 Chrome
    chrome_path = find_chrome_executable()
    if not chrome_path:
        logger.error("找不到 Google Chrome，请确认已安装。")
        return 1

    # 4. 登录保障（无 Cookie 时等待用户手动登录）
    try:
        ensure_bc_login(chrome_path, start, end)
    except Exception:
        logger.error("Chrome 启动或登录流程失败", exc_info=True)
        return 1

    # 5. 分页采集：逐页点击 button → 等待 ZIP → 解压
    try:
        extracted_files = collect_all_pages(
            chrome_path, start, end, BC_OUTPUT_DIR, BC_EXTRACT_DIR
        )
    except NotImplementedError as e:
        logger.warning("采集流程中遇到未实现步骤: %s", e)
        return 1
    except Exception:
        logger.error("采集过程中发生错误", exc_info=True)
        return 1

    # 6. 合并解压文件为单个 xlsx
    if not extracted_files:
        logger.warning("无解压文件，跳过合并输出")
        return 0

    output_path = _build_output_path(start, end)
    try:
        merge_extracted_files(extracted_files, output_path)
        logger.info("输出文件：%s", output_path)
    except NotImplementedError as e:
        logger.warning("合并步骤尚未实现: %s", e)
        return 1

    return 0
