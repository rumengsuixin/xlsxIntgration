"""代号4 子功能 main() - 1epin.com 浏览器自动化数据提取。"""
import json
import logging
import subprocess
import time
from pathlib import Path

from .app4 import (
    find_chrome_executable,
    build_chrome_args,
    has_chrome_cookie_store,
    log_cookie_store_status,
)
from .browser_operator import (
    ChromeOperator,
    is_chrome_running,
    get_chrome_pages,
    open_new_tab,
)
from .config4_epin import (
    TARGET_URL_EPIN,
    CHROME_PROFILE_DIR_EPIN,
    OUTPUT_DIR_EPIN,
    CHROME_DEBUG_PORT_EPIN,
)

logger = logging.getLogger(__name__)

_EPIN_ORIGIN = "https://www.1epin.com/"

_EXTRACT_JS = """
(function() {
  function getFieldValue(td, label) {
    var children = Array.from(td.children);
    for (var i = 0; i < children.length; i++) {
      var div = children[i];
      var st = div.querySelector('strong.alan-etiketi');
      if (st && st.textContent.trim() === label) {
        return div.textContent.replace(st.textContent, '').trim();
      }
    }
    return '';
  }

  function getSpanTitle(td, label) {
    var children = Array.from(td.children);
    for (var i = 0; i < children.length; i++) {
      var div = children[i];
      var st = div.querySelector('strong.alan-etiketi');
      if (st && st.textContent.trim() === label) {
        var sp = div.querySelector('span[title]');
        return sp ? sp.getAttribute('title') : '';
      }
    }
    return '';
  }

  function getStatusText(td) {
    var children = Array.from(td.children);
    for (var i = 0; i < children.length; i++) {
      var div = children[i];
      var st = div.querySelector('strong.alan-etiketi');
      if (st && st.textContent.trim() === 'Sipariş Durumu:') {
        var sp = div.querySelector('span');
        return sp ? sp.textContent.trim() : '';
      }
    }
    return '';
  }

  function getOrderNo(td) {
    var children = Array.from(td.children);
    for (var i = 0; i < children.length; i++) {
      var div = children[i];
      var st = div.querySelector('strong.alan-etiketi');
      if (st && st.textContent.trim() === 'Sipariş No:') {
        var a = div.querySelector('a');
        if (a) return a.textContent.trim().replace(/^\\s*#/, '');
        return div.textContent.replace(st.textContent, '').trim().replace(/^\\s*#/, '');
      }
    }
    return '';
  }

  var rows = document.querySelectorAll('#myTable tbody tr');
  var result = [];

  rows.forEach(function(tr) {
    var tds = tr.querySelectorAll('td');
    var td1 = tds[1] || null;
    var td2 = tds[2] || null;
    var td3 = tds[3] || null;
    var td4 = tds[4] || null;
    var td5 = tds[5] || null;

    var noteDiv = td1 ? td1.querySelector('div[style*="3c9b49"]') : null;
    var greenSpan = td5 ? td5.querySelector('span[style*="7CFC00"]') : null;
    var yellowSpan = td5 ? td5.querySelector('span[style*="FFD700"]') : null;

    result.push({
      siparis_id:     tr.getAttribute('data-id') || '',
      siparis_durumu: td1 ? getStatusText(td1) : '',
      siparis_tarihi: td1 ? getFieldValue(td1, 'Sipariş Tarihi:') : '',
      onay_tarihi:    td1 ? getFieldValue(td1, 'Onay Tarihi:') : '',
      siparis_no:     td1 ? getOrderNo(td1) : '',
      not_alani:      noteDiv ? noteDiv.textContent.trim() : '',
      kategori:       td2 ? getFieldValue(td2, 'Kategori:') : '',
      urun:           td2 ? getFieldValue(td2, 'Ürün:') : '',
      adet:           td3 ? getFieldValue(td3, 'Adet:') : '',
      birim_fiyat:    td3 ? getSpanTitle(td3, 'Birim Fiyat:') : '',
      tutar:          td3 ? getSpanTitle(td3, 'Tutar:') : '',
      olusturan:      td3 ? getFieldValue(td3, 'Oluşturan:') : '',
      once:           td4 ? getSpanTitle(td4, 'Önce:') : '',
      sonra:          td4 ? getSpanTitle(td4, 'Sonra:') : '',
      acik_adet:      greenSpan ? greenSpan.textContent.trim() : '',
      kilitli_adet:   yellowSpan ? yellowSpan.textContent.trim() : '',
    });
  });

  return JSON.stringify(result);
})()
"""

_TR_MONTHS = {
    'Ocak': 1, 'Şubat': 2, 'Mart': 3, 'Nisan': 4,
    'Mayıs': 5, 'Haziran': 6, 'Temmuz': 7, 'Ağustos': 8,
    'Eylül': 9, 'Ekim': 10, 'Kasım': 11, 'Aralık': 12,
}

_DATE_COLS = ('siparis_tarihi', 'onay_tarihi')


def _parse_tr_date(s: str) -> str:
    """将 '18 Mayıs 2026 19:15' 转换为 '2026-05-18 19:15'，失败时原样返回。"""
    if not s:
        return s
    try:
        parts = s.split()
        day = int(parts[0])
        month = _TR_MONTHS.get(parts[1], 0)
        year = int(parts[2])
        time_part = parts[3] if len(parts) > 3 else ''
        if month == 0:
            return s
        base = f"{year:04d}-{month:02d}-{day:02d}"
        return f"{base} {time_part}" if time_part else base
    except Exception:
        return s


_COLUMN_MAP = {
    'siparis_id':     '订单ID',
    'siparis_durumu': '订单状态',
    'siparis_tarihi': '下单时间',
    'onay_tarihi':    '确认时间',
    'siparis_no':     '订单号',
    'not_alani':      '备注',
    'kategori':       '类别',
    'urun':           '产品',
    'adet':           '数量',
    'birim_fiyat':    '单价(USD)',
    'tutar':          '金额(USD)',
    'olusturan':      '创建者',
    'once':           '交易前余额(USD)',
    'sonra':          '交易后余额(USD)',
    'acik_adet':      '已解锁数量',
    'kilitli_adet':   '已锁定数量',
}


def _load_all_orders(op: ChromeOperator, max_clicks: int = 200) -> None:
    """反复点击"加载更多"按钮，直到按钮消失或行数不再增加。"""
    for i in range(max_clicks):
        visible = op.evaluate(
            "!!(document.querySelector('#showMore') && "
            "document.querySelector('#showMore').offsetParent !== null)"
        )
        if not visible:
            logger.info("'加载更多'按钮不可见，数据已全部加载")
            break

        before = op.evaluate("document.querySelectorAll('#myTable tbody tr').length") or 0
        op.click("#showMore")
        logger.info("第 %d 次点击加载更多，当前 %d 行...", i + 1, before)

        try:
            op.wait_for_condition(
                f"document.querySelectorAll('#myTable tbody tr').length > {before}",
                timeout=10.0,
                poll=0.5,
            )
        except TimeoutError:
            logger.info("等待超时，检查按钮状态...")

        after = op.evaluate("document.querySelectorAll('#myTable tbody tr').length") or 0
        if after <= before:
            logger.info("行数未增加（%d → %d），数据已全部加载", before, after)
            break
    else:
        logger.warning("已达最大点击次数 %d，可能仍有未加载数据", max_clicks)

    total = op.evaluate("document.querySelectorAll('#myTable tbody tr').length") or 0
    logger.info("共加载 %d 条订单记录", total)


def _extract_orders(op: ChromeOperator) -> list:
    """通过 CDP JS 一次性提取 #myTable 中所有行，返回字典列表。"""
    raw = op.evaluate(_EXTRACT_JS)
    if not raw:
        return []
    return json.loads(raw)


def _save_orders_excel(rows: list, output_dir: Path) -> Path:
    """将订单列表写入 Excel，返回输出文件路径。"""
    import pandas as pd
    from datetime import date

    df = pd.DataFrame(rows)
    for col in _DATE_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_parse_tr_date)
    df = df.rename(columns=_COLUMN_MAP)
    df = df[[v for v in _COLUMN_MAP.values() if v in df.columns]]

    filename = output_dir / f"epin_siparisler_{date.today():%Y%m%d}.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    return filename


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # 1. 查找 Chrome
    chrome_path = find_chrome_executable()
    if not chrome_path:
        logger.error("找不到 Google Chrome，请确认已安装。")
        return 1

    # 2. 创建目录
    CHROME_PROFILE_DIR_EPIN.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR_EPIN.mkdir(parents=True, exist_ok=True)

    # 3. 提前检查 Cookie（后续无论哪条路径都需要判断是否要等待登录）
    has_cookie = has_chrome_cookie_store(CHROME_PROFILE_DIR_EPIN)

    # 4. 检测 Chrome 是否已运行，并定位或打开目标标签页
    chrome_ready = False
    if is_chrome_running(CHROME_DEBUG_PORT_EPIN):
        logger.info("检测到 Chrome 已在端口 %d 运行", CHROME_DEBUG_PORT_EPIN)
        try:
            pages = get_chrome_pages(CHROME_DEBUG_PORT_EPIN)
        except Exception:
            pages = []

        logger.debug(
            "Chrome 当前标签页: %s",
            [(p.get("type", "?"), p.get("url", "")) for p in pages],
        )
        target_pages = [p for p in pages if p.get("url", "").startswith(_EPIN_ORIGIN)]
        if target_pages:
            logger.info("找到目标网站标签页，直接使用: %s", target_pages[0].get("url"))
            chrome_ready = True
        else:
            logger.info("未找到目标网站标签页，正在通过 CDP 打开新标签页...")
            try:
                open_new_tab(CHROME_DEBUG_PORT_EPIN, TARGET_URL_EPIN)
                time.sleep(1.5)  # 等待新标签页加载
                chrome_ready = True
            except Exception:
                logger.warning("CDP 打开新标签页失败，将重新启动 Chrome", exc_info=True)

    # 5. Chrome 未运行时，正常启动
    if not chrome_ready:
        logger.info("正在启动 Chrome，目标页面: %s", TARGET_URL_EPIN)
        try:
            subprocess.Popen(
                build_chrome_args(
                    chrome_path, CHROME_PROFILE_DIR_EPIN, [TARGET_URL_EPIN], CHROME_DEBUG_PORT_EPIN
                )
            )
        except Exception:
            logger.error("启动 Chrome 失败", exc_info=True)
            return 1

    # 6. 无论哪条路径，Cookie 不存在时都需要等待用户登录
    if not has_cookie:
        logger.info("当前独立 Chrome profile 还没有 Cookie 数据。")
        logger.info("请在打开的 Chrome 窗口中完成登录，登录后回到此终端按回车继续。")
        input("登录完成后按回车继续：")
        log_cookie_store_status(CHROME_PROFILE_DIR_EPIN)

    # 7. CDP 连接到目标标签页
    logger.info("正在通过 CDP 连接到 Chrome（端口 %d）...", CHROME_DEBUG_PORT_EPIN)
    try:
        op = ChromeOperator(CHROME_DEBUG_PORT_EPIN).connect(tab_url=_EPIN_ORIGIN)
    except Exception:
        logger.error("CDP 连接失败，请确认 Chrome 已启动并端口正确", exc_info=True)
        return 1

    try:
        logger.info("导航到目标页面: %s", TARGET_URL_EPIN)
        op.navigate(TARGET_URL_EPIN)

        # 8. 等待订单表格渲染完成
        logger.info("等待订单表格加载...")
        op.wait_for_condition("!!document.querySelector('#myTable')", timeout=15.0)

        # 9. 反复点击"加载更多"直到数据全部展示
        _load_all_orders(op)

        # 10. 结构化提取全部订单
        rows = _extract_orders(op)
        if not rows:
            logger.warning("未提取到任何订单数据，请确认页面已正确加载")
            return 1

        # 11. 写入 Excel
        logger.info("共提取 %d 条订单记录，正在写入 Excel...", len(rows))
        output_file = _save_orders_excel(rows, OUTPUT_DIR_EPIN)
        logger.info("已输出：%s", output_file)

    finally:
        op.disconnect()

    return 0
