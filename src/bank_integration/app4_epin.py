"""代号4 子功能 main() - 1epin.com 浏览器自动化数据提取。"""
import json
import logging
import random
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
    EPIN_ORDER_LOAD_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)

_EPIN_ORIGIN = "https://www.1epin.com/"
_EPIN_DETAIL_BASE = "https://www.1epin.com/siparis/"

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

_EXTRACT_PINS_JS = """
(function() {
  // 方式一：#pin_form table（多 pin 码标准结构）
  var table = null;
  var tables = document.querySelectorAll('#pin_form table');
  for (var i = 0; i < tables.length; i++) {
    var ths = tables[i].querySelectorAll('thead th');
    for (var j = 0; j < ths.length; j++) {
      if (ths[j].textContent.trim() === 'Pin') {
        table = tables[i];
        break;
      }
    }
    if (table) break;
  }
  if (table) {
    var rows = table.querySelectorAll('tbody tr');
    var result = [];
    rows.forEach(function(tr) {
      var tds = tr.querySelectorAll('td');
      var seq       = tds[0] ? tds[0].textContent.trim().replace(/\\.$/, '') : '';
      var cb        = tds[1] ? tds[1].querySelector('input[name="sec"]') : null;
      var pin_id    = cb ? cb.value : '';
      var pin       = tds[2] ? tds[2].textContent.trim() : '';
      var view_date = tds[3] ? tds[3].textContent.trim() : '';
      result.push({seq: seq, pin_id: pin_id, pin: pin, view_date: view_date});
    });
    return JSON.stringify(result);
  }
  // 方式二：#review_form textarea（锁定数量=1 独立结构）
  var textarea = document.querySelector('#review_form textarea');
  if (textarea) {
    var pinVal = (textarea.value || textarea.textContent || '').trim();
    if (pinVal) {
      return JSON.stringify([{seq: '1', pin_id: '', pin: pinVal, view_date: ''}]);
    }
  }
  return JSON.stringify([]);
})()
"""

_TR_MONTHS = {
    'Ocak': 1, 'Şubat': 2, 'Mart': 3, 'Nisan': 4,
    'Mayıs': 5, 'Haziran': 6, 'Temmuz': 7, 'Ağustos': 8,
    'Eylül': 9, 'Ekim': 10, 'Kasım': 11, 'Aralık': 12,
}

_DATE_COLS   = ('siparis_tarihi', 'onay_tarihi')
_AMOUNT_COLS = ('birim_fiyat', 'tutar', 'once', 'sonra')


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

_PIN_COLUMN_MAP = {
    'siparis_id': '订单ID',
    'siparis_no': '订单号',
    'seq':        '序号',
    'pin_id':     'Pin ID',
    'pin':        'Pin码',
    'view_date':  '查看时间',
}


def _is_show_more_visible(op: ChromeOperator) -> bool:
    return bool(op.evaluate(
        "!!(document.querySelector('#showMore') && "
        "document.querySelector('#showMore').offsetParent !== null)"
    ))


def _search_and_extract_by_pin(
    op: ChromeOperator,
    pin_code: str,
    click_interval_seconds: int = EPIN_ORDER_LOAD_INTERVAL_SECONDS,
) -> list:
    """通过搜索框输入 PIN 码，提取该 PIN 对应的（隐藏）订单列表。"""
    import json as _json

    try:
        op.wait_for_condition(
            "!!document.querySelector('input#pin')",
            timeout=10.0,
        )
    except TimeoutError:
        logger.warning("找不到搜索框（input#pin），跳过 PIN 搜索：%s", pin_code)
        return []

    js_fill_and_submit = (
        "(function(){"
        "var el=document.querySelector('input#pin');"
        "if(!el)return false;"
        f"el.value={_json.dumps(pin_code)};"
        "el.dispatchEvent(new Event('input',{bubbles:true}));"
        "var form=el.closest('form');"
        "if(form){form.submit();}else{"
        "el.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',keyCode:13,bubbles:true}));}"
        "return true;"
        "})()"
    )
    if not op.evaluate(js_fill_and_submit):
        logger.warning("填入搜索框失败，跳过 PIN 搜索：%s", pin_code)
        return []

    logger.info("已提交 PIN 搜索：%s，等待页面更新...", pin_code)
    time.sleep(1.5)
    try:
        op.wait_for_condition("!!document.querySelector('#myTable')", timeout=15.0)
    except TimeoutError:
        logger.warning("搜索后等待 #myTable 超时，PIN：%s", pin_code)
        return []

    _load_all_orders(op, click_interval_seconds=click_interval_seconds)
    orders = _extract_orders(op)
    logger.info("PIN '%s' 搜索提取 %d 条订单", pin_code, len(orders))
    return orders


def _load_all_orders(
    op: ChromeOperator,
    max_clicks: int = 200,
    click_interval_seconds: int = EPIN_ORDER_LOAD_INTERVAL_SECONDS,
) -> None:
    """反复点击"加载更多"按钮，直到按钮消失或行数不再增加。"""
    for i in range(max_clicks):
        if not _is_show_more_visible(op):
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
        if i < max_clicks - 1 and _is_show_more_visible(op):
            logger.info("订单列表加载间隔等待 %d 秒", click_interval_seconds)
            time.sleep(click_interval_seconds)
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
    for col in _AMOUNT_COLS:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: v.split()[0] if isinstance(v, str) and v.strip() else v
            )
    df = df.rename(columns=_COLUMN_MAP)
    df = df[[v for v in _COLUMN_MAP.values() if v in df.columns]]
    df = df.drop_duplicates(subset=['订单ID'], keep='first')

    filename = output_dir / f"epin_siparisler_{date.today():%Y%m%d}.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    return filename


class _TabOperator:
    """轻量 CDP 封装，管理单个 Chrome 标签页，供并行 Pin 码提取使用。"""

    def __init__(self, ws_url: str) -> None:
        import websocket as _ws_lib
        self._ws = _ws_lib.create_connection(ws_url.replace("://localhost:", "://127.0.0.1:"))
        self._msg_id = 0

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:
            pass

    def _send(self, method: str, params=None) -> dict:
        self._msg_id += 1
        msg = {"id": self._msg_id, "method": method, "params": params or {}}
        self._ws.send(json.dumps(msg))
        while True:
            resp = json.loads(self._ws.recv())
            if resp.get("id") == self._msg_id:
                if "error" in resp:
                    raise RuntimeError(f"CDP 错误 [{method}]: {resp['error']}")
                return resp

    def navigate(self, url: str) -> None:
        self._send("Page.navigate", {"url": url})

    def evaluate(self, expression: str):
        result = self._send("Runtime.evaluate", {"expression": expression, "returnByValue": True})
        return result.get("result", {}).get("result", {}).get("value")

    def wait_for_condition(self, js_condition: str, timeout: float = 10.0, poll: float = 0.5) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.evaluate(js_condition):
                return
            time.sleep(poll)
        raise TimeoutError(f"等待超时（{timeout}s）：{js_condition}")


def _create_chrome_tab(debug_port: int) -> tuple:
    """在已运行的 Chrome 中创建新空白标签页，返回 (tab_id, ws_url)。"""
    import urllib.request as _req
    req = _req.Request(f"http://127.0.0.1:{debug_port}/json/new", method="PUT")
    data = _req.urlopen(req, timeout=5).read()
    info = json.loads(data)
    return info["id"], info["webSocketDebuggerUrl"]


def _close_chrome_tab(debug_port: int, tab_id: str) -> None:
    """关闭指定 Chrome 标签页。"""
    import urllib.request as _req
    try:
        _req.urlopen(f"http://127.0.0.1:{debug_port}/json/close/{tab_id}", timeout=3)
    except Exception:
        pass


def _extract_pins_in_tab(debug_port: int, siparis_id: str, siparis_no: str) -> tuple:
    """在新建标签页中提取指定订单的 Pin 码，返回 (tab_id, pins)，标签页由调用方负责关闭。"""
    try:
        tab_id, ws_url = _create_chrome_tab(debug_port)
    except Exception:
        logger.warning("订单 %s 无法创建标签页，跳过", siparis_id, exc_info=True)
        return '', []

    op = None
    try:
        op = _TabOperator(ws_url)
        op.navigate(f"{_EPIN_DETAIL_BASE}{siparis_id}")
        op.wait_for_condition(
            "!!document.querySelector('#pin_form') || !!document.querySelector('#review_form')",
            timeout=15.0,
        )

        # 等待含"Pin"表头的 PIN 数据表格出现，或 #review_form textarea 有内容
        _PIN_TABLE_JS = (
            "(function(){"
            "var tables=document.querySelectorAll('#pin_form table');"
            "for(var i=0;i<tables.length;i++){"
            "var ths=tables[i].querySelectorAll('thead th');"
            "for(var j=0;j<ths.length;j++){if(ths[j].textContent.trim()==='Pin')return true;}"
            "};"
            "var ta=document.querySelector('#review_form textarea');"
            "return !!(ta&&(ta.value||ta.textContent||'').trim());"
            "})()"
        )
        try:
            op.wait_for_condition(_PIN_TABLE_JS, timeout=15.0, poll=0.5)
        except TimeoutError:
            logger.warning("订单 %s 等待 PIN 表格超时，尝试直接提取", siparis_id)

        raw = op.evaluate(_EXTRACT_PINS_JS)
        if not raw:
            return tab_id, []

        pin_rows = json.loads(raw)
        for row in pin_rows:
            row['siparis_id'] = siparis_id
            row['siparis_no'] = siparis_no
            if row.get('view_date'):
                row['view_date'] = _parse_tr_date(row['view_date'])
        return tab_id, pin_rows
    except Exception:
        logger.warning("订单 %s Pin 码提取失败，跳过", siparis_id, exc_info=True)
        return tab_id, []
    finally:
        if op:
            op.close()


def _fetch_all_pins_parallel(debug_port: int, orders: list, batch_size: int = 3) -> list:
    """分批并行提取所有订单的 Pin 码。
    批次大小默认3；批次内随机间隔0.2-1.0s逐个开启标签页；批次间固定等待20s。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_pins = []
    total = len(orders)
    prev_tab_ids: list = []

    for batch_start in range(0, total, batch_size):
        if batch_start > 0:
            logger.info("批次间隔等待 20 秒，可查看上一批标签页...")
            time.sleep(20)
            # 等待结束后再关闭上一批的标签页
            for tid in prev_tab_ids:
                _close_chrome_tab(debug_port, tid)
            prev_tab_ids = []

        batch = [o for o in orders[batch_start: batch_start + batch_size] if o.get('siparis_id')]
        if not batch:
            continue
        logger.info(
            "正在并行提取 Pin 码 第%d-%d单（共%d单）...",
            batch_start + 1, min(batch_start + batch_size, total), total,
        )

        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {}
            for order in batch:
                futures[executor.submit(
                    _extract_pins_in_tab,
                    debug_port,
                    order['siparis_id'],
                    order.get('siparis_no', ''),
                )] = order['siparis_id']
                # 批次内随机间隔（较短），最后一个不等待
                if order is not batch[-1]:
                    time.sleep(random.uniform(0.2, 1.0))

            batch_pins = []
            for future in as_completed(futures):
                siparis_id = futures[future]
                try:
                    tab_id, pins = future.result()
                    if tab_id:
                        prev_tab_ids.append(tab_id)
                    batch_pins.extend(pins)
                    all_pins.extend(pins)
                except Exception:
                    logger.warning("订单 %s Pin 码提取出现未处理异常", siparis_id, exc_info=True)

        logger.info("本批完成，本批提取 %d 个 Pin 码，累计 %d 个", len(batch_pins), len(all_pins))
        visible_pins = [p for p in batch_pins if not set(p.get('pin', '')).issubset({'*', ' ', ''})]
        if visible_pins:
            logger.info("本批可见 Pin 码明细（共 %d 个）：", len(visible_pins))
            for p in visible_pins:
                logger.info(
                    "  订单ID=%-8s  订单号=%-12s  序号=%-3s  Pin码=%s  查看时间=%s",
                    p.get('siparis_id', ''),
                    p.get('siparis_no', ''),
                    p.get('seq', ''),
                    p.get('pin', ''),
                    p.get('view_date', ''),
                )
        else:
            ids = [str(o['siparis_id']) for o in batch if o.get('siparis_id')]
            logger.info("本批无可见 Pin 码（全部已遮蔽），本批订单ID：%s", ', '.join(ids))

    # 关闭最后一批的标签页
    for tid in prev_tab_ids:
        _close_chrome_tab(debug_port, tid)

    logger.info("共提取 %d 个 Pin 码记录", len(all_pins))
    return all_pins


def _save_pins_excel(rows: list, output_dir: Path) -> Path:
    """将 Pin 码列表写入 Excel，返回输出文件路径。"""
    import pandas as pd
    from datetime import date

    df = pd.DataFrame(rows)
    df = df.rename(columns=_PIN_COLUMN_MAP)
    df = df[[v for v in _PIN_COLUMN_MAP.values() if v in df.columns]]
    filename = output_dir / f"epin_pinler_{date.today():%Y%m%d}.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    return filename


def _get_retry_locked_orders(siparisler_file: Path, pinler_file: Path) -> list:
    """返回需要补抓的订单列表，同时满足以下两个条件：
    条件一（来自 siparisler）：已锁定数量 == 1
    条件二（来自 pinler）  ：该订单 ID 在 pinler 中完全不存在，或存在但 Pin码 为空
    """
    import pandas as pd
    df_s = pd.read_excel(siparisler_file, engine='openpyxl', dtype=str).fillna('')
    locked = df_s[df_s['已锁定数量'].str.strip() == '1'][['订单ID', '订单号']].copy()
    if locked.empty:
        return []
    if pinler_file.exists():
        df_p = pd.read_excel(pinler_file, engine='openpyxl', dtype=str).fillna('')
        valid_ids = set(df_p[df_p['Pin码'].str.strip() != '']['订单ID'].str.strip())
        locked = locked[~locked['订单ID'].str.strip().isin(valid_ids)]
    return locked.rename(columns={'订单ID': 'siparis_id', '订单号': 'siparis_no'}).to_dict('records')


def _merge_pins_to_file(new_rows: list, pinler_file: Path) -> None:
    """将补抓的 pin 行合并到已有 epin_pinler 文件中（追加或替换空 pin 行）。"""
    import pandas as pd
    df_new = pd.DataFrame(new_rows).rename(columns=_PIN_COLUMN_MAP)
    df_new = df_new[[v for v in _PIN_COLUMN_MAP.values() if v in df_new.columns]]
    if pinler_file.exists():
        df_old = pd.read_excel(pinler_file, engine='openpyxl', dtype=str).fillna('')
        retry_ids = set(df_new['订单ID'].str.strip())
        df_old = df_old[~(
            df_old['订单ID'].str.strip().isin(retry_ids) &
            (df_old['Pin码'].str.strip() == '')
        )]
        df_merged = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_merged = df_new
    df_merged.to_excel(pinler_file, index=False, engine='openpyxl')


def main() -> int:
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="1epin.com 数据提取")
    parser.add_argument(
        '--mode',
        choices=['all', 'orders', 'pins', 'retry-locked'],
        default='all',
        help='all=订单+PIN（默认），orders=仅抓订单列表，pins=仅提取PIN码，retry-locked=仅补抓锁定数量=1的漏抓订单',
    )
    parser.add_argument(
        '--search-pin',
        action='append',
        dest='search_pins',
        default=[],
        metavar='PIN_CODE',
        help='通过搜索框输入 PIN 码提取隐藏订单，可多次指定',
    )
    args = parser.parse_args()
    # 提供了 --search-pin 时只做订单列表抓取，不自动触发 pinler 提取
    if args.search_pins and args.mode == 'all':
        args.mode = 'orders'
    only_orders   = (args.mode == 'orders')
    only_pins     = (args.mode == 'pins')
    retry_locked  = (args.mode == 'retry-locked')

    # 0. 准备订单数据
    from datetime import date as _date
    orders_file = OUTPUT_DIR_EPIN / f"epin_siparisler_{_date.today():%Y%m%d}.xlsx"
    pinler_file = OUTPUT_DIR_EPIN / f"epin_pinler_{_date.today():%Y%m%d}.xlsx"
    rows = []

    if only_pins:
        # pins 模式：必须有今日订单文件
        if not orders_file.exists():
            logger.error("--mode pins 需要今日订单文件，未找到：%s", orders_file)
            return 1
        import pandas as _pd
        _df = _pd.read_excel(orders_file, engine='openpyxl')
        rows = (
            _df[['订单ID', '订单号']]
            .rename(columns={'订单ID': 'siparis_id', '订单号': 'siparis_no'})
            .dropna(subset=['siparis_id'])
            .to_dict('records')
        )
        logger.info("仅提取 PIN 模式，已加载今日订单文件，共 %d 条记录", len(rows))
    elif retry_locked:
        if not orders_file.exists():
            logger.error("--mode retry-locked 需要今日订单文件，未找到：%s", orders_file)
            return 1
        rows = _get_retry_locked_orders(orders_file, pinler_file)
        if not rows:
            logger.info("没有需要补抓的订单（锁定数量=1 且无有效 pin 的订单为零）")
            return 0
        logger.info("retry-locked 模式：共 %d 个订单需要补抓", len(rows))
    else:
        has_existing_orders = orders_file.exists()
        if has_existing_orders:
            logger.info("检测到已有订单文件，将跳过订单列表抓取：%s", orders_file)
            import pandas as _pd
            _df = _pd.read_excel(orders_file, engine='openpyxl')
            rows = (
                _df[['订单ID', '订单号']]
                .rename(columns={'订单ID': 'siparis_id', '订单号': 'siparis_no'})
                .dropna(subset=['siparis_id'])
                .to_dict('records')
            )

    # 1. 查找 Chrome
    chrome_path = find_chrome_executable()
    if not chrome_path:
        logger.error("找不到 Google Chrome，请确认已安装。")
        return 1

    # 2. 创建目录
    CHROME_PROFILE_DIR_EPIN.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR_EPIN.mkdir(parents=True, exist_ok=True)

    # 3. 提前检查 Cookie
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
                time.sleep(1.5)
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

    # 6. Cookie 不存在时等待用户登录
    if not has_cookie:
        logger.info("当前独立 Chrome profile 还没有 Cookie 数据。")
        logger.info("请在打开的 Chrome 窗口中完成登录，登录后回到此终端按回车继续。")
        input("登录完成后按回车继续：")
        log_cookie_store_status(CHROME_PROFILE_DIR_EPIN)

    # 7. pins/retry-locked 模式不需要连接订单列表标签页，直接进入 PIN 提取
    op = None
    try:
        if not only_pins and not retry_locked:
            logger.info("正在通过 CDP 连接到 Chrome（端口 %d）...", CHROME_DEBUG_PORT_EPIN)
            try:
                op = ChromeOperator(CHROME_DEBUG_PORT_EPIN).connect(tab_url=_EPIN_ORIGIN)
            except Exception:
                logger.error("CDP 连接失败，请确认 Chrome 已启动并端口正确", exc_info=True)
                return 1

            if not rows:
                logger.info("导航到目标页面: %s", TARGET_URL_EPIN)
                op.navigate(TARGET_URL_EPIN)

                # 8. 等待订单表格渲染完成
                logger.info("等待订单表格加载...")
                op.wait_for_condition("!!document.querySelector('#myTable')", timeout=15.0)

                # 9. 若传入 PIN 码，先逐一搜索并收集隐藏订单
                hidden_orders: list = []
                seen_hidden_ids: set = set()
                for pin_code in args.search_pins:
                    logger.info("正在通过搜索提取隐藏订单，PIN：%s", pin_code)
                    for o in _search_and_extract_by_pin(
                        op, pin_code, EPIN_ORDER_LOAD_INTERVAL_SECONDS
                    ):
                        sid = o.get('siparis_id', '')
                        if sid and sid not in seen_hidden_ids:
                            seen_hidden_ids.add(sid)
                            hidden_orders.append(o)
                    logger.info("返回完整订单列表：%s", TARGET_URL_EPIN)
                    op.navigate(TARGET_URL_EPIN)
                    op.wait_for_condition("!!document.querySelector('#myTable')", timeout=15.0)
                if hidden_orders:
                    logger.info("PIN 搜索共收集 %d 条隐藏订单（去重后）", len(hidden_orders))

                # 10. 反复点击"加载更多"直到数据全部展示
                _load_all_orders(op, click_interval_seconds=EPIN_ORDER_LOAD_INTERVAL_SECONDS)

                # 11. 结构化提取全部订单
                rows = _extract_orders(op)

                # 12. 合并隐藏订单（只追加完整列表中不存在的）
                if hidden_orders:
                    regular_ids = {o.get('siparis_id', '') for o in rows}
                    new_hidden = [
                        o for o in hidden_orders
                        if o.get('siparis_id', '') not in regular_ids
                    ]
                    if new_hidden:
                        logger.info("合并 %d 条仅通过搜索可见的隐藏订单", len(new_hidden))
                        rows = rows + new_hidden

                if not rows:
                    logger.warning("未提取到任何订单数据，请确认页面已正确加载")
                    return 1

                # 13. 写入 Excel
                logger.info("共提取 %d 条订单记录，正在写入 Excel...", len(rows))
                output_file = _save_orders_excel(rows, OUTPUT_DIR_EPIN)
                logger.info("已输出：%s", output_file)
            elif args.search_pins:
                # 已有订单文件，仅执行 PIN 搜索补充隐藏订单
                logger.info("已有订单文件（%d 条），仅执行 PIN 搜索补充隐藏订单...", len(rows))
                op.navigate(TARGET_URL_EPIN)
                op.wait_for_condition("!!document.querySelector('#myTable')", timeout=15.0)

                # rows 只有 siparis_id/siparis_no 两列，仅用于去重判断
                existing_ids = {o.get('siparis_id', '') for o in rows}
                hidden_orders: list = []
                seen_hidden_ids: set = set()
                for pin_code in args.search_pins:
                    logger.info("正在通过搜索提取隐藏订单，PIN：%s", pin_code)
                    for o in _search_and_extract_by_pin(op, pin_code, EPIN_ORDER_LOAD_INTERVAL_SECONDS):
                        sid = o.get('siparis_id', '')
                        if sid and sid not in seen_hidden_ids:
                            seen_hidden_ids.add(sid)
                            hidden_orders.append(o)
                    logger.info("返回完整订单列表：%s", TARGET_URL_EPIN)
                    op.navigate(TARGET_URL_EPIN)
                    op.wait_for_condition("!!document.querySelector('#myTable')", timeout=15.0)

                new_hidden = [o for o in hidden_orders if o.get('siparis_id', '') not in existing_ids]
                if new_hidden:
                    import pandas as _pd
                    logger.info("合并 %d 条仅通过搜索可见的隐藏订单", len(new_hidden))
                    # 全列读取已有文件，避免覆盖原始数据
                    df_existing = _pd.read_excel(orders_file, engine='openpyxl', dtype=str).fillna('')
                    # 对新行做与 _save_orders_excel 相同的清洗和列名转换
                    df_new = _pd.DataFrame(new_hidden)
                    for col in _DATE_COLS:
                        if col in df_new.columns:
                            df_new[col] = df_new[col].apply(_parse_tr_date)
                    for col in _AMOUNT_COLS:
                        if col in df_new.columns:
                            df_new[col] = df_new[col].apply(
                                lambda v: v.split()[0] if isinstance(v, str) and v.strip() else v
                            )
                    df_new = df_new.rename(columns=_COLUMN_MAP)
                    df_new = df_new[[v for v in _COLUMN_MAP.values() if v in df_new.columns]]
                    df_merged = _pd.concat([df_existing, df_new], ignore_index=True)
                    df_merged = df_merged.drop_duplicates(subset=['订单ID'], keep='first')
                    df_merged.to_excel(orders_file, index=False, engine='openpyxl')
                    logger.info("已更新：%s", orders_file)
                else:
                    logger.info("PIN 搜索未发现新的隐藏订单")
            else:
                logger.info("使用已有订单文件，共 %d 条记录", len(rows))

        # 12. 并行提取各订单 Pin 码并写入 Excel
        if not only_orders:
            logger.info("正在并行提取 Pin 码（共 %d 单，每批3个）...", len(rows))
            pin_rows = _fetch_all_pins_parallel(CHROME_DEBUG_PORT_EPIN, rows)
            if pin_rows:
                if retry_locked:
                    _merge_pins_to_file(pin_rows, pinler_file)
                    logger.info("已合并补抓 Pin 码到：%s", pinler_file)
                else:
                    pin_file = _save_pins_excel(pin_rows, OUTPUT_DIR_EPIN)
                    logger.info("Pin 码已输出：%s", pin_file)
            else:
                logger.warning("未提取到任何 Pin 码数据")
        else:
            logger.info("--mode orders：跳过 PIN 提取")

    finally:
        if op:
            op.disconnect()

    return 0
