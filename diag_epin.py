"""
诊断脚本：连接已运行的 1epin Chrome，打开指定订单详情页，
完整输出 #pin_form 区域的 DOM 结构，帮助定位 PIN 码提取失败原因。

用法：
    venv/Scripts/python.exe diag_epin.py [订单ID]
    venv/Scripts/python.exe diag_epin.py 1430646
"""
import json
import sys
import time
import urllib.request

# ── 配置 ──────────────────────────────────────────────────────────────
DEBUG_PORT  = 9225
SIPARIS_ID  = sys.argv[1] if len(sys.argv) > 1 else "1430646"
TARGET_URL  = f"https://www.1epin.com/siparis/{SIPARIS_ID}"
WAIT_LOAD   = 20   # 等待页面加载的最大秒数
# ──────────────────────────────────────────────────────────────────────


def cdp_new_tab(port):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/json/new", method="PUT")
    data = urllib.request.urlopen(req, timeout=5).read()
    info = json.loads(data)
    return info["id"], info["webSocketDebuggerUrl"]


def cdp_close_tab(port, tab_id):
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/close/{tab_id}", timeout=3)
    except Exception:
        pass


class Tab:
    def __init__(self, ws_url):
        import websocket as _ws
        self._ws = _ws.create_connection(ws_url.replace("://localhost:", "://127.0.0.1:"))
        self._id = 0

    def close(self):
        try: self._ws.close()
        except Exception: pass

    def _send(self, method, params=None):
        self._id += 1
        self._ws.send(json.dumps({"id": self._id, "method": method, "params": params or {}}))
        while True:
            resp = json.loads(self._ws.recv())
            if resp.get("id") == self._id:
                if "error" in resp:
                    raise RuntimeError(f"CDP 错误: {resp['error']}")
                return resp

    def navigate(self, url):
        self._send("Page.navigate", {"url": url})

    def js(self, expr):
        r = self._send("Runtime.evaluate", {"expression": expr, "returnByValue": True})
        return r.get("result", {}).get("result", {}).get("value")

    def wait(self, condition, timeout=20.0, poll=0.5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.js(condition):
                return True
            time.sleep(poll)
        return False


def sep(title=""):
    print("\n" + "=" * 70)
    if title:
        print(f"  {title}")
        print("=" * 70)


def main():
    # 检查 Chrome 是否可连接
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{DEBUG_PORT}/json", timeout=3)
    except Exception:
        print(f"[错误] 无法连接 Chrome 调试端口 {DEBUG_PORT}，请先运行 整合4.py 让 Chrome 启动。")
        sys.exit(1)

    print(f"正在打开新标签页: {TARGET_URL}")
    tab_id, ws_url = cdp_new_tab(DEBUG_PORT)
    tab = Tab(ws_url)

    try:
        tab.navigate(TARGET_URL)

        sep("等待页面加载 (#pin_form)")
        found = tab.wait("!!document.querySelector('#pin_form')", timeout=WAIT_LOAD)
        if not found:
            print("[超时] #pin_form 未出现，页面可能未登录或结构不同。")
            return

        print(f"#pin_form 已出现，再等待 3 秒让 AJAX 完成...")
        time.sleep(3)

        # ── 1. #pin_form 完整 innerHTML（前 3000 字符）────────────────
        sep("① #pin_form innerHTML（前 3000 字符）")
        form_html = tab.js(
            "(function(){"
            "var f=document.querySelector('#pin_form');"
            "return f?f.innerHTML.substring(0,3000):'NOT FOUND';"
            "})()"
        )
        print(form_html)

        # ── 2. PIN 表格内所有行的各列 textContent ─────────────────────
        sep("② PIN 表格各行 td[0~3] textContent")
        rows_info = tab.js(
            "(function(){"
            "var tables=document.querySelectorAll('#pin_form table');"
            "var table=null;"
            "for(var i=0;i<tables.length;i++){if(tables[i].querySelector('input[name=\"sec\"]')){table=tables[i];break;}}"
            "if(!table)return JSON.stringify({err:'no pin table'});"
            "var rows=Array.from(table.querySelectorAll('tbody tr')).map(function(tr){"
            "var tds=tr.querySelectorAll('td');"
            "return{"
            "td0:tds[0]?tds[0].textContent.trim():'',"
            "td1_inputVal:tds[1]&&tds[1].querySelector('input')?tds[1].querySelector('input').value:'',"
            "td2_text:tds[2]?tds[2].textContent.trim():'',"
            "td2_title:tds[2]&&tds[2].querySelector('[title]')?tds[2].querySelector('[title]').getAttribute('title'):'',"
            "td2_dataAttrs:tds[2]?Object.fromEntries(Array.from(tds[2].querySelectorAll('[data-\\\\w]')).concat([tds[2]]).map(function(el){return Array.from(el.attributes).filter(function(a){return a.name.startsWith('data-');}).map(function(a){return[a.name,a.value];});}).flat()):{},"
            "td3:tds[3]?tds[3].textContent.trim():''"
            "};"
            "});"
            "return JSON.stringify(rows,null,2);"
            "})()"
        )
        print(rows_info)

        # ── 3. 第一行 td[2] 完整 innerHTML ───────────────────────────
        sep("③ 第一行 td[2]（PIN 列）完整 innerHTML")
        td2_html = tab.js(
            "(function(){"
            "var table=null;"
            "var tables=document.querySelectorAll('#pin_form table');"
            "for(var i=0;i<tables.length;i++){if(tables[i].querySelector('input[name=\"sec\"]')){table=tables[i];break;}}"
            "if(!table)return 'no table';"
            "var tr=table.querySelector('tbody tr');"
            "if(!tr)return 'no row';"
            "var td=tr.querySelectorAll('td')[2];"
            "return td?td.outerHTML:'no td';"
            "})()"
        )
        print(td2_html)

        # ── 4. #pin_form 内所有可交互元素 ────────────────────────────
        sep("④ #pin_form 内所有可交互元素（button / a / input）")
        buttons = tab.js(
            "(function(){"
            "var form=document.querySelector('#pin_form');"
            "if(!form)return '[]';"
            "var els=Array.from(form.querySelectorAll('button,a,input[type=button],input[type=submit],input[type=image]'));"
            "return JSON.stringify(els.map(function(e){"
            "return{tag:e.tagName,id:e.id,className:e.className,href:e.href||'',text:e.textContent.trim().substring(0,80),onclick:e.getAttribute('onclick')||''};"
            "}),null,2);"
            "})()"
        )
        print(buttons)

        # ── 5. 等 10 秒后再次检查 td[2] textContent（看是否自动变化）─
        sep("⑤ 再等 10 秒，再次读取第一行 td[2] textContent")
        time.sleep(10)
        td2_after = tab.js(
            "(function(){"
            "var table=null;"
            "var tables=document.querySelectorAll('#pin_form table');"
            "for(var i=0;i<tables.length;i++){if(tables[i].querySelector('input[name=\"sec\"]')){table=tables[i];break;}}"
            "if(!table)return 'no table';"
            "var tr=table.querySelector('tbody tr');"
            "if(!tr)return 'no row';"
            "var td=tr.querySelectorAll('td')[2];"
            "return td?td.textContent.trim():'no td';"
            "})()"
        )
        print(f"10 秒后 td[2] 内容: {td2_after!r}")

    finally:
        tab.close()
        print("\n[诊断完成] 标签页保持打开，请对照浏览器查看。")
        print(f"标签页 ID: {tab_id}（如需关闭可忽略）")


if __name__ == "__main__":
    main()
