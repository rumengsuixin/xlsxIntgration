"""浏览器操作封装模块（代号4）。

通过 Chrome DevTools Protocol (CDP) 连接到已由 app4.launch_chrome() 启动的 Chrome 实例。
复用同一 user-data-dir，完全保留登录态。连接断开后 Chrome 继续运行。

使用前提：Chrome 必须以固定端口启动，例如：
    --remote-debugging-port=9224
可在 .env 中配置 MODE4_DEBUG_PORT=9224 或使用默认值 CHROME_DEBUG_PORT_4。
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger(__name__)


def _require_websocket():
    try:
        import websocket
        return websocket
    except ImportError as exc:
        raise ImportError(
            "浏览器自动化操作需要 websocket-client 库。"
            "请运行：pip install websocket-client"
        ) from exc


def is_chrome_running(debug_port: int, timeout: float = 2.0) -> bool:
    """检查 Chrome DevTools 端口是否可达（即 Chrome 是否已启动）。"""
    try:
        urllib.request.urlopen(
            f"http://localhost:{debug_port}/json", timeout=timeout
        )
        return True
    except Exception:
        return False


def get_chrome_pages(debug_port: int, timeout: float = 3.0) -> list:
    """返回 Chrome DevTools /json 端点的标签页列表。"""
    data = urllib.request.urlopen(
        f"http://localhost:{debug_port}/json", timeout=timeout
    ).read()
    return json.loads(data)


class ChromeOperator:
    """通过 CDP WebSocket 连接到已启动的 Chrome 实例，提供高层浏览器操作 API。

    Parameters
    ----------
    debug_port : int
        Chrome 的 --remote-debugging-port 值，必须是固定端口（非 0）。
    connect_retries : int
        等待 Chrome DevTools 端口就绪的最大重试次数，每次间隔 1 秒。
    """

    def __init__(self, debug_port: int, connect_retries: int = 5) -> None:
        if debug_port == 0:
            raise ValueError(
                "debug_port 不能为 0。请将 Chrome 启动参数改为固定端口，"
                "或在 .env 中设置 MODE4_DEBUG_PORT=9224。"
            )
        self._debug_port = debug_port
        self._connect_retries = connect_retries
        self._ws = None
        self._msg_id = 0

    # ── 连接生命周期 ──────────────────────────────────────────────────────────

    def connect(self, tab_url: Optional[str] = None) -> "ChromeOperator":
        """连接到已运行的 Chrome（不启动新进程）。返回 self，支持链式调用。

        tab_url 非空时，优先连接 URL 以该前缀开头的标签页；无匹配则连接第一个标签页。
        """
        websocket = _require_websocket()
        for attempt in range(self._connect_retries):
            try:
                data = urllib.request.urlopen(
                    f"http://localhost:{self._debug_port}/json", timeout=3
                ).read()
                break
            except Exception:
                if attempt == self._connect_retries - 1:
                    raise
                time.sleep(1.0)

        pages = json.loads(data)
        if not pages:
            raise RuntimeError(f"Chrome 端口 {self._debug_port} 没有打开的页面")

        ws_url = None
        if tab_url:
            for page in pages:
                if page.get("type") == "page" and page.get("url", "").startswith(tab_url):
                    ws_url = page["webSocketDebuggerUrl"]
                    logger.info("已定位到目标标签页: %s", page.get("url"))
                    break
        if ws_url is None:
            ws_url = pages[0]["webSocketDebuggerUrl"]

        # Windows 上 localhost 可能解析为 ::1（IPv6），而 Chrome 只监听 127.0.0.1
        ws_url = ws_url.replace("://localhost:", "://127.0.0.1:")
        self._ws = websocket.create_connection(ws_url)
        logger.info("已通过 CDP 连接到 Chrome 端口 %d", self._debug_port)
        return self

    def disconnect(self) -> None:
        """断开 WebSocket 连接（不关闭 Chrome）。"""
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            finally:
                self._ws = None
            logger.info("已断开 Chrome 端口 %d 的 CDP 连接", self._debug_port)

    @contextmanager
    def session(self) -> Generator["ChromeOperator", None, None]:
        """上下文管理器：自动 connect / disconnect。

        用法::

            with ChromeOperator(9224).session() as op:
                op.navigate("https://example.com")
        """
        self.connect()
        try:
            yield self
        finally:
            self.disconnect()

    # ── 底层 CDP 通信 ─────────────────────────────────────────────────────────

    def _send(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        """发送 CDP 命令并等待对应响应（忽略中间推送事件）。"""
        self._msg_id += 1
        msg = {"id": self._msg_id, "method": method, "params": params or {}}
        self._ws.send(json.dumps(msg))
        while True:
            raw = self._ws.recv()
            resp = json.loads(raw)
            if resp.get("id") == self._msg_id:
                if "error" in resp:
                    raise RuntimeError(f"CDP 错误 [{method}]: {resp['error']}")
                return resp

    # ── 高层操作 ──────────────────────────────────────────────────────────────

    def navigate(self, url: str) -> None:
        """在当前 Tab 打开 URL。"""
        self._send("Page.navigate", {"url": url})
        logger.debug("导航到: %s", url)

    def evaluate(self, expression: str) -> Any:
        """执行 JavaScript 表达式，返回结果值。JS 异常时打印 warning 并返回 None。"""
        resp = self._send("Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
        })
        cdp_eval = resp.get("result", {})
        if "exceptionDetails" in cdp_eval:
            exc_desc = (
                cdp_eval["exceptionDetails"]
                .get("exception", {})
                .get("description")
                or cdp_eval["exceptionDetails"].get("text", "未知 JS 异常")
            )
            logger.warning("JS 执行异常: %s", exc_desc)
            return None
        return cdp_eval.get("result", {}).get("value")

    def click(self, css_selector: str) -> None:
        """通过 JavaScript 点击 CSS 选择器匹配的第一个元素。"""
        self.evaluate(
            f"document.querySelector({json.dumps(css_selector)}).click()"
        )
        logger.debug("点击元素: %s", css_selector)

    def fill(self, css_selector: str, text: str) -> None:
        """通过 JavaScript 填写输入框（触发 input / change 事件）。"""
        self.evaluate(
            f"(function(){{"
            f"  var el = document.querySelector({json.dumps(css_selector)});"
            f"  el.value = {json.dumps(text)};"
            f"  el.dispatchEvent(new Event('input', {{bubbles:true}}));"
            f"  el.dispatchEvent(new Event('change', {{bubbles:true}}));"
            f"}})()"
        )
        logger.debug("填写输入框 %s", css_selector)

    def get_text(self, css_selector: str) -> str:
        """获取元素文本内容。"""
        return self.evaluate(
            f"document.querySelector({json.dumps(css_selector)})?.textContent || ''"
        ) or ""

    def current_url(self) -> str:
        """返回当前页面 URL。"""
        return self.evaluate("location.href") or ""

    def wait_for_condition(
        self,
        js_condition: str,
        timeout: float = 10.0,
        poll: float = 0.5,
    ) -> None:
        """轮询等待 JavaScript 条件表达式为真。

        Parameters
        ----------
        js_condition : str
            返回布尔值的 JS 表达式，例如 ``"!!document.querySelector('#login-btn')"``
        timeout : float
            最长等待秒数。
        poll : float
            轮询间隔秒数。
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.evaluate(js_condition):
                return
            time.sleep(poll)
        raise TimeoutError(f"等待超时（{timeout}s）：{js_condition}")


def connect_to_chrome(debug_port: int) -> ChromeOperator:
    """快捷工厂：创建并连接 ChromeOperator。

    用法::

        op = connect_to_chrome(9224)
        try:
            print(op.current_url())
        finally:
            op.disconnect()
    """
    return ChromeOperator(debug_port).connect()


def open_new_tab(debug_port: int, url: str) -> None:
    """在已运行的 Chrome 中通过 CDP Target.createTarget 打开新标签页。"""
    op = ChromeOperator(debug_port, connect_retries=1)
    op.connect()
    try:
        op._send("Target.createTarget", {"url": url})
        logger.info("已通过 CDP 在 Chrome 中打开新标签页: %s", url)
    finally:
        op.disconnect()
