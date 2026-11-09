"""本地 HTTP mock：供 writer / 端到端离线 / 管理面测试使用。

默认行为是「只记账、回 state.status」；需要响应体时按 (method, path 前缀) 注册路由。
"""
from __future__ import annotations

import gzip
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class MockState:
    def __init__(self, status: int = 204):
        self.status = status
        self.sequence: list[int] = []
        self.requests: list[dict] = []
        self.lock = threading.Lock()
        self.routes: dict[tuple[str, str], tuple[int, str, str]] = {}

    def route(self, method: str, path_prefix: str, body: str = "",
              status: int = 200, content_type: str = "application/json") -> None:
        """GET/POST/DELETE 命中该前缀时返回指定状态与响应体。"""
        self.routes[(method.upper(), path_prefix)] = (status, content_type, body)

    def matched(self, method: str, path: str = "") -> list[dict]:
        """已收到的请求（可按方法与前缀过滤），便于断言只发了该发的那一次。"""
        return [r for r in self.requests
                if (not method or r["method"] == method) and r["path"].startswith(path)]


class _BaseHandler(BaseHTTPRequestHandler):
    state: MockState

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        body = self.rfile.read(length) if length else b""
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        with self.state.lock:
            self.state.requests.append({
                "method": self.command,
                "path": self.path,
                "headers": dict(self.headers),
                "body": body.decode("utf-8", "replace"),
            })
            hit = next((v for (m, p), v in self.state.routes.items()
                        if m == self.command and self.path.startswith(p)), None)
            if self.state.sequence:
                status = self.state.sequence.pop(0)
            else:
                status = hit[0] if hit else self.state.status
        payload = hit[2].encode("utf-8") if hit else b""
        self.send_response(status)
        if hit:
            self.send_header("Content-Type", hit[1])
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    do_POST = do_GET = do_DELETE = _handle

    def log_message(self, *args):
        pass


def start_mock_server(state: MockState) -> ThreadingHTTPServer:
    handler = type("MockHandler", (_BaseHandler,), {"state": state})
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv.daemon_threads = True
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv
