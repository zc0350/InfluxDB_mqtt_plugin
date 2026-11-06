"""本地 HTTP mock：供 writer / 端到端离线测试使用。"""
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


class _BaseHandler(BaseHTTPRequestHandler):
    state: MockState

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        with self.state.lock:
            self.state.requests.append(
                {
                    "path": self.path,
                    "headers": dict(self.headers),
                    "body": body.decode("utf-8", "replace"),
                }
            )
            status = self.state.sequence.pop(0) if self.state.sequence else self.state.status
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


def start_mock_server(state: MockState) -> ThreadingHTTPServer:
    handler = type("MockHandler", (_BaseHandler,), {"state": state})
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv.daemon_threads = True
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv
