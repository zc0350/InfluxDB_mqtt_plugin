"""InfluxDB HTTP 写入客户端与批量刷写逻辑（v1 / v2 / v3 端点自适应）。"""
from __future__ import annotations

import base64
import gzip
import http.client
import logging
import ssl
from urllib.parse import urlencode, urlsplit

from .config import InfluxConfig, WriterConfig
from .wal import Row, Wal

log = logging.getLogger(__name__)


class TransientError(Exception):
    """可重试：网络错误、5xx、429。"""


class PermanentError(Exception):
    """不可重试：4xx（鉴权、schema 冲突、非法 line protocol）。"""


class HttpClient:
    """基于标准库 http.client 的 keep-alive 客户端，无第三方依赖。"""

    def __init__(self, base_url: str, timeout: float = 10.0, insecure: bool = False):
        u = urlsplit(base_url.rstrip("/"))
        if u.scheme not in ("http", "https"):
            raise ValueError(f"influx.url 必须是 http/https: {base_url}")
        self.scheme = u.scheme
        self.host = u.hostname or "127.0.0.1"
        self.port = u.port or (443 if u.scheme == "https" else 80)
        self.base_path = u.path.rstrip("/")
        self.timeout = timeout
        self._context = ssl._create_unverified_context() if (insecure and u.scheme == "https") else None
        self._conn: http.client.HTTPConnection | None = None

    def _new_conn(self) -> http.client.HTTPConnection:
        if self.scheme == "https":
            return http.client.HTTPSConnection(
                self.host, self.port, timeout=self.timeout, context=self._context
            )
        return http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def post(self, path: str, body: bytes, headers: dict[str, str]) -> tuple[int, bytes]:
        return self.request("POST", path, body, headers)

    def get(self, path: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return self.request("GET", path, None, headers)

    def delete(self, path: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return self.request("DELETE", path, None, headers)

    def request(
        self, method: str, path: str, body: bytes | None, headers: dict[str, str]
    ) -> tuple[int, bytes]:
        if self._conn is None:
            self._conn = self._new_conn()
        try:
            self._conn.request(method, self.base_path + path, body=body, headers=headers)
            resp = self._conn.getresponse()
            data = resp.read()
            return resp.status, data
        except (OSError, http.client.HTTPException, ssl.SSLError) as e:
            self.close()
            raise TransientError(f"HTTP 请求失败: {e}") from e

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


def auth_header(cfg: InfluxConfig) -> str:
    """三代的鉴权头不同：v1 Basic(username/password)，v2 Token，v3 Bearer。"""
    if cfg.version == "v1" and cfg.username:
        raw = f"{cfg.username}:{cfg.password}".encode()
        return "Basic " + base64.b64encode(raw).decode()
    if cfg.version == "v2" and cfg.token:
        return f"Token {cfg.token}"
    if cfg.version == "v3" and cfg.token:
        return f"Bearer {cfg.token}"
    return ""


class InfluxWriter:
    def __init__(self, cfg: InfluxConfig, http: HttpClient | None = None):
        self.cfg = cfg
        self.http = http or HttpClient(cfg.url, cfg.timeout_seconds, cfg.insecure_skip_verify)

    def endpoint(self) -> str:
        if self.cfg.version == "v3":
            params = {
                "db": self.cfg.database,
                "precision": "nanosecond",
                "no_sync": "true" if self.cfg.no_sync else "false",
                "accept_partial": "true" if self.cfg.accept_partial else "false",
            }
            return "/api/v3/write_lp?" + urlencode(params)
        if self.cfg.version == "v2":
            params = {"org": self.cfg.org, "bucket": self.cfg.database, "precision": "ns"}
            return "/api/v2/write?" + urlencode(params)
        return "/write?" + urlencode({"db": self.cfg.database, "precision": "ns"})

    def headers(self) -> dict[str, str]:
        h = {"Content-Type": "text/plain; charset=utf-8"}
        auth = auth_header(self.cfg)
        if auth:
            h["Authorization"] = auth
        if self.cfg.gzip:
            h["Content-Encoding"] = "gzip"
        return h

    def write(self, lines: list[str]) -> None:
        body = "\n".join(lines).encode("utf-8")
        if self.cfg.gzip:
            body = gzip.compress(body)
        status, data = self.http.post(self.endpoint(), body, self.headers())
        if status in (200, 201, 204):
            return
        snippet = data[:400].decode("utf-8", "replace")
        if 400 <= status < 500 and status not in (408, 429):
            raise PermanentError(f"HTTP {status}: {snippet}")
        raise TransientError(f"HTTP {status}: {snippet}")

    def close(self) -> None:
        self.http.close()


def _isolate_rejected(rows: list[Row], writer: InfluxWriter, wal: Wal, cfg: WriterConfig) -> str:
    """整批被 4xx 拒绝后二分定位坏行：好行落库、单行确认坏的进 DLQ、判不出的退回重试。

    InfluxDB 3 在 accept_partial=false 下会因为一行字段类型冲突拒掉整批，
    不做隔离就会让一条毒药消息把同批上千行好数据一起拖进 DLQ。
    """
    ok: list[Row] = []
    bad: list[tuple[Row, str]] = []
    undecided: list[Row] = []
    stack: list[list[Row]] = [rows]
    probes = 0
    while stack:
        group = stack.pop()
        try:
            writer.write([r.line for r in group])
        except PermanentError as e:
            if len(group) == 1:
                bad.append((group[0], str(e)))
            elif probes < cfg.max_split_probes:
                probes += 1
                mid = len(group) // 2
                stack.append(group[:mid])
                stack.append(group[mid:])
            else:
                undecided.extend(group)
        except TransientError as e:
            # DB 转为不可用，剩余分组不再判定，避免把好数据误判成坏数据
            undecided.extend(group)
            for rest in stack:
                undecided.extend(rest)
            stack.clear()
            log.warning("隔离过程中写入中断（%s），%d 行退回重试", e, len(undecided))
        else:
            ok.extend(group)

    if ok:
        wal.delete([r.id for r in ok])
    for row, reason in bad:
        wal.to_dlq([row], f"permanent: {reason}")
    if undecided:
        wal.defer([r.id for r in undecided], cfg.retry_base_ms, cfg.retry_max_ms)
    log.error(
        "整批被拒后二分隔离（%d 次探测）：%d 行已落库 / %d 行进 DLQ / %d 行待重试",
        probes, len(ok), len(bad), len(undecided),
    )
    if bad:
        return "dlq"
    return "retry" if undecided else "ok"


def flush_once(
    wal: Wal,
    writer: InfluxWriter,
    cfg: WriterConfig,
    on_result=None,
) -> int:
    """取一批 pending -> 批量写入 -> 删除 / 退避 / 死信。返回处理的行数。"""
    rows: list[Row] = wal.fetch(cfg.batch_size)
    if not rows:
        return 0
    ids = [r.id for r in rows]
    lines = [r.line for r in rows]
    try:
        writer.write(lines)
    except PermanentError as e:
        if cfg.split_on_reject and cfg.max_split_probes > 0 and len(rows) > 1:
            outcome = _isolate_rejected(rows, writer, wal, cfg)
        else:
            wal.to_dlq(rows, f"permanent: {e}")
            outcome = "dlq"
            log.error("整批写入被拒绝，%d 行转入 DLQ：%s", len(rows), e)
    except TransientError as e:
        if cfg.max_retries and any(r.attempts + 1 >= cfg.max_retries for r in rows):
            wal.to_dlq(rows, f"retry limit: {e}")
            outcome = "dlq"
            log.error("重试次数超限，%d 行转入 DLQ", len(rows))
        else:
            wal.defer(ids, cfg.retry_base_ms, cfg.retry_max_ms)
            outcome = "retry"
            log.warning("写入失败，%d 行退避重试：%s", len(rows), e)
    else:
        wal.delete(ids)
        outcome = "ok"
    if on_result:
        on_result(outcome, ids)
    return len(rows)
