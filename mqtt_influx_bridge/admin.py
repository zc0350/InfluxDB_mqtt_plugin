"""库与表的建/查/删：按 influx.version 分派 v1 / v2 / v3 三套 API 形状。

line protocol 只有一种，所以写入路径是通用的（见 writer.py）；建库、探活、查询、删表
这四件，各代后端的路径、参数名和查询方言都不一样，差异全部收敛在这个模块里。
"""
from __future__ import annotations

import json
import logging
import re
from urllib.parse import quote_plus, urlencode

from .config import InfluxConfig
from .writer import HttpClient, TransientError, auth_header

log = logging.getLogger(__name__)

# v1/v3 的库名、表名规则；同时挡住拼进 InfluxQL 的注入
_IDENT = re.compile(r"[A-Za-z0-9_\-]{1,257}")
_DURATION = re.compile(r"^(\d+)(h|d|w|mo|y)$")
_DAYS = {"h": 1 / 24, "d": 1.0, "w": 7.0, "mo": 30.0, "y": 365.0}

# 「有哪些表」在三家方言里的写法。v3 只取 table_schema='iox'：
# information_schema 里还挂着 system / information_schema 自己的视图，不滤就是噪音。
TABLES_QUERY = {
    "v1": "SHOW MEASUREMENTS",
    "v2": 'import "influxdata/influxdb/schema"\n  schema.measurements(bucket: "{bucket}")',
    "v3": "SELECT table_name, table_type FROM information_schema.tables "
          "WHERE table_schema = 'iox' ORDER BY table_name",
}
_PING_PATH = {"v1": "/ping", "v2": "/health", "v3": "/health"}
_DROP_HINT = (
    "v2 没有按 measurement 删除的接口：只能整 bucket 删（DELETE /api/v2/buckets/{id}），"
    "或者给 bucket 设 retention。按年分表在 v2 上建议一 bucket 一年 + bucket retention。"
)


class BackendError(Exception):
    """后端拒绝了管理请求，带状态码与响应片段。"""

    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status} {body[:300]}")
        self.status = status
        self.body = body


class UnsupportedBackend(Exception):
    """这代后端确实没有该能力 —— 不做假兼容，直接说清楚。"""


def duration_to_days(text: str) -> float | None:
    """30d/4w/1y 这类时长换算成天；换不出来返回 None。"""
    m = _DURATION.match(text.strip())
    return int(m.group(1)) * _DAYS[m.group(2)] if m else None


class AdminClient:
    def __init__(self, cfg: InfluxConfig, http: HttpClient | None = None):
        self.cfg = cfg
        self.http = http or HttpClient(cfg.url, cfg.timeout_seconds, cfg.insecure_skip_verify)

    def close(self) -> None:
        self.http.close()

    # ---------------- 内部 ----------------
    def _headers(self, content_type: str | None = "application/json", accept: str | None = None):
        h = {}
        auth = auth_header(self.cfg)
        if auth:
            h["Authorization"] = auth
        if content_type:
            h["Content-Type"] = content_type
        if accept:
            h["Accept"] = accept
        return h

    def _call(self, method: str, path: str, body=None, headers=None, form: str = "") -> bytes:
        if form:
            raw, hdr = form.encode("utf-8"), self._headers("application/x-www-form-urlencoded")
        elif body is None:
            raw, hdr = None, headers or self._headers(content_type=None)
        else:
            raw, hdr = json.dumps(body).encode("utf-8"), headers or self._headers()
        status, data = self.http.request(method, path, raw, hdr)
        if status >= 400:
            raise BackendError(status, data.decode("utf-8", "replace"))
        return data

    def _check(self, name: str, what: str) -> str:
        if not _IDENT.fullmatch(name):
            raise ValueError(f"{what} 不合法（只允许字母数字与 - _）：{name!r}")
        return name

    def _influxql(self, q: str, use_db: bool = True) -> dict:
        """v1 的查询与 DDL 都走 /query；错误在 200 响应体里，得自己翻。

        SHOW DATABASES / CREATE DATABASE 不能带 db=：那时库还不存在，1.x 会直接回 database not found。
        """
        path = "/query?" + urlencode({"db": self.cfg.database}) if use_db else "/query"
        raw = self._call("POST", path, form="q=" + quote_plus(q)) or b"{}"
        data = json.loads(raw)
        for res in data.get("results", []):
            if res.get("error"):
                raise BackendError(200, res["error"])
        return data

    @staticmethod
    def _rows(data: dict) -> list[list]:
        out: list[list] = []
        for res in data.get("results", []):
            for series in res.get("series", []):
                out.extend(series.get("values") or [])
        return out

    # ---------------- 探活 ----------------
    def ping(self) -> tuple[bool, str]:
        path = _PING_PATH[self.cfg.version]
        try:
            status, _ = self.http.get(path, self._headers(content_type=None))
        except TransientError as e:
            return False, f"{self.cfg.url}{path} 连不上：{e}"
        return status < 400, f"{self.cfg.url}{path} -> HTTP {status}"

    # ---------------- 建库 ----------------
    def ensure_database(self, retention: str = "") -> tuple[str, str]:
        """建库 / 建 bucket，幂等。返回 (状态, 说明)，状态 in created|exists|ignored。"""
        db = self._check(self.cfg.database, "influx.database")
        if self.cfg.version == "v1":
            names = [v[0] for v in self._rows(self._influxql("SHOW DATABASES", use_db=False)) if v]
            if db in names:
                return "exists", f"database {db} 已存在"
            self._influxql(f"CREATE DATABASE {db}", use_db=False)
            note = f"database {db} 已创建"
            if retention:
                note += "（v1 的保留期请用 retention policy 管理，这里没建 RP）"
            return "created", note
        if self.cfg.version == "v2":
            if not self.cfg.org:
                raise ValueError("v2 建 bucket 需要 influx.org")
            org = urlencode({"org": self.cfg.org})
            listed = json.loads(self._call("GET", f"/api/v2/buckets?{org}&name={db}") or b"{}")
            if listed.get("buckets"):
                return "exists", f"bucket {db} 已存在"
            body = {"name": db, "org": self.cfg.org, "retentionRules": []}
            if retention:
                days = duration_to_days(retention)
                if days is None or days < 1:
                    log.warning("v2 的 bucket retention 按秒存但有最小粒度，%r 解析不了，已忽略", retention)
                else:
                    body["retentionRules"] = [{"type": "expire", "everySeconds": int(days * 86400)}]
            self._call("POST", f"/api/v2/buckets?{org}", body)
            return "created", f"bucket {db} 已创建（org={self.cfg.org}）"
        body: dict = {"db": db}
        if retention:
            body["retention_period"] = retention
        try:
            self._call("POST", "/api/v3/configure/database", body)
        except BackendError as e:
            if "already exist" in e.body.lower():
                return "exists", f"database {db} 已存在"
            raise
        return "created", f"database {db} 已创建（implicit schema，写入即建表）"

    # ---------------- 查询 ----------------
    def query(self, statement: str) -> str:
        """按 influx.version 用各家原生读接口；statement 得是该家的方言，不自动翻译。"""
        if self.cfg.version == "v1":
            return json.dumps(self._influxql(statement), ensure_ascii=False, indent=2)
        if self.cfg.version == "v2":
            if not self.cfg.org:
                raise ValueError("v2 查询需要 influx.org")
            body = {"query": statement,
                    "dialect": {"header": True, "dateTimeFormat": "RFC3339Nano"}}
            hdr = self._headers(accept="text/csv")
            return self._call("POST", f"/api/v2/query?{urlencode({'org': self.cfg.org})}",
                              body, hdr).decode("utf-8", "replace")
        hdr = self._headers(accept="application/json")
        body = {"db": self.cfg.database, "q": statement, "format": "json"}
        return self._call("POST", "/api/v3/query_sql", body, hdr).decode("utf-8", "replace")

    def tables(self) -> str:
        stmt = TABLES_QUERY[self.cfg.version]
        if self.cfg.version == "v2":
            stmt = stmt.format(bucket=self.cfg.database)
        return self.query(stmt)

    # ---------------- 删表 ----------------
    def drop_table(self, table: str, hard: bool = False) -> tuple[str, str]:
        """删一整张表（年表清理的唯一通用手段）。v2 不支持，抛 UnsupportedBackend。"""
        name = self._check(table, "表名")
        if self.cfg.version == "v1":
            self._influxql(f"DROP MEASUREMENT {name}")
            return "dropped", f"measurement {name} 已删除（v1 立即生效）"
        if self.cfg.version == "v2":
            raise UnsupportedBackend(_DROP_HINT)
        params = {"db": self.cfg.database, "table": name}
        if hard:
            params["hard_delete_at"] = "now"
        try:
            self._call("DELETE", "/api/v3/configure/table?" + urlencode(params))
        except BackendError as e:
            if e.status == 404 or "not found" in e.body.lower():
                return "missing", f"表 {name} 不存在"
            if e.status == 409:
                # 官方文档：删已删过的表回 409。软删只改名，重复删不会额外回收磁盘。
                return "dropped", f"表 {name} 此前已删除（409）：改名条目要等 hard_delete 才消失"
            raise
        if hard:
            return "dropped", f"表 {name} 已硬删除，磁盘立即可回收"
        return "dropped", (f"表 {name} 已软删除：改名成 {name}-<ts> 仍会出现在表清单里，"
                           "磁盘等后台 compaction 回收；要立刻回收加 --hard")
