"""真机冒烟：对着一个真实后端跑「建库 → 写 → 查行数 → 删表」。

默认 skip。CI 的 backend-smoke job 会起 v1/v2/v3 三个服务并注入下面这组环境变量；
本地想跑就自己填：

  LIVE_INFLUX_URL=http://127.0.0.1:8181 LIVE_INFLUX_VERSION=v3 \
  LIVE_INFLUX_TOKEN=... python -m unittest tests.test_backends_live

这里只验证「三代共用的那条写路径 + 分派出去的管理面」真能对上后端，不重复断言请求形状
（那是 test_admin.py 用 mock 干的事）。
"""
from __future__ import annotations

import csv
import io
import json
import os
import time
import unittest

from mqtt_influx_bridge.admin import (
    AdminClient, BackendError, UnsupportedBackend,
)
from mqtt_influx_bridge.config import InfluxConfig
from mqtt_influx_bridge.writer import InfluxWriter


def _cfg() -> InfluxConfig:
    return InfluxConfig(
        version=os.environ.get("LIVE_INFLUX_VERSION", "v3"),
        url=os.environ["LIVE_INFLUX_URL"],
        database=os.environ.get("LIVE_INFLUX_DB", "mqtt"),
        org=os.environ.get("LIVE_INFLUX_ORG", ""),
        token=os.environ.get("LIVE_INFLUX_TOKEN", ""),
        username=os.environ.get("LIVE_INFLUX_USER", ""),
        password=os.environ.get("LIVE_INFLUX_PASS", ""),
        timeout_seconds=20.0,
    )


def _first_int(doc) -> int | None:
    """在 JSON 响应里找第一个整数 —— count(*) 的结果位置各家不同，值本身只有一个。"""
    if isinstance(doc, bool):
        return None
    if isinstance(doc, int):
        return doc
    if isinstance(doc, list):
        for item in doc:
            got = _first_int(item)
            if got is not None:
                return got
    if isinstance(doc, dict):
        for item in doc.values():
            got = _first_int(item)
            if got is not None:
                return got
    return None


def _count(text: str, version: str) -> int | None:
    if version == "v2":
        rows = list(csv.DictReader(io.StringIO(text)))
        for row in rows:
            for key in ("_value", "count", "result"):
                if row.get(key, "").strip().isdigit():
                    return int(row[key])
        return _first_int([[int(v) for v in row.values() if v.strip().isdigit()] for row in rows])
    return _first_int(json.loads(text))


@unittest.skipUnless(os.environ.get("LIVE_INFLUX_URL"), "未设 LIVE_INFLUX_URL，跳过真机冒烟")
class TestLiveBackend(unittest.TestCase):
    def setUp(self):
        self.cfg = _cfg()
        self.version = self.cfg.version
        self.table = f"live_smoke_{int(time.time())}"
        self.admin = AdminClient(self.cfg)
        self.addCleanup(self.admin.close)

    def rows(self, n: int = 3) -> list[str]:
        now = time.time_ns()
        return [
            f"{self.table},dev=d{i} temp={20 + i}.0 {now + i * 1000}" for i in range(n)
        ]

    def test_ping(self):
        ok, note = self.admin.ping()
        self.assertTrue(ok, note)

    def test_write_then_count_then_drop(self):
        state, note = self.admin.ensure_database()
        self.assertIn(state, ("created", "exists"), note)

        writer = InfluxWriter(self.cfg)
        self.addCleanup(writer.close)
        writer.write(self.rows())

        # 各家都是「写入即可见」（v3 的 no_sync=false 已等 WAL），不需要 sleep；
        # 但真机偶发跨节点可见性延迟，给 5 次机会免得 CI 抖。
        got = None
        for _ in range(5):
            got = _count(self.admin.query(self.count_sql()), self.version)
            if got == 3:
                break
            time.sleep(1)
        self.assertEqual(got, 3, f"查询结果：{self.admin.query(self.count_sql())}")

        self.assertIn(self.table, self.admin.tables())
        self._cleanup_table()

    def count_sql(self) -> str:
        if self.version == "v2":
            return (
                f'from(bucket: {json.dumps(self.cfg.database)})'
                f' |> range(start: -1h) |> filter(fn: (r) => r._measurement == "{self.table}") |> count()'
            )
        return f'SELECT count(*) FROM "{self.table}"'

    def _cleanup_table(self):
        try:
            state, note = self.admin.drop_table(self.table, hard=True)
        except UnsupportedBackend as e:
            # v2 确实没有按 measurement 删的接口：这是能力边界，不是 bug。
            print(f"\n[skip cleanup] {e}")
            return
        self.assertIn(state, ("dropped", "missing"), note)


@unittest.skipUnless(os.environ.get("LIVE_INFLUX_URL"), "未设 LIVE_INFLUX_URL，跳过真机冒烟")
class TestLiveIdempotentOverwrite(unittest.TestCase):
    """同 (表, tagset, 时间) 覆盖写：三代一致，这是重投不重复的关键前提。"""

    def test_same_point_written_twice_is_one_point(self):
        cfg = _cfg()
        admin = AdminClient(cfg)
        writer = InfluxWriter(cfg)
        self.addCleanup(admin.close)
        self.addCleanup(writer.close)
        admin.ensure_database()
        table = f"live_idem_{int(time.time())}"
        line = f"{table},dev=d1 temp=21.0 1780000000000000000"
        writer.write([line])
        writer.write([line])
        if cfg.version == "v2":
            sql = (f'from(bucket: {json.dumps(cfg.database)}) |> range(start: -1y)'
                   f' |> filter(fn: (r) => r._measurement == "{table}") |> count()')
        else:
            sql = f'SELECT count(*) FROM "{table}"'
        got = None
        for _ in range(5):
            got = _count(admin.query(sql), cfg.version)
            if got == 1:
                break
            time.sleep(1)
        self.assertEqual(got, 1)
        try:
            admin.drop_table(table, hard=True)
        except (UnsupportedBackend, BackendError):
            pass


if __name__ == "__main__":
    unittest.main()
