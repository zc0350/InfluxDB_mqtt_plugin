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


def _count(text: str, version: str) -> int | None:
    """只认 count 列，不能抓响应里的第一个整数：v1 的 statement_id 和 v2 的 table
    都是 0，且都排在真正的计数前面，抓第一个整数会把「写进去 3 行」读成 0。"""
    if version == "v2":
        # Flux CSV 默认带 #datatype / #group / #default 三行注解，真正的表头在它们后面。
        rows = [r for r in csv.reader(io.StringIO(text)) if r and not r[0].startswith("#")]
        if len(rows) < 2 or "_value" not in rows[0]:
            return None
        i = rows[0].index("_value")
        # count() 按 series 分组：3 个 dev tag 就是 3 行 _value=1，要加起来才是总点数。
        values = [int(row[i]) for row in rows[1:]
                  if len(row) > i and row[i].strip().lstrip("-").isdigit()]
        return sum(values) if values else None
    doc = json.loads(text)
    if version == "v1":
        doc = [dict(zip(s.get("columns") or [], values))
               for res in doc.get("results", [])
               for s in res.get("series", [])
               for values in s.get("values") or []]
    for row in doc:
        for name, value in row.items():
            if name.startswith("count") and isinstance(value, (int, float)):
                return int(value)
    return None


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
        got, raw = None, ""
        for _ in range(5):
            raw = self.admin.query(self.count_sql())
            got = _count(raw, self.version)
            if got == 3:
                break
            time.sleep(1)
        self.assertEqual(got, 3, f"查询结果：{raw}")

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
        got, raw = None, ""
        for _ in range(5):
            raw = admin.query(sql)
            got = _count(raw, cfg.version)
            if got == 1:
                break
            time.sleep(1)
        self.assertEqual(got, 1, f"查询结果：{raw}")
        try:
            admin.drop_table(table, hard=True)
        except (UnsupportedBackend, BackendError):
            pass


class TestCountParsing(unittest.TestCase):
    """钉住三家 count 响应的形状：v1/v3 是真机样本，v2 按 Flux 默认注解构造。不需要后端。"""

    V1 = json.dumps({"results": [{"statement_id": 0, "series": [
        {"name": "live_smoke_1794236204", "columns": ["time", "count_temp"],
         "values": [["1970-01-01T00:00:00Z", 3]]}]}]})
    V2 = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string\r\n"
        "#group,false,false,true,true,false,false,true,true\r\n"
        "#default,_result,,,,,,,\r\n"
        ",result,table,_start,_stop,_time,_value,_measurement,dev\r\n"
        ",,0,2026-11-09T14:00:00Z,2026-11-09T15:00:00Z,2026-11-09T15:00:00Z,1,live_smoke_1,d0\r\n"
        ",,1,2026-11-09T14:00:00Z,2026-11-09T15:00:00Z,2026-11-09T15:00:00Z,1,live_smoke_1,d1\r\n"
        ",,2,2026-11-09T14:00:00Z,2026-11-09T15:00:00Z,2026-11-09T15:00:00Z,1,live_smoke_1,d2\r\n"
    )
    V3 = '[{"count(*)":83}]'

    def test_v1_reads_count_column_not_statement_id(self):
        self.assertEqual(_count(self.V1, "v1"), 3)

    def test_v1_single_point(self):
        self.assertEqual(_count(self.V1.replace(", 3]", ", 1]"), "v1"), 1)

    def test_v2_sums_the_per_series_counts(self):
        self.assertEqual(_count(self.V2, "v2"), 3)

    def test_v2_single_series(self):
        one = "\r\n".join(self.V2.split("\r\n")[:5]) + "\r\n"
        self.assertEqual(_count(one, "v2"), 1)

    def test_v2_without_annotations(self):
        plain = "\r\n".join(line for line in self.V2.split("\r\n") if not line.startswith("#"))
        self.assertEqual(_count(plain, "v2"), 3)

    def test_v3_reads_count_star(self):
        self.assertEqual(_count(self.V3, "v3"), 83)

    def test_no_rows_is_none_not_zero(self):
        self.assertIsNone(_count('{"results":[{"statement_id":0}]}', "v1"))
        self.assertIsNone(_count("\r\n".join(self.V2.split("\r\n")[:4]) + "\r\n", "v2"))
        self.assertIsNone(_count("[]", "v3"))


if __name__ == "__main__":
    unittest.main()
