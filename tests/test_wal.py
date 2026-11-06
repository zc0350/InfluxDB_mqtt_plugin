from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from mqtt_influx_bridge.wal import Wal


class TestWal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "queue.db"
        self.wal = Wal(self.path)

    def tearDown(self):
        self.wal.close()
        self.tmp.cleanup()

    def test_append_fetch_delete(self):
        ids = self.wal.append(["m,h=a v=1 1", "m,h=b v=2 2"], "sensors/a")
        self.assertEqual(len(ids), 2)
        self.assertEqual(self.wal.pending_count(), 2)
        rows = self.wal.fetch(10)
        self.assertEqual([r.line for r in rows], ["m,h=a v=1 1", "m,h=b v=2 2"])
        self.assertEqual(rows[0].topic, "sensors/a")
        self.assertTrue(self.wal.exists_any(ids))
        self.wal.delete(ids)
        self.assertEqual(self.wal.pending_count(), 0)
        self.assertFalse(self.wal.exists_any(ids))

    def test_defer_pushes_retry_into_future(self):
        ids = self.wal.append(["m v=1 1"], "t")
        self.wal.defer(ids, base_ms=5000, max_ms=30000)
        self.assertEqual(self.wal.fetch(10, now=time.time()), [])  # 退避窗口内取不到
        rows = self.wal.fetch(10, now=time.time() + 60)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].attempts, 1)

    def test_to_dlq_removes_from_pending(self):
        rows = self.wal.fetch(10)  # empty
        self.assertEqual(rows, [])
        ids = self.wal.append(["bad line"], "t")
        self.wal.to_dlq(self.wal.fetch(10), "invalid schema")
        self.assertEqual(self.wal.pending_count(), 0)
        self.assertFalse(self.wal.exists_any(ids))
        self.assertEqual(self.wal.stats()["dlq"], 1)

    def test_raw_dlq_for_unparseable_messages(self):
        self.wal.to_dlq_raw("not json", "sensors/x", "parse: bad json")
        self.assertEqual(self.wal.stats()["dlq"], 1)
        self.assertEqual(self.wal.pending_count(), 0)

    def test_survives_restart(self):
        self.wal.append(["m,h=a v=1 1"], "t")
        self.wal.close()
        self.wal = Wal(self.path)  # 模拟进程重启
        self.assertEqual(self.wal.pending_count(), 1)
        self.assertEqual(self.wal.fetch(10)[0].line, "m,h=a v=1 1")

    def test_stats(self):
        self.wal.append(["a"], "t")
        s = self.wal.stats()
        self.assertEqual(s["pending"], 1)
        self.assertEqual(s["dlq"], 0)
        self.assertGreaterEqual(s["oldest_pending_age_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
