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

    def test_dlq_rows_exposes_id_line_topic_reason(self):
        """dlq_rows 每条是 (dlq_id, line, topic, reason)：id 在第 0 位，且按 id 倒序（最新在前）。"""
        self.wal.append(["m,h=a v=1 1"], "sensors/a")
        self.wal.to_dlq(self.wal.fetch(10), "invalid schema")
        self.wal.to_dlq_raw("not json", "sensors/x", "parse: bad json")

        rows = self.wal.dlq_rows()
        self.assertEqual(len(rows), 2, "两条死信都应被列出")
        self.assertTrue(all(len(r) == 4 for r in rows), "元组形状应为 (dlq_id, line, topic, reason)")
        self.assertTrue(all(isinstance(r[0], int) for r in rows), "第 0 位必须是整型 dlq_id")
        self.assertEqual([r[0] for r in rows], sorted([r[0] for r in rows], reverse=True),
                         "应按 dlq_id 倒序返回")

        dlq_id, line, topic, reason = rows[0]  # 最后写入的 to_dlq_raw
        self.assertEqual(line, "not json", "第 1 位应是原始行")
        self.assertEqual(topic, "sensors/x", "第 2 位应是 topic")
        self.assertEqual(reason, "parse: bad json", "第 3 位应是失败原因")
        self.assertEqual(rows[1][1], "m,h=a v=1 1")
        self.assertEqual(rows[1][2], "sensors/a")
        self.assertEqual(rows[1][3], "invalid schema")
        self.assertEqual(len(self.wal.dlq_rows(limit=1)), 1, "limit 应生效")

    def test_requeue_dlq_moves_row_back_to_pending(self):
        """requeue_dlq：死信放回 pending（next_retry=0 立刻可取），DLQ 少 1 条、pending 多 1 条。"""
        self.wal.append(["m,h=a v=1 1"], "sensors/a")
        self.wal.to_dlq(self.wal.fetch(10), "permanent: HTTP 400 bad field type")
        self.assertEqual(self.wal.pending_count(), 0)
        dlq_id, line, topic, _ = self.wal.dlq_rows()[0]

        self.assertTrue(self.wal.requeue_dlq(dlq_id), "存在的 id 应返回 True")
        self.assertEqual(self.wal.dlq_rows(), [], "重放后 DLQ 里的这条应被删除")
        self.assertEqual(self.wal.stats()["dlq"], 0, "stats 里的 dlq 计数应随之下降")
        self.assertEqual(self.wal.pending_count(), 1, "pending 应多出一条")
        self.assertEqual(self.wal.stats()["pending"], 1)

        rows = self.wal.fetch(10, now=time.time())
        self.assertEqual(len(rows), 1, "重排的行 next_retry=0，应立刻取得到")
        self.assertEqual(rows[0].line, line, "重放的应是原来那一行")
        self.assertEqual(rows[0].topic, topic)
        self.assertEqual(rows[0].next_retry, 0, "不应带着退避时间回来")
        self.assertEqual(rows[0].attempts, 0)

        self.assertFalse(self.wal.requeue_dlq(dlq_id), "同一 id 二次重放应返回 False")
        self.assertEqual(self.wal.pending_count(), 1, "二次重放不得凭空新增行")

    def test_requeue_dlq_unknown_id_does_nothing(self):
        """id 不存在时 requeue_dlq 返回 False，且不改动 pending / dlq。"""
        self.wal.append(["m v=1 1"], "t")
        self.wal.to_dlq_raw("bad", "t", "reason")
        pending_before = self.wal.pending_count()
        dlq_before = self.wal.stats()["dlq"]

        self.assertFalse(self.wal.requeue_dlq(999999), "不存在的 id 应返回 False")
        self.assertEqual(self.wal.pending_count(), pending_before, "不应凭空产生 pending 行")
        self.assertEqual(self.wal.stats()["dlq"], dlq_before, "DLQ 里的其他行不应被误删")
        self.assertEqual(self.wal.dlq_rows()[0][1], "bad", "原死信应原样保留")

        # 删掉之后再重放同一个 id：同样返回 False，不会 resurrect 已删除的行
        dead_id = self.wal.dlq_rows()[0][0]
        self.assertTrue(self.wal.drop_dlq(dead_id))
        self.assertFalse(self.wal.requeue_dlq(dead_id), "已删除的 id 再重放应返回 False")
        self.assertEqual(self.wal.pending_count(), pending_before)

    def test_drop_dlq_removes_the_row(self):
        """drop_dlq：删除该行并返回 True；重复 drop / 不存在的 id 返回 False；不影响 pending。"""
        self.wal.append(["m,h=a v=1 1"], "sensors/a")
        self.wal.to_dlq_raw("bad-1", "t1", "reason-1")
        self.wal.to_dlq_raw("bad-2", "t2", "reason-2")
        pending_before = self.wal.pending_count()
        rows = self.wal.dlq_rows()
        self.assertEqual([r[1] for r in rows], ["bad-2", "bad-1"])
        target, keep = rows[0][0], rows[1][0]

        self.assertTrue(self.wal.drop_dlq(target), "删除存在的行应返回 True")
        self.assertEqual(self.wal.stats()["dlq"], 1, "2 条死信删 1 条还剩 1 条")
        self.assertEqual([r[0] for r in self.wal.dlq_rows()], [keep], "只应删掉指定的那条")
        self.assertEqual(self.wal.pending_count(), pending_before, "drop 不应把行放回 pending")

        self.assertFalse(self.wal.drop_dlq(target), "重复 drop 同一 id 应返回 False")
        self.assertFalse(self.wal.drop_dlq(999999), "不存在的 id 应返回 False")
        self.assertEqual(self.wal.stats()["dlq"], 1, "失败的 drop 不应删掉别的行")

        self.assertTrue(self.wal.drop_dlq(keep))
        self.assertEqual(self.wal.dlq_rows(), [])
        self.assertEqual(self.wal.stats()["dlq"], 0)
        self.assertEqual(self.wal.pending_count(), pending_before, "DLQ 清空后 pending 不受影响")

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
