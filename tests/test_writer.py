from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mqtt_influx_bridge.config import InfluxConfig, WriterConfig
from mqtt_influx_bridge.wal import Wal
from mqtt_influx_bridge.writer import InfluxWriter, PermanentError, TransientError, flush_once

from tests._http_server import MockState, start_mock_server


class TestEndpoints(unittest.TestCase):
    def test_v3_endpoint_and_headers(self):
        w = InfluxWriter(InfluxConfig(version="v3", url="http://127.0.0.1:8181", database="sensors", token="tok"))
        self.assertIn("/api/v3/write_lp?", w.endpoint())
        for part in ("db=sensors", "precision=nanosecond", "no_sync=false", "accept_partial=false"):
            self.assertIn(part, w.endpoint())
        self.assertEqual(w.headers()["Authorization"], "Bearer tok")
        self.assertEqual(w.headers()["Content-Encoding"], "gzip")

    def test_v2_and_v1_endpoints(self):
        w2 = InfluxWriter(InfluxConfig(version="v2", url="http://h:8086", database="bkt", org="acme", token="t"))
        self.assertIn("/api/v2/write?", w2.endpoint())
        self.assertIn("bucket=bkt", w2.endpoint())
        self.assertIn("org=acme", w2.endpoint())
        self.assertEqual(w2.headers()["Authorization"], "Token t")
        w1 = InfluxWriter(InfluxConfig(version="v1", url="http://h:8086", database="db", username="u", password="p"))
        self.assertTrue(w1.endpoint().startswith("/write?db=db"))
        self.assertTrue(w1.headers()["Authorization"].startswith("Basic "))


class TestHttpWrite(unittest.TestCase):
    def setUp(self):
        self.state = MockState()
        self.srv = start_mock_server(self.state)
        self.port = self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def _writer(self, **kw) -> InfluxWriter:
        cfg = InfluxConfig(version="v3", url=f"http://127.0.0.1:{self.port}", database="sensors", token="tok")
        for k, v in kw.items():
            setattr(cfg, k, v)
        return InfluxWriter(cfg)

    def test_gzip_body_and_path(self):
        w = self._writer()
        w.write(["m,h=a v=1 1", "m,h=b v=2 2"])
        req = self.state.requests[0]
        self.assertIn("db=sensors", req["path"])
        self.assertEqual(req["headers"]["Authorization"], "Bearer tok")
        self.assertEqual(req["body"], "m,h=a v=1 1\nm,h=b v=2 2")
        w.close()

    def test_status_classification(self):
        w = self._writer()
        self.state.status = 500
        with self.assertRaises(TransientError):
            w.write(["m v=1 1"])
        self.state.status = 429
        with self.assertRaises(TransientError):
            w.write(["m v=1 1"])
        self.state.status = 400
        with self.assertRaises(PermanentError):
            w.write(["m v=1 1"])
        w.close()


class TestFlushOnce(unittest.TestCase):
    def setUp(self):
        self.state = MockState()
        self.srv = start_mock_server(self.state)
        self.tmp = tempfile.TemporaryDirectory()
        self.wal = Wal(Path(self.tmp.name) / "q.db")
        self.cfg = InfluxConfig(
            version="v3", url=f"http://127.0.0.1:{self.srv.server_address[1]}",
            database="sensors", gzip=False,
        )
        self.writer = InfluxWriter(self.cfg)
        self.wcfg = WriterConfig(batch_size=10, retry_base_ms=0, retry_max_ms=0, max_retries=0)

    def tearDown(self):
        self.writer.close()
        self.wal.close()
        self.tmp.cleanup()
        self.srv.shutdown()
        self.srv.server_close()

    def test_success_removes_rows(self):
        self.wal.append(["m v=1 1"], "t")
        outcomes = []
        self.assertEqual(flush_once(self.wal, self.writer, self.wcfg, lambda o, i: outcomes.append(o)), 1)
        self.assertEqual(outcomes, ["ok"])
        self.assertEqual(self.wal.pending_count(), 0)

    def test_transient_failure_keeps_rows_then_succeeds(self):
        self.wal.append(["m v=1 1"], "t")
        self.state.sequence = [500]
        outcomes = []
        flush_once(self.wal, self.writer, self.wcfg, lambda o, i: outcomes.append(o))
        self.assertEqual(outcomes, ["retry"])
        self.assertEqual(self.wal.pending_count(), 1)
        self.assertEqual(self.wal.fetch(10)[0].attempts, 1)
        flush_once(self.wal, self.writer, self.wcfg, lambda o, i: outcomes.append(o))
        self.assertEqual(outcomes[-1], "ok")
        self.assertEqual(self.wal.pending_count(), 0)

    def test_permanent_failure_goes_to_dlq(self):
        self.wal.append(["m v=1 1"], "t")
        self.state.status = 400
        outcomes = []
        flush_once(self.wal, self.writer, self.wcfg, lambda o, i: outcomes.append(o))
        self.assertEqual(outcomes, ["dlq"])
        self.assertEqual(self.wal.pending_count(), 0)
        self.assertEqual(self.wal.stats()["dlq"], 1)

    def test_retry_limit_goes_to_dlq(self):
        self.wcfg.max_retries = 2
        self.state.status = 503
        self.wal.append(["m v=1 1"], "t")
        flush_once(self.wal, self.writer, self.wcfg)
        flush_once(self.wal, self.writer, self.wcfg)
        self.assertEqual(self.wal.pending_count(), 0)
        self.assertEqual(self.wal.stats()["dlq"], 1)


if __name__ == "__main__":
    unittest.main()
