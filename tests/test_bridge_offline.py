from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from mqtt_influx_bridge.bridge import Bridge
from mqtt_influx_bridge.config import Config, FormatConfig, InfluxConfig, MqttConfig, WalConfig, WriterConfig
from mqtt_influx_bridge.writer import flush_once
from tests._http_server import MockState, start_mock_server


@dataclass
class FakeMsg:
    topic: str
    payload: bytes
    mid: int = 1
    qos: int = 1


class FakeClient:
    def __init__(self):
        self.acks: list[tuple[int, int]] = []
        self.subscriptions: list[tuple[str, int]] = []

    def ack(self, mid, qos):
        self.acks.append((mid, qos))

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))

    def connect_async(self, *a, **kw):
        pass

    def loop_start(self):
        pass

    def disconnect(self):
        pass

    def loop_stop(self):
        pass


PAYLOAD = b'{"ts": 1638360000000, "device_id": "dev-01", "temp": 22.5, "seq": 7}'


class TestBridgeOffline(unittest.TestCase):
    def setUp(self):
        self.state = MockState()
        self.srv = start_mock_server(self.state)
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = Config(
            mqtt=MqttConfig(topics=["sensors/+/telemetry"], qos=1, client_id="offline-test"),
            influx=InfluxConfig(
                version="v3", url=f"http://127.0.0.1:{self.srv.server_address[1]}",
                database="sensors", gzip=False,
            ),
            format=FormatConfig(
                type="json", measurement="sensor_data", timestamp_path="ts", timestamp_unit="ms",
                static_tags={"source": "mqtt"}, tags={"device": "device_id"},
                fields={"temperature": "temp:float", "seq": "seq:int"},
            ),
            wal=WalConfig(path=str(Path(self.tmp.name) / "q.db")),
            writer=WriterConfig(batch_size=10, ack_mode="persisted", retry_base_ms=0, retry_max_ms=0),
        )
        self.fake = FakeClient()
        self.bridge = Bridge(self.cfg, client_factory=lambda *a: self.fake)

    def tearDown(self):
        self.bridge.writer.close()
        if not self.bridge._closed:
            self.bridge.wal.close()
            self.bridge._closed = True
        self.tmp.cleanup()
        self.srv.shutdown()
        self.srv.server_close()

    def _flush(self):
        return flush_once(self.bridge.wal, self.bridge.writer, self.cfg.writer, self.bridge._on_batch_result)

    def test_persisted_acks_after_local_commit(self):
        self.bridge.on_message(self.fake, None, FakeMsg("sensors/dev-01/telemetry", PAYLOAD))
        self.assertEqual(self.fake.acks, [(1, 1)])       # 落盘即确认
        self.assertEqual(self.bridge.wal.pending_count(), 1)
        self.assertEqual(self._flush(), 1)
        self.assertEqual(self.bridge.wal.pending_count(), 0)
        self.assertEqual(len(self.state.requests), 1)
        self.assertEqual(
            self.state.requests[0]["body"],
            "sensor_data,source=mqtt,device=dev-01 temperature=22.5,seq=7i 1638360000000000000",
        )

    def test_written_acks_only_after_influxdb_confirms(self):
        self.cfg.writer.ack_mode = "written"
        self.bridge._client = self.fake
        self.bridge.on_message(self.fake, None, FakeMsg("sensors/dev-01/telemetry", PAYLOAD))
        self.assertEqual(self.fake.acks, [])             # 尚未确认
        self.bridge._settle_acks()
        self.assertEqual(self.fake.acks, [])             # 未落库仍不确认
        self._flush()
        self.bridge._settle_acks()
        self.assertEqual(self.fake.acks, [(1, 1)])       # 2xx 之后才 PUBACK
        self.assertEqual(self.bridge.wal.pending_count(), 0)

    def test_written_waits_while_influxdb_is_down(self):
        self.cfg.writer.ack_mode = "written"
        self.bridge._client = self.fake
        self.state.status = 503
        self.bridge.on_message(self.fake, None, FakeMsg("sensors/dev-01/telemetry", PAYLOAD))
        self._flush()
        self.bridge._settle_acks()
        self.assertEqual(self.fake.acks, [])
        self.assertEqual(self.bridge.wal.pending_count(), 1)
        self.state.status = 204
        self._flush()
        self.bridge._settle_acks()
        self.assertEqual(self.fake.acks, [(1, 1)])

    def test_permanent_rejection_also_settles_ack(self):
        self.cfg.writer.ack_mode = "written"
        self.bridge._client = self.fake
        self.state.status = 400
        self.bridge.on_message(self.fake, None, FakeMsg("sensors/dev-01/telemetry", PAYLOAD))
        self._flush()
        self.bridge._settle_acks()
        self.assertEqual(self.fake.acks, [(1, 1)])       # 毒药消息进 DLQ，不能卡住 ACK
        self.assertEqual(self.bridge.wal.stats()["dlq"], 1)

    def test_parse_error_goes_to_dlq_and_acks(self):
        self.bridge.on_message(self.fake, None, FakeMsg("sensors/dev-01/telemetry", b"not json"))
        self.assertEqual(self.fake.acks, [(1, 1)])
        self.assertEqual(self.bridge.wal.pending_count(), 0)
        self.assertEqual(self.bridge.wal.stats()["dlq"], 1)
        self.assertEqual(self.bridge.stats.parse_errors, 1)

    def test_high_watermark_applies_backpressure(self):
        self.cfg.wal.max_pending = 1
        self.cfg.writer.ack_timeout_seconds = 0.05
        self.bridge.wal.append(["m v=1 1"], "t")
        self.bridge.on_message(self.fake, None, FakeMsg("sensors/dev-01/telemetry", PAYLOAD))
        self.assertEqual(self.fake.acks, [])             # 超限：不 ACK，等 broker 重投
        self.assertEqual(self.bridge.wal.pending_count(), 1)

    def test_on_connect_subscribes_with_version2_signature(self):
        self.bridge.on_connect(self.fake, None, {}, 0, None)
        self.assertEqual(self.fake.subscriptions, [("sensors/+/telemetry", 1)])


if __name__ == "__main__":
    unittest.main()
