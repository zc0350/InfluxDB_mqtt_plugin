from __future__ import annotations

import unittest

from mqtt_influx_bridge.config import MqttConfig

try:
    import paho.mqtt.client  # noqa: F401
    HAS_PAHO = True
except ImportError:
    HAS_PAHO = False


@unittest.skipUnless(HAS_PAHO, "未安装 paho-mqtt")
class TestPahoCompat(unittest.TestCase):
    def test_factory_creates_manual_ack_client(self):
        from mqtt_influx_bridge.bridge import _paho_client_factory

        cfg = MqttConfig(client_id="compat-test", persistent_session=True, topics=["t/#"])
        client = _paho_client_factory(cfg, lambda *a: None, lambda *a: None, lambda *a: None)
        self.assertTrue(callable(client.ack))
        self.assertTrue(callable(client.connect_async))
        self.assertTrue(bool(getattr(client, "_manual_ack", False)))

    def test_callbacks_registered(self):
        from mqtt_influx_bridge.bridge import _paho_client_factory

        cfg = MqttConfig(client_id="compat-test-2")
        client = _paho_client_factory(cfg, lambda *a: None, lambda *a: None, lambda *a: None)
        self.assertIsNotNone(client.on_connect)
        self.assertIsNotNone(client.on_message)


if __name__ == "__main__":
    unittest.main()
