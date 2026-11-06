from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mqtt_influx_bridge.config import FormatConfig
from mqtt_influx_bridge.lineprotocol import ParseError, parse_payload, timestamp_to_ns


def json_fmt(**kw) -> FormatConfig:
    base = dict(
        type="json",
        measurement="sensor_data",
        timestamp_path="ts",
        timestamp_unit="ms",
        static_tags={"source": "mqtt"},
        tags={"device": "device_id", "site": "location"},
        fields={"temperature": "temp:float", "seq": "seq:int"},
    )
    base.update(kw)
    return FormatConfig(**base)


class TestTimestamp(unittest.TestCase):
    def test_units(self):
        self.assertEqual(timestamp_to_ns(1_638_360_000_000, "ms"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000, "s"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000_000_000_000, "ns"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000, "auto"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns("2021-12-01T12:00:00Z", "datetime"), 1_638_360_000_000_000_000)

    def test_bad_timestamp(self):
        with self.assertRaises(ParseError):
            timestamp_to_ns("not-a-number", "ms")


class TestJsonFormat(unittest.TestCase):
    def test_basic_line(self):
        payload = b'{"ts": 1638360000000, "device_id": "dev-01", "location": "wh", "temp": 22.5, "seq": 7}'
        lines = parse_payload(payload, json_fmt(), "sensors/dev-01/telemetry")
        self.assertEqual(
            lines,
            ["sensor_data,source=mqtt,device=dev-01,site=wh temperature=22.5,seq=7i 1638360000000000000"],
        )

    def test_type_inference_and_nested_path(self):
        fmt = json_fmt(
            timestamp_path="meta.ts",
            fields={"temperature": "meta.temp", "online": "flags.up:bool", "note": "text:str"},
        )
        payload = b'{"meta": {"ts": 1000, "temp": 3.5}, "flags": {"up": true}, "text": "a \\"quoted\\" value"}'
        line = parse_payload(payload, fmt, "t")[0]
        self.assertIn("temperature=3.5", line)
        self.assertIn("online=true", line)
        self.assertIn('note="a \\"quoted\\" value"', line)
        self.assertTrue(line.endswith(" 1000000000"))

    def test_topic_placeholders_and_topic_tags(self):
        fmt = json_fmt(tags={"device": "@topic[1]"}, topic_tags={"room": 2})
        line = parse_payload(b'{"ts": 1000, "temp": 1.0, "seq": 1}', fmt, "sensors/dev-9/room3")[0]
        self.assertIn("device=dev-9", line)
        self.assertIn("room=room3", line)

    def test_json_array_produces_multiple_lines(self):
        payload = b'[{"ts": 1000, "temp": 1.0, "seq": 1}, {"ts": 1001, "temp": 2.0, "seq": 2}]'
        lines = parse_payload(payload, json_fmt(), "sensors/x")
        self.assertEqual(len(lines), 2)
        self.assertTrue(lines[0].endswith(" 1000000000"))
        self.assertTrue(lines[1].endswith(" 1001000000"))

    def test_escaping(self):
        line = parse_payload(
            b'{"ts": 1, "temp": 1.0, "seq": 1, "device_id": "a b,c", "location": "x=y"}',
            json_fmt(measurement="my measure,ment"),
            "t",
        )[0]
        self.assertTrue(line.startswith(r"my\ measure\,ment,"))
        self.assertIn(r"device=a\ b\,c", line)
        self.assertIn(r"site=x\=y", line)

    def test_dynamic_measurement(self):
        line = parse_payload(
            b'{"ts": 1000, "type": "cpu", "temp": 1.0, "seq": 1}',
            json_fmt(measurement_path="type"),
            "t",
        )[0]
        self.assertTrue(line.startswith("cpu,"))

    def test_errors(self):
        with self.assertRaises(ParseError):
            parse_payload(b"not json", json_fmt(), "t")
        with self.assertRaises(ParseError):
            parse_payload(b"   ", json_fmt(), "t")
        with self.assertRaises(ParseError):
            parse_payload(b'{"ts": 1000}', json_fmt(), "t")  # 无字段
        with self.assertRaises(ParseError):
            parse_payload(b"\xff\xfe", json_fmt(), "t")


class TestLineProtocolFormat(unittest.TestCase):
    def test_appends_missing_timestamp(self):
        line = parse_payload(b"cpu,host=a usage=1.0", FormatConfig(type="lineprotocol"), "t")[0]
        self.assertRegex(line, r"^cpu,host=a usage=1\.0 \d{19}$")

    def test_normalizes_existing_timestamp(self):
        fmt = FormatConfig(type="lineprotocol", timestamp_unit="ms")
        line = parse_payload(b"cpu,host=a usage=1.0 1000", fmt, "t")[0]
        self.assertEqual(line, "cpu,host=a usage=1.0 1000000000")

    def test_multiline(self):
        lines = parse_payload(
            b"m,h=a v=1 1000\nm,h=b v=2 1001",
            FormatConfig(type="lineprotocol", timestamp_unit="ms"),
            "t",
        )
        self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
