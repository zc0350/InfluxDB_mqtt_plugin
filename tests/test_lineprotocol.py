from __future__ import annotations

import unittest

from mqtt_influx_bridge.lineprotocol import (
    ParseError,
    build_line,
    format_value,
    normalize_line,
    resolve_path,
    split_field_spec,
    timestamp_to_ns,
)


class TestTimestamp(unittest.TestCase):
    def test_units(self):
        self.assertEqual(timestamp_to_ns(1_638_360_000_000, "ms"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000, "s"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000_000_000_000, "ns"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000, "auto"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns(1_638_360_000_000, "auto"), 1_638_360_000_000_000_000)
        self.assertEqual(timestamp_to_ns("2021-12-01T12:00:00Z", "datetime"), 1_638_360_000_000_000_000)

    def test_naive_datetime_treated_as_utc(self):
        self.assertEqual(timestamp_to_ns("2021-12-01T12:00:00", "datetime"), 1_638_360_000_000_000_000)

    def test_bad_timestamp(self):
        with self.assertRaises(ParseError):
            timestamp_to_ns("not-a-number", "ms")

    def test_nanosecond_ints_keep_every_digit(self):
        # int(float()) 时代：1.79e18 附近 float 的最小刻度是 256ns，相邻纳秒会被并成同一个点
        self.assertEqual(timestamp_to_ns(1_794_228_571_529_000_060, "ns"), 1_794_228_571_529_000_060)
        self.assertNotEqual(
            timestamp_to_ns(1_794_228_571_529_000_060, "auto"),
            timestamp_to_ns(1_794_228_571_529_000_061, "auto"),
        )
        self.assertEqual(timestamp_to_ns("1794228571529000060", "auto"), 1_794_228_571_529_000_060)

    def test_sub_second_fractions_survive(self):
        self.assertEqual(timestamp_to_ns(1_638_360_000.5, "s"), 1_638_360_000_500_000_000)
        self.assertEqual(timestamp_to_ns("1638360000.123456", "s"), 1_638_360_000_123_456_000)
        self.assertEqual(timestamp_to_ns("2021-12-01T12:00:00.123456Z", "datetime"), 1_638_360_000_123_456_000)

    def test_bool_and_nan_are_not_timestamps(self):
        for bad in (True, None, "nan", "inf"):
            with self.assertRaises(ParseError):
                timestamp_to_ns(bad, "ms")


class TestPathResolution(unittest.TestCase):
    def test_nested_list_and_topic_placeholder(self):
        obj = {"a": {"b": 7}, "list": [{"v": 1}, {"v": 2}]}
        self.assertEqual(resolve_path("a.b", obj, "t"), 7)
        self.assertEqual(resolve_path("list.1.v", obj, "t"), 2)
        self.assertIsNone(resolve_path("a.missing", obj, "t"))
        self.assertEqual(resolve_path("@topic[2]", obj, "pk/dev/cmd"), "cmd")
        self.assertIsNone(resolve_path("@topic[9]", obj, "pk/dev/cmd"))

    def test_field_spec_split(self):
        self.assertEqual(split_field_spec("temp:float"), ("temp", "float"))
        self.assertEqual(split_field_spec("a.b:int"), ("a.b", "int"))
        self.assertEqual(split_field_spec("temp"), ("temp", "auto"))
        self.assertEqual(split_field_spec("path:unknown"), ("path:unknown", "auto"))


class TestValueFormatting(unittest.TestCase):
    def test_types(self):
        self.assertEqual(format_value(1, "auto"), "1i")
        self.assertEqual(format_value(1.5, "auto"), "1.5")
        self.assertEqual(format_value(True, "auto"), "true")
        self.assertEqual(format_value("dev", "auto"), '"dev"')
        self.assertEqual(format_value(7, "float"), "7.0")
        self.assertEqual(format_value("25", "int"), "25i")
        self.assertEqual(format_value("YES", "bool"), "true")
        self.assertIsNone(format_value(None, "auto"))

    def test_rejects_non_finite_and_unconvertible(self):
        with self.assertRaises(ParseError):
            format_value(float("nan"), "float")
        with self.assertRaises(ParseError):
            format_value("abc", "int")

    def test_string_field_escapes(self):
        self.assertEqual(format_value('a "b" c\\d', "str"), '"a \\"b\\" c\\\\d"')

    def test_array_and_object_stringify_as_compact_json(self):
        # explicit 模式声明的数组列要和 auto 模式同格式（无空格），否则同一列两种写法
        self.assertEqual(format_value([1, 2], "auto"), '"[1,2]"')
        self.assertEqual(format_value({"a": 1}, "str"), '"{\\"a\\":1}"')


class TestBuildLine(unittest.TestCase):
    def test_escaping_and_empty_tags(self):
        line = build_line(
            "my measure,ment",
            {"device": "a b,c", "site": "x=y", "empty": ""},
            {"v": "1.0", 'k ey': '"s"'},
            1000,
        )
        self.assertTrue(line.startswith(r"my\ measure\,ment,device=a\ b\,c,site=x\=y"))
        self.assertNotIn("empty", line)
        self.assertIn(r'k\ ey="s"', line)
        self.assertTrue(line.endswith(" 1000"))


class TestLineProtocolPassthrough(unittest.TestCase):
    def test_appends_missing_timestamp(self):
        self.assertRegex(normalize_line("cpu,host=a usage=1.0", "ms"), r"^cpu,host=a usage=1\.0 \d{19}$")

    def test_normalizes_existing_timestamp(self):
        self.assertEqual(normalize_line("cpu,host=a usage=1.0 1000", "ms"), "cpu,host=a usage=1.0 1000000000")
        self.assertEqual(normalize_line("cpu usage=1.0 1000", "ns"), "cpu usage=1.0 1000")


if __name__ == "__main__":
    unittest.main()
