from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from mqtt_influx_bridge.config import Config, load_config, validate

TWO_ROUTES = """
[mqtt]
client_id = "t-01"

[[route]]
name = "uplink"
topics = ["${productKey}/+/data"]
measurement = "mqtt_data"
tags = { event = "event", devicecode = "deviceCode" }

[[route]]
name = "control"
topics = ["${productKey}/+/control", "${productKey}/+/response/+"]
measurement = "device_control"
static_tags = { direction = "downlink" }
"""


class RouteConfigTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "config.toml"

    def load(self, text: str) -> tuple[Config, list[str]]:
        self.path.write_text(text, encoding="utf-8")
        return load_config(self.path)

    def base(self) -> Config:
        cfg, _ = self.load(TWO_ROUTES)
        return cfg


class TestRouteParsing(RouteConfigTestCase):
    def test_routes_and_subscription_union_from_routes(self):
        cfg = self.base()
        self.assertEqual([r.name for r in cfg.routes], ["uplink", "control"])
        self.assertEqual(cfg.subscribe_topics, ["+/+/data", "+/+/control", "+/+/response/+"])
        self.assertEqual(cfg.routes[1].measurement, "device_control")
        self.assertEqual(cfg.routes[1].static_tags, {"direction": "downlink"})
        self.assertTrue(cfg.routes[0].year_table)

    def test_explicit_mqtt_topics_win(self):
        text = TWO_ROUTES.replace('client_id = "t-01"', 'client_id = "t-01"\ntopics = ["a/#"]')
        cfg, _ = self.load(text)
        self.assertEqual(cfg.subscribe_topics, ["a/#"])

    def test_product_key_env_overrides_placeholder(self):
        with mock.patch.dict(os.environ, {"PRODUCT_KEY": "TT1000"}):
            cfg = self.base()
        self.assertEqual(cfg.subscribe_topics[0], "TT1000/+/data")

    def test_unknown_key_warns_but_does_not_fail(self):
        cfg, warnings = self.load(TWO_ROUTES + '\n[[route]]\nname = "x"\ntypos = 1\n')
        self.assertTrue(any("未知配置项" in w for w in warnings))
        self.assertEqual(cfg.routes[-1].name, "x")


class TestRouteValidation(RouteConfigTestCase):
    def test_no_routes_raises(self):
        with self.assertRaises(ValueError):
            validate(Config())

    def test_duplicate_route_name_raises(self):
        cfg = self.base()
        cfg.routes[1].name = "uplink"
        with self.assertRaises(ValueError):
            validate(cfg)

    def test_bad_wildcard_raises(self):
        cfg = self.base()
        cfg.routes[0].topics = ["a/#/b"]
        with self.assertRaises(ValueError):
            validate(cfg)

    def test_explicit_without_any_column_raises(self):
        cfg = self.base()
        cfg.routes[0].mode = "explicit"
        cfg.routes[0].blob_field = ""
        cfg.routes[0].topic_field = ""
        with self.assertRaises(ValueError):
            validate(cfg)

    def test_explicit_with_only_blob_is_allowed(self):
        """显式模式可以只存 blob（兜底表就这样配），不再强制要求 fields。"""
        cfg = self.base()
        cfg.routes[0].mode = "explicit"
        self.assertFalse(any("mode=explicit" in w for w in validate(cfg)))

    def test_bad_table_period_raises(self):
        cfg = self.base()
        cfg.routes[0].table_period = "day"
        with self.assertRaises(ValueError):
            validate(cfg)

    def test_month_period_without_year_table_warns(self):
        cfg = self.base()
        cfg.routes[0].year_table = False
        cfg.routes[0].table_period = "month"
        self.assertTrue(any("table_period=month 需要 year_table" in w for w in validate(cfg)))

    def test_year_tables_summarised_once(self):
        warnings = [w for w in validate(self.base()) if "逐表 delete table" in w]
        self.assertEqual(len(warnings), 1)
        self.assertIn("mqtt_data_YYYY", warnings[0])
        self.assertIn("device_control_YYYY", warnings[0])

    def test_month_tables_report_twelve_tables_per_year(self):
        cfg = self.base()
        for r in cfg.routes:
            r.table_period = "month"
        warnings = [w for w in validate(cfg) if "逐表 delete table" in w]
        self.assertEqual(len(warnings), 1)
        self.assertIn("mqtt_data_YYYYMM", warnings[0])
        self.assertIn("约 24 张表/年", warnings[0])

    def test_missing_timestamp_path_warns(self):
        cfg = self.base()
        cfg.routes[0].timestamp_path = ""
        self.assertTrue(any("重投会产生重复点" in w for w in validate(cfg)))

    def test_too_many_tags_warns(self):
        cfg = self.base()
        cfg.routes[0].tags = {f"k{i}": f"v{i}" for i in range(6)}
        self.assertTrue(any("series 基数" in w for w in validate(cfg)))

    def test_mapped_columns_cover_tags_blob_and_topic(self):
        route = self.base().routes[0]
        self.assertEqual(route.mapped_columns, {"event", "devicecode", "data", "topic"})


if __name__ == "__main__":
    unittest.main()
