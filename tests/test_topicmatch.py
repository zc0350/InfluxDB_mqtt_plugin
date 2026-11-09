from __future__ import annotations

import unittest

from mqtt_influx_bridge.topicmatch import expand_topic, expand_topics, topic_matches, valid_topic_pattern


class TestTopicMatches(unittest.TestCase):
    def test_exact_and_single_level(self):
        self.assertTrue(topic_matches("TT1000/TT10002/control", "TT1000/TT10002/control"))
        self.assertTrue(topic_matches("TT1000/+/control", "TT1000/TT10002/control"))
        self.assertFalse(topic_matches("TT1000/+/control", "TT1000/TT10002/control/extra"))
        self.assertFalse(topic_matches("TT1000/+/data", "TT1000/TT10002/control"))

    def test_multi_level_wildcard(self):
        self.assertTrue(topic_matches("TT1000/#", "TT1000/TT10002/control"))
        self.assertTrue(topic_matches("TT1000/#", "TT1000"))
        self.assertTrue(topic_matches("#", "anything/at/all"))
        self.assertFalse(topic_matches("TT1000/#", "OTHER/TT10002/control"))
        self.assertTrue(topic_matches("+/+/response/+", "TT1000/TT10002/response/aabbbcc"))
        self.assertFalse(topic_matches("+/+/response/+", "TT1000/TT10002/response"))

    def test_empty_level(self):
        # MQTT 允许空分段：a/+/c 匹配 a//c
        self.assertTrue(topic_matches("a/+/c", "a//c"))


class TestPlaceholderExpansion(unittest.TestCase):
    def test_substitution_and_default_wildcard(self):
        subs = {"productKey": "TT1000"}
        self.assertEqual(expand_topic("${productKey}/+/control", subs), "TT1000/+/control")
        # 未提供的占位符按单层通配处理，避免误配成字面量
        self.assertEqual(expand_topic("${productKey}/${deviceCode}/data", subs), "TT1000/+/data")
        self.assertEqual(expand_topics(["${productKey}/#/x"], {}), ["+/#/x"])

    def test_valid_pattern(self):
        self.assertTrue(valid_topic_pattern("a/+/c"))
        self.assertTrue(valid_topic_pattern("a/b/#"))
        self.assertFalse(valid_topic_pattern("a/#/c"))
        self.assertFalse(valid_topic_pattern("a/b#"))
        self.assertFalse(valid_topic_pattern("a/c+"))


if __name__ == "__main__":
    unittest.main()
