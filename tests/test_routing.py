"""topic 路由 + payload 映射 -> 按年/按月分表的 line protocol。"""
from __future__ import annotations

import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mqtt_influx_bridge.config import MappingConfig, RouteConfig, load_config
from mqtt_influx_bridge.lineprotocol import ParseError
from mqtt_influx_bridge.routing import parse_payload, resolve_route

STATUS = (
    '{"event":"status","deviceCode":"TT10002","imei":"864925080140265","sigal": 25,'
    '"door_status": 0,"server_id": 1,"version":"mini_esp32_127","cam":"ready","sd":"ready",'
    '"time":1793936202}'
)
ALARM = '{"event":"alarms","deviceCode":"TT10002","door_num":[1],"code": 1,"time":1793936103}'
CONTROL = '{"action":"configure","deviceCode":"bbbbbb","time":1780000000,"uuid":"aabbbcc"}'
REPLY = '{"action":"reboot","deviceCode":"bbbbbb","code":1,"time":1780000000,"uuid":"aabbbcc"}'

UP = "TT1000/TT10002/data"
CTRL = "TT1000/bbbbbb/control"
RSP = "TT1000/bbbbbb/response/aabbbcc"


def mapping(**kw) -> MappingConfig:
    base = dict(year_tz_offset_minutes=480, substitutions={"productKey": "TT1000"})
    base.update(kw)
    return MappingConfig(**base)


def uplink(**kw) -> RouteConfig:
    base = dict(
        name="uplink",
        topics=["${productKey}/+/data"],
        measurement="mqtt_data",
        timestamp_path="time",
        tags={"event": "event", "devicecode": "deviceCode", "imei": "imei",
              "serverid": "server_id", "version": "version"},
        static_tags={"source": "mqtt"},
    )
    base.update(kw)
    return RouteConfig(**base)


def command_route(name="control", topic="${productKey}/+/control", direction="downlink") -> RouteConfig:
    return RouteConfig(
        name=name, topics=[topic], measurement="device_control", timestamp_path="time",
        tags={"event": "action", "devicecode": "deviceCode"},
        static_tags={"direction": direction},
    )


def lines(payload, routes, topic=UP, mp=None):
    return parse_payload(payload, routes, mp or mapping(), topic)


class TestRouting(unittest.TestCase):
    def test_first_matching_route_wins(self):
        routes = [command_route(), uplink()]
        self.assertEqual(resolve_route(routes, {"productKey": "TT1000"}, CTRL).name, "control")
        self.assertEqual(resolve_route(routes, {"productKey": "TT1000"}, UP).name, "uplink")

    def test_unmatched_topic_is_parse_error(self):
        with self.assertRaises(ParseError) as ctx:
            lines(STATUS, [uplink()], "OTHER/TT10002/data")
        self.assertIn("没有 [[route]] 匹配", str(ctx.exception))

    def test_placeholder_without_value_matches_any_product(self):
        mp = mapping(substitutions={})
        route = resolve_route([command_route()], mp.substitutions, CTRL)
        self.assertEqual(route.name, "control")


class TestAutoMapping(unittest.TestCase):
    def test_status_golden_line(self):
        self.assertEqual(
            lines(STATUS, [uplink()]),
            [
                'mqtt_data_2026,source=mqtt,event=status,devicecode=TT10002,'
                'imei=864925080140265,serverid=1,version=mini_esp32_127 '
                'sigal=25.0,door_status=0.0,cam="ready",sd="ready",'
                'data="{\\"event\\":\\"status\\",\\"deviceCode\\":\\"TT10002\\",'
                '\\"imei\\":\\"864925080140265\\",\\"sigal\\":25,\\"door_status\\":0,'
                '\\"server_id\\":1,\\"version\\":\\"mini_esp32_127\\",\\"cam\\":\\"ready\\",'
                '\\"sd\\":\\"ready\\",\\"time\\":1793936202}",'
                'topic="TT1000/TT10002/data" 1793936202000000000'
            ],
        )

    def test_alarm_has_no_absent_tags_and_json_array_becomes_string(self):
        line = lines(ALARM, [uplink()])[0]
        head = line.split(" ", 1)[0]
        self.assertTrue(head.startswith("mqtt_data_2026,source=mqtt,event=alarms,devicecode=TT10002"))
        self.assertNotIn("imei=", head)          # 缺失的 tag 不写成空值，避免多余 series
        self.assertIn('door_num="[1]"', line)
        self.assertIn("code=1.0", line)
        self.assertTrue(line.endswith(" 1793936103000000000"))

    def test_control_and_reply_share_one_table_separated_by_direction(self):
        routes = [command_route(), command_route("response", "${productKey}/+/response/+", "uplink")]
        down = lines(CONTROL, routes, CTRL)[0]
        up = lines(REPLY, routes, RSP)[0]
        self.assertTrue(down.startswith("device_control_2026,direction=downlink,event=configure,devicecode=bbbbbb"))
        self.assertTrue(up.startswith("device_control_2026,direction=uplink,event=reboot,devicecode=bbbbbb"))
        self.assertIn('uuid="aabbbcc"', down)
        self.assertIn('uuid="aabbbcc"', up)
        self.assertIn("code=1.0", up)

    def test_key_case_exclude_keys_and_string_type_policy(self):
        payload = '{"time":1793936202,"deviceCode":"d1","batteryPct":88,"cam":"ready"}'
        tags = {"devicecode": "deviceCode"}
        keep = lines(payload, [uplink(tags=tags, key_case="keep")])[0]
        self.assertIn("batteryPct=88.0", keep)   # key_case 只影响自动展开的 key，显式列名原样保留
        self.assertIn("devicecode=d1", keep)
        self.assertIn('batteryPct="88"', lines(payload, [uplink(tags=tags, key_case="keep", type_policy="string")])[0])
        self.assertNotIn("cam=", lines(payload, [uplink(tags=tags, exclude_keys=["cam"])])[0])

    def test_reserved_columns_win_over_payload_keys(self):
        payload = '{"time":1793936202,"data":"mine","topic":"fake","temp":1.5}'
        line = lines(payload, [uplink(tags={})])[0]
        self.assertIn('topic="TT1000/TT10002/data"', line)   # payload 的 topic 让位给真实 topic
        self.assertNotIn('"mine"', line)                     # payload 的 data 不覆盖 blob
        self.assertEqual(line.count("data="), 1)
        self.assertIn("temp=1.5", line)

    def test_explicit_mode_drops_unlisted_keys(self):
        route = uplink(mode="explicit", fields={"signal": "sigal:float", "nested": "a.b"}, blob_field="")
        payload = '{"time":1793936202,"sigal":20,"a":{"b":7},"extra":"x"}'
        line = lines(payload, [route])[0]
        self.assertIn("signal=20.0", line)
        self.assertIn("nested=7i", line)
        self.assertNotIn("extra", line)

    def test_dynamic_measurement_still_gets_year_suffix(self):
        route = uplink(measurement="unused", measurement_path="event")
        self.assertTrue(lines(ALARM, [route])[0].startswith("alarms_2026,"))

    def test_json_array_payload(self):
        rows = lines('[{"time":1793936202,"v":1},{"time":1793936203,"v":2}]', [uplink(tags={})])
        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[1].endswith(" 1793936203000000000"))

    def test_bad_payloads_raise(self):
        for bad in ("not json", "   ", b"\xff\xfe"):
            with self.assertRaises(ParseError):
                lines(bad, [uplink()])


class TestYearTable(unittest.TestCase):
    def _epoch(self, iso: str) -> int:
        return int(datetime.fromisoformat(iso).timestamp())

    def test_year_boundary_uses_configured_timezone(self):
        # 2025-12-31T16:00:00Z 正好是 UTC+8 的 2026-01-01 00:00:00
        ts = self._epoch("2025-12-31T16:00:00+00:00")
        routes = [uplink(tags={})]
        east = mapping(year_tz_offset_minutes=480)
        utc = mapping(year_tz_offset_minutes=0)
        self.assertTrue(lines(f'{{"time":{ts},"v":1}}', routes, mp=east)[0].startswith("mqtt_data_2026,"))
        self.assertTrue(lines(f'{{"time":{ts},"v":1}}', routes, mp=utc)[0].startswith("mqtt_data_2025,"))
        self.assertTrue(lines(f'{{"time":{ts - 1},"v":1}}', routes, mp=east)[0].startswith("mqtt_data_2025,"))

    def test_year_table_disabled_keeps_plain_measurement(self):
        self.assertTrue(lines(STATUS, [uplink(year_table=False)])[0].startswith("mqtt_data,source=mqtt"))

    def test_missing_timestamp_path_falls_back_to_receive_time(self):
        current_year = datetime.now(timezone.utc).year
        line = lines('{"v":1,"event":"status"}', [uplink(timestamp_path="")])[0]
        self.assertIn(f"mqtt_data_{current_year},", line)


class TestTablePeriod(unittest.TestCase):
    def _ts(self, iso: str) -> int:
        return int(datetime.fromisoformat(iso).timestamp())

    def month_routes(self) -> list[RouteConfig]:
        return [uplink(tags={}, table_period="month")]

    def test_month_suffix_and_rollover_into_january(self):
        mp = mapping(year_tz_offset_minutes=480)
        dec = f'{{"time":{self._ts("2026-12-31T23:30:00+08:00")},"v":1}}'
        jan = f'{{"time":{self._ts("2027-01-01T00:30:00+08:00")},"v":1}}'
        self.assertTrue(lines(dec, self.month_routes(), mp=mp)[0].startswith("mqtt_data_202612,"))
        self.assertTrue(lines(jan, self.month_routes(), mp=mp)[0].startswith("mqtt_data_202701,"))

    def test_month_boundary_follows_configured_timezone(self):
        # 同一个瞬间：UTC+8 已跨月，UTC 还在当月
        ts = f'{{"time":{self._ts("2026-11-30T16:30:00+00:00")},"v":1}}'
        routes = self.month_routes()
        self.assertTrue(lines(ts, routes, mp=mapping(year_tz_offset_minutes=480))[0].startswith("mqtt_data_202612,"))
        self.assertTrue(lines(ts, routes, mp=mapping(year_tz_offset_minutes=0))[0].startswith("mqtt_data_202611,"))

    def test_year_period_is_the_default(self):
        ts = f'{{"time":{self._ts("2026-12-31T23:30:00+08:00")},"v":1}}'
        self.assertTrue(lines(ts, [uplink(tags={})], mp=mapping(year_tz_offset_minutes=480))[0].startswith("mqtt_data_2026,"))


class TestTimestampUnitsAndFallback(unittest.TestCase):
    """多固件版本共存：同一份配置吃秒 / 毫秒 / 微秒 / 纳秒，空时间回落接收时刻。"""

    def _ts(self, line: str) -> int:
        return int(line.rsplit(" ", 1)[1])

    def test_all_units_land_on_the_same_instant_and_table(self):
        routes = [uplink(tags={}, table_period="month")]
        rows = [lines(p, routes)[0] for p in (
            '{"time":1793936202,"v":1}',
            '{"time":"1793936202000","v":1}',
            '{"time":1793936202000000,"v":1}',
            '{"time":1793936202000000000,"v":1}',
        )]
        self.assertEqual([self._ts(r) for r in rows], [1_793_936_202_000_000_000] * 4)
        self.assertEqual({r.split(",")[0] for r in rows}, {"mqtt_data_202611"})

    def test_empty_or_missing_time_uses_receive_time(self):
        routes = [uplink(tags={}, table_period="month")]
        table = "mqtt_data_" + datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m")
        before = time.time_ns()
        for payload in ('{"v":1}', '{"v":1,"time":null}', '{"v":1,"time":""}', '{"v":1,"time":"  "}'):
            line = lines(payload, routes)[0]
            self.assertTrue(line.startswith(f"{table},"), line)
            self.assertLess(abs(self._ts(line) - before), 5_000_000_000)

    def test_garbage_time_still_raises(self):
        with self.assertRaises(ParseError):
            lines('{"v":1,"time":"abc"}', [uplink(tags={})])

    def test_zero_and_negative_time_use_receive_time(self):
        routes = [uplink(tags={}, table_period="month")]
        table = "mqtt_data_" + datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m")
        before = time.time_ns()
        for payload in ('{"time":0,"v":1}', '{"time":"0","v":1}', '{"time":0.0,"v":1}', '{"time":-1,"v":1}'):
            line = lines(payload, routes)[0]
            self.assertTrue(line.startswith(f"{table},"), line)
            self.assertLess(abs(self._ts(line) - before), 5_000_000_000)


class TestTT1000ShippedConfig(unittest.TestCase):
    """config.tt1000.toml 的列集合就是上线后的查询面，改那份配置必须同步这里。"""

    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "config.tt1000.toml"
        cls.cfg, _ = load_config(path)

    def line(self, payload: str, topic: str = UP) -> str:
        return parse_payload(payload, self.cfg.routes, self.cfg.mapping, topic)[0]

    def test_status_writes_the_declared_columns_only(self):
        self.assertEqual(
            self.line(STATUS),
            'mqtt_data_202611,event=status,devicecode=TT10002,imei=864925080140265 '
            'data="{\\"event\\":\\"status\\",\\"deviceCode\\":\\"TT10002\\",'
            '\\"imei\\":\\"864925080140265\\",\\"sigal\\":25,\\"door_status\\":0,'
            '\\"server_id\\":1,\\"version\\":\\"mini_esp32_127\\",\\"cam\\":\\"ready\\",'
            '\\"sd\\":\\"ready\\",\\"time\\":1793936202}",'
            'topic="TT1000/TT10002/data" 1793936202000000000',
        )

    def test_dropped_keys_survive_only_in_the_blob(self):
        head = self.line(STATUS).split(" ", 1)[0]
        for gone in ("sigal", "door_status", "serverid", "version", "source", "cam", "sd"):
            self.assertNotIn(f"{gone}=", head)
        self.assertIn('\\"cam\\":\\"ready\\"', self.line(STATUS))
        self.assertIn('\\"sigal\\":25', self.line(STATUS))

    def test_alarm_and_status_share_one_month_table(self):
        self.assertTrue(self.line(ALARM).startswith("mqtt_data_202611,event=alarms,devicecode=TT10002 "))
        self.assertIn('code=1.0', self.line(ALARM))
        self.assertIn('door_num="[1]"', self.line(ALARM))

    def test_control_pair_keeps_direction_tag(self):
        down = self.line(CONTROL, CTRL)
        up = self.line(REPLY, RSP)
        # CONTROL/RSP 的 time=1780000000 落在 2026-05，表后缀跟着月份走
        self.assertTrue(down.startswith("device_control_202605,direction=downlink,event=configure,devicecode=bbbbbb "))
        self.assertTrue(up.startswith("device_control_202605,direction=uplink,event=reboot,devicecode=bbbbbb "))
        self.assertIn('uuid="aabbbcc"', down)
        self.assertIn("code=1.0", up)

    def test_fallback_route_archives_unmatched_topic(self):
        line = self.line('{"deviceCode":"UNMATCH1","time":1793936202}', "OTHERPRODUCT/UNMATCH1/telemetry")
        self.assertTrue(line.startswith("mqtt_unknown_202611,matched=false,devicecode=UNMATCH1 "))
        self.assertIn('topic="OTHERPRODUCT/UNMATCH1/telemetry"', line)


if __name__ == "__main__":
    unittest.main()
