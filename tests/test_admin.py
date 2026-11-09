"""管理面（建库 / 探活 / 查询 / 删表）在 v1 / v2 / v3 三套 API 上的分派。"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from urllib.parse import unquote_plus

from mqtt_influx_bridge.admin import (
    AdminClient, BackendError, UnsupportedBackend, duration_to_days,
)
from mqtt_influx_bridge.config import (
    Config, InfluxConfig, MqttConfig, RouteConfig, WriterConfig, validate,
)
from tests._http_server import MockState, start_mock_server  # noqa: E402


class AdminCase(unittest.TestCase):
    def setUp(self):
        self.state = MockState()
        self.srv = start_mock_server(self.state)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def client(self, **kw) -> AdminClient:
        base = dict(version="v3", url=self.url, database="mqtt", org="acme",
                    token="tok", username="u", password="p")
        base.update(kw)
        return AdminClient(InfluxConfig(**base))

    def last(self) -> dict:
        return self.state.requests[-1]

    @staticmethod
    def v1_rows(*names: str) -> str:
        """v1 /query 的 SHOW 响应形状。"""
        return json.dumps({"results": [{"series": [{"values": [[n] for n in names]}]}]})

    def last_q(self) -> str:
        """v1 的 /query 请求体是 form 编码，还原成可读语句再断言。"""
        return unquote_plus(self.last()["body"])


class TestPing(AdminCase):
    def test_ping_paths_per_version(self):
        for version, want in (("v1", "/ping"), ("v2", "/health"), ("v3", "/health")):
            self.state.requests.clear()
            ok, _ = self.client(version=version).ping()
            self.assertTrue(ok)
            self.assertEqual(self.last()["path"], want)
            self.assertEqual(self.last()["method"], "GET")

    def test_ping_reports_unreachable_without_raising(self):
        dead = AdminClient(InfluxConfig(url="http://127.0.0.1:1"))
        ok, note = dead.ping()
        self.assertFalse(ok)
        self.assertIn("连不上", note)


class TestEnsureDatabase(AdminCase):
    def test_v3_posts_configure_database(self):
        state, note = self.client().ensure_database()
        self.assertEqual(state, "created")
        req = self.last()
        self.assertEqual(req["method"], "POST")
        self.assertTrue(req["path"].startswith("/api/v3/configure/database"))
        self.assertEqual(json.loads(req["body"]), {"db": "mqtt"})
        self.assertEqual(req["headers"]["Authorization"], "Bearer tok")

    def test_v3_existing_database_is_not_an_error(self):
        self.state.route("POST", "/api/v3/configure/database",
                         '{"error":"Database already exists"}', status=400)
        self.assertEqual(self.client().ensure_database()[0], "exists")

    def test_v3_retention_period_goes_into_the_body(self):
        self.client().ensure_database(retention="30d")
        self.assertEqual(json.loads(self.last()["body"]),
                         {"db": "mqtt", "retention_period": "30d"})

    def test_v1_creates_only_when_missing(self):
        self.state.route("POST", "/query", self.v1_rows("db1", "mqtt"))
        c = self.client(version="v1")
        self.assertEqual(c.ensure_database()[0], "exists")
        self.assertIn("SHOW DATABASES", self.last_q())
        self.assertNotIn("db=mqtt", self.last()["path"])   # 查库列表时不能带还不存在的 db=

        self.state.requests.clear()
        self.state.route("POST", "/query", self.v1_rows("db1"))
        state, note = c.ensure_database(retention="30d")
        self.assertEqual(state, "created")
        self.assertIn("retention policy", note)
        self.assertIn("CREATE DATABASE", self.last_q())

    def test_v1_error_inside_200_body_raises(self):
        self.state.route("POST", "/query",
                         '{"results": [{"error": "authorization failed"}]}')
        with self.assertRaises(BackendError):
            self.client(version="v1").ensure_database()

    def test_v2_checks_bucket_then_creates_with_retention(self):
        self.state.route("GET", "/api/v2/buckets", '{"buckets":[]}')
        state, note = self.client(version="v2").ensure_database(retention="4w")
        self.assertEqual(state, "created")
        body = json.loads(self.last()["body"])
        self.assertEqual(body["name"], "mqtt")
        self.assertEqual(body["retentionRules"], [{"type": "expire", "everySeconds": 2419200}])
        self.assertIn("org=acme", self.last()["path"])
        self.assertEqual(self.last()["headers"]["Authorization"], "Token tok")
        # 先查后建，两次请求
        self.assertEqual(len(self.state.matched("GET")), 1)

    def test_v2_existing_bucket_short_circuits(self):
        self.state.route("GET", "/api/v2/buckets", '{"buckets":[{"name":"mqtt"}]}')
        self.assertEqual(self.client(version="v2").ensure_database()[0], "exists")
        self.assertEqual(self.state.matched("POST"), [])

    def test_v2_without_org_is_refused(self):
        with self.assertRaises(ValueError):
            self.client(version="v2", org="").ensure_database()

    def test_illegal_database_name_never_reaches_the_server(self):
        for bad in ("mqtt; DROP MEASUREMENT x", "mqtt db", ""):
            with self.assertRaises(ValueError):
                self.client(database=bad).ensure_database()
        self.assertEqual(self.state.requests, [])


class TestQuery(AdminCase):
    def test_v1_uses_influxql_endpoint(self):
        self.state.route("POST", "/query", '{"results": [{"series": [{"values": [[3]]}]}]}')
        out = self.client(version="v1").query("SELECT count(*) FROM mqtt_data_2026")
        self.assertIn("db=mqtt", self.last()["path"])
        self.assertIn("SELECT count", self.last_q())
        self.assertIn("values", out)

    def test_v2_uses_flux_with_csv_dialect(self):
        self.client(version="v2").query('from(bucket:"mqtt")')
        req = self.last()
        self.assertEqual(req["path"], "/api/v2/query?org=acme")
        self.assertEqual(req["headers"]["Accept"], "text/csv")
        self.assertIn("from(bucket", json.loads(req["body"])["query"])

    def test_v3_uses_query_sql(self):
        self.client().query("SELECT 1")
        req = self.last()
        self.assertTrue(req["path"].startswith("/api/v3/query_sql"))
        self.assertEqual(json.loads(req["body"]), {"db": "mqtt", "q": "SELECT 1", "format": "json"})

    def test_tables_statement_matches_the_backend(self):
        self.client(version="v1").tables()
        self.assertIn("SHOW MEASUREMENTS", self.last_q())
        self.state.requests.clear()
        self.client(version="v2").tables()
        self.assertIn('schema.measurements(bucket: "mqtt")',
                      json.loads(self.last()["body"])["query"])
        self.state.requests.clear()
        self.client().tables()
        body = json.loads(self.last()["body"])["q"]
        self.assertIn("information_schema.tables", body)
        self.assertIn("table_schema = 'iox'", body)   # 不滤会把 system 视图一起列出来


class TestDropTable(AdminCase):
    def test_v1_drops_measurement_immediately(self):
        state, note = self.client(version="v1").drop_table("mqtt_data_2023")
        self.assertEqual(state, "dropped")
        self.assertIn("DROP MEASUREMENT mqtt_data_2023", self.last_q())

    def test_v3_delete_table_endpoint(self):
        self.client().drop_table("mqtt_data_2023")
        req = self.last()
        self.assertEqual(req["method"], "DELETE")
        self.assertIn("db=mqtt", req["path"])
        self.assertIn("table=mqtt_data_2023", req["path"])
        self.assertNotIn("hard_delete_at", req["path"])
        self.assertIn("软删除", self.client().drop_table("mqtt_data_2024")[1])

    def test_v3_hard_delete_sets_hard_delete_at_now(self):
        self.client().drop_table("mqtt_data_2023", hard=True)
        self.assertIn("hard_delete_at=now", self.last()["path"])

    def test_v3_missing_table_is_reported_not_raised(self):
        self.state.route("DELETE", "/api/v3/configure/table", '{"error":"table not found"}', status=404)
        self.assertEqual(self.client().drop_table("nope")[0], "missing")

    def test_v3_double_delete_is_409_not_a_crash(self):
        # 官方文档：删已经删过的表返回 409，不是错误
        self.state.route("DELETE", "/api/v3/configure/table", '{"error":"already deleted"}', status=409)
        state, note = self.client().drop_table("mqtt_data_2023")
        self.assertEqual(state, "dropped")
        self.assertIn("409", note)

    def test_v2_cannot_drop_a_measurement(self):
        with self.assertRaises(UnsupportedBackend):
            self.client(version="v2").drop_table("mqtt_data_2023")

    def test_illegal_table_name_is_refused(self):
        for bad in ("x; DROP", "a b", ""):
            with self.assertRaises(ValueError):
                self.client().drop_table(bad)
            self.assertEqual(self.state.requests, [])


class TestDuration(unittest.TestCase):
    def test_units(self):
        self.assertEqual(duration_to_days("30d"), 30)
        self.assertEqual(duration_to_days("4w"), 28)
        self.assertEqual(duration_to_days("2y"), 730)
        self.assertIsNone(duration_to_days("30days"))
        self.assertIsNone(duration_to_days(""))


class TestValidateHints(AdminCase):
    def cfg(self, **kw) -> Config:
        influx = InfluxConfig(**kw)
        route = RouteConfig(name="u", topics=["a/+/b"], measurement="mqtt_data")
        return Config(mqtt=MqttConfig(), influx=influx, writer=WriterConfig(), routes=[route])

    def test_v1_retention_is_flagged(self):
        w = "".join(validate(self.cfg(version="v1", retention_period="30d")))
        self.assertIn("retention policy", w)

    def test_v3_immutable_retention_is_flagged(self):
        w = "".join(validate(self.cfg(version="v3", retention_period="30d")))
        self.assertIn("不可修改", w)
        # 没设 retention 时不该拿这条刷屏
        self.assertNotIn("不可修改", "".join(validate(self.cfg(version="v3"))))


class TestBridgeStartup(AdminCase):
    def test_ensure_backend_is_best_effort(self):
        from mqtt_influx_bridge.bridge import Bridge
        from mqtt_influx_bridge.config import WalConfig

        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(
                mqtt=MqttConfig(),
                influx=InfluxConfig(version="v3", url=self.url, database="mqtt", token="tok"),
                wal=WalConfig(path=Path(tmp) / "w.db"),
                writer=WriterConfig(),
                routes=[],
            )
            bridge = Bridge(cfg, client_factory=lambda *a: None)
            try:
                bridge._ensure_backend()                      # 不抛
                self.assertTrue(self.state.matched("POST", "/api/v3/configure/database"))

                self.state.requests.clear()
                cfg.influx.auto_create_database = False
                bridge._ensure_backend()
                self.assertEqual(self.state.requests, [])

                # 后端挂了也不能拦启动
                cfg.influx.auto_create_database = True
                cfg.influx.url = "http://127.0.0.1:1"
                bridge._ensure_backend()
            finally:
                bridge.wal.close()        # 不关的话 Windows 删不掉临时目录


class TestCli(AdminCase):
    def test_init_db_and_ping_and_purge_exit_codes(self):
        from mqtt_influx_bridge.__main__ import main

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.toml"
            path.write_text(
                f'[mqtt]\nhost = "127.0.0.1"\n[influx]\nversion = "v3"\nurl = "{self.url}"\n'
                'database = "mqtt"\ntoken = "tok"\n[[route]]\nname = "u"\ntopics = ["a/+/b"]\n'
                'measurement = "mqtt_data"\n',
                encoding="utf-8",
            )
            self.assertEqual(main(["-c", str(path), "init-db"]), 0)
            self.assertEqual(main(["-c", str(path), "ping"]), 0)
            self.assertEqual(main(["-c", str(path), "purge-table", "--table", "mqtt_data_2023"]), 0)
            self.assertEqual(self.state.matched("DELETE"), [])      # 没加 --execute 就什么都不做
            self.assertEqual(main(["-c", str(path), "purge-table", "--table", "mqtt_data_2023",
                                   "--execute"]), 0)
            self.assertEqual(len(self.state.matched("DELETE")), 1)
            self.assertEqual(main(["-c", str(path), "query"]), 2)   # 缺 --sql
            self.assertEqual(
                main(["-c", str(Path(tmp) / "missing.toml"), "ping"]), 2)


if __name__ == "__main__":
    unittest.main()
