"""命令行入口：python -m mqtt_influx_bridge [-c config.toml] [run|status|check]"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys

from .admin import AdminClient, BackendError, UnsupportedBackend
from .bridge import Bridge
from .config import Config, load_config
from .routing import ParseError, parse_payload, resolve_route
from .topicmatch import expand_topics
from .wal import Wal
from .writer import TransientError


def _summary(cfg: Config) -> dict:
    return {
        "mqtt": {
            "host": cfg.mqtt.host,
            "port": cfg.mqtt.port,
            "topics": cfg.subscribe_topics,
            "qos": cfg.mqtt.qos,
            "client_id": cfg.mqtt.client_id,
            "persistent_session": cfg.mqtt.persistent_session,
            "tls": bool(cfg.mqtt.tls_ca or cfg.mqtt.tls_cert),
        },
        "influx": {
            "version": cfg.influx.version,
            "url": cfg.influx.url,
            "database": cfg.influx.database,
            "org": cfg.influx.org,
            "token": "***" if cfg.influx.token else "(none)",
            "auto_create_database": cfg.influx.auto_create_database,
            "retention_period": cfg.influx.retention_period,
        },
        "mapping": {
            "year_tz_offset_minutes": cfg.mapping.year_tz_offset_minutes,
            "substitutions": cfg.mapping.substitutions,
        },
        "routes": [
            {
                "name": r.name,
                "topics": expand_topics(r.topics, cfg.mapping.substitutions),
                "table": (f"{r.measurement}_{'YYYYMM' if r.table_period == 'month' else 'YYYY'}"
                          if r.year_table else r.measurement),
                "timestamp": f"{r.timestamp_path or '(接收时刻)'} [{r.timestamp_unit}]",
                "mode": r.mode,
                "tags": {**r.static_tags, **r.tags},
                "fields": r.fields,
                "blob_field": r.blob_field,
                "topic_field": r.topic_field,
            }
            for r in cfg.routes
        ],
        "wal": {"path": cfg.wal.path, "max_pending": cfg.wal.max_pending},
        "writer": {
            "batch_size": cfg.writer.batch_size,
            "ack_mode": cfg.writer.ack_mode,
        },
    }


def _sample(cfg: Config, topic: str, payload: str) -> int:
    """不连 broker / 不连库，干跑一条 payload 的映射结果，用于上线前核对表名与列名。"""
    topic = topic or "/"
    raw = payload or sys.stdin.read()
    try:
        route = resolve_route(cfg.routes, cfg.mapping.substitutions, topic)
        lines = parse_payload(raw, cfg.routes, cfg.mapping, topic)
    except ParseError as e:
        print(f"映射失败: {e}", file=sys.stderr)
        return 1
    table = lines[0].split(" ", 1)[0].split(",")[0]
    print(json.dumps(
        {"topic": topic, "route": route.name, "table": table, "lines": lines},
        ensure_ascii=False, indent=2,
    ))
    return 0


def _backend(cfg: Config, args) -> int:
    """ping / init-db / query / tables / purge-table：按 influx.version 分派到后端管理 API。"""
    admin = AdminClient(cfg.influx)
    try:
        if args.command == "ping":
            ok, note = admin.ping()
            print(note)
            return 0 if ok else 1
        if args.command == "init-db":
            state, detail = admin.ensure_database(cfg.influx.retention_period)
            print(f"{state}: {detail}")
            return 0
        if args.command == "tables":
            print(admin.tables())
            return 0
        if args.command == "query":
            if not args.sql:
                print("query 需要 --sql（语句按 influx.version 的方言写）", file=sys.stderr)
                return 2
            print(admin.query(args.sql))
            return 0
        if not args.table:
            print("purge-table 需要 --table <表名>", file=sys.stderr)
            return 2
        if not args.execute:
            print(f"将要删除 {cfg.influx.database}.{args.table}"
                  f"（{'硬删除，立即回收磁盘' if args.hard else '默认软删除'}）；加 --execute 真删")
            return 0
        state, note = admin.drop_table(args.table, hard=args.hard)
        print(f"{state}: {note}")
        return 0
    except UnsupportedBackend as e:
        print(f"该后端不支持：{e}", file=sys.stderr)
        return 3
    except (BackendError, TransientError, ValueError) as e:
        print(f"后端返回：{e}", file=sys.stderr)
        return 1
    finally:
        admin.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="mqtt-influx-bridge",
        description="常驻 MQTT 订阅 -> 本地 WAL -> 批量写入 InfluxDB",
    )
    parser.add_argument(
        "-c", "--config", default=os.environ.get("BRIDGE_CONFIG", "config.toml"),
        help="TOML 配置文件路径（默认 config.toml）",
    )
    parser.add_argument(
        "command", nargs="?", default="run",
        choices=["run", "status", "check", "sample",
                 "ping", "init-db", "query", "tables", "purge-table"],
        help="run=启动桥接（默认）；status=本地队列；check=校验配置；sample=干跑映射；"
             "ping/init-db/query/tables/purge-table=按 influx.version 操作后端",
    )
    parser.add_argument("--topic", default="", help="sample：模拟的 MQTT topic")
    parser.add_argument("--payload", default="", help="sample：payload 文本；留空则从 stdin 读")
    parser.add_argument("--sql", default="", help="query：该后端方言的语句（v1 InfluxQL / v2 Flux / v3 SQL）")
    parser.add_argument("--table", default="", help="purge-table：要删的表 / measurement 名")
    parser.add_argument("--execute", action="store_true", help="purge-table：真的删（默认只打印将做什么）")
    parser.add_argument("--hard", action="store_true",
                        help="purge-table：v3 用硬删除立即回收磁盘（不可恢复）")
    args = parser.parse_args(argv)

    try:
        cfg, warnings = load_config(args.config)
    except (FileNotFoundError, ValueError, OSError) as e:
        print(f"配置错误: {e}", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=getattr(logging, cfg.log.level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    for w in warnings:
        logging.getLogger("config").warning(w)

    if args.command == "check":
        print(json.dumps(_summary(cfg), ensure_ascii=False, indent=2))
        return 0

    if args.command == "sample":
        return _sample(cfg, args.topic, args.payload)

    if args.command == "status":
        wal = Wal(cfg.wal.path)
        try:
            print(json.dumps(wal.stats(), ensure_ascii=False, indent=2))
        finally:
            wal.close()
        return 0

    if args.command in ("ping", "init-db", "query", "tables", "purge-table"):
        return _backend(cfg, args)

    bridge = Bridge(cfg)

    def _handler(signum, frame):
        logging.getLogger("main").info("收到信号 %s，准备退出…", signum)
        bridge.request_stop()

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)
    bridge.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
