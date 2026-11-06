"""命令行入口：python -m mqtt_influx_bridge [-c config.toml] [run|status|check]"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys

from .bridge import Bridge
from .config import Config, load_config
from .wal import Wal


def _summary(cfg: Config) -> dict:
    return {
        "mqtt": {
            "host": cfg.mqtt.host,
            "port": cfg.mqtt.port,
            "topics": cfg.mqtt.topics,
            "qos": cfg.mqtt.qos,
            "client_id": cfg.mqtt.client_id,
            "persistent_session": cfg.mqtt.persistent_session,
            "tls": bool(cfg.mqtt.tls_ca or cfg.mqtt.tls_cert),
        },
        "influx": {
            "version": cfg.influx.version,
            "url": cfg.influx.url,
            "database": cfg.influx.database,
            "token": "***" if cfg.influx.token else "(none)",
        },
        "format": {"type": cfg.format.type, "measurement": cfg.format.measurement},
        "wal": {"path": cfg.wal.path, "max_pending": cfg.wal.max_pending},
        "writer": {
            "batch_size": cfg.writer.batch_size,
            "ack_mode": cfg.writer.ack_mode,
        },
    }


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
        "command", nargs="?", default="run", choices=["run", "status", "check"],
        help="run=启动桥接（默认）；status=查看本地队列；check=仅校验配置",
    )
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

    if args.command == "status":
        wal = Wal(cfg.wal.path)
        try:
            print(json.dumps(wal.stats(), ensure_ascii=False, indent=2))
        finally:
            wal.close()
        return 0

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
