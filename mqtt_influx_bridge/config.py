"""配置加载与校验：TOML 文件 + 环境变量覆盖。"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

VALID_VERSIONS = ("v1", "v2", "v3")
VALID_ACK_MODES = ("persisted", "written")
VALID_FORMATS = ("json", "lineprotocol")
VALID_UNITS = ("ns", "us", "ms", "s", "auto", "datetime")
VALID_WHEN_FULL = ("block", "drop")


@dataclass
class MqttConfig:
    host: str = "127.0.0.1"
    port: int = 1883
    topics: list[str] = field(default_factory=lambda: ["sensors/#"])
    qos: int = 1
    client_id: str = "influx-mqtt-bridge"
    persistent_session: bool = True
    username: str = ""
    password: str = ""
    keepalive: int = 60
    tls_ca: str = ""
    tls_cert: str = ""
    tls_key: str = ""
    insecure_skip_verify: bool = False


@dataclass
class InfluxConfig:
    version: str = "v3"
    url: str = "http://127.0.0.1:8181"
    database: str = "sensors"          # v3: db / v1: db / v2: bucket
    org: str = ""                      # v2 only
    token: str = ""                    # v2/v3；建议用 INFLUX_TOKEN
    username: str = ""                 # v1 basic auth
    password: str = ""
    timeout_seconds: float = 10.0
    no_sync: bool = False              # v3 only；false = 等 WAL 持久化
    accept_partial: bool = False       # v3 only；false = 整批原子，避免部分成功后重试重复写
    gzip: bool = True
    insecure_skip_verify: bool = False


@dataclass
class FormatConfig:
    type: str = "json"                 # json | lineprotocol
    measurement: str = "sensor_data"
    measurement_path: str = ""         # 可选：从 payload 动态取表名
    timestamp_path: str = "ts"         # payload 内的时间戳路径；留空则用接收时刻
    timestamp_unit: str = "ms"         # ns | us | ms | s | auto | datetime
    tags: dict[str, str] = field(default_factory=dict)       # tag 名 -> payload 路径
    fields: dict[str, str] = field(default_factory=dict)     # field 名 -> "路径:类型"
    topic_tags: dict[str, int] = field(default_factory=dict) # tag 名 -> topic 分段下标
    static_tags: dict[str, str] = field(default_factory=lambda: {"source": "mqtt"})


@dataclass
class WalConfig:
    path: str = "./data/bridge.db"
    max_pending: int = 500_000         # 高水位；超出后暂不 ACK，由 broker 窗口形成背压


@dataclass
class WriterConfig:
    batch_size: int = 1000
    flush_interval_ms: int = 200
    retry_base_ms: int = 500
    retry_max_ms: int = 30_000
    max_retries: int = 0               # 0 = 无限重试；超限进 DLQ
    ack_mode: str = "persisted"        # persisted | written
    ack_timeout_seconds: float = 120.0 # written 模式下未完成 ACK 的放弃时间
    when_full: str = "block"           # block = 等待并暂不 ACK；drop = 写入 DLQ 后 ACK


@dataclass
class LogConfig:
    level: str = "INFO"
    stats_interval_seconds: int = 30


@dataclass
class Config:
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    influx: InfluxConfig = field(default_factory=InfluxConfig)
    format: FormatConfig = field(default_factory=FormatConfig)
    wal: WalConfig = field(default_factory=WalConfig)
    writer: WriterConfig = field(default_factory=WriterConfig)
    log: LogConfig = field(default_factory=LogConfig)


_ENV_MAP = {
    "MQTT_HOST": ("mqtt", "host", str),
    "MQTT_PORT": ("mqtt", "port", int),
    "MQTT_QOS": ("mqtt", "qos", int),
    "MQTT_CLIENT_ID": ("mqtt", "client_id", str),
    "MQTT_USERNAME": ("mqtt", "username", str),
    "MQTT_PASSWORD": ("mqtt", "password", str),
    "MQTT_TOPICS": ("mqtt", "topics", "list"),
    "INFLUX_URL": ("influx", "url", str),
    "INFLUX_TOKEN": ("influx", "token", str),
    "INFLUX_DATABASE": ("influx", "database", str),
    "INFLUX_ORG": ("influx", "org", str),
    "INFLUX_VERSION": ("influx", "version", str),
    "WAL_PATH": ("wal", "path", str),
    "BRIDGE_ACK_MODE": ("writer", "ack_mode", str),
    "LOG_LEVEL": ("log", "level", str),
}


def _section(data: dict, name: str, cls, warnings: list[str]):
    raw = data.get(name, {}) or {}
    known = cls.__dataclass_fields__
    unknown = [k for k in raw if k not in known]
    if unknown:
        warnings.append(f"[{name}] 未知配置项（已忽略）: {', '.join(unknown)}")
    return cls(**{k: v for k, v in raw.items() if k in known})


def _coerce(value: str, kind):
    if kind == "list":
        return [x.strip() for x in value.split(",") if x.strip()]
    if kind is bool:
        return value.strip().lower() in ("1", "true", "yes", "on")
    return kind(value)


def load_config(path: str | Path) -> tuple[Config, list[str]]:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"配置文件不存在: {p}（可从 config.example.toml 复制）")
    with p.open("rb") as f:
        data = tomllib.load(f)

    warnings: list[str] = []
    cfg = Config(
        mqtt=_section(data, "mqtt", MqttConfig, warnings),
        influx=_section(data, "influx", InfluxConfig, warnings),
        format=_section(data, "format", FormatConfig, warnings),
        wal=_section(data, "wal", WalConfig, warnings),
        writer=_section(data, "writer", WriterConfig, warnings),
        log=_section(data, "log", LogConfig, warnings),
    )

    for env_name, (section, key, kind) in _ENV_MAP.items():
        if env_name in os.environ:
            setattr(getattr(cfg, section), key, _coerce(os.environ[env_name], kind))

    warnings.extend(validate(cfg))
    return cfg, warnings


def validate(cfg: Config) -> list[str]:
    w: list[str] = []
    if cfg.influx.version not in VALID_VERSIONS:
        raise ValueError(f"influx.version 必须是 {VALID_VERSIONS} 之一")
    if cfg.writer.ack_mode not in VALID_ACK_MODES:
        raise ValueError(f"writer.ack_mode 必须是 {VALID_ACK_MODES} 之一")
    if cfg.format.type not in VALID_FORMATS:
        raise ValueError(f"format.type 必须是 {VALID_FORMATS} 之一")
    if cfg.format.timestamp_unit not in VALID_UNITS:
        raise ValueError(f"format.timestamp_unit 必须是 {VALID_UNITS} 之一")
    if cfg.writer.when_full not in VALID_WHEN_FULL:
        raise ValueError(f"writer.when_full 必须是 {VALID_WHEN_FULL} 之一")
    if cfg.mqtt.qos not in (0, 1, 2):
        raise ValueError("mqtt.qos 必须是 0/1/2")
    if cfg.writer.batch_size < 1 or cfg.wal.max_pending < 1:
        raise ValueError("batch_size 与 max_pending 必须 > 0")

    if cfg.mqtt.qos == 0:
        w.append("mqtt.qos=0 无 ACK 语义，无法保证不丢数；建议改为 1")
        if cfg.writer.ack_mode == "written":
            w.append("qos=0 时 written 模式无效，已按 persisted 语义运行")
    if not cfg.mqtt.persistent_session or not cfg.mqtt.client_id:
        w.append("持久会话需要 persistent_session=true 且固定 client_id，否则离线期间消息会丢")
    if not cfg.format.timestamp_path and cfg.format.type == "json":
        w.append("未配置 format.timestamp_path：重投会产生重复点（用接收时刻作时间戳）")
    if cfg.format.type == "json" and not cfg.format.fields:
        raise ValueError("json 模式必须配置 format.fields")
    if cfg.mqtt.username and not (cfg.mqtt.tls_ca or cfg.mqtt.tls_cert):
        hosts = ("127.0.0.1", "localhost", "::1")
        if cfg.mqtt.host not in hosts:
            w.append("MQTT 凭据未走 TLS，明文传输；生产环境请配置 mqtt.tls_ca")
    if cfg.influx.version == "v2" and not cfg.influx.org:
        w.append("v2 写入需要 influx.org")
    if cfg.influx.version in ("v2", "v3") and not cfg.influx.token:
        w.append("未配置 influx.token（如服务端未启用鉴权可忽略）")
    if cfg.influx.accept_partial:
        w.append("accept_partial=true 时整批可能部分成功；重试会造成重复点，建议保持 false")
    if cfg.influx.no_sync:
        w.append("no_sync=true 时 InfluxDB 未持久化即返回 2xx，崩溃仍可能丢数")
    return w
