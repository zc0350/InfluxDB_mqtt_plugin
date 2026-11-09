"""配置加载与校验：TOML 文件 + 环境变量覆盖。

一条 [[route]] = 一类 topic -> 一张按时间分表（year / month）；路由按声明顺序匹配，首个命中生效。
"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path

from .topicmatch import expand_topics, valid_topic_pattern

VALID_VERSIONS = ("v1", "v2", "v3")
VALID_ACK_MODES = ("persisted", "written")
VALID_PAYLOAD_TYPES = ("json", "lineprotocol")
VALID_MODES = ("auto", "explicit")
VALID_TABLE_PERIODS = ("year", "month")      # 分表后缀粒度，仅 year_table=true 时生效
VALID_UNITS = ("ns", "us", "ms", "s", "auto", "datetime")
VALID_WHEN_FULL = ("block", "drop")
VALID_TYPE_POLICIES = ("infer", "string")
VALID_KEY_CASES = ("lower", "keep")


@dataclass
class MqttConfig:
    host: str = "127.0.0.1"
    port: int = 1883
    topics: list[str] = field(default_factory=list)   # 留空 = 取所有 route.topics 的并集
    qos: int = 1
    client_id: str = "influx-mqtt-bridge"
    persistent_session: bool = True
    username: str = ""
    password: str = ""
    keepalive: int = 60
    tls_ca: str = ""                           # CA 证书路径；配置后启用 TLS
    tls_cert: str = ""
    tls_key: str = ""
    insecure_skip_verify: bool = False


@dataclass
class InfluxConfig:
    version: str = "v3"
    url: str = "http://127.0.0.1:8181"
    database: str = "mqtt"                     # v3: db / v1: db / v2: bucket
    org: str = ""                              # v2 必填
    token: str = ""                            # 建议用 INFLUX_TOKEN 环境变量
    username: str = ""                         # v1 basic auth
    password: str = ""
    timeout_seconds: float = 10.0
    no_sync: bool = False                      # v3；false = WAL 持久化后才返回 2xx
    accept_partial: bool = False               # v3；false = 整批原子
    auto_create_database: bool = True          # 启动时建库/建 bucket（幂等，失败只告警）
    retention_period: str = ""                 # 建库时的保留期，如 30d；v1 忽略（由 RP 管理）
    gzip: bool = True
    insecure_skip_verify: bool = False


@dataclass
class RouteConfig:
    """payload -> 表（measurement）的映射规则。"""

    name: str = "default"
    topics: list[str] = field(default_factory=lambda: ["#"])
    type: str = "json"                         # json | lineprotocol
    measurement: str = "mqtt_data"
    measurement_path: str = ""                 # 可选：从 payload 动态取表名（不含年份后缀）
    year_table: bool = True                    # 表名追加时间后缀，粒度见 table_period
    table_period: str = "year"                 # year -> _2026 / month -> _202611（需 year_table=true）
    timestamp_path: str = "time"               # 分表后缀与写入幂等都依赖它
    timestamp_unit: str = "auto"               # ns | us | ms | s | auto | datetime
    mode: str = "auto"                         # auto = 未列出的 key 自动展开成 field
    tags: dict[str, str] = field(default_factory=dict)         # tag 名 -> payload 路径 / @topic[N]
    fields: dict[str, str] = field(default_factory=dict)       # field 名 -> "路径:类型"
    static_tags: dict[str, str] = field(default_factory=dict)  # 固定 tag，如 direction
    blob_field: str = "data"                   # 完整 payload JSON 的列名；"" = 不存
    topic_field: str = "topic"                 # 原始 topic 的列名；"" = 不存
    type_policy: str = "infer"                 # infer = 数值转 float；string = 一律转字符串
    key_case: str = "lower"                    # lower = deviceCode -> devicecode
    exclude_keys: list[str] = field(default_factory=list)      # auto 模式下忽略的 payload key

    @property
    def mapped_columns(self) -> set[str]:
        """显式映射已占用的列名，自动展开时跳过以免歧义。"""
        used = set(self.tags) | set(self.fields) | set(self.static_tags)
        if self.blob_field:
            used.add(self.blob_field)
        if self.topic_field:
            used.add(self.topic_field)
        return used


@dataclass
class MappingConfig:
    year_tz_offset_minutes: int = 480          # 分表后缀按时区切：480 = UTC+8
    substitutions: dict[str, str] = field(default_factory=dict)  # ${productKey} -> 实际值


@dataclass
class WalConfig:
    path: str = "./data/bridge.db"
    max_pending: int = 500_000                 # 高水位；超出后暂不 ACK，借 broker 窗口背压


@dataclass
class WriterConfig:
    batch_size: int = 1000
    flush_interval_ms: int = 200
    retry_base_ms: int = 500
    retry_max_ms: int = 30_000
    max_retries: int = 0                       # 0 = 无限重试；超限进 DLQ
    ack_mode: str = "persisted"                # persisted | written
    ack_timeout_seconds: float = 120.0         # written 模式下未完成 ACK 的放弃时间
    when_full: str = "block"                   # block = 等待并暂不 ACK；drop = 转 DLQ 后 ACK
    split_on_reject: bool = True               # 整批被 4xx 拒绝时二分定位坏行，只丢坏行
    max_split_probes: int = 64                 # 单批最多额外探测次数


@dataclass
class LogConfig:
    level: str = "INFO"
    stats_interval_seconds: int = 30


@dataclass
class Config:
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    influx: InfluxConfig = field(default_factory=InfluxConfig)
    mapping: MappingConfig = field(default_factory=MappingConfig)
    routes: list[RouteConfig] = field(default_factory=list)
    wal: WalConfig = field(default_factory=WalConfig)
    writer: WriterConfig = field(default_factory=WriterConfig)
    log: LogConfig = field(default_factory=LogConfig)

    @property
    def subscribe_topics(self) -> list[str]:
        """实际订阅列表：显式配置优先，否则取所有路由 topic 的并集。"""
        if self.mqtt.topics:
            return list(self.mqtt.topics)
        out: list[str] = []
        for r in self.routes:
            for t in expand_topics(r.topics, self.mapping.substitutions):
                if t not in out:
                    out.append(t)
        return out


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
    "INFLUX_AUTO_CREATE_DB": ("influx", "auto_create_database", bool),
    "INFLUX_RETENTION_PERIOD": ("influx", "retention_period", str),
    "PRODUCT_KEY": ("mapping", "productKey", str),          # 写入 substitutions
    "YEAR_TZ_OFFSET_MINUTES": ("mapping", "year_tz_offset_minutes", int),
    "WAL_PATH": ("wal", "path", str),
    "BRIDGE_ACK_MODE": ("writer", "ack_mode", str),
    "LOG_LEVEL": ("log", "level", str),
}


def _section(data: dict, name: str, cls, warnings: list[str]):
    raw = data.get(name, {}) or {}
    known = {f.name for f in fields(cls)}
    unknown = [k for k in raw if k not in known]
    if unknown:
        warnings.append(f"[{name}] 未知配置项（已忽略）: {', '.join(unknown)}")
    return cls(**{k: v for k, v in raw.items() if k in known})


def _routes(data: dict, warnings: list[str]) -> list[RouteConfig]:
    known = {f.name for f in fields(RouteConfig)}
    out: list[RouteConfig] = []
    for i, raw in enumerate(data.get("route", []) or [], start=1):
        raw = raw or {}
        name = str(raw.get("name", f"route{i}"))
        unknown = [k for k in raw if k not in known]
        if unknown:
            warnings.append(f"[[route]] {name} 未知配置项（已忽略）: {', '.join(unknown)}")
        out.append(RouteConfig(**{k: v for k, v in raw.items() if k in known}))
    return out


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
        mapping=_section(data, "mapping", MappingConfig, warnings),
        routes=_routes(data, warnings),
        wal=_section(data, "wal", WalConfig, warnings),
        writer=_section(data, "writer", WriterConfig, warnings),
        log=_section(data, "log", LogConfig, warnings),
    )

    for env_name, (section, key, kind) in _ENV_MAP.items():
        if env_name not in os.environ:
            continue
        value = os.environ[env_name]
        if (section, key) == ("mapping", "productKey"):
            cfg.mapping.substitutions["productKey"] = value
        else:
            setattr(getattr(cfg, section), key, _coerce(value, kind))

    warnings.extend(validate(cfg))
    return cfg, warnings


def validate(cfg: Config) -> list[str]:
    w: list[str] = []
    if cfg.influx.version not in VALID_VERSIONS:
        raise ValueError(f"influx.version 必须是 {VALID_VERSIONS} 之一")
    if cfg.writer.ack_mode not in VALID_ACK_MODES:
        raise ValueError(f"writer.ack_mode 必须是 {VALID_ACK_MODES} 之一")
    if cfg.writer.when_full not in VALID_WHEN_FULL:
        raise ValueError(f"writer.when_full 必须是 {VALID_WHEN_FULL} 之一")
    if cfg.mqtt.qos not in (0, 1, 2):
        raise ValueError("mqtt.qos 必须是 0/1/2")
    if cfg.writer.batch_size < 1 or cfg.wal.max_pending < 1:
        raise ValueError("batch_size 与 max_pending 必须 > 0")
    if not cfg.routes:
        raise ValueError("至少需要一个 [[route]]：topic -> 表的映射规则")

    names: set[str] = set()
    for r in cfg.routes:
        where = f"[[route]] {r.name}"
        if r.name in names:
            raise ValueError(f"{where}：name 重复")
        names.add(r.name)
        if r.type not in VALID_PAYLOAD_TYPES:
            raise ValueError(f"{where}：type 必须是 {VALID_PAYLOAD_TYPES} 之一")
        if r.mode not in VALID_MODES:
            raise ValueError(f"{where}：mode 必须是 {VALID_MODES} 之一")
        if r.timestamp_unit not in VALID_UNITS:
            raise ValueError(f"{where}：timestamp_unit 必须是 {VALID_UNITS} 之一")
        if r.table_period not in VALID_TABLE_PERIODS:
            raise ValueError(f"{where}：table_period 必须是 {VALID_TABLE_PERIODS} 之一")
        if r.table_period == "month" and not r.year_table:
            w.append(f"{where}：table_period=month 需要 year_table=true 才生效，当前被忽略")
        if r.type_policy not in VALID_TYPE_POLICIES:
            raise ValueError(f"{where}：type_policy 必须是 {VALID_TYPE_POLICIES} 之一")
        if r.key_case not in VALID_KEY_CASES:
            raise ValueError(f"{where}：key_case 必须是 {VALID_KEY_CASES} 之一")
        if not r.topics:
            raise ValueError(f"{where}：topics 不能为空")
        bad = [t for t in expand_topics(r.topics, cfg.mapping.substitutions)
               if not valid_topic_pattern(t)]
        if bad:
            raise ValueError(f"{where}：topic 通配符不合法（# 只能在末段独占）: {', '.join(bad)}")
        if not r.measurement and not r.measurement_path:
            raise ValueError(f"{where}：需要 measurement 或 measurement_path")
        if r.mode == "explicit" and not r.fields and not r.blob_field and not r.topic_field:
            raise ValueError(
                f"{where}：mode=explicit 且 fields/blob_field/topic_field 全空，任何消息都不会有 field，只能全进 DLQ"
            )
        clash = set(r.fields) & set(r.tags)
        if clash:
            w.append(f"{where}：同名列同时配成 tag 和 field，查询会歧义: {', '.join(sorted(clash))}")
        if not r.timestamp_path:
            w.append(f"{where}：timestamp_path 为空，重投会产生重复点")
        if len(r.tags) + len(r.static_tags) > 5:
            w.append(f"{where}：tag 数 {len(r.tags) + len(r.static_tags)} 偏多，series 基数按 tag 组合相乘")
        if r.mode == "auto" and not r.blob_field:
            w.append(f"{where}：blob_field 为空，字段漂移时无法回溯原始 payload")

    period_by_meas: dict[str, str] = {}
    for r in cfg.routes:
        if r.year_table and r.measurement:
            cur = period_by_meas.get(r.measurement, "year")
            period_by_meas[r.measurement] = "month" if "month" in (cur, r.table_period) else "year"
    if period_by_meas:
        cleanup = {
            "v1": "清理用 DROP MEASUREMENT（立即生效），保留期由 retention policy 管",
            "v2": "不能按 measurement 删，只能整 bucket 删或给 bucket 设 retention",
            "v3": "只能逐表 delete table（默认软删，磁盘等 compaction），保留期建库时定且之后改不了",
        }[cfg.influx.version]
        label = {"year": "YYYY", "month": "YYYYMM"}
        parts = ", ".join(f"{m}_{label[p]}" for m, p in sorted(period_by_meas.items()))
        per_year = sum(12 if p == "month" else 1 for p in period_by_meas.values())
        w.append(f"按{'月' if 'month' in period_by_meas.values() else '年'}分表 {parts}（约 {per_year} 张表/年）：{cleanup}")

    if cfg.mqtt.qos == 0:
        w.append("mqtt.qos=0 无 ACK 语义，无法保证不丢数；建议改为 1")
        if cfg.writer.ack_mode == "written":
            w.append("qos=0 时 written 模式无效，已按 persisted 语义运行")
    if not cfg.mqtt.persistent_session or not cfg.mqtt.client_id:
        w.append("持久会话需要 persistent_session=true 且固定 client_id，否则离线期间消息会丢")
    if cfg.mqtt.username and not (cfg.mqtt.tls_ca or cfg.mqtt.tls_cert):
        hosts = ("127.0.0.1", "localhost", "::1")
        if cfg.mqtt.host not in hosts:
            w.append("MQTT 凭据未走 TLS，明文传输；生产环境请配置 mqtt.tls_ca")
    if cfg.influx.version == "v2" and not cfg.influx.org:
        w.append("v2 写入需要 influx.org")
    if cfg.influx.version == "v1" and cfg.influx.retention_period:
        w.append("v1 的保留期由 retention policy 管，influx.retention_period 只在 v2/v3 建库时生效")
    if cfg.influx.auto_create_database and cfg.influx.version == "v3" and cfg.influx.retention_period:
        w.append("v3 建库后的 retention 不可修改；启动时若库已存在，retention_period 不会生效")
    if cfg.influx.version in ("v2", "v3") and not cfg.influx.token:
        w.append("未配置 influx.token（如服务端未启用鉴权可忽略）")
    if cfg.influx.accept_partial:
        w.append("accept_partial=true 时整批可能部分成功；重试会造成重复点，建议保持 false")
    if cfg.influx.no_sync:
        w.append("no_sync=true 时 InfluxDB 未持久化即返回 2xx，崩溃仍可能丢数")
    return w
