"""topic 路由 + payload -> 按时间分表（year / month）的 line protocol 映射。

列从哪来：
  时间列  <- route.timestamp_path（同时决定落哪张分表）
  tag     <- route.tags / route.static_tags，显式声明、数量可控
  field   <- route.fields（两种 mode 都生效）+ mode="auto" 时未占用的每个 key
  数组/对象 -> JSON 字符串 field；数值 -> float（type_policy=string 时全部转字符串）
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import MappingConfig, RouteConfig
from .lineprotocol import (
    ParseError,
    as_string_field,
    build_line,
    format_value,
    normalize_line,
    resolve_path,
    split_field_spec,
    timestamp_to_ns,
)
from .topicmatch import expand_topics, topic_matches

_SIMPLE_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def decode_text(raw: bytes | str) -> str:
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    except UnicodeDecodeError as e:
        raise ParseError(f"payload 非 UTF-8: {e}") from e
    if not text.strip():
        raise ParseError("payload 为空")
    return text


def resolve_route(cfg_routes: list[RouteConfig], substitutions: dict[str, str], topic: str) -> RouteConfig:
    """按声明顺序返回首个命中的路由；命中不了视为配置缺失，交给上层进 DLQ。"""
    for route in cfg_routes:
        for pattern in expand_topics(route.topics, substitutions):
            if topic_matches(pattern, topic):
                return route
    raise ParseError(f"没有 [[route]] 匹配 topic [{topic}]")


def period_suffix(period: str, ts_ns: int, offset_minutes: int) -> str:
    """分表后缀：year -> 2026，month -> 202611。时间取 payload 时间戳，重投不会跑错表。"""
    dt = datetime.fromtimestamp(ts_ns // 1_000_000_000, tz=timezone(timedelta(minutes=offset_minutes)))
    return f"{dt.year:04d}{dt.month:02d}" if period == "month" else f"{dt.year}"


def table_name(route: RouteConfig, mapping: MappingConfig, ts_ns: int, item: dict, topic: str) -> str:
    base = route.measurement
    if route.measurement_path:
        dynamic = resolve_path(route.measurement_path, item, topic)
        if dynamic:
            base = str(dynamic)
    if route.year_table:
        return f"{base}_{period_suffix(route.table_period, ts_ns, mapping.year_tz_offset_minutes)}"
    return base


def auto_value(value: Any, policy: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return as_string_field(canonical_json(value))
    if policy == "string":
        return as_string_field(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return format_value(value, "float")
    return as_string_field(value)


def canonical_json(item: Any) -> str:
    return json.dumps(item, ensure_ascii=False, separators=(",", ":"))


def _column(name: str, key_case: str) -> str:
    return name.lower() if key_case == "lower" else name


def _is_simple_key(spec: str) -> bool:
    return bool(_SIMPLE_KEY_RE.match(spec))


def _missing_device_time(value: Any) -> bool:
    """空时间 = null / 缺 key / 空串 / 纯空白 / 非正数（RTC 没同步的固件发 0）。
    真垃圾值（"abc"）不在这里判，留给 timestamp_to_ns 抛 ParseError 进 DLQ。"""
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    try:
        return float(text) <= 0
    except ValueError:
        return False


def build_from_json(item: dict, route: RouteConfig, mapping: MappingConfig, topic: str) -> str:
    if not isinstance(item, dict):
        raise ParseError("JSON 消息必须是对象或对象数组")

    consumed: set[str] = set()
    ts_ns: int | None = None
    if route.timestamp_path:
        spec = route.timestamp_path
        raw_ts = resolve_path(spec, item, topic)
        if _is_simple_key(spec):
            consumed.add(spec)
        if not _missing_device_time(raw_ts):
            ts_ns = timestamp_to_ns(raw_ts, route.timestamp_unit)
    if ts_ns is None:
        # 空时间用接收时刻落库，别整条进 DLQ，也别造出 mqtt_data_197001 这种没人清的表。
        # 时间在这里就固化进 line protocol 再进 WAL，所以重试不会把时间戳推到 flush 时刻。
        ts_ns = time.time_ns()

    tags: dict[str, str] = {}
    for key, value in route.static_tags.items():
        tags[key] = str(value)
    for key, spec in route.tags.items():
        value = resolve_path(spec, item, topic)
        if _is_simple_key(spec):
            consumed.add(spec)
        if value is None or value == "":
            continue
        tags[key] = (
            value if isinstance(value, str) else canonical_json(value) if isinstance(value, (list, dict)) else str(value)
        )

    fields: dict[str, str] = {}
    for key, spec in route.fields.items():
        path, typ = split_field_spec(spec)
        value = resolve_path(path, item, topic)
        if _is_simple_key(path):
            consumed.add(path)
        if value is not None:
            formatted = format_value(value, typ)
            if formatted is not None:
                fields[key] = formatted

    if route.mode == "auto":
        reserved = route.mapped_columns
        for key, value in item.items():
            if key in consumed or key in route.exclude_keys:
                continue
            name = _column(key, route.key_case)
            if name in reserved or name in tags or name in fields:
                continue
            formatted = auto_value(value, route.type_policy)
            if formatted is not None:
                fields[name] = formatted

    if route.blob_field:
        fields[route.blob_field] = as_string_field(canonical_json(item))
    if route.topic_field:
        fields[route.topic_field] = as_string_field(topic)
    if not fields:
        raise ParseError("消息未解析出任何 field")

    return build_line(table_name(route, mapping, ts_ns, item, topic), tags, fields, ts_ns)


def parse_payload(raw: bytes | str, routes: list[RouteConfig], mapping: MappingConfig, topic: str) -> list[str]:
    route = resolve_route(routes, mapping.substitutions, topic)
    text = decode_text(raw)
    if route.type == "lineprotocol":
        return [normalize_line(line, route.timestamp_unit) for line in text.splitlines() if line.strip()]
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ParseError(f"JSON 解析失败: {e}") from e
    items = data if isinstance(data, list) else [data]
    return [build_from_json(item, route, mapping, topic) for item in items]
