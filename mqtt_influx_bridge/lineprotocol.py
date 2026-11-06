"""MQTT payload -> InfluxDB line protocol（时间戳统一为纳秒）。"""
from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timezone
from typing import Any

from .config import FormatConfig


class ParseError(ValueError):
    """消息无法转换为合法 line protocol。"""


TYPES = {"float", "int", "bool", "str", "auto"}
_TOPIC_RE = re.compile(r"^@topic\[(\d+)\]$")
_LP_TS_RE = re.compile(r"\s(-?\d+)\s*$")
_UNIT_FACTOR = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


def timestamp_to_ns(value: Any, unit: str = "auto") -> int:
    if unit == "datetime":
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as e:
            raise ParseError(f"无法解析 ISO 8601 时间戳 {value!r}: {e}") from e
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    try:
        n = int(float(value))
    except (TypeError, ValueError) as e:
        raise ParseError(f"非法时间戳 {value!r}: {e}") from e
    if unit == "auto":
        a = abs(n)
        factor = 1_000_000_000 if a < 5e9 else 1_000_000 if a < 5e12 else 1_000 if a < 5e15 else 1
    else:
        factor = _UNIT_FACTOR[unit]
    return int(n * factor)


def _esc_measurement(s: Any) -> str:
    return str(s).replace(",", r"\,").replace(" ", r"\ ")


def _esc_key(s: Any) -> str:
    return str(s).replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")


def _esc_string(s: Any) -> str:
    return '"' + str(s).replace("\\", r"\\").replace('"', r'\"') + '"'


def _resolve(spec: str, obj: Any, topic: str) -> Any:
    m = _TOPIC_RE.match(spec)
    if m:
        parts = topic.split("/")
        idx = int(m.group(1))
        return parts[idx] if idx < len(parts) else None
    cur = obj
    for part in spec.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, (list, tuple)) and part.isdigit():
            i = int(part)
            cur = cur[i] if i < len(cur) else None
        else:
            return None
    return cur


def _split_field_spec(spec: str) -> tuple[str, str]:
    if ":" in spec:
        path, typ = spec.rsplit(":", 1)
        if typ in TYPES:
            return path, typ
    return spec, "auto"


def _format_value(value: Any, typ: str) -> str | None:
    if value is None:
        return None
    if typ == "auto":
        if isinstance(value, bool):
            typ = "bool"
        elif isinstance(value, int):
            typ = "int"
        elif isinstance(value, float):
            typ = "float"
        else:
            typ = "str"
    try:
        if typ == "float":
            f = float(value)
            if not math.isfinite(f):
                raise ParseError(f"非法浮点值 {value!r}")
            return repr(f)
        if typ == "int":
            return f"{int(value)}i"
        if typ == "bool":
            if isinstance(value, str):
                return "true" if value.strip().lower() in ("true", "1", "yes", "on") else "false"
            return "true" if bool(value) else "false"
        return _esc_string(value)
    except (TypeError, ValueError) as e:
        raise ParseError(f"字段值 {value!r} 转 {typ} 失败: {e}") from e


def build_line(measurement: str, tags: dict[str, str], fields: dict[str, str], ts_ns: int) -> str:
    tag_part = ",".join(
        f"{_esc_key(k)}={_esc_key(v)}" for k, v in tags.items() if v is not None and str(v) != ""
    )
    field_part = ",".join(f"{_esc_key(k)}={v}" for k, v in fields.items())
    head = _esc_measurement(measurement) + ("," + tag_part if tag_part else "")
    return f"{head} {field_part} {ts_ns}"


def _normalize_lp(line: str, fmt: FormatConfig) -> str:
    m = _LP_TS_RE.search(line)
    if m:
        ts = timestamp_to_ns(int(m.group(1)), fmt.timestamp_unit)
        return line[: m.start()].rstrip() + f" {ts}"
    return line.rstrip() + f" {time.time_ns()}"


def _build_from_json(item: dict, fmt: FormatConfig, topic: str) -> str:
    ts_ns = None
    if fmt.timestamp_path:
        raw_ts = _resolve(fmt.timestamp_path, item, topic)
        if raw_ts is not None:
            ts_ns = timestamp_to_ns(raw_ts, fmt.timestamp_unit)
    if ts_ns is None:
        ts_ns = time.time_ns()

    measurement = fmt.measurement
    if fmt.measurement_path:
        dynamic = _resolve(fmt.measurement_path, item, topic)
        if dynamic:
            measurement = str(dynamic)

    tags: dict[str, str] = dict(fmt.static_tags)
    for key, path in fmt.tags.items():
        v = _resolve(path, item, topic)
        if v is not None:
            tags[key] = str(v)
    for key, idx in fmt.topic_tags.items():
        parts = topic.split("/")
        if 0 <= idx < len(parts):
            tags[key] = parts[idx]

    fields: dict[str, str] = {}
    for key, spec in fmt.fields.items():
        path, typ = _split_field_spec(spec)
        v = _resolve(path, item, topic)
        if v is not None:
            formatted = _format_value(v, typ)
            if formatted is not None:
                fields[key] = formatted
    if not fields:
        raise ParseError("消息未解析出任何 field")
    return build_line(measurement, tags, fields, ts_ns)


def parse_payload(raw: bytes | str, fmt: FormatConfig, topic: str) -> list[str]:
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    except UnicodeDecodeError as e:
        raise ParseError(f"payload 非 UTF-8: {e}") from e
    if not text.strip():
        raise ParseError("payload 为空")

    if fmt.type == "lineprotocol":
        return [_normalize_lp(line, fmt) for line in text.splitlines() if line.strip()]

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ParseError(f"JSON 解析失败: {e}") from e
    items = data if isinstance(data, list) else [data]
    out = []
    for item in items:
        if not isinstance(item, dict):
            raise ParseError("JSON 消息必须是对象或对象数组")
        out.append(_build_from_json(item, fmt, topic))
    return out
