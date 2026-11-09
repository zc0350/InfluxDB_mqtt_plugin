"""line protocol 原语：转义、时间戳归一、值格式化、单行拼装。"""
from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, InvalidOperation, ROUND_DOWN, localcontext
from typing import Any


class ParseError(ValueError):
    """消息无法转换为合法 line protocol。"""


TYPES = {"float", "int", "bool", "str", "auto"}
_TOPIC_RE = re.compile(r"^@topic\[(\d+)\]$")
_LP_TS_RE = re.compile(r"\s(-?\d+)\s*$")
_UNIT_FACTOR = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_US = timedelta(microseconds=1)
# 换算用十进制，39 位有效数字够 19 位纳秒整数再带 9 位小数
_NS_CTX = Context(prec=39, rounding=ROUND_DOWN)


def _as_decimal(value: Any) -> Decimal:
    """float 在 1.8e18 附近的最小刻度是 256ns，纳秒时间戳过一遍 float 会把相差 1ns
    的两个点变成同一个点（IOx 按 tagset+time 合并），所以全程不做 float 转换。"""
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    try:
        d = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as e:
        raise ParseError(f"非法时间戳 {value!r}: {e}") from e
    if not d.is_finite():
        raise ParseError(f"非法时间戳 {value!r}")
    return d


def timestamp_to_ns(value: Any, unit: str = "auto") -> int:
    if unit == "datetime":
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as e:
            raise ParseError(f"无法解析 ISO 8601 时间戳 {value!r}: {e}") from e
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int((dt - _EPOCH) // _ONE_US) * 1_000
    d = _as_decimal(value)
    if unit == "auto":
        a = abs(d)
        factor = 1_000_000_000 if a < 5_000_000_000 else 1_000_000 if a < 5_000_000_000_000 else 1_000 if a < 5_000_000_000_000_000 else 1
    else:
        factor = _UNIT_FACTOR[unit]
    with localcontext(_NS_CTX):
        return int(d * factor)


def resolve_path(spec: str, obj: Any, topic: str) -> Any:
    """按 `a.b` / `a.0` 取 payload 值，`@topic[N]` 取 topic 分段。"""
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


def split_field_spec(spec: str) -> tuple[str, str]:
    if ":" in spec:
        path, typ = spec.rsplit(":", 1)
        if typ in TYPES:
            return path, typ
    return spec, "auto"


def format_value(value: Any, typ: str) -> str | None:
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
        return as_string_field(value)
    except (TypeError, ValueError) as e:
        raise ParseError(f"字段值 {value!r} 转 {typ} 失败: {e}") from e


def as_string_field(value: Any) -> str:
    """双引号字段：数组/对象按 JSON 序列化，内部引号与反斜杠按 line protocol 规则转义。"""
    if isinstance(value, (list, dict)):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return '"' + str(value).replace("\\", r"\\").replace('"', r'\"') + '"'


def _esc_measurement(s: Any) -> str:
    return str(s).replace(",", r"\,").replace(" ", r"\ ")


def _esc_key(s: Any) -> str:
    return str(s).replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")


def build_line(measurement: str, tags: dict[str, str], fields: dict[str, str], ts_ns: int) -> str:
    tag_part = ",".join(
        f"{_esc_key(k)}={_esc_key(v)}" for k, v in tags.items() if v is not None and str(v) != ""
    )
    field_part = ",".join(f"{_esc_key(k)}={v}" for k, v in fields.items())
    head = _esc_measurement(measurement) + ("," + tag_part if tag_part else "")
    return f"{head} {field_part} {ts_ns}"


def normalize_line(line: str, unit: str) -> str:
    """lineprotocol 透传时把行尾时间戳统一换算成纳秒；缺失则用接收时刻。"""
    m = _LP_TS_RE.search(line)
    if m:
        ts = timestamp_to_ns(int(m.group(1)), unit)
        return line[: m.start()].rstrip() + f" {ts}"
    return line.rstrip() + f" {time.time_ns()}"
