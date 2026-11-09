"""MQTT topic 通配符与 ${var} 占位符。"""
from __future__ import annotations

import re

_VAR_RE = re.compile(r"\$\{([^}]*)\}")


def expand_topic(pattern: str, substitutions: dict[str, str]) -> str:
    """把 ${productKey} 之类占位符换成实际值；未提供的按单层通配符 + 处理。"""
    return _VAR_RE.sub(lambda m: substitutions.get(m.group(1), "+"), pattern)


def expand_topics(patterns: list[str], substitutions: dict[str, str]) -> list[str]:
    return [expand_topic(p, substitutions) for p in patterns]


def valid_topic_pattern(pattern: str) -> bool:
    """+ 可出现在任意层；# 只能作为最后一段且独占一层。"""
    parts = pattern.split("/")
    for i, seg in enumerate(parts):
        if "#" in seg and not (seg == "#" and i == len(parts) - 1):
            return False
        if "+" in seg and seg != "+":
            return False
    return True


def topic_matches(pattern: str, topic: str) -> bool:
    pp = pattern.split("/")
    tp = topic.split("/")
    for i, seg in enumerate(pp):
        if seg == "#":
            return len(tp) >= i and tp[:i] == pp[:i]
        if i >= len(tp):
            return False
        if seg == "+":
            continue
        if seg != tp[i]:
            return False
    return len(pp) == len(tp)
