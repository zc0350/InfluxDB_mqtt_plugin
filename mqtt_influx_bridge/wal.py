"""SQLite 持久化队列（WAL）+ 死信队列。

ACK 语义的关键：消息在 on_message 里同步提交到这里（synchronous=FULL）
之后才允许 PUBACK，因此「已确认」==「已落盘」，进程崩溃不丢数据。
"""
from __future__ import annotations

import random
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, NamedTuple


class Row(NamedTuple):
    id: int
    line: str
    topic: str
    received_at: float
    attempts: int
    next_retry: float


def _chunks(values: list[int], size: int = 400) -> Iterable[list[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


class Wal:
    def __init__(self, path: str | Path):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(p), check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS pending (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   line TEXT NOT NULL,
                   topic TEXT NOT NULL,
                   received_at REAL NOT NULL,
                   attempts INTEGER NOT NULL DEFAULT 0,
                   next_retry REAL NOT NULL DEFAULT 0
               )"""
        )
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS dlq (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   original_id INTEGER,
                   line TEXT,
                   topic TEXT,
                   reason TEXT,
                   received_at REAL,
                   failed_at REAL
               )"""
        )
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_pending_retry ON pending(next_retry)")

    def append(self, lines: list[str], topic: str) -> list[int]:
        now = time.time()
        with self._lock:
            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                ids = []
                for line in lines:
                    cur.execute(
                        "INSERT INTO pending(line, topic, received_at, next_retry) VALUES (?, ?, ?, 0)",
                        (line, topic, now),
                    )
                    ids.append(int(cur.lastrowid))
                cur.execute("COMMIT")
                return ids
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def fetch(self, limit: int, now: float | None = None) -> list[Row]:
        now = time.time() if now is None else now
        with self._lock:
            rows = self._db.execute(
                "SELECT id, line, topic, received_at, attempts, next_retry "
                "FROM pending WHERE next_retry <= ? ORDER BY id LIMIT ?",
                (now, limit),
            ).fetchall()
        return [Row(*r) for r in rows]

    def delete(self, ids: list[int]) -> None:
        if not ids:
            return
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for chunk in _chunks(ids):
                    marks = ",".join("?" * len(chunk))
                    self._db.execute(f"DELETE FROM pending WHERE id IN ({marks})", chunk)
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def defer(self, ids: list[int], base_ms: int, max_ms: int) -> None:
        if not ids:
            return
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for row_id in ids:
                    r = self._db.execute(
                        "SELECT attempts FROM pending WHERE id = ?", (row_id,)
                    ).fetchone()
                    if r is None:
                        continue
                    attempts = int(r[0]) + 1
                    backoff = min(base_ms * (2 ** min(attempts - 1, 20)), max_ms) / 1000.0
                    backoff *= random.uniform(0.8, 1.2)
                    self._db.execute(
                        "UPDATE pending SET attempts = ?, next_retry = ? WHERE id = ?",
                        (attempts, now + backoff, row_id),
                    )
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def to_dlq(self, rows: list[Row], reason: str) -> None:
        if not rows:
            return
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.executemany(
                    "INSERT INTO dlq(original_id, line, topic, reason, received_at, failed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [(r.id, r.line, r.topic, reason[:500], r.received_at, now) for r in rows],
                )
                for chunk in _chunks([r.id for r in rows]):
                    marks = ",".join("?" * len(chunk))
                    self._db.execute(f"DELETE FROM pending WHERE id IN ({marks})", chunk)
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def to_dlq_raw(self, line: str, topic: str, reason: str) -> None:
        now = time.time()
        with self._lock:
            self._db.execute(
                "INSERT INTO dlq(original_id, line, topic, reason, received_at, failed_at) "
                "VALUES (NULL, ?, ?, ?, ?, ?)",
                (line[:2000], topic, reason[:500], now, now),
            )

    def exists_any(self, ids: list[int]) -> bool:
        with self._lock:
            for chunk in _chunks(ids):
                marks = ",".join("?" * len(chunk))
                if self._db.execute(
                    f"SELECT 1 FROM pending WHERE id IN ({marks}) LIMIT 1", chunk
                ).fetchone():
                    return True
        return False

    def pending_count(self) -> int:
        with self._lock:
            return int(self._db.execute("SELECT COUNT(*) FROM pending").fetchone()[0])

    def stats(self) -> dict:
        with self._lock:
            pending = self._db.execute("SELECT COUNT(*), MIN(received_at), MAX(attempts) FROM pending").fetchone()
            dlq = int(self._db.execute("SELECT COUNT(*) FROM dlq").fetchone()[0])
        return {
            "pending": int(pending[0]),
            "oldest_pending_age_seconds": round(time.time() - pending[1], 1) if pending[1] else 0,
            "max_attempts": int(pending[2] or 0),
            "dlq": dlq,
        }

    def close(self) -> None:
        with self._lock:
            self._db.close()
