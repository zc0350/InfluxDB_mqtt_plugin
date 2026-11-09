"""MQTT -> 本地 WAL -> InfluxDB 常驻桥接服务。

两种 ACK 语义（writer.ack_mode）：
  persisted : 消息同步落盘到 SQLite 后才 PUBACK。吞吐高，崩溃不丢。
  written   : InfluxDB 返回 2xx（或进 DLQ）后才 PUBACK。严格「落库再确认」，
              不阻塞 paho 网络线程：由 writer 线程异步结算 ACK 等待者。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from .admin import AdminClient, BackendError
from .config import Config, MqttConfig
from .lineprotocol import ParseError
from .routing import parse_payload
from .wal import Wal
from .writer import InfluxWriter, TransientError, flush_once

log = logging.getLogger(__name__)

ClientFactory = Callable[[MqttConfig, Callable, Callable, Callable], Any]


@dataclass
class Counters:
    received: int = 0
    lines: int = 0
    acked: int = 0
    parse_errors: int = 0
    batches_ok: int = 0
    batches_retry: int = 0
    batches_dlq: int = 0

    def snapshot(self) -> dict:
        return {
            "received": self.received,
            "lines": self.lines,
            "acked": self.acked,
            "parse_errors": self.parse_errors,
            "batches_ok": self.batches_ok,
            "batches_retry": self.batches_retry,
            "batches_dlq": self.batches_dlq,
        }


@dataclass
class _AckWaiter:
    mid: int
    qos: int
    ids: list[int]
    deadline: float


def _paho_client_factory(
    cfg: MqttConfig, on_connect: Callable, on_message: Callable, on_disconnect: Callable
):
    """兼容 paho-mqtt 1.x / 2.x，强制开启 manual_ack。"""
    try:
        import paho.mqtt.client as mqtt
    except ImportError as e:
        raise RuntimeError("缺少依赖 paho-mqtt，请先执行: pip install -r requirements.txt") from e

    kwargs = dict(
        client_id=cfg.client_id,
        clean_session=not cfg.persistent_session,
        protocol=mqtt.MQTTv311,
        manual_ack=True,
    )
    version2 = getattr(getattr(mqtt, "CallbackAPIVersion", None), "VERSION2", None)
    if version2 is None:                       # paho 1.x
        client = mqtt.Client(**kwargs)
    else:                                      # paho 2.x
        try:
            client = mqtt.Client(version2, **kwargs)
        except TypeError:                      # 2.0 无 manual_ack 构造参数
            base = {k: v for k, v in kwargs.items() if k != "manual_ack"}
            client = mqtt.Client(version2, **base)
            client.manual_ack_set(True)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    if cfg.username:
        client.username_pw_set(cfg.username, cfg.password or None)
    if cfg.tls_ca or cfg.tls_cert:
        client.tls_set(
            ca_certs=cfg.tls_ca or None,
            certfile=cfg.tls_cert or None,
            keyfile=cfg.tls_key or None,
        )
        if cfg.insecure_skip_verify:
            client.tls_insecure_set(True)
    return client


class Bridge:
    def __init__(self, cfg: Config, client_factory: ClientFactory | None = None):
        self.cfg = cfg
        self.wal = Wal(cfg.wal.path)
        self.writer = InfluxWriter(cfg.influx)
        self._factory = client_factory or _paho_client_factory
        self._client: Any = None
        self._stop = threading.Event()
        self._closed = False
        self._ack_lock = threading.RLock()
        self._waiters: list[_AckWaiter] = []
        self._writer_thread: threading.Thread | None = None
        self.stats = Counters()

    # ---------- MQTT 回调 ----------

    def on_connect(self, *args) -> None:
        client = args[0]
        rc = args[3] if len(args) >= 4 else 0
        failed = rc.is_failure if hasattr(rc, "is_failure") else rc != 0
        if failed:
            log.error("MQTT 连接失败: %s", rc)
            return
        topics = self.cfg.subscribe_topics
        log.info("MQTT 已连接，订阅 %s (qos=%d)，路由 %d 条", topics, self.cfg.mqtt.qos, len(self.cfg.routes))
        for topic in topics:
            client.subscribe(topic, qos=self.cfg.mqtt.qos)

    def on_disconnect(self, *args) -> None:
        rc = args[3] if len(args) >= 5 else (args[2] if len(args) >= 3 else 0)
        log.warning("MQTT 连接断开 (rc=%s)，后台线程将自动重连", rc)

    def on_message(self, client, userdata, msg) -> None:
        topic = msg.topic
        mid = getattr(msg, "mid", 0)
        qos = getattr(msg, "qos", 0)
        self.stats.received += 1
        try:
            lines = parse_payload(msg.payload, self.cfg.routes, self.cfg.mapping, topic)
        except ParseError as e:
            self.stats.parse_errors += 1
            raw = (
                msg.payload[:2000].decode("utf-8", "replace")
                if isinstance(msg.payload, bytes)
                else str(msg.payload)[:2000]
            )
            self.wal.to_dlq_raw(raw, topic, f"parse: {e}")
            log.warning("解析失败，原始消息进 DLQ [%s]: %s", topic, e)
            self._ack(client, mid, qos)  # 毒药消息也确认，避免无限重投
            return

        if not self._wait_for_capacity():
            if self.cfg.writer.when_full == "drop":
                for line in lines:
                    self.wal.to_dlq_raw(line, topic, "WAL 高水位，按配置丢弃")
                self._ack(client, mid, qos)
                log.error("WAL 高水位，消息转入 DLQ [%s]", topic)
            else:
                log.error("WAL 高水位且等待超时，暂不 ACK，等 broker 重投 [%s]", topic)
            return

        ids = self.wal.append(lines, topic)
        self.stats.lines += len(lines)
        mode = self.cfg.writer.ack_mode if qos > 0 else "persisted"
        if mode == "persisted":
            self._ack(client, mid, qos)
        else:
            with self._ack_lock:
                self._waiters.append(
                    _AckWaiter(
                        mid=mid,
                        qos=qos,
                        ids=ids,
                        deadline=time.time() + self.cfg.writer.ack_timeout_seconds,
                    )
                )

    # ---------- ACK ----------

    def _ack(self, client, mid: int, qos: int) -> None:
        if qos and qos > 0:
            try:
                client.ack(mid, qos)
                self.stats.acked += 1
            except Exception as e:
                log.error("PUBACK 失败 mid=%s: %s", mid, e)

    def _settle_acks(self) -> None:
        """written 模式：仅当该消息的所有行都离开 pending 表后才确认。"""
        if self._client is None:
            return
        now = time.time()
        with self._ack_lock:
            keep = []
            for w in self._waiters:
                if not self.wal.exists_any(w.ids):
                    self._ack(self._client, w.mid, w.qos)
                elif now > w.deadline:
                    log.warning(
                        "ACK 超时 mid=%s：数据仍在 WAL，放弃确认，依赖 broker 重投 + 时间戳幂等",
                        w.mid,
                    )
                else:
                    keep.append(w)
            self._waiters = keep

    def _wait_for_capacity(self) -> bool:
        # 上限 5 秒：避免长时间阻塞 paho 网络线程导致 keepalive 超时；
        # 等待失败则不 ACK，由 broker 重投（payload 时间戳保证幂等）。
        deadline = time.time() + min(self.cfg.writer.ack_timeout_seconds, 5.0)
        while self.wal.pending_count() >= self.cfg.wal.max_pending:
            if self._stop.is_set() or time.time() > deadline:
                return False
            time.sleep(0.05)
        return True

    # ---------- writer 线程 ----------

    def _on_batch_result(self, outcome: str, ids: list[int]) -> None:
        if outcome == "ok":
            self.stats.batches_ok += 1
        elif outcome == "retry":
            self.stats.batches_retry += 1
        else:
            self.stats.batches_dlq += 1

    def _writer_loop(self) -> None:
        interval = max(self.cfg.writer.flush_interval_ms, 10) / 1000.0
        last_stats = time.time()
        while not self._stop.is_set():
            try:
                n = flush_once(self.wal, self.writer, self.cfg.writer, self._on_batch_result)
            except Exception as e:
                log.exception("刷写循环异常: %s", e)
                n = 0
            if self.cfg.writer.ack_mode == "written":
                self._settle_acks()
            if n == 0:
                self._stop.wait(interval)
            if time.time() - last_stats >= max(self.cfg.log.stats_interval_seconds, 1):
                log.info("运行指标 %s | 队列 %s", self.stats.snapshot(), self.wal.stats())
                last_stats = time.time()

    # ---------- 生命周期 ----------

    def _ensure_backend(self) -> None:
        """启动时把库/bucket 备好（幂等）。

        失败只告警不退出：不丢数靠本地 WAL，不靠这一次调用。
        """
        if not self.cfg.influx.auto_create_database:
            return
        admin = AdminClient(self.cfg.influx)
        try:
            state, detail = admin.ensure_database(self.cfg.influx.retention_period)
            log.info("目标库就绪 %s -> %s", state, detail)
        except (TransientError, BackendError, ValueError) as e:
            log.warning("自动建库未成功（%s），继续启动；需要时手动跑 init-db", e)
        finally:
            admin.close()

    def request_stop(self) -> None:
        self._stop.set()

    def start(self) -> None:
        self._ensure_backend()
        self._client = self._factory(
            self.cfg.mqtt, self.on_connect, self.on_message, self.on_disconnect
        )
        self._writer_thread = threading.Thread(
            target=self._writer_loop, name="influx-writer", daemon=True
        )
        self._writer_thread.start()
        c = self.cfg.mqtt
        log.info(
            "启动桥接 mqtt=%s:%d topics=%s -> influx %s (%s) ack_mode=%s qos=%d",
            c.host, c.port, self.cfg.subscribe_topics, self.cfg.influx.url,
            self.cfg.influx.version, self.cfg.writer.ack_mode, c.qos,
        )
        self._client.connect_async(c.host, c.port, c.keepalive)
        self._client.loop_start()
        try:
            while not self._stop.is_set():
                self._stop.wait(0.5)
        finally:
            self.stop()

    def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._writer_thread is not None:
            self._writer_thread.join(timeout=5)
        if self._client is not None:
            try:
                self._client.disconnect()
                self._client.loop_stop()
            except Exception:
                pass
        self.writer.close()
        log.info("已停止。指标 %s | 队列 %s", self.stats.snapshot(), self.wal.stats())
        self.wal.close()
