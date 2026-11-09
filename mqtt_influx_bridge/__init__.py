"""InfluxDB 自研 MQTT 桥接器。

核心保证：消息同步落盘到 SQLite WAL 后才允许 PUBACK；
writer 线程批量写入 InfluxDB，成功后删除，失败退避重试，永不静默丢数。
"""
__version__ = "0.2.0"
