# mqtt-influx-bridge

常驻 MQTT 订阅 → 本地 SQLite WAL → 批量写入 InfluxDB。仅一个第三方依赖（paho-mqtt），HTTP 用标准库。

## 设计取舍

官方 InfluxDB 3 的 MQTT Subscriber 插件是「定时触发 + 短连 + paho 自动 PUBACK」，ACK 早于落库。本实现把顺序反过来：

```
MQTT broker (QoS 1, 固定 client_id, 持久会话)
     │  PUBLISH
     ▼
paho 网络线程 · manual_ack=True
     │  on_message：解析 → 同步写入 SQLite（synchronous=FULL）
     │  ├─ 成功 → PUBACK（persisted 模式）
     │  └─ 成功 → 等 InfluxDB 2xx 后再 PUBACK（written 模式）
     ▼
SQLite WAL  ./data/bridge.db   ← 进程崩溃后重启自动续传
     │  writer 线程：批量取 1000 行 → POST（gzip，整批原子）
     ├─ 2xx        → 删除
     ├─ 5xx / 网络  → 指数退避，留在队列
     └─ 4xx / 超限  → 转 DLQ，不阻塞后续消息
     ▼
InfluxDB  v1 /write · v2 /api/v2/write · v3 /api/v3/write_lp
```

**关键点**：ACK 对齐「本地持久化」，而不是对齐「远端落库」。`written` 模式提供后者，由 writer 线程异步结算 ACK 等待者，不阻塞 paho 网络线程。两种模式都不丢数；区别只在确认时机与背压强度。

## 快速开始

```bash
pip install -r requirements.txt
cp config.example.toml config.toml      # Windows: copy config.example.toml config.toml
# 编辑 config.toml：broker 地址、topics、InfluxDB 版本/地址/库名
python -m mqtt_influx_bridge check      # 校验配置（token 已脱敏）
python -m mqtt_influx_bridge run        # 启动；Ctrl+C 优雅退出
```

离线验证（不需要真实 broker 或 InfluxDB）：

```bash
python -m unittest discover -s tests -v
```

敏感值用环境变量覆盖，避免写进配置文件：

```bash
export MQTT_USERNAME=xxx MQTT_PASSWORD=xxx INFLUX_TOKEN=xxx INFLUX_DATABASE=sensors
export MQTT_HOST=broker.example.com INFLUX_URL=http://127.0.0.1:8181
```

## 两种 ACK 模式

| | `persisted`（默认） | `written` |
|---|---|---|
| PUBACK 时机 | SQLite 提交后立即 | InfluxDB 返回 2xx 或转入 DLQ 后 |
| 吞吐 | 高，与 DB 延迟解耦 | 受 DB 延迟影响，靠未确认窗口形成背压 |
| 崩溃后 | WAL 里的数据重启续传 | 未确认消息由 broker 重投 + WAL 续传（双重保护） |
| 适用 | 绝大多数场景 | 要求「DB 没收到就等于没消费」 |

`written` 模式用 `writer.ack_timeout_seconds` 防止死等；超时放弃确认，由 broker 重投，靠 payload 时间戳幂等兜底。

## 必须配对的 4 件事

1. **发布端也用 QoS 1/2**，且订阅端固定 `client_id` + `persistent_session=true`。否则离线期间消息在 broker 侧不排队。
2. **`format.timestamp_path` 必填**。时间戳从 payload 取，重投时同一 `(measurement, tagset, timestamp)` 覆盖，天然幂等。留空会用接收时刻，重投 = 重复点。
3. **`influx.accept_partial=false`**（v3）。整批原子，避免部分成功后整批重试写成两份。
4. **监控三处**：日志里的运行指标、`mqtt_influx_bridge status`、DLQ 表。

## 配置速查

| 段 | 关键项 | 说明 |
|---|---|---|
| `mqtt` | `qos=1`、`client_id` 固定、`persistent_session=true` | 可靠性前提 |
| `mqtt` | `tls_ca` / `tls_cert` / `tls_key` | 配了才启用 TLS；明文凭据会告警 |
| `influx` | `version=v1\|v2\|v3` | 自动选端点，时间戳统一纳秒 |
| `influx` | `no_sync=false` | v3：等 WAL 持久化才返回 2xx |
| `format` | `type=json\|lineprotocol` | JSON 支持数组、嵌套路径、`@topic[N]`、类型 `:float/:int/:bool/:str` |
| `format` | `timestamp_unit` | `lineprotocol` 模式下若原始时间戳已是纳秒，须设为 `ns`，否则会被二次换算 |
| `wal` | `max_pending` | 高水位；`when_full=block` 时暂不 ACK，借 broker 窗口背压 |
| `writer` | `batch_size` / `flush_interval_ms` | 批量与延迟 |
| `writer` | `max_retries=0` | 无限重试；>0 超限转 DLQ |

JSON 映射示例（`sensors/dev-01/telemetry`）：

```json
{"ts": 1638360000000, "device_id": "dev-01", "location": "wh", "temp": 22.5, "online": true, "seq": 7}
```

```
sensor_data,source=mqtt,device=dev-01,site=wh temperature=22.5,online=true,seq=7i 1638360000000000000
```

## 运维

```bash
python -m mqtt_influx_bridge status          # pending / 最老积压 / 最大重试次数 / DLQ 数
sqlite3 data/bridge.db "SELECT * FROM dlq ORDER BY id DESC LIMIT 20;"
sqlite3 data/bridge.db "DELETE FROM dlq WHERE id = 123;"   # 处理后清理
```

程序重启自动续传 `pending` 行；不需要额外操作。日志每 30s 打印一次指标（`log.stats_interval_seconds` 可调）。

## 上线前验证

1. 压测：按峰值速率观察 `pending` 是否持续增长，据此调 `batch_size` / `flush_interval_ms`。
2. 断网：停掉 InfluxDB 发消息 → `pending` 增长且无数据丢失 → 恢复后自动清空。
3. 杀进程：`kill -9` 后重启，比对 broker 发布数与 DB 点数。
4. 重投：手动重复发同一条 payload，DB 点数不增。
5. 毒药消息：发一条非法 JSON，确认进 DLQ 且不阻塞后续消息。

## 已知限制

- 单进程单 writer 线程；横向扩展需多实例 + MQTT 5 共享订阅（`$share`），或按 topic 分片，且每个实例用独立 `client_id` 与独立 WAL 文件。
- `written` 模式下如果 InfluxDB 长时间不可用，未确认窗口会填满（MQTT 3.1.1 默认 20 条 in-flight），此时表现为背压而非丢数；可调 `max_inflight_messages_set` 或改用 `persisted`。
- WAL 触顶时最多等待 5 秒（避免阻塞 paho 网络线程导致 keepalive 超时），之后不 ACK，由 broker 重投。
- DLQ 只存原始行与原因，不做自动重放，需要人工处理。
- v1 用 Basic auth，v2 用 `Token`，v3 用 `Bearer`；未启用鉴权的服务端可留空 token。

## 参考

- [InfluxDB 3 `/api/v3/write_lp`](https://docs.influxdata.com/influxdb3/core/write-data/http-api/v3-write-lp/)
- [InfluxDB 3 写端点选择（v1/v2/v3）](https://docs.influxdata.com/influxdb3/core/write-data/)
- [MQTT Subscriber 官方插件文档](https://docs.influxdata.com/influxdb3/core/plugins/library/official/mqtt-subscriber/)
- [Telegraf mqtt_consumer](https://www.influxdata.com/integration/mqtt-telegraf-consumer/)

## CI/CD 与产物

仓库内置两条 GitHub Actions 流水线（`.github/workflows/`）：

- **CI**（`ci.yml`）：每次 push / PR 到 `main`，在 Python 3.11 / 3.12 / 3.13 矩阵上 `compileall` + 跑全部单测；通过后 `python -m build` 产出 wheel + sdist 并作为 artifact 上传。
- **Release**（`release.yml`）：推送 `v*` tag 时自动触发，构建 wheel + sdist 并打包一个可直接部署的 zip（`mqtt-influx-bridge-<版本>.zip`，含包代码、`config.example.toml`、`requirements.txt`、`README`、`LICENSE`），然后发布到 GitHub Release 并附带上述产物；每次运行也会把产物上传为 artifact。

本地产出同样产物：

```bash
python -m pip install build
python -m build                      # 生成 dist/*.whl 与 dist/*.tar.gz
pip install dist/*.whl               # 安装后可用 mqtt-influx-bridge 命令
```

版本号在 `pyproject.toml` 与 `mqtt_influx_bridge/__init__.py.__version__` 两处保持一致；打 tag 用 `git tag v0.1.0 && git push --tags`。
