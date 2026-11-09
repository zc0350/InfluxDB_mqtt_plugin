# mqtt-influx-bridge

常驻 MQTT 订阅 → 本地 SQLite WAL → 批量写入 InfluxDB。仅一个第三方依赖（paho-mqtt），HTTP 用标准库。

按 topic 路由到不同表、列按 `[[route]]` 显式声明、表名按年或按月分片（`mqtt_data_2026` / `mqtt_data_202611`）的完整配置与上线方案见 [deploy.md](deploy.md)；`deploy/` 下有 docker-compose 与 systemd 部署产物。

## 设计取舍

官方 InfluxDB 3 的 MQTT Subscriber 插件是「定时触发 + 短连 + paho 自动 PUBACK」，ACK 早于落库。本实现把顺序反过来：

```
MQTT broker (QoS 1, 固定 client_id, 持久会话)
     │  PUBLISH
     ▼
paho 网络线程 · manual_ack=True
     │  on_message：topic 命中 [[route]] → 映射成 line protocol（表名带年份）→ 同步写 SQLite（synchronous=FULL）
     │  ├─ 成功 → PUBACK（persisted 模式）
     │  └─ 成功 → 等 InfluxDB 2xx 后再 PUBACK（written 模式）
     ▼
SQLite WAL  ./data/bridge.db   ← 进程崩溃后重启自动续传
     │  writer 线程：批量取 1000 行 → POST（gzip，整批原子）
     ├─ 2xx        → 删除
     ├─ 5xx / 网络  → 指数退避，留在队列
     └─ 4xx / 超限  → 二分隔离坏行：好行落库，坏行转 DLQ，判不出的退回重试
     ▼
InfluxDB  v1 /write · v2 /api/v2/write · v3 /api/v3/write_lp
```

**关键点**：ACK 对齐「本地持久化」，而不是对齐「远端落库」。`written` 模式提供后者，由 writer 线程异步结算 ACK 等待者，不阻塞 paho 网络线程。两种模式都不丢数；区别只在确认时机与背压强度。

## 快速开始

```bash
pip install -r requirements.txt
cp config.example.toml config.toml      # Windows: copy config.example.toml config.toml
# 编辑 config.toml：broker 地址、[[route]]（topic -> 表 + 映射规则）、[mapping]（切年时区 / topic 占位符）、InfluxDB 版本/地址/库名
python -m mqtt_influx_bridge check      # 校验配置（token 已脱敏；会打印实际订阅列表与每条路由的目标表）
python -m mqtt_influx_bridge -c config.toml sample --topic "sensors/dev-01/telemetry" \
    --payload '{"ts":1638360000000,"device_id":"dev-01","temp":22.5}'    # 上线前干跑映射
python -m mqtt_influx_bridge run        # 启动；Ctrl+C 优雅退出
```

`sample` 不连 broker 也不连库，只把一条 payload 过一遍路由 + 映射并打印生成的 line protocol——表名、列名和直觉不符时先用它确认。TT1000 场景可以直接 `python -m mqtt_influx_bridge -c config.tt1000.toml check` 看整套路由怎么落地。

离线验证（不需要真实 broker 或 InfluxDB）：

```bash
python -m unittest discover -s tests -v
```

敏感值用环境变量覆盖，避免写进配置文件（下面是 `_ENV_MAP` 全集）：

```bash
export MQTT_HOST=broker.example.com MQTT_PORT=1883 MQTT_QOS=1 MQTT_CLIENT_ID=bridge-01
export MQTT_USERNAME=xxx MQTT_PASSWORD=xxx MQTT_TOPICS=sensors/dev-01/telemetry,sensors/dev-02/telemetry
export INFLUX_URL=http://127.0.0.1:8181 INFLUX_VERSION=v3 INFLUX_DATABASE=sensors INFLUX_ORG= INFLUX_TOKEN=xxx
export INFLUX_AUTO_CREATE_DB=true INFLUX_RETENTION_PERIOD=
export PRODUCT_KEY=TT1000 YEAR_TZ_OFFSET_MINUTES=480
export WAL_PATH=./data/bridge.db BRIDGE_ACK_MODE=persisted LOG_LEVEL=INFO
```

`MQTT_TOPICS` 逗号分隔。`PRODUCT_KEY` 写进 `mapping.substitutions` 的 `productKey`，路由 topic 里的 `${productKey}` 靠它收紧；不给则 `${...}` 一律按 `+` 处理（订阅全部产品）。`-c` 的默认路径另由 `BRIDGE_CONFIG` 决定。

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
2. **每条 `[[route]].timestamp_path` 必填**。时间戳从 payload 取，重投时同一 `(表名, tagset, timestamp)` 覆盖，天然幂等；分表名（`mqtt_data_202611`）也由同一个时间戳推出，所以 broker 重投、补历史数据都落回原本那张表。留空会回落接收时刻，重投 = 重复点，跨分表周期还会跑表。
3. **`influx.accept_partial=false`**（v3）。整批原子，避免部分成功后整批重试写成两份。
4. **监控三处**：日志里的运行指标、`mqtt_influx_bridge status`、DLQ 表。

## 配置速查

| 段 | 关键项 | 说明 |
|---|---|---|
| `mqtt` | `qos=1`、`client_id` 固定、`persistent_session=true` | 可靠性前提 |
| `mqtt` | `topics=[]` | 留空 = 自动订阅所有 `[[route]].topics` 的并集（去重、保持声明顺序） |
| `mqtt` | `tls_ca` / `tls_cert` / `tls_key` | 配了才启用 TLS；明文凭据会告警 |
| `influx` | `version=v1\|v2\|v3` | 选端点与鉴权头，管理面（建库/探活/查询/删表）按它分派；时间戳统一纳秒。边界见 [deploy.md](deploy.md) §12 |
| `influx` | `auto_create_database=true` | 启动时幂等建库/建 bucket；失败只告警不拦启动（可靠性靠 WAL，不靠这一次调用），`init-db` 可手动重试 |
| `influx` | `retention_period=""` | 建库时的保留期（`30d`/`4w`/`1y`）；库已存在则不生效，v3 建库后改不了，v1 忽略（该用 retention policy） |
| `influx` | `no_sync=false` | v3：等 WAL 持久化才返回 2xx |
| `route` | `name` / `topics` | 一条 `[[route]]` = 一类 topic；按声明顺序首个命中生效，topic 支持 `+`、`#` 与 `${var}` |
| `route` | `type=json\|lineprotocol` | JSON 支持数组、嵌套路径 `a.b.0`、`@topic[N]`、字段类型 `:float/:int/:bool/:str`；`lineprotocol` 原样透传 |
| `route` | `measurement` / `measurement_path` / `year_table` / `table_period` | 表名来源；`year_table=true` 时追加时间后缀，`table_period=year\|month` 决定粒度 → `mqtt_data_2026` / `mqtt_data_202611`；`measurement_path` 从 payload 动态取基础表名 |
| `route` | `timestamp_path` / `timestamp_unit` | 单位 `ns\|us\|ms\|s\|auto\|datetime`；`auto` 按数值量级判档（秒/毫秒/微秒/纳秒混发不用拆路由），同时决定落哪张分表；该 key 缺失/`null`/空串/`0` 或负数（RTC 未同步）时回落接收时刻；`lineprotocol` 若原始时间戳已是纳秒须设 `ns`，否则会被二次换算 |
| `route` | `mode=auto\|explicit` | auto：未被占用的 key 全部自动成 field；explicit：只写 `fields` 声明的列（`fields` 可以为空，只要还存 `blob_field` / `topic_field`） |
| `route` | `tags` / `static_tags` / `fields` | tag/field 名 -> payload 路径或 `@topic[N]`；`static_tags` 是固定值（如 `direction`）；tag 总数 >5 会告警（series 基数按 tag 组合相乘） |
| `route` | `blob_field` / `topic_field` | 完整 payload JSON 与原始 topic 的列名；留空 = 不存（auto 模式不存 `blob_field` 会告警，字段漂移后无法回溯） |
| `route` | `type_policy=infer\|string` | infer：数值转 float；string：一律转字符串，字段类型反复漂移的设备用它，零类型冲突但没法做数值聚合 |
| `route` | `key_case=lower\|keep` / `exclude_keys` | 自动展开的 key 是否转小写（`deviceCode` -> `devicecode`），手写列名不受影响；`exclude_keys` 屏蔽个别噪音 key |
| `mapping` | `year_tz_offset_minutes=480` | 分表后缀按哪个时区切周期（480 = UTC+8，0 = UTC）；被 `YEAR_TZ_OFFSET_MINUTES` 覆盖 |
| `mapping` | `substitutions` | topic 里 `${productKey}` 的取值表；`PRODUCT_KEY` 环境变量等价于写 `productKey`；未提供的占位符按 `+` 处理 |
| `wal` | `max_pending` | 高水位；`when_full=block` 时暂不 ACK，借 broker 窗口背压 |
| `writer` | `batch_size` / `flush_interval_ms` | 批量与延迟 |
| `writer` | `max_retries=0` | 无限重试；>0 超限转 DLQ |
| `writer` | `split_on_reject=true` / `max_split_probes=64` | 整批被 4xx 拒绝时二分定位坏行：好行落库、确认坏的进 DLQ、判不出的退回重试——一行类型冲突不再拖垮同批上千行 |

一条 topic 都没被 `[[route]]` 命中时消息进 DLQ（不静默丢）。想让它正常入库并顺便发现新设备/新 topic，就在最后加一条兜底路由 `topics = ["#"]`，配一个 `static_tags = { matched = "false" }` 便于事后筛。

映射示例（`config.tt1000.toml` 的 `uplink` 路由，节选）：

```toml
[[route]]
name = "uplink"
topics = ["${productKey}/+/data"]     # 给了 PRODUCT_KEY 就收紧，不给按 +/+/data 订阅全部产品
measurement = "mqtt_data"
year_table = true                     # 表名追加时间后缀
table_period = "month"                # 实际写入 mqtt_data_<年><月>
timestamp_path = "time"
timestamp_unit = "auto"               # 1793936103 这类秒级时间戳自动识别
mode = "explicit"                     # 只建声明过的列，其余 key 只留在 data 里
tags = { event = "event", devicecode = "deviceCode", imei = "imei" }
static_tags = {}
fields = { code = "code:float", door_num = "door_num" }
blob_field = "data"
topic_field = "topic"
key_case = "lower"
```

`alarms` 消息干跑（`sample` 只要配置文件，不连 broker 也不连库；stderr 上的配置告警略去）：

```bash
python -m mqtt_influx_bridge -c config.tt1000.toml sample --topic "TT1000/TT10002/data" --payload '{"event":"alarms","deviceCode":"TT10002","door_num":[1],"code":1,"time":1793936103}'
```

```json
{
  "topic": "TT1000/TT10002/data",
  "route": "uplink",
  "table": "mqtt_data_202611",
  "lines": [
    "mqtt_data_202611,event=alarms,devicecode=TT10002 code=1.0,door_num=\"[1]\",data=\"{\\\"event\\\":\\\"alarms\\\",\\\"deviceCode\\\":\\\"TT10002\\\",\\\"door_num\\\":[1],\\\"code\\\":1,\\\"time\\\":1793936103}\",topic=\"TT1000/TT10002/data\" 1793936103000000000"
  ]
}
```

逐列对上：

- 表名 `mqtt_data_202611` 的年月来自 payload 的 `time`，不是接收时刻；同一条重投永远回到这张表的同一行。
- `mode=explicit` 后查询面就是 `fields` 那一行：`code`、`door_num` 两个 field 加 `event` / `devicecode` / `imei` 三个 tag，`sigal` / `door_status` / `server_id` / `version` / `cam` / `sd` 这些不再成列，只留在 `data` 里归档。Core 没有 JSON 函数也没有 backfill，**列一旦定了就只能往前改**，所以先想清楚要查什么再声明。
- 没出现在这条 payload 里的列直接不写（alarms 没有 `imei`，status 没有 `code`），不会产生空值列，也不影响幂等键。
- 数组按紧凑 JSON 落字符串（`door_num="[1]"`），数值按声明转 float（`code=1.0`）。
- `data` 是完整 payload 的规范化 JSON，`topic` 是原始 topic，行尾是纳秒时间戳。status / control / response 三类 payload 的映射结果见 [deploy.md](deploy.md) §1.2。

## 运维

```bash
python -m mqtt_influx_bridge status          # pending / 最老积压 / 最大重试次数 / DLQ 数
python -m mqtt_influx_bridge -c config.toml sample --topic "TT1000/TT10002/data" --payload '{"event":"alarms","time":1793936103}'   # 表名/列名有疑问先干跑

# 后端管理面（按 influx.version 分派，v1/v2/v3 各家走各自的原生接口）
python -m mqtt_influx_bridge ping              # 探活：v1 /ping、v2/v3 /health；连不上退出码 1
python -m mqtt_influx_bridge init-db           # 幂等建库/建 bucket，打印 created|exists|ignored
python -m mqtt_influx_bridge tables            # 有哪些表（InfluxQL / Flux / SQL 各家方言）
python -m mqtt_influx_bridge query --sql 'SELECT count(*) FROM mqtt_data_202611'   # 方言不互转
python -m mqtt_influx_bridge purge-table --table mqtt_data_2023            # 干跑，只打印
python -m mqtt_influx_bridge purge-table --table mqtt_data_2023 --execute [--hard]
sqlite3 data/bridge.db "SELECT * FROM dlq ORDER BY id DESC LIMIT 20;"
sqlite3 data/bridge.db "DELETE FROM dlq WHERE id = 123;"   # 处理后清理
```

死信的重放不必手写 SQL：`Wal.requeue_dlq(dlq_id)` 把一条死信放回待写队列（`drop_dlq` 则确认放弃）。容器化部署里镜像没有 sqlite3 CLI，用 `bash deploy/scripts/dlq.sh`（走 `docker compose exec bridge`，读 `BRIDGE_CONFIG`，默认 `/config/config.toml`）：

- `dlq.sh`（= `list`）：打印最近 20 条的 id、topic、拒绝原因和行内容。
- `dlq.sh requeue 12`：放回 pending 重试。先 `docker compose stop bridge` → 改好 `[[route]]` / `[mapping]` → requeue → `docker compose up -d bridge`，否则会原样再进 DLQ。
- `dlq.sh drop 12`：确认放弃这条。

程序重启自动续传 `pending` 行；不需要额外操作。日志每 30s 打印一次指标（`log.stats_interval_seconds` 可调）。部署、备份与分表清理见 [deploy.md](deploy.md)。

## 上线前验证

1. 压测：按峰值速率观察 `pending` 是否持续增长，据此调 `batch_size` / `flush_interval_ms`。
2. 断网：停掉 InfluxDB 发消息 → `pending` 增长且无数据丢失 → 恢复后自动清空。
3. 杀进程：`kill -9` 后重启，比对 broker 发布数与 DB 点数。
4. 重投：手动重复发同一条 payload，DB 点数不增。
5. 毒药消息：发一条非法 JSON，确认进 DLQ 且不阻塞后续消息；再发一条字段类型与已有数据冲突的行，确认 `writer.split_on_reject` 只把那一行送进 DLQ、同批好行照常落库。
6. 路由漏配：发一条没有 `[[route]]` 命中的 topic，确认它进 DLQ（或落 `topics = ["#"]` 那条兜底路由）而不是被静默丢弃。

## 已知限制

- 单进程单 writer 线程；横向扩展需多实例 + MQTT 5 共享订阅（`$share`），或按 topic 分片，且每个实例用独立 `client_id` 与独立 WAL 文件。
- `written` 模式下如果 InfluxDB 长时间不可用，未确认窗口会填满（MQTT 3.1.1 默认 20 条 in-flight），此时表现为背压而非丢数；可调 `max_inflight_messages_set` 或改用 `persisted`。
- WAL 触顶时最多等待 5 秒（避免阻塞 paho 网络线程导致 keepalive 超时），之后不 ACK，由 broker 重投。
- DLQ 只存原始行与原因，不做自动重放，需要人工处理。
- v1 用 Basic auth，v2 用 `Token`，v3 用 `Bearer`；未启用鉴权的服务端可留空 token。
- 管理面按代际分派，但**不翻译查询方言**：`query --sql` 得按当前 `influx.version` 写（InfluxQL / Flux / SQL）。v2 没有「删某个 measurement」的接口，`purge-table` 在它上面直接抛 `UnsupportedBackend` 并说明替代做法（一 bucket 一年 + bucket retention）；v1 的保留期由 retention policy 管，配置里的 `retention_period` 只提示不代建 RP。

## 参考

- [InfluxDB 3 `/api/v3/write_lp`](https://docs.influxdata.com/influxdb3/core/write-data/http-api/v3-write-lp/)
- [InfluxDB 3 写端点选择（v1/v2/v3）](https://docs.influxdata.com/influxdb3/core/write-data/)
- [MQTT Subscriber 官方插件文档](https://docs.influxdata.com/influxdb3/core/plugins/library/official/mqtt-subscriber/)
- [Telegraf mqtt_consumer](https://www.influxdata.com/integration/mqtt-telegraf-consumer/)

## CI/CD 与产物

仓库内置两条 GitHub Actions 流水线（`.github/workflows/`）：

- **CI**（`ci.yml`）：每次 push / PR 到 `main`，在 Python 3.11 / 3.12 / 3.13 矩阵上 `compileall` + 跑全部单测；另有 `backend-smoke` job 起真实的 `influxdb:1.8-alpine` / `influxdb:2.7-alpine` / `influxdb:3-core` 各跑一遍「建库 → 写 → 数行数 → 删表 + 覆盖写幂等」（`tests/test_backends_live.py`，本地要用 `LIVE_INFLUX_URL` 显式开启）；通过后 `python -m build` 产出 wheel + sdist 并作为 artifact 上传。
- **Release**（`release.yml`）：推送 `v*` tag 时自动触发，构建 wheel + sdist 并打包一个可直接部署的 zip（`mqtt-influx-bridge-<版本>.zip`，含包代码、`config.example.toml`、`requirements.txt`、`README`、`LICENSE`），然后发布到 GitHub Release 并附带上述产物；每次运行也会把产物上传为 artifact。

本地产出同样产物：

```bash
python -m pip install build
python -m build                      # 生成 dist/*.whl 与 dist/*.tar.gz
pip install dist/*.whl               # 安装后可用 mqtt-influx-bridge 命令
```

版本号在 `pyproject.toml` 与 `mqtt_influx_bridge/__init__.py.__version__` 两处保持一致；打 tag 用 `git tag v0.1.0 && git push --tags`。
