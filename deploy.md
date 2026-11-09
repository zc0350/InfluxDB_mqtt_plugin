# 部署方案：MQTT → InfluxDB 3 Core（TT1000 冷柜监控）

本文覆盖三件事：**自动建表与字段映射怎么落地**、**这套东西怎么部署**、**上线前后要验什么、有什么坑**。
桥接器代码改造已完成（`mqtt_influx_bridge/routing.py`、`topicmatch.py`、多路由配置），部署产物在 `deploy/`。

## 0. 一页结论

| 问题 | 结论 |
|---|---|
| 怎么「自动建表」 | InfluxDB 3 Core 只有 implicit schema：**写入即建表建列**，没有 DDL，也不需要预建表。表名由桥接器按 payload 时间戳拼出 `mqtt_data_202611` 这种时间分表 |
| 怎么「按年月分表」 | 靠 measurement 名字后缀。Core **没有** partitioned databases（`--template-tag` / `--template-time` 是 Clustered/Cloud 专属），所以表名后缀是自托管下唯一可控的做法；`year_table = true` 开启，`table_period = "year"` 得 `_2026`、`"month"` 得 `_202611` |
| 怎么「映射字段」 | 本配置用 `mode = "explicit"`：只建 `tags` + `fields` 声明的列，其余 key 只进 `data` blob 归档。`mode = "auto"` 是另一种口味——未被占用的 key 全部自动成 field，新字段不用改配置 |
| 三类 payload 落哪 | 上行 → `mqtt_data_{ym}`；下行命令与设备回执 → `device_control_{ym}`（用 `direction` tag 区分）；没命中的 → `mqtt_unknown_{ym}`（只存 blob） |
| 部署形态 | `deploy/docker-compose.yml` 三个容器：`influxdb:3-core` + `eclipse-mosquitto:2` + 桥接器镜像。一条命令 `bash deploy/scripts/bootstrap.sh` 起全栈 |
| 能不能换后端 | 能。同一份代码按 `influx.version` 分派：写路径（line protocol）三代通用，建库/探活/查询/删表走各家自己的 API。能力边界见 §12 —— 例如 v2 根本没有「删某个 measurement」的接口，桥接器直接拒绝而不是假装支持 |
| 必须先知道的三个约束 | ① `create database` / `delete table` / `query` 都是打 HTTP API 的客户端命令，**要 serve 在跑**，且**没有离线模式**（这仨不吃 `--object-store`/`--data-dir`；`--offline` 只属于 `create token`）；② 同名列类型冲突整批 HTTP 400（用 `split_on_reject` 二分隔离）；③ tag 数量决定 series 基数，别把 uuid/door_num 之类放进 tag |
| 待你确认（阻塞项） | `status`/`alarms` 的**真实上行 topic**。目前写成 `${productKey}/+/data`，配置里有 TODO 标记 |

## 1. 数据模型

### 1.1 设备字段落到 InfluxDB 的哪里

查询面就是这张表，其余 key 只在 `data` 里归档：

| 设备字段 | 落到 | 类型 | 来源 |
|---|---|---|---|
| `ts` | `time` 列（纳秒） | 时间戳 | `timestamp_path = "time"`，`timestamp_unit = "auto"` 自动识别秒/毫秒/纳秒 |
| `event` | tag | 字符串 | payload `event`（控制类取 `action`） |
| `devicecode` | tag | 字符串 | payload `deviceCode` |
| `imei` | tag | 字符串 | payload `imei`（alarms 没带就不写这个 tag） |
| `code` | field | float | `fields` 里声明，告警码要筛要聚合，留成数值列 |
| `door_num` | field | 字符串 | 数组按紧凑 JSON 落 `"[1,2]"` |
| `uuid` | field | 字符串 | 控制类 `fields = { uuid = "uuid:str" }` |
| `data` | field | 字符串 | **完整 payload 规范化后的 JSON** |
| `topic` | field | 字符串 | MQTT topic 原文 |
| `sigal` / `door_status` / `server_id` / `version` / `cam` / `sd` | **不成列** | — | 只留在 `data` blob 里，需要时按行取出；信号强度和门状态属于「看 blob 就够」的状态快照 |

三个和直觉不一样的点：

- `ts` 不是普通列。line protocol 里时间戳是每条线的第三个部分，没有列名，查询时叫 `time`。
- 「列」在 InfluxDB 3 里 SQL 都能直接 `WHERE devicecode='TT10002'`，tag 和 field 的区别不在能不能查，而在**tag 参与 series 分组与索引**。所以 tag 只放低基数、要用来筛/聚合的字段：设备号、事件类型、imei。`uuid`、`door_num`、`code` 这些留在 field。
- Core **没有任何 JSON 函数**（`json_extract*`、`->>` 都不可用，实测 400/405），blob 里的 key 等于查不了；也**没有 backfill**，删列只能删表重灌。所以 `mode=explicit` 之前先把「以后要按什么筛」定下来——列就是查询面，改列只能往前改。

### 1.2 实测映射结果（`python -m mqtt_influx_bridge -c config.tt1000.toml sample`，不需要 broker 和数据库）

上行 status（`sigal`/`door_status`/`cam`/`sd`/`server_id`/`version` 只出现在 `data` 里）：

```
mqtt_data_202611,event=status,devicecode=TT10002,imei=864925080140265 data="{\"event\":\"status\",...,\"sigal\":25,\"door_status\":0,...,\"time\":1793936202}",topic="TT1000/TT10002/data" 1793936202000000000
```

上行 alarms（同表；这条没有 `imei`，tag 直接不写）：

```
mqtt_data_202611,event=alarms,devicecode=TT10002 code=1.0,door_num="[1]",data="{...}",topic="TT1000/TT10002/data" 1793936103000000000
```

下行命令（`time=1780000000` 是 2026-05，表后缀跟着走）：

```
device_control_202605,direction=downlink,event=configure,devicecode=bbbbbb uuid="aabbbcc",data="{...}",topic="TT1000/bbbbbb/control" 1780000000000000000
```

设备回执（同表，`direction=uplink`）：

```
device_control_202605,direction=uplink,event=reboot,devicecode=bbbbbb uuid="aabbbcc",code=1.0,data="{...}",topic="TT1000/bbbbbb/response/aabbbcc" 1780000000000000000
```

`mode=explicit` 之后列集合固定为 `time` + 声明过的 tag/field + `data` + `topic`：设备加字段不用改配置也不会撑爆列数，但**新字段要能查就得加进 `fields`**。想要「自动映射」（新 key 自动成列）把 `mode` 改回 `"auto"`，代价是列数不可控 + 类型漂移。

### 1.3 分表规则

- 表名 = `measurement` + `_` + 时间后缀：`table_period = "year"` 得 `mqtt_data_2026`，`"month"` 得 `mqtt_data_202611`、`device_control_202701`。
- 后缀取 **payload 的时间戳**，不是接收时刻。所以 broker 重投、桥接器补历史数据，都会落回原本那张表，不会跨周期跑到新表。
- 时区偏移由 `[mapping] year_tz_offset_minutes = 480` 决定（东八）。按月切时边界同样按北京时间：`2026-11-30 23:30+08` 写 `_202611`，`2026-12-01 00:10+08` 写 `_202612`。
- 想要 UTC 切就设 0；不想分表把 `year_table = false`（此时 `table_period` 被忽略，`validate()` 会告警）。
- 表数量上限要算一下：Core 每库 **2000 张表**。按月分表 = 每类表 12 张/年，本配置 3 类 measurement 约 36 张/年，纯按月能撑 55 年；但如果用 `measurement_path` 从 payload 动态取表名，measurement 数量不受控，按月切会成倍放大表数，那种配法建议保持 `table_period = "year"`。

### 1.4 保留期与清理历史（这条最容易踩）

Core 的清理能力比 Enterprise 弱很多，两条约束要提前定策略：

1. **保留期只能建库时设定，之后改不了**：`influxdb3 create database mqtt [--retention-period 30d]`（库名是**位置参数**，Core 没有 `--database-name`）。时长单位只有 `h d w mo y`，最小 `1h`，`none` = 不限 —— 写成 `3m`、`30s` 会被拒。
2. **删数据只有整表删除**：`influxdb3 delete table --database mqtt mqtt_data_202512`（走 `DELETE /api/v3/configure/table`，服务在跑就能删）。行级/时间范围删除（`--min-time`/`--all-time`、`DELETE FROM`）是 Enterprise/Cloud 的功能，Core 没有。

推荐策略：建库时**不设** retention（`RETENTION_PERIOD=` 留空），靠分表做清理 —— 需要腾空间时删整张旧表（按月分表就能精确到「删掉某个月」），`bash deploy/scripts/purge-table.sh` 默认只打印命令，`--execute` 才真删，全程不停服。按月切之后一次清理要删 12 张（一类 measurement 一个月的量），`purge-table.sh` 一次只处理一张，脚本里按 `for m in 202501 202502 …` 循环；表名不用手抄，`python -m mqtt_influx_bridge tables` 列当前所有表。

两个删除细节要提前知道：默认是**软删除**，表会被改名成 `mqtt_data_202512-<ts>` 并且仍出现在表清单里，磁盘要等后台 compaction 才回收；要立刻物理回收加 `--hard-delete now`（`purge-table.sh --execute --hard`），它和 `serve --delete-grace-period`（默认 24h）配合，删完基本不可恢复，所以先确认再执行。已经删过的表再删一次回 **HTTP 409**（官方行为，不是故障），桥接器把它当成「已删除」而不是报错。

没有「停服离线删表」这条路：`delete table` 和 `create database` 一样只打 HTTP API，不吃 `--object-store`/`--data-dir`，所以清理全程不停服 —— 停机期间桥接器靠本地 WAL 攒着能续传，但那是 WAL 的功劳，不是离线命令的。

## 2. 映射的确切规则

一条 JSON payload 按这个顺序被消化（`mode = "auto"` 走完 1~7，`mode = "explicit"` 跳过第 4 步）：

1. `timestamp_path` 指定的 key → 时间戳，并据此推出分表后缀；该 key 不再重复成列。换算全程用十进制（不过 `float`）：19 位纳秒值过一遍 float 会被量化到 256 ns 栅格，同一毫秒内相差 1 ns 的两点就会并成一点。前提是设备发**整数字面量**；写成科学计数法的 JSON float（`1.794228571529e18`）在 JSON 解析阶段就已经丢精度了，桥接器救不回来。
   - `timestamp_unit = "auto"` 按数值量级判档：`<5e9` 当秒、`<5e12` 当毫秒、`<5e15` 当微秒、否则当纳秒。三个阈值换算到墙上时间都是 **2128-06-11**，所以同一时刻不管发哪种单位都判到同一档 —— 多固件版本混发秒/毫秒**不用拆路由、不用改配置**。
   - `time` 缺失 / `null` / `""` / 纯空白 → 用**接收时刻**（`time.time_ns()`）落库，分表后缀跟着接收时刻走。时间戳在解析时就固化进 line protocol 再进 WAL，所以 WAL 重试不会把时间推到 flush 时刻。代价：这类行没有设备侧幂等键，broker 重投会多出一行；哪条是回落的可以从 blob 里的原始 `time` 看出来。
   - `time: 0` / 负数（RTC 没同步）**也当空** → 接收时刻。不当空的话它会按秒解析成 1970 年，凭空造出 `mqtt_data_197001` 这种按月清理脚本覆盖不到的表，行数只增不减。
   - 真垃圾值（`"abc"`、`true`、`NaN`）仍然 `ParseError` → DLQ，不静默改写成接收时刻。
2. `tags` / `static_tags` 声明的列 → tag；被 `tags` 引用的 payload key 不再自动展开。
3. `fields` 声明的列（**两种 mode 都生效**）→ 按 `name = "path:type"` 强转类型；路径在这条消息里不存在就跳过该列。
4. `mode = "auto"` 时剩下的每个顶层 key 自动成 field：
   - `list` / `dict` → 紧凑 JSON 字符串（`[1,2]`，不带空格）；
   - 数值 → float；布尔 → `true`/`false`；字符串 → 带引号字符串；
   - `null` → 跳过这一列；
   - `type_policy = "string"` → **全部**转字符串（字段类型反复漂移的设备用这个，代价是不能直接做数值聚合）。
5. `blob_field`（默认 `data`）写完整 payload JSON；`topic_field`（默认 `topic`）写 topic。
6. `exclude_keys` 黑名单可以屏蔽个别噪音 key；`key_case = "lower"` 只作用于**自动展开**的 key，配置里手写的列名保持原样。
7. 一条消息里的 JSON 数组（`[{...},{...}]`）会展开成多行。

`mode = "explicit"` 是反过来做「严格表结构」：只写 `fields` 里声明的列，其余 key 全部忽略，永不出现类型冲突，列数也不会被设备端顶到 500 上限，代价是新字段要改配置。`fields` 允许留空，只要还写 `blob_field` 或 `topic_field`（兜底路由 `mqtt_unknown` 就只存 blob）；三样全空 `validate()` 直接报错，因为那种配法任何消息都写不出行、只能全进 DLQ。

**blob 的代价要认**：同一份 JSON 存了两遍（列 + `data`），行体积大约翻倍，gzip 后大约 1.3~1.6x。换来的是「设备加字段/改类型永远不会丢数」。存储敏感就关掉 `blob_field`，或只给 `control`/`response` 这类小 payload 留 blob。反过来 Core 查不了 blob（没有 JSON 函数），所以「先不建列、以后再说」在 v3 上不成立。

## 3. topic 路由与订阅

`config.tt1000.toml`（与 `deploy/config/config.toml.example` 同源）里四条路由：

| 路由 | topic 模式 | 实际订阅 | 表（`table_period=month`） | tag | field |
|---|---|---|---|---|---|
| uplink | `${productKey}/+/data` | `+/+/data` | `mqtt_data_{ym}` | event/devicecode/imei | code/door_num + data/topic |
| control | `${productKey}/+/control` | `+/+/control` | `device_control_{ym}` | direction=downlink/event/devicecode | uuid/code + data/topic |
| response | `${productKey}/+/response/+` | `+/+/response/+` | `device_control_{ym}` | direction=uplink/event/devicecode | uuid/code + data/topic |
| fallback | `#` | `#` | `mqtt_unknown_{ym}` | matched=false/devicecode | data/topic |

- `[mqtt] topics = []` 时订阅列表**由路由自动推导**（上面 `check` 输出的 `mqtt.topics`），不需要两处维护。
- `${productKey}` 是占位符：`PRODUCT_KEY=TT1000` 时订阅收紧成 `TT1000/+/data`；留空展开成 `+`，即一个桥接器服务所有产品。
- 命中规则是**声明顺序取首个**，所以 fallback 放最后。
- 两个 MQTT 语义细节：`#` 匹配任意层级但**不匹配以 `$` 开头的 topic**（`$SYS/...` 不会进兜底表，这是好事）；`+/+/data` 不匹配 `TT1000/AAA/BBB/data`（层数不同），新设备换 topic 结构时会掉进 fallback 而不是静默丢数。
- ⚠️ **`uplink` 的 topic 是我按你给的另外两类的命名风格推的，你的 status/alarms 实际发在哪个 topic 还需要确认**。确认后改一行、`docker compose restart bridge` 即可，不用改代码。
- `fallback` 的 `#` 和上面三条是重叠订阅。MQTT 3.1.1 允许 broker 对每条匹配的过滤各投一份，个别 broker 会真的重复送达；即便重复也不会写出重复行（同一 `(表, tagset, time)` 覆盖写），但白做功。观察到重复投递就把 `#` 换成不重叠的具体前缀。

## 4. 部署形态 A：Docker Compose（推荐）

### 4.1 文件

```
deploy/
├── docker-compose.yml          # influxdb3 + mosquitto + bridge
├── docker-compose.auth.yml     # 可选叠加：启用 token 鉴权
├── .env.example                # 镜像/端口/保留期/测试设备账号
├── bridge.env.example          # 桥接器环境变量（覆盖 config.toml，含凭据与 PRODUCT_KEY）
├── config/config.toml.example  # 容器内 /config/config.toml
├── mosquitto/mosquitto.conf
├── systemd/mqtt-influx-bridge.service
├── Dockerfile.bridge
└── scripts/                      # _common.sh 提供 COMPOSE / say / bexec / iq，其余脚本 source 它
    ├── bootstrap.sh  verify-e2e.sh  status.sh
    └── dlq.sh  backup.sh  purge-table.sh
```

### 4.2 步骤

```bash
cd deploy
cp .env.example .env && cp bridge.env.example bridge.env
cp config/config.toml.example config/config.toml     # 三步 bootstrap 也会自动补齐
$EDITOR .env bridge.env config/config.toml             # 改掉 change-me 占位密码
bash scripts/bootstrap.sh                              # 建账号 → 起 DB → 建库 → 起全栈 → 自检
bash scripts/verify-e2e.sh                             # 发 4 条真实 payload 并查库
bash scripts/status.sh                                 # 日常巡检
```

`bootstrap.sh` 里三个非显然的动作值得知道：

- **建库要先有服务**。`influxdb3 create database` 是打 HTTP API 的客户端命令（`--host` 默认 `http://127.0.0.1:8181`），所以脚本先 `up -d --build influxdb3 mosquitto`，再在 influxdb3 容器里 `exec` 这条命令 —— 容器内的回环就是服务本身，不用管 DNS。60 次重试同时兼作「等 API 就绪」；60 次都没成就直接报错并列出排查顺序，**没有离线兜底**（这命令不吃 `--object-store`/`--data-dir`，早期版本里那段兜底代码是错的，已删）。
- **修正数据卷属主**。官方镜像里 serve 以非 root 的 `influxdb3`（UID/GID **1500:1500**）运行，而 `/var/lib/influxdb3` 在镜像里不一定存在，命名卷会被 root 建出来，于是写不进去；脚本用一个一次性 `--user root` 容器 chown 成 1500（拿不到该用户时退化为 `chmod 0777`）。
- **`docker compose exec` 不会重放服务的 `env_file`**。exec 出来的进程只拿到镜像里的 `ENV` 和 compose 的 `environment:`，看不到 `bridge.env` 里的 `MQTT_PASSWORD` / `WAL_PATH` / `PRODUCT_KEY`。所以 `bridge.env` 额外只读挂载成容器里的 `/bridge.env`，脚本统一走 `_common.sh` 的 `bexec()`：先 source 它再 `exec`，保证与 1 号进程同环境。别改成「在 compose 的 `environment:` 里逐个变量写 `${VAR:-}`」—— 那里的 `${...}` 只从 `.env`/shell 插值，会把 `bridge.env` 里用户设的值静默清成空串。

### 4.3 网络与鉴权边界

| 端口 | 默认绑定 | 暴露给谁 | 风险 |
|---|---|---|---|
| 1883 | `0.0.0.0` | 设备 | 已强制 `allow_anonymous false` + `password_file`；上生产前把设备账号按设备粒度发（`mosquitto_passwd`） |
| 8181 | `127.0.0.1` | 只本机 | 默认栈用 `--without-auth`，**任何能连到 8181 的人都能读写全库**，所以只绑回环；要远程查询就加反代鉴权，或切鉴权版 |

切鉴权（官方文档给的是一条在线命令 `influxdb3 create token --admin`，token 只在创建时打印一次，所以要自己存；存好之后必须以启动参数形式回传给 serve，否则每次重启都换一个新 token、旧的集体失效）：

```bash
# 1) 在线生成，并把 stdout 落成数据卷里的文件（0600）
docker compose exec -T influxdb3 sh -c \
  'umask 077; influxdb3 create token --admin > /var/lib/influxdb3/admin.token'
# 2) cat 出来，把 token 明文抄进 .env 的 INFLUX_ADMIN_TOKEN 与 bridge.env 的 INFLUX_TOKEN
docker compose -f docker-compose.yml -f docker-compose.auth.yml up -d
```

`--output-file` **不是**在线命令的参数，它属于 `create token --offline`（不起服务本地造 token）那条路；早期文档里 `--admin --output-file` 一起写是错的，已按官方 CLI 参考改正。

`docker-compose.auth.yml` 做两件事：把 `serve` 的参数换成 `--admin-token-file=${INFLUX_ADMIN_TOKEN_FILE:-/var/lib/influxdb3/admin.token}`，并给容器注入 `INFLUXDB3_AUTH_TOKEN=${INFLUX_ADMIN_TOKEN}` —— 容器里的 `influxdb3` CLI（`create database` / `delete table` / `query`）默认读这个变量，于是脚本不用给每条命令都拼 `--token`。

- Core 只有 operator / admin 两种 token，**没有** `--permissions` 细粒度授权（`<库名>::READ_WRITE` 那套是 Clustered/Cloud 的），所以桥接器拿的就是全库权限 —— 这也是 8181 只绑回环的另一个理由。
- 3.x 各小版本的参数名有过出入：`serve` 这一侧见过 `--admin-token`（收字符串）与 `--admin-token-file`（收 JSON）两种，`create database` 的库名在文档里一直是**位置参数**（`--database-name` 并不存在）。脚本按文档写法来，报错就进容器 `influxdb3 <子命令> --help` 核对，别照搬网上的老帖子。

### 4.4 备份

```bash
bash deploy/scripts/backup.sh      # 停写 → tar 三个数据卷 + 配置凭据 → 拉起
```

脚本先按 `bridge → mosquitto → influxdb3` 停写，再用一次性容器把卷内容 tar 到 `backup/<时间戳>/`（`-T` 是必需的：compose run 默认给 TTY，会破坏 tar 的二进制流），最后 `up -d`。

这是**整栈停机**（几十秒量级），期间 broker 也不接单，只适合低峰或单机维护。日常只想备 DB 时不必停全栈：`docker compose stop influxdb3` → 一次性容器 tar 出 `influxdb3-data` → `docker compose up -d influxdb3`。桥接器和 broker 照常工作，这段时间的数据在本地 WAL 里攒着，库起来后续传。（DB 侧必须停：catalog 与 object store 只在服务停止时是一致的。§1.4 的 purge 不需要停服 —— 它走的是 HTTP API，停服只对「直接 tar 文件」这件事必要。）

三卷各自独立备份也够用：`influxdb3-data`（catalog + object store，恢复时以它为准）、`bridge-data`（`bridge.db`，纯缓冲，丢了等价于放弃未写入的行）、`mosquitto-data`（持久会话的未投递消息）。备份目录里有明文口令（`bridge.env`、`mosquitto/passwd`），脚本已 `chmod 700`，落盘到异地前请加密。

## 5. 部署形态 B：裸机 systemd（不用 Docker）

适合单机、想少一层虚拟化的场景。三个服务：influxdb3、mosquitto、bridge。

```bash
# 1) InfluxDB 3 Core：从官方仓库装包或下载二进制（Linux x86_64/arm64）
sudo useradd -r -m -d /var/lib/influxdb3 influxdb3
sudo -u influxdb3 influxdb3 serve --node-id=node0 --object-store=file --data-dir=/var/lib/influxdb3 --without-auth
# 另开一个终端：create database 打的是 127.0.0.1:8181 的 HTTP API，所以要等服务起来之后
sudo -u influxdb3 influxdb3 create database mqtt          # 要保留期就加 --retention-period 30d
# 之后把 serve 交给 systemd（Restart=always）；再也不需要用离线方式建库

# 2) mosquitto：apt install mosquitto mosquitto-clients，用 deploy/mosquitto/mosquitto.conf
#    注意把 password_file 路径改成 /etc/mosquitto/passwd，并 mosquitto_passwd -c 建账号

# 3) 桥接器：包还没上 PyPI，从源码目录或 dist/*.whl 装
sudo useradd -r -m -d /var/lib/mqtt-influx-bridge mqttbridge
sudo python3 -m venv /opt/mqtt-influx-bridge/venv
cd /path/to/InfluxDB_tell && sudo /opt/mqtt-influx-bridge/venv/bin/pip install .
#   离线机器：先 python -m build，再 pip install dist/mqtt_influx_bridge-0.2.0-py3-none-any.whl
sudo mkdir -p /etc/mqtt-influx-bridge /var/lib/mqtt-influx-bridge
sudo cp deploy/config/config.toml.example /etc/mqtt-influx-bridge/config.toml
sudo cp deploy/bridge.env.example /etc/mqtt-influx-bridge/bridge.env
# 裸机上把 bridge.env 里的容器服务名改回本机地址：MQTT_HOST=127.0.0.1、INFLUX_URL=http://127.0.0.1:8181、WAL_PATH=/var/lib/mqtt-influx-bridge/bridge.db
sudo chmod 600 /etc/mqtt-influx-bridge/bridge.env
sudo chown -R mqttbridge:mqttbridge /var/lib/mqtt-influx-bridge /etc/mqtt-influx-bridge
sudo cp deploy/systemd/mqtt-influx-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now mqtt-influx-bridge
```

`deploy/systemd/mqtt-influx-bridge.service` 里已配好：`EnvironmentFile=/etc/mqtt-influx-bridge/bridge.env` 注入凭据（权限 0600）、`TimeoutStopSec=120` + `SIGTERM` 保证 WAL 刷完、`ReadWritePaths` 只放行 `/var/lib/mqtt-influx-bridge`、`MemoryMax=512M`。上面的 `serve` 是前台进程，`Ctrl+C` 停掉后照抄一份 influxdb3 的 unit（同样的 `--data-dir`，`Restart=always`）；官方未公布 Core 的最低 CPU/内存（文档只给 OS 要求），别把桥接器的 `MemoryMax=512M` 照搬给它。裸机上 `delete table` 与查询同理：服务在跑就能做。

## 6. 为什么没用官方 MQTT Subscriber 插件

InfluxDB 3 从 3.8.2 起有官方 MQTT 插件，看着更省事，但四点不合这个需求：

| | 官方插件 | 本桥接器 |
|---|---|---|
| 运行前提 | 要启用 Processing Engine（`--plugin-dir`、`--trigger-spec`），插件跑在 DB 进程内 | 独立进程，DB 挂了照样缓冲 |
| 字段 | `payload_type = "json"` 必须**显式列出**要建哪些列，不是 schema-free | `mode=explicit` 同样显式，但完整 payload 一定进 blob；换 `mode=auto` 就能全自动展开 |
| 按年月分表 | 只支持固定 `table_name` 或从字段取表名，没有内置时间后缀 | `year_table = true` + `table_period = "year"/"month"` 自动后缀 |
| ACK 语义 | paho 默认自动 PUBACK，**确认早于落库**；队列 64MiB 满了直接丢 | 落库/落 WAL 后才 ACK，本地 SQLite 无队列上限告警 |

结论：插件适合「已经在用 influxdb3，且能接受显式列声明 + 不介意 at-most-once」；这个场景要求不丢数 + 按年月分表，所以自研桥接器 + Core 的 schema-on-write。

## 7. 上线检查清单

```
[ ] 1. 改 config.tt1000.toml：确认 uplink 真实 topic、PRODUCT_KEY、设备账号；确认 fields 覆盖了要筛/要聚合的列（Core 不能 backfill）
[ ] 2. 干跑映射（不需要 broker/DB）：
       python -m mqtt_influx_bridge -c config.tt1000.toml sample \
         --topic TT1000/TT10002/data --payload '{"event":"status","deviceCode":"TT10002","time":1793936202}'
       → 表名 mqtt_data_<年><月>（如 mqtt_data_202611）、tag 与 field 是否符合预期（对照 §1.2）
[ ] 3. python -m mqtt_influx_bridge -c config.tt1000.toml check
       → 看 mqtt.topics 是不是想要的订阅；WARNING 逐条判断（分表条数、token 未配是已知的）
[ ] 4. bash deploy/scripts/bootstrap.sh
[ ] 5. bash deploy/scripts/verify-e2e.sh        → 期望「全部通过」
[ ] 6. 故障演练（现场或预生产各做一次）：
       docker compose stop influxdb3 → 持续发消息 10 分钟 → bridge status 看 pending 增长 →
         start → pending 归零、DB 行数等于发布数
       docker compose kill -s KILL bridge → 重启 → 行数不丢（broker 重投 + WAL 续传双保险）
       同一 payload 手动重发 3 遍 → 行数不增（(表, tagset, time) 覆盖写）
       发一条非法 JSON（如 `{"a":`）→ 进 DLQ 且后续消息照常入库
       改错一条路由的 timestamp_path → 确认能靠 dlq.sh 找回并 requeue
[ ] 7. 观察 24 小时后：pending 无持续增长、DLQ 数量可解释、mosquitto 无 queued 丢弃日志
[ ] 8. 定清理策略：retention 或按分表 purge（§1.4，按月就能删单月），写进值班手册；备份脚本进定时任务（§4.4）
```

## 8. 容量估算（数量级，必须压测确认）

按 1 万台设备、30 秒上报一次估算：

| 项 | 值 |
|---|---|
| 消息速率 | ~333 msg/s，2.9e7 行/天 |
| 单条 payload | ~200 B；line protocol 含 blob ~400 B |
| 未压缩写入量 | ~12 GB/天；gzip（JSON 重复度高）后网络流量约 1/4 |
| 批写次数 | `batch_size=1000` + `flush_interval_ms=200` → 峰值约 1 req/s |
| 桥接器内存 | WAL 是磁盘队列，内存主要花在批次缓冲；`MemoryMax=512M` 够用 |
| InfluxDB 3 Core | 官方未公布最低内存；compose 里的 `mem_limit: 1g` 只是防 OOM 的护栏，实际峰值要压测确认。333 行/s 是 Core 的舒适区，瓶颈通常在查询而不是写入 |
| mosquitto 内存 | 持久会话积压是主要变量：1000 条 × 200B ≈ 200KB，可忽略 |

series 基数：tag 组合数 ≈ 设备数 × 每台设备的 tag 组合数。`mode=explicit` 后 uplink 只剩 `event/devicecode/imei` 三个 tag —— `imei` 只有 status 带、alarms 不带，所以每台设备 2~3 个 series，且**不再随固件版本累积**（`version`/`serverid` 已从 tag 摘掉，这是这次改列最大的收益）。1 万设备 ≈ 2~3 万 series，100 万设备 ≈ 200~300 万。官方对 Core 的 tag 基数没给上限值，但这个数字要进监控：

```sql
SELECT count(*) FROM (SELECT DISTINCT devicecode, imei FROM mqtt_data_202611);
```

## 9. 风险与待确认

| # | 风险 | 影响 | 处置 | 状态 |
|---|---|---|---|---|
| 1 | uplink 真实 topic 未知 | 上行走 fallback，落 `mqtt_unknown_*` | 确认设备端发布 topic，改 `[[route]] uplink.topics` | **待你确认** |
| 2 | 同名字段类型漂移（`door_num` 先是数组后是数字） | 整批 HTTP 400 | `writer.split_on_reject=true` 二分隔离：坏行进 DLQ、好行照常入库；长期可上 `type_policy="string"` | 已实现 |
| 3 | `data` blob 让体积翻倍 | 存储成本 | 可关；或对大 payload 用 `exclude_keys` | 取舍待定 |
| 4 | payload 缺 `time` | 回落接收时刻 → 重投不再幂等、跨周期可能跑表 | 发布端强制带 `time`；`sample` 干跑验证 | 需设备侧配合 |
| 5 | 设备时钟错（跑到未来/过去一年） | 数据落错分表，查询看不见；按月切更容易撞上月初月尾 | 监控 `max(time)` 与 `now` 偏差；Core 侧只能删表清理，代价高 | 建议加告警 |
| 6 | `mode=explicit` 把列定死了 | 设备新增的 key 只进 blob，Core 没 JSON 函数 = 查不了，且不能 backfill | 上线前按 §1.1 核对查询面；要加维度就改 `fields` 从今往后生效，历史数据只能导出重灌 | 取舍已确认 |
| 7 | 默认 `--without-auth` | 能连 8181 即可读写全库 | 8181 只绑 127.0.0.1；远程走反代鉴权或 §4.3 切鉴权版 | 已缓解 |
| 8 | mosquitto `queued_message_limit` 默认 1000 | 桥接器停机久了 broker 丢最老消息 | `deploy/mosquitto/mosquitto.conf` 已调 1000000 并注释说明 | 已缓解 |
| 9 | `accept_partial=false` + 整批重试 | 部分成功后重试会写重复点 | 保持 `false`（本项目 writer 的默认；HTTP 参数侧默认反而是 `true`）；靠 (表,tagset,time) 覆盖幂等 | 已实现 |
| 10 | 软删除不立即回收磁盘 | 删完表空间没降，表清单里还看得到 `<表>-<ts>` | `purge-table.sh --execute --hard` 走 `--hard-delete now`；注意 `serve --delete-grace-period` 默认 24h，硬删后基本不可恢复 | 已知约束 |
| 11 | 3.x CLI 参数名版本漂移 | 脚本按老写法调用会失败 | 脚本只用它自己的 `--help` 里确认存在的形式（库名位置参数、SQL 位置参数），失败时提示核对；建库只走在线 HTTP API，没有「离线兜底」这种第二路（早期脚本里那段是错的，已删） | 已缓解 |
| 12 | 单实例吞吐上限 | 峰值超单进程能力 | 多实例 + MQTT 5 `$share` 共享订阅，各自独立 `client_id` 与 WAL 文件 | 未实现，按需 |

## 10. 常用查询

```sql
-- 有哪些表（Core 3.12 实测：带下划线的 _information_schema 不能 SELECT，会 400；
-- SHOW TABLES 会把 system/information_schema 自己的视图一起列出，所以按 table_schema='iox' 滤）
SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY 1;

-- 某张表有哪些列 + 类型（按月分表后核对新列集合就靠它）
DESCRIBE mqtt_data_202611;

-- 最近 10 条某设备上报（列 = §1.1 那张表；sigal/cam/sd 这种只进 blob 的查不了）
SELECT time, event, code, door_num
FROM mqtt_data_202611 WHERE devicecode='TT10002' ORDER BY time DESC LIMIT 10;

-- 开门告警按天统计
SELECT date_trunc('day', time) AS d, count(*) FROM mqtt_data_202611
WHERE event='alarms' GROUP BY 1 ORDER BY 1 DESC;

-- 跨月：Core 没有通配表名，只能 UNION ALL 显式列出
SELECT * FROM (
  SELECT time, devicecode, event FROM mqtt_data_202611
  UNION ALL
  SELECT time, devicecode, event FROM mqtt_data_202612
) WHERE devicecode='TT10002' ORDER BY time DESC LIMIT 10;

-- 命令与回执配对（同表，direction 区分）
SELECT time, event, direction, devicecode, uuid FROM device_control_202605
ORDER BY time DESC LIMIT 50;

-- 哪些 topic 掉进了兜底表（发现新设备/新 topic 结构）
SELECT topic, count(*) FROM mqtt_unknown_202611 GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
```

两个 Core 的 SQL 手感：`time` 直接选出来是 RFC3339 字符串，要纳秒数值做减法得写 `arrow_cast(time,'Int64')`；`count(*) FILTER (WHERE …)` 可用，`json_extract*` / `->>` 全部不可用（实测 400/405），所以 blob 里的 key 连模糊匹配都只能靠 `substr`/`regexp_match`。

命令行：`docker compose exec influxdb3 influxdb3 query --database mqtt "SELECT ..."` —— SQL 是**位置参数**，没有 `--sql`；`--format pretty|json|jsonl|csv|parquet` 换输出，`-o` 落文件。Core 的 query **没有 `--precision`**，要自定义时间格式就在 SQL 里 `CAST(time AS VARCHAR)` 或用 `to_char`。巡检/验证脚本走 `_common.sh` 里的 `iq()`，同样按位置参数拼（启用了鉴权时用 `.env` 的 `INFLUX_ADMIN_TOKEN` 补 `--token`）。

## 11. 还需要你给的信息

1. `status`/`alarms` 的实际发布 topic（阻塞项 #1）。
2. 设备规模与上报频率（校准 §8 的容量估算）。
3. 是一个产品（`PRODUCT_KEY=TT1000`）还是多产品共用一个桥接器。
4. 数据要留几年、清理用 retention 还是年度 purge（§1.4：purge 默认不停服，只是软删除要等 compaction 才回收磁盘）。
5. 是否需要从外网查询 InfluxDB（决定是否切鉴权版 + 反代）。
6. 目标机器：单台 Linux 物理机 / 已有 Docker / 已有 InfluxDB 或 mosquitto 实例（决定是否复用现网组件）。

## 12. 附录：同时支持 v1 / v2 / v3 的边界

一份代码、一个开关：`[influx] version = "v1" | "v2" | "v3"`（环境变量 `INFLUX_VERSION`）。设计取向是**写路径通用、管理面分派** —— line protocol 三家完全一致，所以 `lineprotocol.py` / `routing.py` / `wal.py` / `writer.py` 里没有一处 `if version`；差异全收敛在 `admin.py`（建库、探活、查询、删表）和 `writer.py` 的 `endpoint()` + `auth_header()` 两处。

|  | v1（1.8 OSS） | v2（2.7 OSS） | v3（3.x Core） |
|---|---|---|---|
| 写 | `POST /write?db=&precision=ns` | `POST /api/v2/write?org=&bucket=&precision=ns` | `POST /api/v3/write_lp?db=&precision=nanosecond` |
| 鉴权头 | `Basic`（`username`/`password`） | `Token <api-token>` | `Bearer <token>` |
| 建库 | `POST /query` + `CREATE DATABASE`（先 `SHOW DATABASES` 再建） | `GET/POST /api/v2/buckets` | `POST /api/v3/configure/database` |
| 保留期 | **库级不限**，靠 retention policy；配置里的 `retention_period` 在 v1 只提示不建 RP | 建 bucket 时 `retentionRules:[{type:expire,everySeconds:…}]`，**之后可改** | 建库时 `retention_period`，**之后改不了** |
| 探活 | `GET /ping` | `GET /health` | `GET /health` |
| 查询 | InfluxQL，错误藏在 200 响应体的 `results[].error` | Flux + `Accept: text/csv` | SQL（DataFusion）+ `/api/v3/query_sql` |
| 部分失败 | `partial_write` 可开关 | 整批拒绝 | `accept_partial` 默认 true |
| 删一张表 | `DROP MEASUREMENT`（立即生效） | **做不到** → `UnsupportedBackend` | `DELETE /api/v3/configure/table`（软删；409=已删过） |
| 按时间删数据 | `DELETE FROM … WHERE time<…` | `influx delete` / API（按 predicate） | 不支持，只能删表或等 retention |

三条要提前认的取舍：

- **按时间分表在三代上的清理手感不同**：v1 删 `DROP MEASUREMENT` 干净利落；v3 删表是软删、磁盘等 compaction；v2 干脆不能按 measurement 删 —— 所以在 v2 上正确做法是**一个分表周期一个 bucket**（按月切就一月一 bucket）+ bucket retention，而不是把时间写进 measurement 名。配置里 `year_table = true` 时 `validate()` 会按代际给出对应告警。
- **`retention_period` 只在「库还不存在」那一次生效**。v3 建库后改不了（官方文档明说），v1 忽略，v2 可改。所以 `init-db` 的输出会明确告诉你是 `created` 还是 `exists`。
- **不做假兼容**。v2 的删表、v1 的 retention 这类「那代后端确实没有」的能力，代码里是抛 `UnsupportedBackend` / 返回 `ignored` 并解释原因，而不是翻译成一条看着像能用的语句。

切换成本：改 `version` + `url`（v1/v2 默认 8086）+ 凭据形式（v1 用 `username`/`password`，v2/v3 用 `token`，v2 还必须填 `org`）。`python -m mqtt_influx_bridge -c … check` 会把当前代际的边界打印出来；`ping` / `init-db` / `query --sql` / `tables` / `purge-table` 这几个子命令都按 `version` 分派，可以直接拿去验后端。

验证覆盖：`tests/test_admin.py` 用本地 mock server 断言三家的请求形状（路径、头、body、方言、错误语义），CI 的 `backend-smoke` job 再拿真实服务（`influxdb:1.8-alpine` / `influxdb:2.7-alpine` / `influxdb:3-core`）各跑一遍「建库 → 写 → 数行数 → 删表 + 覆盖写幂等」（`tests/test_backends_live.py`，本地要用 `LIVE_INFLUX_URL` 显式开启）。查询语言仍不互转：`query --sql` 得按当前代际的方言写。
