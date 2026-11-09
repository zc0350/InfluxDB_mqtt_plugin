# InfluxDB v1 / v2 / v3 综合对比（性能 · 稳定性 · 正式部署）

面向本仓库的使用形态：**单机 Linux + docker-compose/systemd，只用开源版（无 Enterprise 授权），MQTT 桥接器写入约 333 msg/s**。
资料截至 **2026-11-09**，官方文档/GitHub 为主，第三方基准会标注来源与偏见。带 ❓ 的条目是本次未能一手验证的。

---

## 0. 一页结论

| 场景 | 推荐 | 理由 |
|---|---|---|
| 新部署、单机、要 SQL/BI 接入、要做大范围聚合 | **InfluxDB 3 Core**（当前 3.12.2，pin 镜像标签） | 列存 Parquet + DataFusion 向量化，聚合/分位数比 TSM 引擎快一个量级；写入即建表，无 DDL 心智负担 |
| 现网已有 1.8 且查询以"某设备最近 N 条"为主 | **留在 1.x（升到 ≥1.12.4，建议 1.13.2）** | 1.x OSS 仍在发版；低基数点查 1.x 不吃亏；迁移 3 要重灌历史数据，收益不明确 |
| 现网已有 2.x 在用（UI / tasks / Flux 看板） | **保留，不扩大** | Flux 官方进入"维护模式"且 3.x 不支持，投资 Flux 是往回走 |
| 需要高可用 / 副本 / 行级按时间删除 / 细粒度权限 | **OSS 三代都做不到** | 1.x、2.x OSS 无集群；3 Core 无 HA、无 `DELETE FROM`。要么应用侧兜（本项目：SQLite WAL + QoS1 重投），要么 Enterprise/Cloud |
| 边缘、低内存、只存最近几天热数据 | **3 Core**（`--object-store=file`）或 1.8 | 3 Core 单二进制无依赖；1.8 生态熟、坑已被踩平 |

**一句话**：v3 是架构代际差（列存/向量化/SQL），赢在查询和高基数，输在运维成熟度与功能完整度（HA、删除、备份都要自己补）；v1 是被低估的老兵，写入与点查够用、十年踩坑文档齐全；v2 是过渡代，唯一不可替代的是内置 UI 和 tasks，而这恰好是 3.x 用 Processing Engine 替换掉的部分。

---

## 1. 生命周期与支持状态（最容易搞错的一节）

| | v1 OSS | v2 OSS | v3 Core OSS |
|---|---|---|---|
| 最新可见版本 | **v1.13.2**（release notes 标注 2026-09-02；GitHub tag 到 2026-10） | **v2.9.1**（2026-05-12）；仓库有 `v2.10.0` tag，但官方 release notes 尚无 2.10 条目 ❓ | **v3.12.2**（2026-10-29），并行维护 3.11.8 |
| 是否"已停止维护" | **否**。1.13 还在加功能：HTTP/OpenTSDB/subscriber 的 mTLS、自适应 TSI cache、`hardening-enabled`(SSRF 缓解)、备份 gzip 级别、TSI/TSM compaction 死锁修复 | **否**。2.8 起可选 token 哈希，2.9 默认开启、并从 1.12.x 回合 compaction 修复 | GA 于 **2025-04-15**，仍在快速迭代 |
| 官方 EOL 日期 | 未公布 | 未公布 | 未公布 |
| 查询语言前景 | InfluxQL 稳定 | Flux = **维护模式**（官方原文："not actively developing any new Flux features"；不 EOL；**3.x 不支持 Flux**） | SQL(DataFusion) 为主，InfluxQL **部分**兼容 |
| 官方文档横幅 | "InfluxDB 3 Core is the latest stable version" | 同左 | — |
| 周边生态 | Kapacitor 仍在维护（v1.8.9，2026-11-04） | 同 | Telegraf/客户端按 `db`/`bucket` 参数区分代际 |
| 中国用户额外一条 | 阿里云 TSDB for InfluxDB（1.x 兼容托管）**2026-10-23 已 EOL** | — | — |

> **纠正三个流传很广的说法**
> 1. "1.x 早就不维护了" —— 错，1.13.x 2026 年还在发功能版本。
> 2. "2.x OSS 没有备份命令" —— 错，`influx backup` / `influx restore` + `/api/v2/backup/*` 存在。
> 3. "Flux 已废弃" —— 官方措辞是 maintenance mode，且明确说不会 EOL；真正的问题是**它进不了 3.x**。

---

## 2. 架构差异（决定后面所有性能与运维结论）

| 维度 | v1 | v2 | v3 |
|---|---|---|---|
| 实现语言 | Go | Go | **Rust**（IOx） |
| 存储引擎 | TSM（LSM 变体，行组）+ TSI 倒排索引；**默认 `inmem` 索引**，`tsi1` 需显式开 | 同族演进，**索引恒为 TSI** | **Apache Parquet 列存**，内存用 Arrow |
| 落盘位置 | 本地磁盘（分片目录） | 本地磁盘 `engine/` + `influxdb.bolt` 元数据 | `--object-store=memory\|file\|s3\|google\|azure`，`--data-dir` 必填（file） |
| 逻辑模型 | database → retention policy → measurement + tags/fields | org → bucket → measurement + tags/fields | database → **table** + tags/fields（implicit schema，写入即建） |
| 分区 | shard group（按时长切） | 同（bucket retention 推导默认时长） | **无 partitioned database**（`--template-*` 是 Clustered/Cloud 专属）→ 本项目按年或按月拼表名 |
| 连续查询/降采样 | CQ 原生 | **无 CQ**，换成 Flux tasks | **无 CQ**，换成 Processing Engine（内嵌 Python VM + plugin/trigger） |
| 集群/HA | OSS 无，Enterprise 有 | OSS 无 | Core 无；Clustered/Enterprise 有 |
| 元数据鉴权 | `influxdb.conf` TOML + `INFLUXDB_*` 环境变量 | CLI flag / `INFLUXD_*` / config，token 存 bolt | CLI flag / `INFLUXDB3_*` / TOML |
| 许可 | MIT | MIT | MIT / Apache-2.0 双许可 |

关键推论：
- **TSM → Parquet 不是同一份文件**，所以任何一代到 3.x 都**没有原地升级**，只能重灌（见 §6）。
- 行式 LSM 变体每次读要还原整行再挑字段；列存 + 谓词下推让"扫大范围做聚合"变成 v3 的主场。
- v1 的 `inmem` 索引把整个 series 索引放内存、每次启动重建 —— 这是 1.x 大库重启慢的根因，也是生产上必须换 `tsi1` 的理由。

---

## 3. 性能对比

### 3.1 按维度

| 维度 | v1 (1.8/1.13) | v2 (2.7/2.9) | v3 Core (3.12) |
|---|---|---|---|
| 写入吞吐 | 高。官方 sizing 给的单机上限参考是 **75 万 field writes/s**，超过就建议 Enterprise | 与 v1 同量级 | 官方口径 "millions of measurements/s"（营销，无严谨对比数据）。第三方 TSBS 复现：**≤10 万设备时比 1.8 慢**，≥100 万设备才反超 |
| 末值/点查（"这台设备最近 10 条"） | 靠 series 索引 + TSM block 读，够快 | 同 v1 | **Last-Value Cache 官方 <10ms**、distinct metadata ~30ms；不开 cache 时冷 Parquet 扫描会慢 |
| 大范围聚合 / 分位数 / 多维 group by | 慢（要解整行）；大报表查询易打爆内存 | 同 v1；Flux 更灵活但吃内存 | **明显快**（列裁剪 + 向量化）。社区实测一例：1.8 的 40s 日报聚合成 3 Core 3s（自媒体，量级参考，非严格基准） |
| 高基数（tag 组合爆炸） | **最差**：官方点名 series 基数是内存头号杀手；官方 sizing 单机建议 **<1000 万 series** | 同 v1，且有 `vm.max_mmap_count` 耗尽导致数据查不了的已知坑（2.7.11 修复） | 架构级红利（Parquet 字典编码 + 元数据统计），官方称"无基数限制"；但 Core 的 catalog/元数据仍会随基数膨胀，实践上依旧要把 uuid 之类挪出 tag |
| 存储压缩 | Gorilla + Snappy | 同 | Parquet 列压缩，官方宣称空间降 ~4.5x、对象存储成本降 ~90%（营销，含企业版特性） |
| 冷启动/重启 | `inmem` 重建索引很慢；`tsi1` 加载 index log | TSI 加载 + bolt | catalog 加载（3.11.6~3.11.8 连续修 catalog 加载/重复名/Parquet 索引加载慢） |
| 并发查询保护 | `[coordinator] max-concurrent-queries`、`query-timeout`（默认 0=不限！） | `query-memory-bytes`（**历史上不生效**，见 §4） | `--max-concurrent-queries`（3.10 起可运行时调）、`--exec-mem-pool-*`、`--parquet-mem-cache-size`，超限返回 429（3.12.2 才修对 HTTP 码） |

### 3.2 对本项目的实际含义

- 333 msg/s（约 2.9e7 行/天、未压缩 12 GB/天）在**三代都是写入舒适区**，写入不是选型依据。
- 选型依据是**查询形态**：
  - "某设备最近几条 + 按天计数" —— 三代都行，v3 要记得建 `last_cache`，否则冷数据点查不如 1.8 手感。
  - "跨设备/跨维度统计、告警回溯、导出分析" —— v3 优势明显，且能直接 `--format parquet` 出文件接 Python/BI。
- 存储成本：v3 的 Parquet 压缩对本场景这种高重复 JSON blob 收益明显（本项目 `data` 字段存完整 payload，v1/v2 的 Snappy 与 v3 的列压缩在这类数据上差距会体现在磁盘占用）。
- **别信单一来源的倍数**：官方没给 3 Core vs 1.x 的严谨倍数；"3 比 1.8 慢 48x""QuestDB 比 3 Core 快 36x"这类数字出自 **TDengine/QuestDB 竞品博客**，方法论不可复核，只能当作"低基数写入与点查 v3 不占优"的定性佐证。

---

## 4. 稳定性对比

### 4.1 有证据的真实故障（不是道听途说）

| 代 | 版本/证据 | 表现 | 状态 |
|---|---|---|---|
| v1 | **v1.12.3 引入 TSI 索引 `concurrent map iteration and map write` panic**（由 2.x 分支的不必要 backport 造成） | 进程直接崩 | 官方在 1.12.4 修复并明确"**强烈建议升到 ≥1.12.4**" |
| v1 | v1.13.1 修复 compaction 与热查询之间的 **deadlock**（TSM file store）+ TSI compaction 规划锁竞争 | 写入停顿、磁盘不回收 | 已修 |
| v1 | 长期未解的 OOM/慢 compaction 类 issue：#9938（TSM compaction 触发内存崩溃）、#14426（neverending compaction）、#23982（compaction OOM）、#11830（TSI 比 inmem 更吃内存） | 高负载单机 OOM | 部分长期 open |
| v1 | #14013 `DROP MEASUREMENT` 不会删掉所有 retention policy；#10052 删掉的 measurement 旧字段/标签会"复活" | 清理不干净 | 历史遗留，靠 `influx_inspect deletetsm` 离线兜 |
| v2 | **`query-memory-bytes` 不生效**（issue #27070：一条查询分配 ~10GB，容器被 OOM kill） | 一个烂查询打挂整库 | 已修（2.7.12 线） |
| v2 | 2.9.1 修复 "level 3 compactions 停止"、"资源受限机器上 compaction 队列卡死" | 查询变慢、磁盘暴涨 | 已修 |
| v2 | 2.7.11 修复 `vm.max_mmap_count` 耗尽导致数据查不到；2.7.12 修 `TagValueIterator` 锁导致读写阻塞；2.7.8 修写路径死循环；2.7.7 修 retention service 失去响应 + task scheduler 夏令时 bug | 各种"看着像数据丢了" | 已修 |
| v2 | 2.9 起 token 哈希默认开启后，**降级到 <2.8.0 会清空所有 API token** | 升级易、回退炸 | 官方已知约束 |
| v3 | **3.11.7 修 "WAL flush serialization，防止已确认写入丢失"；3.12.1 继续修 "acknowledged write preservation"** | 服务端返过 204/2xx 的点，崩溃后不在库里 —— 直接冲击"ACK 对齐落库"的语义 | 已修（需 ≥3.11.7/3.12.1） |
| v3 | 3.12.2 修 **catalog 死锁/修复**；3.11.6 新增 `catalog repair` 命令；3.11.6 修 distinct cache panic | 元数据面故障，重启难恢复 | 已修/有工具 |
| v3 | 3.12.2 才把**内存超限的 HTTP 状态码修对（429）** | 之前超限返回错误的码，客户端会误判成成功/永久失败 | 已修 |
| v3 | 3.10 的 **catalog 格式 v2→v3 是单向自动升级，不支持降级**，官方要求升级前备份 catalog | 升级即不可回头 | 官方已知约束 |

### 4.2 语义层面的可靠性差异

| 能力 | v1 | v2 | v3 Core |
|---|---|---|---|
| 崩溃后已确认数据 | WAL（`wal-fsync-delay` 可调，机械盘建议 0–100ms） | WAL；**2.7.11 才加 `--storage-wal-flush-on-shutdown`**（此前关机不全 fsync） | WAL + 异步 Parquet 快照；`no_sync=true` 换吞吐（**本项目默认不启用**）；3.11.7 前"已确认写入丢失"有实锤 |
| 整批 vs 部分写入 | `partial_write` 参数控制 | **422 = 部分或全部被拒（其余已入库）**，400/413 = 整批拒 | `accept_partial` 默认 **true**；设 false 才整批原子 |
| 副本/故障转移 | ❌ | ❌ | ❌（Enterprise 才有） |
| 备份恢复 | `influxd backup/restore`（要开 8088 RPC，portable 格式）；历史上有"不能完整恢复用户+库"的痛点 | `influx backup/restore` + HTTP 备份端点（恢复要 `--full --operator-token`） | **Core 无内置备份命令**，靠停服冷拷贝 `--data-dir`（Enterprise 3.10 才加自动备份，beta） |
| 按时间删数据 | `DELETE FROM ... WHERE time<` 可用（有 RP 相关 bug） | `influx delete`（predicate）可用；**但没有按 measurement 删的接口** | ❌ `DML not supported: Delete`，只能删表或等 retention |
| retention 可改性 | RP 可增可改 | bucket retention 可改 | ❌ **建库时定死，之后改不了** |
| 观测面 | `_internal` 库、`SHOW STATS/DIAGNOSTICS`、`/debug/vars` | system bucket + 指标 | `/health`、`/ready`（3.10+）、`/metrics`（需 admin token） |
| 成熟度判断 | 十年生产沉淀，坑都有文档 | 六年，坑基本修平，但方向是"不再投新功能" | GA ~18 个月，**近一年还在修 WAL/catalog/compaction 这类地基问题**；生产可用，但要把它当"新软件"运维：pin 版本、灰度、备份 catalog |

---

## 5. 正式部署要点

### 5.1 交付形态

| | v1 | v2 | v3 Core |
|---|---|---|---|
| 包 | deb / rpm / tarball / Docker `influxdb` | deb / rpm / tarball / Docker `influxdb2` + **独立 `influx-cli`** | DEB/RPM（含 `influxdb3-core` systemd unit）/ brew / tarball / Docker **`influxdb:3-core`** |
| 端口 | **8086** HTTP + **8088** RPC（备份专用，必须开才能 backup） | **8086**（UI + API，备份走 HTTP，无 8088） | **8181**（默认 `--http-bind 0.0.0.0:8181`） |
| 默认是否监听全网卡 | 是（8086） | 是 | **是** —— `--without-auth` 时任何能连到 8181 的人都能读写全库 |
| 配置方式 | `influxdb.conf`（TOML），`INFLUXDB_*` 环境变量优先 | flag / `INFLUXD_*` / config | flag / `INFLUXDB3_*` / TOML |
| 依赖 | 无（自成对，配 Kapacitor/Telegraf 另装） | 无 | **无**（UI 要自己配外部 Grafana/自建） |
| 内置 UI | ❌（1.x 的 Chronograf 另装） | ✅ | ❌ |
| K8s/Helm | 非官方主推 | 非官方主推 | **Helm 只服务 Clustered**，Core 用 systemd/compose |
| 硬件底线 | 官方："设计针对 SSD，未在 HDD 上测试"，恢复与可用性需 **≥1000 IOPS** | 同 | 本地 `file` 模式同；对象存储模式免除本地容量压力 |

### 5.2 三个必须写进上线清单的坑

1. **`influxdb:latest` 从 2026-09-15 起指向 InfluxDB 3 Core**。任何没 pin 标签的老 compose 在下次 `pull` 时会把 1.x/2.x 数据目录遇上不兼容引擎 —— 本仓库统一用 `influxdb:3-core` / 显式版本号（见 `deploy/docker-compose.yml`）。
2. **v3 默认免鉴权 + 默认绑 0.0.0.0**。要么像本项目把 8181 只绑 `127.0.0.1`，要么上 admin token 并用反代鉴权；Core 只有 operator/admin 两种 token，**没有细粒度权限**（那是 Clustered/Cloud 的能力）。
3. **3 Core 的 retention 建库时定死、删数据只能删表、备份要自己写脚本**。上线前必须把"清理年表 + 冷拷贝 data-dir"跑通一次，而不是等到磁盘告急。

### 5.3 各代最小可用生产形态

```
# v1（1.13.2；避开 1.12.3）
influxdb.conf: index-version = "tsi1"; [data] cache-max-memory-size = "1g";
               max-values-per-tag = 100000（默认）; [coordinator] query-timeout = "60s"（默认 0=不限，必须改）
               max-concurrent-queries = 20（默认 0=不限）
backup:        开 8088 → influxd backup -portable -host 127.0.0.1:8088

# v2（2.9.1）
INFLUXD_QUERY_MEMORY_BYTES 必须实测生效；OS 层 vm.max_mmap_count 调大
upgrade from 1.x: influxd upgrade（原地迁数据+WAL+bolt 元数据；首次启动重建 TSI 索引，可能很久）
回退警告: token 哈希开启后不能降回 <2.8.0

# v3（3.12.2，不要停在 3.11.6 及以前）
serve --object-store=file --data-dir=... （别用默认 0.0.0.0:8181）
       --exec-mem-pool-* / --parquet-mem-cache-size / --max-concurrent-queries 按机器上限设
       --data-retention（建库时给）  --delete-grace-period（默认 24h，硬删等于不可恢复）
升级前: 备份 catalog（3.10 起格式单向）
```

---

## 6. 迁移与共存

| 路径 | 是否原地升级 | 手段 | 代价 |
|---|---|---|---|
| 1.x → 2.x | ✅ 有官方工具 | `influxd upgrade`（数据/WAL/用户/权限 → bucket + bolt，自动建 DBRP 映射） | 首次启动重建 TSI 索引，大库数小时 |
| 1.x/2.x → 3.x | ❌ **没有** | ①双写/重灌：`influx_inspect export -lponly`（1.x）或 `influx export` / `influxd inspect export-lp`（2.x）导出 line protocol → `influxdb3 write --database X --file Y.lp`（3.12 起 `import` 有任务跟踪，官方称导入快 ~3x）；②用 3.x 的**兼容端点**直接让老客户端继续打：`/write`、`/api/v2/write`、`/query`(InfluxQL) | 历史数据要全量重灌；CQ/tasks 要重写成 Processing Engine 插件；Flux 查询要改 SQL；3.x 的 InfluxQL **不完整**（`SLIMIT`/`SOFFSET`、部分元查询不支持） |
| 3.x 跨小版本 | ⚠️ 只能往前 | 二进制替换 + catalog 自动迁移（3.10 格式 v2→v3 单向） | 降级不支持；先备份 catalog |
| Parquet/WAL 格式跨 3.x 小版本兼容性 | ❓ 官方未见明确的前/后向兼容承诺，3.10 的 catalog 单向迁移说明格式确会变 | — | 生产上按"每次升级都可能不可回头"准备 |

**共存策略（本项目现状）**：写路径三代通用（line protocol 完全一致），差异收敛在管理面，因此切换后端只是配置项 `[influx] version`。详细分派边界见 [deploy.md §12](deploy.md)，CI 的 `backend-smoke` 用 `influxdb:1.8-alpine` / `influxdb:2.7-alpine` / `influxdb:3-core` 各跑一遍"建库→写→数行数→删表+覆盖写幂等"。

---

## 7. 给本项目的落地建议

1. **默认后端继续选 v3 Core**，但版本下限设在 **≥3.11.7、推荐 3.12.2** —— 3.11.7 之前存在"服务端确认过的写入在崩溃后丢失"的已确认修复项，与本项目 `written` ACK 模式的语义直接冲突。
2. **把 v3 缺的三件事在桥接器侧补齐**（已经做了两处，第三处要排上）：
   - 崩溃续传：SQLite WAL 前置队列 ✅
   - 整批原子：`accept_partial=false` ✅
   - **备份**：`deploy/scripts/backup.sh` 目前只能是冷拷贝 `--data-dir`；要么给 compose 加 `stop → tar → start` 的窗口，要么显式声明"单机不做备份，靠重灌"并让业务签字。
3. **不要按代际"顺手"选 v2**。新项目用 v2 意味着把查询资产押在 Flux（维护模式、进不了 3.x），同时放弃 3 的列存优势 —— 两头不占。
4. **保留 v1 分派能力**（已实现）。现网客户大量是 1.8 存量，`version=v1` 让交付不需要说服对方升级 —— 这比"我们只支持最新代"值钱。
5. **年表 + tag 白名单**这个决定在三代上都是对的：本项目 333 msg/s、万级设备约 3 万 series，离任何一代的瓶颈都有距离；真正会把单机打死的是 `uuid` 进 tag（基数爆炸）和一个不带 `LIMIT` 的全表 `GROUP BY`（v1/v2 尤其危险，因为 1.x 默认 `query-timeout=0` 不限时）。
6. **回退方案**：如果压测发现 v3 Core 在"最近数据点查 + 高并发"上不如预期，先加 `last_cache` 而不是换代；真要换，`version=v1` + 8086 一行配置就能切，历史数据继续留在 1.8 不动（双写期由 broker QoS1 + 固定 client_id 保证不漏）。

### 上线检查清单（go/no-go）

- [ ] 镜像标签写死版本号（**绝不使用 `influxdb:latest`**）
- [ ] 数据端口不暴露公网：3.x 绑 `127.0.0.1` 或启用 admin token；1.x 关闭匿名、改默认空密码
- [ ] 3.x：`--data-retention` 在建库前确定（之后改不了）；1.x：retention policy 建好；2.x：bucket retention 配好
- [ ] 内存护栏：1.x 设 `query-timeout`/`max-concurrent-queries`；2.x 验证 `query-memory-bytes` 生效；3.x 设 `--max-concurrent-queries` + 容器 `mem_limit`（本仓库给 1g，压测后校准）
- [ ] series 基数进监控（SQL/InfluxQL 语句见 deploy.md §8），设告警阈值
- [ ] 备份演练做一次并验证可恢复；3.x 额外备份 catalog；1.x 确认 8088 可达；2.x 确认 restore 带 `--operator-token`
- [ ] 清理路径验证：3.x 软删表 → 磁盘什么时候真降（`--delete-grace-period` 与 compaction），别把"已删除"当成"已释放"
- [ ] 客户端错误处理与代际语义对齐（v2 的 422 = 部分入库、v3 `accept_partial` 默认 true、1.x 的错误藏在 200 响应体里）
- [ ] 升级预案写明"不可回退点"（v2 token 哈希、v3 catalog 格式）

---

## 8. 来源

官方（InfluxData）
- [InfluxDB 3 Core release notes](https://docs.influxdata.com/influxdb3/core/release-notes/)（2026-10-29，v3.12.2 / 3.11.8 / 3.11.7 WAL flush、catalog repair）
- [InfluxDB 3 Core 文档首页](https://docs.influxdata.com/influxdb3/core/) 与 [3 Enterprise 文档](https://docs.influxdata.com/influxdb3/enterprise/)（Core 特性清单；HA 与历史查询属 Enterprise）
- [Install InfluxDB 3 Core](https://docs.influxdata.com/influxdb3/core/install/)（`influxdb:3-core`；"2026-09-15 起 latest 指向 3 Core"）
- [InfluxDB 3 Core config options](https://docs.influxdata.com/influxdb3/core/reference/config-options/)（`--object-store=memory|file|s3|google|azure`、默认 `0.0.0.0:8181`、`--without-auth`、retention/compaction 相关 flag）
- [Migrate from InfluxDB v1/v2 to 3](https://docs.influxdata.com/influxdb3/core/get-started/migrate-from-influxdb-v1-v2/) 与 [InfluxQL feature support](https://docs.influxdata.com/influxdb3/core/reference/influxql/feature-support/)
- [The plan for InfluxDB 3.0 Open Source](https://www.influxdata.com/blog/the-plan-for-influxdb-3-0-open-source/)（2025-04-15 GA 更新）· [InfluxDB 3 OSS GA](https://www.influxdata.com/blog/influxdb-3-oss-ga/)
- [The future of Flux](https://docs.influxdata.com/flux/v0/future-of-flux/)（2025-01-13：维护模式、不 EOL、3.x 不支持）
- [InfluxDB v1 release notes](https://docs.influxdata.com/influxdb/v1/about_the_project/release-notes/)（v1.13.2 = 2026-09-02；"强烈建议 ≥1.12.4"）· [v1 stability & compatibility](https://docs.influxdata.com/influxdb/v1/administration/stability_and_compatibility/) · [v1 config](https://docs.influxdata.com/influxdb/v1/administration/config/) · [v1 hardware sizing](https://docs.influxdata.com/influxdb/v1/guides/hardware_sizing/) · [v1 ports](https://docs.influxdata.com/influxdb/v1/administration/ports/)
- [InfluxDB OSS v2 release notes](https://docs.influxdata.com/influxdb/v2/reference/release-notes/influxdb/)（2.7.11 mmap / 2.7.12 TagValueIterator / 2.9.1 compaction）· [v1→v2 automatic upgrade](https://docs.influxdata.com/influxdb/v2/install/upgrade/v1-to-v2/automatic-upgrade/) · [migrate CQs to tasks](https://docs.influxdata.com/influxdb/v2/install/upgrade/v1-to-v2/migrate-cqs/) · [backup](https://docs.influxdata.com/influxdb/v2/admin/backup-restore/backup/) · [write troubleshooting（422 语义）](https://docs.influxdata.com/influxdb/v2/write-data/troubleshoot/) · [resolve high cardinality](https://docs.influxdata.com/influxdb/v2/write-data/best-practices/resolve-high-cardinality/)
- GitHub：[influxdata/influxdb releases](https://github.com/influxdata/influxdb/releases)、tags `v1.13.2` / `v2.9.1` / `v2.10.0`（本次用 `git ls-remote` 复核）；[#27070 Flux 内存配额未生效](https://github.com/influxdata/influxdb/issues/27070)；[discussion #27419（Core 不支持按行删除）](https://github.com/influxdata/influxdb/discussions/27419)

第三方（含竞品，带立场）
- [InfoQ：InfluxDB 3 开源版本 GA 访谈 Paul Dix](https://www.infoq.cn/article/gouqKaozMzooQWdc4WDC)（Core 省略长期存储优化/压缩/HA/读副本/细粒度权限）
- [TDengine：InfluxDB Leaves OSS Users Behind](https://tdengine.com/influxdb-leaves-oss-users-behind/)、[query comparison](https://tdengine.com/influxdb-3-query-comparison/)（竞品：低基数 1.8 更快、48x 数字来源）
- [QuestDB：Best Time-Series Databases 2026](https://questdb.com/blog/best-time-series-databases/)（竞品：36x/130x 自测）
- [SegmentFault：InfluxDB 3.0 写入吞吐实测](https://segmentfault.com/a/1190000046196700)（<100 万设备时 3.0 写慢于 1.8）
- [百家号：1.8 → 3 Core 自建迁移实录](https://baijiahao.baidu.com/s?id=1873858987037541127)（40s→3s 聚合、CQ 缺失、tag 基数踩坑）
- [endoflife.date/influxdb](https://endoflife.date/influxdb)（OSS 无正式支持政策）· [阿里云 TSDB for InfluxDB EOL 公告](https://help.aliyun.com/document_detail/112677.html)

项目内
- [deploy.md §12 v1/v2/v3 能力边界](deploy.md) · `deploy/docker-compose.yml` · `mqtt_influx_bridge/admin.py` · `mqtt_influx_bridge/writer.py`

---

## 9. 未证实 / 需上线前复核

1. `v2.10.0` 是否已发布正式二进制（有 tag，release notes 无条目）。
2. 3.x **Parquet/WAL 跨小版本前/后向兼容**是否有官方承诺（3.10 catalog 单向迁移暗示会变）。
3. `--object-store=s3|google|azure` 在 **Core** 上的成熟度：config-options 未标注 Enterprise-only，但官方对外把对象存储作为 Enterprise/云的主打能力，单机建议只用 `file`。
4. 内存超限的确切行为（哪些路径返回 429、哪些直接 OOM kill），以及 `--exec-mem-pool-*` / `--file-cache-size` 等 flag 在 3.12 的准确拼写（以 `influxdb3 serve --help` 为准 —— 本仓库已因"3.x CLI 参数名版本漂移"吃过亏）。
5. Core 的官方最低内存要求（未公布）；本仓库 `mem_limit: 1g` 是护栏不是结论，必须压测校准。
