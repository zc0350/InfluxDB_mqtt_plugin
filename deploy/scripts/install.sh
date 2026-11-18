#!/usr/bin/env bash
# 一键安装：InfluxDB 3 Core + MQTT→InfluxDB 桥接器（裸机 / systemd，Linux）
#
# 用法：sudo bash deploy/scripts/install.sh [选项]
#
# 做四件事：装 influxdb3 二进制 → 装桥接器 venv → 写 systemd unit → 建库并启动。
# 幂等：已存在的 config.toml / bridge.env / mosquitto 密码一律不覆盖；
#       已安装且可执行的 influxdb3 跳过下载。重复执行只会 reconcile 服务状态。
set -euo pipefail

say()  { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m!!  %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m!! %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- 参数
INFLUXDB_VER="${INFLUXDB_VER:-3.12.2}"     # 内部变量：别叫 INFLUX_VERSION（那是桥接器的 v3/v2/v1）
DB_NAME="${DB_NAME:-mqtt}"
RETENTION="${RETENTION:-}"                 # 如 30d；留空 = 不限。v3 建库后不可改
HTTP_BIND="${HTTP_BIND:-127.0.0.1:8181}"   # 默认只绑回环；--without-auth 下别改成 0.0.0.0
MQTT_PORT="${MQTT_PORT:-1883}"
MQTT_USER="${MQTT_USER:-bridge}"
MQTT_PASS="${MQTT_PASS:-}"                 # 空 = 自动生成随机密码
PRODUCT_KEY="${PRODUCT_KEY:-}"             # 单产品收紧订阅面，如 TT1000
WITH_BROKER=1                              # 默认同时装 mosquitto
BRIDGE_WHL=""                              # 可选：指定本地 whl（离线机器）

_repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

usage() {
  cat <<'USAGE'
用法: sudo bash deploy/scripts/install.sh [选项]

  --influxdb-version V   InfluxDB 3 Core 版本（默认 3.12.2）
  --db NAME              数据库名（默认 mqtt）
  --retention PERIOD     建库保留期，如 30d；仅在库不存在时生效（默认不限）
  --http-bind ADDR       influxdb3 监听地址（默认 127.0.0.1:8181）
  --mqtt-port PORT       MQTT 端口（默认 1883）
  --mqtt-user USER       桥接器 MQTT 账号（默认 bridge）
  --mqtt-pass PASS       桥接器 MQTT 密码（留空则随机生成并打印）
  --product-key KEY      ${productKey} 占位符取值（默认空 = 订阅所有产品）
  --no-broker            不装 mosquitto（连接已有外部 broker，用 --mqtt-port/改 bridge.env）
  --bridge-whl PATH      用本地 wheel 安装桥接器（离线机器）
  -h, --help             显示帮助

环境变量同样生效：INFLUXDB_VER / DB_NAME / RETENTION / HTTP_BIND /
MQTT_PORT / MQTT_USER / MQTT_PASS / PRODUCT_KEY。
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --influxdb-version) INFLUXDB_VER="${2:?--influxdb-version 缺值}"; shift 2 ;;
    --db)               DB_NAME="${2:?--db 缺值}"; shift 2 ;;
    --retention)        RETENTION="${2:?--retention 缺值}"; shift 2 ;;
    --http-bind)        HTTP_BIND="${2:?--http-bind 缺值}"; shift 2 ;;
    --mqtt-port)        MQTT_PORT="${2:?--mqtt-port 缺值}"; shift 2 ;;
    --mqtt-user)        MQTT_USER="${2:?--mqtt-user 缺值}"; shift 2 ;;
    --mqtt-pass)        MQTT_PASS="${2:?--mqtt-pass 缺值}"; shift 2 ;;
    --product-key)      PRODUCT_KEY="${2:?--product-key 缺值}"; shift 2 ;;
    --no-broker)        WITH_BROKER=0; shift ;;
    --bridge-whl)       BRIDGE_WHL="${2:?--bridge-whl 缺值}"; shift 2 ;;
    -h|--help)          usage; exit 0 ;;
    *)                  die "未知参数: $1（--help 查看用法）" ;;
  esac
done

# ---------------------------------------------------------------- 前置检查
[ "$(id -u)" -eq 0 ] || die "请用 root 运行：sudo bash $0 $*"
[ -r /proc/meminfo ] || die "本脚本仅支持 Linux"

if ! command -v systemctl >/dev/null 2>&1 || [ ! -d /run/systemd/system ]; then
  die "未检测到运行中的 systemd；本脚本只支持 systemd 主机"
fi
for c in curl tar awk python3; do
  command -v "$c" >/dev/null 2>&1 || die "缺少命令: $c"
done

_py_major="$(python3 -c 'import sys; print(sys.version_info[0])')"
_py_minor="$(python3 -c 'import sys; print(sys.version_info[1])')"
{ [ "$_py_major" -gt 3 ] || { [ "$_py_major" -eq 3 ] && [ "$_py_minor" -ge 11 ]; }; } \
  || die "桥接器需要 Python >= 3.11，当前 $(python3 --version 2>&1)"

case "$(uname -m)" in
  x86_64|amd64)   _arch="amd64" ;;
  aarch64|arm64)  _arch="arm64" ;;
  *)              die "不支持的架构: $(uname -m)（仅 amd64 / arm64）" ;;
esac

# 随机密码：只在既没传 --mqtt-pass 又没有现成 bridge.env 时生成，避免覆盖用户密码
_generated_pass=0
if [ -z "$MQTT_PASS" ] && [ ! -f /etc/mqtt-influx-bridge/bridge.env ]; then
  MQTT_PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24)"
  _generated_pass=1
fi

# ---------------------------------------------------------------- 自适应内存
# 关键：influxdb3 的三个内存 flag 默认是整机总内存的百分比（20%/50%/20%），
# cgroup 限住 MemoryMax 后进程仍按大机器的比例申请，会被 OOM kill —— 在桥接器侧
# 表现成一片 connection refused。所以必须按本机内存算成绝对值写死。
_calc_memory() {
  local mem_mb pct budget exec_mb snap_mb cache_mb high_mb
  mem_mb="$(awk '/MemTotal/{printf "%d", $2/1024}' /proc/meminfo)"
  if [ "$mem_mb" -ge 16384 ]; then pct=60; else pct=50; fi   # 小机器多留给系统/page cache
  budget=$((mem_mb * pct / 100))
  exec_mb=$((budget * 30 / 100));  [ "$exec_mb"  -lt 256 ] && exec_mb=256
  snap_mb=$((budget * 30 / 100));  [ "$snap_mb"  -lt 256 ] && snap_mb=256
  cache_mb=$((budget * 20 / 100)); [ "$cache_mb" -lt 256 ] && cache_mb=256
  high_mb=$((budget * 85 / 100))
  echo "$exec_mb $snap_mb $cache_mb $high_mb $budget"
}
read -r EXEC_MEM SNAP_MEM CACHE_MEM HIGH_MEM MAX_MEM <<<"$(_calc_memory)"
say "本机内存适配：exec=${EXEC_MEM}mb snapshot=${SNAP_MEM}mb cache=${CACHE_MEM}mb (MemoryMax=${MAX_MEM}M)"

# ---------------------------------------------------------------- 1/7 安装 InfluxDB 3 Core
say "1/7 安装 InfluxDB 3 Core ${INFLUXDB_VER}"
if [ -x /usr/local/bin/influxdb3 ]; then
  say "  /usr/local/bin/influxdb3 已存在，跳过下载（$(/usr/local/bin/influxdb3 --version 2>&1 | head -1)）"
elif command -v influxdb3 >/dev/null 2>&1; then
  say "  系统已有 influxdb3 ($(command -v influxdb3))，跳过下载"
else
  _artifact="influxdb3-core-${INFLUXDB_VER}_linux_${_arch}.tar.gz"
  _base="https://dl.influxdata.com/influxdb/releases"
  _tmp="$(mktemp -d)"
  trap 'rm -rf "$_tmp"' EXIT
  say "  下载 ${_artifact}"
  curl -fsSL --retry 3 --max-time 900 "$_base/${_artifact}" -o "$_tmp/$_artifact" \
    || die "下载失败，请检查网络与版本号（$_base/${_artifact}）"
  curl -fsSL --retry 3 --max-time 60 "$_base/${_artifact}.sha256" -o "$_tmp/$_artifact.sha256" \
    || die "下载 SHA256 校验文件失败"
  ( cd "$_tmp" && awk '{print $1"  "$2}' "$_artifact.sha256" | sha256sum -c - ) \
    || die "SHA256 校验失败：文件被篡改或下载不完整"
  tar -xzf "$_tmp/$_artifact" -C "$_tmp"
  _bin="$(find "$_tmp" -type f -name influxdb3 -perm -u+x | head -1)"
  [ -n "$_bin" ] || die "tar 包里找不到 influxdb3 可执行文件"
  install -m 0755 "$_bin" /usr/local/bin/influxdb3
  rm -rf "$_tmp"
  trap - EXIT
  say "  已安装：$(influxdb3 --version 2>&1 | head -1)"
fi
if ! id influxdb3 >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/influxdb3 --create-home --shell /usr/sbin/nologin influxdb3
  say "  已创建用户 influxdb3"
fi

# ---------------------------------------------------------------- 2/7 安装 mosquitto（可选）
if [ "$WITH_BROKER" -eq 1 ]; then
  say "2/7 安装并配置 MQTT broker (mosquitto)"
  if command -v mosquitto >/dev/null 2>&1; then
    say "  mosquitto 已安装，跳过包安装"
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq mosquitto mosquitto-clients
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y mosquitto mosquitto-clients
  elif command -v yum >/dev/null 2>&1; then
    yum install -y mosquitto mosquitto-clients
  else
    die "无法自动安装 mosquitto（未找到 apt-get/dnf/yum）；请自行安装或改用 --no-broker"
  fi

  mkdir -p /etc/mosquitto/conf.d /var/lib/mosquitto
  # 部分发行版的主配置自带 listener/allow_anonymous，与 conf.d 叠加会重复监听或放开匿名。
  # 在主配置里把这些行注释掉，统一由 bridge.conf 接管（幂等，只在首次含原样行时替换）。
  if [ -f /etc/mosquitto/mosquitto.conf ]; then
    sed -i -E 's/^[[:space:]]*(listener[[:space:]]|allow_anonymous[[:space:]]|password_file[[:space:]]|protocol[[:space:]])/# install.sh-disabled: &/' \
      /etc/mosquitto/mosquitto.conf
  fi
  cat > /etc/mosquitto/conf.d/bridge.conf <<'EOF'
# 由 install.sh 生成：桥接器场景的 MQTT broker 配置
listener 1883 0.0.0.0
protocol mqtt
allow_anonymous false
password_file /etc/mosquitto/passwd

# QoS 1 持久会话：桥接器离线期间的消息靠 broker 排队。默认 1000 条会丢 oldest，这里放大。
queued_message_limit 1000000
max_inflight_messages 20
persistence true
persistence_location /var/lib/mosquitto/
autosave_interval 180
log_dest stdout
log_type warning
log_type error
log_type notice
EOF
  # 部分发行版的默认配置未 include conf.d，补一行（幂等）
  if [ -f /etc/mosquitto/mosquitto.conf ] \
     && ! grep -q '^[[:space:]]*include_dir[[:space:]]\+/etc/mosquitto/conf\.d' /etc/mosquitto/mosquitto.conf; then
    printf '\ninclude_dir /etc/mosquitto/conf.d\n' >> /etc/mosquitto/mosquitto.conf
  fi
  if [ ! -f /etc/mosquitto/passwd ]; then
    mosquitto_passwd -c -b /etc/mosquitto/passwd "$MQTT_USER" "$MQTT_PASS"
    say "  已创建 MQTT 账号 $MQTT_USER"
  else
    say "  /etc/mosquitto/passwd 已存在，保留原有账号（桥接器凭据见 bridge.env）"
  fi
  chown mosquitto:mosquitto /etc/mosquitto/passwd /etc/mosquitto/conf.d/bridge.conf 2>/dev/null || true
  chmod 0640 /etc/mosquitto/passwd
  systemctl enable --now mosquitto >/dev/null 2>&1 || warn "mosquitto 启动失败，检查: journalctl -u mosquitto -n 50"
else
  say "2/7 跳过 MQTT broker（--no-broker），请在 bridge.env 里指向外部 broker"
fi

# ---------------------------------------------------------------- 3/7 安装桥接器
say "3/7 安装桥接器到 /opt/mqtt-influx-bridge/venv"
if [ ! -x /opt/mqtt-influx-bridge/venv/bin/mqtt-influx-bridge ]; then
  python3 -m venv /opt/mqtt-influx-bridge/venv
  /opt/mqtt-influx-bridge/venv/bin/pip install --quiet --upgrade pip
  if [ -n "$BRIDGE_WHL" ]; then
    [ -f "$BRIDGE_WHL" ] || die "wheel 不存在: $BRIDGE_WHL"
    /opt/mqtt-influx-bridge/venv/bin/pip install --quiet "$BRIDGE_WHL"
  elif [ -f "$_repo_dir/pyproject.toml" ]; then
    /opt/mqtt-influx-bridge/venv/bin/pip install --quiet "$_repo_dir"
  elif compgen -G "$_repo_dir/dist/mqtt_influx_bridge-*.whl" >/dev/null; then
    # 发布 zip 里可能没有 pyproject.toml，但有随包附带的 whl
    /opt/mqtt-influx-bridge/venv/bin/pip install --quiet "$_repo_dir"/dist/mqtt_influx_bridge-*.whl
  else
    die "找不到桥接器源码（pyproject.toml）或 wheel；用 --bridge-whl 指定本地 whl"
  fi
  say "  已安装：$('/opt/mqtt-influx-bridge/venv/bin/python' -c 'import mqtt_influx_bridge as m; print("v"+m.__version__)')"
else
  say "  桥接器已安装，跳过"
fi

# ---------------------------------------------------------------- 4/7 写配置（不覆盖已有）
say "4/7 准备配置目录与文件"
if ! id mqttbridge >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/mqtt-influx-bridge --create-home --shell /usr/sbin/nologin mqttbridge
fi
mkdir -p /etc/mqtt-influx-bridge /var/lib/mqtt-influx-bridge

if [ ! -f /etc/mqtt-influx-bridge/config.toml ]; then
  _cfg_src=""
  for _candidate in "$_repo_dir/deploy/config/config.toml.example" "$_repo_dir/config.tt1000.toml" "$_repo_dir/config.example.toml"; do
    [ -f "$_candidate" ] && { _cfg_src="$_candidate"; break; }
  done
  [ -n "$_cfg_src" ] || die "找不到 config.toml 样例（deploy/config/config.toml.example）"
  cp "$_cfg_src" /etc/mqtt-influx-bridge/config.toml
  say "  已从 $(basename "$_cfg_src") 生成 config.toml —— 请核对 uplink 的真实 topic"
else
  say "  config.toml 已存在，保留不覆盖"
fi

# INFLUX_URL 从 HTTP_BIND 推导；0.0.0.0 换回回环
_bind_host="${HTTP_BIND%%:*}"
_bind_port="${HTTP_BIND##*:}"
case "$_bind_port" in ''|*[!0-9]*) die "--http-bind 端口非法: $HTTP_BIND" ;; esac
[ "$_bind_host" = "0.0.0.0" ] && _bind_host="127.0.0.1"
_influx_origin="http://${_bind_host}:${_bind_port}"
if [ ! -f /etc/mqtt-influx-bridge/bridge.env ]; then
  cat > /etc/mqtt-influx-bridge/bridge.env <<EOF
# 由 install.sh 生成，权限 0600。敏感值放这里，不写进 config.toml。
WAL_PATH=/var/lib/mqtt-influx-bridge/bridge.db
MQTT_HOST=127.0.0.1
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USER}
MQTT_PASSWORD=${MQTT_PASS}
INFLUX_URL=${_influx_origin}
INFLUX_DATABASE=${DB_NAME}
INFLUX_VERSION=v3
INFLUX_AUTO_CREATE_DB=true
INFLUX_RETENTION_PERIOD=${RETENTION}
INFLUX_TOKEN=
PRODUCT_KEY=${PRODUCT_KEY}
YEAR_TZ_OFFSET_MINUTES=480
BRIDGE_ACK_MODE=persisted
LOG_LEVEL=INFO
EOF
  chmod 0600 /etc/mqtt-influx-bridge/bridge.env
  say "  已生成 bridge.env（0600）"
else
  say "  bridge.env 已存在，保留不覆盖"
fi
chown -R mqttbridge:mqttbridge /var/lib/mqtt-influx-bridge /etc/mqtt-influx-bridge
chmod 0600 /etc/mqtt-influx-bridge/bridge.env

# ---------------------------------------------------------------- 5/7 写 systemd unit
say "5/7 写入 systemd unit"
mkdir -p /etc/influxdb3
cat > /etc/influxdb3/influxdb3-core.env <<'EOF'
# 由 install.sh 生成。追加的 serve flag，一个词一个 flag；改完需 restart（serve 不热加载）。
INFLUXDB3_EXTRA_ARGS=
EOF
chmod 0600 /etc/influxdb3/influxdb3-core.env
chown root:influxdb3 /etc/influxdb3/influxdb3-core.env 2>/dev/null || true

cat > /etc/systemd/system/influxdb3-core.service <<EOF
# 由 install.sh 生成。内存 flag 按本机 $(awk '/MemTotal/{printf "%d GiB", $2/1024/1024}' /proc/meminfo) 算成绝对值。
[Unit]
Description=InfluxDB 3 Core (single-node, Arrow/Parquet engine)
Documentation=https://docs.influxdata.com/influxdb3/core/install/
After=network-online.target local-fs.target remote-fs.target
Wants=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=10

[Service]
Type=simple
User=influxdb3
Group=influxdb3
ExecStart=/usr/local/bin/influxdb3 serve \\
    --node-id=node0 \\
    --object-store=file \\
    --data-dir=/var/lib/influxdb3/data \\
    --http-bind=${HTTP_BIND} \\
    --without-auth \\
    --exec-mem-pool-size=${EXEC_MEM}mb \\
    --force-snapshot-mem-size=${SNAP_MEM}mb \\
    --file-cache-size=${CACHE_MEM}mb \\
    \$INFLUXDB3_EXTRA_ARGS
Environment=INFLUXDB3_EXTRA_ARGS=
EnvironmentFile=-/etc/influxdb3/influxdb3-core.env
StateDirectory=influxdb3/data
StateDirectoryMode=0700
Restart=always
RestartSec=3
TimeoutStartSec=600
TimeoutStopSec=120
KillSignal=SIGTERM
KillMode=control-group
LimitNOFILE=65536
TasksMax=8192
MemoryHigh=${HIGH_MEM}M
MemoryMax=${MAX_MEM}M
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ProtectKernelTunables=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictNamespaces=true
LockPersonality=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
StandardOutput=journal
StandardError=journal
SyslogIdentifier=influxdb3

[Install]
WantedBy=multi-user.target
EOF

if [ -f "$_repo_dir/deploy/systemd/mqtt-influx-bridge.service" ]; then
  cp "$_repo_dir/deploy/systemd/mqtt-influx-bridge.service" /etc/systemd/system/mqtt-influx-bridge.service
else
  warn "找不到桥接器 unit 模板，沿用已有文件（若有）"
fi
systemctl daemon-reload
say "  unit 已就位"

# ---------------------------------------------------------------- 6/7 建库 + 启动
say "6/7 启动服务并建库"
systemctl enable --now influxdb3-core >/dev/null 2>&1 || die "influxdb3-core 启动失败：journalctl -u influxdb3-core -n 50"

# create database 打的是 HTTP API，要等服务就绪；没有离线建库这条路
_ready=0
for _i in $(seq 1 60); do
  if curl -fsS --max-time 2 "${_influx_origin}/health" >/dev/null 2>&1; then _ready=1; break; fi
  sleep 1
done
[ "$_ready" -eq 1 ] || die "60s 内 influxdb3 /health 未就绪：journalctl -u influxdb3-core -n 50"

_last=""
for _i in $(seq 1 30); do
  if [ -n "$RETENTION" ]; then
    _last="$(influxdb3 create database "$DB_NAME" --host "$_influx_origin" --retention-period "$RETENTION" 2>&1)" && break
  else
    _last="$(influxdb3 create database "$DB_NAME" --host "$_influx_origin" 2>&1)" && break
  fi
  echo "$_last" | grep -qi 'already exist' && break
  sleep 1
done
if echo "$_last" | grep -qi 'already exist'; then
  say "  库 $DB_NAME 已存在，跳过（retention 建库后改不了）"
else
  say "  库 $DB_NAME 就绪${RETENTION:+（retention=$RETENTION）}"
fi

systemctl enable --now mqtt-influx-bridge >/dev/null 2>&1 \
  || warn "桥接器启动失败：journalctl -u mqtt-influx-bridge -n 50"

# ---------------------------------------------------------------- 7/7 自检
say "7/7 自检"
_status_db="$(systemctl is-active influxdb3-core || true)"
_status_bridge="$(systemctl is-active mqtt-influx-bridge || true)"
_status_broker="$(systemctl is-active mosquitto 2>/dev/null || echo skipped)"
printf '  influxdb3-core:     %s\n  mqtt-influx-bridge: %s\n  mosquitto:          %s\n' \
  "$_status_db" "$_status_bridge" "$_status_broker"

( set -a; . /etc/mqtt-influx-bridge/bridge.env; set +a
  /opt/mqtt-influx-bridge/venv/bin/mqtt-influx-bridge \
    -c /etc/mqtt-influx-bridge/config.toml check >/dev/null 2>&1 ) \
  && echo "  配置校验: pass" || warn "配置校验未过：/opt/mqtt-influx-bridge/venv/bin/mqtt-influx-bridge -c /etc/mqtt-influx-bridge/config.toml check"

cat <<NEXT

完成。常用命令：
  systemctl status influxdb3-core mqtt-influx-bridge
  journalctl -u mqtt-influx-bridge -f                      # 桥接器日志
  /opt/mqtt-influx-bridge/venv/bin/mqtt-influx-bridge -c /etc/mqtt-influx-bridge/config.toml status   # WAL 队列
  /opt/mqtt-influx-bridge/venv/bin/mqtt-influx-bridge -c /etc/mqtt-influx-bridge/config.toml query --sql "SELECT count(*) FROM mqtt_data_$(date +%Y%m)"

注意：
  1. 编辑 config.toml / bridge.env 后需 systemctl restart mqtt-influx-bridge。
  2. influxdb3 只绑 ${HTTP_BIND}（无鉴权）。要远程查询请加反代鉴权，别直接对公网放开。
  3. 上线前确认 config.toml 里 uplink 的真实 topic，否则数据落进 mqtt_unknown_* 兜底表。
NEXT

[ "$_generated_pass" -eq 1 ] && printf '\n自动生成的 MQTT 密码：%s\n（已写入 /etc/mqtt-influx-bridge/bridge.env 与 /etc/mosquitto/passwd，请妥善保存）\n' "$MQTT_PASS"
