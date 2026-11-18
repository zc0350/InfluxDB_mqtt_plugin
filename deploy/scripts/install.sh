#!/usr/bin/env bash
# 安装 MQTT→InfluxDB 桥接器（裸机 / systemd，Linux）
#
# 前提：InfluxDB（v1/v2/v3 任一）与 MQTT broker 由你自己安装、配置并启动。
#       本脚本只装桥接器本体、写配置与 systemd unit、启动并自检。
#
# 用法：sudo bash deploy/scripts/install.sh [选项]
# 幂等：已存在的 config.toml / bridge.env 一律不覆盖；已装的桥接器跳过 pip install。
set -euo pipefail

say()  { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m!!  %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m!! %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- 参数
INFLUX_URL="${INFLUX_URL:-http://127.0.0.1:8181}"  # 你已安装的 InfluxDB HTTP 地址
INFLUX_VERSION="${INFLUX_VERSION:-v3}"             # 后端代际 v1 | v2 | v3
DB_NAME="${DB_NAME:-mqtt}"                         # v3=db / v1=db / v2=bucket
RETENTION="${RETENTION:-}"                         # 建库保留期，如 30d；留空 = 不限
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"
MQTT_USER="${MQTT_USER:-bridge}"
MQTT_PASS="${MQTT_PASS:-}"
PRODUCT_KEY="${PRODUCT_KEY:-}"                     # 单产品收紧订阅面，如 TT1000
INFLUX_TOKEN="${INFLUX_TOKEN:-}"                   # v2/v3 若启用了鉴权
BRIDGE_WHL=""                                      # 离线机器：指定本地 whl

_repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
_bridge="/opt/mqtt-influx-bridge/venv/bin/mqtt-influx-bridge"
_cfg="/etc/mqtt-influx-bridge/config.toml"
_env="/etc/mqtt-influx-bridge/bridge.env"

usage() {
  cat <<'USAGE'
用法: sudo bash deploy/scripts/install.sh [选项]

前提：InfluxDB 与 MQTT broker 已由你自己装好并启动，本脚本不安装它们。

  --influx-url URL       InfluxDB HTTP 地址（默认 http://127.0.0.1:8181）
  --influx-version V     后端代际 v1|v2|v3（默认 v3；v2 还需 org，写在 bridge.env）
  --db NAME              库名 / bucket（默认 mqtt）
  --retention PERIOD     建库保留期，如 30d；仅库不存在时生效（默认不限）
  --influx-token TOKEN   v2/v3 鉴权 token（默认空，即无鉴权）
  --mqtt-host HOST       MQTT broker 地址（默认 127.0.0.1）
  --mqtt-port PORT       MQTT 端口（默认 1883）
  --mqtt-user USER       桥接器 MQTT 账号（默认 bridge）
  --mqtt-pass PASS       桥接器 MQTT 密码（留空则需在 bridge.env 里手动填）
  --product-key KEY      ${productKey} 占位符取值（默认空 = 订阅所有产品）
  --bridge-whl PATH      用本地 wheel 安装（离线机器）
  -h, --help             显示帮助

环境变量同样生效：INFLUX_URL / INFLUX_VERSION / DB_NAME / RETENTION /
INFLUX_TOKEN / MQTT_HOST / MQTT_PORT / MQTT_USER / MQTT_PASS / PRODUCT_KEY。

幂等：config.toml 与 bridge.env 已存在则不覆盖。
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --influx-url)      INFLUX_URL="${2:?--influx-url 缺值}"; shift 2 ;;
    --influx-version)  INFLUX_VERSION="${2:?--influx-version 缺值}"; shift 2 ;;
    --db)              DB_NAME="${2:?--db 缺值}"; shift 2 ;;
    --retention)       RETENTION="${2:?--retention 缺值}"; shift 2 ;;
    --influx-token)    INFLUX_TOKEN="${2:?--influx-token 缺值}"; shift 2 ;;
    --mqtt-host)       MQTT_HOST="${2:?--mqtt-host 缺值}"; shift 2 ;;
    --mqtt-port)       MQTT_PORT="${2:?--mqtt-port 缺值}"; shift 2 ;;
    --mqtt-user)       MQTT_USER="${2:?--mqtt-user 缺值}"; shift 2 ;;
    --mqtt-pass)       MQTT_PASS="${2:?--mqtt-pass 缺值}"; shift 2 ;;
    --product-key)     PRODUCT_KEY="${2:?--product-key 缺值}"; shift 2 ;;
    --bridge-whl)      BRIDGE_WHL="${2:?--bridge-whl 缺值}"; shift 2 ;;
    -h|--help)         usage; exit 0 ;;
    *)                 die "未知参数: $1（--help 查看用法）" ;;
  esac
done

# ---------------------------------------------------------------- 前置检查
[ "$(id -u)" -eq 0 ] || die "请用 root 运行：sudo bash $0 $*"
[ -r /proc/meminfo ] || die "本脚本仅支持 Linux"
if ! command -v systemctl >/dev/null 2>&1 || [ ! -d /run/systemd/system ]; then
  die "未检测到运行中的 systemd；本脚本只支持 systemd 主机"
fi
command -v python3 >/dev/null 2>&1 || die "缺少命令: python3"

_py_major="$(python3 -c 'import sys; print(sys.version_info[0])')"
_py_minor="$(python3 -c 'import sys; print(sys.version_info[1])')"
{ [ "$_py_major" -gt 3 ] || { [ "$_py_major" -eq 3 ] && [ "$_py_minor" -ge 11 ]; }; } \
  || die "桥接器需要 Python >= 3.11，当前 $(python3 --version 2>&1)"

case "$INFLUX_VERSION" in v1|v2|v3) ;; *) die "--influx-version 必须是 v1 / v2 / v3" ;; esac
[ -n "$MQTT_PASS" ] || warn "未设置 MQTT 密码；若 broker 需要鉴权，请装完后编辑 $_env 的 MQTT_PASSWORD"

# ---------------------------------------------------------------- 1/4 安装桥接器
say "1/4 安装桥接器到 /opt/mqtt-influx-bridge/venv"
if [ ! -x "$_bridge" ]; then
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

# ---------------------------------------------------------------- 2/4 写配置（不覆盖已有）
say "2/4 准备配置目录与文件"
if ! id mqttbridge >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/mqtt-influx-bridge --create-home --shell /usr/sbin/nologin mqttbridge
fi
mkdir -p /etc/mqtt-influx-bridge /var/lib/mqtt-influx-bridge

if [ ! -f "$_cfg" ]; then
  _cfg_src=""
  for _candidate in "$_repo_dir/deploy/config/config.toml.example" "$_repo_dir/config.tt1000.toml" "$_repo_dir/config.example.toml"; do
    [ -f "$_candidate" ] && { _cfg_src="$_candidate"; break; }
  done
  [ -n "$_cfg_src" ] || die "找不到 config.toml 样例（deploy/config/config.toml.example）"
  cp "$_cfg_src" "$_cfg"
  say "  已从 $(basename "$_cfg_src") 生成 config.toml —— 请核对 uplink 的真实 topic"
else
  say "  config.toml 已存在，保留不覆盖"
fi

if [ ! -f "$_env" ]; then
  cat > "$_env" <<EOF
# 由 install.sh 生成，权限 0600。敏感值放这里，不写进 config.toml。
# InfluxDB 与 MQTT broker 由你自行安装；地址/凭据在这里指向它们。
WAL_PATH=/var/lib/mqtt-influx-bridge/bridge.db
MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USER}
MQTT_PASSWORD=${MQTT_PASS}
INFLUX_URL=${INFLUX_URL}
INFLUX_DATABASE=${DB_NAME}
INFLUX_VERSION=${INFLUX_VERSION}
# v2 必填 org；写在这里覆盖配置里的空值（v1/v3 留空即可）
INFLUX_ORG=
INFLUX_TOKEN=${INFLUX_TOKEN}
INFLUX_AUTO_CREATE_DB=true
INFLUX_RETENTION_PERIOD=${RETENTION}
PRODUCT_KEY=${PRODUCT_KEY}
YEAR_TZ_OFFSET_MINUTES=480
BRIDGE_ACK_MODE=persisted
LOG_LEVEL=INFO
EOF
  say "  已生成 bridge.env（0600）"
else
  say "  bridge.env 已存在，保留不覆盖"
fi
chown -R mqttbridge:mqttbridge /var/lib/mqtt-influx-bridge /etc/mqtt-influx-bridge
chmod 0600 "$_env"

# ---------------------------------------------------------------- 3/4 写 systemd unit 并启动
say "3/4 写入 systemd unit 并启动桥接器"
if [ -f "$_repo_dir/deploy/systemd/mqtt-influx-bridge.service" ]; then
  cp "$_repo_dir/deploy/systemd/mqtt-influx-bridge.service" /etc/systemd/system/mqtt-influx-bridge.service
else
  warn "找不到桥接器 unit 模板，沿用已有文件（若有）"
fi
systemctl daemon-reload
systemctl enable --now mqtt-influx-bridge >/dev/null 2>&1 \
  || warn "桥接器启动失败：journalctl -u mqtt-influx-bridge -n 50"

# ---------------------------------------------------------------- 4/4 自检
say "4/4 自检"
_status="$(systemctl is-active mqtt-influx-bridge || true)"
printf '  mqtt-influx-bridge: %s\n' "$_status"

( set -a; . "$_env"; set +a
  "$_bridge" -c "$_cfg" check >/dev/null 2>&1 ) \
  && echo "  配置校验: pass" || warn "配置校验未过：$_bridge -c $_cfg check"

# 用桥接器自带的 init-db 探后端并按代际建库（走 HTTP API，不依赖 influxdb3 CLI）
if ( set -a; . "$_env"; set +a; "$_bridge" -c "$_cfg" ping >/dev/null 2>&1 ); then
  if ( set -a; . "$_env"; set +a; "$_bridge" -c "$_cfg" init-db >/dev/null 2>&1 ); then
    echo "  后端探活 + 建库: pass"
  else
    warn "后端可达但建库失败：$_bridge -c $_cfg init-db"
  fi
else
  warn "InfluxDB 当前不可达（$_env 里 INFLUX_URL）；确认它已启动，桥接器启动后也会自动重试建库"
fi

cat <<NEXT

完成。常用命令：
  systemctl status mqtt-influx-bridge
  journalctl -u mqtt-influx-bridge -f
  $_bridge -c $_cfg status                                   # WAL 队列
  $_bridge -c $_cfg ping                                     # 后端探活
  $_bridge -c $_cfg query --sql "SELECT count(*) FROM mqtt_data_$(date +%Y%m)"

注意：
  1. 编辑 $_cfg / $_env 后需 systemctl restart mqtt-influx-bridge。
  2. InfluxDB 与 MQTT broker 由你自己维护；本脚本不管理它们的服务。
  3. 上线前确认 config.toml 里 uplink 的真实 topic，否则数据落进 mqtt_unknown_* 兜底表。
NEXT
