#!/usr/bin/env bash
# 一键初始化并启动 InfluxDB 3 Core + mosquitto + 桥接器。
# 幂等：重复执行只会跳过已存在的库/账号，不会覆盖你改过的 .env 与 config.toml。
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/_common.sh

die() { printf '\033[1;31m!! %s\033[0m\n' "$*" >&2; exit 1; }

$COMPOSE version >/dev/null 2>&1 || die "找不到 docker compose（可用 COMPOSE='podman-compose' 覆盖）"

say "1/6 准备配置文件"
for f in .env bridge.env config/config.toml; do
  [ -e "$f" ] && { echo "  已有 $f，跳过"; continue; }
  cp "$f.example" "$f"
  echo "  从示例生成 $f  ← 请检查凭据、productKey 与 uplink topic"
done
grep -q 'change-me' .env bridge.env \
  && echo "  !! .env / bridge.env 里还留着 change-me 占位密码，上线前务必改掉" || true

# 上面刚生成的文件已经存在，重新读一遍（_common.sh 在循环之前 source 的）
if [ -f .env ]; then set -a; . ./.env; set +a; fi
if [ -f bridge.env ]; then set -a; . ./bridge.env; set +a; fi

say "2/6 修数据卷属主"
# 命名卷首次挂载时继承镜像内目录的属主；/var/lib/influxdb3 在镜像里不一定存在，
# 于是会被 root:root 创建，而 serve 以非 root 的 influxdb3（1500:1500）跑就会 permission denied。
$COMPOSE run --rm --no-deps --user root --entrypoint sh influxdb3 -c '
  d=/var/lib/influxdb3
  mkdir -p "$d"
  u="$(id -u influxdb3 2>/dev/null || echo 1500)"
  g="$(id -g influxdb3 2>/dev/null || echo 1500)"
  chown -R "$u:$g" "$d" 2>/dev/null || chmod 0777 "$d"
  ls -ld "$d"'

# mosquitto.conf 的 password_file 是单文件 bind mount；文件不存在时 Docker 会建成目录
touch mosquitto/passwd

say "3/6 建 MQTT 账号"
# 桥接器账号取自 bridge.env（真正拿它登录的是桥接器），测试设备账号取自 .env
: "${MQTT_USERNAME:?bridge.env 里缺 MQTT_USERNAME}"
: "${MQTT_PASSWORD:?bridge.env 里缺 MQTT_PASSWORD}"
$COMPOSE run --rm --no-deps --entrypoint mosquitto_passwd mosquitto \
  -c -b /mosquitto/config/passwd "$MQTT_USERNAME" "$MQTT_PASSWORD"
$COMPOSE run --rm --no-deps --entrypoint mosquitto_passwd mosquitto \
  -b /mosquitto/config/passwd "${DEVICE_MQTT_USER:-device1}" "${DEVICE_MQTT_PASS:-change-me-device}"
echo "  账号：$(cut -d: -f1 < mosquitto/passwd | tr '\n' ' ')"

say "4/6 建库"
DB="${DB_NAME:?缺少 DB_NAME}"
# create database 打的是 HTTP API（--host 默认 http://127.0.0.1:8181），服务必须在跑。
# 它没有 --object-store/--data-dir，也就是说没有「停服离线建库」这条路 —— 建不出来就是
# 服务没就绪或参数不对，重试 + 明确报错，不要拿离线兜底把问题盖住。
# 库名是位置参数（--database-name 不存在）。
$COMPOSE up -d --build influxdb3 mosquitto
created=""
last=""
for i in $(seq 1 60); do
  if [ -n "${RETENTION_PERIOD:-}" ]; then
    last="$($COMPOSE exec -T influxdb3 influxdb3 create database \
             "$DB" --retention-period "$RETENTION_PERIOD" 2>&1)" && { created=1; break; }
  else
    last="$($COMPOSE exec -T influxdb3 influxdb3 create database "$DB" 2>&1)" && { created=1; break; }
  fi
  echo "$last" | grep -qi 'already exist' && { created=already; break; }
  sleep 1          # 前几十次多半是服务还没监听，重试即等待就绪
done
[ -n "$created" ] || die "60s 没建成库 $DB。最后一次输出：
$last
排查顺序：
  1) $COMPOSE ps influxdb3 && $COMPOSE logs --tail=50 influxdb3   # 服务起没起、端口对不对
  2) curl -s http://127.0.0.1:\${INFLUXDB3_PORT:-8181}/health      # 容器外探活
  3) $COMPOSE exec influxdb3 influxdb3 create database --help     # 参数名以本机版本为准
  4) 启用了鉴权却没给 token：$COMPOSE exec influxdb3 env | grep -i INFLUXDB3_AUTH"
[ "$created" = already ] && echo "  库 $DB 已存在，跳过（retention 建库后改不了，要变就重建）" \
                          || echo "  库 $DB 就绪"

say "5/6 启动"
$COMPOSE up -d --build

say "6/6 自检"
$COMPOSE ps
bexec mqtt-influx-bridge check || echo "  !! 配置校验未通过，见上"

cat <<'NEXT'
下一步：
  bash deploy/scripts/verify-e2e.sh                 # 发测试消息 + 查库，验证全链路
  bash deploy/scripts/status.sh                     # 日常巡检：队列 / 表清单 / 告警
  bash deploy/scripts/dlq.sh                        # 看死信
  bash deploy/scripts/purge-table.sh mqtt_data_202512 # 清整张旧分表（默认只打印命令）
NEXT
