# deploy/scripts 公用片段：被各脚本 source，不要直接执行。
# 约定调用方已 cd 到 deploy/ 且开启 set -euo pipefail。

COMPOSE="${COMPOSE:-docker compose}"

# .env 给 compose 与 broker/DB 参数，bridge.env 给桥接器；导出后两边共用同一份值
if [ -f .env ]; then set -a; . ./.env; set +a; fi
if [ -f bridge.env ]; then set -a; . ./bridge.env; set +a; fi

DB="${DB_NAME:-mqtt}"

# docker compose exec 不会重放服务的 env_file，exec 出来的进程拿不到 MQTT/INFLUX 凭据。
# bridge.env 已只读挂进容器 /bridge.env，先 source 再 exec，保证与 1 号进程同环境。
bexec() {
  $COMPOSE exec -T bridge sh -c 'set -a; [ -f /bridge.env ] && . /bridge.env; set +a; exec "$@"' sh "$@"
}

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# influxdb3 的 query / create database / delete table 打的是 HTTP API
# （--host 默认 http://127.0.0.1:8181），所以在 influxdb3 容器里 exec，回环就是服务本身。
# SQL 是位置参数：没有 --sql，也没有 --precision。
iq() { # iq <SQL> [influxdb3 query 的其它参数...]
  if [ -n "${INFLUX_ADMIN_TOKEN:-}" ]; then
    $COMPOSE exec -T influxdb3 influxdb3 query --database "$DB" \
      --token "$INFLUX_ADMIN_TOKEN" "$1" "${@:2}"
  else
    # --without-auth 时不用给 token；启用了鉴权，容器里已有 INFLUXDB3_AUTH_TOKEN
    # （见 docker-compose.auth.yml），CLI 自己会读，这里也不拼 --token。
    $COMPOSE exec -T influxdb3 influxdb3 query --database "$DB" "$1" "${@:2}"
  fi
}
