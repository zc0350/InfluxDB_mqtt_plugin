#!/usr/bin/env bash
# 端到端验证：发 3 类真实 payload（上行 / 下行命令 / 设备回执），再查库确认落表。
# 只依赖 docker compose，不需要宿主机装 mosquitto 客户端或 influx 客户端。
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/_common.sh

NOW="$(date +%s)"
# 分表名要和桥接器的 year_tz_offset_minutes=480 + table_period=month 对齐
PERIOD="$(TZ=Asia/Shanghai date +%Y%m)"
# 用桥接器账号发消息：它一定在 mosquitto/passwd 里（bootstrap 建的），顺带验证这套凭据可用
USER_="${MQTT_USERNAME:?bridge.env 里缺 MQTT_USERNAME}"
PASS_="${MQTT_PASSWORD:?bridge.env 里缺 MQTT_PASSWORD}"
DEV="${DEVICE_MQTT_USER:-E2E0001}"
PK="${PRODUCT_KEY:-TT1000}"

fail=0
step() { say "$@"; }
bad()  { printf '\033[1;31m  FAIL %s\033[0m\n' "$*"; fail=1; }
good() { printf '  ok   %s\n' "$*"; }

pub() { # pub <topic> <payload>
  $COMPOSE exec -T mosquitto mosquitto_pub -h 127.0.0.1 -p 1883 \
    -u "$USER_" -P "$PASS_" -q 1 -t "$1" -m "$2"
}

q() { # q <sql> [influxdb3 query 其它参数...]
  iq "$@"
}

step "0. 配置自检 + 映射干跑（不写库，先确认表名/列名符合预期）"
bexec mqtt-influx-bridge check >/dev/null && good "check 通过" || bad "check 失败"
bexec mqtt-influx-bridge sample \
  --topic "$PK/$DEV/data" \
  --payload "{\"event\":\"status\",\"deviceCode\":\"$DEV\",\"time\":$NOW}" || bad "sample 失败"

step "1. 发测试消息"
pub "$PK/$DEV/data" "{\"event\":\"status\",\"deviceCode\":\"$DEV\",\"imei\":\"864925080140265\",\"sigal\":25,\"door_status\":0,\"server_id\":1,\"version\":\"mini_esp32_127\",\"cam\":\"ready\",\"sd\":\"ready\",\"time\":$NOW}"
pub "$PK/$DEV/data" "{\"event\":\"alarms\",\"deviceCode\":\"$DEV\",\"door_num\":[1],\"code\":1,\"time\":$((NOW+1))}"
pub "$PK/$DEV/control" "{\"action\":\"reboot\",\"deviceCode\":\"$DEV\",\"time\":$NOW,\"uuid\":\"aabbbcc\"}"
pub "$PK/$DEV/response/aabbbcc" "{\"action\":\"reboot\",\"deviceCode\":\"$DEV\",\"code\":1,\"time\":$NOW,\"uuid\":\"aabbbcc\"}"
good "已发 4 条（QoS 1）"

step "2. 等 writer 刷库（最多 30s）"
# 只判定「有没有非零数字出现」，是冒烟检查不是精确计数；精确值看第 3 步的 GROUP BY
for i in $(seq 1 30); do
  if q "SELECT count(*) AS n FROM mqtt_data_$PERIOD WHERE event='status'" | grep -q '[1-9]'; then
    rows=ok; break
  fi
  rows=0; sleep 1
done
[ "${rows:-0}" = ok ] && good "mqtt_data_$PERIOD 已有 status 行" || bad "mqtt_data_$PERIOD 查不到数据"

step "3. 校验三类落表"
q "SELECT event, devicecode, count(*) AS n FROM mqtt_data_$PERIOD GROUP BY 1,2" || bad "上行表查询失败"
q "SELECT direction, event, count(*) AS n FROM device_control_$PERIOD GROUP BY 1,2" \
  || bad "控制表查询失败（检查 ${PK}/+/{control,response/+} 是否被订阅）"
q "SELECT topic, count(*) AS n FROM mqtt_unknown_$PERIOD GROUP BY 1 ORDER BY n DESC LIMIT 10" \
  || echo "  --   兜底表没有数据（说明四类 topic 都被前三条路由接住了，正常）"

dlq_of() {
  bexec mqtt-influx-bridge status | grep -o '"dlq": *[0-9]*' | grep -o '[0-9]*'
}

step "4. 队列与 DLQ（此刻应当干净）"
before=$(dlq_of)
bexec mqtt-influx-bridge status
if [ "${before:-?}" = 0 ]; then
  good "DLQ 为空"
else
  bad "DLQ 已有 $before 条：bash deploy/scripts/dlq.sh 看原因（多半是 uplink topic 猜错，消息掉进兜底路由之外的映射失败）"
fi

step "5. 类型冲突隔离：同名列 door_num 先字符串后数值"
# mode=explicit 下没声明的 key 不成列，所以拿声明过的 door_num 造冲突：
# 第 1 步的 alarms 把它写成字符串 "[1]"（库内 string），这里发 door_num:1（int64）就是类型冲突，
# 两条消息赶同一个批次 -> 整批 400 -> 二分隔离，期望好行落库、坏行进 DLQ。
pub "$PK/$DEV/data" "{\"event\":\"probe\",\"deviceCode\":\"$DEV\",\"code\":7,\"time\":$((NOW+2))}"
pub "$PK/$DEV/data" "{\"event\":\"probe\",\"deviceCode\":\"$DEV\",\"door_num\":1,\"time\":$((NOW+3))}"
sleep 3
after=$(dlq_of)
bexec mqtt-influx-bridge status
if [ "${after:-0}" -gt "${before:-0}" ]; then
  good "坏行被单独隔离进 DLQ（$before → $after），好行没被整批拖走"
  echo "     看一眼：bash deploy/scripts/dlq.sh ；修好后：dlq.sh drop <id>"
else
  bad "DLQ 没增加：可能 $PERIOD 这张表里 door_num 还没被写成字符串（第 1 步的 alarms 没落库？），冲突没发生"
fi
q "SELECT count(*) AS n FROM mqtt_data_$PERIOD WHERE event='probe' AND code=7" || bad "隔离后好行查不到"

printf '\n'
[ $fail -eq 0 ] && echo "全部通过" || echo "有失败项，见上面 FAIL"
exit $fail
