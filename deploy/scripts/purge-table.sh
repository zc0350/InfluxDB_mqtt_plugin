#!/usr/bin/env bash
# 清理一整张时间分表（按年是 _2023，按月是 _202512）。默认只打印将要做什么，加 --execute 才动手。
#   bash purge-table.sh mqtt_data_202512                  # 干跑
#   bash purge-table.sh mqtt_data_202512 --execute        # v3 软删除（改名，磁盘等 compaction）
#   bash purge-table.sh mqtt_data_202512 --execute --hard # 删且立刻物理回收磁盘
# 表清单：python -m mqtt_influx_bridge tables
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_common.sh

table="${1:?用法: purge-table.sh <表名> [--execute] [--hard]}"
shift
args=()
for a in "$@"; do
  case "$a" in
    --execute|--hard) args+=("$a") ;;
    *)                echo "未知参数: $a"; exit 1 ;;
  esac
done

# 删表走的是后端自己的管理接口，各家不同：v3 DELETE /api/v3/configure/table、v1 DROP
# MEASUREMENT、v2 根本没有按 measurement 删的接口（桥接器会直接拒绝并说明替代做法）。
# 所以这里交给桥接器的 purge-table 子命令去做，而不是拼 influxdb3 CLI —— 后者在不同小版本
# 里参数名漂移过（--table-name / 位置参数 / 是否要确认），要以容器里的 --help 为准。
# 想要硬删且桥接器拒绝，说明桥接器的 token 没有管理权限：把 bridge.env 的 INFLUX_TOKEN
# 换成 admin token，或手动进容器执行（先 influxdb3 delete table --help 核对参数名）。
echo "==> 通过桥接器删除 $DB.$table"
bexec mqtt-influx-bridge purge-table --table "$table" ${args[@]+"${args[@]}"} || {
  [ ${#args[@]} -eq 0 ] && echo "（上面只是干跑；确认无误后加 --execute）"
  exit 1
}
[ ${#args[@]} -eq 0 ] && exit 0

echo "==> 剩余表"
bexec mqtt-influx-bridge tables || true
echo "==> 桥接器队列"
bexec mqtt-influx-bridge status || true
