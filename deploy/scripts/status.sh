#!/usr/bin/env bash
# 日常巡检：容器状态 + 本地队列 + 库表清单 + 最近告警。
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_common.sh

echo "== 容器 ==";      $COMPOSE ps
echo; echo "== 本地队列（pending / 最老积压 / 重试次数 / DLQ）=="
bexec mqtt-influx-bridge status || true
echo; echo "== 表清单 =="
iq "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY 1" || echo "  !! 查询失败：确认库名与鉴权方式（Core 3.x 用 information_schema，不带下划线）"
echo; echo "== 桥接器最近告警 =="
$COMPOSE logs --tail=200 --since=30m bridge 2>/dev/null | grep -Ei 'error|warn|dlq|指标|stats' | tail -20 || echo "  （无）"
