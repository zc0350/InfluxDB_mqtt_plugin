#!/usr/bin/env bash
# 看/处理死信队列（DLQ）。容器里没有 sqlite3 CLI，所以借桥接器镜像里的 python + 公共 API。
#   dlq.sh                列出最近 20 条（带 id）
#   dlq.sh requeue 12     放回 pending 重试 —— 先 stop bridge，改好配置再回来，否则会重新进 DLQ
#   dlq.sh drop 12        确认放弃这条
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_common.sh
action="${1:-list}"; id="${2:-}"

if [ "$action" = requeue ]; then
  echo "提示：$COMPOSE stop bridge → 修配置 → 本脚本 → $COMPOSE up -d bridge"
fi

bexec python - <<PY
import os
from mqtt_influx_bridge.config import load_config
from mqtt_influx_bridge.wal import Wal

cfg, _ = load_config(os.environ.get("BRIDGE_CONFIG", "/config/config.toml"))
wal = Wal(cfg.wal.path)
action, target = "$action", "$id"

if action == "list":
    rows = wal.dlq_rows(20)
    if not rows:
        print("DLQ 为空")
    for dlq_id, line, topic, reason in rows:
        print(f"[{dlq_id}] topic={topic or '-'}  reason={reason}")
        print(f"       line={line[:400]}\n")
elif action in ("requeue", "drop"):
    if not target.isdigit():
        raise SystemExit(f"需要数字 id，例如 dlq.sh {action} 12")
    fn = wal.requeue_dlq if action == "requeue" else wal.drop_dlq
    print("已处理" if fn(int(target)) else f"dlq id={target} 不存在")
    print(wal.stats())
else:
    raise SystemExit(f"未知动作: {action}")
wal.close()
PY
