#!/usr/bin/env bash
# 全栈快照：停写 → 把三个数据卷 tar 到 backup/<时间戳>/ → 拉起。
# 覆盖 InfluxDB 3 的 catalog+object store、桥接器 WAL、mosquitto 持久会话与账号文件。
#  restore：把对应 tar 解到同名卷（脚本末尾有命令），再 bootstrap 一次即可。
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="${COMPOSE:-docker compose}"

out="backup/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$out"
# 备份里有明文口令（bridge.env / mosquitto/passwd），只给本机可读
chmod 700 "$out"

echo "==> 停写（顺序：桥接器 → broker → DB）"
$COMPOSE stop bridge mosquitto influxdb3

for pair in "influxdb3:/var/lib/influxdb3" "bridge:/data" "mosquitto:/mosquitto/data"; do
  svc="${pair%%:*}"
  path="${pair#*:}"
  echo "==> $svc  $path"
  # -T 关掉 TTY：否则 tar 的二进制流会被终端换行转换破坏
  $COMPOSE run -T --rm --no-deps --user root --entrypoint sh "$svc" \
    -c "tar czf - -C '$path' ." > "$out/$svc.tar.gz" \
    || echo "  !! $svc 没备成（目录不存在或镜像无 tar）"
done

echo "==> 配置与凭据"
for f in .env bridge.env config/config.toml mosquitto/mosquitto.conf mosquitto/passwd; do
  [ -f "$f" ] && install -m 600 "$f" "$out/$(echo "$f" | tr '/' '_')"
done

echo "==> 拉起"
$COMPOSE up -d

echo
ls -lh "$out"
echo "队列现在应该在被清空：bash deploy/scripts/status.sh"
cat <<'RESTORE'
恢复（以 influxdb3 卷为例）：
  docker compose stop
  docker run --rm -v <卷名>:/d -v "$PWD/backup/<时间戳>":/b alpine \
    sh -c 'rm -rf /d/* && tar xzf /b/influxdb3.tar.gz -C /d'
  docker compose up -d
卷名用 docker volume ls 找（前缀是 compose 项目名，不是目录名）。
RESTORE
