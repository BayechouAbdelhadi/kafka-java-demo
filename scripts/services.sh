#!/usr/bin/env bash
# Runs the three services as local JVMs against the compose infrastructure (`make infra`).
#   services.sh start   start the built jars in the background, wait until each is healthy
#   services.sh stop    stop them
# Logs and pid files go to logs/<service>.{log,pid}.
set -euo pipefail

cd "$(dirname "$0")/.."
SERVICES=(order-service:8080 inventory-service:8082 notification-service:8083)
LOGS=logs
TIMEOUT=${STARTUP_TIMEOUT:-120}

running() { [ -f "$LOGS/$1.pid" ] && kill -0 "$(cat "$LOGS/$1.pid")" 2>/dev/null; }

start() {
  mkdir -p "$LOGS"
  for entry in "${SERVICES[@]}"; do
    local name=${entry%%:*}
    if running "$name"; then
      echo "$name already running (pid $(cat "$LOGS/$name.pid"))"
      continue
    fi
    local jar
    jar=$(ls "$name"/target/"$name"-*.jar 2>/dev/null | head -1) || true
    [ -n "$jar" ] || { echo "$name: no jar in $name/target, run 'make build'" >&2; exit 1; }
    nohup java -jar "$jar" > "$LOGS/$name.log" 2>&1 &
    echo $! > "$LOGS/$name.pid"
    echo "started $name (pid $!)"
  done

  for entry in "${SERVICES[@]}"; do
    local name=${entry%%:*} port=${entry##*:}
    printf 'waiting for %s on :%s ' "$name" "$port"
    for ((i = 0; i < TIMEOUT; i++)); do
      if curl -fs "http://localhost:$port/actuator/health" > /dev/null; then echo UP; continue 2; fi
      if ! running "$name"; then break; fi
      sleep 1
      printf .
    done
    echo " FAILED, last log lines:" >&2
    tail -n 30 "$LOGS/$name.log" >&2
    exit 1
  done
}

stop() {
  for entry in "${SERVICES[@]}"; do
    local name=${entry%%:*}
    if running "$name"; then
      kill "$(cat "$LOGS/$name.pid")"
      echo "stopped $name"
    fi
    rm -f "$LOGS/$name.pid"
  done
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
