#!/usr/bin/env bash
# End-to-end scenarios against running services (`make up` or `make infra` + services). Needs curl and jq.
set -euo pipefail

ORDERS=${ORDERS_URL:-http://localhost:8080/api/orders}
INVENTORY=${INVENTORY_URL:-http://localhost:8082/api/inventory}

# order <customerId> <productId> <quantity>  -> prints the order id
order() {
  curl -fsS -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"$1\",\"items\":[{\"productId\":\"$2\",\"quantity\":$3,\"unitPrice\":10.0}]}" \
    "$ORDERS" | jq -r .id
}

# await_status <orderId> <expectedStatus>
await_status() {
  local status
  for _ in $(seq 1 30); do
    status=$(curl -fsS "$ORDERS/$1" | jq -r .status)
    if [ "$status" = "$2" ]; then echo "   order $1 -> $status"; return; fi
    sleep 0.5
  done
  echo "   order $1 is $status, expected $2" >&2
  exit 1
}

echo "Stock PROD-1 = 5"
curl -fsS -X POST "$INVENTORY/PROD-1/init?quantity=5" > /dev/null

echo "1. Happy path: outbox -> orders.events -> inventory reserves -> inventory.events -> CONFIRMED"
await_status "$(order alice PROD-1 2)" CONFIRMED

echo "2. Insufficient stock -> REJECTED, stock untouched"
await_status "$(order bob PROD-1 10)" REJECTED
echo "   $(curl -fsS "$INVENTORY/PROD-1" | jq -c .)"

echo "3. Notification transient failure: retried via orders.events-notification-retry, then succeeds"
order flaky_carol PROD-1 1 > /dev/null

echo "4. Notification permanent failure: straight to orders.events-notification-dlt"
order invalid_dave PROD-1 1 > /dev/null

echo "Follow 3 and 4 with 'make logs' (notification-service) or in Kafka UI: http://localhost:8090"
