#!/usr/bin/env bash
set -e

SCHEMA_REGISTRY_URL="${1:-http://localhost:8081}"
MAX_RETRIES="${2:-40}"
RETRY_INTERVAL=2

echo "⏳ [wait-for-sr] Vérification de l'accès à Confluent Schema Registry sur ${SCHEMA_REGISTRY_URL}..."

RETRY=0
while [ $RETRY -lt $MAX_RETRIES ]; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${SCHEMA_REGISTRY_URL}/subjects" 2>/dev/null || true)
    if [ "$HTTP_CODE" = "200" ]; then
        echo "✅ [wait-for-sr] Schema Registry est opérationnel (HTTP 200 sur /subjects) !"
        exit 0
    fi
    RETRY=$((RETRY + 1))
    echo "   Attente de Schema Registry (code reçu: ${HTTP_CODE:-none})... ($RETRY/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done

echo "❌ [ERROR] Impossible de joindre Schema Registry sur ${SCHEMA_REGISTRY_URL} après $MAX_RETRIES tentatives."
exit 1
