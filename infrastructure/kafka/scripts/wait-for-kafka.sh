#!/usr/bin/env bash
set -e

# Script d'attente active de la disponibilité effective de Kafka
# Référence : Section 5.2 & 5.4 du cahier des charges

BOOTSTRAP_SERVERS="${1:-localhost:9092}"
MAX_RETRIES="${2:-45}"
RETRY_INTERVAL=2

echo "⏳ [wait-for-kafka] Vérification de l'accès au cluster Kafka sur ${BOOTSTRAP_SERVERS}..."

# 1. Vérification réseau TCP bas niveau
RETRY=0
while [ $RETRY -lt $MAX_RETRIES ]; do
    HOST=$(echo "$BOOTSTRAP_SERVERS" | cut -d: -f1)
    PORT=$(echo "$BOOTSTRAP_SERVERS" | cut -d: -f2)
    
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
        echo "✅ [wait-for-kafka] Port TCP ${PORT} accessible sur ${HOST}."
        break
    fi
    
    RETRY=$((RETRY + 1))
    echo "   Attente du port TCP... ($RETRY/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done

if [ $RETRY -ge $MAX_RETRIES ]; then
    echo "❌ [ERROR] Impossible de joindre le port TCP de Kafka sur ${BOOTSTRAP_SERVERS}."
    exit 1
fi

# 2. Vérification protocolaire Kafka (API Metadata fonctionnelle)
echo "🔍 [wait-for-kafka] Vérification du protocole Kafka via kafka-topics..."

KAFKA_CLI=""
if command -v kafka-topics.sh &> /dev/null; then
    KAFKA_CLI="kafka-topics.sh"
elif command -v kafka-topics &> /dev/null; then
    KAFKA_CLI="kafka-topics"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "kafka-1"; then
    KAFKA_CLI="docker exec -i kafka-1 kafka-topics"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "kafka"; then
    KAFKA_CLI="docker exec -i kafka kafka-topics"
fi

if [ -n "$KAFKA_CLI" ]; then
    RETRY=0
    while [ $RETRY -lt 15 ]; do
        if $KAFKA_CLI --bootstrap-server "${BOOTSTRAP_SERVERS}" --list >/dev/null 2>&1; then
            echo "✅ [wait-for-kafka] Broker Kafka pleinement opérationnel et prêt à traiter les requêtes !"
            exit 0
        fi
        RETRY=$((RETRY + 1))
        echo "   Attente du quorum KRaft et de la disponibilité des métadonnées... ($RETRY/15)"
        sleep 2
    done
    echo "❌ [ERROR] Le broker Kafka ne répond pas aux requêtes de métadonnées."
    exit 1
else
    echo "ℹ️  [wait-for-kafka] Outil CLI kafka-topics non détecté localement; port TCP vérifié."
fi

exit 0
