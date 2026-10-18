#!/usr/bin/env bash
set -e

BOOTSTRAP_SERVERS="${1:-localhost:9092}"

KAFKA_CLI=""
if command -v kafka-topics.sh &> /dev/null; then
    KAFKA_CLI="kafka-topics.sh"
elif command -v kafka-topics &> /dev/null; then
    KAFKA_CLI="kafka-topics"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^kafka$"; then
    KAFKA_CLI="docker exec -i kafka /opt/kafka/bin/kafka-topics.sh"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "kafka-1"; then
    KAFKA_CLI="docker exec -i kafka-1 /opt/kafka/bin/kafka-topics.sh"
elif sudo -n docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^kafka$"; then
    KAFKA_CLI="sudo docker exec -i kafka /opt/kafka/bin/kafka-topics.sh"
else
    echo "❌ [ERROR] Aucun outil kafka-topics (local ou conteneur Docker) n'a été trouvé."
    exit 1
fi

echo "=========================================================="
echo " Description Détaillée des Topics Kafka sur ${BOOTSTRAP_SERVERS} "
echo "=========================================================="

$KAFKA_CLI --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe
