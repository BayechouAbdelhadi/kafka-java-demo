#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPICS_DIR="${SCRIPT_DIR}/../topics"
BOOTSTRAP_SERVERS="${1:-localhost:9092}"

echo "=========================================================="
echo " Validation de Conformité des Topics Kafka "
echo "=========================================================="

KAFKA_CLI=""
CONFIGS_CLI=""
if command -v kafka-topics.sh &> /dev/null; then
    KAFKA_CLI="kafka-topics.sh"
    CONFIGS_CLI="kafka-configs.sh"
elif command -v kafka-topics &> /dev/null; then
    KAFKA_CLI="kafka-topics"
    CONFIGS_CLI="kafka-configs"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "kafka-1"; then
    KAFKA_CLI="docker exec -i kafka-1 kafka-topics"
    CONFIGS_CLI="docker exec -i kafka-1 kafka-configs"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "kafka"; then
    KAFKA_CLI="docker exec -i kafka kafka-topics"
    CONFIGS_CLI="docker exec -i kafka kafka-configs"
else
    echo "❌ [ERROR] Aucun outil kafka-topics (local ou conteneur Docker) n'a été trouvé."
    exit 1
fi

NON_COMPLIANT_COUNT=0
TOTAL_CHECKED=0

for PROP_FILE in "${TOPICS_DIR}"/*.properties; do
    [ -f "$PROP_FILE" ] || continue
    
    TOPIC_NAME=$(grep -E '^topic\.name=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    EXPECTED_PARTITIONS=$(grep -E '^partitions=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    EXPECTED_REPLICATION=$(grep -E '^replication\.factor=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    EXPECTED_CLEANUP=$(grep -E '^cleanup\.policy=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    EXPECTED_RETENTION=$(grep -E '^retention\.ms=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    EXPECTED_MIN_ISR=$(grep -E '^min\.insync\.replicas=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')

    [ -z "$TOPIC_NAME" ] && continue
    TOTAL_CHECKED=$((TOTAL_CHECKED + 1))

    echo "🔎 Contrôle du topic '$TOPIC_NAME'..."

    # 1. Vérification existence et description
    DESCRIBE_OUTPUT=$($KAFKA_CLI --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "$TOPIC_NAME" 2>/dev/null || true)
    if [ -z "$DESCRIBE_OUTPUT" ]; then
        echo "   ❌ [ÉCART] Le topic '$TOPIC_NAME' N'EXISTE PAS sur le cluster !"
        NON_COMPLIANT_COUNT=$((NON_COMPLIANT_COUNT + 1))
        continue
    fi

    # 2. Vérification Partitions
    ACTUAL_PARTITIONS=$(echo "$DESCRIBE_OUTPUT" | head -n 1 | grep -o 'PartitionCount: [0-9]*' | awk '{print $2}')
    if [ "$ACTUAL_PARTITIONS" != "$EXPECTED_PARTITIONS" ]; then
        echo "   ❌ [ÉCART Partitions] Attendu: $EXPECTED_PARTITIONS | Effectif sur cluster: $ACTUAL_PARTITIONS"
        NON_COMPLIANT_COUNT=$((NON_COMPLIANT_COUNT + 1))
    else
        echo "   ✅ Partitions: $ACTUAL_PARTITIONS (conforme)"
    fi

    # 3. Vérification Replication Factor
    ACTUAL_REPLICATION=$(echo "$DESCRIBE_OUTPUT" | head -n 1 | grep -o 'ReplicationFactor: [0-9]*' | awk '{print $2}')
    if [ "$ACTUAL_REPLICATION" != "$EXPECTED_REPLICATION" ]; then
        echo "   ❌ [ÉCART Réplication] Attendu: $EXPECTED_REPLICATION | Effectif sur cluster: $ACTUAL_REPLICATION"
        NON_COMPLIANT_COUNT=$((NON_COMPLIANT_COUNT + 1))
    else
        echo "   ✅ Facteur de réplication: $ACTUAL_REPLICATION (conforme)"
    fi

    # 4. Vérification Configurations Spécifiques (Configs)
    ACTUAL_CONFIGS=$($CONFIGS_CLI --bootstrap-server "${BOOTSTRAP_SERVERS}" --entity-type topics --entity-name "$TOPIC_NAME" --describe 2>/dev/null || true)

    if [ -n "$EXPECTED_CLEANUP" ]; then
        if ! echo "$ACTUAL_CONFIGS" | grep -q "cleanup.policy=$EXPECTED_CLEANUP"; then
            echo "   ⚠️  [ÉCART cleanup.policy] Attendu: $EXPECTED_CLEANUP"
        fi
    fi

    if [ -n "$EXPECTED_RETENTION" ]; then
        if ! echo "$ACTUAL_CONFIGS" | grep -q "retention.ms=$EXPECTED_RETENTION"; then
            echo "   ⚠️  [ÉCART retention.ms] Attendu: $EXPECTED_RETENTION"
        fi
    fi
done

echo "=========================================================="
if [ $NON_COMPLIANT_COUNT -gt 0 ]; then
    echo "❌ VALIDATION ÉCHOUÉE : $NON_COMPLIANT_COUNT écart(s) détecté(s) sur $TOTAL_CHECKED topic(s) analysé(s)."
    exit 1
else
    echo "✅ VALIDATION RÉUSSIE : Tous les topics ($TOTAL_CHECKED) sont conformes aux spécifications déclaratives."
    exit 0
fi
