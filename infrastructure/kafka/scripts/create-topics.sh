#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPICS_DIR="${SCRIPT_DIR}/../topics"
BOOTSTRAP_SERVERS="${1:-localhost:9092}"

echo "=========================================================="
echo " Création Reproductible et Idempotente des Topics Kafka "
echo "=========================================================="

# 1. Attente de la disponibilité effective
"${SCRIPT_DIR}/wait-for-kafka.sh" "${BOOTSTRAP_SERVERS}" 30

# 2. Détection de l'exécutable kafka-topics
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

echo "📦 Récupération de la liste des topics existants sur le cluster..."
EXISTING_TOPICS=$($KAFKA_CLI --bootstrap-server "${BOOTSTRAP_SERVERS}" --list 2>/dev/null || true)

CREATED_COUNT=0
EXISTING_COUNT=0
ERROR_COUNT=0

for PROP_FILE in "${TOPICS_DIR}"/*.properties; do
    [ -f "$PROP_FILE" ] || continue
    
    # Lecture des propriétés
    TOPIC_NAME=$(grep -E '^topic\.name=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    PARTITIONS=$(grep -E '^partitions=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    REPLICATION=$(grep -E '^replication\.factor=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    CLEANUP=$(grep -E '^cleanup\.policy=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    RETENTION=$(grep -E '^retention\.ms=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')
    MIN_ISR=$(grep -E '^min\.insync\.replicas=' "$PROP_FILE" | cut -d= -f2 | tr -d '\r ')

    if [ -z "$TOPIC_NAME" ]; then
        echo "⚠️  [WARN] Fichier $PROP_FILE sans topic.name valide, ignoré."
        continue
    fi

    # Vérification d'existence préalable (idempotence)
    if echo "$EXISTING_TOPICS" | grep -qw "$TOPIC_NAME"; then
        echo "ℹ️  [EXISTE DÉJÀ] Topic '$TOPIC_NAME' déjà présent. Données préservées."
        EXISTING_COUNT=$((EXISTING_COUNT + 1))
    else
        echo "🚀 [CRÉATION] Topic '$TOPIC_NAME' (Partitions: $PARTITIONS, Réplication: $REPLICATION)..."
        
        CONFIG_ARGS=()
        [ -n "$CLEANUP" ] && CONFIG_ARGS+=(--config "cleanup.policy=$CLEANUP")
        [ -n "$RETENTION" ] && CONFIG_ARGS+=(--config "retention.ms=$RETENTION")
        [ -n "$MIN_ISR" ] && CONFIG_ARGS+=(--config "min.insync.replicas=$MIN_ISR")

        if $KAFKA_CLI --bootstrap-server "${BOOTSTRAP_SERVERS}" \
            --create --if-not-exists \
            --topic "$TOPIC_NAME" \
            --partitions "$PARTITIONS" \
            --replication-factor "$REPLICATION" \
            "${CONFIG_ARGS[@]}"; then
            echo "   ✅ Topic '$TOPIC_NAME' créé avec succès."
            CREATED_COUNT=$((CREATED_COUNT + 1))
        else
            echo "   ❌ [ERROR] Échec de création du topic '$TOPIC_NAME'."
            ERROR_COUNT=$((ERROR_COUNT + 1))
        fi
    fi
done

echo "----------------------------------------------------------"
echo "Bilan : $CREATED_COUNT créé(s), $EXISTING_COUNT existant(s), $ERROR_COUNT erreur(s)."
echo "----------------------------------------------------------"

if [ $ERROR_COUNT -gt 0 ]; then
    exit 1
fi
exit 0
