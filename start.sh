#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " 🚀 Démarrage du Cluster Kafka 4 Brokers (KRaft) "
echo "=========================================================="

DOCKER_CMD="docker"
if ! docker info >/dev/null 2>&1; then
    DOCKER_CMD="sudo docker"
fi

echo "🧹 Nettoyage et démarrage des 4 brokers, Schema Registry et PostgreSQL..."
$DOCKER_CMD compose down --remove-orphans 2>/dev/null || true
$DOCKER_CMD compose up -d

echo "⏳ Attente de la stabilisation du quorum KRaft (10 secondes)..."
sleep 10

# Création des topics avec Facteur de Réplication 3 (Multi-Brokers Réel)
echo "📦 Création des topics avec réplication x3..."
$DOCKER_CMD exec -i kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic orders.events --partitions 3 --replication-factor 3 2>/dev/null || true

$DOCKER_CMD exec -i kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic inventory.events --partitions 3 --replication-factor 3 2>/dev/null || true

$DOCKER_CMD exec -i kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic orders.events-dlt --partitions 3 --replication-factor 3 2>/dev/null || true

echo "✅ Cluster 4 Brokers opérationnel avec réplication x3 !"
echo ""
echo "Pour tester le scénario normal :"
echo "   ./test-normal.sh"
