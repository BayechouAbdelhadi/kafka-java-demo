#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " 🚀 Démarrage Simple de l'Environnement Kafka & Services "
echo "=========================================================="

# 1. Démarrer Docker (Kafka, Schema Registry, PostgreSQL)
if docker info >/dev/null 2>&1; then
    docker compose up -d
else
    sudo docker compose up -d
fi

echo "⏳ Attente de Kafka et Schema Registry..."
sleep 5

# 2. Création automatique des topics indispensables
docker exec -i kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.events --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec -i kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic inventory.events --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec -i kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.events-dlt --partitions 3 --replication-factor 1 2>/dev/null || true

echo "✅ Infrastructure prête !"
echo ""
echo "Pour tester le scénario normal en une commande :"
echo "   ./test-normal.sh"
