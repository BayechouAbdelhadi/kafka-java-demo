#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================================="
echo " Plateforme de Formation Kafka - Démarrage Automatisé "
echo "=========================================================="

# 1. Vérification et lancement de Docker Compose
DOCKER_CMD="docker"
if ! docker info >/dev/null 2>&1; then
    if sudo -n docker info >/dev/null 2>&1; then
        DOCKER_CMD="sudo docker"
    elif sudo docker info >/dev/null 2>&1; then
        DOCKER_CMD="sudo docker"
    else
        echo "❌ [ERROR] Impossible de joindre le démon Docker."
        exit 1
    fi
fi

echo "🐳 Vérification des conteneurs de l'infrastructure..."
RUNNING_COUNT=$($DOCKER_CMD compose ps --status running -q 2>/dev/null | wc -l)
if [ "$RUNNING_COUNT" -eq 0 ]; then
    echo "▶️  Démarrage de Kafka (KRaft), Schema Registry et PostgreSQL..."
    $DOCKER_CMD compose up -d
else
    echo "✅ Les conteneurs de l'infrastructure sont déjà actifs."
fi

# 2. Attente de la disponibilité effective de Kafka
"${SCRIPT_DIR}/infrastructure/kafka/scripts/wait-for-kafka.sh" localhost:9092 30

# 3. Création reproductible et idempotente des topics déclaratifs
echo ""
"${SCRIPT_DIR}/infrastructure/kafka/scripts/create-topics.sh" localhost:9092

# 4. Validation stricte de la conformité des topics
echo ""
"${SCRIPT_DIR}/infrastructure/kafka/scripts/validate-topics.sh" localhost:9092

# 5. Enregistrement des schémas Avro dans Schema Registry
echo ""
"${SCRIPT_DIR}/infrastructure/schema-registry/scripts/register-schemas.sh" http://localhost:8081

# 6. Démarrage des microservices Spring Boot
echo ""
echo "=========================================================="
echo " 🚀 Lancement des Microservices Spring Boot "
echo "=========================================================="
echo "Services disponibles :"
echo " - order-service        : http://localhost:8081/api/orders"
echo " - inventory-service    : http://localhost:8082/api/inventory"
echo " - notification-service : http://localhost:8083/actuator/health"
echo "----------------------------------------------------------"
echo "Pour lancer manuellement les services en arrière-plan :"
echo "   mvn spring-boot:run -pl order-service &"
echo "   mvn spring-boot:run -pl inventory-service &"
echo "   mvn spring-boot:run -pl notification-service &"
echo "=========================================================="
