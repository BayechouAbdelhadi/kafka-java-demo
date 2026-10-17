#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/../../.."
SCHEMA_REGISTRY_URL="${1:-http://localhost:8081}"

echo "=========================================================="
echo " Enregistrement Automatisé des Schémas Avro Initiaux "
echo "=========================================================="

# 1. Attente de Schema Registry
"${SCRIPT_DIR}/wait-for-schema-registry.sh" "${SCHEMA_REGISTRY_URL}" 30

register_schema() {
    local SUBJECT="$1"
    local AVSC_PATH="$2"
    local COMPATIBILITY="${3:-FORWARD_TRANSITIVE}"

    echo "📋 Enregistrement du sujet '$SUBJECT'..."
    
    # 1. Configuration de la compatibilité
    "${SCRIPT_DIR}/set-compatibility.sh" "$SCHEMA_REGISTRY_URL" "$SUBJECT" "$COMPATIBILITY"

    # 2. Préparation du payload JSON (échappement des guillemets dans le schéma Avro)
    local ESCAPED_SCHEMA
    ESCAPED_SCHEMA=$(jq -Rs . < "$AVSC_PATH")
    local PAYLOAD="{\"schema\": $ESCAPED_SCHEMA}"

    # 3. Enregistrement POST /subjects/<subject>/versions
    local RESPONSE
    RESPONSE=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "$PAYLOAD" \
        "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions")

    echo "   Résultat : $RESPONSE"
}

# Enregistrement des 3 contrats métier principaux
register_schema "orders.events-value" "${ROOT_DIR}/avro-schemas/src/main/avro/OrderEvent.avsc" "FORWARD_TRANSITIVE"
register_schema "inventory.events-value" "${ROOT_DIR}/avro-schemas/src/main/avro/InventoryResultEvent.avsc" "FORWARD_TRANSITIVE"
register_schema "notifications.events-value" "${ROOT_DIR}/avro-schemas/src/main/avro/NotificationEvent.avsc" "FORWARD_TRANSITIVE"

echo "=========================================================="
echo "✅ Tous les schémas ont été enregistrés avec succès."
echo "Liste des sujets enregistrés : $(curl -s ${SCHEMA_REGISTRY_URL}/subjects)"
echo "=========================================================="
