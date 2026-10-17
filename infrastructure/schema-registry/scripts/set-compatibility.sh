#!/usr/bin/env bash
set -e

# Script de configuration explicite du mode de compatibilité d'un sujet Schema Registry
# Référence : Section 8.3 & 8.4 du cahier des charges

SCHEMA_REGISTRY_URL="${1:-http://localhost:8081}"
SUBJECT="${2:-orders.events-value}"
COMPATIBILITY="${3:-FORWARD_TRANSITIVE}"

echo "⚙️  Configuration du mode de compatibilité pour '$SUBJECT' -> '$COMPATIBILITY'..."

# Appel API Confluent Schema Registry PUT /config/<subject>
RESPONSE=$(curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"compatibility\": \"${COMPATIBILITY}\"}" \
    "${SCHEMA_REGISTRY_URL}/config/${SUBJECT}")

echo "   Réponse : $RESPONSE"

# Vérification du mode effectif
EFFECTIVE=$(curl -s "${SCHEMA_REGISTRY_URL}/config/${SUBJECT}")
echo "✅ Mode de compatibilité effectif pour '$SUBJECT' : $EFFECTIVE"
