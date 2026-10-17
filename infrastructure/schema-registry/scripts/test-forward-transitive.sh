#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMAS_DIR="${SCRIPT_DIR}/../schemas"
SCHEMA_REGISTRY_URL="${1:-http://localhost:8081}"
SUBJECT="demo.forward-transitive-value"

echo "================================================================================"
echo " DÉMONSTRATION COMPLÈTE : ÉVOLUTION DE SCHÉMAS & FORWARD_TRANSITIVE "
echo "================================================================================"

"${SCRIPT_DIR}/wait-for-schema-registry.sh" "${SCHEMA_REGISTRY_URL}" 20

# Fonction utilitaire pour tester la compatibilité ou enregistrer
post_schema() {
    local ENDPOINT="$1"
    local AVSC_PATH="$2"
    local ESCAPED_SCHEMA
    ESCAPED_SCHEMA=$(jq -Rs . < "$AVSC_PATH")
    local PAYLOAD="{\"schema\": $ESCAPED_SCHEMA}"

    curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "$PAYLOAD" \
        "${SCHEMA_REGISTRY_URL}/${ENDPOINT}"
}

echo ""
echo "--- ÉTAPE 1 : Configuration du mode FORWARD_TRANSITIVE sur le sujet ---"
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "FORWARD_TRANSITIVE"}' \
    "${SCHEMA_REGISTRY_URL}/config/${SUBJECT}"
echo ""
echo "Compatibilité configurée : $(curl -s ${SCHEMA_REGISTRY_URL}/config/${SUBJECT})"

echo ""
echo "--- ÉTAPE 2 : Enregistrement de la Version 1 (V1 - Schéma Initial) ---"
RES_V1=$(post_schema "subjects/${SUBJECT}/versions" "${SCHEMAS_DIR}/v1-order-event.avsc")
BODY_V1=$(echo "$RES_V1" | head -n 1)
CODE_V1=$(echo "$RES_V1" | tail -n 1)
echo "HTTP Code: $CODE_V1 | Réponse: $BODY_V1"
if [ "$CODE_V1" -ne 200 ]; then
    echo "❌ Échec de l'enregistrement de V1 !"
    exit 1
fi
echo "✅ V1 enregistrée avec succès."

echo ""
echo "--- ÉTAPE 3 : Évolution Validée vers Version 2 (V2 - Ajout champ avec valeur par défaut) ---"
echo "Règle : En FORWARD, le nouveau lecteur (V2) peut lire les anciens messages (V1) car 'currency' a un défaut."
RES_V2=$(post_schema "subjects/${SUBJECT}/versions" "${SCHEMAS_DIR}/v2-order-event-valid.avsc")
BODY_V2=$(echo "$RES_V2" | head -n 1)
CODE_V2=$(echo "$RES_V2" | tail -n 1)
echo "HTTP Code: $CODE_V2 | Réponse: $BODY_V2"
if [ "$CODE_V2" -ne 200 ]; then
    echo "❌ Échec inattendu de V2 !"
    exit 1
fi
echo "✅ V2 acceptée et enregistrée avec succès."

echo ""
echo "--- ÉTAPE 4 : Tentative d'évolution INVALIDE sous FORWARD (Champ obligatoire sans défaut) ---"
echo "Test : Ajout de 'trackingNumber' SANS valeur par défaut."
echo "Raison du rejet : Le nouveau lecteur s'attend à 'trackingNumber', or les messages V1/V2 ne l'ont pas !"
RES_INV=$(post_schema "compatibility/subjects/${SUBJECT}/versions/latest" "${SCHEMAS_DIR}/invalid-forward-new-field-no-default.avsc")
BODY_INV=$(echo "$RES_INV" | head -n 1)
CODE_INV=$(echo "$RES_INV" | tail -n 1)
echo "Test compatibilité -> HTTP Code: $CODE_INV | Réponse: $BODY_INV"

# Tentative d'enregistrement effectif (doit être rejeté avec 409 Conflict)
RES_INV_REG=$(post_schema "subjects/${SUBJECT}/versions" "${SCHEMAS_DIR}/invalid-forward-new-field-no-default.avsc")
BODY_INV_REG=$(echo "$RES_INV_REG" | head -n 1)
CODE_INV_REG=$(echo "$RES_INV_REG" | tail -n 1)
echo "Tentative d'enregistrement -> HTTP Code: $CODE_INV_REG | Réponse: $BODY_INV_REG"
if [ "$CODE_INV_REG" -eq 409 ]; then
    echo "✅ [SUCCÈS DE LA VALIDATION] Schema Registry a bien REJETÉ le schéma avec 409 Conflict !"
else
    echo "⚠️  Comportement inattendu sur le schéma invalide (reçu code $CODE_INV_REG)"
fi

echo ""
echo "--- ÉTAPE 5 : Cas spécifique TRANSITIF (Valide vs V2, mais Invalide vs V1) ---"
echo "Test : Changement de type incompatible de totalAmount avec l'historique V1."
RES_TRANS=$(post_schema "subjects/${SUBJECT}/versions" "${SCHEMAS_DIR}/invalid-transitive-type-change.avsc")
BODY_TRANS=$(echo "$RES_TRANS" | head -n 1)
CODE_TRANS=$(echo "$RES_TRANS" | tail -n 1)
echo "Tentative d'enregistrement -> HTTP Code: $CODE_TRANS | Réponse: $BODY_TRANS"
if [ "$CODE_TRANS" -eq 409 ]; then
    echo "✅ [SUCCÈS DE LA VALIDATION TRANSITIVE] Schema Registry a bien vérifié TOUTES les versions passées (V1 et V2) et a rejeté la rupture avec V1 !"
else
    echo "ℹ️ Code obtenu : $CODE_TRANS"
fi

echo ""
echo "--- ÉTAPE 6 : Évolution Validée vers Version 3 (V3 - Multiple versions transitives valides) ---"
RES_V3=$(post_schema "subjects/${SUBJECT}/versions" "${SCHEMAS_DIR}/v3-order-event-valid.avsc")
BODY_V3=$(echo "$RES_V3" | head -n 1)
CODE_V3=$(echo "$RES_V3" | tail -n 1)
echo "HTTP Code: $CODE_V3 | Réponse: $BODY_V3"
if [ "$CODE_V3" -eq 200 ]; then
    echo "✅ V3 enregistrée avec succès. FORWARD_TRANSITIVE est validé sur l'ensemble de l'historique V1, V2, V3 !"
fi

echo ""
echo "================================================================================"
echo " BILAN DES VERSIONS ENREGISTRÉES DANS SCHEMA REGISTRY : "
curl -s "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions"
echo ""
echo "================================================================================"
