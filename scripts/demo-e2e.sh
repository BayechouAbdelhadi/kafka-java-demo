#!/usr/bin/env bash
set -e

# Scénario Démo 1 : Parcours Nominal de bout en bout (E2E)
# Commande HTTP -> Outbox PostgreSQL -> Kafka orders.events -> Inventory Service -> Notification Service

ORDER_API="${1:-http://localhost:8081/api/orders}"
INVENTORY_API="${2:-http://localhost:8082/api/inventory}"

echo "=========================================================="
echo " 🛒 DÉMONSTRATION 1 : Parcours Nominal de Commande (E2E) "
echo "=========================================================="

echo "1. Initialisation du stock produit 'PROD-42' à 50 unités..."
curl -s -X POST "${INVENTORY_API}/PROD-42/init?quantity=50" | jq . || true
echo ""

echo "2. Création d'une commande via POST /api/orders (Client: JohnDoe)..."
ORDER_PAYLOAD='{
  "customerId": "cust_john_doe",
  "items": [
    { "productId": "PROD-42", "quantity": 2, "unitPrice": 49.99 }
  ]
}'

ORDER_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -d "$ORDER_PAYLOAD" "$ORDER_API")
echo "$ORDER_RESPONSE" | jq .
ORDER_ID=$(echo "$ORDER_RESPONSE" | jq -r .id)
echo "✅ Commande créée avec ID : $ORDER_ID (Statut initial: CREATED)"

echo ""
echo "3. Vérification immédiate de la table Outbox (statut avant/après relay)..."
curl -s "${ORDER_API}/outbox/pending" | jq . || true

echo ""
echo "⏳ Attente du relais Outbox, de la réservation de stock et de la notification (3 secondes)..."
sleep 3

echo ""
echo "4. Vérification du statut final de la commande (doit être CONFIRMED)..."
curl -s "${ORDER_API}/${ORDER_ID}" | jq .

echo ""
echo "5. Vérification du stock restant pour PROD-42 (doit être 48 disponible, 2 réservé)..."
curl -s "${INVENTORY_API}/PROD-42" | jq .

echo "=========================================================="
echo "✅ Démonstration E2E terminée avec succès !"
echo "=========================================================="
