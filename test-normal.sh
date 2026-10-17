#!/usr/bin/env bash
set -e

ORDER_URL="http://localhost:8081/api/orders"
INV_URL="http://localhost:8082/api/inventory"

echo "=========================================================="
echo " 🛒 Test du Scénario Normal (Happy Path) "
echo "=========================================================="

echo "1. Initialisation du stock pour le produit 'MACBOOK-PRO' (10 pièces)..."
curl -s -X POST "${INV_URL}/MACBOOK-PRO/init?quantity=10" | jq . || true
echo ""

echo "2. Envoi d'une commande (Achat de 1 MACBOOK-PRO)..."
ORDER_RES=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"customerId": "client_alice", "items": [{"productId": "MACBOOK-PRO", "quantity": 1, "unitPrice": 1999.99}]}' \
  "$ORDER_URL")

echo "$ORDER_RES" | jq .
ORDER_ID=$(echo "$ORDER_RES" | jq -r .id)
echo "Commande créée : $ORDER_ID (Statut initial : CREATED)"

echo ""
echo "⏳ Kafka transporte l'événement, l'inventaire réserve le stock..."
sleep 2

echo ""
echo "3. Résultat final :"
echo "👉 État de la commande (doit être CONFIRMED) :"
curl -s "${ORDER_URL}/${ORDER_ID}" | jq .

echo ""
echo "👉 État du stock (doit être 9 disponibles, 1 réservé) :"
curl -s "${INV_URL}/MACBOOK-PRO" | jq .

echo "=========================================================="
echo "🎉 Scénario normal terminé avec succès !"
echo "=========================================================="
