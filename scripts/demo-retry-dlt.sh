#!/usr/bin/env bash
set -e

ORDER_API="${1:-http://localhost:8081/api/orders}"

echo "=========================================================="
echo " 🔁 DÉMONSTRATION 2 : Retries & Dead Letter Topic (DLT) "
echo "=========================================================="

echo "--- CAS A : Erreur Permanente (Envoi direct en DLT sans bloquer les autres messages) ---"
echo "Création d'une commande avec un client corrompu ('invalid_corrupt_123')..."
PERM_PAYLOAD='{
  "customerId": "invalid_corrupt_123",
  "items": [
    { "productId": "PROD-99", "quantity": 1, "unitPrice": 10.0 }
  ]
}'
curl -s -X POST -H "Content-Type: application/json" -d "$PERM_PAYLOAD" "$ORDER_API" | jq .
echo "👉 Consultez les logs de notification-service : l'exception PermanentNotificationException"
echo "   déroute immédiatement le message vers 'orders.events-dlt' !"

echo ""
echo "--- CAS B : Erreur Transitoire avec Backoff Exponentiel & Rétablissement ---"
echo "Création d'une commande avec un client instable ('flaky_customer_456')..."
FLAKY_PAYLOAD='{
  "customerId": "flaky_customer_456",
  "items": [
    { "productId": "PROD-99", "quantity": 1, "unitPrice": 10.0 }
  ]
}'
curl -s -X POST -H "Content-Type: application/json" -d "$FLAKY_PAYLOAD" "$ORDER_API" | jq .
echo "👉 Consultez les logs de notification-service : échecs aux tentatives 1 et 2, puis SUCCÈS à la tentative 3 !"

echo "=========================================================="
