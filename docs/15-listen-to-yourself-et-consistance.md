# 15 - Le Pattern "Listen to Yourself" et la Consistance Éventuelle

[⬅️ Précédent : Transactional Outbox](file:///home/abdelhadi/Documents/kafka-java/docs/14-transactional-outbox.md) | [Suivant : Observabilité & Diagnostic ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/16-observabilite-et-diagnostic.md)

---

## 1. Principe du Pattern "Listen to Yourself"

Dans une architecture événementielle pure, certains services n'écrivent pas directement leur état de lecture final lors de la requête HTTP entrante. Au lieu de cela :
1. La requête HTTP dépose une intention (*Command*).
2. L'événement est publié dans Kafka.
3. Le service lui-même consomme cet événement pour mettre à jour sa propre vue de lecture ou déclencher une suite d'actions.

Dans notre projet, `order-service` écoute le topic de résultat [`inventory.events`](file:///home/abdelhadi/Documents/kafka-java/order-service/src/main/java/com/example/kafka/order/consumer/InventoryResultConsumer.java) pour basculer la commande de l'état `CREATED` vers `CONFIRMED` ou `REJECTED` (Saga chorégraphiée).

---

## 2. Consistance Éventuelle (*Eventual Consistency*)

Entre le moment où le client reçoit la réponse HTTP `201 Created` et le moment où l'inventaire a réservé le stock :
- Il s'écoule quelques millisecondes ou secondes.
- Si le client interroge immédiatement `GET /api/orders/{id}`, le statut peut encore afficher `CREATED`.
- **Réflexe de conception** : Les interfaces utilisateur modernes doivent afficher des états intermédiaires (*"Commande en cours de validation..."*) plutôt que de supposer une consistance immédiate bloquante.
