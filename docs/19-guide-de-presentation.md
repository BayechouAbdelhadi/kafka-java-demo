# 19 - Guide du Formateur & Déroulé de Démonstration en Direct

[⬅️ Précédent : Exploitation & Runbook](file:///home/abdelhadi/Documents/kafka-java/docs/18-exploitation-et-runbook.md) | [Sommaire Principal 🏠](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md)

---

## 🎯 Objectif du Formateur

Ce guide vous donne le script exact, minute par minute, pour animer une formation percutante auprès d'un public de développeurs Java/Spring Boot.
**Règle d'or :** Vous n'avez pas besoin de coder en direct. Le projet est intégralement fonctionnel. Vous expliquez les concepts en ouvrant le code existant et en lançant des commandes concrètes.

---

## ⏱️ Déroulé Pédagogique (Timing Recommandé : 2h à 3h)

### Séquence 1 : Introduction & Modèle Mental (20 min)
1. **Ouvrir [docs/02-modele-mental-kafka.md](file:///home/abdelhadi/Documents/kafka-java/docs/02-modele-mental-kafka.md)**.
2. **Question à poser à la salle** : *"Quelle est la différence fondamentale entre RabbitMQ et Kafka lorsqu'un consommateur a lu un message ?"*
3. **Point clé à expliquer** : Le journal append-only immuable, la rétention temporelle et le rejeu.

### Séquence 2 : Infrastructure & Création des Topics (25 min)
1. **Ouvrir [infrastructure/kafka/topics/orders.events.properties](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/orders.events.properties)**.
2. Montrer pourquoi `auto.create.topics=false` est la seule politique saine en production.
3. **Exécuter la création et la validation des topics** :
   ```bash
   ./infrastructure/kafka/scripts/create-topics.sh localhost:9092
   ./infrastructure/kafka/scripts/validate-topics.sh localhost:9092
   ```
4. Montrer que la réexécution est **idempotente** et ne supprime aucune donnée existante.

### Séquence 3 : Transactional Outbox Pattern (30 min)
1. **Ouvrir [OrderService.java](file:///home/abdelhadi/Documents/kafka-java/order-service/src/main/java/com/example/kafka/order/service/OrderService.java)**.
2. Montrer la méthode `createOrder` avec l'annotation `@Transactional` :
   - L'entité `Order` ET l'entité `OutboxMessage` sont écrites dans la **même transaction PostgreSQL**.
3. **Lancer le scénario de commande nominal** :
   ```bash
   ./scripts/demo-e2e.sh
   ```
4. Pointer du doigt les logs du Relay qui dépile l'Outbox et publie sur Kafka de manière asynchrone.

### Séquence 4 : Schema Registry & Évolution FORWARD_TRANSITIVE (35 min)
1. **Ouvrir [docs/10-evolution-des-schemas-et-forward-transitive.md](file:///home/abdelhadi/Documents/kafka-java/docs/10-evolution-des-schemas-et-forward-transitive.md)**.
2. **Question à poser** : *"Si j'ajoute un nouveau champ obligatoire sans valeur par défaut dans mon schéma Avro, qui va planter et pourquoi ?"*
3. **Lancer la démonstration automatisée** :
   ```bash
   ./infrastructure/schema-registry/scripts/test-forward-transitive.sh
   ```
4. Faire constater au public l'erreur `409 Conflict` renvoyée en direct par Schema Registry lorsqu'un schéma incompatible est proposé !

### Séquence 5 : Résilience : Retries & Dead Letter Topic (30 min)
1. **Ouvrir [NotificationConsumer.java](file:///home/abdelhadi/Documents/kafka-java/notification-service/src/main/java/com/example/kafka/notification/consumer/NotificationConsumer.java)**.
2. Montrer l'annotation `@RetryableTopic` et l'exclusion des erreurs permanentes.
3. **Lancer la démonstration de pannes** :
   ```bash
   ./scripts/demo-retry-dlt.sh
   ```
4. Observer dans les logs de `notification-service` :
   - Le message corrompu qui atterrit directement en DLT sans retry inutile.
   - Le message instable qui réessaie 2 fois avec backoff et réussit à la 3ᵉ tentative !
