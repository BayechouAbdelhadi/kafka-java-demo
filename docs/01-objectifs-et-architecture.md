# 01 - Objectifs et Architecture Globale

[⬅️ Retour au Sommaire](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md) | [Suivant : Modèle Mental Kafka ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/02-modele-mental-kafka.md)

---

## 1. Objectifs de la Formation

L'objectif de cette plateforme est de transformer la théorie acquise lors des formations d'introduction à Apache Kafka en **réflexes opérationnels de production**.
Dans les architectures d'entreprise réelles, Kafka n'est pas un simple bus de messages passif : il impose une conception rigoureuse de la **sérialisation**, de l'**idempotence**, du **séquencement** et de la **résilience aux pannes**.

---

## 2. Cas d'Usage Métier : E-Commerce Distribué

Le cas pratique implémente le cycle de vie d'une commande d'achat en ligne à travers trois microservices Spring Boot autonomes et découplés :

```mermaid
flowchart TD
    Client[Client HTTP / Front-end] -->|POST /api/orders| OrderService[order-service :8081]
    
    subgraph OrderServiceScope [order-service]
        OrderDB[(PostgreSQL\ntables: orders, outbox)]
        OrderService -->|Transaction Locale Atomique| OrderDB
        OutboxRelay[Outbox Relay Scheduler] -->|Lit messages PENDING| OrderDB
    end

    OutboxRelay -->|Publie OrderEvent Avro\nkey = orderId| TopicOrders[Topic: orders.events]

    subgraph InventoryScope [inventory-service :8082]
        InvConsumer[OrderEventConsumer]
        InvDB[(PostgreSQL\ntables: stocks, processed_events)]
        InvConsumer -->|Déduplication & Réservation| InvDB
    end

    TopicOrders -->|Consomme orders.events| InvConsumer
    InvConsumer -->|Publie InventoryResultEvent\nkey = orderId| TopicInventory[Topic: inventory.events]
    TopicInventory -->|Saga Result Callback| OrderService

    subgraph NotificationScope [notification-service :8083]
        NotifConsumer[NotificationConsumer\n@RetryableTopic]
    end

    TopicOrders -->|Consomme orders.events| NotifConsumer
    NotifConsumer -.->|Erreur transitoire 3 essais| RetryTopic[Topic: orders.events-retry]
    NotifConsumer -.->|Erreur permanente ou épuisée| DltTopic[Topic: orders.events-dlt]
```

---

## 3. Découpage des Microservices

| Service | Port | Rôle & Responsabilité | Base de Données |
|---|---|---|---|
| **`order-service`** | `8081` | Point d'entrée HTTP. Enregistre la commande et l'événement Outbox dans la même transaction PostgreSQL. Son relay publie vers Kafka. | `orders`, `outbox_messages` |
| **`inventory-service`** | `8082` | Consomme `orders.events` de manière idempotente (table `processed_events`), décrémente le stock et émet le résultat sur `inventory.events`. | `stocks`, `processed_events` |
| **`notification-service`** | `8083` | Consomme `orders.events` avec stratégie de non-blocking retries et Dead Letter Topic pour simuler l'envoi de SMS/emails. | Aucune (stateless) |

---

## 4. Justifications Architecturales Clés

1. **Pourquoi le pattern Transactional Outbox plutôt qu'un `KafkaTemplate.send()` direct dans le contrôleur ?**
   - Si la publication Kafka échoue après le `commit` en base de données, la commande existe mais aucun événement n'est émis (incohérence fantôme).
   - Si la publication réussit mais que la base de données crash avant le commit, un événement est envoyé pour une commande qui n'existe pas.
   - L'Outbox garantit l'atomicité locale : la commande et l'intention de publication sont validées dans le même journal de transactions PostgreSQL (WAL).

2. **Pourquoi Avro + Schema Registry plutôt que du JSON brut ?**
   - Typage fort, validation stricte à la sérialisation, compacité binaire (pas de transport répété des noms de champs à chaque message).
   - Évolution de schéma gouvernée (`FORWARD_TRANSITIVE`) évitant les crashs silencieux de désérialisation chez les consommateurs.
