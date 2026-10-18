# Kafka & Spring Boot — architecture orientée événements

Système de commandes e-commerce en trois microservices qui communiquent uniquement par Kafka (Avro + Schema Registry).

```
             POST /api/orders
                    │
            ┌───────▼────────┐  orders.events   ┌───────────────────┐
            │ order-service  │ ───────────────► │ inventory-service │
            │ outbox + saga  │ ◄─────────────── │ idempotent + saga │
            └────────────────┘ inventory.events └───────────────────┘
                    │ orders.events
            ┌───────▼──────────────┐
            │ notification-service │ ─► orders.events-notification-retry ─► …-dlt
            └──────────────────────┘
```

## Patterns et où les trouver

| Pattern | Implémentation |
|---|---|
| Transactional Outbox | `common/outbox` : `OutboxWriter` (même transaction que le métier), `OutboxRelay` (publie puis supprime, `FOR UPDATE SKIP LOCKED`) |
| Saga chorégraphiée | `OrderService.applyInventoryResult` ← `inventory.events` ← `OrderEventConsumer` |
| Idempotent Consumer | table `processed_events` (inventory, notification) ; machine à états `CREATED → CONFIRMED/REJECTED` (order) |
| Retries bloquants + DLT | `common/kafka/KafkaErrorHandlingConfig` : 3 essais puis `<topic>-dlt`, poison pills incluses |
| Retries non bloquants + DLT | `NotificationConsumer` (`@RetryableTopic`) |
| Clé de partitionnement | `orderId` : ordre garanti par commande |
| Contrats Avro | `avro-schemas/src/main/avro`, compatibilité `FORWARD_TRANSITIVE` |
| Database per service | `order_db`, `inventory_db`, `notification_db`, migrations Flyway |

## Démarrage

Prérequis : Docker (utilisateur dans le groupe `docker`), Java 21, Maven 3.9, `make` ; `curl` et `jq` pour la démo.

```bash
make up      # infrastructure + services en conteneurs
make demo    # scénarios de bout en bout
make logs    # logs des services
make down    # tout arrêter (aucun volume : état réinitialisé)
```

Pour développer dans l'IDE : `make infra`, puis lancer `OrderApplication`, `InventoryApplication` et `NotificationApplication` (ou `mvn -pl order-service spring-boot:run`). Les tests (`make test`) n'ont pas besoin de Docker : Kafka embarqué, H2 et Schema Registry `mock://`.

| Service | URL |
|---|---|
| order-service | http://localhost:8080/api/orders (`/outbox/pending` pour l'outbox) |
| inventory-service | http://localhost:8082/api/inventory (`POST /{productId}/init?quantity=N`) |
| notification-service | http://localhost:8083/actuator/health |
| Kafka UI | http://localhost:8090 |
| Schema Registry | http://localhost:8081 |
| Kafka (hôte) | localhost:9092, 9094, 9095, 9096 |
| PostgreSQL | localhost:5432 (postgres / postgres) |

## Configuration

- **Infrastructure** : `docker-compose.yml` uniquement. Cluster KRaft de 4 nœuds (3 broker+controller, 1 broker), RF 3, `min.insync.replicas=2`, auto-création des topics désactivée. Tous les topics sont créés par le conteneur `kafka-init`.
- **Services** : les réglages communs (Kafka, Avro, datasource, JPA, Flyway) sont dans `common/src/main/resources/common-defaults.yml`. Chaque `application.yml` ne définit que le nom, le port, la base et `app.outbox.enabled`.
- **Variables d'environnement** (valeurs par défaut adaptées à l'hôte) : `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
- **Schéma SQL** : migrations Flyway (`db/common/V1` partagée, `db/migration/V2+` par service) ; Hibernate en `validate`.
- **Nouveau topic** : constante dans `KafkaTopics` + ajout à la liste de `kafka-init` (et son `-dlt` s'il est consommé).
