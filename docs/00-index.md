# Plateforme de Formation Technique Apache Kafka & Spring Boot

Bienvenue sur la documentation pédagogique de référence de la formation **Apache Kafka & Spring Boot**.
Ce projet fournit une implémentation complète, exécutable, testée et reproductible d'une architecture orientée événements (*Event-Driven Architecture*).

---

## 🗺️ Sommaire de la Formation

| N° | Document | Thématique Principale |
|---|---|---|
| **01** | [Objectifs & Architecture](file:///home/abdelhadi/Documents/kafka-java/docs/01-objectifs-et-architecture.md) | Cas métier e-commerce, composants distribués, flux d'événements. |
| **02** | [Modèle Mental Kafka](file:///home/abdelhadi/Documents/kafka-java/docs/02-modele-mental-kafka.md) | Log distribué append-only vs message queues traditionnelles (RabbitMQ/JMS). |
| **03** | [Brokers KRaft & Configuration](file:///home/abdelhadi/Documents/kafka-java/docs/03-brokers-kraft-et-configuration.md) | Quorum KRaft sans ZooKeeper, listeners, réplication, `min.insync.replicas`. |
| **04** | [Topics, Partitions & Réplication](file:///home/abdelhadi/Documents/kafka-java/docs/04-topics-partitions-et-replication.md) | Scripts reproductibles `create-topics.sh` & `validate-topics.sh`. |
| **05** | [Clés & Ordre des Événements](file:///home/abdelhadi/Documents/kafka-java/docs/05-cles-et-ordre-des-evenements.md) | Stratégie de hashing, préservation d'ordre par commande, hot partitions. |
| **06** | [Producteurs & Fiabilité](file:///home/abdelhadi/Documents/kafka-java/docs/06-producers-et-fiabilite.md) | Idempotence producer, `acks=all`, `delivery.timeout.ms`, batching & compression. |
| **07** | [Consommateurs, Offsets & Groups](file:///home/abdelhadi/Documents/kafka-java/docs/07-consumers-offsets-et-groups.md) | Consumer groups, commits d'offsets manuels vs automatiques, crash recovery. |
| **08** | [Rebalancing & Performance](file:///home/abdelhadi/Documents/kafka-java/docs/08-rebalancing-et-performance.md) | Protocoles de rééquilibrage, partition assignment, tuning du throughput/latence. |
| **09** | [Schema Registry & Avro](file:///home/abdelhadi/Documents/kafka-java/docs/09-schema-registry-et-avro.md) | Contrats d'interface `.avsc`, magic byte Confluent, génération Maven. |
| **10** | [Évolution des Schémas & FORWARD_TRANSITIVE](file:///home/abdelhadi/Documents/kafka-java/docs/10-evolution-des-schemas-et-forward-transitive.md) | Démonstration V1, V2, V3, évolutions acceptées et rejets 409 Conflict. |
| **11** | [Retries & Dead Letter Topics (DLT)](file:///home/abdelhadi/Documents/kafka-java/docs/11-retries-et-dead-letter-topics.md) | Erreurs transitoires vs permanentes, backoff non-bloquant, routing DLT. |
| **12** | [Idempotence & Déduplication Métier](file:///home/abdelhadi/Documents/kafka-java/docs/12-idempotence-et-deduplication.md) | Table `processed_events`, déduplication dans la transaction locale PostgreSQL. |
| **13** | [Transactions & Exactly-Once Semantics](file:///home/abdelhadi/Documents/kafka-java/docs/13-transactions-et-exactly-once.md) | Démystification de l'EOS : limites entre Kafka et le monde extérieur (Bases/APIs). |
| **14** | [Transactional Outbox Pattern](file:///home/abdelhadi/Documents/kafka-java/docs/14-transactional-outbox.md) | Atomicité DB/Kafka, relay applicatif vs CDC Debezium, at-least-once. |
| **15** | [Listen to Yourself & Consistance](file:///home/abdelhadi/Documents/kafka-java/docs/15-listen-to-yourself-et-consistance.md) | Pattern CQRS / autoliste, projection locale et consistance à terme. |
| **16** | [Observabilité & Diagnostic](file:///home/abdelhadi/Documents/kafka-java/docs/16-observabilite-et-diagnostic.md) | Spring Boot Actuator, métriques Micrometer, lag de consommation. |
| **17** | [Tests & Scénarios de Panne](file:///home/abdelhadi/Documents/kafka-java/docs/17-tests-et-scenarios-de-panne.md) | Tests d'intégration automatisés, tests d'évolution et résilience. |
| **18** | [Exploitation & Runbook](file:///home/abdelhadi/Documents/kafka-java/docs/18-exploitation-et-runbook.md) | Commandes quotidiennes CLI, purge sécurisée `reset-local-environment.sh`. |
| **19** | [Guide du Formateur & Déroulé de Démo](file:///home/abdelhadi/Documents/kafka-java/docs/19-guide-de-presentation.md) | Script pas-à-pas de présentation en live pour captiver l'audience. |

---

## ⚡ Démarrage Rapide

1. **Vérifier les prérequis** : Docker et Java 21+ installés.
2. **Lancer l'infrastructure et l'application** :
   ```bash
   ./run.sh
   ```
3. **Exécuter la démonstration nominale** :
   ```bash
   ./scripts/demo-e2e.sh
   ```
4. **Exécuter la démonstration de résilience (Retry & DLT)** :
   ```bash
   ./scripts/demo-retry-dlt.sh
   ```
5. **Tester l'évolution des schémas (`FORWARD_TRANSITIVE`)** :
   ```bash
   ./infrastructure/schema-registry/scripts/test-forward-transitive.sh
   ```
