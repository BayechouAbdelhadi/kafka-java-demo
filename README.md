# Plateforme de Formation Apache Kafka & Spring Boot

Projet complet, robuste, testé et exécutable pour la formation technique de développeurs Java & Spring Boot aux architectures orientées événements avec **Apache Kafka**.

---

## 🏗️ Architecture et Composants

Le projet implémente un système distribué de commandes e-commerce :

* **`order-service`** (Port 8081) : API REST de commande, base PostgreSQL, implémentation du pattern **Transactional Outbox** et publication des événements Avro.
* **`inventory-service`** (Port 8082) : Consommateur **idempotent** avec table `processed_events`, réservation de stock et publication du résultat.
* **`notification-service`** (Port 8083) : Consommateur avec stratégie de **Non-Blocking Retries** et **Dead Letter Topic (DLT)**.
* **`avro-schemas`** : Définition des contrats `.avsc` et génération Maven des classes Java fortement typées.
* **`infrastructure/`** : Fichiers déclaratifs des topics Kafka, scripts d'initialisation idempotente et Confluent Schema Registry configuré en `FORWARD_TRANSITIVE`.

---

## 🚀 Démarrage Rapide

### Prérequis
* **Docker & Docker Compose**
* **Java 21+** (compilation OpenJDK 25 avec bytecode release 21)
* **Maven 3.9+** (inclus automatiquement dans votre environnement utilisateur)

### 1. Démarrer l'infrastructure complète
```bash
./run.sh
```
Ce script vérifie la disponibilité de Docker, démarre Kafka, PostgreSQL et Schema Registry, attend que le cluster soit prêt, crée et valide les topics déclaratifs, puis lance l'application.

---

## 🧪 Démonstrations en Ligne de Commande

### Démo 1 : Parcours Nominal de Commande (E2E)
```bash
./scripts/demo-e2e.sh
```

### Démo 2 : Retries Non-Bloquants & Dead Letter Topic
```bash
./scripts/demo-retry-dlt.sh
```

### Démo 3 : Évolution des Schémas Avro sous `FORWARD_TRANSITIVE`
```bash
./infrastructure/schema-registry/scripts/test-forward-transitive.sh
```

### Vérification de la Conformité des Topics Kafka
```bash
./infrastructure/kafka/scripts/validate-topics.sh localhost:9092
```

---

## 📚 Documentation Technique Complète

Tous les concepts, schémas Mermaid, justifications d'architecture et runbooks sont documentés dans le dossier [`docs/`](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md) :

1. [00 - Sommaire Général](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md)
2. [01 - Objectifs & Architecture](file:///home/abdelhadi/Documents/kafka-java/docs/01-objectifs-et-architecture.md)
3. [02 - Modèle Mental Kafka vs Queues](file:///home/abdelhadi/Documents/kafka-java/docs/02-modele-mental-kafka.md)
4. [03 - Brokers KRaft & Configuration](file:///home/abdelhadi/Documents/kafka-java/docs/03-brokers-kraft-et-configuration.md)
5. [04 - Topics, Partitions & Scripts Reproductibles](file:///home/abdelhadi/Documents/kafka-java/docs/04-topics-partitions-et-replication.md)
6. [05 - Clés de Partitionnement & Ordre](file:///home/abdelhadi/Documents/kafka-java/docs/05-cles-et-ordre-des-evenements.md)
7. [06 - Producteurs & Fiabilité](file:///home/abdelhadi/Documents/kafka-java/docs/06-producers-et-fiabilite.md)
8. [07 - Consommateurs, Offsets & Groups](file:///home/abdelhadi/Documents/kafka-java/docs/07-consumers-offsets-et-groups.md)
9. [08 - Rebalancing & Performance](file:///home/abdelhadi/Documents/kafka-java/docs/08-rebalancing-et-performance.md)
10. [09 - Schema Registry & Sérialisation Avro](file:///home/abdelhadi/Documents/kafka-java/docs/09-schema-registry-et-avro.md)
11. [10 - Évolution des Schémas & FORWARD_TRANSITIVE](file:///home/abdelhadi/Documents/kafka-java/docs/10-evolution-des-schemas-et-forward-transitive.md)
12. [11 - Retries Non-Bloquants & Dead Letter Topics](file:///home/abdelhadi/Documents/kafka-java/docs/11-retries-et-dead-letter-topics.md)
13. [12 - Idempotence & Déduplication Métier](file:///home/abdelhadi/Documents/kafka-java/docs/12-idempotence-et-deduplication.md)
14. [13 - Transactions & Mythe de l'Exactly-Once](file:///home/abdelhadi/Documents/kafka-java/docs/13-transactions-et-exactly-once.md)
15. [14 - Le Pattern Transactional Outbox](file:///home/abdelhadi/Documents/kafka-java/docs/14-transactional-outbox.md)
16. [15 - Listen to Yourself & Consistance Éventuelle](file:///home/abdelhadi/Documents/kafka-java/docs/15-listen-to-yourself-et-consistance.md)
17. [16 - Observabilité & Métriques](file:///home/abdelhadi/Documents/kafka-java/docs/16-observabilite-et-diagnostic.md)
18. [17 - Tests & Scénarios de Panne](file:///home/abdelhadi/Documents/kafka-java/docs/17-tests-et-scenarios-de-panne.md)
19. [18 - Exploitation & Runbook](file:///home/abdelhadi/Documents/kafka-java/docs/18-exploitation-et-runbook.md)
20. [19 - Guide du Formateur & Déroulé de Démo Live](file:///home/abdelhadi/Documents/kafka-java/docs/19-guide-de-presentation.md)
# kafka-java-demo
