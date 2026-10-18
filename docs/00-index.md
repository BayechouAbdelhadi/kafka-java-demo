# 📚 Fiche Mémo Kafka & Spring Boot (Essentiel)

Ce guide résume l'essentiel à comprendre et à retenir pour concevoir une architecture avec **Apache Kafka** et **Spring Boot**, sans jargon inutile.

---

## 🗺️ Les 5 Fiches Clés

| Fiche | Sujet | Ce qu'on y apprend |
|---|---|---|
| **[01. Le Scénario Métier](file:///home/abdelhadi/Documents/kafka-java/docs/01-architecture-et-scenario.md)** | Comment ça marche | Le parcours d'une commande de l'API jusqu'au stock. |
| **[02. Les Configurations](file:///home/abdelhadi/Documents/kafka-java/docs/02-configurations-cles.md)** | Les bons réglages | Les 5 options à connaître pour le Broker, les Topics, le Producer et le Consumer. |
| **[03. Pattern Outbox & Déduplication](file:///home/abdelhadi/Documents/kafka-java/docs/03-patterns-outbox-et-idempotence.md)** | Fiabilité des données | Comment ne jamais perdre d'événement et ne jamais facturer deux fois. |
| **[04. Pannes & Dead Letter Queue](file:///home/abdelhadi/Documents/kafka-java/docs/04-resilience-et-dlq.md)** | Que faire quand ça plante | Différence entre erreur temporaire (retry) et définitive (DLQ). |
| **[05. Schema Registry & Avro](file:///home/abdelhadi/Documents/kafka-java/docs/05-schema-registry-et-avro.md)** | Évolution des contrats | Pourquoi `FORWARD_TRANSITIVE` et le piège du champ sans valeur par défaut. |

---

## ⚡ En Pratique : Deux Commandes

```bash
# 1. Démarrer Kafka et créer les topics
./start.sh

# 2. Exécuter le test du scénario normal
./test-normal.sh
```
