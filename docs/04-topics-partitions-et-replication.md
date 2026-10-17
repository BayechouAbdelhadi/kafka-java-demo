# 04 - Topics, Partitions et Scripts Reproductibles

[⬅️ Précédent : Brokers KRaft](file:///home/abdelhadi/Documents/kafka-java/docs/03-brokers-kraft-et-configuration.md) | [Suivant : Clés & Ordre des Événements ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/05-cles-et-ordre-des-evenements.md)

---

## 1. Philosophie : Aucune Création Manuelle de Topic

En production, `auto.create.topics.enable=false` doit impérativement être activé. Laisser les producteurs créer les topics à la volée conduit à des désastres opérationnels (topics avec 1 seule partition, réplication par défaut non maîtrisée, fautes de frappe dans les noms).

Tous les topics de notre projet sont définis sous forme déclarative dans :
[`infrastructure/kafka/topics/`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/)

| Fichier Déclaratif | Topic Kafka | Partitions | Rétention | Rôle Métier |
|---|---|---|---|---|
| [`orders.events.properties`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/orders.events.properties) | `orders.events` | 3 | 7 jours | Événements du cycle de vie des commandes. |
| [`inventory.events.properties`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/inventory.events.properties) | `inventory.events` | 3 | 7 jours | Résultats de réservation de stock. |
| [`notifications.events.properties`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/notifications.events.properties) | `notifications.events` | 3 | 3 jours | Demandes de diffusion de SMS/Emails. |
| [`orders.retry.properties`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/orders.retry.properties) | `orders.retry` | 3 | 24 heures | File d'attente pour retries non-bloquants. |
| [`orders.dlt.properties`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/topics/orders.dlt.properties) | `orders.dlt` | 3 | 30 jours | Dead Letter Topic pour erreurs irrécupérables. |

---

## 2. Le Script de Création Idempotente : `create-topics.sh`

Le script [`infrastructure/kafka/scripts/create-topics.sh`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/scripts/create-topics.sh) :
1. Attend activement la disponibilité effective du quorum KRaft via `wait-for-kafka.sh`.
2. Inspecte la liste des topics existants sur le cluster.
3. Si le topic existe déjà, il **préserve scrupuleusement les données** et affiche un message d'information.
4. Si le topic est absent, il le crée avec ses paramètres précis (`--partitions`, `--replication-factor`, `--config cleanup.policy=...`).

Exécution :
```bash
./infrastructure/kafka/scripts/create-topics.sh localhost:9092
```

---

## 3. Le Script de Validation de Conformité : `validate-topics.sh`

Le script [`infrastructure/kafka/scripts/validate-topics.sh`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/kafka/scripts/validate-topics.sh) ne se contente pas de vérifier l'existence :
- Il interroge Kafka via `kafka-topics.sh --describe` et `kafka-configs.sh --describe`.
- Il compare les propriétés **réelles** appliquées sur le cluster avec les propriétés **attendues** dans les fichiers `.properties`.
- Si un écart est détecté (ex: quelqu'un a créé le topic avec 1 partition au lieu de 3), il affiche l'anomalie et retourne un code de sortie non-nul (`exit 1`).

Exécution :
```bash
./infrastructure/kafka/scripts/validate-topics.sh localhost:9092
```

---

## 4. Attention : Pourquoi augmenter les partitions casse l'ordre des clés

La fonction de hachage par défaut de Kafka (`murmur2`) détermine la partition selon la formule :
$$\text{Partition} = \text{murmur2}(\text{key}) \pmod{\text{Nombre de Partitions}}$$

Si un topic passe de 3 à 5 partitions :
- $\text{murmur2}("order\_123") \pmod 3 = 1$ (Partition 1)
- $\text{murmur2}("order\_123") \pmod 5 = 4$ (Partition 4)
- **Conséquence** : les nouveaux messages de la commande `order_123` atterrissent sur la Partition 4 alors que les anciens sont sur la Partition 1. Deux consommateurs différents vont lire ces événements en parallèle, rompant ainsi l'ordre chronologique métier !
