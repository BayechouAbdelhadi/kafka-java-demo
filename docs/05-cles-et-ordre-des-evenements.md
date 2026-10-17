# 05 - Clés de Partitionnement et Ordre des Événements

[⬅️ Précédent : Topics & Partitions](file:///home/abdelhadi/Documents/kafka-java/docs/04-topics-partitions-et-replication.md) | [Suivant : Producteurs & Fiabilité ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/06-producers-et-fiabilite.md)

---

## 1. Le Mythe de l'Ordre Global dans Kafka

> **Règle absolue :** Apache Kafka **ne garantit JAMAIS** un ordre global à travers toutes les partitions d'un topic.
> Kafka garantit l'ordre chronologique strict **uniquement au sein d'une même partition**.

Si vous avez besoin que les événements d'une entité (ex: `ORDER_CREATED`, `ORDER_PAID`, `ORDER_SHIPPED`) soient consommés dans l'ordre où ils se sont produits, **tous ces événements doivent obligatoirement être dirigés vers la même partition**.

---

## 2. Rôle et Stratégie du Choix de la Clé

| Topic | Clé Choisie | Justification Métier |
|---|---|---|
| `orders.events` | `orderId` | Garantit que tous les événements d'une même commande sont traités séquentiellement par le même thread de consommation. |
| `inventory.events` | `orderId` | Permet à l'écouteur d'associer immédiatement le résultat à la commande d'origine sans désynchronisation. |
| `notifications.events` | `recipient` | Regroupe les alertes d'un même utilisateur sur la même partition. |

---

## 3. Le Risque de Hot Partition (Partition Chaude)

Si une clé unique concentre une part démesurée du trafic (par exemple un client grossiste qui passe 80% des commandes de la journée) :
- Tous ses événements se retrouvent sur une seule et même partition.
- Le consommateur assigné à cette partition sature, tandis que les autres consommateurs restent inactifs.
- **Solution d'atténuation** : Salage de clé (*Key Salting*, ex: `orderId_0`, `orderId_1`) lorsque le séquencement absolu peut être relaxé au profit du débit.

---

## 4. Distinguer les 3 Notions d'Identifiants

1. **Clé de message Kafka (`Record Key`)** : Utilisée par le partitionneur pour router vers une partition précise ($murmur2(key)$).
2. **Identifiant d'événement (`eventId`)** : UUID unique généré à chaque émission, transporté dans le payload pour la traçabilité et la déduplication dans la table `processed_events`.
3. **Identifiant d'agrégat métier (`orderId`)** : Référence de l'entité dans le domaine métier.
