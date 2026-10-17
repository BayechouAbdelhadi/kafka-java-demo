# 09 - Schema Registry et Sérialisation Avro

[⬅️ Précédent : Rebalancing](file:///home/abdelhadi/Documents/kafka-java/docs/08-rebalancing-et-performance.md) | [Suivant : Évolution des Schémas & FORWARD_TRANSITIVE ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/10-evolution-des-schemas-et-forward-transitive.md)

---

## 1. Pourquoi Schema Registry et Apache Avro ?

Dans une architecture distribuée, échanger du JSON non typé crée des couplages fragiles : un producteur qui renomme ou supprime un champ fait planter silencieusement les consommateurs.
**Confluent Schema Registry** agit comme un registre centralisé de contrats d'interface :
- Les schémas sont écrits en JSON déclaratif standardisé (`.avsc`).
- Le plugin Maven compile ces schémas en classes Java fortement typées à la compilation.
- Aucun message non conforme au contrat ne peut être publié.

---

## 2. Le Format Binaire Confluent Wire Format (Magic Byte)

À la différence d'un fichier Avro autonome qui embarque tout le schéma en en-tête, le sérialiseur Confluent utilise un préfixe ultra-léger de **5 octets** :

```text
[ Octet 0 : Magic Byte (0x00) ] [ Octets 1 à 4 : Schema ID (uint32) ] [ Octets 5+ : Données Avro Compactes ]
```

1. **Octet 0 (`0x00`)** : Magic Byte indiquant le protocole de sérialisation Confluent.
2. **Octets 1 à 4** : L'identifiant numérique unique du schéma dans Schema Registry (ex: ID `42`).
3. **Octets 5 et suivants** : Le payload binaire pur.

### Consommation et Cache Local :
Lorsque le consommateur lit le message, il extrait le Schema ID `42`. S'il l'a déjà en cache local mémoire, il désérialise sans faire aucun appel réseau !
Si Schema Registry devient temporairement indisponible, **les flux de messages continuent de fonctionner à plein régime grâce aux caches des clients** (tant qu'aucun nouveau schéma inconnu n'est introduit).

---

## 3. Stratégies de Nommage des Sujets (*Subject Naming Strategy*)

1. **`TopicNameStrategy` (Standard par défaut adopté dans ce projet)** :
   - Le sujet dans Schema Registry s'appelle `<topic>-key` ou `<topic>-value` (ex: `orders.events-value`).
   - Idéal pour les topics qui transportent un seul type d'événement métier.
2. **`RecordNameStrategy`** :
   - Le sujet porte le nom complet de la classe Avro (`com.example.OrderEvent`). Permet de mélanger plusieurs types d'événements dans le même topic.
3. **`TopicRecordNameStrategy`** :
   - Combine le nom du topic et le nom du record (`orders.events-com.example.OrderEvent`).
