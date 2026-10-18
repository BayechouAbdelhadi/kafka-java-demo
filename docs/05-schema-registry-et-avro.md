# 05 - Schema Registry & Évolution des Schémas

[⬅️ Précédent](file:///home/abdelhadi/Documents/kafka-java/docs/04-resilience-et-dlq.md) | [Sommaire Principal 🏠](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md)

---

## 1. À Quoi Sert Schema Registry ?

Quand plusieurs équipes travaillent sur Kafka, si l'équipe "Commandes" change le format de son événement, l'équipe "Inventaire" peut crasher.
**Schema Registry** sert d'arbitre central : chaque nouveau schéma d'événement doit être validé avant d'être accepté.

---

## 2. La Règle `FORWARD_TRANSITIVE` en Clair

> **Règle :** Un nouveau consommateur doit toujours pouvoir lire les anciens messages encore stockés dans Kafka.

```text
Message ancien dans Kafka (V1) : { "orderId": "123", "montant": 50 }
                                        │
                                        ▼
Nouveau Consommateur (V2)      : Attend { "orderId", "montant", "devise" }
```

### Le Piège du Champ sans Valeur par Défaut :
* ❌ **Si on ajoute `"devise"` SANS valeur par défaut** : Le nouveau consommateur ne sait pas quoi lire dans les vieux messages V1. **Schema Registry bloque cette version avec une erreur `409 Conflict` !**
* ✅ **Si on ajoute `"devise": "EUR"` AVEC valeur par défaut** : Le nouveau consommateur sait qu'il doit appliquer `"EUR"` lorsqu'il lit un ancien message. **Schema Registry valide l'évolution**.
