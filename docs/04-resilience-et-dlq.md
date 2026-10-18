# 04 - Que Faire Quand ça Plante (Retries & DLQ)

[⬅️ Précédent](file:///home/abdelhadi/Documents/kafka-java/docs/03-patterns-outbox-et-idempotence.md) | [Sommaire](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md) | [Fiche Suivante : Schema Registry ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/05-schema-registry-et-avro.md)

---

## Les 2 Types d'Erreurs

Dans un système distribué, il y a deux manières d'échouer :

| Type d'Erreur | Exemple | Action |
|---|---|---|
| **Erreur Temporaire** | Coupure réseau de 2 secondes, base occupée. | **Retry** : On réessaie après un court délai (1s, 2s). |
| **Erreur Définitive** | Données corrompues, JSON illisible, ID invalide. | **DLQ (Dead Letter Queue)** : On déroute le message immédiatement. |

---

## Pourquoi la DLQ est Obligatoire ?

Si on retentait une erreur définitive en boucle :
* Le message bloquerait toute la partition.
* Aucun des milliers de messages suivants ne pourrait passer !

### La Solution avec Spring Kafka :
```java
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2.0),
    exclude = {PermanentException.class} // Les erreurs définitives vont direct en DLQ
)
@KafkaListener(topics = "orders.events")
public void handle(OrderEvent event) { ... }

@DltHandler
public void handleDeadLetter(OrderEvent event) {
    // Stocké dans le topic "orders.events-dlt" pour analyse humaine
    log.error("Message en échec mis de côté : " + event.getOrderId());
}
```
**Résultat** : Les bons messages continuent d'être traités, et les mauvais sont isolés dans la boîte de rejet (DLQ).
