# 03 - Pattern Outbox & Déduplication

[⬅️ Précédent](file:///home/abdelhadi/Documents/kafka-java/docs/02-configurations-cles.md) | [Sommaire](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md) | [Fiche Suivante : Résilience & DLQ ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/04-resilience-et-dlq.md)

---

## 1. Le Pattern Outbox (Zéro Perte de Message)

### Le Piège :
```java
// À NE PAS FAIRE !
orderRepository.save(order);       // Écrit en base
kafkaTemplate.send("orders", ...);  // Envoie sur Kafka
```
Si le serveur crash entre les deux lignes, la commande est créée mais personne ne la prépare !

### La Solution :
On écrit la commande ET un message dans une table `outbox` **dans la même transaction SQL** :
```java
@Transactional
public void createOrder(Order order) {
    orderRepository.save(order);
    outboxRepository.save(new OutboxMessage("ORDER_CREATED", ...));
}
```
Un petit processus en tâche de fond lit la table `outbox` et publie vers Kafka. Impossible de perdre un événement !

---

## 2. La Déduplication (Zéro Double Facturation)

Kafka livre les messages au moins une fois (*At-Least-Once*). Si un réseau saute, un message peut être renvoyé deux fois.

### Comment on évite de débiter le stock deux fois :
Le consommateur maintient une table `processed_events` :
```java
if (processedEvents.existsById(event.getEventId())) {
    // Déjà vu ! On ignore l'action métier
    ack.acknowledge();
    return;
}
// Première fois : on réserve le stock et on mémorise l'ID
stock.decrement();
processedEvents.save(new ProcessedEvent(event.getEventId()));
ack.acknowledge();
```
Même si le message arrive 10 fois, le stock n'est décrémenté qu'une seule fois.
