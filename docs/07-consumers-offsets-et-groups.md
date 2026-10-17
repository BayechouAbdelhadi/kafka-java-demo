# 07 - Consommateurs, Offsets et Consumer Groups

[⬅️ Précédent : Producteurs & Fiabilité](file:///home/abdelhadi/Documents/kafka-java/docs/06-producers-et-fiabilite.md) | [Suivant : Rebalancing & Performance ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/08-rebalancing-et-performance.md)

---

## 1. Règle Fondamentale des Consumer Groups

Dans Apache Kafka :
> Au sein d'un même **Consumer Group**, chaque partition d'un topic est assignée à **au plus un seul thread consommateur** à un instant donné.

```text
Topic orders.events (3 Partitions) :
  [ Partition 0 ] ──────────► Consommateur 1 (Groupe: inventory-group)
  [ Partition 1 ] ──────────► Consommateur 2 (Groupe: inventory-group)
  [ Partition 2 ] ──────────► Consommateur 3 (Groupe: inventory-group)
                              Consommateur 4 (INACTIF / En attente !)
```

Si vous ajoutez un 4ᵉ consommateur sur un topic à 3 partitions dans le même groupe, il reste **inactif** tant qu'aucun des 3 autres ne tombe en panne. Pour augmenter le parallélisme, il faut d'abord augmenter le nombre de partitions.

---

## 2. Le Piège de l'Auto-Commit (`enable.auto.commit = true`)

Par défaut, les clients Kafka historiques activent l'auto-commit toutes les 5 secondes (`auto.commit.interval.ms = 5000`).

### Scénario de Perte de Données (*At-Most-Once Accidentel*) :
1. Le consommateur lit 100 messages dans son buffer mémoire.
2. 5 secondes s'écoulent en arrière-plan : l'offset 100 est automatiquement commité sur Kafka.
3. Le consommateur crash alors qu'il n'avait traité que le 20ᵉ message.
4. Au redémarrage, le consommateur reprend à l'offset 100. **Les messages 21 à 99 sont définitivement perdus !**

---

## 3. La Solution Adoptée : Commit Manuel Contrôlé (`MANUAL_IMMEDIATE`)

Dans `inventory-service` :
```yaml
spring.kafka:
  consumer:
    enable-auto-commit: false
  listener:
    ack-mode: manual_immediate
```

Dans le code Java :
```java
@KafkaListener(topics = KafkaTopics.ORDERS_EVENTS, groupId = "inventory-service-group")
@Transactional
public void handleOrderEvent(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
    // 1. Déduplication en base de données locale
    // 2. Traitement métier (réservation stock)
    // 3. Commit de l'offset UNIQUEMENT après validation locale
    ack.acknowledge();
}
```
En cas de crash au milieu du traitement, l'offset n'a pas été commité. Au redémarrage, le message est relu et la table `processed_events` intercepte le doublon (garantie **At-Least-Once** transformée en **Exactly-Once effectif**).
