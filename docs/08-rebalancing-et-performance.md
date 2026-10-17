# 08 - Rebalancing et Optimisation des Performances

[⬅️ Précédent : Consommateurs & Offsets](file:///home/abdelhadi/Documents/kafka-java/docs/07-consumers-offsets-et-groups.md) | [Suivant : Schema Registry & Avro ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/09-schema-registry-et-avro.md)

---

## 1. Qu'est-ce que le Rebalancing ?

Le **Rebalancing** (rééquilibrage) est le processus par lequel le coordinateur de groupe Kafka réassigne les partitions entre les consommateurs lorsqu'un membre :
- Rejoint le groupe (scale-up).
- Quitte volontairement le groupe (arrêt propre).
- Est considéré comme mort (crash ou absence de heartbeat).

---

## 2. Le Protocole Cooperative Sticky (KIP-429)

Historiquement, le protocole de rééquilibrage *Eager* révoquait toutes les partitions de tous les consommateurs (effet *Stop-The-World*).
Le protocole moderne **Cooperative Sticky** (activé par défaut) :
- Ne révoque que les partitions qui doivent effectivement changer de propriétaire.
- Les autres partitions continuent à être consommées sans interruption.

---

## 3. Le Piège du `max.poll.interval.ms`

Deux timers distincts surveillent la santé d'un consommateur :
1. **`session.timeout.ms` (ex: 45s)** : Contrôlé par le thread de fond *Heartbeat*. Si aucun battement de cœur n'est reçu pendant ce délai, le broker considère le processus mort.
2. **`max.poll.interval.ms` (ex: 300s)** : Contrôlé par le thread applicatif principal. Si votre code met plus de 5 minutes à traiter un lot de messages avant de rappeler `consumer.poll()`, Kafka considère le consommateur comme "bloqué" (hang) et déclenche un rebalance immédiat !

### Règle d'or pour éviter les rebalances intempestifs :
- Réduisez `max.poll.records` (ex: 50 ou 100 au lieu de 500) pour garantir que chaque lot est traité en quelques secondes.
- Ne faites jamais d'appels HTTP synchrones lents ou bloquants dans la boucle de consommation sans timeout strict.
