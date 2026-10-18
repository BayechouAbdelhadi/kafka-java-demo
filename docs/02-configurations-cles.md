# 02 - Les 5 Configurations Indispensables

[⬅️ Précédent](file:///home/abdelhadi/Documents/kafka-java/docs/01-architecture-et-scenario.md) | [Sommaire](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md) | [Fiche Suivante : Outbox & Idempotence ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/03-patterns-outbox-et-idempotence.md)

---

## 1. Pour les Topics

* **`partitions` (ex: 3)** : Le nombre d'autoroutes parallèles. Plus il y a de partitions, plus on peut mettre de consommateurs en parallèle (1 partition = 1 consommateur max par groupe).
* **`replication-factor` (1 en dev, 3 en prod)** : Le nombre de copies de sécurité sur des serveurs distincts.
* **`retention.ms` (ex: 7 jours)** : La durée de conservation sur disque. Lire un message ne le supprime jamais !

---

## 2. Pour le Producteur (`application.yml`)

```yaml
spring.kafka.producer:
  acks: all                   # 1. Ne valide que si tous les serveurs ont copié le message
  properties:
    enable.idempotence: true  # 2. Évite les doublons en cas de micro-coupure réseau
```

---

## 3. Pour le Consommateur (`application.yml`)

```yaml
spring.kafka.consumer:
  group-id: inventory-group   # 3. Nom de l'équipe de consommateurs qui se partagent le travail
  enable-auto-commit: false   # 4. TRÈS IMPORTANT : on valide la lecture seulement quand notre traitement SQL a réussi
  auto-offset-reset: earliest # 5. Si nouveau consommateur, lit depuis le début de l'histoire
```
