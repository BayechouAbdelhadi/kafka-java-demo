# 03 - Brokers, KRaft et Configuration

[⬅️ Précédent : Modèle Mental Kafka](file:///home/abdelhadi/Documents/kafka-java/docs/02-modele-mental-kafka.md) | [Suivant : Topics, Partitions & Réplication ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/04-topics-partitions-et-replication.md)

---

## 1. Le Mode KRaft (Kafka Raft Metadata mode)

Depuis Kafka 3.3+ (et en standard dans Kafka 3.7/4.0), le composant externe **Apache ZooKeeper** est obsolète et supprimé au profit de **KRaft** :
- Les métadonnées du cluster (topics, partitions, ACLs, configurations) sont stockées directement dans un topic interne spécial : `@metadata`.
- Un sous-ensemble de brokers (ou tous les brokers) jouent le rôle de **Controllers** formant un quorum Raft (`controller.quorum.voters`).
- Avantages majeurs : temps de démarrage divisé par 10, support de millions de partitions, fin de la désynchronisation ZooKeeper/Brokers.

---

## 2. Le Piège Classique : Listeners vs Advertised Listeners

Dans un environnement Docker / Cloud, un client externe (sur votre machine hôte) et un client interne (un autre conteneur Docker) n'accèdent pas au broker par la même adresse IP/port.

Dans notre [docker-compose.yml](file:///home/abdelhadi/Documents/kafka-java/docker-compose.yml) :
```yaml
environment:
  KAFKA_LISTENERS: INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093
  KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:9092,EXTERNAL://localhost:9092
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
```

- **`listeners`** : Interfaces réseau et ports sur lesquels le broker écoute localement à l'intérieur du conteneur.
- **`advertised.listeners`** : Les adresses et ports réels que le broker **renvoie aux clients** lors de la négociation initiale de métadonnées.
  - Si un client Java tourne sur votre PC (en dehors de Docker), il reçoit `localhost:9092`.
  - Si Schema Registry tourne dans le réseau Docker, il reçoit `kafka-1:9092`.

---

## 3. Durabilité : Facteur de Réplication et `min.insync.replicas`

### La formule mathématique de la tolérance aux pannes :
$$\text{Brokers Tolérés en Panne} = N - \text{min.insync.replicas}$$

Dans un cluster de production standard :
- `replication.factor = 3` (chaque partition est dupliquée sur 3 brokers distincts).
- `min.insync.replicas = 2` (au moins 2 réplicas synchronisés - ISR - doivent acquitter l'écriture lorsque le producteur utilise `acks=all`).
- Résultat : vous pouvez perdre $3 - 2 = 1$ broker sans interrompre les écritures du producteur.

### Pourquoi le profil monoconteneur local ne peut pas simuler cette garantie :
Dans un environnement de développement avec un seul broker actif (`replication.factor = 1`, `min.insync.replicas = 1`) :
- Si vous configurez `min.insync.replicas = 2` sur un seul broker, **tous les `acks=all` échouent avec `NotEnoughReplicasException`**.
- La résilience multi-broker requiert au minimum 3 nœuds actifs, comme configuré dans notre profil étendu de `docker-compose.yml`.
