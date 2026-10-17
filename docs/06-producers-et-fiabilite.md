# 06 - Producteurs et Garanties de Fiabilité

[⬅️ Précédent : Clés & Ordre](file:///home/abdelhadi/Documents/kafka-java/docs/05-cles-et-ordre-des-evenements.md) | [Suivant : Consommateurs, Offsets & Groups ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/07-consumers-offsets-et-groups.md)

---

## 1. Configuration de Fiabilité Maximale du Producteur

Dans notre projet (`order-service` et `inventory-service`), les producteurs sont configurés avec les paramètres les plus stricts du standard industriel :

```yaml
spring.kafka.producer:
  acks: all
  retries: 5
  properties:
    enable.idempotence: true
    delivery.timeout.ms: 120000
    linger.ms: 20
    batch.size: 32768
```

---

## 2. Décryptage des Paramètres

### `acks=all` (ou `-1`)
Le broker leader n'envoie son accusé de réception au producteur que lorsque le message a été répliqué avec succès sur l'ensemble des nœuds de la liste **ISR** (*In-Sync Replicas*). Aucun message ne peut être perdu si le broker leader s'effondre après confirmation.

### `enable.idempotence=true` (Idempotence Native du Producer)
Évite les doublons provoqués par des pertes de paquets réseau entre le producteur et le broker.
- **Fonctionnement interne** :
  1. Au démarrage, le cluster assigne au producteur un identifiant unique : le **PID** (*Producer ID*).
  2. Pour chaque partition, le producteur maintient un numéro de séquence incrémental (`0, 1, 2, ...`).
  3. Si le broker reçoit le message avec un numéro de séquence qu'il a déjà validé (par exemple si l'ACK réseau s'est perdu et que le client a retenté), le broker ignore la duplication et renvoie un ACK valide sans réécrire le message.
- **Attention** : Cette idempotence ne protège que la couche transport Kafka. Elle ne protège **PAS** contre le redémarrage de l'application ou les doublons métier (qui requièrent la table `processed_events` !).

---

## 3. Optimisation : Débit vs Latence

- **`linger.ms = 20`** : Le producteur attend jusqu'à 20 millisecondes avant d'envoyer un lot (*batch*) si le buffer n'est pas plein. Cela permet de regrouper plusieurs messages en un seul appel réseau TCP, augmentant considérablement le débit.
- **Compression** : L'utilisation de `compression.type=zstd` ou `snappy` réduit drastiquement la bande passante réseau et l'espace disque sans impacter le CPU.
