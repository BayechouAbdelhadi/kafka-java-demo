# 02 - Modèle Mental : Comprendre Kafka vs les Files Traditionnelles

[⬅️ Précédent : Objectifs & Architecture](file:///home/abdelhadi/Documents/kafka-java/docs/01-objectifs-et-architecture.md) | [Suivant : Brokers KRaft & Configuration ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/03-brokers-kraft-et-configuration.md)

---

## 1. La Différence Fondamentale : Log Append-Only vs File Destructive

Une erreur courante des développeurs venant de JMS, RabbitMQ ou SQS est de traiter Kafka comme une simple file d'attente (*Queue*).

```text
File Traditionnelle (RabbitMQ / JMS) :
[ Message 1 ] -> [ Message 2 ] -> [ Consommateur lit & acquitte ] -> Message SUPPRIMÉ de la file !

Journal Distribué Kafka (Commit Log) :
Partition 0 :  [ Offset 0 ][ Offset 1 ][ Offset 2 ][ Offset 3 ][ Offset 4 ] ... [ Offset N ] ➡️ (Append-Only)
               ▲                       ▲
               │                       └── Consommateur B (Offset 3)
               └────────────────────────── Consommateur A (Offset 0 - rejeu historique)
```

| Critère | Files Traditionnelles (RabbitMQ / SQS) | Apache Kafka |
|---|---|---|
| **Structure** | Files en mémoire avec purge après acquittement. | Journal séquentiel immuable sur disque (*Append-Only Log*). |
| **Destruction du message** | Dès que le consommateur confirme la réception (`ACK`). | Jamais à la consommation ! Rétention temporelle (`retention.ms`) ou volumétrique. |
| **Parallélisme** | Concurrence par message individuel (*Competing Consumers*). | Concurrence stricte par **Partition** au sein d'un *Consumer Group*. |
| **Rejeu de données** | Impossible (les données ont disparu). | Trivial : il suffit de déplacer le curseur (*Offset*) en arrière. |
| **Débit** | Typiquement 10k - 50k messages/sec. | Plusieurs centaines de milliers à millions de messages/sec (I/O séquentiel et *Zero-Copy*). |

---

## 2. Le Mécanisme de Pull vs Push

- Dans les brokers classiques, le broker **pousse** les messages vers le consommateur (*Push*). Risque : si le consommateur est lent, il s'asphyxie (mémoire saturée).
- Dans Kafka, le consommateur **tire** les messages à son rythme (*Pull*) via la boucle `poll(Duration)`. Le consommateur contrôle sa propre contre-pression (*Backpressure*).

---

## 3. Qu'est-ce qu'un Offset ?

L'**offset** est un entier séquentiel 64 bits (`long`) unique par partition. Il représente la position exacte d'un message dans la partition :
- Les offsets ne sont jamais réassignés ni décrémentés par le broker.
- Le consommateur stocke son avancement dans un topic interne spécial de Kafka : `__consumer_offsets`.
- Le **Consumer Lag** correspond à la différence arithmétique :
  $$\text{Lag} = \text{Log End Offset (dernier produit)} - \text{Current Offset (dernier validé)}$$
  Un lag qui augmente de manière continue indique un goulot d'étranglement dans le consommateur.
