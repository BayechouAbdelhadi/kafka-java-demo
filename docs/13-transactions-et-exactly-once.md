# 13 - Transactions Kafka et le Mythe de l'Exactly-Once Global

[⬅️ Précédent : Idempotence & Déduplication](file:///home/abdelhadi/Documents/kafka-java/docs/12-idempotence-et-deduplication.md) | [Suivant : Transactional Outbox Pattern ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/14-transactional-outbox.md)

---

## 1. La Réalité Technique : Qu'est-ce que l'Exactly-Once de Kafka ?

On entend souvent l'affirmation marketing : *"Kafka garantit l'Exactly-Once"*.
En tant qu'architecte ou développeur senior, il est indispensable de comprendre le périmètre réel de cette garantie :

> **L'Exactly-Once de Kafka ne s'applique STRICTEMENT qu'au flux Kafka-vers-Kafka.**
> Il garantit l'atomicité entre la lecture d'un topic source, la publication vers un topic cible et le commit de l'offset dans `__consumer_offsets`.

```text
Périmètre supporté par les Transactions Kafka (EOS) :
[ Topic Entrant ] ──► Consommateur ──► [ Kafka Transaction ] ──► Producteur ──► [ Topic Sortant ]
                                              │
                                              └── Commit Offset Atomique dans __consumer_offsets
```

---

## 2. Pourquoi "Kafka vers Système Externe" (ou inverse) est une Illusion

Dès qu'un flux implique :
1. Une base de données relationnelle (PostgreSQL, Oracle, MySQL), OU
2. Une API externe (Stripe pour un paiement, Twilio pour un SMS),

**La transaction Kafka ne peut en aucun cas englober le système externe.** Il n'existe pas de Two-Phase Commit (2PC / XA) universel et performant reliant Kafka et le monde extérieur.
- Si le paiement Stripe passe mais que Kafka crash avant le commit, Stripe a déjà prélevé la carte bancaire.
- Si PostgreSQL valide une commande mais que l'envoi Kafka échoue, les deux systèmes divergent.

> **Conclusion industrielle :**
> Dans le monde réel, **At-Least-Once Delivery combiné à une Idempotence Métier (table de déduplication)** est la seule architecture viable, résiliente et universellement adoptée.
> C'est pour cette raison exacte que le pattern **Transactional Outbox** a été inventé !
