# 01 - Le Scénario Métier (Comment ça marche)

[⬅️ Sommaire](file:///home/abdelhadi/Documents/kafka-java/docs/00-index.md) | [Fiche Suivante : Les Configurations ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/02-configurations-cles.md)

---

## Le Cas d'Usage : Commander un Produit

Le projet simule un site e-commerce avec 2 services principaux :

1. **`order-service`** : reçoit la commande depuis le site web.
2. **`inventory-service`** : vérifie et réserve le stock en magasin.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Order as order-service (:8081)
    participant DB as PostgreSQL
    participant Kafka as Topic: orders.events
    participant Inv as inventory-service (:8082)

    Client->>Order: POST /api/orders (Achat 1 Laptop)
    Order->>DB: Écrit la commande + l'événement Outbox (Atomique)
    Order-->>Client: 201 Created (Commande en cours)
    Order->>Kafka: Publie l'événement (Clé: orderId)
    Kafka->>Inv: Livré au consommateur
    Inv->>Inv: Déduplique + Réduit le stock de 1
    Inv->>Kafka: Publie le résultat (Succès)
    Kafka->>Order: Commande mise à jour -> CONFIRMED
```

---

## Pourquoi Kafka ici ?

* **Découplage** : Si le service de stock est temporairement éteint ou redémarre, le service de commande continue de fonctionner sans bloquer l'acheteur.
* **Mise en file d'attente automatique** : Kafka garde les événements sur disque jusqu'à ce que les consommateurs soient prêts.
* **Ordre garanti** : Grâce à la clé `orderId`, toutes les actions concernant une même commande arrivent dans le bon ordre.
