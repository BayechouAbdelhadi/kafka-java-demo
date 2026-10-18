# ⏱️ Guide Démo 1 Heure : Du Concept à la Pratique Réelle

> **Profil de l'apprenant :** A suivi la théorie (sait ce qu'est un Topic, une Partition, un Producer et un Consumer), mais n'a jamais manipulé de code Spring Boot de production.  
> **Durée totale :** 60 minutes chrono.

---

## 🕒 Timing Minute par Minute

```text
[ 00:00 - 00:10 ] (10 min) 1. Accroche & Cas d'usage : Pourquoi l'appel HTTP synchrone est fragile
[ 00:10 - 00:25 ] (15 min) 2. Le piège n°1 en entreprise : Le Pattern Outbox
[ 00:25 - 00:45 ] (20 min) 3. LIVE DEMO : Le parcours nominal d'une commande
[ 00:45 - 00:55 ] (10 min) 4. Que faire quand ça plante ? DLQ & Schema Registry
[ 00:55 - 01:00 ] (05 min) 5. Récapitulatif & Questions / Réponses
```

---

## 1️⃣ [00:00 - 00:10] Accroche & Cas d'Usage (10 min)

* **Ce qu'il sait déjà :** Un topic a des partitions, un producer envoie, un consumer lit.
* **Le saut vers la pratique :**
  > *"En cours, on fait un `kafkaTemplate.send()` dans un contrôleur REST. En production bancaire ou e-commerce, faire ça directement est une catastrophe. Voyons pourquoi."*
* **Le cas concret :**
  * `order-service` prend une commande.
  * `inventory-service` réserve le stock.
  * **La question à poser :** *"Si le service de stock est en maintenance pendant 3 minutes au Black Friday, est-ce que les clients doivent voir un écran d'erreur 500 sur le site ?"*
  * **La réponse :** Non, Kafka encaisse la charge et dépilera dès le retour du service.

---

## 2️⃣ [00:10 - 00:25] Le Piège n°1 : Le Pattern Outbox (15 min)

* **Ouvrir le fichier :** [`OrderService.java`](file:///home/abdelhadi/Documents/kafka-java/order-service/src/main/java/com/example/kafka/order/service/OrderService.java)
* **Poser la question piège :**
  > *"Si je fais `database.save(order)` puis `kafkaTemplate.send(...)` et que le serveur redémarre pile entre les deux, qu'est-ce qui se passe ?"*
  * Réponse attendue : La commande est en base, la carte bleue est débitée, mais l'entrepôt ne recevra jamais l'événement.
* **Montrer la solution Outbox :**
  * On sauvegarde la commande ET une ligne `outbox_messages` **dans la même transaction SQL**.
  * Un relay en tâche de fond publie sur Kafka. Zéro risque de perte.

---

## 3️⃣ [00:25 - 00:45] LIVE DEMO : Le Scénario Normal (20 min)

* **Lancer l'infrastructure :**
  ```bash
  ./start.sh
  ```
* **Lancer le test devant l'apprenant :**
  ```bash
  ./test-normal.sh
  ```
* **Ce qu'on lui montre à l'écran :**
  1. La commande est créée en statut `CREATED`.
  2. L'événement part dans Kafka avec la clé `orderId`.
  3. `inventory-service` consomme l'événement.
  4. Le stock baisse de 10 à 9.
  5. La commande passe à `CONFIRMED`.
* **Le point réflexe à lui apprendre :**
  * Ouvrir [`OrderEventConsumer.java`](file:///home/abdelhadi/Documents/kafka-java/inventory-service/src/main/java/com/example/kafka/inventory/consumer/OrderEventConsumer.java).
  * Pointer la table `processed_events` : *"Si Kafka nous renvoie le même message par erreur réseau, on vérifie si l'ID a déjà été vu pour ne pas débiter le stock une deuxième fois."*

---

## 4️⃣ [00:45 - 00:55] Les Pannes : DLQ & Schema Registry (10 min)

Sans entrer dans des scripts complexes, expliquer les 2 réflexes indispensables :

1. **La DLQ (Dead Letter Queue)** :
   * *"Si un client envoie un message corrompu avec un JSON invalide, on ne va pas bloquer tout le système. On l'isole dans un topic de rejet appelé DLQ (`orders.events-dlt`), et le reste continue de défiler."*
2. **Schema Registry & `FORWARD_TRANSITIVE`** :
   * *"Si j'ajoute un nouveau champ obligatoire dans mon événement sans mettre de valeur par défaut, les anciens messages encore dans Kafka feront crasher les nouveaux consommateurs. Schema Registry sert d'arbitre pour bloquer cette erreur avant la mise en prod."*

---

## 5️⃣ [00:55 - 01:00] Synthèse & Conclusion (5 min)

Les 3 règles d'or à emporter :
1. **Toujours partitionner par clé métier** (`orderId`) pour avoir l'ordre garanti.
2. **Ne jamais faire d'envoi Kafka direct sans Outbox** si on écrit en base de données.
3. **Toujours dédupliquer en base** (`processed_events`) côté consommateur.
