# Mission : développer un projet Kafka complet, exécutable et documenté pour une formation technique

## 1. Objectif général

Construis un projet Kafka complet, réaliste, robuste et exécutable, destiné à servir de support à une formation technique pour des développeurs Java et Spring Boot.

Le projet doit être **entièrement développé avant la formation**. Il ne s'agit pas de coder l'application en direct pendant la présentation, mais d'expliquer les concepts à partir d'une implémentation existante, d'étudier le code, de justifier les choix techniques et de réaliser des démonstrations reproductibles.

Le rendu final doit comprendre :

1. Le code source complet des services.
2. Une infrastructure locale avec Kafka, KRaft, Schema Registry et PostgreSQL.
3. Des scripts reproductibles pour initialiser et vérifier les topics Kafka.
4. Des schémas Avro versionnés et des démonstrations d'évolution avec `FORWARD_TRANSITIVE`.
5. Des tests automatisés et des scénarios de panne reproductibles.
6. Une documentation pédagogique en français composée de fichiers Markdown numérotés dans `/docs`.
7. Un guide de présentation avec les commandes exactes, les résultats attendus et les points à expliquer.

Ne te limite pas à proposer une architecture, des exemples théoriques ou des extraits de code. Implémente le projet, exécute les validations possibles et documente honnêtement les résultats.

## 2. Stack technique

Utilise les technologies suivantes :

- Java LTS.
- Spring Boot et Spring for Apache Kafka.
- Apache Kafka en mode KRaft, sans ZooKeeper.
- Confluent Schema Registry.
- Avro, avec génération des classes Java à partir des fichiers `.avsc`.
- PostgreSQL pour les données métier et le pattern Transactional Outbox.
- Maven.
- Docker Compose.
- JUnit 5, Spring Boot Test et Testcontainers lorsque pertinent.
- Spring Boot Actuator et Micrometer pour l'observabilité.

Choisis des versions compatibles et explicites pour Kafka, Schema Registry, les clients Confluent, Spring Boot et les plugins Maven.

Fournis un `.env.example` sans secret réel. Le démarrage local doit être documenté et aussi simple que possible.

## 3. Cas métier : système de commandes e-commerce

Développe une application distribuée comprenant les composants suivants :

- `order-service` : création et gestion des commandes.
- `inventory-service` : réservation du stock.
- `notification-service` : simulation de notifications client.
- `outbox-relay` : publication des événements enregistrés dans l'Outbox, ou un mécanisme CDC documenté si tu choisis Debezium.

Le scénario principal doit fonctionner ainsi :

1. Une commande est créée via HTTP.
2. La commande et son événement Outbox sont enregistrés dans une transaction PostgreSQL.
3. Le relay publie l'événement dans Kafka.
4. Le service de stock consomme l'événement et réserve le stock.
5. Un événement de résultat est publié.
6. Le service de notification traite les événements pertinents.
7. Les erreurs temporaires sont retentées.
8. Les erreurs permanentes finissent dans une Dead Letter Topic (DLT).
9. Un événement peut être rejoué de manière contrôlée.

Expose des endpoints REST utiles pour créer des commandes, consulter leur état et déclencher certains scénarios de démonstration. Fournis des commandes `curl` exécutables.

## 4. Architecture et configuration Kafka

Explique et démontre les concepts suivants :

- Brokers et cluster.
- KRaft et quorum.
- Topics, partitions, leaders et réplicas.
- Consumer groups et répartition des partitions.
- Offsets et commits.
- Ordre des messages.
- Réplication et tolérance aux pannes.
- Rétention et politiques de nettoyage.
- Sérialisation et désérialisation.
- Rebalancing.
- Débit, latence et backpressure.

### Configuration des brokers

Documente les paramètres importants réellement utilisés, notamment :

- Identité des nœuds KRaft et quorum.
- `listeners` et `advertised.listeners`.
- Facteurs de réplication par défaut.
- `min.insync.replicas`.
- Réplication des topics internes.
- Rétention des logs.
- Élection des leaders et disponibilité.
- Paramètres de sécurité pertinents.

Pour chaque configuration, précise où elle est définie, pourquoi elle existe, son effet et ses limites.

Le profil local peut utiliser un seul broker pour simplifier le démarrage, mais il ne doit jamais être présenté comme hautement disponible. Si un seul broker est utilisé, explique pourquoi la réplication et `min.insync.replicas` ne peuvent pas fournir les mêmes garanties qu'un cluster multi-brokers.

Si pertinent, fournis un profil multi-brokers pour les démonstrations de réplication, de perte de broker et de disponibilité.

## 5. Création et validation reproductibles des topics Kafka — obligatoire

**Tous les topics doivent être créés et configurés par des scripts versionnés dans Git. Aucune création manuelle ne doit être nécessaire pour démarrer le projet.**

### 5.1. Organisation attendue

Prévois une structure similaire à celle-ci, adaptée aux topics réellement utilisés :

```text
infrastructure/
└── kafka/
    ├── config/
    │   └── broker.properties
    ├── topics/
    │   ├── orders.events.properties
    │   ├── inventory.events.properties
    │   ├── notifications.events.properties
    │   ├── orders.retry.properties
    │   └── orders.dlt.properties
    └── scripts/
        ├── wait-for-kafka.sh
        ├── create-topics.sh
        ├── describe-topics.sh
        ├── validate-topics.sh
        └── reset-local-environment.sh
```

Adapte les noms et les fichiers aux topics réellement implémentés. Ne crée pas des topics inutiles uniquement pour remplir l'arborescence.

### 5.2. Script de création

Le script `create-topics.sh` doit :

- Attendre que Kafka soit réellement prêt à recevoir des commandes.
- Créer tous les topics requis par les services, les retries, les DLT et les démonstrations.
- Définir explicitement les paramètres importants : partitions, réplication et configurations propres aux topics.
- Être idempotent : sa réexécution ne doit pas échouer simplement parce que les topics existent déjà.
- Ne jamais supprimer automatiquement les topics existants ou leurs données.
- Afficher les topics créés, ceux qui existaient déjà et les erreurs.
- Retourner un code de sortie non nul si une opération obligatoire échoue.
- Fonctionner sans intervention manuelle.

L'existence d'un topic ne doit pas être considérée comme une preuve de conformité : le script de création et le script de validation doivent avoir des responsabilités distinctes.

### 5.3. Script de validation

Le script `validate-topics.sh` doit vérifier les propriétés réellement appliquées sur le cluster, et non seulement les valeurs déclarées dans le dépôt.

Il doit contrôler, selon les paramètres pertinents :

- L'existence des topics.
- Le nombre de partitions.
- Le facteur de réplication effectif.
- Les paramètres de rétention et de nettoyage.
- `min.insync.replicas` lorsque pertinent.
- Les écarts entre la configuration attendue et la configuration effective.

En cas de non-conformité bloquante, le script doit afficher les écarts et retourner un code de sortie non nul.

Utilise les outils Kafka adaptés aux versions choisies, par exemple `kafka-topics.sh --create`, `kafka-topics.sh --describe` et `kafka-configs.sh --describe`. Ne suppose pas que les valeurs par défaut sont identiques entre versions.

### 5.4. Intégration à Docker Compose

L'initialisation doit suivre un ordre fiable :

1. Démarrer Kafka.
2. Attendre que le broker soit prêt.
3. Exécuter `create-topics.sh`.
4. Exécuter `validate-topics.sh`.
5. Démarrer ou activer les services applicatifs dépendants.

Ne te fie pas uniquement à l'état `running` d'un conteneur. Utilise un contrôle de disponibilité effectif et documente les dépendances entre services.

Les scripts doivent pouvoir être relancés après un redémarrage sans supprimer les données existantes.

### 5.5. Réinitialisation de l'environnement

Le script `reset-local-environment.sh` doit être séparé des scripts d'initialisation. Toute suppression de données doit être explicite, documentée et protégée contre une exécution accidentelle.

Le démarrage normal du projet ne doit jamais exécuter ce script.

Fournis des commandes permettant de créer les topics, les lister, inspecter leurs partitions et leurs réplicas, consulter leurs configurations et vérifier leur conformité.

### 5.6. Configuration et cycle de vie des topics

Pour chaque topic, documente :

- Son objectif métier.
- Son nombre de partitions.
- Son facteur de réplication.
- Sa rétention et sa politique de nettoyage.
- Sa clé de partitionnement.
- Ses producteurs et consommateurs.
- Sa stratégie de reprise en cas d'erreur.
- Les scripts qui le créent et le valident.

Explique comment modifier une configuration existante, quelles propriétés peuvent être modifiées et pourquoi certaines modifications nécessitent une migration ou une procédure particulière.

Explique également pourquoi augmenter le nombre de partitions peut changer la répartition des clés et affecter certains comportements d'ordre.

## 6. Choix des clés de partitionnement

Le choix de la clé doit être explicite et justifié pour chaque topic.

Exemples possibles :

- `orderId` pour préserver l'ordre des événements associés à une commande.
- `productId` ou `sku` pour les événements de stock lorsque l'ordre par produit est requis.
- Une clé adaptée au regroupement des événements de notification.

Pour chaque choix, explique :

- L'ordre métier recherché.
- Le niveau de parallélisme obtenu.
- La distribution des clés sur les partitions.
- Le risque de hot partition.
- Le comportement en l'absence de clé.
- L'effet d'une augmentation du nombre de partitions.
- La différence entre clé Kafka, identifiant d'événement et clé de déduplication métier.

Ne prétends jamais que Kafka garantit un ordre global sur toutes les partitions.

## 7. Producers et consumers

### 7.1. Producers

Implémente et documente les paramètres pertinents :

- `acks`.
- `enable.idempotence`.
- `retries`.
- `delivery.timeout.ms`.
- `request.timeout.ms`.
- `linger.ms`.
- `batch.size`.
- Compression.
- Sérialisation.
- Gestion des erreurs de publication.

Explique les compromis entre fiabilité, débit et latence. Distingue l'idempotence native du producer de l'idempotence métier.

### 7.2. Consumers

Implémente et documente :

- `group.id`.
- `auto.offset.reset`.
- `enable.auto.commit`.
- `max.poll.records`.
- `max.poll.interval.ms`.
- `session.timeout.ms` et `heartbeat.interval.ms`, selon le mode utilisé.
- `isolation.level`.
- Stratégie d'assignation.
- Concurrence Spring Kafka.
- Modes d'acquittement et commits.
- Gestion des erreurs et des offsets.

Démontre les conséquences d'un crash avant le traitement, après l'effet métier ou avant le commit de l'offset.

## 8. Schema Registry et Avro — partie obligatoire

Schema Registry et Avro doivent être réellement intégrés au projet. Ne les traite pas comme une simple extension facultative.

### 8.1. Infrastructure et sérialisation

Configure Schema Registry dans Docker Compose et utilise Avro pour les événements métier.

Le projet doit comprendre :

- Les fichiers `.avsc` versionnés.
- La génération des classes Java Avro par Maven.
- Les serializers et deserializers Confluent.
- La configuration de connexion à Schema Registry.
- Des tests de sérialisation et désérialisation.
- Des scripts ou commandes pour consulter les schémas enregistrés.
- La configuration et la validation des politiques de compatibilité.
- Des démonstrations d'évolution des schémas.

Explique le format de sérialisation Confluent, notamment le magic byte et l'identifiant de schéma, ainsi que la différence entre le nom d'un topic Kafka et le nom d'un sujet Schema Registry.

Choisis explicitement une stratégie de nommage, par exemple `TopicNameStrategy`, puis explique ses implications et compare-la brièvement à `RecordNameStrategy` et `TopicRecordNameStrategy`.

Les événements doivent utiliser une enveloppe cohérente, avec les champs pertinents comme `eventId`, `eventType`, `occurredAt`, `aggregateId` et les données métier.

### 8.2. Évolution des schémas

Explique avec des exemples Avro réels :

- Writer schema et reader schema.
- Champ ajouté ou supprimé.
- Valeurs par défaut.
- Changements de type.
- Champs nullables et unions.
- Compatibilité backward, forward et full.
- Compatibilité transitive et non transitive.
- Rôle et limites de Schema Registry.

Distingue la compatibilité technique du schéma de la compatibilité fonctionnelle métier.

### 8.3. Démonstration obligatoire de `FORWARD_TRANSITIVE`

Configure au moins un sujet Schema Registry avec la politique exacte :

`FORWARD_TRANSITIVE`

Ne confonds pas cette politique avec `FORWARD`, `BACKWARD_TRANSITIVE` ou `FULL_TRANSITIVE`.

Explique précisément que la compatibilité forward vérifie si un lecteur utilisant le nouveau schéma peut lire des données écrites avec les anciens schémas. En mode transitive, la validation est effectuée par rapport à toutes les versions précédentes du sujet, et pas seulement à la dernière.

Construis une démonstration reproductible comprenant :

1. Une version initiale du schéma Avro.
2. Plusieurs versions successives.
3. Une évolution acceptée avec `FORWARD_TRANSITIVE`.
4. Une évolution rejetée avec cette politique.
5. Un cas accepté par rapport à la version précédente, mais rejeté en raison d'une version plus ancienne.
6. Les commandes ou requêtes API pour configurer et consulter la compatibilité.
7. Les réponses attendues de Schema Registry.
8. Des tests automatisés pour les cas acceptés et rejetés.

Explique particulièrement le cas d'un nouveau champ sans valeur par défaut : un nouveau lecteur peut ne pas réussir à lire un ancien message si le champ attendu est absent des données sérialisées.

Les schémas, commandes, réponses et tests doivent être réellement présents dans le dépôt. N'invente pas de résultats d'exécution.

### 8.4. Enregistrement et validation reproductibles des schémas

Applique une démarche versionnée et automatisée similaire à celle des topics.

Prévois des scripts ou des tâches dédiées pour :

- Vérifier que Schema Registry est disponible.
- Enregistrer les schémas requis dans le bon ordre.
- Configurer explicitement la compatibilité des sujets concernés.
- Vérifier la politique de compatibilité effective.
- Vérifier les évolutions prévues et détecter les incompatibilités.

L'initialisation des schémas doit pouvoir être relancée sans créer inutilement des versions dupliquées ou laisser une configuration incohérente.

Distingue explicitement trois opérations :

1. Création d'un topic Kafka.
2. Enregistrement d'un schéma Avro dans Schema Registry.
3. Configuration de compatibilité d'un sujet Schema Registry.

Ces opérations sont différentes et doivent être automatisées et validées séparément.

### 8.5. Exploitation et limites

Explique ce qui se passe si Schema Registry devient temporairement indisponible, pendant l'enregistrement ou la récupération d'un schéma.

Documente les caches clients, les erreurs de sérialisation/désérialisation et les conséquences opérationnelles. Ne prétends pas que Schema Registry valide toutes les règles métier ou rend toute évolution sans risque.

## 9. Retries et Dead Letter Topics

Implémente une stratégie de reprise avec Spring Kafka.

Distingue :

- Erreurs transitoires et permanentes.
- Retry immédiat et backoff.
- Retry Topics.
- Dead Letter Topic.
- Rejeu manuel ou automatisé.

Documente les noms des topics, leur clé, leur rétention, leur configuration et leur procédure de traitement. Explique que les Retry Topics peuvent permettre à des événements ultérieurs d'être traités avant un événement en échec.

Ajoute des tests qui provoquent une erreur, vérifient les tentatives et démontrent le transfert vers la DLT.

Les topics de retry et DLT doivent être créés et validés par les scripts de la section 5.

## 10. Idempotence et déduplication métier

Implémente une vraie protection contre le traitement métier en double.

Utilise un identifiant d'événement stable et une table PostgreSQL avec contrainte d'unicité, par exemple `processed_events`. La déduplication et l'effet métier doivent être effectués dans une transaction locale cohérente.

Démontre le scénario suivant :

1. Le consumer reçoit un événement.
2. Le traitement métier réussit.
3. Le processus tombe avant la confirmation de l'offset.
4. Le même événement est consommé de nouveau.
5. Le second traitement ne provoque pas un second effet métier.

Explique pourquoi une vérification en mémoire est insuffisante et pourquoi l'idempotence du producer Kafka ne remplace pas la déduplication métier.

## 11. Transactions Kafka et Exactly-Once

Explique les garanties réelles et leurs limites :

- At-most-once.
- At-least-once.
- Transactions Kafka.
- `transactional.id`.
- `read_committed`.
- Commit des offsets dans une transaction Kafka, lorsque pertinent.
- Atomicité entre production et consommation dans un flux Kafka-to-Kafka.

Implémente une démonstration transactionnelle adaptée et testable.

Explique clairement qu'une transaction Kafka ne rend pas automatiquement atomique une opération qui inclut PostgreSQL ou une API externe. Ne parle pas d'« exactly-once global » sans définir précisément la frontière de la garantie.

## 12. Transactional Outbox

Implémente le pattern Outbox avec PostgreSQL.

La création de la commande et l'enregistrement de son événement Outbox doivent se produire dans la même transaction locale.

Le relay publie ensuite les événements dans Kafka. Démontre le scénario où il publie un événement, tombe avant de marquer la ligne comme publiée et republie l'événement au redémarrage.

Explique pourquoi l'Outbox évite le problème d'écriture non atomique entre la base de données et Kafka, mais ne garantit pas l'absence de doublons.

Compare brièvement le relay applicatif à une solution CDC comme Debezium.

## 13. Listen to Yourself et consistance

Ajoute un exemple où un service consomme les événements qu'il a lui-même publiés, ou une projection locale alimentée par les événements du domaine.

Explique l'intérêt de cette approche, ses limites et son lien éventuel avec la consistance éventuelle. Ne présente pas Listen to Yourself comme une garantie automatique de consistance éventuelle : explique le mécanisme précis dans l'architecture retenue.

## 14. Performance, rebalancing et observabilité

Prévois des démonstrations pour :

- Le rééquilibrage après l'arrêt d'un consumer.
- Le nombre de consumers par rapport au nombre de partitions.
- La distribution des clés.
- Les effets de `max.poll.records`, `linger.ms` et de la compression.
- Les erreurs de publication et de consommation.
- Le retard de consommation.
- Les messages en retry et dans la DLT.

Utilise des métriques réellement observées. N'invente pas de benchmarks ni de gains de performance.

## 15. Tests et scénarios de panne

Prévois des tests unitaires, d'intégration et de résilience pour les éléments pertinents :

- Création de commande.
- Publication d'événements.
- Avro et Schema Registry.
- Évolution des schémas avec `FORWARD_TRANSITIVE`.
- Réservation de stock.
- Retries et DLT.
- Idempotence métier.
- Transactional Outbox.
- Redémarrage des services.
- Rejeu d'événements.
- Transactions Kafka.
- Indisponibilité temporaire de Schema Registry.
- Création et validation idempotentes des topics.
- Détection d'un écart entre configuration attendue et configuration effective.

Les tests doivent être reproductibles. Distingue les tests réellement exécutés des tests préparés mais non exécutés.

## 16. Documentation pédagogique en français

Tous les documents doivent être versionnés dans `/docs`, numérotés et reliés par des liens Markdown relatifs.

Structure recommandée :

```text
docs/
├── 00-index.md
├── 01-objectifs-et-architecture.md
├── 02-modele-mental-kafka.md
├── 03-brokers-kraft-et-configuration.md
├── 04-topics-partitions-et-replication.md
├── 05-cles-et-ordre-des-evenements.md
├── 06-producers-et-fiabilite.md
├── 07-consumers-offsets-et-groups.md
├── 08-rebalancing-et-performance.md
├── 09-schema-registry-et-avro.md
├── 10-evolution-des-schemas-et-forward-transitive.md
├── 11-retries-et-dead-letter-topics.md
├── 12-idempotence-et-deduplication.md
├── 13-transactions-et-exactly-once.md
├── 14-transactional-outbox.md
├── 15-listen-to-yourself-et-consistance.md
├── 16-observabilite-et-diagnostic.md
├── 17-tests-et-scenarios-de-panne.md
├── 18-exploitation-et-runbook.md
└── 19-guide-de-presentation.md
```

Adapte cette structure si nécessaire, sans supprimer les explications essentielles sur Schema Registry, Avro, `FORWARD_TRANSITIVE` ou la création reproductible des topics.

Chaque document doit expliquer :

- Le problème concret résolu.
- Le concept et son fonctionnement.
- Le lien avec le code réel.
- Les configurations utilisées et leurs raisons.
- Les alternatives et leurs compromis.
- Les limites et pièges.
- Les commandes de démonstration.
- Les résultats attendus et leur vérification.

Utilise des diagrammes Mermaid, des tableaux et des exemples issus du code réel lorsque cela améliore la compréhension.

Le document `docs/04-topics-partitions-et-replication.md` doit expliquer les scripts de création et de validation des topics.

Le document `docs/10-evolution-des-schemas-et-forward-transitive.md` doit détailler la démonstration d'évolution des schémas.

Le document `docs/18-exploitation-et-runbook.md` doit fournir les commandes d'exploitation, de vérification et de réinitialisation.

Le document `docs/19-guide-de-presentation.md` doit contenir le déroulé de la formation, les fichiers à ouvrir, les commandes à lancer, les questions à poser et les résultats attendus.

Le `README.md` à la racine doit contenir les prérequis, les instructions de démarrage, les commandes principales et un lien vers `docs/00-index.md`.

## 17. Ordre de réalisation

Respecte cet ordre de travail :

1. Définir l'architecture et les contrats d'événements.
2. Définir les clés de partitionnement et les topics.
3. Configurer Kafka, KRaft, Schema Registry, PostgreSQL et Docker Compose.
4. Implémenter les scripts de disponibilité, de création, de description et de validation des topics.
5. Définir les schémas Avro et la génération des classes Java.
6. Automatiser l'enregistrement des schémas et la configuration de compatibilité.
7. Tester `FORWARD_TRANSITIVE` avec plusieurs versions historiques.
8. Implémenter les services métier.
9. Implémenter Outbox, idempotence, retries et DLT.
10. Implémenter les transactions Kafka dans le scénario retenu.
11. Écrire et exécuter les tests et scénarios de panne.
12. Rédiger tous les documents Markdown et les scripts de démonstration.
13. Vérifier les liens et la cohérence entre le code et la documentation.
14. Fournir un bilan précis de livraison.

## 18. Exigences de qualité

- Aucun pseudo-code pour les fonctionnalités essentielles.
- Aucune création manuelle de topic requise.
- Aucun topic existant considéré automatiquement comme conforme.
- Aucun script de démarrage qui supprime les données.
- Aucune suppression destructive sans commande explicite.
- Aucune configuration contradictoire entre les fichiers.
- Aucun secret réel dans le dépôt.
- Aucun test présenté comme réussi sans avoir été exécuté.
- Aucune métrique inventée.
- Aucune confusion entre les garanties théoriques et les comportements démontrés.
- Aucune confusion entre idempotence du producer, idempotence métier et transactions Kafka.
- Aucune affirmation selon laquelle Outbox élimine tous les doublons.
- Aucune affirmation selon laquelle `FORWARD_TRANSITIVE` garantit la compatibilité de toutes les évolutions possibles.

## 19. Critères d'acceptation finale

Le projet est terminé uniquement lorsque :

- Le dépôt contient le code source complet et les fichiers de configuration nécessaires.
- Kafka, Schema Registry et PostgreSQL démarrent avec Docker Compose.
- Les topics sont créés par scripts, sans intervention manuelle.
- Les scripts sont réexécutables sans perte de données.
- Le script de validation détecte les écarts de configuration.
- Les schémas Avro sont versionnés et utilisés par les producteurs et consommateurs.
- La démonstration `FORWARD_TRANSITIVE` montre des évolutions acceptées et rejetées avec plusieurs versions historiques.
- Les retries, DLT, Outbox et déduplications peuvent être démontrés.
- Les tests ont été exécutés autant que l'environnement le permet.
- Les fichiers Markdown sont complets, numérotés et reliés.
- Le guide de présentation permet de reproduire les démonstrations sans préparation supplémentaire.

À la fin, fournis un récapitulatif de l'architecture, l'arborescence livrée, les commandes de démarrage, les commandes de création et de validation des topics, les résultats des tests réellement exécutés et les limitations restantes.