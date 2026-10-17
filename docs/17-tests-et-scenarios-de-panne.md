# 17 - Tests et Scénarios de Panne

[⬅️ Précédent : Observabilité](file:///home/abdelhadi/Documents/kafka-java/docs/16-observabilite-et-diagnostic.md) | [Suivant : Exploitation & Runbook ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/18-exploitation-et-runbook.md)

---

## 1. Stratégie de Test

Le projet combine :
1. **Tests Unitaires & Slice Tests Spring** (JUnit 5, Mockito) pour la logique métier et les entités.
2. **Tests d'Intégration Kafka** avec base de données embarquée H2 et Spring Kafka Test.
3. **Scripts de Démonstration Réels de Pannes** exécutés contre l'infrastructure locale complète.

---

## 2. Démonstrations de Panne Reproductibles

### Panne 1 : Erreur Permanente et Routage Immédiat vers la DLT
- **Script** : `./scripts/demo-retry-dlt.sh`
- **Mécanisme** : Envoi d'une commande avec `customerId = "invalid_user"`.
- **Résultat vérifié** : Aucune tentative de retry superflue. Le message est directement consigné dans la Dead Letter Topic `orders.events-dlt`.

### Panne 2 : Erreur Transitoire et Guérison par Backoff Exponentiel
- **Script** : `./scripts/demo-retry-dlt.sh`
- **Mécanisme** : Envoi d'une commande avec `customerId = "flaky_user"`.
- **Résultat vérifié** : Échec à la tentative 1 (1s de délai), échec à la tentative 2 (2s de délai), puis succès et confirmation à la 3e tentative.

### Panne 3 : Rejeu de Message et Déduplication
- **Mécanisme** : Réinjection du même `eventId` dans `orders.events`.
- **Résultat vérifié** : La table `processed_events` intercepte la duplication. Le stock n'est décrémenté qu'une seule et unique fois.

### Panne 4 : Détection d'un Écart de Configuration de Topic
- **Mécanisme** : Modification manuelle des partitions d'un topic test.
- **Résultat vérifié** : `./infrastructure/kafka/scripts/validate-topics.sh` signale l'écart et stoppe avec un code de retour `1`.
