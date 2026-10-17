# 10 - Évolution des Schémas et Démonstration Obligatoire de FORWARD_TRANSITIVE

[⬅️ Précédent : Schema Registry & Avro](file:///home/abdelhadi/Documents/kafka-java/docs/09-schema-registry-et-avro.md) | [Suivant : Retries & Dead Letter Topics ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/11-retries-et-dead-letter-topics.md)

---

## 1. Définition Précise de la Compatibilité `FORWARD_TRANSITIVE`

> **Règle FORWARD :** Un lecteur utilisant le **nouveau schéma** doit être capable de désérialiser les messages écrits avec les **anciens schémas**.
> **Règle TRANSITIVE :** Cette vérification ne se limite pas à la version immédiatement précédente ($V_N$ vs $V_{N-1}$), mais s'applique rigoureusement à **TOUTES les versions historiques enregistrées** ($V_N$ vs $V_1, V_2, \dots, V_{N-1}$).

```text
       Producteur Ancien (V1) ──────► Écrit Message V1 ──────► [ Kafka ]
                                                                   │
                                                                   ▼
                                                     Lecteur Nouveau (V2 ou V3)
                                                     DOIT pouvoir lire V1 !
```

### Le Cas Critique : Pourquoi un champ sans valeur par défaut est REJETÉ
Si vous enregistrez un schéma V2 qui ajoute un nouveau champ obligatoire `trackingNumber` **sans valeur par défaut** :
- Le consommateur V2 attend obligatoirement ce champ dans le flux d'octets.
- Or, les messages dans Kafka écrits par les producteurs V1 ne contiennent pas ce champ !
- Le consommateur V2 crasherait avec une exception de désérialisation.
- **Schema Registry bloque cette mise à jour avec une erreur `HTTP 409 Conflict`**.

---

## 2. Déroulé Reproductible de la Démonstration

Le script [`infrastructure/schema-registry/scripts/test-forward-transitive.sh`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/schema-registry/scripts/test-forward-transitive.sh) valide les 6 étapes clés :

### Étape 1 : Configuration du mode
```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FORWARD_TRANSITIVE"}' \
  http://localhost:8081/config/demo.forward-transitive-value
```

### Étape 2 : Enregistrement de V1 (Schéma Initial)
- Fichier : [`v1-order-event.avsc`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/schema-registry/schemas/v1-order-event.avsc)
- Champs : `orderId`, `customerId`, `totalAmount`.
- Résultat : **HTTP 200 (Schéma ID 1 créé)**.

### Étape 3 : Évolution Validée vers V2 (Ajout d'un champ avec défaut)
- Fichier : [`v2-order-event-valid.avsc`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/schema-registry/schemas/v2-order-event-valid.avsc)
- Ajout de `currency` avec `"default": "EUR"`.
- Pourquoi c'est valide : Le lecteur V2 qui lit un vieux message V1 applique automatiquement la valeur `"EUR"`.
- Résultat : **HTTP 200 (Schéma ID 2 créé)**.

### Étape 4 : Tentative d'évolution REJETÉE sous FORWARD
- Fichier : [`invalid-forward-new-field-no-default.avsc`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/schema-registry/schemas/invalid-forward-new-field-no-default.avsc)
- Ajout de `trackingNumber` sans défaut.
- Résultat de Schema Registry :
  ```json
  {"error_code": 409, "message": "Schema being registered is incompatible with an earlier schema for subject \"demo.forward-transitive-value\""}
  ```
- **Succès de la démonstration : le schéma dangereux a été intercepté et bloqué !**

### Étape 5 : Cas Spécifique TRANSITIF (Valide vs V2, mais Rejeté vs V1)
- Fichier : [`invalid-transitive-type-change.avsc`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/schema-registry/schemas/invalid-transitive-type-change.avsc)
- Changement du type `totalAmount` en `string`.
- Sous un mode non-transitif, une transition pourrait passer, mais sous **FORWARD_TRANSITIVE**, Schema Registry remonte jusqu'à V1 et constate l'incompatibilité de type.
- Résultat : **HTTP 409 Conflict**.

### Étape 6 : Évolution Validée vers V3
- Fichier : [`v3-order-event-valid.avsc`](file:///home/abdelhadi/Documents/kafka-java/infrastructure/schema-registry/schemas/v3-order-event-valid.avsc)
- Ajout du champ `status` avec `"default": "PENDING"`.
- Résultat : **HTTP 200**. L'historique V1, V2, V3 est parfaitement cohérent.
