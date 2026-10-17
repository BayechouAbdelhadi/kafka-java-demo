# 16 - Observabilité, Métriques et Diagnostic

[⬅️ Précédent : Listen to Yourself](file:///home/abdelhadi/Documents/kafka-java/docs/15-listen-to-yourself-et-consistance.md) | [Suivant : Tests & Scénarios de Panne ➡️](file:///home/abdelhadi/Documents/kafka-java/docs/17-tests-et-scenarios-de-panne.md)

---

## 1. Endpoints Spring Boot Actuator

Chaque microservice expose nativement les endpoints de supervision Actuator :
- `order-service` : `http://localhost:8081/actuator/health`
- `inventory-service` : `http://localhost:8082/actuator/health`
- `notification-service` : `http://localhost:8083/actuator/health`

Toutes les métriques Kafka Producer et Consumer sont collectées automatiquement par Micrometer et exposées au format Prometheus sur `/actuator/prometheus`.

---

## 2. Métriques Clés à Surveiller en Production

| Métrique | Seuil d'Alerte | Signification Opérationnelle |
|---|---|---|
| `records-lag-max` | Croissance continue | Le consommateur prend du retard par rapport au rythme des producteurs. |
| `UnderReplicatedPartitions` | $> 0$ | Un broker est tombé ou une partition a perdu un réplica ISR. |
| `request-latency-avg` | Pic brutal | Latence réseau ou contention disque sur les brokers. |
| Messages dans `orders.dlt` | $> 0$ | Des messages ont définitivement échoué et requièrent une investigation humaine. |

---

## 3. Commandes CLI Utiles pour le Diagnostic

### Inspecter l'avancement et le lag d'un Consumer Group :
```bash
docker exec -it kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group inventory-service-group
```

### Afficher les offsets minimaux et maximaux d'un topic :
```bash
docker exec -it kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic orders.events --time -1
```
