# Kafka & Spring Boot — Event-Driven Order System

3 Spring Boot microservices that talk **only through Kafka** (Avro + Schema Registry), each with **its own PostgreSQL database**, publishing through a **Transactional Outbox**.

## Architecture

```mermaid
flowchart LR
    client([Client]) -- "POST /api/orders" --> O

    subgraph O["order-service :8080"]
        ODB[(order_db)]
    end
    subgraph I["inventory-service :8082"]
        IDB[(inventory_db)]
    end
    subgraph N["notification-service :8083"]
        NDB[(notification_db)]
    end

    T1{{orders.events}}
    T2{{inventory.events}}

    O -- "ORDER_CREATED (outbox)" --> T1
    T1 --> I
    T1 --> N
    I -- "RESERVED / FAILED (outbox)" --> T2
    T2 --> O
```

## Components

| Component | Role | Produces | Consumes |
|---|---|---|---|
| **order-service** | REST API, creates orders, sets final status | `orders.events` | `inventory.events` |
| **inventory-service** | Reserves stock (all lines or none) | `inventory.events` | `orders.events` |
| **notification-service** | Notifies customer, demos retry topic + DLT | retry / DLT topics | `orders.events` |
| **avro-schemas** | `OrderEvent`, `InventoryResultEvent` contracts | | |
| **common** | Outbox, idempotency, error handling, shared config | | |
| **Kafka** ×4 | KRaft cluster (3 broker+controller, 1 broker), no ZooKeeper | | |
| **Schema Registry** | Stores Avro schemas, `FORWARD_TRANSITIVE` | | |
| **PostgreSQL** | One database per service | | |
| **Kafka UI** | Browse topics, messages, groups | | |

## Order flow

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant O as order-service
    participant K as Kafka
    participant I as inventory-service
    participant N as notification-service

    C->>O: POST /api/orders
    O->>O: tx: save order (CREATED) + outbox row
    O-->>C: 201 CREATED
    O->>K: relay → orders.events
    par
        K->>I: ORDER_CREATED
        I->>I: tx: reserve stock + outbox row
        I->>K: relay → inventory.events
    and
        K->>N: ORDER_CREATED
        N->>N: send notification
    end
    K->>O: INVENTORY_RESERVED / FAILED
    O->>O: CONFIRMED / REJECTED
```

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> CONFIRMED: INVENTORY_RESERVED
    CREATED --> REJECTED: INVENTORY_FAILED
```

## Kafka

```mermaid
flowchart LR
    subgraph producer[" "]
        A["key = order-A"]
        B["key = order-B"]
    end
    subgraph topic["orders.events — 3 partitions × RF 3"]
        P0[P0]
        P1[P1]
        P2[P2]
    end
    A --> P1
    B --> P0
    topic -- "group: inventory-service" --> INV[inventory]
    topic -- "group: notification-service" --> NOT[notification]
```

- **Key = `orderId`** → all events of an order on one partition, in order.
- **One consumer group per service** → each service gets every event.
- **`acks=all` + `min.insync.replicas=2` + RF 3** → no loss if a broker dies.

| Topic | From → To |
|---|---|
| `orders.events` | order → inventory, notification |
| `inventory.events` | inventory → order |
| `orders.events-dlt`, `inventory.events-dlt` | failed records (3 attempts) |
| `orders.events-notification-retry` / `-dlt` | notification retries / dead letters |

## Databases

```mermaid
flowchart LR
    subgraph pg["PostgreSQL"]
        subgraph odb["order_db"]
            o1[orders] --- o2[order_items]
            o3[outbox_messages]
        end
        subgraph idb["inventory_db"]
            i1[stocks]
            i3[outbox_messages]
            i4[processed_events]
        end
        subgraph ndb["notification_db"]
            n4[processed_events]
        end
    end
```

- `outbox_messages` — events waiting to be published.
- `processed_events` — event IDs already handled (dedup).
- Schemas managed by Flyway (`V1` shared, `V2` per service).

## Transactional Outbox

**Problem:** saving to the DB and sending to Kafka can't be atomic → crash in between = lost or phantom event.

**Solution:** save the event in the DB **in the same transaction**, publish it later.

```mermaid
flowchart LR
    REQ([Request]) --> tx
    subgraph tx["1 transaction"]
        B[INSERT order]
        E[INSERT outbox_messages]
    end
    tx --> DB[(DB)]
    R["OutboxRelay<br/>every 500 ms"] -- "1. lock batch<br/>FOR UPDATE SKIP LOCKED" --> DB
    R -- "2. send, wait acks" --> K{{Kafka}}
    R -- "3. delete sent rows" --> DB
```

```mermaid
flowchart TD
    S[Relay sends] --> A{Acked?}
    A -- no --> RT[Row kept → retried]
    A -- yes --> D{DELETE committed?}
    D -- yes --> OK[✅ Done]
    D -- "no (crash)" --> DUP[Sent again → duplicate]
    DUP --> IDEM[Consumer skips it by eventId]
```

**Never lost, maybe duplicated → consumers are idempotent.**

## Idempotent consumers

```mermaid
flowchart LR
    E[Event] --> Q{eventId in<br/>processed_events?}
    Q -- yes --> S[Skip]
    Q -- no --> P["tx: business change<br/>+ outbox row<br/>+ processed_events"]
```

- **inventory / notification** → `processed_events` table.
- **order** → state machine (only `CREATED` can change).

## Error handling

**Blocking** (order, inventory) — partition waits, order preserved:

```mermaid
flowchart LR
    M[Record] --> A1[try 1] -- fail --> A2[try 2] -- fail --> A3[try 3] -- fail --> DLT{{topic-dlt}}
```

**Non-blocking** (notification) — partition keeps flowing:

```mermaid
flowchart LR
    M{{orders.events}} --> L[try]
    L -- transient --> R{{retry topic<br/>2 s}} --> L
    L -- "4 fails / permanent" --> DLT{{notification-dlt}}
```

Poison pills (undeserializable messages) go straight to the DLT.

## Run

Requires Docker, Java 21, Maven 3.9, `make`, `curl`, `jq`.

```bash
make up     # everything in containers
make demo   # end-to-end scenarios
make logs
make down   # reset everything
make test   # no Docker needed
```

| Demo | Result |
|---|---|
| Order 2 (stock 5) | `CONFIRMED` |
| Order 10 | `REJECTED` |
| Customer `flaky_*` | retried, then notified |
| Customer `invalid_*` | sent to DLT |

| URL | |
|---|---|
| Orders | http://localhost:8080/api/orders (`/outbox/pending`) |
| Inventory | http://localhost:8082/api/inventory |
| Kafka UI | http://localhost:8090 |
| Schema Registry | http://localhost:8081 |
| Kafka | localhost:9092, 9094, 9095, 9096 |
| PostgreSQL | localhost:5432 (postgres / postgres) |
