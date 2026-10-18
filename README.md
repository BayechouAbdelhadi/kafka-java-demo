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

### Without an outbox

The obvious code saves the order, then sends the event:

```java
@Transactional
public Order createOrder(CreateOrderRequest request) {
    orderRepository.save(order);                       // write 1: PostgreSQL
    kafkaTemplate.send("orders.events", id, event);    // write 2: Kafka
    return order;
}
```

That's two writes to **two different systems**, and nothing makes them atomic. Whichever order you put them in, a crash between them breaks something:

```mermaid
sequenceDiagram
    participant S as order-service
    participant DB as PostgreSQL
    participant K as Kafka

    Note over S,K: ❌ Option A: commit, then send
    S->>DB: COMMIT order
    S--xK: send fails (Kafka down / crash / timeout)
    Note over DB,K: Order saved, event LOST<br/>→ stock never reserved, order stuck in CREATED forever

    Note over S,K: ❌ Option B: send, then commit
    S->>K: send ORDER_CREATED ✅
    S--xDB: COMMIT fails (constraint / crash / DB down)
    Note over DB,K: Event published for an order that DOESN'T EXIST<br/>→ stock reserved for a phantom order
```

| Approach | What goes wrong |
|---|---|
| Commit, then send | **Lost event**: the order exists but nobody hears about it |
| Send, then commit | **Phantom event**: everyone hears about an order that was rolled back |
| Send inside `@Transactional` | Same as B: the send happens before the commit, and Kafka can't be rolled back |
| Retry the send in a loop | The process can still crash before retrying, and the retry state is lost with it |
| Kafka transactions | They make Kafka atomic with Kafka, **not** with PostgreSQL |

On top of that, the HTTP request becomes slow and fails whenever Kafka is down, even though the order itself could be saved.

### How we solved it

We don't send to Kafka in the request at all. The event is saved **as a row in the same database, in the same transaction** as the order, so there's only one system and the write is atomic. A background relay publishes it afterwards.

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

| | Without outbox | With outbox |
|---|---|---|
| Order saved, event lost | possible | ❌ impossible: same commit |
| Event for a rolled-back order | possible | ❌ impossible: rolled back together |
| Kafka down | request fails | ✅ request succeeds, event sent when Kafka is back |
| Duplicates | possible | possible, harmless: consumers skip them by `eventId` |

**Never lost, never phantom, maybe duplicated → consumers are idempotent.**

Code: [OutboxWriter](common/src/main/java/com/example/kafka/common/outbox/OutboxWriter.java) (`MANDATORY`: must join the business transaction) · [OutboxRelay](common/src/main/java/com/example/kafka/common/outbox/OutboxRelay.java) (runs never overlap: each run first takes a PostgreSQL advisory lock, `pg_try_advisory_xact_lock`, and if another instance holds it the run is skipped). Watch it: stop Kafka, create orders, see `GET /api/orders/outbox/pending` fill up; restart Kafka and it drains.

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
make test   # starts the compose PostgreSQL; tests use the *_test databases
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
