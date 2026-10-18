# Kafka & Spring Boot — Event-Driven Order System

3 Spring Boot microservices that talk **only through Kafka**, each with **its own PostgreSQL database**.

**Contents:** [What we are building](#what-we-are-building) · [Prerequisites](#prerequisites) · [Learning goals](#learning-goals) · [1. Saga](#1-saga) · [2. Outbox](#2-transactional-outbox) · [3. Retries & DLQ](#3-retries--dlq) · [4. Delivery semantics](#4-delivery-semantics) · [5. Idempotent producer](#5-idempotent-producer) · [6. Idempotent consumers](#6-idempotent-consumers) · [7. Schema Registry & Avro](#7-schema-registry--avro) · [Run](#run)

## What we are building

An online shop's order flow: **place an order → reserve the stock → notify the customer.**

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

    O -- ORDER_CREATED --> T1
    T1 --> I
    T1 --> N
    I -- "RESERVED / FAILED" --> T2
    T2 --> O
```

### Components

| Component | Responsibility | Produces | Consumes |
|---|---|---|---|
| **order-service** | REST API; creates the order, sets its final status | `orders.events` | `inventory.events` |
| **inventory-service** | Owns the stock; reserves an order's lines | `inventory.events` | `orders.events` |
| **notification-service** | Notifies the customer | | `orders.events` |
| **avro-schemas** | Event contracts: `OrderEvent`, `InventoryResultEvent` | | |
| **common** | Kafka and database code shared by the services | | |
| **Kafka** ×4 | KRaft cluster (3 broker+controller, 1 broker), no ZooKeeper | | |
| **Schema Registry** | Stores the event schemas | | |
| **PostgreSQL** | One database per service | | |
| **Kafka UI** | Browse topics, messages, groups | | |

### Order flow

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant O as order-service
    participant K as Kafka
    participant I as inventory-service
    participant N as notification-service

    C->>O: POST /api/orders
    O->>O: save order (CREATED)
    O-->>C: 201 CREATED
    O->>K: ORDER_CREATED → orders.events
    par
        K->>I: ORDER_CREATED
        I->>I: reserve stock
        I->>K: RESERVED / FAILED → inventory.events
    and
        K->>N: ORDER_CREATED
        N->>N: notify customer
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

### Kafka setup

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

### Databases

```mermaid
flowchart LR
    subgraph pg["PostgreSQL"]
        subgraph odb["order_db"]
            o1[orders] --- o2[order_items]
        end
        subgraph idb["inventory_db"]
            i1[stocks]
        end
        subgraph ndb["notification_db"]
            n1["(no business data)"]
        end
    end
```

No service reads another's database. Schemas are managed by Flyway (`V1` shared, `V2` per service). The pattern tables (`outbox_messages`, `processed_events`) are introduced in their sections.

## Prerequisites

Kafka basics, assumed known:

- **Topic → partitions**: an append-only log split into partitions; each record has an **offset**.
- **Record = key + value (+ headers)**: same key → same partition → ordered.
- **Producer**: writes records; `acks` says how many replicas must confirm.
- **Consumer group**: the partitions are shared among its members; each group gets every record; progress is the **committed offset**.
- **Replication**: replication factor (RF), in-sync replicas (ISR), `min.insync.replicas`.

## Learning goals

Three databases must stay consistent **without a distributed transaction**, while any process, broker or network link can fail. Each pattern answers one failure:

| # | Pattern | Problem it solves |
|---|---|---|
| 1 | [Saga](#1-saga) | One business transaction spans 3 databases |
| 2 | [Transactional outbox](#2-transactional-outbox) | Save to the DB **and** publish to Kafka, atomically |
| 3 | [Retries & DLQ](#3-retries--dlq) | Handle failures without losing or blocking messages |
| 4 | [Delivery semantics](#4-delivery-semantics) | At-most / at-least / exactly once: which one, and its cost |
| 5 | [Idempotent producer](#5-idempotent-producer) | Producer retries must not duplicate records |
| 6 | [Idempotent consumers](#6-idempotent-consumers) | Redelivered events must be harmless |
| 7 | [Schema Registry & Avro](#7-schema-registry--avro) | A shared, evolvable contract between services |

## 1. Saga

**Technique:** a business transaction split into **local transactions, one per service**, chained by events. No global rollback: a failure is handled by a **compensating** step.

```mermaid
flowchart LR
    T1["order-service<br/>tx 1: order CREATED"] -- ORDER_CREATED --> T2["inventory-service<br/>tx 2: reserve all lines or none"]
    T2 -- INVENTORY_RESERVED --> T3["order-service<br/>tx 3: CONFIRMED ✅"]
    T2 -- INVENTORY_FAILED --> T4["order-service<br/>tx 3: REJECTED<br/>(compensates tx 1)"]
```

**Why:** each service owns its database, so one ACID transaction across the three is impossible.

- Nothing to undo in inventory: it reserves all-or-nothing in one transaction. A later failing step (e.g. payment) would need a compensating `RELEASE_STOCK`.
- Eventual consistency: between steps the order is visibly `CREATED`.

**Two flavours.** Ours is **choreography**: each service reacts to events.

```mermaid
flowchart LR
    c1[order] -- ORDER_CREATED --> c2[inventory] -- INVENTORY_RESERVED --> c3[order]
```

**Orchestration**: a coordinator sends commands and handles the replies.

```mermaid
flowchart LR
    OR[saga orchestrator] -- "ReserveStock" --> s1[inventory]
    s1 -- reply --> OR
    OR -- "then ChargePayment" --> s2[payment]
    s2 -- reply --> OR
```

| | Choreography (ours) | Orchestration |
|---|---|---|
| Flow lives | Spread across services | In one place |
| Fits | Few steps | Many steps, complex compensations |

## 2. Transactional outbox

**Problem:** saving the order and sending the event are two writes to **two systems**; nothing makes them atomic.

```java
@Transactional
public Order createOrder(CreateOrderRequest request) {
    orderRepository.save(order);                       // write 1: PostgreSQL
    kafkaTemplate.send("orders.events", id, event);    // write 2: Kafka
    return order;
}
```

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

Also: the HTTP request becomes slow, and fails whenever Kafka is down.

**Technique:** write the event **as a row in the same database, in the same transaction** as the order (one system → atomic). A background relay publishes it afterwards.

```mermaid
flowchart LR
    REQ([Request]) --> tx
    subgraph tx["1 transaction"]
        B[INSERT order]
        E[INSERT outbox_messages]
    end
    tx --> DB[(DB)]
    R["OutboxRelay<br/>every 500 ms"] -- "① lock batch<br/>FOR UPDATE SKIP LOCKED" --> DB
    R -- "② send, wait acks" --> K{{Kafka}}
    R -- "③ delete sent rows" --> DB
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

- `outbox_messages` (order_db, inventory_db): events waiting to be published.
- [OutboxWriter](common/src/main/java/com/example/kafka/common/outbox/OutboxWriter.java): `MANDATORY`, must join the business transaction.
- [OutboxRelay](common/src/main/java/com/example/kafka/common/outbox/OutboxRelay.java): runs never overlap; each run first takes a PostgreSQL advisory lock (`pg_try_advisory_xact_lock`) and is skipped if another instance holds it.
- Watch it: stop Kafka, create orders, see `GET /api/orders/outbox/pending` fill up; restart Kafka and it drains.

## 3. Retries & DLQ

**Technique:** retry a failed record; if it keeps failing, park it on a **dead-letter topic (DLT)** for inspection.

**Why:** a failing record must neither be lost nor block the records behind it forever.

```mermaid
flowchart TD
    F[Record fails] --> Q{Does the order of<br/>events matter?}
    Q -- "yes: saga (order, inventory)" --> BL[Blocking retries<br/>partition waits]
    Q -- "no: notification" --> NB[Non-blocking retries<br/>partition keeps flowing]
```

**Blocking** (order, inventory): 3 attempts, 1 s apart ([KafkaErrorHandlingConfig](common/src/main/java/com/example/kafka/common/kafka/KafkaErrorHandlingConfig.java)), then `<topic>-dlt`, same partition.

```mermaid
flowchart LR
    M[Record] --> A1[try 1] -- fail --> A2[try 2] -- fail --> A3[try 3] -- fail --> DLT{{topic-dlt}} -- acked --> C[commit offset,<br/>next record]
```

**Non-blocking** (notification): 4 attempts, 2 s apart, through a retry topic (`@RetryableTopic` in [NotificationConsumer](notification-service/src/main/java/com/example/kafka/notification/consumer/NotificationConsumer.java)). A permanent error skips the retries.

```mermaid
flowchart LR
    M{{orders.events}} --> L[try]
    L -- transient --> R{{retry topic<br/>2 s}} --> L
    L -- "4 fails / permanent" --> DLT{{notification-dlt}} -- acked --> C[commit offset]
```

| Topic | Holds |
|---|---|
| `orders.events-dlt`, `inventory.events-dlt` | Records that failed 3 blocking attempts |
| `orders.events-notification-retry` | Notifications waiting for their next attempt |
| `orders.events-notification-dlt` | Notifications that failed 4 times, or permanently |

- **Retries exhausted → DLT → commit**: once the DLT acknowledged the record, its offset is committed and the partition moves on. If the DLT publication itself fails, nothing is committed and the record is retried: never dropped.
- **Poison pills** (records that cannot be deserialized) go straight to the DLT, as their original bytes ([AvroOrBytesSerializer](common/src/main/java/com/example/kafka/common/kafka/AvroOrBytesSerializer.java)): retrying cannot fix them, and Avro-encoding them would register a `bytes` schema that then rejects every real record on that DLT.
- Demo: customer `flaky_*` fails twice then succeeds; `invalid_*` goes straight to the DLT.

## 4. Delivery semantics

When does the consumer commit its offset, relative to processing? A crash in between decides the guarantee:

**At most once**: commit, then process.

```mermaid
flowchart LR
    a1[poll] --> a2[commit offset] --> a3["💥 crash"] --> a4["record skipped<br/>❌ LOST"]
```

**At least once** (ours): process, then commit.

```mermaid
flowchart LR
    b1[poll] --> b2[process] --> b3["💥 crash"] --> b4["record redelivered<br/>⚠️ DUPLICATE"]
```

| Semantics | How | Risk |
|---|---|---|
| At most once | Commit the offset **before** processing; producer without retries | Loss |
| **At least once** (ours) | Process, **then** commit the offset; producer retries until acked | Duplicates |
| Exactly once | Kafka transactions (read-process-write) | Only covers Kafka → Kafka, not our PostgreSQL writes |

**Why at-least-once:** losing an order is not acceptable; a duplicate can be made harmless. Spring commits the offset after the listener returns, and the relay deletes a row only once Kafka acked it.

Where duplicates come from here:

```mermaid
flowchart LR
    D1["Relay sent a batch,<br/>crashed before DELETE"] --> DUP[Same event<br/>delivered twice]
    D2["Consumer committed its DB tx,<br/>crashed before the offset commit"] --> DUP
    D3["Rebalance before<br/>the offset commit"] --> DUP
    DUP --> IC["Idempotent consumer<br/>(section 6)"] --> EO[✅ Effectively once]
```

## 5. Idempotent producer

**Problem:** the broker writes a record, the ack is lost, the producer retries → the record is written twice.

**Technique:** each producer gets an id and numbers its records per partition; the broker drops a sequence number it already has.

```mermaid
sequenceDiagram
    participant P as Producer (id 42)
    participant B as Broker
    P->>B: record, seq 7
    B->>B: write seq 7
    B--xP: ack lost
    P->>B: retry: record, seq 7
    B->>B: seq 7 already written → drop
    B-->>P: ack ✅ (one copy in the log)
```

| Config | Value | Here |
|---|---|---|
| `enable.idempotence` | `true` | Default since Kafka 3.0 |
| `acks` | `all` (required) | Set in [common-defaults.yml](common/src/main/resources/common-defaults.yml) |
| `retries` | `> 0` | Default `Integer.MAX_VALUE` |
| `max.in.flight.requests.per.connection` | `≤ 5` | Default `5`; order still kept |

- If a conflicting setting is added (e.g. `acks=1`) and `enable.idempotence` is not set explicitly, idempotence is **silently disabled**; set to `true` explicitly, the producer fails at startup instead.
- **Limit:** works within one producer session. After a restart the producer has a new id, so the relay re-sending a batch after a crash is **not** caught: that's the consumers' job. Across sessions, Kafka needs a `transactional.id`.

## 6. Idempotent consumers

**Technique:** remember what was processed, and skip it the second time.

**Why:** at-least-once delivers duplicates; processing an event twice must have the same effect as once.

```mermaid
flowchart LR
    E[Event] --> Q{eventId in<br/>processed_events?}
    Q -- yes --> S[Skip]
    Q -- no --> P["tx: business change<br/>+ outbox row<br/>+ processed_events"]
```

| Service | Technique | Why it holds |
|---|---|---|
| inventory | `processed_events` row in the same transaction as the stock change and the outbox row | Marker and effect commit together, or not at all |
| notification | `processed_events` | The send itself is outside the DB: a crash between send and commit can still notify twice |
| order | State machine: only `CREATED` can change | A second result finds `CONFIRMED`/`REJECTED`: no-op |

- `processed_events` (inventory_db, notification_db): event IDs already handled.
- Dedupe on **`eventId`** (a UUID set when the event is created), not on the offset: a record re-sent by the relay gets a new offset.
- The marker must be written **in the same transaction** as the effect; otherwise a crash leaves one without the other.

## 7. Schema Registry & Avro

**Technique:** services share **contracts**, not code: Avro schemas in [avro-schemas](avro-schemas/src/main/avro) (`OrderEvent`, `InventoryResultEvent`), compiled to Java classes. Schema Registry stores them and checks every change.

**Why:** compact records, and a producer cannot publish a change that would break existing consumers.

```mermaid
sequenceDiagram
    participant P as Producer
    participant SR as Schema Registry
    participant K as Kafka
    participant C as Consumer
    P->>SR: register schema, first send only (subject orders.events-value)
    SR-->>P: schema id
    P->>K: magic byte + schema id + Avro binary
    K->>C: record
    C->>SR: schema for id (cached)
    C->>C: decode to OrderEvent
```

A record's value on the wire:

```mermaid
flowchart LR
    m["magic byte<br/>0"] --- id["schema id<br/>4 bytes"] --- body["Avro binary<br/>no field names"]
```

**Compatibility `FORWARD_TRANSITIVE`:** consumers on **any** older schema can read the new one, so producers can upgrade first.

```mermaid
flowchart LR
    P2["producer on v2"] --> K{{topic}} --> C1["consumer on v1 ✅"]
    K --> C2["consumer on v2 ✅"]
```

| Change | Accepted? |
|---|---|
| Add a field | ✅ old consumers ignore it |
| Remove a field that has a default | ✅ old consumers use the default |
| Remove a field without a default, change a type incompatibly | ❌ rejected at registration |

- Optional fields are unions with a default, e.g. `reason: ["null", "string"]`, default `null`.
- A record that does not match its schema is a poison pill → DLT ([3](#3-retries--dlq)).

## Run

Requires Docker (your user in the `docker` group), Java 21, Maven 3.9, `make`; `curl` and `jq` for `make demo`, Node 18+ for `make ui`.

### Quick start

```bash
make start
```

This one command starts the infrastructure in Docker (Kafka, Schema Registry, PostgreSQL, Kafka UI), builds every module, starts the three services as local JVMs, waits until they are all healthy, then serves the scenario UI on http://localhost:5173. `Ctrl+C` stops the UI only; the services keep running until `make down`.

Without the UI, `make up` does the same and returns; run `make demo` for the scenarios in the terminal.

To run the services from your IDE instead, start only the infrastructure with `make infra`, then run each `*Application` class.

### All targets

```bash
make start  # make up, then make ui
make up     # infrastructure in Docker, services as local JVMs (logs in logs/)
make demo   # end-to-end scenarios
make ui     # scenario UI with each order's path through the services and topics: http://localhost:5173
make logs
make down   # stop the services and reset the infrastructure
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
| Scenario UI | http://localhost:5173 (`make ui`) |
| Orders | http://localhost:8080/api/orders (`/outbox/pending`) |
| Inventory | http://localhost:8082/api/inventory |
| Kafka UI | http://localhost:8090 |
| Schema Registry | http://localhost:8081 |
| Kafka | localhost:9092, 9094, 9095, 9096 |
| PostgreSQL | localhost:5432 (postgres / postgres) |
