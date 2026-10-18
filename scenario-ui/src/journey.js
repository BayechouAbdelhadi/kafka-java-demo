import { api, sleep } from './api.js';

// Reconstructs the path of one order through the system from what is observable:
// the order and outbox over REST, the Kafka records keyed by the order id, and the consumer groups'
// committed offsets (a record below a group's committed offset has been processed by that service).

export const TOPICS = {
  orders: 'orders.events',
  inventory: 'inventory.events',
  retry: 'orders.events-notification-retry',
  dlt: 'orders.events-notification-dlt',
};

// Browser time at which an order was first seen leaving CREATED; the order has no updatedAt.
const settledSeenAt = new Map();
const lastStatus = new Map();

function trackStatus(order) {
  const previous = lastStatus.get(order.id);
  if (previous === 'CREATED' && order.status !== 'CREATED' && !settledSeenAt.has(order.id)) {
    settledSeenAt.set(order.id, Date.now());
  }
  lastStatus.set(order.id, order.status);
}

/** One observation of an order, from every source. */
export async function snapshot(orderId) {
  const order = await api.getOrder(orderId);
  trackStatus(order);
  const products = [...new Set(order.items.map((i) => i.productId))];
  const [outbox, stock] = await Promise.all([
    api.pendingOutbox(),
    Promise.all(products.map((p) => api.getStock(p).catch(() => ({ productId: p, missing: true })))),
  ]);

  let kafka = null;
  let kafkaError = null;
  try {
    const [orders, inventory, retry, dlt, inventoryGroup, orderGroup, notificationGroup] = await Promise.all([
      api.topicMessages(TOPICS.orders, orderId),
      api.topicMessages(TOPICS.inventory, orderId),
      api.topicMessages(TOPICS.retry, orderId),
      api.topicMessages(TOPICS.dlt, orderId),
      api.groupOffsets('inventory-service'),
      api.groupOffsets('order-service'),
      api.groupOffsets('notification-service'),
    ]);
    kafka = { orders, inventory, retry, dlt, groups: { inventoryGroup, orderGroup, notificationGroup } };
  } catch (e) {
    kafkaError = e.message;
  }

  return { order, inOutbox: outbox.some((m) => m.key === orderId), stock, kafka, kafkaError };
}

const consumed = (offsets, record) => record != null && (offsets[`${record.topic}:${record.partition}`] ?? 0) > record.offset;
const where = (r) => `${r.topic} · partition ${r.partition} · offset ${r.offset}`;
const exception = (r) => (r.headers['kafka_exception-message'] ?? 'unknown error').replace(/^Listener failed; /, '');
const items = (order) => order.items.map((i) => `${i.quantity} × ${i.productId}`).join(', ');

/**
 * Turns a snapshot into the two lanes of the diagram (saga and notification), plus a summary used
 * by the scenario checks. Step state: pending | active | done | warn | failed.
 */
export function describe(snap) {
  const { order, kafka } = snap;
  const t0 = Date.parse(order.createdAt);
  const settledStatus = order.status !== 'CREATED';

  const created = kafka?.orders.find((m) => m.value?.eventType === 'ORDER_CREATED') ?? kafka?.orders[0] ?? null;
  const result = kafka?.inventory[0] ?? null;
  const reserved = result?.value?.success === true;
  const g = kafka?.groups;

  const saga = [
    {
      key: 'placed',
      service: 'order-service',
      title: 'Order placed',
      detail: 'POST /api/orders · order (CREATED) + outbox row, one DB transaction',
      state: 'done',
      time: t0,
    },
    {
      key: 'orders-topic',
      kind: 'topic',
      service: 'outbox relay → Kafka',
      title: TOPICS.orders,
      detail: created
        ? 'ORDER_CREATED published'
        : snap.inOutbox
          ? 'waiting in the outbox for the relay'
          : kafka
            ? 'not seen yet'
            : 'unknown (Kafka UI unreachable)',
      where: created && where(created),
      state: created ? 'done' : 'active',
      time: created?.timestamp,
    },
    {
      key: 'inventory',
      service: 'inventory-service',
      title: result ? (reserved ? 'Stock reserved' : 'Reservation refused') : 'Reserve stock',
      detail: result
        ? reserved
          ? `reserved ${items(order)}`
          : result.value?.reason
        : created
          ? consumed(g.inventoryGroup, created)
            ? 'consumed, writing the result…'
            : 'consuming orders.events…'
          : '',
      state: result ? (reserved ? 'done' : 'failed') : created ? 'active' : 'pending',
      time: result?.value?.occurredAt,
    },
    {
      key: 'inventory-topic',
      kind: 'topic',
      service: 'outbox relay → Kafka',
      title: TOPICS.inventory,
      detail: result ? `${result.value?.eventType} published` : '',
      where: result && where(result),
      state: result ? 'done' : 'pending',
      time: result?.timestamp,
    },
    {
      key: 'status',
      service: 'order-service',
      title: settledStatus ? `Order ${order.status}` : 'Update order',
      detail: settledStatus
        ? `consumed ${result?.value?.eventType ?? 'the inventory result'} and updated the order`
        : result
          ? 'consuming inventory.events…'
          : 'still CREATED',
      state: settledStatus ? (order.status === 'CONFIRMED' ? 'done' : 'failed') : result ? 'active' : 'pending',
      time: settledSeenAt.get(order.id),
      approx: true,
    },
  ];

  // Notification fan-out: every failed attempt leaves a record on the retry topic (or the DLT).
  const retries = kafka?.retry ?? [];
  const dlt = kafka?.dlt[0] ?? null;
  const last = retries.at(-1) ?? created;
  const notified = !!created && !dlt && consumed(g.notificationGroup, last);
  const notification = retries.map((r, i) => ({
    key: `retry-${i}`,
    service: 'notification-service',
    title: `Attempt ${i + 1} failed`,
    detail: `${exception(r)}, retried in 2 s`,
    where: where(r),
    state: 'warn',
    time: r.timestamp,
  }));
  const attempt = retries.length + 1;
  if (dlt) {
    notification.push({
      key: 'dlt',
      service: 'notification-service',
      title: `Attempt ${attempt} failed: dead-lettered`,
      detail: exception(dlt),
      where: where(dlt),
      state: 'failed',
      time: dlt.timestamp,
    });
  } else if (notified) {
    notification.push({
      key: 'notified',
      service: 'notification-service',
      title: 'Customer notified',
      detail: attempt === 1 ? 'first attempt succeeded' : `attempt ${attempt} succeeded`,
      state: 'done',
    });
  } else {
    notification.push({
      key: 'pending',
      service: 'notification-service',
      title: `Attempt ${attempt}`,
      detail: !kafka ? 'unknown (Kafka UI unreachable)' : !created ? 'waits for orders.events' : retries.length ? 'waiting for the retry backoff…' : 'consuming orders.events…',
      state: created ? 'active' : 'pending',
    });
  }

  // Reached steps by time; those with no known time (see `approx`, `notified`) go last.
  const reached = [...saga, ...notification].filter((s) => s.state !== 'pending' && s.state !== 'active');
  const timeline = [
    ...reached.filter((s) => s.time != null).sort((a, b) => a.time - b.time),
    ...reached.filter((s) => s.time == null),
  ].map((s) => ({ ...s, delta: s.time == null ? null : s.time - t0 }));

  return {
    order,
    saga,
    notification,
    timeline,
    ordersTopicStep: saga[1],
    summary: { status: order.status, retries: retries.length, deadLettered: !!dlt, notified, stock: snap.stock },
    settled: settledStatus && (!kafka || !!dlt || notified),
    kafkaError: snap.kafkaError,
  };
}

/** Polls an order until its saga and notification are both settled, or throws after timeoutMs. */
export async function awaitSettled(orderId, { timeoutMs = 30000, intervalMs = 700 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const journey = describe(await snapshot(orderId));
    if (journey.settled) return journey;
    if (Date.now() > deadline) throw new Error(`order ${orderId} not settled after ${timeoutMs / 1000}s`);
    await sleep(intervalMs);
  }
}
